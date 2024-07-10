from dataclasses import dataclass
import sys
import os
from sklearn.model_selection import StratifiedKFold, train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeClassifier
from mlflow.pyfunc import PythonModel
from .utils.evaluation import ModelEvaluation
from .utils.logger_utils import get_logger
import mlflow
from mlflow.models import infer_signature
from databricks.sdk.runtime import *
from databricks.feature_store import FeatureStoreClient, FeatureLookup


_logger = get_logger()
fs = FeatureStoreClient()

@dataclass
class MlflowTrackingCfg:
    run_name: str
    experiment_path: str
    model_name: str

@dataclass
class FeatureStoreTableCfg:
    database_name: str
    table_name_customers: str
    table_name_orders: str
    primary_keys_customers: str
    primary_keys_orders: str

@dataclass
class LabelTableCfg:
    database_name: str
    table_name: str
    target: str
        


@dataclass
class ModelTrainConfig:
    mlflow_tracking_cfg: MlflowTrackingCfg
    feature_store_table_cfg: FeatureStoreTableCfg
    labels_table_cfg: LabelTableCfg
    pipeline_params: dict
    model_params: dict
    training_params: dict
    conf: dict
    env_vars: dict


class ModelTrainPipeline:

    @classmethod
    def create_train_pipeline(cls, pipeline_params, model_params):

        # Pipeline params
        numeric_features = pipeline_params['numeric_features']
        folds = pipeline_params['folds']

        # Numerical transformations
        numeric_transformer = Pipeline(
            steps=[("imputer", SimpleImputer(strategy="median"))]
        )


        # Preprocessing pipeline
        preprocessor = ColumnTransformer(
            transformers=[
                ("num", numeric_transformer, numeric_features,),
            
            ],
            remainder='passthrough'
            
        )

        skf = StratifiedKFold(n_splits=folds, shuffle=True, random_state=model_params['random_state'])



        # Create a Random Forest classifier
        model = DecisionTreeClassifier(**model_params)

        # Pipeline de dados
        pipeline = Pipeline( steps = [('preprocessor', preprocessor),
                                      ('dt_classifier', model)])

        return pipeline
    
class ModelWrapper(PythonModel):
    def __init__(self, trained_model): 
        self.model = trained_model

    def predict(self, context, model_input):
        return self.model.predict_proba(model_input)

    

class ModelTrain:
    def __init__(self, cfg: ModelTrainConfig):
        self.cfg = cfg

    @staticmethod
    def _set_experiment(mlflow_tracking_cfg):
       
        if mlflow_tracking_cfg.experiment_path is not None:
            _logger.info(f'MLflow experiment_path: {mlflow_tracking_cfg.experiment_path}')
            mlflow.set_experiment(experiment_name=mlflow_tracking_cfg.experiment_path)
        else:
            raise RuntimeError('MLflow experiment_id or experiment_path must be set in mlflow_params')

    
    def _get_feature_table_lookup(self):

        feature_store_table_cfg = self.cfg.feature_store_table_cfg

        global_feature_store = feature_store_table_cfg.database_name
        feature_table_customers = feature_store_table_cfg.table_name_customers
        feature_table_orders = feature_store_table_cfg.table_name_orders

        primary_keys_customers = feature_store_table_cfg.primary_keys_customers
        primary_keys_orders = feature_store_table_cfg.primary_keys_orders

        variables = self.cfg.pipeline_params['numeric_features'] + self.cfg.pipeline_params['categoric_features']

        # Define feature lookups
        feature_lookups = []

        _logger.info(f'Fetching features from {global_feature_store}.{feature_table_customers} and {global_feature_store}.{feature_table_orders}')

 
        # Iterate over each feature name and create a FeatureLookup instance
        for feature_name in self.cfg.pipeline_params['customers_variables']:
            feature_lookup = FeatureLookup(
                table_name='feature_store.olist_customer_features',
                lookup_key=['fs_reference_timestamp', 'customer_unique_id'],
                feature_names=feature_name
            )
           
            feature_lookups.append(feature_lookup)

        _logger.info(f'Features fetched from {global_feature_store}.{feature_table_customers}')


        # Iterate over each feature name and create a FeatureLookup instance
        for feature_name in self.cfg.pipeline_params['orders_variables']:
            feature_lookup = FeatureLookup(
                table_name='feature_store.olist_orders_features',
                lookup_key=['order_id'],
                feature_names=feature_name
            )
           
            feature_lookups.append(feature_lookup)

        _logger.info(f'Features fetched from {global_feature_store}.{feature_table_orders}')




        return feature_lookups
    

    def get_fs_training_set(self):

        labels_table_cfg = self.cfg.labels_table_cfg
        training_params_cfg = self.cfg.training_params
        

        _logger.info(f'Loading labels from {labels_table_cfg.database_name}.{labels_table_cfg.table_name}')

        labels_df = spark.sql(f"""
            SELECT * FROM {labels_table_cfg.database_name}.{labels_table_cfg.table_name}
            WHERE order_purchase_timestamp >= '{training_params_cfg['start_training_date']}'
            AND order_purchase_timestamp < '{training_params_cfg['end_training_date']}'
        """)


        feature_lookups = self._get_feature_table_lookup()

        _logger.info('Creating Feature Store training set...')


        fs_training_set = fs.create_training_set(
            df=labels_df,
            feature_lookups=feature_lookups,
            label=labels_table_cfg.target

)
        
        
        return fs_training_set
    

    def create_train_test_split(self, fs_training_set):
        
        labels_table_cfg = self.cfg.labels_table_cfg
        pipeline_params_cfg = self.cfg.pipeline_params

        variables = self.cfg.pipeline_params['numeric_features'] + self.cfg.pipeline_params['categoric_features']

        _logger.info('Load training set from Feature Store, converting to pandas DataFrame')

        df = fs_training_set.load_df()
    
        train = df.toPandas()
        
        X = train[variables]
        y = train[labels_table_cfg.target]

        _logger.info(f"Splitting in train and test - test_size: {pipeline_params_cfg['test_size']}")

        X_train, X_test, y_train, y_test = train_test_split(X, y,
                                                            random_state=pipeline_params_cfg['random_state'],
                                                            test_size=pipeline_params_cfg['test_size'],
                                                            stratify=y)
        

        return X_train, X_test, y_train, y_test
    

    def fit_pipeline(self, X_train, y_train):

        _logger.info('Creating sklearn pipeline...')
        pipeline = ModelTrainPipeline.create_train_pipeline(self.cfg.pipeline_params, self.cfg.model_params)

        _logger.info('Fitting sklearn DecisionTree...')
        _logger.info(f'Model params: {self.cfg.model_params}')
        model = pipeline.fit(X_train, y_train)

        return model

     

    
    def save_dataset_as_artifact(self, dataset, dataset_name):
        # Save as parquet
        dataset.to_parquet(f'{dataset_name}.parquet.gzip',
                         compression='gzip')  
            
        # Log as artifact
        mlflow.log_artifact(f'{dataset_name}.parquet.gzip')

        # Remove the tmp file
        os.remove(f'{dataset_name}.parquet.gzip')




    
    def run(self):
        
        mlflow_tracking_cfg = self.cfg.mlflow_tracking_cfg

        _logger.info('==========Setting MLflow experiment==========')
        self._set_experiment(mlflow_tracking_cfg)
        mlflow.sklearn.autolog(log_input_examples=True, silent=True)

        _logger.info('==========Starting MLflow run==========')

        with mlflow.start_run(run_name=mlflow_tracking_cfg.run_name) as mlflow_run:
            mlflow.log_dict(self.cfg.env_vars, 'env_vars.yml')

            _logger.info('==========Creating Feature Store training set==========')
            fs_training_set = self.get_fs_training_set()

            _logger.info('==========Creating train/test splits==========')
            X_train, X_test, y_train, y_test = self.create_train_test_split(fs_training_set)

            _logger.info('==========Fitting Model==========')
            model = self.fit_pipeline(X_train, y_train)


            _logger.info('Logging model to MLflow')

            # Wrapper to include probability as predict
            sklearn_model = ModelWrapper(model)


            
            fs.log_model(
                model = sklearn_model,
                artifact_path= 'fs_model',
                flavor=mlflow.pyfunc,
                training_set=fs_training_set,
                input_example=X_train[:100],
                signature = infer_signature(X_train, y_train))
            
            # Training metrics are logged by MLflow autologging
            # Log metrics for the test set
            _logger.info('==========Model Evaluation==========')
            _logger.info('Evaluating and logging metrics')

            skf = StratifiedKFold(shuffle=True, n_splits=5).split(X=X_train, y=y_train)

            metrics = ModelEvaluation.fold_validation(model, skf, X_train, y_train)
            mlflow.log_metrics(metrics)

            # Retrieve train and test
            train = X_train.copy()
            train['bad_review_flag'] = y_train
            test = X_test.copy()
            test['bad_review_flag'] = y_test

            _logger.info(f'Saving train and test as artifacts (parquet).')
            # Save as artifact 
            self.save_dataset_as_artifact(train, 'train')
            self.save_dataset_as_artifact(test, 'test')




            

            # Register model to MLflow Model Registry if provided
            if mlflow_tracking_cfg.model_name is not None:
                _logger.info('==========MLflow Model Registry==========')
                _logger.info(f'Registering model: {mlflow_tracking_cfg.model_name}')
                mlflow.register_model(f'runs:/{mlflow_run.info.run_id}/fs_model',
                                      name=mlflow_tracking_cfg.model_name)

        _logger.info('==========Model training completed==========')