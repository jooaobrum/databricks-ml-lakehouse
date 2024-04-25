# Databricks notebook source
# MAGIC %md
# MAGIC # Model Train

# COMMAND ----------

dbutils.widgets.dropdown('env','dev',['dev','staging', 'prod'], 'Environment')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

import pandas as pd
import sklearn
import mlflow
from mlflow.models import infer_signature
import databricks
from databricks.feature_store import FeatureStoreClient, FeatureLookup
from mlflow.pyfunc import PythonModel
from dataclasses import dataclass
from sklearn.model_selection import StratifiedKFold, train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import (accuracy_score, precision_score, recall_score, 
                             f1_score, roc_auc_score, average_precision_score)

import sys
import pprint

sys.path.insert(0, '../libs')
from logger_utils import get_logger
from loading_utils import load_and_set_env_vars, load_config

# COMMAND ----------

_logger = get_logger()
fs = FeatureStoreClient()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Pipeline & Config Params

# COMMAND ----------

# Set pipeline name
pipeline_name = 'model_train'

# Load pipeline config
pipeline_config = load_config(pipeline_name)

# Load env vars
env_vars = load_and_set_env_vars(env = dbutils.widgets.get('env'))

# Set pipeline params
pipeline_params = pipeline_config['pipeline_params']

# Set model params
model_params = pipeline_config['model_params']

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cfg Class

# COMMAND ----------

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
        



# COMMAND ----------

# Set Mlflow configuration
mlflow_tracking_cfg = MlflowTrackingCfg(run_name=pipeline_config['mlflow_params']['run_name'],
                                        experiment_path=env_vars['model_train_experiment_path'],
                                        model_name=env_vars['model_name'])


# Set Features configuration
feature_store_table_cfg = FeatureStoreTableCfg(database_name=env_vars['global_feature_store_database_name'],
                                               table_name_customers=env_vars['feature_store_customers_table_name'],
                                               table_name_orders = env_vars['feature_store_orders_table_name']
,                                              primary_keys_customers = [env_vars['primary_key_customers_1'], env_vars               ['primary_key_customers_2']],
                                               primary_keys_orders = [env_vars['primary_key_orders_1']])



# Set Labels configuration
labels_table_cfg = LabelTableCfg(database_name=env_vars['reference_table_database_name'],
                                               table_name=env_vars['labels_table_name'],
                                               target = env_vars['labels_table_col'])


# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Train Pipeline Class

# COMMAND ----------

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
    


# COMMAND ----------

# MAGIC %md
# MAGIC ## ModelWrapper Class

# COMMAND ----------

class ModelWrapper(PythonModel):
    def __init__(self, trained_model): 
        self.model = trained_model

    def predict(self, context, model_input):
        return self.model.predict_proba(model_input)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Train Class

# COMMAND ----------

@dataclass
class ModelTrainConfig:
    mlflow_tracking_cfg: MlflowTrackingCfg
    feature_store_table_cfg: FeatureStoreTableCfg
    labels_table_cfg: LabelTableCfg
    pipeline_params: dict
    model_params: dict
    conf: dict
    env_vars: dict

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
        pipeline_params_cfg = self.cfg.pipeline_params

        global_feature_store = feature_store_table_cfg.database_name
        feature_table_customers = feature_store_table_cfg.table_name_customers
        feature_table_orders = feature_store_table_cfg.table_name_orders

        primary_keys_customers = feature_store_table_cfg.primary_keys_customers
        primary_keys_orders = feature_store_table_cfg.primary_keys_orders

        variables = cfg.pipeline_params['numeric_features'] + cfg.pipeline_params['categoric_features']

        # Define feature lookups
        feature_lookups = []

        _logger.info(f'Fetching features from {global_feature_store}.{feature_table_customers} and {global_feature_store}.{feature_table_orders}')

 
        # Iterate over each feature name and create a FeatureLookup instance
        for feature_name in cfg.pipeline_params['customers_variables']:
            feature_lookup = FeatureLookup(
                table_name='feature_store.olist_customer_features',
                lookup_key=['fs_reference_timestamp', 'customer_unique_id'],
                feature_names=feature_name
            )
           
            feature_lookups.append(feature_lookup)

        _logger.info(f'Features fetched from {global_feature_store}.{feature_table_customers}')


        # Iterate over each feature name and create a FeatureLookup instance
        for feature_name in cfg.pipeline_params['orders_variables']:
            feature_lookup = FeatureLookup(
                table_name='feature_store.olist_orders_features',
                lookup_key=['order_id'],
                feature_names=feature_name
            )
           
            feature_lookups.append(feature_lookup)

        _logger.info(f'Features fetched from {global_feature_store}.{feature_table_orders}')




        return feature_lookups
    

    def get_fs_training_set(self):

        feature_store_table_cfg = self.cfg.feature_store_table_cfg
        labels_table_cfg = self.cfg.labels_table_cfg
        

        _logger.info(f'Loading labels from {labels_table_cfg.database_name}.{labels_table_cfg.table_name}')

        labels_df = spark.table(f"{labels_table_cfg.database_name}.{labels_table_cfg.table_name}")

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
        env_vars_cfg = self.cfg.env_vars

        variables = cfg.pipeline_params['numeric_features'] + cfg.pipeline_params['categoric_features']

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
        _logger.info(f'Model params: {pprint.pformat(self.cfg.model_params)}')
        model = pipeline.fit(X_train, y_train)

        return model

     
    def evaluate_model(self, pipeline, X_test, y_test):

        # Make predictions on the test set
        y_pred = pipeline.predict(X_test)
        y_prob = pipeline.predict_proba(X_test)[:, 1]

        # Calculate evaluation metrics
        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred)
        recall = recall_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)
        roc_auc = roc_auc_score(y_test, y_prob)
        pr_auc = average_precision_score(y_test, y_prob)

        evaluation_metrics = {
            "Test Accuracy": accuracy,
            "Test Precision": precision,
            "Test Recall": recall,
            "Test F1 Score": f1,
            "Test ROC-AUC": roc_auc,
            "Test PR-AUC": pr_auc
        }

        return evaluation_metrics

    
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
            metrics = self.evaluate_model(model, X_test, y_test)
            mlflow.log_metrics(metrics)
            

            # Register model to MLflow Model Registry if provided
            if mlflow_tracking_cfg.model_name is not None:
                _logger.info('==========MLflow Model Registry==========')
                _logger.info(f'Registering model: {mlflow_tracking_cfg.model_name}')
                mlflow.register_model(f'runs:/{mlflow_run.info.run_id}/fs_model',
                                      name=mlflow_tracking_cfg.model_name)

        _logger.info('==========Model training completed==========')








# COMMAND ----------

cfg = ModelTrainConfig(mlflow_tracking_cfg=mlflow_tracking_cfg,
                       feature_store_table_cfg=feature_store_table_cfg,
                       labels_table_cfg=labels_table_cfg,
                       pipeline_params=pipeline_params,
                       model_params=model_params,
                       conf=pipeline_config,    
                       env_vars=env_vars       
                      )


model_train_pipeline = ModelTrain(cfg)
model_train_pipeline.run()

# COMMAND ----------


