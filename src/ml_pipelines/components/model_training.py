import os
from dataclasses import dataclass
from datetime import timedelta

# MLflow and Databricks SDK imports
import mlflow
import pandas as pd
from databricks.sdk.runtime import *
from mlflow.models import infer_signature
from mlflow.pyfunc import PythonModel
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression

# Scikit-learn and imbalanced-learn imports
from sklearn.model_selection import StratifiedKFold, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

# Custom imports for evaluation and logging
from .logger import get_logger
from .model_evaluation import ModelEvaluation

# Initialize the logger
_logger = get_logger()


@dataclass
class MlflowTrackingCfg:
    """
    Configuration for MLflow tracking.
    """

    run_name: str
    model_train_experiment_path: str
    model_name: str


@dataclass
class FeatureStoreTableCfg:
    """
    Configuration for Feature Store table.
    """

    query_features: str


@dataclass
class LabelTableCfg:
    """
    Configuration for Label Table.
    """

    query_target: str
    target: str


@dataclass
class ModelTrainConfig:
    """
    Configuration class to hold parameters for model training.
    """

    mlflow_tracking_cfg: MlflowTrackingCfg
    feature_store_table_cfg: FeatureStoreTableCfg
    labels_table_cfg: LabelTableCfg
    pipeline_params: dict
    model_params: dict
    training_params: dict


class ModelTrainPipeline:
    @classmethod
    def create_preprocessor(cls, pipeline_params):
        """
        Creates a preprocessing pipeline for numerical and categorical features.

        Args:
            pipeline_params (dict): Dictionary containing the feature types.

        Returns:
            ColumnTransformer: A preprocessing pipeline.
        """
        numerical_features = pipeline_params["numerical_features"]
        categorical_features = pipeline_params["categorical_features"]

        # Numerical transformations
        numeric_transformer = Pipeline(
            steps=[("imputer", SimpleImputer(strategy="constant", fill_value=0)), ("scaler", StandardScaler())]
        )

        # Categorical transformations
        categorical_transformer = Pipeline(steps=[("encoder", OneHotEncoder(handle_unknown="ignore"))])

        # Preprocessing pipeline
        preprocessor = ColumnTransformer(
            transformers=[
                ("num", numeric_transformer, numerical_features),
                ("cat", categorical_transformer, categorical_features),
            ],
            remainder="passthrough",
        )

        return preprocessor

    @classmethod
    def create_train_pipeline(cls, pipeline_params, model_params):
        """
        Creates the training pipeline.

        Args:
            pipeline_params (dict): Parameters for the preprocessing pipeline.
            model_params (dict): Parameters for the model.

        Returns:
            Pipeline: A pipeline with preprocessing and classification steps.
        """
        preprocessor = cls.create_preprocessor(pipeline_params)
        model = LogisticRegression(**model_params)

        # Pipeline
        pipeline = Pipeline(steps=[("preprocessor", preprocessor), ("classifier", model)])

        return pipeline


class ModelWrapper(PythonModel):
    def __init__(self, trained_model):
        self.model = trained_model

    def predict(self, context, model_input):
        return self.model.predict_proba(model_input)


class ModelTrain:
    def __init__(self, cfg: ModelTrainConfig):
        """
        Initialize the model training with configuration settings.

        Args:
            cfg (ModelTrainConfig): Configuration for model training.
        """
        self.cfg = cfg

    def read_query(self, path) -> str:
        """
        Reads the transformation query from a file.

        Args:
            path (str): Path to the query file.

        Returns:
            str: Query string.
        """
        _logger.info(f"Reading base query from {path}")
        try:
            with open(path, "r") as f:
                query = f.read()
            _logger.info("Successfully read base query.")
            return query
        except Exception as e:
            _logger.error(f"Error reading query file: {e}")
            raise

    @staticmethod
    def _set_experiment(mlflow_tracking_cfg):
        """
        Sets the MLflow experiment path.

        Args:
            mlflow_tracking_cfg (MlflowTrackingCfg): MLflow configuration.
        """
        if mlflow_tracking_cfg["model_train_experiment_path"]:
            _logger.info(f"MLflow experiment_path: {mlflow_tracking_cfg['model_train_experiment_path']}")
            mlflow.set_experiment(mlflow_tracking_cfg["model_train_experiment_path"])
        else:
            raise RuntimeError("MLflow experiment_path must be set in mlflow_params")

    def get_fs_training_set(self):
        """
        Creates a training set by joining features and labels from queries.

        Returns:
            DataFrame: Training set DataFrame.
        """
        target_query_path = f"queries/{self.cfg.labels_table_cfg['query_target']}"
        features_query_path = f"queries/{self.cfg.feature_store_table_cfg['query_features']}"
        dt_start = self.cfg.training_params["dt_start"]
        dt_stop = self.cfg.training_params["dt_stop"]

        target_query = self.read_query(target_query_path)
        labels_df = spark.sql(target_query.format(dt_start=dt_start, dt_stop=dt_stop))
        features_query = self.read_query(features_query_path)
        features_df = spark.sql(features_query.format(dt_start=dt_start, dt_stop=dt_stop))

        fs_training_set = features_df.join(labels_df, "order_id", "inner")
        return fs_training_set

    def create_train_test_split(self, fs_training_set):
        """
        Splits the training set into training and test sets.

        Args:
            fs_training_set (DataFrame): The full feature store training set.

        Returns:
            tuple: Splits for training and testing.
        """
        variables = (
            ["dt_ref"]
            + self.cfg.pipeline_params["numerical_features"]
            + self.cfg.pipeline_params["categorical_features"]
        )
        target = self.cfg.labels_table_cfg["target"]
        dt_start = self.cfg.training_params["dt_start"]
        dt_stop = self.cfg.training_params["dt_stop"]

        train = fs_training_set.toPandas()
        train["dt_ref"] = pd.to_datetime(train["dt_ref"])

        oot = train[
            (train["dt_ref"] >= pd.to_datetime(dt_stop) - timedelta(days=30))
            & (train["dt_ref"] < pd.to_datetime(dt_stop))
        ]
        train = train[
            (train["dt_ref"] >= pd.to_datetime(dt_start))
            & (train["dt_ref"] < (pd.to_datetime(dt_stop) - timedelta(days=30)))
        ]

        X = train[variables]
        y = train[target]

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=self.cfg.pipeline_params["test_size"],
            random_state=self.cfg.pipeline_params["random_state"],
            stratify=y,
        )

        return X_train, X_test, y_train, y_test, oot

    def fit_pipeline(self, X_train, y_train):
        """
        Fits the model pipeline.

        Args:
            X_train (DataFrame): Training features.
            y_train (Series): Training labels.

        Returns:
            Pipeline: Fitted model pipeline.
        """
        pipeline = ModelTrainPipeline.create_train_pipeline(self.cfg.pipeline_params, self.cfg.model_params)
        return pipeline.fit(X_train, y_train)

    def save_dataset_as_artifact(self, dataset, dataset_name):
        """
        Saves dataset as a parquet artifact.

        Args:
            dataset (DataFrame): The dataset to save.
            dataset_name (str): Name of the dataset.
        """
        dataset.to_parquet(f"{dataset_name}.parquet.gzip", compression="gzip")
        mlflow.log_artifact(f"{dataset_name}.parquet.gzip")
        os.remove(f"{dataset_name}.parquet.gzip")

    def run(self):
        """
        Runs the full model training and logging process.
        """

        features = self.cfg.pipeline_params["numerical_features"] + self.cfg.pipeline_params["categorical_features"]
        target = self.cfg.labels_table_cfg["target"]

        self._set_experiment(self.cfg.mlflow_tracking_cfg)
        mlflow.sklearn.autolog(log_input_examples=True, silent=True)

        with mlflow.start_run(run_name=self.cfg.mlflow_tracking_cfg["run_name"]) as mlflow_run:
            fs_training_set = self.get_fs_training_set()
            X_train, X_test, y_train, y_test, oot = self.create_train_test_split(fs_training_set)
            model = self.fit_pipeline(X_train[features], y_train)

            sklearn_model = ModelWrapper(model)
            mlflow.pyfunc.log_model(
                artifact_path="fs_model",
                python_model=sklearn_model,
                input_example=X_train[:100],
                signature=infer_signature(X_train[features], y_train),
            )

            # Model evaluation and logging
            skf = StratifiedKFold(n_splits=5, shuffle=True)
            metrics = ModelEvaluation.fold_validation(model, skf, X_train[features], y_train)
            mlflow.log_metrics(metrics)

            oot_metrics = ModelEvaluation.evaluate_model(model, oot[features], oot[target], suffix="oot")
            mlflow.log_metrics(oot_metrics)

            self.save_dataset_as_artifact(pd.DataFrame(X_train).assign(target=y_train), "train")
            self.save_dataset_as_artifact(pd.DataFrame(X_test).assign(target=y_test), "test")

            if self.cfg.mlflow_tracking_cfg["model_name"]:
                mlflow.register_model(
                    f"runs:/{mlflow_run.info.run_id}/fs_model", name=self.cfg.mlflow_tracking_cfg["model_name"]
                )

        _logger.info("Model training completed.")
