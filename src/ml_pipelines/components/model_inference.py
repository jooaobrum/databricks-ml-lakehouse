from dataclasses import dataclass
from datetime import datetime

import mlflow
from databricks.sdk.runtime import *
from pyspark.sql.functions import lit

from .logger import get_logger

# Initialize logger
_logger = get_logger()


@dataclass
class MlflowTrackingCfg:
    """
    Configuration for MLflow tracking.
    """

    model_name: str
    model_stage: str


@dataclass
class InferenceTableCfg:
    """
    Configuration class to hold parameters for inference table.
    """

    inference_query: str
    output_db: str
    output_table_name: str


@dataclass
class ModelInferenceCfg:
    """
    Configuration class to hold parameters for model inference.
    """

    mlflow_tracking_cfg: MlflowTrackingCfg
    inference_table_cfg: InferenceTableCfg
    dt_start: str
    dt_stop: str


class ModelInference:
    def __init__(self, cfg: ModelInferenceCfg):
        """
        Initialize the model inference with configuration settings.

        Args:
            cfg (ModelInferenceCfg): Configuration for model inference.
            dt_start (str): Start date for data filtering.
            dt_stop (str): Stop date for data filtering.
        """
        self.cfg = cfg

    def read_query(self, path: str) -> str:
        """
        Reads a SQL query from a file.

        Args:
            path (str): Path to the query file.

        Returns:
            str: SQL query string.
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

    def load_model(self):
        """
        Loads the latest version of the registered MLflow model using the specified model name and stage.

        Returns:
            Tuple[mlflow.pyfunc.PyFuncModel, int, str]: Loaded MLflow model, model version, and run ID.
        """
        model_name = self.cfg.mlflow_tracking_cfg["model_name"]
        model_stage = self.cfg.mlflow_tracking_cfg["model_stage"]

        client = mlflow.MlflowClient()

        try:
            model_uri = f"models:/{model_name}/{model_stage}"
            _logger.info(f"Loading model from Model Registry: {model_uri}")
            latest_model = mlflow.pyfunc.load_model(model_uri=model_uri)

            # Retrieve model metadata
            client = mlflow.MlflowClient()
            model_version_info = client.get_latest_versions(name=model_name, stages=[model_stage])[0]

            # Extract metadata and store in a dictionary
            metadata = {
                "model_name": model_name,
                "model_version": model_version_info.version,
                "stage": model_stage,
                "creation_timestamp": model_version_info.creation_timestamp,
                "last_updated_timestamp": model_version_info.last_updated_timestamp,
                "description": model_version_info.description,
                "run_id": model_version_info.run_id,
            }

            return latest_model, metadata

        except IndexError:
            _logger.error(f"No model found for name '{model_name}' and stage '{model_stage}'.")
            raise ValueError(f"No model found for name '{model_name}' and stage '{model_stage}'.")
        except Exception as e:
            _logger.error(f"Error loading model: {e}")
            raise

    def load_input_df(self):
        """
        Loads input data for inference by executing a SQL query with date filters.

        Returns:
            DataFrame: Spark DataFrame for model inference.
        """
        inference_query_path = f"queries/{self.cfg.inference_table_cfg['inference_query']}"
        query = self.read_query(inference_query_path)
        input_df = spark.sql(query.format(dt_start=self.cfg.dt_start, dt_stop=self.cfg.dt_stop))
        return input_df

    def run_batch(self, df, model):
        """
        Runs inference in batch mode on the provided DataFrame, adding prediction probabilities and predicted labels.

        Args:
            df (DataFrame): Input DataFrame for inference.
            model (mlflow.pyfunc.PyFuncModel): Loaded MLflow model.

        Returns:
            DataFrame: Spark DataFrame with added 'prob' and 'prediction' columns.
        """
        input_df = df.toPandas()

        prob = model.predict(input_df)[:, 1]

        input_df["prob"] = prob
        input_df["prediction"] = (prob > self.cfg.inference_table_cfg["inference_threshold"]).astype(int)

        output_df = spark.createDataFrame(input_df)
        return output_df

    def run(self):
        """
        Executes batch model inference and writes predictions to a Delta table,
        with model metadata included in the output.

        Args:
            mode (str): Write mode for the output table, default is 'overwrite'.
        """
        _logger.info("==========Running batch model inference==========")
        model, metadata = self.load_model()
        df_input = self.load_input_df()
        output_df = self.run_batch(df_input, model)

        inference_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        metadata["inference_datetime"] = inference_datetime

        # Add metadata columns to the predictions DataFrame
        for key, value in metadata.items():
            output_df = output_df.withColumn(key, lit(value))

        table_name = f"{self.cfg.inference_table_cfg['output_db']}.{self.cfg.inference_table_cfg['output_table_name']}"

        if spark.catalog.tableExists(table_name):
            _logger.info("mode=append")
            output_df.write.format("delta").mode("append").saveAsTable(table_name)
        else:
            _logger.info("mode=overwrite")
            output_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

        _logger.info(f"Predictions written to {table_name}")
        _logger.info("==========Batch model inference completed==========")
