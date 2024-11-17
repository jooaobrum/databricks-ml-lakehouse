import sys
from databricks.sdk.runtime import *
from .logger import get_logger
import mlflow
import pandas as pd
from dataclasses import dataclass
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
import os
import re
import nannyml as nml


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
class DataDriftCfg:
    """
    Configuration for MLflow tracking.
    """
    reference_db: str
    table_name: str

@dataclass
class InferenceTableCfg:
    """
    Configuration for Feature Store table.
    """
    query_features: str

@dataclass
class DataDriftConfig:
    """
    Configuration class to hold parameters for model training.
    """
    mlflow_tracking_cfg: MlflowTrackingCfg
    inference_table_cfg: InferenceTableCfg
    data_drift_cfg: DataDriftCfg
    dt_start: str
    dt_stop: str



class DataDrift:
    def __init__(self, cfg: DataDriftConfig):
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
            with open(path, 'r') as f:
                query = f.read()
            _logger.info("Successfully read base query.")
            return query
        except Exception as e:
            _logger.error(f"Error reading query file: {e}")
            raise

    def load_input_df(self):
      """
      Loads input data for inference by executing a SQL query with date filters.

      Returns:
          DataFrame: Spark DataFrame for model inference.
      """
      data_drift_query_path = f"queries/{self.cfg.inference_table_cfg['inference_query']}"
      query = self.read_query(data_drift_query_path)
      input_df = spark.sql(query.format(dt_start=self.cfg.dt_start, dt_stop=self.cfg.dt_stop))
      return input_df.toPandas()
    
    def load_reference_df(self):
      # Retrieve model metadata
      client = mlflow.MlflowClient()
      model_version_info = client.get_latest_versions(name=self.cfg.mlflow_tracking_cfg['model_name'], stages=[self.cfg.mlflow_tracking_cfg['model_stage']])[0]

      local_path = client.download_artifacts(model_version_info.run_id, "test.parquet.gzip", '.')

      reference_df = pd.read_parquet(local_path)

      os.remove(local_path)

      return reference_df
    
    def calculate_multivariate_drift(self, input_df: pd.DataFrame, reference_df: pd.DataFrame):
        """
        Calculate multivariate drift using the nannyML library.

        Args:
            input_df (pd.DataFrame): Input data for drift analysis.
            reference_df (pd.DataFrame): Reference data for drift analysis.

        Returns:
            pd.DataFrame: Multivariate drift results.
        """
        # Identify feature columns
        non_feature_columns = ['dt_ref', 'target']
        feature_column_names = [col for col in reference_df.columns if col not in non_feature_columns]

        # Initialize the Data Reconstruction Drift Calculator
        calc = nml.DataReconstructionDriftCalculator(
            column_names=feature_column_names,
            timestamp_column_name='dt_ref',

            chunk_size=100
        )

        # Fit the calculator on the reference data
        calc.fit(reference_df)

        # Calculate drift on the input data
        results = calc.calculate(input_df)

        # Plot the results
        figure = results.plot()
        figure.show()

        return results.to_df()
      
    def clean_and_save_dataframe(self, df):
        """
        Cleans DataFrame column names by removing special characters and spaces,
        then saves the DataFrame to a Spark table. Creates a new table if it doesn't
        exist; otherwise, appends to the existing table.

        Args:
            df (pyspark.sql.DataFrame): The DataFrame to be cleaned and saved.
            table_name (str): The name of the table to save the DataFrame to.
            database_name (str): The name of the database where the table resides.
                                Defaults to 'default'.
        """
        database_name = self.cfg.data_drift_cfg['reference_db']
        table_name = self.cfg.data_drift_cfg['table_name']

        # Function to clean column names
        def clean_column_name(col_name):
            # Remove special characters and replace spaces with underscores
            cleaned_name = re.sub(r'[^0-9a-zA-Z_]', '', col_name.replace(' ', '_'))
            return cleaned_name

        # Clean column names
        cleaned_columns = [clean_column_name(str(col)) for col in df.columns]
        df_cleaned = df.copy()
        df_cleaned.columns = cleaned_columns
        df_cleaned_spark = spark.createDataFrame(df_cleaned)

        # Full table name including database
        full_table_name = f"{database_name}.{table_name}"

        # Check if the table exists
        if spark.catalog.tableExists(database_name, table_name):
            # Append to existing table
            df_cleaned_spark.write.mode('append').saveAsTable(full_table_name)
            print(f"Data appended to existing table: {full_table_name}")
        else:
            # Create new table
            df_cleaned_spark.write.mode('overwrite').saveAsTable(full_table_name)
            print(f"New table created: {full_table_name}")

      
    def run(self):

    
        # Load reference and input data
        reference_df = self.load_reference_df()
        input_df = self.load_input_df()

        # Calculate multivariate drift
        drift_df = self.calculate_multivariate_drift(input_df, reference_df)

        # Convert drift metrics to Pandas DataFrame if necessary
        if not isinstance(drift_df, pd.DataFrame):
            drift_df = drift_df.to_pandas()

        # Clean and save the drift metrics DataFrame
        self.clean_and_save_dataframe(drift_df)

        