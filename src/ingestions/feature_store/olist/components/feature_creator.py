from dataclasses import dataclass
from .logger import get_logger
import datetime
from typing import List
from tqdm import tqdm
from databricks.sdk.runtime import *
import yaml

# Initialize logger
_logger = get_logger()

@dataclass
class FeatureCreatorConfig:
    """
    Configuration class for FeatureLabelsCreator containing environment 
    and job-specific parameters.
    """
    ref_name: str       # Reference name for feature set
    db_name: str        # Database name in Databricks
    table_name: str     # Table name with placeholders for task key
    fs_path: str        # Feature store path with placeholders for task key
    base_query_path: str  # Path to the base query file
    
    task_key: str       # Task-specific identifier
    dt_start: str       # Start date for processing in 'YYYY-MM-DD' format
    dt_stop: str        # End date for processing in 'YYYY-MM-DD' format
    step: int           # Step size in days for date generation


class FeatureCreator:
    def __init__(self, config: FeatureCreatorConfig):
        """
        Initializes FeatureLabelsCreator with the provided configuration.
        
        :param config: FeatureLabelsCreatorConfig object containing configuration parameters
        """
        self.config = config

    def read_transf_query(self) -> str:
        """
        Reads the transformation query from a file.

        :return: Query string for transformation
        """
        _logger.info(f"Reading base query from {self.config.base_query_path}")
        try:
            with open(self.config.base_query_path, 'r') as f:
                query = f.read()
            _logger.info("Successfully read base query.")
            return query
        except Exception as e:
            _logger.error(f"Error reading query file: {e}")
            raise

    def generate_dates(self) -> List[str]:
        """
        Generates a list of dates from the start to the stop date with a specified step.

        :return: List of dates as strings in 'YYYY-MM-DD' format
        """
        _logger.info("Generating dates for the specified range.")
        try:
            date_start = datetime.datetime.strptime(self.config.dt_start, '%Y-%m-%d')
            date_stop = datetime.datetime.strptime(self.config.dt_stop, '%Y-%m-%d')
            step = int(self.config.step)

            n_days = (date_stop - date_start).days + 1
            dates = [
                (date_start + datetime.timedelta(days=i)).strftime('%Y-%m-%d')
                for i in range(0, n_days, step)
            ]
            _logger.info(f"Generated {len(dates)} dates.")
            return dates
        except Exception as e:
            _logger.error(f"Error generating dates: {e}")
            raise

    def create_feature_table(self):
        """
        Creates or appends to a feature table in the feature store by executing 
        a transformation query for each generated date.
        """
        db_name = self.config.db_name
        table_name = self.config.table_name.replace("{task_key}", self.config.task_key)

        dates = self.generate_dates()
        base_query = self.read_transf_query()

        for date in tqdm(dates, desc="Processing dates"):
            _logger.info(f"Processing date {date}")
            query = base_query.format(dt_ingestion=date)
            
            try:
                # Execute the query to retrieve data
                df_feature_store_base = spark.sql(query)
                
                # Write to table, appending or overwriting as needed
                full_table_name = f"{db_name}.{table_name}"
                if spark.catalog.tableExists(full_table_name):
                    df_feature_store_base.write.format("delta").mode("append").saveAsTable(full_table_name)
                    _logger.info(f"Appended data to existing table {full_table_name}.")
                else:
                    df_feature_store_base.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(full_table_name)
                    _logger.info(f"Created and wrote data to new table {full_table_name}.")
            except Exception as e:
                _logger.error(f"Error processing date {date}: {e}")
                raise

    def run(self):
        """
        Executes the feature creation process.
        """
        _logger.info("========== Feature Store Ingestion Start ==========")
        try:
            self.create_feature_table()
            _logger.info("========== Feature Store Ingestion Completed Successfully ==========")
        except Exception as e:
            _logger.error(f"Feature store ingestion failed: {e}")
            raise