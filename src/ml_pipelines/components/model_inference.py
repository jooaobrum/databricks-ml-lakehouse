import sys
from databricks.feature_store import FeatureStoreClient, FeatureLookup
from databricks.sdk.runtime import *
from .utils.logger_utils import get_logger




_logger = get_logger()
fs = FeatureStoreClient()


class ModelInference():
    def __init__(self, model_uri: str, input_table_name: str, output_table_name: str = None):
        self.model_uri = model_uri
        self.input_table_name = input_table_name
        self.output_table_name = output_table_name


    def _load_input_table(self):
        input_table_name = self.input_table_name
        _logger.info(f"Loading lookup keys from input table: {input_table_name}")
        return spark.table(input_table_name)
    
    def fs_run_batch(self, df):
        fs = FeatureStoreClient()
        _logger.info(f"Loading model from Model Registry: {self.model_uri}")

        return fs.score_batch(self.model_uri, df)
    
    def run_batch(self):

        input_df = self._load_input_table()
        scores_df = self.fs_run_batch(input_df)

        return scores_df
    
    def run_batch_and_write(self, mode: str = 'overwrite'):
        _logger.info("==========Running batch model inference==========")
        scores_df = self.run_batch()

        _logger.info("==========Writing predictions==========")
        _logger.info(f"mode={mode}")
        _logger.info(f"Predictions written to {self.output_table_name}")
        # Model predictions are written to the Delta table provided as input.
        # Delta is the default format in Databricks Runtime 8.0 and above.
        scores_df.write.format("delta").mode(mode).saveAsTable(self.output_table_name)

        _logger.info("==========Batch model inference completed==========")