# Databricks notebook source
# MAGIC %md
# MAGIC # Model Inference

# COMMAND ----------

dbutils.widgets.dropdown('env','dev',['dev','staging', 'prod'], 'Environment')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient
import sys 

sys.path.insert(0, '../libs')
from logger_utils import get_logger
from loading_utils import load_and_set_env_vars, load_config

# COMMAND ----------

_logger = get_logger()
fs = FeatureStoreClient()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Pipeline & Cfg Params

# COMMAND ----------

# Set pipeline name
pipeline_name = 'model_inference'

# Load pipeline config
pipeline_config = load_config(pipeline_name)

# Load env vars
env_vars = load_and_set_env_vars(env = dbutils.widgets.get('env'))



# COMMAND ----------

model_name = env_vars['model_name']
model_registry_stage = pipeline_config['mlflow_params']['model_registry_stage']
model_uri = f'models:/{model_name}/{model_registry_stage}'
print(f'model_uri: {model_uri}')

# COMMAND ----------

# Reference database
reference_database = env_vars['reference_table_database_name']

# COMMAND ----------

# Input table 
input_table_name = pipeline_config['data_input']['table_name']

input_table_name = f'{reference_database}.{input_table_name}'
print(f'input_table_name: {input_table_name}')


# COMMAND ----------

# Output table
predictions_table_name = f'{reference_database}.{env_vars["predictions_table_name"]}'
print(f'predictions_table_name: {predictions_table_name}')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Inference Class

# COMMAND ----------

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

# COMMAND ----------

model_inference_pipeline = ModelInference(model_uri=model_uri,
                                          input_table_name=input_table_name,
                                          output_table_name=predictions_table_name)

model_inference_pipeline.run_batch_and_write(mode=pipeline_config['data_output']['mode'])
