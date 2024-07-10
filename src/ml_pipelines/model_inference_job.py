# Databricks notebook source
# MAGIC %md
# MAGIC # Model Inference

# COMMAND ----------

dbutils.widgets.dropdown('env','dev',['dev','staging', 'prod'], 'Environment')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from components.model_inference import ModelInference
from components.utils.loading_utils import load_and_set_env_vars, load_config


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

model_inference_pipeline = ModelInference(model_uri=model_uri,
                                          input_table_name=input_table_name,
                                          output_table_name=predictions_table_name)

model_inference_pipeline.run_batch_and_write(mode=pipeline_config['data_output']['mode'])
