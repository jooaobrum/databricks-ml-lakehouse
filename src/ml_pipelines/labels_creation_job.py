# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Table/Label Creator 

# COMMAND ----------

dbutils.widgets.dropdown('env','dev',['dev','staging', 'prod'], 'Environment')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------


from components.feature_label_creator import FeatureLabelsCreatorConfig, FeatureLabelsCreator
from components.utils.loading_utils import load_and_set_env_vars, load_config




# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Pipeline & Config Params
# MAGIC

# COMMAND ----------

# Set pipeline name
pipeline_name = 'labels_creation'

# Load pipeline config
pipeline_config = load_config(pipeline_name)

# Load env vars
env_vars = load_and_set_env_vars(env = dbutils.widgets.get('env'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute Pipeline
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 1. Check if databases and tables exists and drop them
# MAGIC 2. Data Ingestion with Labels 
# MAGIC 3. Fetch Features from Feature Store
# MAGIC 4. Write in a table the features
# MAGIC 5. Write in a table the labels

# COMMAND ----------

# Define the config parameters
cfg = FeatureLabelsCreatorConfig(
                                 max_datetime=pipeline_config['max_datetime'],
                                 target=pipeline_config['target'],
                                 reference_database=env_vars['reference_table_database_name'],
                                 labels_table_name=env_vars['labels_table_name'])

# Creathe the feature table object
feature_table_creator_pipeline = FeatureLabelsCreator(cfg)

# Run the pipeline
feature_table_creator_pipeline.run()

# COMMAND ----------


