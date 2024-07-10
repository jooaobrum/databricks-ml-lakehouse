# Databricks notebook source
# MAGIC %md
# MAGIC # Setup for Deployment

# COMMAND ----------

dbutils.widgets.dropdown('env','dev',['dev','staging', 'prod'], 'Environment')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------


from components.setup import Setup
from components.utils.loading_utils import load_and_set_env_vars, load_config


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Pipeline & Config Params

# COMMAND ----------

# Set pipeline name
pipeline_name = 'setup'


# Load pipeline config 
pipeline_config = load_config(pipeline_name)

# Load env vars
env_vars = load_and_set_env_vars(env=dbutils.widgets.get('env'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Setup Pipeline Class 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC Run flow:
# MAGIC 1. Check if model registry exists
# MAGIC 2. Delete the model registry (archive models)
# MAGIC 3. Check if model experiments exists in train/deploy
# MAGIC 4. Delete the model experiments
# MAGIC 5. Check if a table containing the labels exists
# MAGIC 6. Delete the label table

# COMMAND ----------

setup_pipeline = Setup(config = pipeline_config, env_vars=env_vars)
setup_pipeline.run()

# COMMAND ----------


