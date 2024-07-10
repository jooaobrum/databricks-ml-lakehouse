# Databricks notebook source
# MAGIC %md
# MAGIC # Model Train

# COMMAND ----------

dbutils.widgets.dropdown('env','dev',['dev','staging', 'prod'], 'Environment')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from components.model_training import (MlflowTrackingCfg, FeatureStoreTableCfg, 
                                       LabelTableCfg, FeatureStoreTableCfg, 
                                       ModelTrainConfig, ModelTrainPipeline,
                                       ModelWrapper,ModelTrain
                                       )

import sys
import pprint
from components.utils.loading_utils import load_and_set_env_vars, load_config

# COMMAND ----------



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

# Set training params 
training_params = pipeline_config['training_params']

# COMMAND ----------

# Set Mlflow configuration
mlflow_tracking_cfg = MlflowTrackingCfg(
    run_name=pipeline_config['mlflow_params']['run_name'],
    experiment_path=env_vars['model_train_experiment_path'],
    model_name=env_vars['model_name']
)

# Set Features configuration
feature_store_table_cfg = FeatureStoreTableCfg(
    database_name=env_vars['global_feature_store_database_name'],
    table_name_customers=env_vars['feature_store_customers_table_name'],
    table_name_orders=env_vars['feature_store_orders_table_name'],
    primary_keys_customers=[
        env_vars['primary_key_customers_1'], 
        env_vars['primary_key_customers_2']
    ],
    primary_keys_orders=[env_vars['primary_key_orders_1']]
)

# Set Labels configuration
labels_table_cfg = LabelTableCfg(
    database_name=env_vars['reference_table_database_name'],
    table_name=env_vars['labels_table_name'],
    target=env_vars['labels_table_col']
)

# COMMAND ----------

cfg = ModelTrainConfig(mlflow_tracking_cfg=mlflow_tracking_cfg,
                       feature_store_table_cfg=feature_store_table_cfg,
                       labels_table_cfg=labels_table_cfg,
                       pipeline_params=pipeline_params,
                       model_params=model_params,
                       training_params=training_params,
                       conf=pipeline_config,    
                       env_vars=env_vars       
                      )


model_train_pipeline = ModelTrain(cfg)
model_train_pipeline.run()
