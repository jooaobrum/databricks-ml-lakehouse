# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Table/Label Creator 

# COMMAND ----------

dbutils.widgets.dropdown('env','dev',['dev','staging', 'prod'], 'Environment')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

import sys
from dataclasses import dataclass
from typing import Dict, Any, Union, List



sys.path.insert(0, '../libs')
from logger_utils import get_logger
from loading_utils import load_and_set_env_vars, load_config





# COMMAND ----------

_logger = get_logger()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Pipeline & Config Params
# MAGIC

# COMMAND ----------

# Set pipeline name
pipeline_name = 'features_labels_creation'

# Load pipeline config
pipeline_config = load_config(pipeline_name)

# Load env vars
env_vars = load_and_set_env_vars(env = dbutils.widgets.get('env'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature Classes
# MAGIC

# COMMAND ----------

@dataclass
class FeatureLabelsCreatorConfig:

    input_db: str
    input_tables: Union[str, List[str]]
    
    global_feature_store: str
    local_feature_store: str
    primary_keys: Union[str, List[str]]

    variables: Union[str, List[str]]
    target: str
    


class FeatureLabelsCreator:

    def __init__(self, config: FeatureLabelsCreatorConfig):
        self.config = config


    @staticmethod
    def setup(db_name, table_name):



    def run_data_ingestion(self):
    

    def create feature table

    def create label table



# COMMAND ----------

cfg = FeatureLabelsCreatorConfig(input_db=pipeline_config['input_db'],
                                 input_tables=pipeline_config['input_tables'],
                                 variables=pipeline_config['variables'],
                                 primary_keys=pipeline_config['fs_primary_keys'],
                                 target=pipeline_config['target'],
                                 global_feature_store=env_vars['global_feature_store_database_name'],
                                 local_feature_store=env_vars['local_feature_store_database_name'],)
