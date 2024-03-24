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
from databricks.feature_store import FeatureLookup, FeatureStoreClient 



sys.path.insert(0, '../libs')
from logger_utils import get_logger
from loading_utils import load_and_set_env_vars, load_config





# COMMAND ----------

_logger = get_logger()
fs = FeatureStoreClient()



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
    feature_store_table_name: str
    features_store_table_description: str
    primary_keys: Union[str, List[str]]

    max_datetime: str
    variables: Union[str, List[str]]
    target: str

    labels_table_database_name: str
    labels_table_name: str
    


class FeatureLabelsCreator:

    def __init__(self, config: FeatureLabelsCreatorConfig):
        self.config = config


    @staticmethod
    def setup_db(db_name, table_name):
        _logger.info(f"Creating database {db_name} if not exists")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name};")
        spark.sql(f'USE {db_name};')
        spark.sql(f"DROP TABLE IF EXISTS {table_name};")

        if 'feature_store' in db_name:
            try: 
                fs.drop_table(
                name=f'{db_name}.{table_name}'
                )
            except:
                pass


    def run_data_ingestion(self):


        max_datetime = self.config.max_datetime

        query_to_train = """
            WITH orders_reviews_tb AS (
      SELECT DISTINCT
              t2.customer_unique_id,
              date_format(date_add(t1.order_purchase_timestamp, 1), 'yyyy-MM-dd') as fs_reference_timestamp,
              t3.review_score
      FROM silver.olist_orders AS t1
      LEFT JOIN silver.olist_customers AS t2
      ON t1.customer_id = t2.customer_id
      LEFT JOIN silver.olist_order_reviews as t3
      ON t1.order_id = t3.order_id
      WHERE t2.customer_unique_id IS NOT NULL

      ),

      dataset as (

      SELECT 
          t1.fs_reference_timestamp,
          t1.customer_unique_id,
          CASE WHEN t2.review_score < 3 THEN 1 ELSE 0 END as bad_review_flag

      FROM feature_store.olist_customer_features as t1
      INNER JOIN orders_reviews_tb as t2

      ON t1.fs_reference_timestamp = t2.fs_reference_timestamp
      AND t1.customer_unique_id = t2.customer_unique_id

  

      )

      SELECT fs_reference_timestamp,
             customer_unique_id,
             LAST(bad_review_flag) as bad_review_flag
      FROM dataset

      WHERE fs_reference_timestamp <= '{max_datetime}'

      GROUP BY fs_reference_timestamp, customer_unique_id

        """

        query_to_train = query_to_train.format(max_datetime = max_datetime)

        return spark.sql(query_to_train)


    def fetch_features_target(self, input):

        target = self.config.target
        variables = self.config.variables
        primary_keys = self.config.primary_keys
        global_feature_store = self.config.global_feature_store
        table = self.config.feature_store_table_name

        # Define feature lookups
        feature_lookups = []

        _logger.info(f'Fetching features from {global_feature_store}.{table}')

        # Iterate over each feature name and create a FeatureLookup instance
        for feature_name in variables:
            feature_lookup = FeatureLookup(
                table_name=f'{global_feature_store}.{table}',
                lookup_key=primary_keys,
                feature_names=feature_name
            )
            feature_lookups.append(feature_lookup)

        # Create training set with feature lookups and target label
        training_set = fs.create_training_set(
            df=input,
            feature_lookups=feature_lookups,
            label=target
        )


        # Load the DataFrame
        df = training_set.load_df()
        
        _logger.info(f'Features fetched from {global_feature_store}.{table}')


        return df.drop_duplicates()

    

    def create_feature_table(self, df):
        # Store configuration variables locally
        feature_store_db = self.config.local_feature_store
        feature_store_table_name = self.config.feature_store_table_name
        primary_keys = self.config.primary_keys
        features_store_table_description = self.config.features_store_table_description

        # Create db if not exists and delete actual table
        self.setup_db(feature_store_db, feature_store_table_name)

        # Store the features
        features_df = df.drop_duplicates().drop(self.config.target)
        feature_table_name = f'{feature_store_db}.{feature_store_table_name}'
        _logger.info(f'Creating and writing features to feature table: {feature_store_db}')

        fs.create_table(
            name=feature_table_name,
            primary_keys=primary_keys,
            df=features_df,
            partition_columns=primary_keys[0],
            schema=features_df.schema,
            description=features_store_table_description
        )

        _logger.info(f'Created feature store table: {feature_table_name}')




    def create_label_table(self, df):
        # Store configuration variables locally
        labels_table_database_name = self.config.labels_table_database_name
        labels_table_name = self.config.labels_table_name

        # Create db if not exists and delete actual table
        self.setup_db(labels_table_database_name, labels_table_name)

        # Store the labels
        labels_df = df.drop_duplicates().select(self.config.target)
        labels_table_name = f'{labels_table_database_name}.{labels_table_name}'

        _logger.info(f'Creating and writing labels to table: {labels_table_name}')

        # Write labels DataFrame as a Delta table
        labels_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(labels_table_name)

        _logger.info(f'Created labels table: {labels_table_name}')


    def run(self):
        _logger.info('==========Data Ingest==========')
        input_df = self.run_data_ingestion()

        _logger.info('==========Fetching Features and Labels==========')
        proc_df = self.fetch_features_target(input_df)

        _logger.info('==========Create Feature Table==========')
        self.create_feature_table(proc_df)

        _logger.info('==========Create Labels Table==========')
        self.create_label_table(proc_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute Pipeline
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 1. Check if databases and tables exists and drop them
# MAGIC 2. Data Ingestion with Labels 
# MAGIC 3. Fetch Features 
# MAGIC 4. Write in a table the features
# MAGIC 5. Write in a table the labels

# COMMAND ----------

# Define the config parameters
cfg = FeatureLabelsCreatorConfig(input_db=pipeline_config['input_db'],
                                 input_tables=pipeline_config['input_tables'],
                                 max_datetime=pipeline_config['max_datetime'],
                                 variables=pipeline_config['variables'],
                                 primary_keys=pipeline_config['fs_primary_keys'],
                                 target=pipeline_config['target'],
                                 global_feature_store=env_vars['global_feature_store_database_name'],
                                 local_feature_store=env_vars['local_feature_store_database_name'],
                                 feature_store_table_name = env_vars['feature_store_table_name'],
                                 features_store_table_description = env_vars['feature_store_table_description'],
                                 labels_table_database_name = env_vars['labels_table_database_name'],
                                 labels_table_name=env_vars['labels_table_name'])

# Creathe the feature table object
feature_table_creator_pipeline = FeatureLabelsCreator(cfg)

# Run the pipeline
feature_table_creator_pipeline.run()
