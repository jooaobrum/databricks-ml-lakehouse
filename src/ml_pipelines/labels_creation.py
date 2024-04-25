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
pipeline_name = 'labels_creation'

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

    reference_database: str
    max_datetime: str
    target: str
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

        # if 'feature_store' in db_name:
        #     try: 
        #         fs.drop_table(
        #         name=f'{db_name}.{table_name}'
        #         )
        #     except:
        #         pass


    def run_data_ingestion(self):


        max_datetime = self.config.max_datetime

        query_to_train = """
            WITH tb as (
                SELECT DISTINCT t1.order_purchase_timestamp,
                                    CAST(date_format(t1.order_purchase_timestamp, 'yyyy-MM-dd') as DATE) as fs_reference_timestamp,
                                    t2.customer_unique_id,
                                    t1.order_id,
                                    CASE WHEN t3.review_score < 3 THEN 1 ELSE 0 END as bad_review_flag

                FROM silver.olist_orders AS t1
                LEFT JOIN silver.olist_customers AS t2
                ON t1.customer_id = t2.customer_id
                LEFT JOIN silver.olist_order_reviews as t3
                ON t1.order_id = t3.order_id
                WHERE t2.customer_unique_id IS NOT NULL

                )

            SELECT t1.*

            FROM tb as t1
            INNER JOIN feature_store.olist_orders_features as t2
            ON t1.order_id = t2.order_id
            INNER JOIN feature_store.olist_customer_features as t3
            ON t1.fs_reference_timestamp = t3.fs_reference_timestamp
            AND t1.customer_unique_id = t3.customer_unique_id


            WHERE t1.order_purchase_timestamp <= '{max_datetime}'



"""

        query_to_train = query_to_train.format(max_datetime = max_datetime)

        return spark.sql(query_to_train)


        return df


    def create_label_table(self, df):
        # Store configuration variables locally
        reference_db = self.config.reference_database
        labels_table_name = self.config.labels_table_name

        # Create db if not exists and delete actual table
        self.setup_db(reference_db, labels_table_name)

        # Store the labels
        labels_df = df.drop_duplicates()
        labels_table_name = f'{reference_db}.{labels_table_name}'

        _logger.info(f'Creating and writing labels to table: {labels_table_name}')

        # Write labels DataFrame as a Delta table
        labels_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(labels_table_name)

        _logger.info(f'Created labels table: {labels_table_name}')


    def run(self):
        _logger.info('==========Data Ingest==========')
        input_df = self.run_data_ingestion()


        _logger.info('==========Create Labels Table==========')
        self.create_label_table(input_df)


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


