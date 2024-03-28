# Databricks notebook source
import os

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Setup

# COMMAND ----------

# Task key to automate the workflow 
task_key = dbutils.widgets.get('task_key')

# Name of the database to ingest
db_name = 'feature_store'

# Reference of the data
ref_name = 'olist'

dt_start = dbutils.widgets.get('dt_start')
dt_stop = dbutils.widgets.get('dt_stop')
step = int(dbutils.widgets.get('step'))

# Aggregated feature window
window_size = dbutils.widgets.get('window_size').split(',')

# Feature Store table name
table_name = f"{ref_name}_{task_key}_features"
primary_keys = dbutils.widgets.get('primary_keys').split(',')
feature_store_description = dbutils.widgets.get('feature_store_description')

# Query's path to transform to fs
base_query_path = f"feature_store_transformation/{task_key}_feature_store.sql"

agg_query_path = f"feature_store_transformation/{task_key}_feature_store_agg.sql"

# Check if the file exists
if os.path.exists(agg_query_path):
    # File exists, assign the path
    pass  # No need to do anything, agg_query_path already set correctly
else:
    # File doesn't exist, set agg_query_path to None
    agg_query_path = None

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Starting Feature Store Ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Functions

# COMMAND ----------

def read_transf_query(path):
    # Read query 
    if path != None:
        with open(path, 'r') as f:
            query = f.read()

        return query
    
    else:
        None

# COMMAND ----------

def generate_dates(date_start, date_stop, step):
    # Convert dtStart and dtStop to datetime
    date_start = datetime.datetime.strptime(date_start, '%Y-%m-%d')
    date_stop = datetime.datetime.strptime(date_stop, '%Y-%m-%d')

    # Generate dates
    n_days = (date_stop - date_start).days + 1
    dates = [datetime.datetime.strftime(date_start + datetime.timedelta(days = i), '%Y-%m-%d') for i in range(0,n_days, step)]


    return dates

# COMMAND ----------

import datetime
from databricks import feature_store
from functools import reduce
from tqdm import tqdm

# COMMAND ----------

# Initiate Feature Store Object
fs = feature_store.FeatureStoreClient()

# COMMAND ----------

# Generate dates
dates = generate_dates(dt_start, dt_stop, step)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Read Queries

# COMMAND ----------

base_query = read_transf_query(base_query_path)
agg_query = read_transf_query(agg_query_path)

# COMMAND ----------

for date in tqdm(dates):

    # Read base
    df_feature_store_base = spark.sql(base_query.format(date=date))
    df_feature_store_base = df_feature_store_base.dropDuplicates(primary_keys)

    if agg_query != None:
        windows_dfs_list = []  # List to store DataFrames for the current date
        for window in window_size:
            df_feature_store_agg = spark.sql(agg_query.format(date=date, window=window))
            windows_dfs_list.append(df_feature_store_agg)
        
        
        # Concatenate DataFrames for the current date horizontally (along columns)
        df_feature_store_total = reduce(lambda df1, df2: df1.join(df2, on=primary_keys, how='inner'), windows_dfs_list)

    
        # Merge them
        df_feature_store_total = df_feature_store_total.join(df_feature_store_base, on=primary_keys, how='inner')

    else:
        df_feature_store_total = df_feature_store_base

    # Check if table exists
    if spark.catalog.tableExists(f"{db_name}.{table_name}"):
        fs.write_table(name=f'{db_name}.{table_name}', df=df_feature_store_total, mode='merge')
    else:
        fs.create_table(
            name=f'{db_name}.{table_name}',
            primary_keys=primary_keys, 
            df=df_feature_store_total,
            partition_columns=["fs_reference_timestamp"],
            schema=df_feature_store_total.schema,
            description=feature_store_description
        )


# COMMAND ----------


