# Databricks notebook source
# MAGIC %pip install tqdm
# MAGIC %pip install pyyaml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Setup

# COMMAND ----------

# Get dbutils parameters
task_key = dbutils.widgets.get('task_key')
dt_start = dbutils.widgets.get('dt_start')
dt_stop = dbutils.widgets.get('dt_stop')
step = int(dbutils.widgets.get('step'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Starting Feature Store Ingestion

# COMMAND ----------

def read_transf_query(path):
    # Read query 
    with open(path, 'r') as f:
        query = f.read()

    return query

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
from functools import reduce
from tqdm import tqdm
import yaml


# COMMAND ----------

# Load YAML config file
with open('feature_store_ingestion.yaml', 'r') as file:
    config = yaml.safe_load(file)


# Placeholders
db_name = config['db_name']
ref_name = config['ref_name']
table_name = config['table_name'].replace("{task_key}", task_key)
fs_path = config['fs_path'].replace("{task_key}", task_key)
base_query_path = config['base_query_path'].replace("{task_key}", task_key)

# Generate dates
dates = generate_dates(dt_start, dt_stop, step)

# Read query to apply transformation
base_query = read_transf_query(base_query_path)

for date in tqdm(dates):
    # Read base
    df_feature_store_base = spark.sql(base_query.format(dt_ingestion=date))
  
    # Check if table exists and write accordingly
    if spark.catalog.tableExists(f"{db_name}.{table_name}"):
        df_feature_store_base.write.format("delta").mode("append").saveAsTable(f"{db_name}.{table_name}")
    else:
        df_feature_store_base.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{db_name}.{table_name}")
