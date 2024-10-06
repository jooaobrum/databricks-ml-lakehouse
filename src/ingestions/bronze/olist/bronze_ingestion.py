# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Setup

# COMMAND ----------

# Task key definition - match with raw table name
task_key = dbutils.widgets.get('task_key').split('__')[1]

# Name of the database to ingest
db_name = 'olist_bronze'

# Reference of the data
ref_name = 'olist'

# Format of the raw file
file_format = 'csv'

# Raw table
raw_table_name = task_key

# Table name in the database
table_name = f'{ref_name}_{raw_table_name}'

# File's path in the landing zone
raw_path = f"/mnt/datalake/{ref_name}/0-lz/{table_name}.csv"

# Schema path
schema_path = f"/mnt/datalake/{ref_name}/0-lz-schemas/{table_name}_schema.json"

# Output path
bronze_path = f"/mnt/datalake/{ref_name}/1-bronze/{table_name}"

# Reading options 
read_opt = {
        "header": "true",
        "sep": ",",
    }

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Starting Bronze Ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

import json
from pyspark.sql.types import StructType    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions

# COMMAND ----------

def load_schema(schema_path):
    # Read pre-defined Schema 
    with open('/dbfs' + schema_path, 'r') as f:
        schema = f.read()

    # Correct schema
    table_schema = StructType.fromJson(json.loads(schema))

    return table_schema

# COMMAND ----------

def read_data(file_format, table_schema, read_opt, raw_path):
    # Entry point to read data
    spark_reader = (spark.read
                        .format(file_format)
                        .schema(table_schema)
                        .options(**read_opt))

    # Read file
    df = spark_reader.load(raw_path)

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Reading Schema

# COMMAND ----------

# Load schema
table_schema = load_schema(schema_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### 2. Reading Raw File

# COMMAND ----------

# Read file from landing zone
df_read = read_data(file_format, table_schema, read_opt, raw_path)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 3. Transforming Ingested Query 

# COMMAND ----------

# Create tmp view to modify file
view_tmp = f"view_{table_name}"
df_read.createOrReplaceTempView(view_tmp)

# Retrieve file name to ingest
ingestor_file = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# Transform to add file ingestor, key and timestamp
query_to_ingest = """

    SELECT  *,
            '{ingestor_file}' as table_ingestor_file,
            '{task_key}_bronze_ingestion' as table_task_key, 
            DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd') as dt_ingestion
           

           FROM {view_tmp}

"""

df_ingestion = spark.sql(query_to_ingest.format(ingestor_file = ingestor_file, task_key = task_key, view_tmp = view_tmp))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Saving as Delta Table in Bronze Schema

# COMMAND ----------

# Check if the table exists
if spark.catalog.tableExists(f"{db_name}.{table_name}"):
    print('Table exists, not performing full ingestion.')
else:
    print("Table doesn't exist, performing first full ingestion.")
    
    # Save full table
    (
        df_ingestion
            .write
            .partitionBy("dt_ingestion") 
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("path", bronze_path)
            .saveAsTable(f"{db_name}.{table_name}")
    )

