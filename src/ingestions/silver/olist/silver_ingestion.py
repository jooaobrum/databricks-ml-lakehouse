# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Task key to automate the workflow 
task_key = dbutils.widgets.get('task_key').split('__')[1]

# Name of the database bronze
bronze_db_name = 'olist_bronze'
silver_db_name = 'olist_silver'

# Reference of the data
ref_name = 'olist'

# Bronze table name
table_name = f"{ref_name}_{task_key}"
bronze_table_name = f"{bronze_db_name}.{table_name}"


# Silver table to ingest
silver_table_to_ingest = f"{silver_db_name}table.{table_name}"
silver_path = f"/mnt/datalake/{ref_name}/2-silver/{table_name}"


# Query's path to transform to silver
transformation_query_path = f"silver_transformation/{task_key}.sql"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Starting Silver Ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions

# COMMAND ----------

def read_transf_query(path):
    # Read query 
    with open(path, 'r') as f:
        query = f.read()

    return query

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1. Reading Bronze Table

# COMMAND ----------

# Read from Bronze
df = spark.sql(f"SELECT * FROM {bronze_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2. Normalization & Transformation

# COMMAND ----------

# Create tmp view to modify file to silver
view_tmp = f"view_{table_name}"
df.createOrReplaceTempView(view_tmp)

# Read query 
transf_query = read_transf_query(transformation_query_path)

# Retrieve file name to ingest
ingestor_file = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# Apply Normalization - My custom column standard from table view
df_norm = spark.sql(transf_query.format(ingestor_file = ingestor_file, task_key = task_key, view_tmp = view_tmp))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Saving as Delta Table in Silver Schema

# COMMAND ----------

# Check if the table exists
if spark.catalog.tableExists(f"{silver_db_name}.{table_name}"):
    print('Table exists, not performing full ingestion.')
else:
    print("Table doesn't exist, performing first full ingestion.")
    
    # Save full table
    (
        df_norm
            .write
            .partitionBy("dt_ingestion") 
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("path", silver_path)
            .saveAsTable(f"{silver_db_name}.{table_name}")
    )
