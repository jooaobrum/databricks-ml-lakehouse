# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Task key to automate the workflow 
task_key = dbutils.widgets.get('task_key')

# Name of the database to ingest
db_name = 'silver'

# Reference of the data
ref_name = 'olist'

# Bronze table name
table_name = f"{ref_name}_{task_key}"

bronze_table_name = f"bronze.{table_name}"

# Silver table to ingest
silver_table_to_ingest = f"silver.{table_name}"


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

# Save full table
writer = (
                 df_norm.coalesce(1)
                        .write
                        .format("delta")
                        .mode("overwrite")
                        .option("overwriteSchema", "true")
        )

# Check if table exists
if spark.catalog.tableExists(f"{db_name}.{table_name}"):
    print('Table exists, not performing full ingestion.')
else:
    print("Table doesn't exist, performing first full ingestion.")
    writer.saveAsTable(f"{silver_table_to_ingest}")

