# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Setup

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The idea is to use a standard account for this purpose. Then, the unity catalog will not be available free of charges. Then, to overcome this problem, I will create databases to separate each layer of the architecture to simplify this idea. 

# COMMAND ----------

task_key = dbutils.widgets.get('task_key')

# Name of the database to ingest
db_name = 'bronze'

# Reference of the data
ref_name = 'olist'

# Raw table
raw_table_name = task_key

# Table name in the database
table_name = f'{ref_name}_{raw_table_name}'

# Format of the raw file
file_format = 'csv'

# Partition by 
partition_fields = ''

# File's path in the landing zone
raw_path = f"/mnt/landing_zone/olist/{raw_table_name}.csv"


read_opt = {
        "header": "true",
        "sep": ",",
    }

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Bronze Ingestion

# COMMAND ----------

# Entry point to read data
spark_reader = (spark.read
                    .format(file_format)
                    .options(**read_opt))

# Read file
df_read = spark_reader.load(raw_path)

# Create tmp view
view_tmp = f"view_{table_name}"
df_read.createOrReplaceTempView(view_tmp)

# File name
ingestor_file = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# Transform to add file ingestor and dt_ingestor
query_to_ingest = """

    SELECT  *,
            '{ingestor_file}' as table_ingestor_file,
            '{task_name}' as table_task_name, 
            current_timestamp() as table_ingestor_timestamp
           

           FROM {view_tmp}

"""

df_ingestion = spark.sql(query_to_ingest.format(ingestor_file = ingestor_file, task_name = task_name, view_tmp = view_tmp))

# Save full table
writer = (
            df_ingestion.coalesce(1)
                        .write.format("delta")
                        .mode("overwrite")
                        .option("overwriteSchema", "true")
        )

# Check if table exists
if spark.catalog.tableExists(f"{db_name}.{table_name}"):
        print('Table exists, not performing full ingestion.')
else:
        print('Table doesn't exiting, performing first full ingestion.')
        writer.saveAsTable(f"{db_name}.{table_name}")
