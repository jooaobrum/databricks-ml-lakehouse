# Databricks notebook source
# MAGIC %md
# MAGIC 1. Need to package lib for gx
# MAGIC 2. Need to package lib for pandas-profiling
# MAGIC 3. Need to parametrize the notebook

# COMMAND ----------

# MAGIC %pip install great_expectations
# MAGIC %pip install ydata-profiling==4.0.0
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Great expectations

# COMMAND ----------

import sys

sys.path.insert(0, "../libs")
from utils import check_and_write_to_delta_table
from validation_utils import DataQuality
from yaml_expectation_configurations import get_yaml_expectation_configurations

# COMMAND ----------

dt_start = dbutils.widgets.get("dt_start")
dt_stop = dbutils.widgets.get("dt_start")
task_key = dbutils.widgets.get("task_key").split("_")[0]
db_name = "feature_store"
ref = "olist"
table_name = f"{ref}_{task_key}_features"
validator_name = f"{table_name}_expectations"
profiling_name = f"{table_name}_profiling"
yml_expectations = f"expectations_yml/{validator_name}.yaml"


# COMMAND ----------

df = spark.sql(
    "SELECT * FROM feature_store.{table_name} WHERE fs_reference_timestamp >= '{dt_start}' AND fs_reference_timestamp <= '{dt_stop}'".format(
        table_name=table_name, dt_start=dt_start, dt_stop=dt_stop
    )
).toPandas()

# COMMAND ----------

# Generate the expectations rules
expectations_as_list = []
try:
    expectations_as_list = get_yaml_expectation_configurations(yml_expectations)
except Exception as e:
    print(f"An error occured while reading and parsing expectations: {str(e)}")
    raise e


# COMMAND ----------

# Generate the quality report
data_quality = DataQuality(table_name, df, expectations_as_list)

# Validate rules
result_expectations = data_quality.run_validations()

# COMMAND ----------

# Extract statistics from expectations
GE_statistics = data_quality.extract_expectations_statistics(result_expectations)

# COMMAND ----------

# Extract statistics from profiling
df_profiling = data_quality.generate_profiling_dataframe(result_expectations)

# COMMAND ----------

# Transform to spark
expectations_df = spark.createDataFrame(GE_statistics)
profiling_df = spark.createDataFrame(df_profiling)

# COMMAND ----------

# Save results in a delta table
check_and_write_to_delta_table(spark, expectations_df, db_name, validator_name, mode="append")

# For profiling_name
check_and_write_to_delta_table(spark, profiling_df, db_name, profiling_name, mode="append")
