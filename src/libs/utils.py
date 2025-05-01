def write_to_delta_table(spark, df, db_name, table_name, mode="overwrite"):
    """
    Write DataFrame to a Delta table in Databricks.

    Parameters:
    - df: DataFrame to be written.
    - db_name: Database name.
    - table_name: Table name.
    - mode: Write mode, either "overwrite" or "append".
    """
    writer = df.coalesce(1).write.format("delta").mode(mode).option("overwriteSchema", "true")

    writer.saveAsTable(f"{db_name}.{table_name}")


def check_and_write_to_delta_table(spark, df, db_name, table_name, mode="overwrite"):
    """
    Check if a Delta table exists and write DataFrame accordingly.

    Parameters:
    - df: DataFrame to be written.
    - db_name: Database name.
    - table_name: Table name.
    - mode: Write mode, either "overwrite" or "append". Default is "overwrite".
    """
    if not spark.catalog.tableExists(f"{db_name}.{table_name}"):
        mode = "overwrite"  # If the table exists, always overwrite

    write_to_delta_table(spark, df, db_name, table_name, mode=mode)
