import argparse

from pyspark.sql import DataFrame, SparkSession


def parse_arguments():
    """Parse command line arguments or use default values."""
    parser = argparse.ArgumentParser(description="Generalized Data Ingestion to Silver Layer")

    # Required parameters
    parser.add_argument("--root_path", type=str, required=True, help="Root path for the ingestion process")
    parser.add_argument(
        "--task_key", type=str, required=False, default="olist__olist_orders", help="Task key in format source__table"
    )
    parser.add_argument("--env", type=str, required=False, default="dev", help="Environment (dev, qa, prod)")
    parser.add_argument(
        "--partitions", type=str, required=False, default="dt_ingestion", help="Comma-separated partition columns"
    )

    args = parser.parse_args()
    return args


def setup_configuration(args):
    """Set up configuration based on arguments."""
    # Parse task key
    ref_name = args.task_key.split("__")[1]
    bronze_table_name = args.task_key.split("__")[2]
    silver_table_name = args.task_key.split("__")[2]
    partition_columns = args.partitions.split(",")

    # Create configuration dictionary
    cfg = {
        "ref_name": ref_name,
        "bronze_catalog_name": f"uc_{args.env}",
        "bronze_schema_name": f"{ref_name}_bronze",
        "bronze_table_name": bronze_table_name,
        "silver_catalog_name": f"uc_{args.env}",
        "silver_schema_name": f"{ref_name}_silver",
        "silver_table_name": silver_table_name,
        "transformation_query_path": f"silver_transformation/{silver_table_name}.sql",
        "partition_columns": partition_columns,
    }

    return cfg


def read_query(path: str, arguments: dict = None) -> str:
    """
    Read query from file and replace placeholders with arguments.

    Args:
        path (str): Path to the query file
        arguments (dict, optional): Arguments to replace in the query

    Returns:
        str: Query with replaced placeholders
    """
    # Read query
    with open(path, "r") as f:
        query = f.read()

    if arguments is None:
        return query

    # Replace placeholders with arguments
    for key, value in arguments.items():
        if isinstance(value, str):
            # Replace simple {key} format placeholder
            query = query.replace(f"{{{key}}}", value)
        elif isinstance(value, list):
            query = query.replace(f"{{{key}}}", ", ".join(value))
        else:
            raise ValueError(f"Unsupported type for argument {key}: {type(value)}")
    return query


def read_bronze_data(spark: SparkSession, cfg: dict) -> DataFrame:
    """
    Read data from bronze table.

    Args:
        spark (SparkSession): Active Spark session
        cfg (dict): Configuration dictionary

    Returns:
        DataFrame: Loaded data from bronze table
    """
    query = f"SELECT * FROM {cfg['bronze_catalog_name']}.{cfg['bronze_schema_name']}.{cfg['bronze_table_name']}"
    return spark.sql(query)


def add_metadata_columns(spark: SparkSession, df: DataFrame, task_key: str) -> DataFrame:
    """
    Add metadata columns to the DataFrame for silver layer.

    Args:
        spark (SparkSession): Active Spark session
        df (DataFrame): Input DataFrame
        task_key (str): Task key identifier

    Returns:
        DataFrame: DataFrame with added metadata columns
    """
    # Register temp view
    view_name = f"view_{task_key.replace('__', '_')}"
    df.createOrReplaceTempView(view_name)

    # Get ingestor file name
    ingestor_file = "silver_ingestion"

    # Add metadata columns
    query = f"""
        SELECT
            *,
            '{ingestor_file}' as table_ingestor_file,
            '{task_key}_silver_ingestion' as table_task_key,
            DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd') as dt_ingestion
        FROM {view_name}
    """

    return spark.sql(query)


def apply_transformations(spark: SparkSession, df: DataFrame, cfg: dict, task_key: str) -> DataFrame:
    """
    Apply transformations to the DataFrame.

    Args:
        spark (SparkSession): Active Spark session
        df (DataFrame): Input DataFrame
        cfg (dict): Configuration dictionary
        task_key (str): Task key identifier

    Returns:
        DataFrame: Transformed DataFrame
    """
    # Read transformation query
    query_path = cfg["transformation_query_path"]
    query = read_query(query_path, {"task_key": task_key})

    print(f"Executing transformation query: {query}")
    # Register temp view for transformations
    df.createOrReplaceTempView(f"bronze_{task_key}")

    # Execute transformation query
    transformed_df = spark.sql(query)

    # Add metadata columns
    transformed_df = add_metadata_columns(spark, transformed_df, task_key)

    return transformed_df


def save_as_delta_table(
    df: DataFrame, catalog: str, schema: str, table: str, partition_cols: list, mode: str = "overwrite"
) -> None:
    """
    Save DataFrame as a Delta table.

    Args:
        df (DataFrame): DataFrame to save
        catalog (str): Catalog name
        schema (str): Schema name
        table (str): Table name
        partition_cols (list): Columns to partition by
        mode (str): Write mode (overwrite, append, etc.)
    """
    full_table_name = f"{catalog}.{schema}.{table}"

    writer = df.write.format("delta").mode(mode).option("overwriteSchema", "true")

    # Apply partitioning if partition columns are specified
    if partition_cols and partition_cols[0]:  # Check if partition list is not empty
        writer = writer.partitionBy(*partition_cols)

    writer.saveAsTable(full_table_name)
    print(f"Successfully saved table: {full_table_name}")


def main():
    """Main execution function."""
    # Parse arguments
    args = parse_arguments()

    # Set up configuration
    cfg = setup_configuration(args)

    # Initialize Spark session
    spark = SparkSession.builder.appName(f"Ingest_{cfg['silver_table_name']}").getOrCreate()

    print(f"Starting silver ingestion process for {args.task_key}")
    print(f"Configuration: {cfg}")

    try:
        # Read from Bronze
        print(
            f"Reading data from bronze table: {cfg['bronze_catalog_name']}.{cfg['bronze_schema_name']}.{cfg['bronze_table_name']}"
        )
        df_bronze = read_bronze_data(spark, cfg)

        # Print the schema and sample data for verification
        print("Bronze data schema:")
        df_bronze.printSchema()
        print("Sample bronze data (5 rows):")
        df_bronze.show(5, truncate=False)

        # Apply transformations
        print("Applying normalization and transformations")
        df_silver = apply_transformations(spark, df_bronze, cfg, args.task_key)

        # Print transformed data for verification
        print("Silver data schema:")
        df_silver.printSchema()
        print("Sample silver data (5 rows):")
        df_silver.show(5, truncate=False)

        # Check if table exists
        full_table_name = f"{cfg['silver_catalog_name']}.{cfg['silver_schema_name']}.{cfg['silver_table_name']}"
        if spark.catalog.tableExists(full_table_name):
            print(f"Table {full_table_name} exists, not performing full ingestion.")
        else:
            print(f"Table {full_table_name} doesn't exist, performing first full ingestion.")
            # Save as Delta table
            save_as_delta_table(
                df=df_silver,
                catalog=cfg["silver_catalog_name"],
                schema=cfg["silver_schema_name"],
                table=cfg["silver_table_name"],
                partition_cols=cfg["partition_columns"],
                mode="overwrite",
            )

        print("Silver ingestion process completed successfully")

    except Exception as e:
        print(f"Error during silver ingestion process: {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
