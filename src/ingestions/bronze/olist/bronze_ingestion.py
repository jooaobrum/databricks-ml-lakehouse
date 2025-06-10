import argparse
import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def parse_arguments():
    """Parse command line arguments or use default values."""
    parser = argparse.ArgumentParser(description="Generalized Data Ingestion to Bronze Layer")

    # Required parameters
    parser.add_argument("--root_path", type=str, required=True, help="Root path for the ingestion process")
    parser.add_argument(
        "--task_key",
        type=str,
        required=False,
        default="olist__olist_customers",
        help="Task key in format source__table",
    )
    parser.add_argument("--env", type=str, required=False, default="dev", help="Environment (dev, qa, prod)")
    parser.add_argument(
        "--file_format", type=str, required=False, default="csv", help="Source file format (csv, json, parquet, etc.)"
    )
    parser.add_argument(
        "--partitions", type=str, required=False, default="dt_ingestion", help="Comma-separated partition columns"
    )
    parser.add_argument(
        "--delimiter", type=str, required=False, default=",", help="Delimiter for CSV files (,, ;, \\t, etc.)"
    )
    parser.add_argument(
        "--header", type=str, required=False, default="true", help="Whether CSV has header (true/false)"
    )

    args = parser.parse_args()
    return args


def setup_configuration(args):
    """Set up configuration based on arguments."""
    # Parse task key
    ref_name = args.task_key.split("__")[1]
    lz_table_name = args.task_key.split("__")[2]

    # Define reading options based on file format
    read_opt = get_read_options(args.file_format, args)

    # Create configuration dictionary
    cfg = {
        "ref_name": ref_name,
        "bronze_catalog_name": f"uc_{args.env}",
        "bronze_schema_name": f"{ref_name}_bronze",
        "lz_file_format": args.file_format,
        "lz_table_name": lz_table_name,
        "bronze_table_name": lz_table_name,
        "lz_files_path": f"/Volumes/uc_lz/{ref_name}/files/{lz_table_name}.{args.file_format}",
        "lz_schema_path": f"/Volumes/uc_lz/{ref_name}/schemas/{lz_table_name}_schema.json",
        "partition_columns": args.partitions.split(","),
    }

    return cfg, read_opt


def get_read_options(file_format, args):
    """
    Return appropriate read options based on file format and arguments.

    Args:
        file_format (str): The file format (csv, json, parquet, etc.)
        args: Command line arguments containing format-specific options

    Returns:
        dict: Dictionary of reading options
    """
    if file_format == "csv":
        # Handle CSV options with custom delimiter
        return {
            "header": args.header,
            "sep": args.delimiter,
            "inferSchema": "false",
            "escape": '"',
            "quote": '"',
            "nullValue": "",
        }
    elif file_format == "json":
        return {"multiline": "true"}
    elif file_format == "parquet":
        return {}
    elif file_format == "excel" or file_format == "xlsx":
        return {"header": args.header}
    else:
        return {}


def load_schema(schema_path: str) -> StructType:
    """
    Load schema from JSON file path.

    Args:
        schema_path (str): Path to the schema JSON file

    Returns:
        StructType: Spark schema object
    """

    file_content = dbutils.fs.head(schema_path)
    schema_json = json.loads(file_content)
    table_schema = StructType.fromJson(schema_json)
    return table_schema


def read_data(
    spark: SparkSession, path: str, file_format: str, schema: StructType = None, read_opt: dict = None
) -> DataFrame:
    """
    Read data from specified path into a DataFrame.

    Args:
        spark (SparkSession): Active Spark session
        path (str): Path to the data file
        file_format (str): Format of the data file
        schema (StructType, optional): Schema to apply
        read_opt (dict, optional): Reading options

    Returns:
        DataFrame: Loaded data
    """
    if read_opt is None:
        read_opt = {}

    # Create reader
    reader = spark.read.format(file_format)

    # Apply schema if provided
    if schema is not None:
        reader = reader.schema(schema)

    # Apply options
    for key, value in read_opt.items():
        reader = reader.option(key, value)

    # Load data
    return reader.load(path)


def add_metadata_columns(spark: SparkSession, df: DataFrame, task_key: str) -> DataFrame:
    """
    Add metadata columns to the DataFrame.

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
    ingestor_file = "bronze_ingestion"

    # Add metadata columns
    query = f"""
        SELECT
            *,
            '{ingestor_file}' as table_ingestor_file,
            '{task_key}_bronze_ingestion' as table_task_key,
            DATE_FORMAT(current_timestamp(), 'yyyy-MM-dd') as dt_ingestion
        FROM {view_name}
    """

    return spark.sql(query)


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
    cfg, read_opt = setup_configuration(args)

    # Initialize Spark session
    spark = SparkSession.builder.appName(f"Ingest_{cfg['lz_table_name']}").getOrCreate()

    print(f"Starting ingestion process for {args.task_key}")
    print(f"Configuration: {cfg}")
    print(f"Read options: {read_opt}")

    try:
        # Load schema
        print(f"Loading schema from {cfg['lz_schema_path']}")
        table_schema = load_schema(cfg["lz_schema_path"])

        # Read data
        print(f"Reading data from {cfg['lz_files_path']}")
        df_raw = read_data(
            spark=spark,
            path=cfg["lz_files_path"],
            file_format=cfg["lz_file_format"],
            schema=table_schema,
            read_opt=read_opt,
        )

        # Print the schema and sample data for verification
        print("Data schema:")
        df_raw.printSchema()
        print("Sample data (5 rows):")
        df_raw.show(5, truncate=False)

        # Add metadata
        print("Adding metadata columns")
        df_with_metadata = add_metadata_columns(spark, df_raw, args.task_key)

        # Check if table exists
        full_table_name = f"{cfg['bronze_catalog_name']}.{cfg['bronze_schema_name']}.{cfg['bronze_table_name']}"
        if spark.catalog.tableExists(full_table_name):
            print(f"Table {full_table_name} exists, not performing full ingestion.")
        else:
            print(f"Table {full_table_name} doesn't exist, performing first full ingestion.")
            # Save as Delta table
            save_as_delta_table(
                df=df_with_metadata,
                catalog=cfg["bronze_catalog_name"],
                schema=cfg["bronze_schema_name"],
                table=cfg["bronze_table_name"],
                partition_cols=cfg["partition_columns"],
                mode="overwrite",
            )

        print("Ingestion process completed successfully")

    except Exception as e:
        print(f"Error during ingestion process: {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
