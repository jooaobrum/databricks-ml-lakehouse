# Databricks notebook source
# MAGIC %pip install mlflow
# MAGIC %pip install nannyml
# MAGIC %pip install fastparquet

# COMMAND ----------


import logging

from components.data_drift import DataDrift, DataDriftConfig
from components.utils.loading import load_config

# Set up logging
logger = logging.getLogger()


def main():
    """
    Main function to initialize and run the feature store ingestion pipeline.
    """
    # Define the pipeline name
    pipeline_name = "data_drift_pipeline"
    logger.info(f"Initializing pipeline: {pipeline_name}")

    # Load pipeline configuration
    pipeline_config = load_config(pipeline_name)
    logger.info("Pipeline configuration loaded successfully.")

    # Initialize configuration object
    cfg = DataDriftConfig(
        mlflow_tracking_cfg=pipeline_config["mlflow_tracking_cfg"],
        inference_table_cfg=pipeline_config["inference_table_cfg"],
        data_drift_cfg=pipeline_config["data_drift_cfg"],
        dt_start=dbutils.widgets.get("dt_start"),
        dt_stop=dbutils.widgets.get("dt_stop"),
    )

    logger.info("DataDriftCfg initialized successfully.")

    # Initialize and run the feature creation pipeline
    data_drift_pipeline = DataDrift(cfg)
    logger.info("Starting DataDrift pipeline execution.")

    data_drift_pipeline.run()
    logger.info("DataDrift pipeline executed successfully.")


if __name__ == "__main__":
    main()
