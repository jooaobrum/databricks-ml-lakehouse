# Databricks notebook source

from components.feature_creator import FeatureCreatorConfig, FeatureCreator
from components.utils.loading import load_config
import logging

# Set up logging
logger = logging.getLogger(__name__)

def main():
    """
    Main function to initialize and run the feature store ingestion pipeline.
    """
    # Define the pipeline name
    pipeline_name = 'feature_store_ingestion'
    logger.info(f"Initializing pipeline: {pipeline_name}")
    
    # Load pipeline configuration
    pipeline_config = load_config(pipeline_name)
    logger.info("Pipeline configuration loaded successfully.")
    
    # Initialize configuration object
    cfg = FeatureCreatorConfig(
        ref_name=pipeline_config['ref_name'],
        db_name=pipeline_config['db_name'],
        table_name=pipeline_config['table_name'].format(task_key = dbutils.widgets.get('task_key')),
        fs_path=pipeline_config['fs_path'].format(task_key = dbutils.widgets.get('task_key')),
        base_query_path=pipeline_config['base_query_path'].format(task_key = dbutils.widgets.get('task_key')),
        task_key=dbutils.widgets.get('task_key'),
        dt_start=dbutils.widgets.get('dt_start'),
        dt_stop=dbutils.widgets.get('dt_stop'),
        step=int(dbutils.widgets.get('step'))
    )
    logger.info("FeatureCreatorConfig initialized successfully.")
    
    # Initialize and run the feature creation pipeline
    feature_table_creator_pipeline = FeatureCreator(cfg)
    logger.info("Starting FeatureCreator pipeline execution.")
    
    feature_table_creator_pipeline.run()
    logger.info("FeatureCreator pipeline executed successfully.")

if __name__ == "__main__":
    main()
