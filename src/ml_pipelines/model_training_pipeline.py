# Databricks notebook source
# COMMAND ----------
#MAGIC %pip install scikit-plot
#MAGIC %pip install feature-engine
#MAGIC %pip install --upgrade pandas 
#MAGIC %pip install mlflow
#MAGIC %pip install imbalanced-learn
# COMMAND ----------


from components.model_training import ModelTrainConfig, ModelTrain
from components.utils.loading import load_config
import logging

# Set up logging
logger = logging.getLogger(__name__)

def main():
    """
    Main function to initialize and run the feature store ingestion pipeline.
    """
    # Define the pipeline name
    pipeline_name = 'model_training_pipeline'
    logger.info(f"Initializing pipeline: {pipeline_name}")
    
    # Load pipeline configuration
    pipeline_config = load_config(pipeline_name)
    logger.info("Pipeline configuration loaded successfully.")
    
    # Initialize configuration object
    cfg = ModelTrainConfig(
    mlflow_tracking_cfg=pipeline_config['mlflow_tracking_cfg'],
    feature_store_table_cfg=pipeline_config['feature_store_table_cfg'],
    labels_table_cfg=pipeline_config['labels_table_cfg'],
    pipeline_params=pipeline_config['pipeline_params'],
    model_params=pipeline_config['model_params'],
    training_params=pipeline_config['training_params']
    )

    logger.info("ModelTrainConfig initialized successfully.")
    
    # Initialize and run the feature creation pipeline
    model_training_pipeline = ModelTrain(cfg)
    logger.info("Starting ModelTrain pipeline execution.")
    
    model_training_pipeline.run()
    logger.info("ModelTrain pipeline executed successfully.")

if __name__ == "__main__":
    main()
