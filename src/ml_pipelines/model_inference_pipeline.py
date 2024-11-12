# Databricks notebook source
# COMMAND ----------
#MAGIC %pip install scikit-plot
#MAGIC %pip install feature-engine
#MAGIC %pip install --upgrade pandas 
#MAGIC %pip install mlflow
#MAGIC %pip install imbalanced-learn
# COMMAND ----------


from components.model_inference import ModelInferenceCfg, ModelInference
from components.utils.loading import load_config
import logging

# Set up logging
logger = logging.getLogger()

# Dates for prediction
dt_start = '2018-01-01'
dt_stop = '2018-02-01'


def main():
    """
    Main function to initialize and run the feature store ingestion pipeline.
    """
    # Define the pipeline name
    pipeline_name = 'model_inference_pipeline'
    logger.info(f"Initializing pipeline: {pipeline_name}")
    
    # Load pipeline configuration
    pipeline_config = load_config(pipeline_name)
    logger.info("Pipeline configuration loaded successfully.")
    
    # Initialize configuration object
    cfg = ModelInferenceCfg(
    mlflow_tracking_cfg=pipeline_config['mlflow_tracking_cfg'],
    inference_table_cfg=pipeline_config['inference_table_cfg']
    dt_start = dt_start,
    dt_stop = dt_stop
    )

    logger.info("ModelInferenceCfg initialized successfully.")
    
    # Initialize and run the feature creation pipeline
    model_inference_pipeline = ModelInference(cfg)
    logger.info("Starting ModelInference pipeline execution.")
    
    model_inference_pipeline.run()
    logger.info("ModelInference pipeline executed successfully.")

if __name__ == "__main__":
    main()
