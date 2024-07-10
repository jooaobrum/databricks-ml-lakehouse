# Databricks notebook source
# MAGIC %md
# MAGIC # Model Deployment

# COMMAND ----------

dbutils.widgets.dropdown('env','dev',['dev','staging', 'prod'], 'Environment')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

import pandas as pd
import sklearn
import mlflow
from mlflow.models import infer_signature
import databricks
from databricks.feature_store import FeatureStoreClient, FeatureLookup
from mlflow.pyfunc import PythonModel
from dataclasses import dataclass
from sklearn.model_selection import StratifiedKFold, train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import (accuracy_score, precision_score, recall_score, 
                             f1_score, roc_auc_score, average_precision_score)

import sys
import pprint

sys.path.insert(0, '../libs')
from logger_utils import get_logger
from loading_utils import load_and_set_env_vars, load_config

# COMMAND ----------

_logger = get_logger()
fs = FeatureStoreClient()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Pipeline & Config Params

# COMMAND ----------

# Set pipeline name
pipeline_name = 'model_deployment'

# Load pipeline config
pipeline_config = load_config(pipeline_name)

# Load env vars
env_vars = load_and_set_env_vars(env = dbutils.widgets.get('env'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cfg Class

# COMMAND ----------

@dataclass
class MlflowTrackingCfg:
    run_name: str
    experiment_path: str
    model_name: str

@dataclass
class ModelDeploymentCfg:
    mlflow_tracking_cfg: MlflowTrackingCfg
    reference_start_date: str
    label_col: str
    comparison_metric: str
    higher_is_better: bool






# COMMAND ----------

# Set Mlflow configuration
mlflow_tracking_cfg = MlflowTrackingCfg(run_name=pipeline_config['mlflow_params']['run_name'],
                                        experiment_path=env_vars['model_train_experiment_path'],
                                        model_name=env_vars['model_name'])


# Set Deployment configuration
model_deployment_cfg = ModelDeploymentCfg(mlflow_tracking_cfg: MlflowTrackingCfg,
                                          reference_start_date: pipeline_config['reference_start_date']
                                          label_col: env_vars['labels_table_col']
                                          comparison_metric: pipeline_config['metric']
                                          higher_is_better: pipeline_config['higher_is_better']




# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Deployment Pipeline Class

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Set Experiment Id
# MAGIC 2. Get model URI by Stage -> Staging/Production
# MAGIC 3. Inference on reference by Stage (end_training + 30 days)
# MAGIC 4. Compare deployment metric
# MAGIC 5. Promote model

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Deployment Class

# COMMAND ----------

class ModelDeploymentPipeline:
    def __init__(self , cfg: ModelDeploymentCfg):
        self.cfg = cfg

    def _set_experiment_id(self, cfg: ModelDeploymentCfg)
        if mlflow_tracking_cfg.experiment_id is not None:
            _logger.info(f'MLflow experiment_id: {mlflow_tracking_cfg.experiment_id}')
            mlflow.set_experiment(experiment_id=mlflow_tracking_cfg.experiment_id)
        else:
            raise RuntimeError('MLflow experiment_id or experiment_path must be set in MLflowTrackingConfig')

    def _get_model_by_stage(self, stage:)
        return f"models:/{self.cfg.mlflowtracking_cfg.model_name}/{stage}"
    
    def _inference_by_stage(self, stage):
        define query
        score_batch

    def _evaluate_on_reference(self, y_ref, y_prob, scoring, stage):
        evaluate

    def _get_training_metrics(self, s)
    



# COMMAND ----------

cfg = ModelTrainConfig(mlflow_tracking_cfg=mlflow_tracking_cfg,
                       feature_store_table_cfg=feature_store_table_cfg,
                       labels_table_cfg=labels_table_cfg,
                       pipeline_params=pipeline_params,
                       model_params=model_params,
                       conf=pipeline_config,    
                       env_vars=env_vars       
                      )


model_train_pipeline = ModelTrain(cfg)
model_train_pipeline.run()
