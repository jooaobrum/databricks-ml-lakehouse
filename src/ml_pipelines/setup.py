# Databricks notebook source
# MAGIC %md
# MAGIC # Setup for Deployment

# COMMAND ----------

dbutils.widgets.dropdown('env','dev',['dev','staging', 'prod'], 'Environment')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

import mlflow
from databricks.feature_store.client import FeatureStoreClient
from mlflow.tracking import MlflowClient
from mlflow.exceptions import RestException
import sys

sys.path.insert(0, '../libs')
from logger_utils import get_logger
from loading_utils import load_and_set_env_vars, load_config


# COMMAND ----------

client = mlflow.MlflowClient()
fs = FeatureStoreClient()
_logger = get_logger()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Pipeline & Config Params

# COMMAND ----------

# Set pipeline name
pipeline_name = 'setup'


# Load pipeline config 
pipeline_config = load_config(pipeline_name)

# Load env vars
env_vars = load_and_set_env_vars(env=dbutils.widgets.get('env'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Setup Pipeline Class 

# COMMAND ----------

class Setup:
    """
    Class for setting up the environment and performing cleanup tasks.
    """

    def __init__(self, config, env_vars):
        """
        Initialize Setup class with configuration and environment variables.
        
        Args:
            config (dict): Configuration parameters.
            env_vars (dict): Environment variables.
        """

        self.config = config
        self.env_vars = env_vars

    def _get_train_experiment_id(self):
        """
        Get the experiment ID for training.
        
        Returns:
            str or None: Experiment ID for training, or None if not found.
        """

        try:
            return self.env_vars['model_train_experiment_id']
        except KeyError:
            return None
        
    def _get_train_experiment_path(self):
        """
        Get the experiment path for training.
        
        Returns:
            str or None: Experiment path for training, or None if not found.
        """

        try:
            return self.env_vars['model_train_experiment_path']
        except KeyError:
            return None
        
    def _get_deploy_experiment_id(self):
        """
        Get the experiment ID for deployment.
        
        Returns:
            str or None: Experiment ID for deployment, or None if not found.
        """

        try:
            return self.env_vars['model_deploy_experiment_id']
        except KeyError:
            return None
        
    def _get_deploy_experiment_path(self):
        """
        Get the experiment path for deployment.
        
        Returns:
            str or None: Experiment path for deployment, or None if not found.
        """

        try:
            return self.env_vars['model_deploy_experiment_path']
        except KeyError:
            return None

    @staticmethod
    def _check_mlflow_model_registry_exists(model_name):
        """
        Check if a model exists in the MLflow Model Registry.
        
        Args:
            model_name (str): Name of the model to check.
        
        Returns:
            bool: True if the model exists, False otherwise.
        """

        try:
            client.get_registered_model(name=model_name)
            _logger.info(f'Mlflow Register {model_name} exists')
            return True
        except RestException:
            _logger.info(f'Mlflow Register {model_name} does not exists')
            return False

    @staticmethod
    def _archive_registered_model(model_name):
        """
        Archive all versions of a registered model in the MLflow Model Registry.
        
        Args:
            model_name (str): Name of the model to archive.
        """

        model = client.get_registered_model(name=model_name)
        latest_versions_list = model.latest_versions

        _logger.info(f'Mlflow model registry name: {model_name}')
        for model_version in latest_versions_list:
            if model_version.current_stage != 'Archived':
                _logger.info(f'Archiving model version: {model_version.version}')
                client.transition_model_version_stage(
                    name=model_name,
                    version=model_version.version,
                    stage='Archived'
                )

 
    def _delete_registered_model(self, model_name):
        """
        Delete a registered model from the MLflow Model Registry.
        
        Args:
            model_name (str): Name of the model to delete.
        """

        # Archive the model first
        self._archive_registered_model(model_name)  # Corrected method call

        # Delete the registered model
        client.delete_registered_model(name=model_name)

        # Log deletion of the model registry
        _logger.info(f"Deleted model from Mlflow registry: {model_name}")


    @staticmethod
    def _check_by_experiment_id(experiment_id):
        """
        Check if an experiment exists in MLflow Tracking using its ID.
        
        Args:
            experiment_id (str): ID of the experiment to check.
        
        Returns:
            bool: True if the experiment exists, False otherwise.
        """

        try:
            mlflow.get_experiment(experiment_id=experiment_id)
            _logger.info(f'MLflow Tracking experiment_id: {experiment_id} exists')
            return True
        except RestException:
            _logger.info(f'MLflow Tracking experiment_id: {experiment_id} DOES NOT exist')
            return False

    @staticmethod
    def _check_by_experiment_path(experiment_path):
        """
        Check if an experiment exists in MLflow Tracking using its path.
        
        Args:
            experiment_path (str): Path of the experiment to check.
        
        Returns:
            bool: True if the experiment exists, False otherwise.
        """

        experiment = mlflow.get_experiment_by_name(name=experiment_path)
        if experiment is not None:
            _logger.info(f'MLflow Tracking experiment_path: {experiment_path} exists')
            return True
        else:
            _logger.info(f'MLflow Tracking experiment_path: {experiment_path} DOES NOT exist')
            return False

    def _check_mlflow_experiments_exists(self):
        """
        Check the existence of MLflow experiments based on provided IDs or paths.
        
        Returns:
            dict: Dictionary indicating the existence of train and deploy experiments.
        """

        train_experiment_id = self._get_train_experiment_id()
        train_experiment_path = self._get_train_experiment_path()
        deploy_experiment_id = self._get_deploy_experiment_id()
        deploy_experiment_path = self._get_deploy_experiment_path()

        if train_experiment_id is not None:
            train_exp_exists = self._check_by_experiment_id(train_experiment_id)
        elif train_experiment_path is not None:
            train_exp_exists = self._check_by_experiment_path(train_experiment_path)
        else:
            raise RuntimeError('Either model_train_experiment_id or model_train_experiment_path should be passed in deployment.yml')

        if deploy_experiment_id is not None:
            deploy_exp_exists = self._check_by_experiment_id(deploy_experiment_id)
        elif deploy_experiment_path is not None:
            deploy_exp_exists = self._check_by_experiment_path(deploy_experiment_path)
        else:
            raise RuntimeError('Either model_train_experiment_id or model_train_experiment_path should be passed in deployment.yml')

        return {'train_exp_exists': train_exp_exists,
                'deploy_exp_exists': deploy_exp_exists}

    def _delete_mlflow_experiments(self, exp_exists_dict: dict):
        """
        Delete MLflow experiments based on provided existence dictionary.
        
        Args:
            exp_exists_dict (dict): Dictionary indicating the existence of experiments.
        """

        delete_experiments = [exp for exp, exists in exp_exists_dict.items() if exists == True]
        if len(delete_experiments) == 0:
            _logger.info(f'No existing experiments to delete')
        
        if 'train_exp_exists' in delete_experiments:
            if self.env_vars['model_train_experiment_path'] is not None:
                experiment = mlflow.get_experiment_by_name(name=self.env_vars['model_train_experiment_path'])
                mlflow.delete_experiment(experiment_id=experiment.experiment_id)
                _logger.info(f'Deleted existing experiment_path: {self.env_vars["model_train_experiment_path"]}')
            elif self.env_vars['model_train_experiment_id'] is not None:
                mlflow.delete_experiment(experiment_id=self.env_vars['model_train_experiment_id'])
                _logger.info(f'Deleted existing experiment_id: {self.env_vars["model_train_experiment_id"]}')
            else:
                raise RuntimeError('Either model_train_experiment_id or model_train_experiment_path should be passed '
                                   'in deployment.yml')

        if 'deploy_exp_exists' in delete_experiments:
            if self.env_vars['model_deploy_experiment_path'] is not None:
                experiment = mlflow.get_experiment_by_name(name=self.env_vars['model_deploy_experiment_path'])
                mlflow.delete_experiment(experiment_id=experiment.experiment_id)
                _logger.info(
                    f'Deleted existing experiment_path: {self.env_vars["model_deploy_experiment_path"]}')
            elif self.env_vars['model_deploy_experiment_id'] is not None:
                mlflow.delete_experiment(experiment_id=self.env_vars['model_deploy_experiment_id'])
                _logger.info(f'Deleted existing experiment_id: {self.env_vars["model_deploy_experiment_id"]}')

    @staticmethod
    def _check_labels_table(db_name, table_name):
        """
        Check if a table exists in the Spark SQL context.
        
        Args:
            db_name (str): Database name.
            table_name (str): Table name.
        
        Returns:
            bool: True if the table exists, False otherwise.
        """

        databases = [db.databaseName for db in spark.sql('show databases').collect()]
        tables = [
            f"{row['database']}.{row['tableName']}" #<schema>.<table> format
            for db_rows in [
                spark.sql(f'show tables in {db}').collect() for db in databases
            ] 
            for row in db_rows
        ]


        if f'{db_name}.{table_name}' in tables:
            _logger.info(f'Table exists: {db_name}.{table_name} True')
            return True

        else:
            _logger.info(f'Table does not exists: {db_name}.{table_name} False')
            return False

    @staticmethod
    def _delete_labels_table(db_name, table_name):
        """
        Delete a table from the Spark SQL context.
        
        Args:
            db_name (str): Database name.
            table_name (str): Table name.
        """

        _logger.info(f'Deleted labels table: {db_name}.{table_name}')
        return spark.sql(f'DROP TABLE IF EXISTS {db_name}.{table_name}') 

    def run(self):
        """
        Setup steps:
        * Delete Model Registry model if exists (archive any existing models)
        * Delete MLflow experiments if exists
        * Delete Feature Table if exists
        """

        _logger.info('==========Setup=========')
        _logger.info(f'Running setup pipeline in {self.env_vars["env"]} environment')

        if self.config['delete_model_registry']:
            _logger.info('Checking MLflow Model Registry...')
            model_name = self.env_vars['model_name']
            if self._check_mlflow_model_registry_exists(model_name):
                self._delete_registered_model(model_name)

        if self.config['delete_mlflow_experiments']:
            _logger.info('Checking MLflow Tracking...')
            exp_exists_dict = self._check_mlflow_experiments_exists()
            self._delete_mlflow_experiments(exp_exists_dict)

        
        if self.config['drop_labels_table']:
            _logger.info('Checking existing labels table...')
            labels_table_database_name = self.env_vars['reference_table_database_name']
            labels_table_name = self.env_vars['labels_table_name']

            if self._check_labels_table(labels_table_database_name, labels_table_name):
                self._delete_labels_table(labels_table_database_name, labels_table_name)

        _logger.info('==========Setup Complete=========')



# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC Run flow:
# MAGIC 1. Check if model registry exists
# MAGIC 2. Delete the model registry (archive models)
# MAGIC 3. Check if model experiments exists in train/deploy
# MAGIC 4. Delete the model experiments
# MAGIC 5. Check if a table containing the labels exists
# MAGIC 6. Delete the label table

# COMMAND ----------

setup_pipeline = Setup(config = pipeline_config, env_vars=env_vars)
setup_pipeline.run()

# COMMAND ----------


