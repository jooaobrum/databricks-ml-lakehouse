import os
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict

import pandas as pd
from databricks.sdk.runtime import *
from pyspark.sql import DataFrame, Row
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

sys.path.insert(0, "../src")  # Set path to src instead of src/ml_pipelines
from ml_pipelines.components.model_training import ModelTrain, ModelTrainPipeline

# Define the path to the YAML file
yaml_file_path = "../src/ml_pipelines/cfg/model_training_pipeline.yaml"


@dataclass
class MlflowTrackingCfg:
    """
    Configuration for MLflow tracking.
    """

    run_name: str
    model_train_experiment_path: str
    model_name: str


@dataclass
class FeatureStoreTableCfg:
    """
    Configuration for Feature Store table.
    """

    query_features: str


@dataclass
class LabelTableCfg:
    """
    Configuration for Label Table.
    """

    query_target: str
    target: str


@dataclass
class ModelTrainConfig:
    """
    Configuration class to hold parameters for model training.
    """

    mlflow_tracking_cfg: MlflowTrackingCfg
    feature_store_table_cfg: FeatureStoreTableCfg
    labels_table_cfg: LabelTableCfg
    pipeline_params: Dict
    model_params: Dict
    training_params: Dict


def test_create_preprocessor():
    # Define pipeline parameters
    pipeline_params = {
        "numerical_features": [
            "days_to_estimation,",
            "days_after_carrier,",
            "days_to_shipping_limit,",
            "shipping_status,",
        ],
        "categorical_features": ["delivery_status"],
    }

    # Create the preprocessor
    preprocessor = ModelTrainPipeline.create_preprocessor(pipeline_params)

    # Check if the preprocessor is an instance of ColumnTransformer
    assert isinstance(preprocessor, ColumnTransformer), "Output is not a ColumnTransformer"

    # Check if the numerical transformer is set up correctly
    num_transformer = preprocessor.transformers[0][1]
    assert isinstance(num_transformer, Pipeline), "Numerical transformer is not a Pipeline"

    # Check if the categorical transformer is set up correctly
    cat_transformer = preprocessor.transformers[1][1]
    assert isinstance(cat_transformer, Pipeline), "Categorical transformer is not a Pipeline"

    # Check if the remainder option is set to 'passthrough'
    assert preprocessor.remainder == "passthrough", "Remainder is not set to 'passthrough'"


def test_create_train_pipeline():
    # Define pipeline parameters
    pipeline_params = {
        "numerical_features": [
            "days_to_estimation,",
            "days_after_carrier,",
            "days_to_shipping_limit,",
            "shipping_status,",
        ],
        "categorical_features": ["delivery_status"],
    }
    model_params = {"C": 1.0, "solver": "liblinear"}

    # Create the train pipeline
    pipeline = ModelTrainPipeline.create_train_pipeline(pipeline_params, model_params)

    # Check if the returned object is a Pipeline
    assert isinstance(pipeline, Pipeline), "Output is not a Pipeline"


def test_read_query():
    # Sample query content
    mock_query_content = "SELECT * FROM table"

    # Create a temporary file with the query content
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
        temp_file.write(mock_query_content)
        temp_path = temp_file.name  # Get the file path

    # Sample configuration and mock data
    mock_cfg = {
        "labels_table_cfg": {"query_target": "query_target.sql"},
        "feature_store_table_cfg": {"query_features": "query_features.sql"},
        "training_params": {"dt_start": "2023-01-01", "dt_stop": "2023-12-31"},
    }

    # Now use the path in read_query
    result = ModelTrain(mock_cfg).read_query(temp_path)

    # Assert the result
    assert result == mock_query_content, "The query content read is incorrect"


def test_get_fs_training_set():
    # Define sample DataFrames
    labels_data = [{"order_id": "1", "label": "yes", "test": "A"}, {"order_id": "2", "label": "no"}]
    features_data = [{"order_id": "1", "feature": "A"}, {"order_id": "2", "feature": "B"}]

    labels_df = spark.createDataFrame(labels_data)
    features_df = spark.createDataFrame(features_data)

    fs_training_set = features_df.join(labels_df, "order_id", "inner")

    # Check that the result is a DataFrame
    assert isinstance(fs_training_set, DataFrame), "Output is not a DataFrame"

    # Collect the result and verify content
    result_data = fs_training_set.collect()
    expected_data = [
        Row(order_id="1", feature="A", label="yes", test="A"),
        Row(order_id="2", feature="B", label="no", test=None),
    ]

    assert result_data == expected_data, "The joined DataFrame content is incorrect"


def test_yaml_file_exists():
    """Check if the specified YAML configuration file exists."""
    assert os.path.exists(yaml_file_path), f"YAML file not found at {yaml_file_path}"


def test_create_train_test_split():
    # Create a fake configuration with matching values
    mlflow_tracking_cfg = MlflowTrackingCfg(
        run_name="test_run", model_train_experiment_path="/fake/path/to/experiment", model_name="test_model"
    )
    feature_store_table_cfg = FeatureStoreTableCfg(query_features="SELECT * FROM features_table")
    labels_table_cfg = LabelTableCfg(query_target="SELECT * FROM labels_table", target="target")
    pipeline_params = {
        "numerical_features": ["num_feature"],
        "categorical_features": ["cat_feature"],
        "test_size": 0.3,
        "random_state": 42,
    }
    model_params = {"C": 1.0, "solver": "liblinear"}
    training_params = {"dt_start": "2023-01-01", "dt_stop": "2023-12-31"}

    # Create the main configuration object
    cfg = ModelTrainConfig(
        mlflow_tracking_cfg=mlflow_tracking_cfg,
        feature_store_table_cfg=feature_store_table_cfg,
        labels_table_cfg=labels_table_cfg,
        pipeline_params=pipeline_params,
        model_params=model_params,
        training_params=training_params,
    )

    # Sample data creation
    data = [
        Row(
            order_id=i,
            num_feature=i * 1.0,
            cat_feature=str(i % 2),
            target=i % 2,
            dt_ref=(datetime(2023, 1, 1) + timedelta(days=i)).strftime("%Y-%m-%d"),
        )
        for i in range(50)
    ]

    fs_training_set = spark.createDataFrame(data)

    # Instantiate ModelTrainer and set the config
    trainer = ModelTrain(cfg)

    # Call the create_train_test_split function
    X_train, X_test, y_train, y_test, oot = trainer.create_train_test_split(fs_training_set)

    # Convert OOT to a DataFrame for easier testing
    oot_df = pd.DataFrame(oot)

    # Check that the splits are not empty
    assert len(X_train) > 0, "X_train should not be empty"
    assert len(X_test) > 0, "X_test should not be empty"
    assert len(y_train) > 0, "y_train should not be empty"
    assert len(y_test) > 0, "y_test should not be empty"
