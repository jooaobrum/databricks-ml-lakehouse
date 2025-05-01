from datetime import datetime

import great_expectations as gx
import numpy as np
import pandas as pd
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.types.base import (
    DataContextConfig,
    FilesystemStoreBackendDefaults,
)
from ydata_profiling import ProfileReport

yaml = YAMLHandler()


class DataQuality:
    def __init__(self, dataset_name: str, dataframe: pd.DataFrame, expectations_as_list: list):
        """Initialize the DataQuality class."""
        self.dataset_name = dataset_name
        self.dataframe = dataframe
        self.expectations_as_list = expectations_as_list
        self.save_directory_profiling = f"/dbfs/mnt/landing_zone/fs_validation/pandas_profiling/{dataset_name}_{datetime.strftime(datetime.now(), '%Y%m%d')}"
        self.save_directory_expectations = "/dbfs/mnt/landing_zone/fs_validation/great_expectations/"
        self.data_context = self._initialize_data_context()

        # Configure datasource to be used
        self._configure_datasource()

        # Create the batch request
        self.batch_request = self._create_batch_request()

        # Build the expectation suite
        self.expectation_suite_name = f"{dataset_name}_expectations_expectation_suite"
        self._build_expectation_suite()

        # Create a checkpoint
        self.checkpoint_name = f"{dataset_name}_expectations_checkpoint"
        self.checkpoint = self._create_checkpoint()

        # Create a report
        self.report = ProfileReport(
            self.dataframe,
            title=f"{dataset_name}_profiling",
            infer_dtypes=False,
            interactions=None,
            missing_diagrams=None,
            correlations={
                "auto": {"calculate": False},
                "pearson": {"calculate": False},
                "spearman": {"calculate": False},
            },
        )
        self.report.to_file(self.save_directory_profiling)

    def _initialize_data_context(self):
        """Initialize Great Expectations data context."""
        data_context_configuration = DataContextConfig(
            store_backend_defaults=FilesystemStoreBackendDefaults(root_directory=self.save_directory_expectations)
        )
        return gx.get_context(project_config=data_context_configuration)

    def _configure_datasource(self):
        """Configure datasource for Great Expectations."""
        datasource_name = f"{self.dataset_name}_data_source"
        dataconnector_name = f"{self.dataset_name}_data_connector"
        datasource_config = {
            "name": datasource_name,
            "class_name": "Datasource",
            "execution_engine": {"class_name": "PandasExecutionEngine"},
            "data_connectors": {
                dataconnector_name: {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["batch_id", "batch_datetime"],
                }
            },
        }
        self.data_context.add_datasource(**datasource_config)

    def _create_batch_request(self):
        """Create a RuntimeBatchRequest."""
        return RuntimeBatchRequest(
            datasource_name=f"{self.dataset_name}_data_source",
            data_connector_name=f"{self.dataset_name}_data_connector",
            data_asset_name=self.dataset_name,
            runtime_parameters={"batch_data": self.dataframe},
            batch_identifiers={
                "batch_id": f"{self.dataset_name}_{datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')}",  # Modified timestamp format
            },
        )

    def _build_expectation_suite(self):
        """Build the expectation suite for Great Expectations."""
        expectations = [ExpectationConfiguration(**item) for item in self.expectations_as_list]
        expectation_suite = ExpectationSuite(expectation_suite_name=self.expectation_suite_name)
        expectation_suite.add_expectation_configurations(expectations)
        self.data_context.add_or_update_expectation_suite(expectation_suite=expectation_suite)

    def _create_checkpoint(self):
        """Create a checkpoint for Great Expectations."""
        checkpoint_config = {
            "name": self.checkpoint_name,
            "config_version": 1.0,
            "class_name": "SimpleCheckpoint",
            "run_name_template": f"{self.dataset_name}_%Y%m%d",
        }
        return self.data_context.add_or_update_checkpoint(**checkpoint_config)

    def run_validations(self):
        """Run validations using Great Expectations checkpoint."""
        checkpoint_result = self.data_context.run_checkpoint(
            checkpoint_name=self.checkpoint_name,
            validations=[{"batch_request": self.batch_request, "expectation_suite_name": self.expectation_suite_name}],
        )

        self.data_context.build_data_docs()

        return checkpoint_result

    def extract_expectations_statistics(self, result):
        """Extract and format validation statistics."""
        validation_statistics = result.get_statistics()["validation_statistics"]

        # Extract statistics and reset index
        ge_statistics = pd.DataFrame(validation_statistics).reset_index()

        # Rename columns
        ge_statistics.columns = ["expectations", "statistics"]

        # Extract run_id details
        run_id_info = result["run_id"].to_json_dict()

        # Add batch_name and batch_time columns
        ge_statistics["batch_name"] = run_id_info["run_name"]
        ge_statistics["batch_time"] = run_id_info["run_time"].split("T")[0]

        # Rearrange columns
        ge_statistics = ge_statistics[["batch_name", "batch_time", "expectations", "statistics"]]

        return ge_statistics

    def generate_profiling_dataframe(self, result):
        """Generate a profiling DataFrame based on ydata_profiling report."""
        att_keys = self.report.description_set["variables"].keys()
        attributes = self.report.description_set["variables"]

        df_profiling = pd.DataFrame(
            columns=(
                "batch_name",
                "batch_time",
                "feature",
                "missing",
                "percentage_missing",
                "distinct_values",
                "percentage_distinct",
                "mean",
                "variance",
                "max",
                "min",
            )
        )

        for i in att_keys:
            x = attributes[i]
            is_categorical = x["type"] == "Categorical"

            df_profiling = df_profiling.append(
                {
                    "batch_name": result["run_id"].to_json_dict()["run_name"],
                    "batch_time": result["run_id"].to_json_dict()["run_time"].split("T")[0],
                    "feature": i,
                    "missing": x["n_missing"],
                    "percentage_missing": x["p_missing"] * 100,
                    "distinct_values": x["n_distinct"],
                    "percentage_distinct": x["p_distinct"] * 100,
                    "mean": x["mean"] if not is_categorical else np.nan,
                    "variance": x["variance"] if not is_categorical else np.nan,
                    "max": x["max"] if not is_categorical else np.nan,
                    "min": x["min"] if not is_categorical else np.nan,
                },
                ignore_index=True,
            )

        return df_profiling
