import pytest
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime
import sys
sys.path.insert(0, '../src/ingestions/feature_store/olist')
from components.feature_creator import FeatureCreator, FeatureCreatorConfig

# Sample configuration for testing
config = FeatureCreatorConfig(
    ref_name="test_ref",
    db_name="test_db",
    table_name="test_table_{task_key}",
    fs_path="test_fs_path",
    base_query_path="test_query.sql",  # Mocked file path
    task_key="test_task",
    dt_start="2022-01-01",
    dt_stop="2022-01-05",
    step=1
)

# Fixture to initialize the FeatureCreator instance with sample config
@pytest.fixture
def feature_creator():
    return FeatureCreator(config)

def test_generate_dates(feature_creator):
    """
    Test the generate_dates method to ensure it produces the correct date range.
    """
    dates = feature_creator.generate_dates()
    expected_dates = ["2022-01-01", "2022-01-02", "2022-01-03", "2022-01-04", "2022-01-05"]
    assert dates == expected_dates, "generate_dates did not produce the expected date range."


@patch("builtins.open", new_callable=mock_open, read_data="SELECT * FROM table WHERE date = '{dt_ingestion}'")
def test_read_transf_query(mock_open, feature_creator):
    """
    Test the read_transf_query method to ensure it reads the query file correctly.
    """
    query = feature_creator.read_transf_query()
    assert query == "SELECT * FROM table WHERE date = '{dt_ingestion}'", "read_transf_query did not read the query file correctly."


@patch("components.feature_creator.spark")
@patch("builtins.open", new_callable=mock_open, read_data="SELECT * FROM table WHERE date = '{dt_ingestion}'")
def test_create_feature_table(mock_open, mock_spark, feature_creator):
    """
    Test the create_feature_table method with mocked Spark SQL and write operations.
    """
    # Mock spark.sql and tableExists
    mock_spark.sql.return_value = MagicMock()
    mock_spark.catalog.tableExists.return_value = False

    feature_creator.create_feature_table()
    
    # Assert spark.sql was called for each date in the date range
    expected_dates = feature_creator.generate_dates()
    for date in expected_dates:
        mock_spark.sql.assert_any_call(f"SELECT * FROM table WHERE date = '{date}'")
    
    # Assert that saveAsTable was called to create the table
    full_table_name = f"{config.db_name}.test_table_{config.task_key}"
    mock_spark.sql.return_value.write.format().mode().option().saveAsTable.assert_called_with(full_table_name)


@patch("components.feature_creator.spark")
@patch("builtins.open", new_callable=mock_open, read_data="SELECT * FROM table WHERE date = '{dt_ingestion}'")
def test_create_feature_table_append(mock_open, mock_spark, feature_creator):
    """
    Test the create_feature_table method when the table already exists, 
    checking that it appends data instead of overwriting.
    """
    # Mock spark.sql and tableExists to simulate existing table
    mock_spark.sql.return_value = MagicMock()
    mock_spark.catalog.tableExists.return_value = True

    feature_creator.create_feature_table()
    
    # Check that append mode is used for existing table
    mock_spark.sql.return_value.write.format().mode.assert_called_with("append")


def test_run(feature_creator):
    """
    Test the run method to ensure it triggers the feature creation process.
    """
    with patch.object(feature_creator, 'create_feature_table') as mock_create_feature_table:
        feature_creator.run()
        mock_create_feature_table.assert_called_once()
