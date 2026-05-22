import pytest
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.utils.dqx_utils import load_dq_rules, apply_dq_checks


def test_load_dq_rules():
    """
    Test load_dq_rules function:
    - Reads YAML file correctly
    - Returns parsed YAML content as dictionary
    """
    # Create a temporary YAML file
    yaml_content = """
checks:
  - column: customer_id
    rule: not_null
  - column: email
    rule: email_format
    """
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_file:
        temp_file.write(yaml_content)
        temp_file_path = temp_file.name
    
    try:
        # Load the rules
        rules = load_dq_rules(temp_file_path)
        
        # Assert structure
        assert 'checks' in rules
        assert len(rules['checks']) == 2
        assert rules['checks'][0]['column'] == 'customer_id'
        assert rules['checks'][0]['rule'] == 'not_null'
        assert rules['checks'][1]['column'] == 'email'
        assert rules['checks'][1]['rule'] == 'email_format'
    finally:
        # Clean up
        os.unlink(temp_file_path)


def test_load_dq_rules_empty_file():
    """
    Test load_dq_rules with an empty YAML file
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_file:
        temp_file.write("")  # Empty file
        temp_file_path = temp_file.name
    
    try:
        rules = load_dq_rules(temp_file_path)
        assert rules is None  # Empty YAML returns None
    finally:
        os.unlink(temp_file_path)


def test_load_dq_rules_file_not_found():
    """
    Test that load_dq_rules raises FileNotFoundError for non-existent file
    """
    with pytest.raises(FileNotFoundError):
        load_dq_rules("/non/existent/path/rules.yaml")


@patch('src.utils.dqx_utils.WorkspaceClient')
@patch('src.utils.dqx_utils.DQEngine')
def test_apply_dq_checks_basic(mock_dq_engine_class, mock_workspace_client, spark):
    """
    Test apply_dq_checks function:
    - Creates DQEngine with WorkspaceClient
    - Loads rules from file
    - Applies checks and splits data into valid and quarantine DataFrames
    """
    # Create test DataFrame
    input_data = [
        Row(customer_id="CUST-001", email="valid@example.com", amount=100.0),
        Row(customer_id="CUST-002", email="invalid-email", amount=50.0),
        Row(customer_id=None, email="another@example.com", amount=75.0)
    ]
    input_df = spark.createDataFrame(input_data)
    
    # Mock valid and quarantine DataFrames
    valid_data = [Row(customer_id="CUST-001", email="valid@example.com", amount=100.0)]
    quarantine_data = [
        Row(customer_id="CUST-002", email="invalid-email", amount=50.0),
        Row(customer_id=None, email="another@example.com", amount=75.0)
    ]
    valid_df = spark.createDataFrame(valid_data)
    quarantine_df = spark.createDataFrame(quarantine_data)
    
    # Mock DQEngine instance
    mock_engine_instance = MagicMock()
    mock_engine_instance.apply_checks_by_metadata_and_split.return_value = (valid_df, quarantine_df)
    mock_dq_engine_class.return_value = mock_engine_instance
    
    # Create temporary rules file
    yaml_content = """
checks:
  - column: customer_id
    rule: not_null
  - column: email
    rule: email_format
    """
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_file:
        temp_file.write(yaml_content)
        temp_file_path = temp_file.name
    
    try:
        # Apply DQ checks
        result_valid_df, result_quarantine_df = apply_dq_checks(input_df, temp_file_path)
        
        # Assert WorkspaceClient was created
        mock_workspace_client.assert_called_once()
        
        # Assert DQEngine was created with WorkspaceClient
        mock_dq_engine_class.assert_called_once()
        
        # Assert apply_checks_by_metadata_and_split was called
        mock_engine_instance.apply_checks_by_metadata_and_split.assert_called_once()
        
        # Verify the call arguments
        call_args = mock_engine_instance.apply_checks_by_metadata_and_split.call_args
        assert 'df' in call_args.kwargs
        assert 'checks' in call_args.kwargs
        
        # Verify returned DataFrames
        assert result_valid_df.count() == 1
        assert result_quarantine_df.count() == 2
        
        # Verify content
        valid_row = result_valid_df.collect()[0]
        assert valid_row.customer_id == "CUST-001"
        assert valid_row.email == "valid@example.com"
        
    finally:
        os.unlink(temp_file_path)


@patch('src.utils.dqx_utils.WorkspaceClient')
@patch('src.utils.dqx_utils.DQEngine')
def test_apply_dq_checks_all_valid(mock_dq_engine_class, mock_workspace_client, spark):
    """
    Test when all records pass DQ checks
    """
    input_data = [
        Row(customer_id="CUST-001", email="valid1@example.com", amount=100.0),
        Row(customer_id="CUST-002", email="valid2@example.com", amount=50.0)
    ]
    input_df = spark.createDataFrame(input_data)
    
    # All records are valid
    valid_df = input_df
    quarantine_df = spark.createDataFrame([], input_df.schema)
    
    mock_engine_instance = MagicMock()
    mock_engine_instance.apply_checks_by_metadata_and_split.return_value = (valid_df, quarantine_df)
    mock_dq_engine_class.return_value = mock_engine_instance
    
    yaml_content = "checks: []"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_file:
        temp_file.write(yaml_content)
        temp_file_path = temp_file.name
    
    try:
        result_valid_df, result_quarantine_df = apply_dq_checks(input_df, temp_file_path)
        
        assert result_valid_df.count() == 2
        assert result_quarantine_df.count() == 0
    finally:
        os.unlink(temp_file_path)


@patch('src.utils.dqx_utils.WorkspaceClient')
@patch('src.utils.dqx_utils.DQEngine')
def test_apply_dq_checks_all_invalid(mock_dq_engine_class, mock_workspace_client, spark):
    """
    Test when all records fail DQ checks
    """
    # Define schema explicitly for null customer_id columns
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("amount", DoubleType(), True)
    ])
    
    # Use tuple format instead of Row to avoid type inference issues
    input_data = [
        (None, "invalid1", 100.0),
        (None, "invalid2", 50.0)
    ]
    input_df = spark.createDataFrame(input_data, schema=schema)
    
    # All records are invalid
    valid_df = spark.createDataFrame([], schema)
    quarantine_df = input_df
    
    mock_engine_instance = MagicMock()
    mock_engine_instance.apply_checks_by_metadata_and_split.return_value = (valid_df, quarantine_df)
    mock_dq_engine_class.return_value = mock_engine_instance
    
    yaml_content = """
checks:
  - column: customer_id
    rule: not_null
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_file:
        temp_file.write(yaml_content)
        temp_file_path = temp_file.name
    
    try:
        result_valid_df, result_quarantine_df = apply_dq_checks(input_df, temp_file_path)
        
        assert result_valid_df.count() == 0
        assert result_quarantine_df.count() == 2
    finally:
        os.unlink(temp_file_path)
