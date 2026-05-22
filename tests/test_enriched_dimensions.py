import pytest
from datetime import datetime
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from src.transformations.customers import transform_enriched_customers
from src.transformations.products import transform_enriched_products


def test_transform_enriched_customers(spark):
    """
    Test transform_enriched_customers function:
    - Selects all customer columns
    - Preserves data types and values
    - Maintains ingestion_timestamp
    """
    input_data = [
        Row(
            customer_id="CUST-001",
            customer_name="John Doe",
            email="john@example.com",
            phone="555-1234",
            address="123 Main St",
            segment="Consumer",
            country="USA",
            city="New York",
            state="NY",
            postal_code="10001",
            region="East",
            ingestion_timestamp=datetime(2024, 1, 10, 12, 0, 0),
            extra_column="should_be_removed"  # Extra column not in select
        )
    ]
    input_df = spark.createDataFrame(input_data)
    
    expected_data = [
        Row(
            customer_id="CUST-001",
            customer_name="John Doe",
            email="john@example.com",
            phone="555-1234",
            address="123 Main St",
            segment="Consumer",
            country="USA",
            city="New York",
            state="NY",
            postal_code="10001",
            region="East",
            ingestion_timestamp=datetime(2024, 1, 10, 12, 0, 0)
        )
    ]
    expected_df = spark.createDataFrame(expected_data)
    
    result_df = transform_enriched_customers(input_df)
    
    # Assert exact match
    assert_df_equality(result_df, expected_df, ignore_nullable=True)
    # Verify extra column is not present
    assert "extra_column" not in result_df.columns


def test_transform_enriched_customers_multiple_records(spark):
    """
    Test with multiple customer records
    """
    input_data = [
        Row(
            customer_id="CUST-001",
            customer_name="John Doe",
            email="john@example.com",
            phone="555-1234",
            address="123 Main St",
            segment="Consumer",
            country="USA",
            city="New York",
            state="NY",
            postal_code="10001",
            region="East",
            ingestion_timestamp=datetime(2024, 1, 10, 12, 0, 0)
        ),
        Row(
            customer_id="CUST-002",
            customer_name="Jane Smith",
            email="jane@example.com",
            phone="555-5678",
            address="456 Oak Ave",
            segment="Corporate",
            country="Canada",
            city="Toronto",
            state="ON",
            postal_code="M5H 2N2",
            region="Central",
            ingestion_timestamp=datetime(2024, 1, 11, 12, 0, 0)
        )
    ]
    input_df = spark.createDataFrame(input_data)
    expected_df = spark.createDataFrame(input_data)
    
    result_df = transform_enriched_customers(input_df)
    
    assert result_df.count() == 2
    assert_df_equality(result_df, expected_df, ignore_nullable=True)


def test_transform_enriched_customers_with_nulls(spark):
    """
    Test that null values are preserved
    """
    # Define schema explicitly for null columns
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("region", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    
    # Use tuple format instead of Row to avoid type inference issues
    input_data = [
        (
            "CUST-003",
            "Bob Johnson",
            None,  # Null email
            None,  # Null phone
            "789 Pine Rd",
            "Home Office",
            "USA",
            "Los Angeles",
            "CA",
            "90001",
            "West",
            datetime(2024, 1, 12, 12, 0, 0)
        )
    ]
    input_df = spark.createDataFrame(input_data, schema=schema)
    expected_df = spark.createDataFrame(input_data, schema=schema)
    
    result_df = transform_enriched_customers(input_df)
    
    assert_df_equality(result_df, expected_df, ignore_nullable=True)


def test_transform_enriched_products(spark):
    """
    Test transform_enriched_products function:
    - Selects all product columns
    - Preserves data types and values
    - Maintains ingestion_timestamp
    """
    input_data = [
        Row(
            product_id="PROD-001",
            product_name="iPhone 15",
            category="Electronics",
            sub_category="Phones",
            state="Available",
            price_per_product=999.99,
            ingestion_timestamp=datetime(2024, 1, 10, 12, 0, 0),
            extra_field="should_be_removed"  # Extra field not in select
        )
    ]
    input_df = spark.createDataFrame(input_data)
    
    expected_data = [
        Row(
            product_id="PROD-001",
            product_name="iPhone 15",
            category="Electronics",
            sub_category="Phones",
            state="Available",
            price_per_product=999.99,
            ingestion_timestamp=datetime(2024, 1, 10, 12, 0, 0)
        )
    ]
    expected_df = spark.createDataFrame(expected_data)
    
    result_df = transform_enriched_products(input_df)
    
    # Assert exact match
    assert_df_equality(result_df, expected_df, ignore_nullable=True)
    # Verify extra field is not present
    assert "extra_field" not in result_df.columns


def test_transform_enriched_products_multiple_records(spark):
    """
    Test with multiple product records
    """
    input_data = [
        Row(
            product_id="PROD-001",
            product_name="iPhone 15",
            category="Electronics",
            sub_category="Phones",
            state="Available",
            price_per_product=999.99,
            ingestion_timestamp=datetime(2024, 1, 10, 12, 0, 0)
        ),
        Row(
            product_id="PROD-002",
            product_name="Office Chair",
            category="Furniture",
            sub_category="Chairs",
            state="Available",
            price_per_product=299.99,
            ingestion_timestamp=datetime(2024, 1, 11, 12, 0, 0)
        ),
        Row(
            product_id="PROD-003",
            product_name="Desk Lamp",
            category="Office Supplies",
            sub_category="Lighting",
            state="Out of Stock",
            price_per_product=49.99,
            ingestion_timestamp=datetime(2024, 1, 12, 12, 0, 0)
        )
    ]
    input_df = spark.createDataFrame(input_data)
    expected_df = spark.createDataFrame(input_data)
    
    result_df = transform_enriched_products(input_df)
    
    assert result_df.count() == 3
    assert_df_equality(result_df, expected_df, ignore_nullable=True)


def test_transform_enriched_products_with_nulls(spark):
    """
    Test that null values are preserved
    """
    # Define schema explicitly for null columns
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("state", StringType(), True),
        StructField("price_per_product", DoubleType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    
    # Use tuple format instead of Row to avoid type inference issues
    input_data = [
        (
            "PROD-004",
            "Mystery Box",
            None,  # Null category
            None,  # Null sub_category
            "Available",
            19.99,
            datetime(2024, 1, 13, 12, 0, 0)
        )
    ]
    input_df = spark.createDataFrame(input_data, schema=schema)
    expected_df = spark.createDataFrame(input_data, schema=schema)
    
    result_df = transform_enriched_products(input_df)
    
    assert_df_equality(result_df, expected_df, ignore_nullable=True)
