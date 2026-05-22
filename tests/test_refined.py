import pytest
from datetime import datetime, date
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, IntegerType, DoubleType, TimestampType
from src.transformations.refined import (
    transform_refined_orders,
    transform_refined_customers,
    transform_refined_products
)


def test_transform_refined_orders(spark):
    """
    Test transform_refined_orders function:
    - Trims whitespace from string columns
    - Converts dates from d/M/yyyy format
    - Casts numeric fields to appropriate types
    - Preserves ingestion_timestamp
    """
    # Input data
    input_data = [
        Row(
            row_id=" 1 ",
            order_id=" ORD-001 ",
            order_date=" 1/1/2024 ",
            ship_date=" 5/1/2024 ",
            ship_mode=" Standard ",
            customer_id=" CUST-001 ",
            product_id=" PROD-001 ",
            quantity=" 2 ",
            price=" 99.99 ",
            discount=" 0.1 ",
            profit=" 15.50 ",
            ingestion_timestamp=datetime(2024, 1, 10, 12, 0, 0)
        )
    ]
    input_df = spark.createDataFrame(input_data)
    
    # Execute transformation
    result_df = transform_refined_orders(input_df)
    
    # Create expected DataFrame with proper schema - use LongType for quantity
    expected_schema = StructType([
        StructField("row_id", LongType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("ship_date", DateType(), True),
        StructField("ship_mode", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("profit", DoubleType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    
    expected_data = [
        (1, "ORD-001", date(2024, 1, 1), date(2024, 1, 5), "Standard",
         "CUST-001", "PROD-001", 2, 99.99, 0.1, 15.50, datetime(2024, 1, 10, 12, 0, 0))
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_column_order=True)


def test_transform_refined_orders_with_nulls(spark):
    """
    Test transform_refined_orders handles invalid data gracefully:
    - Invalid numeric strings become null with try_cast
    - Invalid dates become null with to_date
    """
    input_data = [
        Row(
            row_id="invalid",
            order_id="ORD-002",
            order_date="31/12/2024",  # Valid date
            ship_date="31/12/2024",
            ship_mode="Express",
            customer_id="CUST-002",
            product_id="PROD-002",
            quantity="abc",  # Invalid
            price="xyz",  # Invalid
            discount="0.05",
            profit="10.00",
            ingestion_timestamp=datetime(2024, 1, 10, 12, 0, 0)
        )
    ]
    input_df = spark.createDataFrame(input_data)
    
    result_df = transform_refined_orders(input_df)
    
    # Create expected DataFrame - use LongType for quantity
    expected_schema = StructType([
        StructField("row_id", LongType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("ship_date", DateType(), True),
        StructField("ship_mode", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("profit", DoubleType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    
    expected_data = [
        (None, "ORD-002", date(2024, 12, 31), date(2024, 12, 31), "Express",
         "CUST-002", "PROD-002", None, None, 0.05, 10.0, datetime(2024, 1, 10, 12, 0, 0))
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_column_order=True)


def test_transform_refined_customers(spark):
    """
    Test transform_refined_customers function:
    - Trims whitespace from all string columns
    - Preserves all customer fields
    - Maintains ingestion_timestamp
    """
    input_data = [
        Row(
            customer_id=" CUST-001 ",
            customer_name=" John Doe ",
            email=" john@example.com ",
            phone=" 555-1234 ",
            address=" 123 Main St ",
            segment=" Consumer ",
            country=" USA ",
            city=" New York ",
            state=" NY ",
            postal_code=" 10001 ",
            region=" East ",
            ingestion_timestamp=datetime(2024, 1, 10, 12, 0, 0)
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
    
    result_df = transform_refined_customers(input_df)
    
    assert_df_equality(result_df, expected_df, ignore_nullable=True)


def test_transform_refined_products(spark):
    """
    Test transform_refined_products function:
    - Trims whitespace from string columns
    - Casts price_per_product to DOUBLE
    - Preserves all product fields
    """
    input_data = [
        Row(
            product_id=" PROD-001 ",
            category=" Electronics ",
            sub_category=" Phones ",
            product_name=" iPhone 15 ",
            state=" Available ",
            price_per_product=" 999.99 ",
            ingestion_timestamp=datetime(2024, 1, 10, 12, 0, 0)
        )
    ]
    input_df = spark.createDataFrame(input_data)
    
    expected_data = [
        Row(
            product_id="PROD-001",
            category="Electronics",
            sub_category="Phones",
            product_name="iPhone 15",
            state="Available",
            price_per_product=999.99,
            ingestion_timestamp=datetime(2024, 1, 10, 12, 0, 0)
        )
    ]
    expected_df = spark.createDataFrame(expected_data)
    
    result_df = transform_refined_products(input_df)
    
    assert_df_equality(result_df, expected_df, ignore_nullable=True)


def test_transform_refined_products_invalid_price(spark):
    """
    Test that invalid price strings become null with try_cast
    """
    input_data = [
        Row(
            product_id="PROD-002",
            category="Furniture",
            sub_category="Chairs",
            product_name="Office Chair",
            state="Available",
            price_per_product="invalid_price",
            ingestion_timestamp=datetime(2024, 1, 10, 12, 0, 0)
        )
    ]
    input_df = spark.createDataFrame(input_data)
    
    result_df = transform_refined_products(input_df)
    
    # Create expected DataFrame with null price
    expected_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("price_per_product", DoubleType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    
    expected_data = [
        ("PROD-002", "Furniture", "Chairs", "Office Chair", "Available", None, datetime(2024, 1, 10, 12, 0, 0))
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_column_order=True)
