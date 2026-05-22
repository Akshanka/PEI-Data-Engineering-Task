import pytest
from datetime import datetime, date
from decimal import Decimal
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, IntegerType, DoubleType, TimestampType, DecimalType
from src.transformations.orders import transform_enriched_orders


def test_transform_enriched_orders_with_full_join(spark):
    """
    Test transform_enriched_orders with complete customer and product data:
    - Performs left joins with customers and products
    - Extracts order_year from order_date
    - Rounds profit to 2 decimal places and casts to decimal(18,2)
    - Selects all required columns from joined data
    """
    # Orders data
    orders_data = [
        Row(
            row_id=1,
            order_id="ORD-001",
            order_date=date(2024, 3, 15),
            ship_date=date(2024, 3, 20),
            ship_mode="Standard",
            customer_id="CUST-001",
            product_id="PROD-001",
            quantity=2,
            price=99.99,
            discount=0.1,
            profit=15.555,  # Will be rounded
            ingestion_timestamp=datetime(2024, 3, 15, 10, 0, 0)
        )
    ]
    orders_df = spark.createDataFrame(orders_data)
    
    # Customers data
    customers_data = [
        Row(
            customer_id="CUST-001",
            customer_name="John Doe",
            country="USA",
            email="john@example.com",
            phone="555-1234",
            address="123 Main St",
            segment="Consumer",
            city="New York",
            state="NY",
            postal_code="10001",
            region="East"
        )
    ]
    customers_df = spark.createDataFrame(customers_data)
    
    # Products data
    products_data = [
        Row(
            product_id="PROD-001",
            product_name="iPhone 15",
            category="Electronics",
            sub_category="Phones",
            state="Available",
            price_per_product=999.99
        )
    ]
    products_df = spark.createDataFrame(products_data)
    
    # Execute transformation
    result_df = transform_enriched_orders(orders_df, customers_df, products_df)
    
    # Create expected DataFrame with proper schema - use LongType for order_year
    expected_schema = StructType([
        StructField("row_id", LongType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("ship_date", DateType(), True),
        StructField("ship_mode", StringType(), True),
        StructField("order_year", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_sub_category", StringType(), True),
        StructField("quantity", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("profit", DecimalType(18, 2), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    
    expected_data = [
        (1, "ORD-001", date(2024, 3, 15), date(2024, 3, 20), "Standard", 2024,
         "CUST-001", "John Doe", "USA", "PROD-001", "iPhone 15", "Electronics", "Phones",
         2, 99.99, 0.1, Decimal("15.56"), datetime(2024, 3, 15, 10, 0, 0))
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_column_order=True)


def test_transform_enriched_orders_with_missing_customer(spark):
    """
    Test left join behavior when customer doesn't exist:
    - Customer fields should be null
    - Order and product fields should be preserved
    """
    orders_data = [
        Row(
            row_id=2,
            order_id="ORD-002",
            order_date=date(2024, 4, 10),
            ship_date=date(2024, 4, 15),
            ship_mode="Express",
            customer_id="CUST-999",  # Non-existent
            product_id="PROD-001",
            quantity=1,
            price=50.00,
            discount=0.0,
            profit=10.00,
            ingestion_timestamp=datetime(2024, 4, 10, 10, 0, 0)
        )
    ]
    orders_df = spark.createDataFrame(orders_data)
    
    customers_df = spark.createDataFrame([], "customer_id STRING, customer_name STRING, country STRING")
    
    products_data = [
        Row(
            product_id="PROD-001",
            product_name="Widget",
            category="Tools",
            sub_category="Hand Tools",
            state="Available",
            price_per_product=50.00
        )
    ]
    products_df = spark.createDataFrame(products_data)
    
    result_df = transform_enriched_orders(orders_df, customers_df, products_df)
    
    # Create expected DataFrame with null customer fields - use LongType for order_year
    expected_schema = StructType([
        StructField("row_id", LongType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("ship_date", DateType(), True),
        StructField("ship_mode", StringType(), True),
        StructField("order_year", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_sub_category", StringType(), True),
        StructField("quantity", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("profit", DecimalType(18, 2), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    
    expected_data = [
        (2, "ORD-002", date(2024, 4, 10), date(2024, 4, 15), "Express", 2024,
         "CUST-999", None, None, "PROD-001", "Widget", "Tools", "Hand Tools",
         1, 50.00, 0.0, Decimal("10.00"), datetime(2024, 4, 10, 10, 0, 0))
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_column_order=True)


def test_transform_enriched_orders_with_missing_product(spark):
    """
    Test left join behavior when product doesn't exist:
    - Product fields should be null
    - Order and customer fields should be preserved
    """
    orders_data = [
        Row(
            row_id=3,
            order_id="ORD-003",
            order_date=date(2024, 5, 20),
            ship_date=date(2024, 5, 25),
            ship_mode="Standard",
            customer_id="CUST-001",
            product_id="PROD-999",  # Non-existent
            quantity=3,
            price=75.00,
            discount=0.2,
            profit=20.00,
            ingestion_timestamp=datetime(2024, 5, 20, 10, 0, 0)
        )
    ]
    orders_df = spark.createDataFrame(orders_data)
    
    customers_data = [
        Row(
            customer_id="CUST-001",
            customer_name="Jane Smith",
            country="Canada",
            email="jane@example.com",
            phone="555-5678"
        )
    ]
    customers_df = spark.createDataFrame(customers_data)
    
    products_df = spark.createDataFrame([], "product_id STRING, product_name STRING, category STRING, sub_category STRING")
    
    result_df = transform_enriched_orders(orders_df, customers_df, products_df)
    
    # Create expected DataFrame with null product fields - use LongType for order_year
    expected_schema = StructType([
        StructField("row_id", LongType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("ship_date", DateType(), True),
        StructField("ship_mode", StringType(), True),
        StructField("order_year", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_sub_category", StringType(), True),
        StructField("quantity", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("profit", DecimalType(18, 2), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    
    expected_data = [
        (3, "ORD-003", date(2024, 5, 20), date(2024, 5, 25), "Standard", 2024,
         "CUST-001", "Jane Smith", "Canada", "PROD-999", None, None, None,
         3, 75.00, 0.2, Decimal("20.00"), datetime(2024, 5, 20, 10, 0, 0))
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_column_order=True)


def test_transform_enriched_orders_profit_rounding(spark):
    """
    Test profit rounding to 2 decimal places
    """
    orders_data = [
        Row(
            row_id=4,
            order_id="ORD-004",
            order_date=date(2024, 6, 1),
            ship_date=date(2024, 6, 5),
            ship_mode="Standard",
            customer_id="CUST-001",
            product_id="PROD-001",
            quantity=1,
            price=100.00,
            discount=0.0,
            profit=25.556,  # Should round to 25.56
            ingestion_timestamp=datetime(2024, 6, 1, 10, 0, 0)
        )
    ]
    orders_df = spark.createDataFrame(orders_data)
    
    customers_data = [
        Row(
            customer_id="CUST-001",
            customer_name="Test User",
            country="USA",
            email="test@example.com",
            phone="555-0000"
        )
    ]
    customers_df = spark.createDataFrame(customers_data)
    
    products_data = [
        Row(
            product_id="PROD-001",
            product_name="Test Product",
            category="Test Category",
            sub_category="Test Sub",
            state="Available",
            price_per_product=100.00
        )
    ]
    products_df = spark.createDataFrame(products_data)
    
    result_df = transform_enriched_orders(orders_df, customers_df, products_df)
    
    # Create expected DataFrame - use LongType for order_year
    expected_schema = StructType([
        StructField("row_id", LongType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("ship_date", DateType(), True),
        StructField("ship_mode", StringType(), True),
        StructField("order_year", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_sub_category", StringType(), True),
        StructField("quantity", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("profit", DecimalType(18, 2), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    
    expected_data = [
        (4, "ORD-004", date(2024, 6, 1), date(2024, 6, 5), "Standard", 2024,
         "CUST-001", "Test User", "USA", "PROD-001", "Test Product", "Test Category", "Test Sub",
         1, 100.00, 0.0, Decimal("25.56"), datetime(2024, 6, 1, 10, 0, 0))
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_column_order=True)


def test_transform_enriched_orders_year_extraction(spark):
    """
    Test order_year extraction from order_date
    """
    orders_data = [
        Row(
            row_id=5,
            order_id="ORD-005",
            order_date=date(2023, 12, 31),  # 2023
            ship_date=date(2024, 1, 5),
            ship_mode="Express",
            customer_id="CUST-001",
            product_id="PROD-001",
            quantity=2,
            price=50.00,
            discount=0.1,
            profit=8.00,
            ingestion_timestamp=datetime(2023, 12, 31, 10, 0, 0)
        )
    ]
    orders_df = spark.createDataFrame(orders_data)
    
    customers_data = [
        Row(
            customer_id="CUST-001",
            customer_name="Year Test",
            country="USA",
            email="year@example.com",
            phone="555-9999"
        )
    ]
    customers_df = spark.createDataFrame(customers_data)
    
    products_data = [
        Row(
            product_id="PROD-001",
            product_name="Year Product",
            category="Year Category",
            sub_category="Year Sub",
            state="Available",
            price_per_product=50.00
        )
    ]
    products_df = spark.createDataFrame(products_data)
    
    result_df = transform_enriched_orders(orders_df, customers_df, products_df)
    
    # Create expected DataFrame - use LongType for order_year
    expected_schema = StructType([
        StructField("row_id", LongType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("ship_date", DateType(), True),
        StructField("ship_mode", StringType(), True),
        StructField("order_year", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_sub_category", StringType(), True),
        StructField("quantity", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("profit", DecimalType(18, 2), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    
    expected_data = [
        (5, "ORD-005", date(2023, 12, 31), date(2024, 1, 5), "Express", 2023,
         "CUST-001", "Year Test", "USA", "PROD-001", "Year Product", "Year Category", "Year Sub",
         2, 50.00, 0.1, Decimal("8.00"), datetime(2023, 12, 31, 10, 0, 0))
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_column_order=True)
