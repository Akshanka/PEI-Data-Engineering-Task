import pytest
from decimal import Decimal
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, LongType, StringType, DecimalType
from src.transformations.aggregations import transform_profit_aggregate


def test_transform_profit_aggregate_single_group(spark):
    """
    Test transform_profit_aggregate with a single group:
    - Groups by order_year, product_category, product_sub_category, customer_id, customer_name
    - Sums profit and rounds to 2 decimal places
    - Casts to decimal(38,18) by default
    """
    input_data = [
        Row(
            order_year=2024,
            product_category="Electronics",
            product_sub_category="Phones",
            customer_id="CUST-001",
            customer_name="John Doe",
            profit=10.555
        ),
        Row(
            order_year=2024,
            product_category="Electronics",
            product_sub_category="Phones",
            customer_id="CUST-001",
            customer_name="John Doe",
            profit=5.445
        )
    ]
    input_df = spark.createDataFrame(input_data)
    
    result_df = transform_profit_aggregate(input_df)
    
    # Create expected DataFrame with proper schema - use DecimalType(18, 2) to match Spark default
    expected_schema = StructType([
        StructField("order_year", LongType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_sub_category", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("total_profit", DecimalType(18, 2), True)
    ])
    
    expected_data = [
        (2024, "Electronics", "Phones", "CUST-001", "John Doe", Decimal("16.00"))
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    # Use DataFrame equality check
    assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_column_order=True)


def test_transform_profit_aggregate_multiple_groups(spark):
    """
    Test with multiple different groups
    """
    input_data = [
        # Group 1: Customer 1, Electronics, 2024
        Row(
            order_year=2024,
            product_category="Electronics",
            product_sub_category="Phones",
            customer_id="CUST-001",
            customer_name="John Doe",
            profit=100.50
        ),
        Row(
            order_year=2024,
            product_category="Electronics",
            product_sub_category="Phones",
            customer_id="CUST-001",
            customer_name="John Doe",
            profit=50.25
        ),
        # Group 2: Customer 2, Furniture, 2024
        Row(
            order_year=2024,
            product_category="Furniture",
            product_sub_category="Chairs",
            customer_id="CUST-002",
            customer_name="Jane Smith",
            profit=75.00
        ),
        # Group 3: Customer 1, Electronics, 2023 (different year)
        Row(
            order_year=2023,
            product_category="Electronics",
            product_sub_category="Phones",
            customer_id="CUST-001",
            customer_name="John Doe",
            profit=200.00
        )
    ]
    input_df = spark.createDataFrame(input_data)
    
    result_df = transform_profit_aggregate(input_df)
    
    # Create expected DataFrame
    expected_schema = StructType([
        StructField("order_year", LongType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_sub_category", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("total_profit", DecimalType(18, 2), True)
    ])
    
    expected_data = [
        (2024, "Electronics", "Phones", "CUST-001", "John Doe", Decimal("150.75")),
        (2024, "Furniture", "Chairs", "CUST-002", "Jane Smith", Decimal("75.00")),
        (2023, "Electronics", "Phones", "CUST-001", "John Doe", Decimal("200.00"))
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)


def test_transform_profit_aggregate_different_subcategories(spark):
    """
    Test that different subcategories create separate groups
    even with same customer and category
    """
    input_data = [
        Row(
            order_year=2024,
            product_category="Electronics",
            product_sub_category="Phones",
            customer_id="CUST-001",
            customer_name="John Doe",
            profit=100.00
        ),
        Row(
            order_year=2024,
            product_category="Electronics",
            product_sub_category="Laptops",  # Different subcategory
            customer_id="CUST-001",
            customer_name="John Doe",
            profit=200.00
        )
    ]
    input_df = spark.createDataFrame(input_data)
    
    result_df = transform_profit_aggregate(input_df)
    
    expected_schema = StructType([
        StructField("order_year", LongType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_sub_category", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("total_profit", DecimalType(18, 2), True)
    ])
    
    expected_data = [
        (2024, "Electronics", "Phones", "CUST-001", "John Doe", Decimal("100.00")),
        (2024, "Electronics", "Laptops", "CUST-001", "John Doe", Decimal("200.00"))
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_row_order=True, ignore_column_order=True)


def test_transform_profit_aggregate_rounding(spark):
    """
    Test that profit is properly rounded to 2 decimal places
    """
    input_data = [
        Row(
            order_year=2024,
            product_category="Office Supplies",
            product_sub_category="Paper",
            customer_id="CUST-003",
            customer_name="Bob Johnson",
            profit=10.124
        ),
        Row(
            order_year=2024,
            product_category="Office Supplies",
            product_sub_category="Paper",
            customer_id="CUST-003",
            customer_name="Bob Johnson",
            profit=5.555
        ),
        Row(
            order_year=2024,
            product_category="Office Supplies",
            product_sub_category="Paper",
            customer_id="CUST-003",
            customer_name="Bob Johnson",
            profit=2.876
        )
    ]
    input_df = spark.createDataFrame(input_data)
    
    result_df = transform_profit_aggregate(input_df)
    
    expected_schema = StructType([
        StructField("order_year", LongType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_sub_category", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("total_profit", DecimalType(18, 2), True)
    ])
    
    # Sum: 10.124 + 5.555 + 2.876 = 18.555, Rounded: 18.56
    expected_data = [
        (2024, "Office Supplies", "Paper", "CUST-003", "Bob Johnson", Decimal("18.56"))
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_column_order=True)


def test_transform_profit_aggregate_negative_profit(spark):
    """
    Test aggregation with negative profits (losses)
    """
    input_data = [
        Row(
            order_year=2024,
            product_category="Furniture",
            product_sub_category="Tables",
            customer_id="CUST-004",
            customer_name="Alice Williams",
            profit=50.00
        ),
        Row(
            order_year=2024,
            product_category="Furniture",
            product_sub_category="Tables",
            customer_id="CUST-004",
            customer_name="Alice Williams",
            profit=-30.00  # Loss
        ),
        Row(
            order_year=2024,
            product_category="Furniture",
            product_sub_category="Tables",
            customer_id="CUST-004",
            customer_name="Alice Williams",
            profit=-10.50  # Loss
        )
    ]
    input_df = spark.createDataFrame(input_data)
    
    result_df = transform_profit_aggregate(input_df)
    
    expected_schema = StructType([
        StructField("order_year", LongType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_sub_category", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("total_profit", DecimalType(18, 2), True)
    ])
    
    # Sum: 50.00 + (-30.00) + (-10.50) = 9.50
    expected_data = [
        (2024, "Furniture", "Tables", "CUST-004", "Alice Williams", Decimal("9.50"))
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_column_order=True)


def test_transform_profit_aggregate_zero_profit(spark):
    """
    Test aggregation with zero profit
    """
    input_data = [
        Row(
            order_year=2024,
            product_category="Office Supplies",
            product_sub_category="Binders",
            customer_id="CUST-005",
            customer_name="Charlie Brown",
            profit=5.00
        ),
        Row(
            order_year=2024,
            product_category="Office Supplies",
            product_sub_category="Binders",
            customer_id="CUST-005",
            customer_name="Charlie Brown",
            profit=-5.00
        )
    ]
    input_df = spark.createDataFrame(input_data)
    
    result_df = transform_profit_aggregate(input_df)
    
    expected_schema = StructType([
        StructField("order_year", LongType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_sub_category", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("total_profit", DecimalType(18, 2), True)
    ])
    
    # Sum: 5.00 + (-5.00) = 0.00
    expected_data = [
        (2024, "Office Supplies", "Binders", "CUST-005", "Charlie Brown", Decimal("0.00"))
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_column_order=True)
