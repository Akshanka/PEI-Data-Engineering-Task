# PEI Data Engineering Task - E-commerce Sales Data Processing

## Overview

This project implements a data processing system using Databricks and PySpark for an e-commerce platform. The solution processes sales data including orders, products, customers, and transactions to provide valuable insights for business stakeholders.

## Project Context

**Assignment by:** PEI (Senior Data Engineer Assessment)

**Objective:** Design and implement a scalable, efficient, and reliable data engineering solution using Databricks that:
* Processes raw e-commerce sales data
* Creates refined and enriched data layers
* Performs aggregations for business analytics
* Implements data quality checks
* Follows test-driven development practices

## Project Structure

```
PEI-Data-Engineering-Task/
├── config/
│   ├── dev_config.json              # Environment configuration
│   └── dq_rules/                    # Data quality rules in YAML
│       ├── orders_dq_rules.yml
│       ├── customers_dq_rules.yml
│       └── products_dq_rules.yml
├── src/
│   ├── utils/                       # Utility functions
│   │   ├── common.py               # File I/O, config, transformations
│   │   └── dqx_utils.py            # Data quality functions
│   └── transformations/            # Data transformation logic
│       ├── refined.py              # Refined layer (cleaning)
│       ├── customers.py            # Enriched customers
│       ├── products.py             # Enriched products
│       ├── orders.py               # Enriched orders (joins)
│       └── aggregations.py         # Profit aggregations
├── tests/                          # Unit tests
│   ├── conftest.py                # Pytest configuration
│   ├── test_common.py
│   ├── test_dqx_utils.py
│   ├── test_refined.py
│   ├── test_enriched_dimensions.py
│   ├── test_enriched_orders.py
│   └── test_aggregations.py
└── requirements.txt               # Python dependencies
```

---

## Function Documentation

### 1. `src/utils/common.py`

Utility functions for configuration management, file I/O, column standardization, and table operations.

#### `read_config(config_path: str) -> Dict[str, Any]`
Reads the project configuration JSON file.

**Example:**
```python
config = read_config("config/dev_config.json")
# Returns: {"catalog": "dev", "raw": {...}, "refined": {...}}
```

#### `to_snake_case(column_name: str) -> str`
Converts column names like 'Order ID' or 'Sub-Category' into snake_case like 'order_id' or 'sub_category'.

**Example:**
```python
result = to_snake_case("Order ID")
# Returns: "order_id"

result = to_snake_case("Sub-Category")
# Returns: "sub_category"
```

#### `standardize_column_names(df: DataFrame) -> DataFrame`
Standardizes all DataFrame column names to snake_case.

**Example:**
```python
# Input DataFrame with columns: ["Row ID", "Order-Date", "Customer Name"]
df_standardized = standardize_column_names(df)
# Output columns: ["row_id", "order_date", "customer_name"]
```

#### `add_ingestion_columns(df: DataFrame, source_file_name: str) -> DataFrame`
Adds audit columns for tracking data lineage: `source_file_name` and `ingestion_timestamp`.

**Example:**
```python
df_with_audit = add_ingestion_columns(df, "Orders.json")
# Adds columns:
# - source_file_name: "Orders.json"
# - ingestion_timestamp: current_timestamp()
```

#### `build_source_path(config: Dict[str, Any], file_key: str) -> str`
Builds full source file path from configuration.

**Example:**
```python
config = {
    "source": {
        "base_path": "/Volumes/dev/landing/landing_zone",
        "files": {"orders": {"file_name": "Orders.json"}}
    }
}
path = build_source_path(config, "orders")
# Returns: "/Volumes/dev/landing/landing_zone/Orders.json"
```

#### `build_table_name(config: Dict[str, Any], layer: str, table_key: str) -> str`
Builds fully qualified Databricks table name (catalog.schema.table).

**Example:**
```python
config = {
    "catalog": "dev",
    "raw": {"schema": "raw", "tables": {"orders": "raw_orders"}}
}
table_name = build_table_name(config, "raw", "orders")
# Returns: "dev.raw.raw_orders"
```

#### `read_json_file(spark: SparkSession, file_path: str, multi_line: bool = True) -> DataFrame`
Reads a JSON file and returns a Spark DataFrame with all columns as strings.

**Example:**
```python
df = read_json_file(spark, "/path/to/Orders.json", multi_line=True)
# Reads multi-line JSON with primitivesAsString=true
```

#### `read_csv_file(spark: SparkSession, file_path: str, header: bool = True, infer_schema: bool = False) -> DataFrame`
Reads a CSV file and returns a Spark DataFrame.

**Example:**
```python
df = read_csv_file(spark, "/path/to/customers.csv", header=True, infer_schema=False)
# Reads CSV with headers, all columns as strings
```

#### `read_excel_file(spark: SparkSession, file_path: str, sheet_name=0) -> DataFrame`
Reads an Excel file using pandas and converts it to a Spark DataFrame. All columns are read as strings to preserve raw values.

**Example:**
```python
df = read_excel_file(spark, "/path/to/products.xlsx", sheet_name=0)
# Reads first sheet with all columns as strings
```

#### `write_delta_table(df: DataFrame, table_name: str, mode: str = "overwrite") -> None`
Writes a DataFrame to a Delta table.

**Example:**
```python
write_delta_table(df, "dev.raw.raw_orders", mode="overwrite")
# Writes DataFrame to Delta table in overwrite mode
```

---

### 2. `src/utils/dqx_utils.py`

Data quality utilities using Databricks DQX framework.

#### `load_dq_rules(rules_file_path: str) -> dict`
Reads data quality checks from a YAML file.

**Example:**
```python
rules = load_dq_rules("config/dq_rules/orders_dq_rules.yml")
# Returns: {"checks": [{"column": "order_id", "rule": "not_null"}, ...]}
```

#### `apply_dq_checks(df: DataFrame, rules_file_path: str) -> Tuple[DataFrame, DataFrame]`
Applies DQX checks and splits data into valid and quarantine DataFrames.

**Example:**
```python
valid_df, quarantine_df = apply_dq_checks(df, "config/dq_rules/orders_dq_rules.yml")
# Returns two DataFrames:
# - valid_df: Records that passed all checks
# - quarantine_df: Records that failed checks (with DQ metadata)
```

---

### 3. `src/transformations/refined.py`

Refined layer transformations for data cleaning and type casting.

#### `transform_refined_orders(df: DataFrame) -> DataFrame`
Cleans and standardizes orders data:
* Trims whitespace from string columns
* Converts dates from `d/M/yyyy` format to DATE type
* Casts numeric fields using `try_cast` (invalid values become NULL)
* Preserves `ingestion_timestamp`

**Example:**
```python
refined_orders = transform_refined_orders(raw_orders_df)
# Input:  row_id=" 1 ", order_date=" 1/1/2024 ", quantity=" 2 "
# Output: row_id=1, order_date=date(2024, 1, 1), quantity=2
```

#### `transform_refined_customers(df: DataFrame) -> DataFrame`
Cleans customers data by trimming whitespace from all string columns.

**Example:**
```python
refined_customers = transform_refined_customers(raw_customers_df)
# Input:  customer_id=" CUST-001 ", customer_name=" John Doe "
# Output: customer_id="CUST-001", customer_name="John Doe"
```

#### `transform_refined_products(df: DataFrame) -> DataFrame`
Cleans products data:
* Trims whitespace from string columns
* Casts `price_per_product` to DOUBLE using `try_cast`

**Example:**
```python
refined_products = transform_refined_products(raw_products_df)
# Input:  product_id=" PROD-001 ", price_per_product=" 999.99 "
# Output: product_id="PROD-001", price_per_product=999.99
```

---

### 4. `src/transformations/customers.py`

Enriched layer transformation for customers dimension.

#### `transform_enriched_customers(df: DataFrame) -> DataFrame`
Selects and passes through all customer columns to the enriched layer.

**Example:**
```python
enriched_customers = transform_enriched_customers(refined_customers_df)
# Selects: customer_id, customer_name, email, phone, address, segment,
#          country, city, state, postal_code, region, ingestion_timestamp
```

---

### 5. `src/transformations/products.py`

Enriched layer transformation for products dimension.

#### `transform_enriched_products(df: DataFrame) -> DataFrame`
Selects and passes through all product columns to the enriched layer.

**Example:**
```python
enriched_products = transform_enriched_products(refined_products_df)
# Selects: product_id, product_name, category, sub_category,
#          state, price_per_product, ingestion_timestamp
```

---

### 6. `src/transformations/orders.py`

Enriched layer transformation for orders fact table with dimension joins.

#### `transform_enriched_orders(orders_df: DataFrame, customers_df: DataFrame, products_df: DataFrame) -> DataFrame`
Creates enriched orders table by:
* Left joining orders with customers (on `customer_id`)
* Left joining with products (on `product_id`)
* Extracting `order_year` from `order_date`
* Rounding `profit` to 2 decimal places as `DECIMAL(18,2)`
* Selecting order info, customer details (name, country), and product details (category, sub_category)

**Example:**
```python
enriched_orders = transform_enriched_orders(
    refined_orders_df,
    enriched_customers_df,
    enriched_products_df
)
# Output includes:
# - Order: row_id, order_id, order_date, ship_date, ship_mode, order_year
# - Customer: customer_id, customer_name, country
# - Product: product_id, product_name, product_category, product_sub_category
# - Metrics: quantity, price, discount, profit (rounded to 2 decimals)
```

---

### 7. `src/transformations/aggregations.py`

Aggregate transformations for business reporting.

#### `transform_profit_aggregate(enriched_orders_df: DataFrame) -> DataFrame`
Aggregates profit by:
* `order_year`
* `product_category`
* `product_sub_category`
* `customer_id` and `customer_name`

Sums profit rounded to 2 decimal places as `DECIMAL(18,2)`.

**Example:**
```python
profit_agg = transform_profit_aggregate(enriched_orders_df)
# Groups by: order_year, product_category, product_sub_category, 
#            customer_id, customer_name
# Returns: total_profit (sum of profit rounded to 2 decimals)
#
# Sample output:
# | order_year | product_category | product_sub_category | customer_id | customer_name | total_profit |
# |------------|------------------|----------------------|-------------|---------------|--------------|
# | 2024       | Electronics      | Phones               | CUST-001    | John Doe      | 150.75       |
# | 2024       | Furniture        | Chairs               | CUST-002    | Jane Smith    | 75.00        |
```

---

## Unit Tests Documentation

### 1. `tests/test_common.py`

Tests for utility functions in `common.py`.

#### `test_build_table_name()`
Verifies that fully qualified table names are built correctly.
```python
# Tests: "dev.raw.raw_orders" from config
```

#### `test_build_source_path()`
Verifies that source file paths are constructed correctly.
```python
# Tests: "/Volumes/dev/landing/landing_zone/Orders.json"
```

#### `test_standardize_column_names(spark)`
Verifies column name standardization to snake_case.
```python
# Input columns: ["Row ID", "Order ID"]
# Expected output: ["row_id", "order_id"]
```

#### `test_add_ingestion_columns(spark)`
Verifies that `source_file_name` and `ingestion_timestamp` columns are added.
```python
# Checks presence and correctness of audit columns
```

---

### 2. `tests/test_dqx_utils.py`

Tests for data quality utilities.

#### `test_load_dq_rules()`
Tests YAML file reading for DQ rules.
* Reads YAML correctly
* Returns parsed dictionary structure

#### `test_load_dq_rules_empty_file()`
Tests behavior with empty YAML file.
* Returns `None` for empty file

#### `test_load_dq_rules_file_not_found()`
Tests error handling for non-existent file.
* Raises `FileNotFoundError`

#### `test_apply_dq_checks_basic(mock_dq_engine_class, mock_workspace_client, spark)`
Tests basic DQ check application:
* Creates DQEngine with WorkspaceClient
* Loads rules from YAML file
* Splits data into valid and quarantine DataFrames
* Verifies 1 valid record and 2 quarantine records

#### `test_apply_dq_checks_all_valid(mock_dq_engine_class, mock_workspace_client, spark)`
Tests when all records pass DQ checks.
* All records in valid_df, quarantine_df is empty

#### `test_apply_dq_checks_all_invalid(mock_dq_engine_class, mock_workspace_client, spark)`
Tests when all records fail DQ checks.
* All records in quarantine_df, valid_df is empty

---

### 3. `tests/test_refined.py`

Tests for refined layer transformations.

#### `test_transform_refined_orders(spark)`
Tests orders refinement:
* Trims whitespace from strings
* Converts dates from `d/M/yyyy` format
* Casts numeric fields to appropriate types (BIGINT, INT, DOUBLE)
* Preserves `ingestion_timestamp`

**Test data:**
```python
Input:  row_id=" 1 ", order_date=" 1/1/2024 ", quantity=" 2 "
Output: row_id=1, order_date=date(2024,1,1), quantity=2
```

#### `test_transform_refined_orders_with_nulls(spark)`
Tests that invalid data becomes NULL:
* Invalid numeric strings (`"abc"`, `"xyz"`) → NULL
* Invalid dates remain NULL
* Valid values are preserved

#### `test_transform_refined_customers(spark)`
Tests customers refinement:
* Trims whitespace from all 11 customer fields
* Maintains `ingestion_timestamp`

**Test data:**
```python
Input:  customer_id=" CUST-001 ", customer_name=" John Doe "
Output: customer_id="CUST-001", customer_name="John Doe"
```

#### `test_transform_refined_products(spark)`
Tests products refinement:
* Trims whitespace
* Casts `price_per_product` to DOUBLE

**Test data:**
```python
Input:  product_id=" PROD-001 ", price_per_product=" 999.99 "
Output: product_id="PROD-001", price_per_product=999.99
```

#### `test_transform_refined_products_invalid_price(spark)`
Tests that invalid price strings become NULL.
```python
Input:  price_per_product="invalid_price"
Output: price_per_product=None
```

---

### 4. `tests/test_enriched_dimensions.py`

Tests for enriched customers and products transformations.

#### `test_transform_enriched_customers(spark)`
Tests enriched customers transformation:
* Selects all required customer columns
* Removes extra columns not in the selection
* Preserves data types and values

#### `test_transform_enriched_customers_multiple_records(spark)`
Tests with multiple customer records:
* Verifies count = 2
* Checks data equality

#### `test_transform_enriched_customers_with_nulls(spark)`
Tests that NULL values are preserved correctly in optional fields (email, phone).

#### `test_transform_enriched_products(spark)`
Tests enriched products transformation:
* Selects all required product columns
* Removes extra fields
* Preserves data types and values

#### `test_transform_enriched_products_multiple_records(spark)`
Tests with multiple product records (3 products with different categories).

#### `test_transform_enriched_products_with_nulls(spark)`
Tests NULL preservation in optional fields (category, price_per_product).

---

### 5. `tests/test_enriched_orders.py`

Tests for enriched orders transformation with joins.

#### `test_transform_enriched_orders_with_full_join(spark)`
Tests complete join scenario:
* Left joins orders with customers and products
* Extracts `order_year` from `order_date`
* Rounds profit to 2 decimals: `15.555` → `Decimal("15.56")`
* Casts profit to `DECIMAL(18,2)`
* Selects all required columns (18 total)

**Test data:**
```python
Order: row_id=1, order_id="ORD-001", profit=15.555
Customer: customer_name="John Doe", country="USA"
Product: category="Electronics", sub_category="Phones"
Output profit: Decimal("15.56")
```

#### `test_transform_enriched_orders_with_missing_customer(spark)`
Tests left join when customer doesn't exist:
* Customer fields (`customer_name`, `country`) → NULL
* Order and product fields are preserved

#### `test_transform_enriched_orders_with_missing_product(spark)`
Tests left join when product doesn't exist:
* Product fields (`product_name`, `product_category`, `product_sub_category`) → NULL
* Order and customer fields are preserved

#### `test_transform_enriched_orders_year_extraction(spark)`
Tests `order_year` extraction from different dates:
```python
order_date=date(2023, 1, 15)  → order_year=2023
order_date=date(2024, 12, 31) → order_year=2024
```

#### `test_transform_enriched_orders_profit_rounding_edge_cases(spark)`
Tests profit rounding precision:
```python
10.124 → Decimal("10.12")  # Round down
10.125 → Decimal("10.13")  # Round up
10.999 → Decimal("11.00")  # Round up
```

---

### 6. `tests/test_aggregations.py`

Tests for profit aggregation transformations.

#### `test_transform_profit_aggregate_single_group(spark)`
Tests single group aggregation:
* Groups by year, category, sub_category, customer
* Sums: `10.555 + 5.445 = 16.00`
* Rounds to 2 decimals: `Decimal("16.00")`

**Test data:**
```python
2 rows with same: order_year=2024, category="Electronics", customer="John Doe"
Input profits: 10.555, 5.445
Output: total_profit=Decimal("16.00")
```

#### `test_transform_profit_aggregate_multiple_groups(spark)`
Tests multiple different groups:
* 3 groups based on different combinations of year/category/customer
* Group 1: Customer 1, Electronics, 2024 → `150.75`
* Group 2: Customer 2, Furniture, 2024 → `75.00`
* Group 3: Customer 1, Electronics, 2023 → `200.00`

#### `test_transform_profit_aggregate_different_subcategories(spark)`
Tests that different sub_categories create separate groups:
```python
Same customer, same category, different sub_categories:
- Electronics > Phones → 100.00
- Electronics > Laptops → 200.00
```

#### `test_transform_profit_aggregate_rounding(spark)`
Tests aggregation with precise rounding:
```python
Sum: 10.124 + 5.555 + 2.876 = 18.555
Rounded: Decimal("18.56")
```

#### `test_transform_profit_aggregate_negative_profit(spark)`
Tests handling of losses (negative profits):
```python
50.00 + (-30.00) = 20.00
```

#### `test_transform_profit_aggregate_zero_profit(spark)`
Tests zero profit handling:
```python
All zeros → Decimal("0.00")
```

---

## Running Tests

### Prerequisites
```bash
pip install -r requirements.txt
```

### Run All Tests
```bash
pytest tests/ -v
```

### Run Specific Test File
```bash
pytest tests/test_refined.py -v
pytest tests/test_enriched_orders.py -v
pytest tests/test_aggregations.py -v
```

### Run Specific Test Function
```bash
pytest tests/test_refined.py::test_transform_refined_orders -v
```

### Run Tests with Coverage
```bash
pytest tests/ --cov=src --cov-report=html
```

---

## Data Pipeline Architecture

### Layers

1. **Raw Layer** (`dev.raw.*`)
   * Ingests data from source files (JSON, CSV, Excel)
   * Preserves original data types (all strings)
   * Adds audit columns: `source_file_name`, `ingestion_timestamp`

2. **Refined Layer** (`dev.refined.*`)
   * Cleans and standardizes data
   * Applies data type casting with `try_cast`
   * Trims whitespace, formats dates
   * Tables: `refined_orders`, `refined_customers`, `refined_products`

3. **Enriched Layer** (`dev.enriched.*`)
   * Applies business logic and joins
   * Creates denormalized fact and dimension tables
   * Tables: `enriched_customers`, `enriched_products`, `enriched_orders`

4. **Aggregate Layer** (`dev.aggregate.*`)
   * Pre-computed aggregations for analytics
   * Tables: `profit_aggregate`

### Data Quality

* YAML-based rule definitions in `config/dq_rules/`
* Uses Databricks DQX framework
* Splits data into valid and quarantine tables
* Enables automated data quality monitoring

---

## SQL Analytics Queries

The aggregate table supports various profit analyses:

```sql
-- Profit by Year
SELECT order_year, SUM(total_profit) as yearly_profit
FROM dev.aggregate.profit_aggregate
GROUP BY order_year
ORDER BY order_year;

-- Profit by Year + Product Category
SELECT order_year, product_category, SUM(total_profit) as category_profit
FROM dev.aggregate.profit_aggregate
GROUP BY order_year, product_category
ORDER BY order_year, product_category;

-- Profit by Customer
SELECT customer_name, SUM(total_profit) as customer_profit
FROM dev.aggregate.profit_aggregate
GROUP BY customer_name
ORDER BY customer_profit DESC;

-- Profit by Customer + Year
SELECT customer_name, order_year, SUM(total_profit) as profit
FROM dev.aggregate.profit_aggregate
GROUP BY customer_name, order_year
ORDER BY customer_name, order_year;
```

---

## Key Design Decisions

1. **Test-Driven Development:** Comprehensive unit tests cover all transformation logic
2. **Type Safety:** Using `try_cast` instead of `cast` to handle invalid data gracefully
3. **Data Quality:** DQX framework integration for automated validation
4. **Medallion Architecture:** Raw → Refined → Enriched → Aggregate layers
5. **Auditability:** Tracking source files and ingestion timestamps
6. **Idempotency:** Overwrite mode for rerunnable pipelines
7. **Precision:** Using `DECIMAL(18,2)` for financial calculations

---

## Technologies Used

* **Databricks** - Cloud data platform
* **PySpark** - Distributed data processing
* **Delta Lake** - ACID transactions and data versioning
* **pytest** - Testing framework
* **chispa** - PySpark DataFrame assertion library
* **Databricks DQX** - Data quality framework
* **PyYAML** - Configuration management

---

## Author

Akshay Galaxy (akshaygalaxy23@gmail.com)

**Assessment:** PEI Senior Data Engineer Task
