# Databricks E-commerce Data Engineering Assessment

## Overview

This project implements a complete end-to-end data engineering solution for an e-commerce platform using:

- PySpark
- Databricks
- Delta Lake
- Databricks DQX
- Medallion Architecture
- Pytest based unit testing
- Config-driven pipelines

The objective of the project is to process raw sales datasets and transform them into business-ready analytical tables that can be consumed by reporting and analytics teams.

The implementation focuses on:

- scalability
- modular design
- data quality
- layered architecture
- reusable transformations
- unit testing
- enterprise-grade structuring

---

# Assessment Requirements

The assessment required the following:

1. Create raw tables for each source dataset
2. Create enriched customer and product datasets
3. Create an enriched order table containing:
   - order information
   - rounded profit
   - customer details
   - product category and sub-category
4. Create aggregate profit tables
5. Provide SQL-based reporting outputs
6. Implement unit testing
7. Use PySpark for transformations
8. Handle data quality issues
9. Implement robust error handling
10. Optimize for maintainability and scalability

---

# Final Architecture

The final architecture follows Medallion Architecture.

```text
Source Files
    ↓
Raw Layer
    ↓
Refined Layer
    ↓
Enriched Dimensions
    ↓
Enriched Fact Table
    ↓
Aggregate Layer
    ↓
SQL Reporting Layer
```

---

# Folder Structure

```text
PEI-Data-Engineering-Task/
│
├── config/
│   ├── dev_config.json
│   └── dq_rules/
│       ├── orders_dq_rules.yml
│       ├── customers_dq_rules.yml
│       └── products_dq_rules.yml
│
├── notebooks/
│   ├── 01_setup
│   ├── 02_raw_load
│   ├── 03_refined_load
│   ├── 04_enriched_dimensions
│   ├── 05_enriched_orders
│   ├── 06_aggregate_profit
│   ├── 07_sql_outputs
│   └── 08_run_unit_tests
│
├── src/
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── common.py
│   │   └── dqx_utils.py
│   │
│   └── transformations/
│       ├── __init__.py
│       ├── refined.py
│       ├── customers.py
│       ├── products.py
│       ├── orders.py
│       └── aggregations.py
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_common.py
│   ├── test_refined.py
│   ├── test_enriched_dimensions.py
│   ├── test_enriched_orders.py
│   └── test_aggregations.py
│
├── requirements.txt
└── README.md
```

---

# Source Datasets

The project processes the following datasets:

1. Orders JSON file
2. Customers Excel file
3. Products CSV file

The datasets are loaded into the Landing Zone and processed through different Medallion layers.

---

# Config-Driven Design

The project is fully config-driven.

All schemas, table names, source paths, and file details are maintained inside:

```text
config/dev_config.json
```

This design allows:

- easy environment changes
- reduced hardcoding
- easier scalability
- reusable pipelines
- centralized configuration management

Example:

```json
{
  "catalog": "dev",

  "raw": {
    "schema": "raw",
    "tables": {
      "orders": "raw_orders"
    }
  }
}
```

---

# Raw Layer

## Purpose

The Raw Layer preserves source-aligned data.

The objective of the raw layer is:

- minimal transformation
- preserve source structure
- support reruns
- maintain auditability
- avoid schema enforcement too early

## Key Design Decisions

- Most columns stored as STRING
- No business logic applied
- Metadata columns added
- Data remains close to source

## Metadata Columns

The following metadata columns are added:

```text
source_file_name
ingestion_timestamp
```

## Important JSON Handling

While reading JSON, the following option was used:

```python
.option("primitivesAsString", "true")
```

Reason:

Avoid Delta schema merge conflicts caused by automatic datatype inference.

## Important Excel Handling

Excel files were read using:

```python
pd.read_excel(
    file_path,
    engine="openpyxl",
    dtype=str,
    keep_default_na=False
)
```

Reason:

Avoid Arrow conversion issues and preserve all columns as STRING.

---

# Refined Layer

## Purpose

The Refined Layer contains:

- cleaned data
- datatype conversions
- trimmed values
- validated data
- parsed dates

This layer acts as the trusted typed layer.

## Responsibilities

### Data Type Casting

Examples:

```text
quantity → INT
price → DOUBLE
profit → DOUBLE
order_date → DATE
```

### Data Cleaning

- trim spaces
- remove malformed records
- standardize formats

### Data Quality Validation

Data Quality checks were implemented using Databricks DQX.

---

# Data Quality Framework (DQX)

## Why DQX

DQX was used to implement configurable and reusable data quality validation.

## YAML Driven Rules

DQ rules are maintained separately under:

```text
config/dq_rules/
```

Example:

```yaml
- criticality: error
  check:
    function: is_not_null_and_not_empty
    for_each_column:
      - order_id
      - customer_id
      - product_id
```

## DQ Design

The following strategy was implemented:

```text
Valid Records → Refined Tables
Invalid Records → Quarantine Tables
```

## Quarantine Tables

Separate quarantine tables were created:

```text
quarantine_orders
quarantine_customers
quarantine_products
```

## Duplicate Handling

Duplicate validation was added using:

```yaml
function: is_unique
```

This prevented duplicate product IDs and customer IDs from flowing downstream.

---

# Enriched Dimensions

## enriched_customers

Purpose:

Provide business-ready customer dimension.

Columns:

- customer_id
- customer_name
- email
- phone
- address
- segment
- country
- city
- state
- postal_code
- region
- ingestion_timestamp

## enriched_products

Purpose:

Provide business-ready product dimension.

Columns:

- product_id
- product_name
- category
- sub_category
- state
- price_per_product
- ingestion_timestamp

---

# Enriched Fact Table

## fact_orders_enriched

Purpose:

Create business-ready fact table by joining:

- refined_orders
- enriched_customers
- enriched_products

## Features Implemented

### Order Year Extraction

```python
F.year(F.col("o.order_date"))
```

### Profit Rounding

```python
F.round(F.col("o.profit"), 2)
```

### Product Renaming

The following columns were renamed:

```text
category → product_category
sub_category → product_sub_category
```

## Final Columns

- order information
- customer information
- product information
- rounded profit
- ingestion timestamp

---

# Aggregate Layer

## Purpose

Generate reporting-ready aggregate tables.

## Aggregate Table

```text
agg_profit_by_year_category_subcategory_customer
```

## Aggregation Logic

Grouping performed on:

- order_year
- product_category
- product_sub_category
- customer_id
- customer_name

## Output

```text
total_profit
```

## Important Design Decision

The aggregate layer intentionally does NOT contain:

```text
ingestion_timestamp
```

Reason:

Aggregate tables are reporting outputs and not ingestion/audit datasets.

---

# SQL Reporting Layer

The following SQL reports were implemented.

## Profit by Year

```sql
SELECT
    order_year,
    ROUND(SUM(total_profit), 2) AS total_profit
FROM dev.aggregate.agg_profit_by_year_category_subcategory_customer
GROUP BY order_year
ORDER BY order_year;
```

## Profit by Year and Product Category

```sql
SELECT
    order_year,
    product_category,
    ROUND(SUM(total_profit), 2) AS total_profit
FROM dev.aggregate.agg_profit_by_year_category_subcategory_customer
GROUP BY order_year, product_category
ORDER BY order_year, product_category;
```

## Profit by Customer

```sql
SELECT
    customer_id,
    customer_name,
    ROUND(SUM(total_profit), 2) AS total_profit
FROM dev.aggregate.agg_profit_by_year_category_subcategory_customer
GROUP BY customer_id, customer_name
ORDER BY total_profit DESC;
```

## Profit by Customer and Year

```sql
SELECT
    customer_id,
    customer_name,
    order_year,
    ROUND(SUM(total_profit), 2) AS total_profit
FROM dev.aggregate.agg_profit_by_year_category_subcategory_customer
GROUP BY customer_id, customer_name, order_year
ORDER BY customer_id, order_year;
```

---

# Utility Functions

## common.py

This file contains reusable helper functions.

### read_config()

Purpose:

Read JSON configuration file.

### build_source_path()

Purpose:

Build source file path dynamically.

### build_table_name()

Purpose:

Generate fully qualified table name.

Example:

```text
dev.raw.raw_orders
```

### standardize_column_names()

Purpose:

Convert column names to snake_case.

### add_ingestion_columns()

Purpose:

Add metadata columns.

### write_delta_table()

Purpose:

Write DataFrame to Delta table.

---

# Transformation Files

## refined.py

Contains:

- transform_refined_orders()
- transform_refined_customers()
- transform_refined_products()

Responsibilities:

- datatype casting
- trimming
- date parsing
- refined layer standardization

---

## customers.py

Contains:

```python
transform_enriched_customers()
```

Purpose:

Create business-ready customer dimension.

---

## products.py

Contains:

```python
transform_enriched_products()
```

Purpose:

Create business-ready product dimension.

---

## orders.py

Contains:

```python
transform_enriched_orders()
```

Responsibilities:

- join dimensions
- create fact table
- extract order year
- round profit
- rename business columns

---

## aggregations.py

Contains:

```python
transform_profit_aggregate()
```

Purpose:

Generate reporting aggregates.

---

# Unit Testing

Unit testing was implemented using:

- pytest
- pyspark.testing.utils.assertDataFrameEqual

## Testing Strategy

Transformation functions were tested directly.

Read/write operations were intentionally excluded because they are integration concerns.

---

# Test Coverage

## test_common.py

Tests:

### test_build_table_name()

Validates:

- fully qualified table name generation

### test_build_source_path()

Validates:

- source file path generation

### test_standardize_column_names()

Validates:

- snake_case conversion

### test_add_ingestion_columns()

Validates:

- source_file_name addition
- ingestion_timestamp addition

---

## test_refined.py

Tests:

### test_transform_refined_orders()

Validates:

- datatype casting
- trimming
- date conversion
- timestamp preservation

### test_transform_refined_customers()

Validates:

- trimming
- customer field cleanup

### test_transform_refined_products()

Validates:

- datatype casting
- product transformations

---

## test_enriched_dimensions.py

Tests:

### customer dimension transformation

Validates:

- correct customer columns
- null preservation
- metadata preservation

### product dimension transformation

Validates:

- correct product columns
- metadata preservation

---

## test_enriched_orders.py

Tests:

### enriched order join logic

Validates:

- joins between orders/customers/products
- product category mapping
- customer mapping
- order year extraction
- rounded profit
- missing dimension handling

---

## test_aggregations.py

Tests:

### aggregation logic

Validates:

- grouping correctness
- profit summation
- decimal rounding
- multiple group handling
- negative profit handling
- zero profit handling

---

# Running Unit Tests in Databricks

The following command was used:

```python
%sh
cd /Workspace/Users/akshaygalaxy23@gmail.com/PEI-Data-Engineering-Task
PYTHONDONTWRITEBYTECODE=1 pytest tests/ -v --tb=short
```

Reason for PYTHONDONTWRITEBYTECODE:

Avoid __pycache__ creation issue in Databricks Workspace filesystem.

---

# Error Handling Strategy

Separate try-except blocks were implemented for:

- imports
- config loading
- source reads
- transformations
- writes
- validations

Reason:

Avoid hiding failures and improve debuggability.

---

# Performance and Scalability Considerations

The following best practices were implemented:

- Delta Lake tables
- CLUSTER BY AUTO
- modular reusable transformations
- config-driven pipelines
- reusable utility functions
- reusable DQ framework
- separation of layers
- quarantine handling
- scalable Medallion Architecture

---

# Final Execution Order

```text
01_setup
02_raw_load
03_refined_load
04_enriched_dimensions
05_enriched_orders
06_aggregate_profit
07_sql_outputs
08_run_unit_tests
```

---

# Key Interview Talking Points

## Why Raw Layer Uses STRING Columns

To avoid schema evolution issues and preserve source fidelity.

## Why DQ Is Applied in Refined Layer

Because refined layer is the trusted typed layer.

## Why Quarantine Tables Were Created

To isolate invalid records while allowing pipeline continuity.

## Why Enriched Dimensions Were Created

To separate reusable dimensions from fact tables.

## Why Aggregate Layer Does Not Contain Metadata Columns

Aggregate tables are reporting outputs and not ingestion datasets.

## Why Unit Tests Were Added at Transformation Layer

Transformation functions contain business logic and are ideal for isolated testing.

## Why Config-Driven Design Was Used

To improve maintainability and environment portability.

---

# Conclusion

This project demonstrates a production-style data engineering solution using Databricks and PySpark.

The implementation focuses not only on solving the assessment but also on demonstrating:

- scalable architecture
- modular design
- enterprise engineering practices
- data quality management
- reusable code design
- proper unit testing
- layered data processing
- maintainable pipeline structure

