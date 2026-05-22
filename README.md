# PEI Data Engineering Assessment
## E-commerce Sales Data Processing with Databricks

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apache-spark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.12-blue?style=flat&logo=python&logoColor=white)
![Tests](https://img.shields.io/badge/Tests-32%20Passing-success?style=flat)

---

## 📋 Table of Contents
- [Project Overview](#-project-overview)
- [Assessment Requirements](#-assessment-requirements)
- [Solution Architecture](#-solution-architecture)
- [Project Structure](#-project-structure)
- [Setup Instructions](#-setup-instructions)
- [Running the Pipeline](#-running-the-pipeline)
- [Testing](#-testing)
- [Data Flow](#-data-flow)
- [Key Features](#-key-features)
- [Technologies Used](#-technologies-used)

---

## 🎯 Project Overview

This project implements a **scalable, efficient, and reliable data engineering solution** using Databricks to process and analyze e-commerce sales data. The solution transforms raw sales data through multiple layers (Bronze → Silver → Gold) following medallion architecture principles, providing valuable business insights for stakeholders.

### Business Context
An e-commerce platform generates high volumes of sales data including:
- **Orders** - Transaction details, pricing, discounts
- **Customers** - Customer demographics and contact information
- **Products** - Product catalog with categories and pricing

The pipeline processes this data to enable:
- Profit analysis by year, category, and customer
- Data quality validation and monitoring
- Scalable batch processing with incremental loads

---

## 📝 Assessment Requirements

### Core Tasks

#### 1. **Raw Layer (Bronze)**
- ✅ Create raw tables for each source dataset (Orders, Customers, Products)
- ✅ Preserve source data with minimal transformation
- ✅ Add ingestion metadata (timestamp, source)

#### 2. **Refined Layer (Silver)**
- ✅ Data cleaning and standardization
- ✅ Type casting and validation
- ✅ Handle invalid/missing data gracefully

#### 3. **Enriched Layer (Gold)**
- ✅ **Enriched Dimensions**: Customer and Product master tables
- ✅ **Enriched Orders**: Fact table with:
  - Order information
  - Profit rounded to 2 decimal places
  - Customer name and country
  - Product category and sub-category
  - Order year extraction

#### 4. **Aggregations**
- ✅ Profit aggregation by:
  - Year
  - Product Category
  - Product Sub-Category
  - Customer

#### 5. **SQL Analytics**
- ✅ Profit by Year
- ✅ Profit by Year + Product Category
- ✅ Profit by Customer
- ✅ Profit by Customer + Year

#### 6. **Testing**
- ✅ **32 comprehensive unit tests** covering all transformations
- ✅ Test-driven development approach
- ✅ Edge case and data quality validation

---

## 🏗️ Solution Architecture

### Medallion Architecture

```
┌─────────────────┐
│  Source Data    │  CSV files from Google Drive
│  (Google Drive) │  - orders.csv
└────────┬────────┘  - customers.csv
         │           - products.csv
         ▼
┌─────────────────┐
│  BRONZE LAYER   │  Raw ingestion
│  (Raw Tables)   │  - Preserve source data
└────────┬────────┘  - Add metadata
         │
         ▼
┌─────────────────┐
│  SILVER LAYER   │  Data cleansing
│ (Refined Tables)│  - Trim whitespace
└────────┬────────┘  - Type casting
         │           - Date parsing
         ▼
┌─────────────────┐
│   GOLD LAYER    │  Business logic
│ (Enriched/Agg)  │  - Joins & enrichment
└─────────────────┘  - Aggregations
         │           - Analytics-ready
         ▼
┌─────────────────┐
│   SQL Analysis  │  Business insights
│   & Reporting   │  - Profit metrics
└─────────────────┘  - Dashboards
```

### Data Quality Framework

- **Declarative Rules**: YAML-based DQ rules configuration
- **Automated Validation**: DQX (Data Quality eXtended) integration
- **Quarantine Handling**: Failed records isolated for review
- **Metadata Tracking**: Ingestion timestamps, source tracking

---

## 📁 Project Structure

```
PEI-Data-Engineering-Task/
├── data/                          # Source data files
│   ├── orders.csv
│   ├── customers.csv
│   └── products.csv
│
├── src/                           # Source code modules
│   ├── transformations/           # Transformation logic
│   │   ├── refined.py            # Silver layer transformations
│   │   ├── customers.py          # Customer enrichment
│   │   ├── products.py           # Product enrichment
│   │   ├── orders.py             # Order enrichment
│   │   └── aggregations.py       # Profit aggregations
│   │
│   └── utils/                     # Utility modules
│       ├── common.py             # Common functions
│       └── dqx_utils.py          # Data quality framework
│
├── tests/                         # Unit tests (32 tests)
│   ├── conftest.py               # Pytest fixtures
│   ├── test_refined.py           # Silver layer tests (5)
│   ├── test_enriched_orders.py   # Order enrichment tests (5)
│   ├── test_enriched_dimensions.py # Dimension tests (6)
│   ├── test_aggregations.py      # Aggregation tests (6)
│   ├── test_dqx_utils.py         # DQ framework tests (6)
│   └── test_common.py            # Utility tests (4)
│
├── notebook/                      # Databricks notebooks
│   ├── 01_raw_orders             # Bronze layer - orders
│   ├── 02_raw_customers          # Bronze layer - customers
│   ├── 03_raw_products           # Bronze layer - products
│   ├── 04_refined_layer          # Silver layer - cleaning
│   ├── 05_enriched_layer         # Gold layer - enrichment
│   ├── 06_aggregations           # Gold layer - aggregations
│   ├── 07_analysis               # SQL analytics
│   └── 08_run_unit_tests         # Test execution notebook
│
├── config/                        # Configuration files
│   └── dq_rules.yaml             # Data quality rules
│
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

---

## 🚀 Setup Instructions

### Prerequisites

- **Databricks Workspace** (AWS, Azure, or GCP)
- **Databricks Runtime** 14.3 LTS or higher
- **Unity Catalog** enabled (recommended)
- **Python 3.10+**

### Step 1: Clone/Import Project

#### Option A: Import to Databricks via Repos
```bash
# In Databricks Repos, import from Git URL
https://github.com/your-repo/PEI-Data-Engineering-Task
```

#### Option B: Manual Upload
1. Upload project folder to Databricks Workspace
2. Ensure folder structure is preserved

### Step 2: Download Source Data

1. Download datasets from: [Google Drive Link](https://drive.google.com/drive/folders/1eWxfGcFwJJKAK0Nj4zZeCVx6gagPEEVc?usp=sharing)
2. Place CSV files in `data/` folder:
   - `orders.csv`
   - `customers.csv`
   - `products.csv`

### Step 3: Install Dependencies

```python
# Run in Databricks notebook cell
%pip install -r requirements.txt
dbutils.library.restartPython()
```

**Key Dependencies:**
- `pyspark` - Spark processing
- `pytest` - Unit testing
- `chispa` - DataFrame testing
- `pyyaml` - Config parsing
- `databricks-sdk` - Databricks API

### Step 4: Configure Catalog and Schema

Update catalog/schema names in notebooks (default: `main.default`):

```python
# In each notebook, update these variables:
CATALOG = "your_catalog"  # e.g., "main"
SCHEMA = "your_schema"    # e.g., "pei_assessment"
```

---

## ▶️ Running the Pipeline

### End-to-End Execution

Run notebooks in sequence:

1. **Bronze Layer** - Raw ingestion
   ```
   01_raw_orders      → Creates: raw_orders
   02_raw_customers   → Creates: raw_customers
   03_raw_products    → Creates: raw_products
   ```

2. **Silver Layer** - Data cleansing
   ```
   04_refined_layer   → Creates: refined_orders
                                  refined_customers
                                  refined_products
   ```

3. **Gold Layer** - Enrichment & aggregation
   ```
   05_enriched_layer  → Creates: enriched_orders
                                  enriched_customers
                                  enriched_products
   
   06_aggregations    → Creates: profit_aggregate
   ```

4. **Analytics** - SQL analysis
   ```
   07_analysis        → Business insights queries
   ```

### Incremental Processing

- **Append Mode**: New records added to source files
- **Overwrite Mode**: Full refresh of tables
- **Configurable**: Set `write_mode` parameter in notebooks

---

## 🧪 Testing

### Test Suite Overview

**32 comprehensive unit tests** with 100% pass rate ✅

| Test Module | Tests | Coverage |
|-------------|-------|----------|
| `test_refined.py` | 5 | Data cleaning, type casting, null handling |
| `test_enriched_orders.py` | 5 | Order enrichment, joins, profit rounding |
| `test_enriched_dimensions.py` | 6 | Customer/product transformations |
| `test_aggregations.py` | 6 | Profit aggregations, grouping logic |
| `test_dqx_utils.py` | 6 | Data quality framework, rule validation |
| `test_common.py` | 4 | Utility functions, metadata |
| **Total** | **32** | **All transformation functions** |

### Running Tests

#### Option 1: Via Notebook (Recommended)
```python
# Open notebook: 08_run_unit_tests
# Run cells in sequence:
1. Install dependencies (%pip install)
2. Restart Python (dbutils.library.restartPython())
3. Run tests (pytest.main)
```

#### Option 2: Via Command Line
```bash
# From project root
pytest tests/ -v --tb=short

# Run specific test file
pytest tests/test_refined.py -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

### Test Features

- ✅ **DataFrame Equality Checks** - Using `chispa.assert_df_equality`
- ✅ **Schema Validation** - Verify data types and structure
- ✅ **Edge Case Testing** - Null values, invalid data, boundary conditions
- ✅ **Mocking** - External dependencies isolated
- ✅ **Fixtures** - Spark session from notebook context

---

## 🔄 Data Flow

### Transformation Pipeline

```
Source CSV Files
      ↓
┌─────────────────────────────────────────┐
│  BRONZE: Raw Ingestion                  │
│  - Load CSV → DataFrame                 │
│  - Add ingestion_timestamp              │
│  - Write to Delta tables                │
└─────────────────────────────────────────┘
      ↓
┌─────────────────────────────────────────┐
│  SILVER: Data Cleansing                 │
│  - Trim whitespace                      │
│  - Type casting (dates, numbers)        │
│  - Standardize column names             │
│  - Handle invalid data → NULL           │
└─────────────────────────────────────────┘
      ↓
┌─────────────────────────────────────────┐
│  GOLD: Enrichment                       │
│  Orders:                                │
│    - Join customers (name, country)     │
│    - Join products (category, subcategory)│
│    - Round profit to 2 decimals         │
│    - Extract order_year                 │
│  Dimensions:                            │
│    - Select relevant columns            │
│    - Apply business logic               │
└─────────────────────────────────────────┘
      ↓
┌─────────────────────────────────────────┐
│  GOLD: Aggregations                     │
│  - Group by year, category, customer    │
│  - SUM(profit) rounded to 2 decimals    │
│  - Create pre-aggregated tables         │
└─────────────────────────────────────────┘
      ↓
┌─────────────────────────────────────────┐
│  ANALYTICS: SQL Queries                 │
│  - Ad-hoc analysis                      │
│  - Dashboard queries                    │
│  - Business metrics                     │
└─────────────────────────────────────────┘
```

### Key Transformations

#### Refined Orders
```python
# Data cleaning example
- trim(order_id)
- to_date(order_date, 'd/M/yyyy')
- try_cast(quantity AS INTEGER)
- try_cast(profit AS DOUBLE)
```

#### Enriched Orders
```python
# Business logic
- LEFT JOIN customers ON customer_id
- LEFT JOIN products ON product_id
- ROUND(profit, 2) AS profit
- YEAR(order_date) AS order_year
```

#### Profit Aggregate
```python
# Aggregation logic
GROUP BY 
  order_year,
  product_category,
  product_sub_category,
  customer_id,
  customer_name
AGGREGATE
  ROUND(SUM(profit), 2) AS total_profit
```

---

## ✨ Key Features

### 1. **Modular Design**
- Reusable transformation functions
- Separation of concerns (transformation logic vs orchestration)
- Easy to maintain and extend

### 2. **Data Quality Framework**
- YAML-based DQ rules configuration
- Automated validation with Databricks DQX
- Quarantine tables for failed records
- Metadata tracking for auditability

### 3. **Error Handling**
- Graceful handling of invalid data
- Use of `try_cast` and `to_date` for safe conversions
- NULL values for parse failures (not exceptions)
- Comprehensive logging

### 4. **Test-Driven Development**
- 32 unit tests covering all transformation logic
- DataFrame equality assertions using `chispa`
- Edge case and boundary testing
- Mocking for external dependencies

### 5. **Performance Optimization**
- Delta Lake for ACID transactions
- Partitioning strategies (by year, category)
- Broadcast joins for dimension tables
- Incremental processing support

### 6. **Scalability**
- PySpark for distributed processing
- Horizontal scaling via Databricks clusters
- Efficient memory management
- Optimized query plans

---

## 🛠️ Technologies Used

| Technology | Purpose | Version |
|------------|---------|----------|
| **Databricks** | Unified analytics platform | Runtime 14.3 LTS |
| **Apache Spark** | Distributed data processing | 3.5.0 |
| **PySpark** | Python API for Spark | 3.5.0 |
| **Delta Lake** | Storage layer with ACID | 3.1.0 |
| **Python** | Programming language | 3.12 |
| **Pytest** | Testing framework | 8.3.5 |
| **Chispa** | DataFrame testing utilities | 0.10.1 |
| **Unity Catalog** | Data governance | Latest |

---

## 📊 Sample Analytics Output

### Profit by Year
```sql
SELECT order_year, SUM(total_profit) as yearly_profit
FROM profit_aggregate
GROUP BY order_year
ORDER BY order_year;
```

### Profit by Year + Category
```sql
SELECT 
  order_year,
  product_category,
  SUM(total_profit) as category_profit
FROM profit_aggregate
GROUP BY order_year, product_category
ORDER BY order_year, category_profit DESC;
```

### Top Customers by Profit
```sql
SELECT 
  customer_name,
  SUM(total_profit) as customer_profit
FROM profit_aggregate
GROUP BY customer_name
ORDER BY customer_profit DESC
LIMIT 10;
```

---

## 🤝 Contributing

This is an assessment project. For questions or clarifications, please contact the repository owner.

---

## 📄 License

This project is part of the PEI Data Engineering Assessment.

---

## 📧 Contact

For any queries regarding this assessment:
- **GitHub**: [Your GitHub Profile]
- **Email**: akshaygalaxy23@gmail.com

---

**Last Updated**: May 22, 2026
**Status**: ✅ All requirements completed | 32/32 tests passing