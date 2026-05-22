import json
import re
from typing import Dict, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import pandas as pd

def read_config(config_path: str) -> Dict[str, Any]:
    """
    Reads the project configuration JSON file.
    """
    with open(config_path, "r") as file:
        return json.load(file)


def to_snake_case(column_name: str) -> str:
    """
    Converts column names like 'Order ID' or 'Sub-Category'
    into snake_case like 'order_id' or 'sub_category'.
    """
    column_name = column_name.strip()
    column_name = re.sub(r"[^a-zA-Z0-9]+", "_", column_name)
    column_name = re.sub(r"_+", "_", column_name)
    column_name = column_name.strip("_")
    return column_name.lower()


def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Standardizes all dataframe column names to snake_case.
    """
    for column_name in df.columns:
        df = df.withColumnRenamed(column_name, to_snake_case(column_name))
    return df


def add_ingestion_columns(df: DataFrame, source_file_name: str) -> DataFrame:
    """
    Adds common raw-layer audit columns.
    """
    return (
        df
        .withColumn("source_file_name", F.lit(source_file_name))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


def build_source_path(config: Dict[str, Any], file_key: str) -> str:
    """
    Builds full source file path from config.
    """
    base_path = config["source"]["base_path"]
    file_name = config["source"]["files"][file_key]["file_name"]

    return f"{base_path}/{file_name}"


def build_table_name(config: Dict[str, Any], layer: str, table_key: str) -> str:
    """
    Builds fully qualified Databricks table name.
    Example: dev.raw.raw_orders
    """
    catalog = config["catalog"]
    schema = config[layer]["schema"]
    table = config[layer]["tables"][table_key]

    return f"{catalog}.{schema}.{table}"


def read_json_file(
    spark: SparkSession,
    file_path: str,
    multi_line: bool = True
) -> DataFrame:
    """
    Reads a JSON file and returns a Spark DataFrame.
    """

    return (
        spark.read
        .option("multiLine", str(multi_line).lower())
        .option("primitivesAsString", "true")
        .json(file_path)
    )


def read_csv_file(
    spark: SparkSession,
    file_path: str,
    header: bool = True,
    infer_schema: bool = False
) -> DataFrame:
    """
    Reads a CSV file and returns a Spark DataFrame.
    """

    return (
        spark.read
        .option("header", str(header).lower())
        .option("inferSchema", str(infer_schema).lower())
        .csv(file_path)
    )


def read_excel_file(
    spark: SparkSession,
    file_path: str,
    sheet_name=0
) -> DataFrame:
    """
    Reads an Excel file using pandas and converts it to a Spark DataFrame.

    All columns are read as strings because raw layer should preserve
    source values without applying datatype casting.
    """

    pandas_df = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        engine="openpyxl",
        dtype=str,
        keep_default_na=False
    )

    # Ensure every column is string/object-safe before Spark conversion
    pandas_df = pandas_df.astype(str)

    return spark.createDataFrame(pandas_df)


def write_delta_table(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    """
    Writes a DataFrame to a Delta table.

    The table is already created in 01_setup.
    For this assignment, overwrite mode is used to make the raw load rerunnable.
    """

    (
        df.write
        .format("delta")
        .mode(mode)
        .saveAsTable(table_name)
    )