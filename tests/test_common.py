from pyspark.testing.utils import assertDataFrameEqual

from src.utils.common import (
    build_source_path,
    build_table_name,
    standardize_column_names,
    add_ingestion_columns
)


def test_build_table_name():
    config = {
        "catalog": "dev",
        "raw": {
            "schema": "raw",
            "tables": {
                "orders": "raw_orders"
            }
        }
    }

    actual = build_table_name(config, "raw", "orders")

    assert actual == "dev.raw.raw_orders"


def test_build_source_path():
    config = {
        "source": {
            "base_path": "/Volumes/dev/landing/landing_zone",
            "files": {
                "orders": {
                    "file_name": "Orders.json"
                }
            }
        }
    }

    actual = build_source_path(config, "orders")

    assert actual == "/Volumes/dev/landing/landing_zone/Orders.json"


def test_standardize_column_names(spark):
    input_df = spark.createDataFrame(
        [(1, "ORD-1")],
        ["Row ID", "Order ID"]
    )

    actual_df = standardize_column_names(input_df)

    expected_df = spark.createDataFrame(
        [(1, "ORD-1")],
        ["row_id", "order_id"]
    )

    assertDataFrameEqual(actual_df, expected_df)


def test_add_ingestion_columns(spark):
    input_df = spark.createDataFrame(
        [(1, "ORD-1")],
        ["row_id", "order_id"]
    )

    actual_df = add_ingestion_columns(
        input_df,
        "Orders.json"
    )

    assert "source_file_name" in actual_df.columns
    assert "ingestion_timestamp" in actual_df.columns

    expected_source_file_name = (
        actual_df
        .select("source_file_name")
        .collect()[0][0]
    )

    assert expected_source_file_name == "Orders.json"