from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_refined_orders(df: DataFrame) -> DataFrame:
    return df.select(
        F.expr("try_cast(trim(row_id) as BIGINT)").alias("row_id"),
        F.trim(F.col("order_id")).alias("order_id"),
        F.to_date(F.trim(F.col("order_date")), "d/M/yyyy").alias("order_date"),
        F.to_date(F.trim(F.col("ship_date")), "d/M/yyyy").alias("ship_date"),
        F.trim(F.col("ship_mode")).alias("ship_mode"),
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.trim(F.col("product_id")).alias("product_id"),
        F.expr("try_cast(trim(quantity) as INT)").alias("quantity"),
        F.expr("try_cast(trim(price) as DOUBLE)").alias("price"),
        F.expr("try_cast(trim(discount) as DOUBLE)").alias("discount"),
        F.expr("try_cast(trim(profit) as DOUBLE)").alias("profit"),
        F.col("ingestion_timestamp").alias("ingestion_timestamp")
    )


def transform_refined_customers(df: DataFrame) -> DataFrame:
    return df.select(
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.trim(F.col("customer_name")).alias("customer_name"),
        F.trim(F.col("email")).alias("email"),
        F.trim(F.col("phone")).alias("phone"),
        F.trim(F.col("address")).alias("address"),
        F.trim(F.col("segment")).alias("segment"),
        F.trim(F.col("country")).alias("country"),
        F.trim(F.col("city")).alias("city"),
        F.trim(F.col("state")).alias("state"),
        F.trim(F.col("postal_code")).alias("postal_code"),
        F.trim(F.col("region")).alias("region"),
        F.col("ingestion_timestamp").alias("ingestion_timestamp")
    )


def transform_refined_products(df: DataFrame) -> DataFrame:
    return df.select(
        F.trim(F.col("product_id")).alias("product_id"),
        F.trim(F.col("category")).alias("category"),
        F.trim(F.col("sub_category")).alias("sub_category"),
        F.trim(F.col("product_name")).alias("product_name"),
        F.trim(F.col("state")).alias("state"),
        F.expr("try_cast(trim(price_per_product) as DOUBLE)").alias("price_per_product"),
        F.col("ingestion_timestamp").alias("ingestion_timestamp")
    )