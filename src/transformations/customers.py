from pyspark.sql import functions as F


def transform_enriched_customers(df):

    return (
        df.select(
            F.col("customer_id"),
            F.col("customer_name"),
            F.col("email"),
            F.col("phone"),
            F.col("address"),
            F.col("segment"),
            F.col("country"),
            F.col("city"),
            F.col("state"),
            F.col("postal_code"),
            F.col("region"),
            F.col("ingestion_timestamp")
        )
    )