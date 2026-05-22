from pyspark.sql import functions as F


def transform_enriched_products(df):

    return (
        df.select(
            F.col("product_id"),
            F.col("product_name"),
            F.col("category"),
            F.col("sub_category"),
            F.col("state"),
            F.col("price_per_product"),
            F.col("ingestion_timestamp")
        )
    )