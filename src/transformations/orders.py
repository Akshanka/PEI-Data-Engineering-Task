from pyspark.sql import functions as F


def transform_enriched_orders(
    orders_df,
    customers_df,
    products_df
):

    return (
        orders_df.alias("o")

        .join(
            customers_df.alias("c"),
            on="customer_id",
            how="left"
        )

        .join(
            products_df.alias("p"),
            on="product_id",
            how="left"
        )

        .select(
            F.col("o.row_id"),
            F.col("o.order_id"),
            F.col("o.order_date"),
            F.col("o.ship_date"),
            F.col("o.ship_mode"),

            F.year(
                F.col("o.order_date")
            ).alias("order_year"),

            F.col("o.customer_id"),
            F.col("c.customer_name"),
            F.col("c.country"),

            F.col("o.product_id"),
            F.col("p.product_name"),
            F.col("p.category").alias("product_category"),
            F.col("p.sub_category").alias("product_sub_category"),

            F.col("o.quantity"),
            F.col("o.price"),
            F.col("o.discount"),
            F.round(
                F.col("o.profit"),
                2
            ).cast("decimal(18,2)")
            .alias("profit"),

            F.col("o.ingestion_timestamp")
        )
    )