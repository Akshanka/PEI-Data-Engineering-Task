from pyspark.sql import functions as F


def transform_profit_aggregate(enriched_orders_df):
    return (
        enriched_orders_df
        .groupBy(
            "order_year",
            "product_category",
            "product_sub_category",
            "customer_id",
            "customer_name"
        )
        .agg(
            F.round(F.sum("profit"), 2)
             .cast("decimal(18,2)")
             .alias("total_profit")
        )
    )