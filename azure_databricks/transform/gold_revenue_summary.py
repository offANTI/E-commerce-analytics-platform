from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from storage.delta_writer import write_delta


def build_gold_revenue_summary(spark: SparkSession, silver_base: str, gold_base: str) -> None:
    silver_orders = spark.read.format("delta").load(f"{silver_base}/silver_orders/")

    df = (
        silver_orders
        .groupBy("store_name", "order_date")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
            F.sum("quantity").alias("total_items_sold"),
        )
    )

    write_delta(df=df, path=f"{gold_base}/gold_revenue_summary/", mode="overwrite")