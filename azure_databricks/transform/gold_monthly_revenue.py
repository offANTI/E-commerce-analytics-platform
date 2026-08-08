from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from storage.delta_writer import write_delta


def build_gold_monthly_revenue(spark: SparkSession, silver_base: str, gold_base: str) -> None:
    silver_orders = spark.read.format("delta").load(f"{silver_base}/silver_orders/")

    df = (
        silver_orders
        .withColumn("order_month", F.trunc("order_date", "month"))
        .groupBy("order_month", "store_name")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("order_id").alias("orders_count"),
            F.avg("total_amount").alias("avg_order_value"),
        )
        .orderBy("order_month")
    )

    write_delta(df=df, path=f"{gold_base}/gold_monthly_revenue/", mode="overwrite")