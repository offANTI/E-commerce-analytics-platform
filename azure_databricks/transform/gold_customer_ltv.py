from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from storage.delta_writer import write_delta


def build_gold_customer_ltv(spark: SparkSession, silver_base: str, gold_base: str) -> None:
    silver_orders = spark.read.format("delta").load(f"{silver_base}/silver_orders/")

    aggregated = (
        silver_orders
        .groupBy("user_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("lifetime_value"),
            F.avg("total_amount").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
        .withColumn(
            "customer_lifespan_days",
            F.datediff(F.col("last_order_date"), F.col("first_order_date")),
        )
    )

    window = Window.orderBy(F.col("lifetime_value").desc())
    df = aggregated.withColumn("ltv_rank", F.rank().over(window))

    write_delta(df=df, path=f"{gold_base}/gold_customer_ltv/", mode="overwrite")