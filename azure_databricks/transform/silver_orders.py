from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from storage.silver_writer import write_to_silver


def build_silver_orders(spark: SparkSession, bronze_base: str, silver_base: str) -> None:
    df = (
        spark.read.format("delta").load(f"{bronze_base}/orders/")
        .withColumn("order_date", F.to_date("order_date"))
        .filter(
            (F.col("total_amount") > 0)
            & (F.col("quantity") > 0)
            & (F.col("order_date").isNotNull())
        )
    )

    write_to_silver(df=df, path=f"{silver_base}/silver_orders/", merge_key="order_id", spark=spark)