from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from storage.silver_writer import write_to_silver


def build_silver_escuela_products(spark: SparkSession, bronze_base: str, silver_base: str) -> None:
    schema = "id INT, title STRING, price DOUBLE, category STRUCT<id: INT>"

    df = (
        spark.read.format("delta").load(f"{bronze_base}/escuela_products/")
        .withColumn("parsed", F.from_json("raw_data", schema))
        .select(
            F.col("parsed.id").alias("product_id"),
            F.col("parsed.title").alias("title"),
            F.when(
                F.col("parsed.price").between(1, 10000), F.col("parsed.price").cast("decimal(12,2)")
            ).otherwise(F.lit(None)).alias("price"),
            F.col("parsed.price").between(1, 10000).alias("is_valid_price"),
            F.lit(0.00).cast("decimal(5,2)").alias("discount_percentage"),
            F.lit(None).cast("decimal(3,2)").alias("rating"),
            F.lit(None).cast("string").alias("brand"),
            F.col("parsed.category.id").alias("category_id"),
            F.col("loaded_at"),
        )
    )

    write_to_silver(df=df, path=f"{silver_base}/silver_products/", merge_key="product_id", spark=spark)