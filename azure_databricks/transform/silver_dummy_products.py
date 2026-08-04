from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from storage.silver_writer import write_to_silver
from transform.dedup import deduplicate_by_key


def build_silver_dummy_products(spark: SparkSession, bronze_base: str, silver_base: str) -> None:
    schema = "id INT, title STRING, brand STRING, category STRING, price DOUBLE, stock INT"

    parsed_df = (
        spark.read.format("delta").load(f"{bronze_base}/dummy_products/")
        .withColumn("parsed", F.from_json("raw_data", schema))
        .select(
            F.col("parsed.id").alias("id"),
            F.col("parsed.title").alias("product_title"),
            F.col("parsed.brand").alias("brand"),
            F.col("parsed.category").alias("category"),
            F.col("parsed.price").cast("decimal(10,2)").alias("price"),
            F.col("parsed.stock").cast("int").alias("stock"),
            F.col("loaded_at"),
        )
        .filter(F.col("price").isNotNull())
    )

    df = deduplicate_by_key(parsed_df, key_column="id")

    write_to_silver(df=df, path=f"{silver_base}/silver_dummy_products/", merge_key="id", spark=spark)