from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from storage.silver_writer import write_to_silver
from transform.dedup import deduplicate_by_key


def build_silver_users(spark: SparkSession, bronze_base: str, silver_base: str) -> None:
    schema = "id INT, email STRING, name STRING, role STRING, avatar STRING"

    parsed_df = (
        spark.read.format("delta").load(f"{bronze_base}/escuela_users/")
        .withColumn("parsed", F.from_json("raw_data", schema))
        .select(
            F.col("parsed.id").alias("user_id"),
            F.col("parsed.email").alias("email"),
            F.col("parsed.name").alias("name"),
            F.col("parsed.role").alias("role"),
            F.col("parsed.avatar").alias("avatar_url"),
            F.col("loaded_at"),
        )
        .filter(F.col("email").isNotNull())
    )

    df = deduplicate_by_key(parsed_df, key_column="user_id")

    write_to_silver(df=df, path=f"{silver_base}/silver_users/", merge_key="user_id", spark=spark)