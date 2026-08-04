from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from storage.silver_writer import write_to_silver


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

    window = Window.partitionBy("user_id").orderBy(F.col("loaded_at").desc())

    df = (
        parsed_df
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    write_to_silver(df=df, path=f"{silver_base}/silver_users/", merge_key="user_id", spark=spark)