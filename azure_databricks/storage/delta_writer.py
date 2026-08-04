from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def write_delta(df: DataFrame, path: str, mode: str = "append") -> None:
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .save(path)
    )


def merge_into_delta(
    df: DataFrame,
    path: str,
    merge_key: str,
    spark: SparkSession,
) -> None:
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    if DeltaTable.isDeltaTable(spark, path):
        target = DeltaTable.forPath(spark, path)
        (
            target.alias("target")
            .merge(
                df.alias("source"),
                f"target.{merge_key} = source.{merge_key}",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").save(path)