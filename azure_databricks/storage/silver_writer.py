from pyspark.sql import DataFrame, SparkSession

from storage.delta_writer import merge_into_delta


def write_to_silver(df: DataFrame, path: str, merge_key: str, spark: SparkSession) -> None:
    merge_into_delta(df=df, path=path, merge_key=merge_key, spark=spark)