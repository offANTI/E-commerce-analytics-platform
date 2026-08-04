from pyspark.sql import DataFrame

from storage.delta_writer import write_delta


def write_to_bronze(df: DataFrame, path: str) -> None:
    write_delta(df=df, path=path, mode="append")