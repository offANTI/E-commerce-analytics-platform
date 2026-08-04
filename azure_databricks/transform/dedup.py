from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def deduplicate_by_key(
    df: DataFrame,
    key_column: str,
    order_column: str = "loaded_at",
) -> DataFrame:
    window = Window.partitionBy(key_column).orderBy(F.col(order_column).desc())

    return (
        df
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )