from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def deduplicate_by_key(df, key_column, order_column="loaded_at"):
    window = Window.partitionBy(key_column).orderBy(
        F.col(order_column).desc(),
        F.col("ingestion_id").desc(),
    )
    return (
        df
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )