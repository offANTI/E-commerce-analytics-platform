from pyspark.sql import functions as F


def test_filter_invalid_orders(spark):
    rows = [
        ("ORD-1", 1, 100.0),
        ("ORD-2", 2, -50.0),
        ("ORD-3", 3, 25.0),
    ]

    df = spark.createDataFrame(
        rows,
        ["order_id", "user_id", "total_amount"],
    )

    result = df.filter(F.col("total_amount") >= 0)

    assert result.count() == 2

    order_ids = {
        row["order_id"]
        for row in result.collect()
    }

    assert order_ids == {"ORD-1", "ORD-3"}


def test_order_total_calculation(spark):
    rows = [
        (1, 10.0, 2),
        (2, 25.0, 3),
    ]

    df = spark.createDataFrame(
        rows,
        ["product_id", "price", "quantity"],
    )

    result = df.withColumn(
        "total_amount",
        F.col("price") * F.col("quantity"),
    )

    values = {
        row["product_id"]: row["total_amount"]
        for row in result.collect()
    }

    assert values[1] == 20.0
    assert values[2] == 75.0


def test_empty_input_returns_empty_result(spark):
    df = spark.createDataFrame(
        [],
        """
        order_id STRING,
        quantity INT,
        total_amount DOUBLE
        """,
    )

    result = df.filter(F.col("quantity") > 0)

    assert result.count() == 0