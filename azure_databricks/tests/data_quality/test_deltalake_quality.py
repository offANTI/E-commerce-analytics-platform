from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)




def assert_schema(df, expected_schema):

    assert df.schema == expected_schema, (
        f"Schema mismatch.\n"
        f"Expected:\n{expected_schema}\n"
        f"Actual:\n{df.schema}"
    )


def assert_not_null(df, columns):
    for column in columns:
        count = df.filter(F.col(column).isNull()).count()
        assert count == 0, (
            f"Column '{column}' contains {count} NULL values"
        )


def assert_unique(df, columns):
    total_count = df.count()
    unique_count = df.select(*columns).distinct().count()

    assert total_count == unique_count, (
        f"Duplicate values found for key {columns}: "
        f"{total_count} rows vs {unique_count} unique keys"
    )


def assert_non_negative(df, column):
    invalid_count = df.filter(
        F.col(column) < 0
    ).count()

    assert invalid_count == 0, (
        f"Column '{column}' contains {invalid_count} negative values"
    )


def assert_positive(df, column):
    invalid_count = df.filter(
        F.col(column) <= 0
    ).count()

    assert invalid_count == 0, (
        f"Column '{column}' contains {invalid_count} "
        f"non-positive values"
    )


def assert_valid_timestamp(df, column):
    invalid_count = (
        df.withColumn(
            "_parsed_ts",
            F.to_timestamp(F.col(column)),
        )
        .filter(F.col("_parsed_ts").isNull())
        .count()
    )

    assert invalid_count == 0, (
        f"Column '{column}' contains {invalid_count} invalid timestamps"
    )




BRONZE_SCHEMA = StructType(
    [
        StructField("raw_data", StringType(), False),
        StructField("source", StringType(), False),
        StructField("loaded_at", StringType(), False),
        StructField("ingestion_id", LongType(), False),
    ]
)


SILVER_PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), False),
        StructField("title", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("category_id", IntegerType(), True),
    ]
)


SILVER_USERS_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), False),
        StructField("email", StringType(), False),
        StructField("name", StringType(), True),
        StructField("role", StringType(), True),
    ]
)


SILVER_ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("order_date", StringType(), False),
    ]
)


GOLD_REVENUE_SCHEMA = StructType(
    [
        StructField("store_name", StringType(), False),
        StructField("order_date", StringType(), False),
        StructField("total_orders", LongType(), False),
        StructField("total_revenue", DoubleType(), False),
        StructField("avg_order_value", DoubleType(), False),
        StructField("total_items_sold", LongType(), False),
    ]
)



class TestBronzeLayer:

    def test_bronze_schema(self, spark):
        df = spark.createDataFrame(
            [
                (
                    '{"id": 1}',
                    "dummyjson",
                    "2026-01-01T00:00:00Z",
                    1,
                )
            ],
            BRONZE_SCHEMA,
        )

        assert_schema(df, BRONZE_SCHEMA)

    def test_bronze_required_columns_not_null(self, spark):
        df = spark.createDataFrame(
            [
                (
                    '{"id": 1}',
                    "dummyjson",
                    "2026-01-01T00:00:00Z",
                    1,
                ),
                (
                    '{"id": 2}',
                    "escuela",
                    "2026-01-01T01:00:00Z",
                    2,
                ),
            ],
            BRONZE_SCHEMA,
        )

        assert_not_null(
            df,
            [
                "raw_data",
                "source",
                "loaded_at",
                "ingestion_id",
            ],
        )

    def test_bronze_loaded_at_is_valid(self, spark):
        df = spark.createDataFrame(
            [
                (
                    '{"id": 1}',
                    "dummyjson",
                    "2026-01-01T00:00:00Z",
                    1,
                ),
            ],
            BRONZE_SCHEMA,
        )

        assert_valid_timestamp(df, "loaded_at")

    def test_bronze_ingestion_id_unique(self, spark):
        df = spark.createDataFrame(
            [
                (
                    '{"id": 1}',
                    "dummyjson",
                    "2026-01-01T00:00:00Z",
                    1,
                ),
                (
                    '{"id": 2}',
                    "dummyjson",
                    "2026-01-01T00:01:00Z",
                    2,
                ),
            ],
            BRONZE_SCHEMA,
        )

        assert_unique(df, ["ingestion_id"])



class TestSilverProducts:

    def test_schema(self, spark):
        df = spark.createDataFrame(
            [
                (1, "Laptop", 1000.0, 10),
            ],
            SILVER_PRODUCTS_SCHEMA,
        )

        assert_schema(df, SILVER_PRODUCTS_SCHEMA)

    def test_product_id_not_null(self, spark):
        df = spark.createDataFrame(
            [
                (1, "Laptop", 1000.0, 10),
                (2, "Phone", 500.0, 20),
            ],
            SILVER_PRODUCTS_SCHEMA,
        )

        assert_not_null(df, ["product_id"])

    def test_product_id_unique(self, spark):
        df = spark.createDataFrame(
            [
                (1, "Laptop", 1000.0, 10),
                (2, "Phone", 500.0, 20),
            ],
            SILVER_PRODUCTS_SCHEMA,
        )

        assert_unique(df, ["product_id"])

    def test_price_non_negative(self, spark):
        df = spark.createDataFrame(
            [
                (1, "Laptop", 1000.0, 10),
                (2, "Phone", 0.0, 20),
            ],
            SILVER_PRODUCTS_SCHEMA,
        )

        assert_non_negative(df, "price")




class TestSilverUsers:

    def test_schema(self, spark):
        df = spark.createDataFrame(
            [
                (
                    1,
                    "user@example.com",
                    "John Doe",
                    "customer",
                )
            ],
            SILVER_USERS_SCHEMA,
        )

        assert_schema(df, SILVER_USERS_SCHEMA)

    def test_required_columns_not_null(self, spark):
        df = spark.createDataFrame(
            [
                (
                    1,
                    "user1@example.com",
                    "John",
                    "customer",
                ),
                (
                    2,
                    "user2@example.com",
                    "Anna",
                    "customer",
                ),
            ],
            SILVER_USERS_SCHEMA,
        )

        assert_not_null(
            df,
            [
                "user_id",
                "email",
            ],
        )

    def test_user_id_unique(self, spark):
        df = spark.createDataFrame(
            [
                (1, "a@example.com", "A", "customer"),
                (2, "b@example.com", "B", "customer"),
            ],
            SILVER_USERS_SCHEMA,
        )

        assert_unique(df, ["user_id"])




class TestSilverOrders:

    def test_schema(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "ORD-1",
                    1,
                    100,
                    2,
                    200.0,
                    "2026-01-01",
                )
            ],
            SILVER_ORDERS_SCHEMA,
        )

        assert_schema(df, SILVER_ORDERS_SCHEMA)

    def test_required_columns_not_null(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "ORD-1",
                    1,
                    100,
                    2,
                    200.0,
                    "2026-01-01",
                ),
                (
                    "ORD-2",
                    2,
                    101,
                    1,
                    50.0,
                    "2026-01-02",
                ),
            ],
            SILVER_ORDERS_SCHEMA,
        )

        assert_not_null(
            df,
            [
                "order_id",
                "user_id",
                "product_id",
                "quantity",
                "total_amount",
                "order_date",
            ],
        )

    def test_order_id_unique(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "ORD-1",
                    1,
                    100,
                    2,
                    200.0,
                    "2026-01-01",
                ),
                (
                    "ORD-2",
                    2,
                    101,
                    1,
                    50.0,
                    "2026-01-02",
                ),
            ],
            SILVER_ORDERS_SCHEMA,
        )

        assert_unique(df, ["order_id"])

    def test_quantity_positive(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "ORD-1",
                    1,
                    100,
                    1,
                    100.0,
                    "2026-01-01",
                ),
                (
                    "ORD-2",
                    2,
                    101,
                    3,
                    150.0,
                    "2026-01-02",
                ),
            ],
            SILVER_ORDERS_SCHEMA,
        )

        assert_positive(df, "quantity")

    def test_total_amount_non_negative(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "ORD-1",
                    1,
                    100,
                    1,
                    100.0,
                    "2026-01-01",
                ),
                (
                    "ORD-2",
                    2,
                    101,
                    2,
                    200.0,
                    "2026-01-02",
                ),
            ],
            SILVER_ORDERS_SCHEMA,
        )

        assert_non_negative(df, "total_amount")




class TestGoldRevenue:

    def test_schema(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "Store A",
                    "2026-01-01",
                    10,
                    1000.0,
                    100.0,
                    25,
                )
            ],
            GOLD_REVENUE_SCHEMA,
        )

        assert_schema(df, GOLD_REVENUE_SCHEMA)

    def test_required_columns_not_null(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "Store A",
                    "2026-01-01",
                    10,
                    1000.0,
                    100.0,
                    25,
                ),
            ],
            GOLD_REVENUE_SCHEMA,
        )

        assert_not_null(
            df,
            [
                "store_name",
                "order_date",
                "total_orders",
                "total_revenue",
                "avg_order_value",
                "total_items_sold",
            ],
        )

    def test_revenue_non_negative(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "Store A",
                    "2026-01-01",
                    10,
                    1000.0,
                    100.0,
                    25,
                ),
            ],
            GOLD_REVENUE_SCHEMA,
        )

        assert_non_negative(df, "total_revenue")

    def test_one_row_per_store_and_date(self, spark):
        df = spark.createDataFrame(
            [
                (
                    "Store A",
                    "2026-01-01",
                    10,
                    1000.0,
                    100.0,
                    25,
                ),
                (
                    "Store A",
                    "2026-01-02",
                    5,
                    500.0,
                    100.0,
                    10,
                ),
                (
                    "Store B",
                    "2026-01-01",
                    3,
                    300.0,
                    100.0,
                    7,
                ),
            ],
            GOLD_REVENUE_SCHEMA,
        )

        assert_unique(
            df,
            [
                "store_name",
                "order_date",
            ],
        )