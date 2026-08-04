import random
from datetime import datetime, timedelta, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession


def generate_mock_orders(
    spark: SparkSession,
    silver_base: str,
    order_count: int = 5000,
    seed: int = 42,
) -> DataFrame:
    print(f"Starting mock orders generation. order_count={order_count}")
    random.seed(seed)

    users_df = spark.read.format("delta").load(f"{silver_base}/silver_users/")
    dummy_products_df = (
        spark.read.format("delta").load(f"{silver_base}/silver_dummy_products/")
        .filter("price IS NOT NULL AND price > 0 AND price <= 100000")
    )
    escuela_products_df = (
        spark.read.format("delta").load(f"{silver_base}/silver_products/")
        .filter("price IS NOT NULL AND price > 0 AND price <= 100000")
    )

    user_ids = [row["user_id"] for row in users_df.select("user_id").collect()]

    product_pool: list[dict[str, Any]] = []
    for row in dummy_products_df.select("product_id", "price").collect():
        product_pool.append({"product_id": row["product_id"], "price": float(row["price"]), "store_name": "DummyJSON"})
    for row in escuela_products_df.select("product_id", "price").collect():
        product_pool.append({"product_id": row["product_id"], "price": float(row["price"]), "store_name": "Escuela"})

    if not user_ids:
        raise ValueError("No users found in silver_users")
    if not product_pool:
        raise ValueError("No products found in silver product tables")

    now = datetime.now(timezone.utc)
    orders = []
    for _ in range(order_count):
        user_id = random.choice(user_ids)
        product = random.choice(product_pool)
        quantity = random.randint(1, 5)
        unit_price = product["price"]
        total_amount = round(quantity * unit_price, 2)
        order_date = now - timedelta(days=random.randint(0, 90))
        orders.append({
            "user_id": user_id,
            "product_id": product["product_id"],
            "store_name": product["store_name"],
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "order_date": order_date.isoformat(),
        })

    print(f"Mock orders generated successfully. rows={order_count}")
    return spark.createDataFrame(orders)