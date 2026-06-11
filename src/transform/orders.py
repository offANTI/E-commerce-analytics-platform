import random
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text

from database.connection import engine
from utils.logger import get_project_logger


logger = get_project_logger(__name__)


def generate_mock_orders(order_count: int = 5000, seed: int = 42) -> None:
    logger.info("Starting mock orders generation. order_count=%s", order_count)

    random.seed(seed)

    with engine.begin() as connection:
        users = connection.execute(
            text("SELECT user_id FROM silver.escuela_users")
        ).fetchall()

        dummy_products = connection.execute(
            text("""
                SELECT product_id, price
                FROM silver.dummy_products
                WHERE price IS NOT NULL
                  AND price > 0
                  AND price <= 100000
            """)
        ).fetchall()

        escuela_products = connection.execute(
            text("""
                SELECT product_id, price
                FROM silver.escuela_products
                WHERE price IS NOT NULL
                  AND price > 0
                  AND price <= 100000
            """)
        ).fetchall()

        user_ids = [row[0] for row in users]

        product_pool: list[dict[str, Any]] = []

        for product_id, price in dummy_products:
            product_pool.append(
                {
                    "product_id": product_id,
                    "price": float(price),
                    "store_name": "DummyJSON",
                }
            )

        for product_id, price in escuela_products:
            product_pool.append(
                {
                    "product_id": product_id,
                    "price": float(price),
                    "store_name": "Escuela",
                }
            )

        if not user_ids:
            raise ValueError("No users found in silver.escuela_users")

        if not product_pool:
            raise ValueError("No products found in silver product tables")

        orders = []

        now = datetime.now(timezone.utc)

        for _ in range(order_count):
            user_id = random.choice(user_ids)
            product = random.choice(product_pool)

            quantity = random.randint(1, 5)
            unit_price = product["price"]
            total_amount = round(quantity * unit_price, 2)

            order_date = now - timedelta(days=random.randint(0, 90))

            orders.append(
                {
                    "user_id": user_id,
                    "product_id": product["product_id"],
                    "store_name": product["store_name"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "total_amount": total_amount,
                    "order_date": order_date,
                }
            )

        connection.execute(
            text("""
                INSERT INTO bronze.orders (
                    user_id,
                    product_id,
                    store_name,
                    quantity,
                    unit_price,
                    total_amount,
                    order_date
                )
                VALUES (
                    :user_id,
                    :product_id,
                    :store_name,
                    :quantity,
                    :unit_price,
                    :total_amount,
                    :order_date
                )
            """),
            orders,
        )

    logger.info("Mock orders generated successfully. rows=%s", order_count)