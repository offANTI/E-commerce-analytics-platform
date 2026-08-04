from pyspark.sql import SparkSession

from storage.bronze_writer import write_to_bronze
from transform.silver_dummy_products import build_silver_dummy_products
from transform.generate_mock_orders import generate_mock_orders

spark = SparkSession.builder.getOrCreate()

STORAGE_ACCOUNT = "rgecommerceanalytics"
BRONZE_BASE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_BASE = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"


build_silver_dummy_products(spark, BRONZE_BASE, SILVER_BASE)

orders_df = generate_mock_orders(spark=spark, silver_base=SILVER_BASE, order_count=5000, seed=42)
write_to_bronze(orders_df, f"{BRONZE_BASE}/orders/")