import json
import os
import sys
from datetime import datetime, timezone

sys.path.append(os.path.abspath(".."))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from extract.api_client import APIClient
from storage.bronze_writer import write_to_bronze
from transform.generate_mock_orders import generate_mock_orders
from transform.silver_dummy_products import build_silver_dummy_products
from transform.silver_escuela_products import build_silver_escuela_products
from transform.silver_orders import build_silver_orders
from transform.silver_users import build_silver_users
from transform.gold_revenue_summary import build_gold_revenue_summary
from transform.gold_customer_ltv import build_gold_customer_ltv
from transform.gold_monthly_revenue import build_gold_monthly_revenue
from utils.logger import get_project_logger

logger = get_project_logger(__name__)

spark = SparkSession.builder.getOrCreate()

STORAGE_ACCOUNT = "rgecommerceanalytics"
BRONZE_BASE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_BASE = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_BASE = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"


def to_bronze_df(raw_data, source_name):
    if isinstance(raw_data, dict):
        raw_data = [raw_data]
    rows = [
        {"raw_data": json.dumps(r), "source": source_name, "loaded_at": datetime.now(timezone.utc).isoformat()}
        for r in raw_data
    ]
    df = spark.createDataFrame(rows)
    return df.withColumn("ingestion_id", F.monotonically_increasing_id())



logger.info("Starting extract phase")

dummy_client = APIClient(base_url="https://dummyjson.com")
escuela_client = APIClient(base_url="https://api.escuelajs.co/api/v1")

dummy_products_raw = dummy_client.get("products", params={"limit": 0})["products"]
dummy_categories_raw = dummy_client.get("products/categories")
escuela_products_raw = escuela_client.get("products")
escuela_users_raw = escuela_client.get("users")

logger.info(
    "Extracted %d dummy products, %d escuela products, %d escuela users",
    len(dummy_products_raw), len(escuela_products_raw), len(escuela_users_raw),
)

write_to_bronze(to_bronze_df(dummy_products_raw, "dummyjson"), f"{BRONZE_BASE}/dummy_products/")
write_to_bronze(to_bronze_df(dummy_categories_raw, "dummyjson"), f"{BRONZE_BASE}/dummy_categories/")
write_to_bronze(to_bronze_df(escuela_products_raw, "escuela"), f"{BRONZE_BASE}/escuela_products/")
write_to_bronze(to_bronze_df(escuela_users_raw, "escuela"), f"{BRONZE_BASE}/escuela_users/")

logger.info("Starting Silver layer build")


build_silver_dummy_products(spark, BRONZE_BASE, SILVER_BASE)
build_silver_escuela_products(spark, BRONZE_BASE, SILVER_BASE)
build_silver_users(spark, BRONZE_BASE, SILVER_BASE)


logger.info("Generating mock orders")
mock_orders_df = generate_mock_orders(spark, SILVER_BASE)
write_to_bronze(mock_orders_df, f"{BRONZE_BASE}/orders/")


build_silver_orders(spark, BRONZE_BASE, SILVER_BASE)

logger.info("Starting Gold layer build")
build_gold_revenue_summary(spark, SILVER_BASE, GOLD_BASE)
build_gold_customer_ltv(spark, SILVER_BASE, GOLD_BASE)
build_gold_monthly_revenue(spark, SILVER_BASE, GOLD_BASE)
logger.info("Pipeline completed successfully")
