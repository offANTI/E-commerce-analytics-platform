import json
import os
import sys
from datetime import datetime, timezone

sys.path.append(os.path.abspath(".."))

from pyspark.sql import SparkSession

from extract.api_client import APIClient
from storage.bronze_writer import write_to_bronze
from transform.generate_mock_orders import generate_mock_orders
from transform.silver_dummy_products import build_silver_dummy_products
from transform.silver_escuela_products import build_silver_escuela_products
from transform.silver_orders import build_silver_orders
from transform.silver_users import build_silver_users
from utils.logger import get_project_logger

logger = get_project_logger(__name__)

spark = SparkSession.builder.getOrCreate()

STORAGE_ACCOUNT = "rgecommerceanalytics"
BRONZE_BASE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_BASE = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"

def to_bronze_df(raw_data, source_name):
    if isinstance(raw_data, dict):
        raw_data = [raw_data]
    rows = [
        {"raw_data": json.dumps(r), "source": source_name, "loaded_at": datetime.now(timezone.utc).isoformat()}
        for r in raw_data
    ]
    return spark.createDataFrame(rows)


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