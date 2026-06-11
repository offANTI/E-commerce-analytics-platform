import json
from typing import Any

from sqlalchemy import text

from database.connection import get_connection
from utils.logger import get_project_logger


logger = get_project_logger(__name__)


ALLOWED_BRONZE_TABLES = {
    "bronze.dummy_products",
    "bronze.dummy_categories",
    "bronze.escuela_products",
    "bronze.escuela_categories",
    "bronze.escuela_users",
}


def load_to_bronze(raw_data: dict[str, Any] | list[dict[str, Any]], table_name: str) -> None:
    if table_name not in ALLOWED_BRONZE_TABLES:
        raise ValueError(f"Table is not allowed for bronze load: {table_name}")

    logger.info("Loading raw data to Bronze table: %s", table_name)

    query = text(f"INSERT INTO {table_name} (raw_data) VALUES (:data)")

    try:
        with get_connection() as connection:
            connection.execute(query, {"data": json.dumps(raw_data)})
            connection.commit()

        logger.info("Raw data successfully loaded to Bronze table: %s", table_name)

    except Exception:
        logger.exception("Failed to load raw data to Bronze table: %s", table_name)
        raise