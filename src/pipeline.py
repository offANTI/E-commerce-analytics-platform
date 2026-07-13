from config.settings import settings
from database.connection import run_sql_file
from src.extract.api_client import extract_from_api
from src.load.postgres_loader import load_to_bronze
from src.transform.orders import generate_mock_orders
from utils.logger import get_project_logger

logger = get_project_logger(__name__)

#test comment
ETL_TASKS = [
    {
        "url": settings.DUMMY_PRODUCTS_URL,
        "table": "bronze.dummy_products",
        "is_critical": True,
    },
    {
        "url": settings.DUMMY_CATEGORIES_URL,
        "table": "bronze.dummy_categories",
        "is_critical": False,
    },
    {
        "url": settings.ESCUELA_PRODUCTS_URL,
        "table": "bronze.escuela_products",
        "is_critical": False,
    },
    {
        "url": settings.ESCUELA_CATEGORIES_URL,
        "table": "bronze.escuela_categories",
        "is_critical": False,
    },
    {
        "url": settings.ESCUELA_USERS_URL,
        "table": "bronze.escuela_users",
        "is_critical": False,
    },
]


def run_bronze_pipeline() -> None:
    logger.info("Starting Bronze Layer pipeline.")

    for task in ETL_TASKS:
        try:
            data = extract_from_api(str(task["url"]))

            if not data:
                raise ValueError("API returned empty data.")

            load_to_bronze(raw_data=data, table_name=task["table"])

        except Exception:
            logger.exception("Failed Bronze task: %s", task["url"])

            if task["is_critical"]:
                raise

    logger.info("Bronze Layer pipeline finished.")


def run_silver_pipeline() -> None:
    logger.info("Starting Silver Layer pipeline.")
    run_sql_file("silver_views.sql")
    logger.info("Silver Layer pipeline finished.")


def run_orders_simulation() -> None:
    logger.info("Starting Orders simulation.")
    generate_mock_orders(order_count=1000)
    logger.info("Orders simulation finished.")


def run_gold_pipeline() -> None:
    logger.info("Starting Gold Layer pipeline.")
    run_sql_file("gold_analytics.sql")
    logger.info("Gold Layer pipeline finished.")


def run_full_pipeline() -> None:
    logger.info("Starting full E-commerce Analytics ETL pipeline.")
    run_sql_file("bronze_tables.sql")
    run_bronze_pipeline()
    run_silver_pipeline()
    run_orders_simulation()
    run_gold_pipeline()

    logger.info("Full ETL pipeline finished successfully.")
