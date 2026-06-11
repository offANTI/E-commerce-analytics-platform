
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

from config.settings import BASE_DIR, settings
from utils.logger import get_project_logger

logger = get_project_logger(__name__)

engine = create_engine(settings.database_url)


def get_connection() -> Connection:
    logger.info("Connecting to PostgreSQL...")

    try:
        connection = engine.connect()
        logger.info("PostgreSQL connection established.")
        return connection
    except Exception as error:
        logger.exception("PostgreSQL connection failed.")
        raise error


def run_sql_file(file_name: str) -> None:
    sql_path = BASE_DIR / "database" / file_name

    logger.info("Running SQL script: %s", sql_path)

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql_script = sql_path.read_text(encoding="utf-8")

    try:
        with engine.begin() as connection:
            for statement in sql_script.split(";"):
                clean_statement = statement.strip()

                if clean_statement:
                    connection.execute(text(clean_statement))

        logger.info("SQL script executed successfully: %s", file_name)

    except Exception:
        logger.exception("Failed to execute SQL script: %s", file_name)
        raise
