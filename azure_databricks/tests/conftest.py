import os
import sys
from pathlib import Path

import pytest


AZURE_DATABRICKS_DIR = Path(__file__).resolve().parents[1]

if str(AZURE_DATABRICKS_DIR) not in sys.path:
    sys.path.insert(0, str(AZURE_DATABRICKS_DIR))


@pytest.fixture(scope="session")
def spark():
    
    python_executable = sys.executable

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("ecommerce-analytics-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .getOrCreate()
    )

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    yield spark

    spark.stop()