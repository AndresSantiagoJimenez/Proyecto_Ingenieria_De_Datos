"""Spark session factory configured with Delta Lake support."""

import os
import sys
from typing import Any


def get_spark_session(
    app_name: str | None = None,
    master: str | None = None,
    warehouse_dir: str | None = None,
) -> Any:
    """Create or retrieve an active SparkSession with Delta Lake extensions."""
    # Ensure PySpark workers use the exact active Python interpreter
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Ensure mismatched host SPARK_HOME is ignored in favor of the active environment
    if "SPARK_HOME" in os.environ and not os.environ["SPARK_HOME"].startswith(
        sys.prefix
    ):
        del os.environ["SPARK_HOME"]

    try:
        from pyspark.sql import SparkSession
    except ImportError as e:
        raise ImportError(
            "PySpark is not installed in the current environment. "
            "Please install it using 'pip install -r requirements.txt' or run within the Docker container."
        ) from e

    resolved_app_name = app_name or os.getenv("SPARK_APP_NAME", "OlistDeltaLakehouse")
    resolved_master = master or os.getenv("SPARK_MASTER", "local[*]")
    resolved_warehouse = warehouse_dir or os.getenv(
        "SPARK_WAREHOUSE_DIR", "./spark-warehouse"
    )

    builder = (
        SparkSession.builder.appName(resolved_app_name)
        .master(resolved_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", resolved_warehouse)
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
    )

    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except ImportError:
        pass

    return builder.getOrCreate()
