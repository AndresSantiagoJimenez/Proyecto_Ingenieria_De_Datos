"""Storage abstraction and Delta Lake table management."""

from src.data_platform.storage.session import get_spark_session

__all__ = ["get_spark_session"]
