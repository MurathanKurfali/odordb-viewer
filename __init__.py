"""Simple unified odor DB over local datasets/, with GUI support."""

from .config import get_default_db_path  # noqa: F401
from .schema import get_connection, initialize_database  # noqa: F401

__all__ = ["get_default_db_path", "get_connection", "initialize_database"]

__version__ = "0.3.2"
