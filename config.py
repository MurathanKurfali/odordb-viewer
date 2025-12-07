from __future__ import annotations

import os
from pathlib import Path


DEFAULT_DB_FILENAME = "odors.db"


def get_default_data_dir() -> Path:
    """Return the default directory where the database file lives.

    This version uses the current working directory
    """
    home = Path(os.getcwd())
    return home


def get_default_db_path() -> Path:
    """Return the full path to the default SQLite database file."""
    return get_default_data_dir() / DEFAULT_DB_FILENAME
