from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path


SCHEMA = """    CREATE TABLE IF NOT EXISTS odors (
    unified_odor_id TEXT PRIMARY KEY,
    slug_first TEXT,
    stimulus_first TEXT,
    name TEXT,
    cid TEXT,
    cas TEXT,
    smiles TEXT
);

CREATE TABLE IF NOT EXISTS odor_facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    unified_odor_id TEXT NOT NULL,
    slug TEXT NOT NULL,
    file TEXT NOT NULL,
    column TEXT NOT NULL,
    value_text TEXT,
    value_num REAL
);

CREATE INDEX IF NOT EXISTS idx_odor_facts_odor ON odor_facts(unified_odor_id);
CREATE INDEX IF NOT EXISTS idx_odor_facts_slug ON odor_facts(slug);
"""


def get_connection(db_path: Path | str) -> sqlite3.Connection:
    """Create a connection to the SQLite database.

    We set check_same_thread=False so the connection can be used across
    Streamlit's internal threads without raising ProgrammingError.
    """
    path = Path(db_path)
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    return conn


def initialize_database(conn: sqlite3.Connection) -> None:
    """Ensure the database schema exists."""
    with closing(conn.cursor()) as cur:
        cur.executescript(SCHEMA)
    conn.commit()
