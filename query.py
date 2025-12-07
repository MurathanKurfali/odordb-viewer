from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import sqlite3


@dataclass
class OdorRow:
    unified_odor_id: str
    name: Optional[str]
    cid: Optional[str]
    cas: Optional[str]
    smiles: Optional[str]
    slug_first: Optional[str]
    stimulus_first: Optional[str]


def list_odors(
    conn: sqlite3.Connection,
    limit: Optional[int] = None,
) -> List[OdorRow]:
    """Return odors ordered by name/stimulus/id."""
    sql = """        SELECT unified_odor_id, name, cid, cas, smiles, slug_first, stimulus_first
    FROM odors
    ORDER BY COALESCE(name, stimulus_first, unified_odor_id) ASC
    """
    params: list[object] = []
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(sql, tuple(params)).fetchall()
    return [
        OdorRow(
            unified_odor_id=row[0],
            name=row[1],
            cid=row[2],
            cas=row[3],
            smiles=row[4],
            slug_first=row[5],
            stimulus_first=row[6],
        )
        for row in rows
    ]


def search_odors(
    conn: sqlite3.Connection,
    query: str,
    limit: Optional[int] = None,
) -> List[OdorRow]:
    """Search odors by ID, name, CID, CAS, or first stimulus."""
    q = f"%{query.strip()}%"
    sql = """        SELECT unified_odor_id, name, cid, cas, smiles, slug_first, stimulus_first
    FROM odors
    WHERE unified_odor_id LIKE ?
       OR COALESCE(name, '') LIKE ?
       OR COALESCE(cid, '') LIKE ?
       OR COALESCE(cas, '') LIKE ?
       OR COALESCE(stimulus_first, '') LIKE ?
    ORDER BY COALESCE(name, stimulus_first, unified_odor_id) ASC
    """
    params: list[object] = [q, q, q, q, q]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(sql, tuple(params)).fetchall()
    return [
        OdorRow(
            unified_odor_id=row[0],
            name=row[1],
            cid=row[2],
            cas=row[3],
            smiles=row[4],
            slug_first=row[5],
            stimulus_first=row[6],
        )
        for row in rows
    ]


def get_odor_facts(
    conn: sqlite3.Connection,
    unified_odor_id: str,
):
    """Yield all facts for a given odor, ordered by dataset/file/column."""
    sql = """        SELECT slug, file, column, value_text, value_num
    FROM odor_facts
    WHERE unified_odor_id = ?
    ORDER BY slug, file, column
    """
    for row in conn.execute(sql, (unified_odor_id,)):
        slug, file, column, value_text, value_num = row
        value = value_text if value_text is not None else value_num
        yield {
            "slug": slug,
            "file": file,
            "column": column,
            "value": value,
        }


def get_datasets_for_odor(
    conn: sqlite3.Connection,
    unified_odor_id: str,
) -> List[str]:
    """Return a sorted list of dataset slugs in which this odor appears."""
    sql = """        SELECT DISTINCT slug
    FROM odor_facts
    WHERE unified_odor_id = ?
    ORDER BY slug
    """
    rows = conn.execute(sql, (unified_odor_id,)).fetchall()
    return [r[0] for r in rows]


def list_all_datasets(conn: sqlite3.Connection) -> List[str]:
    """Return a sorted list of all dataset slugs present in odor_facts."""
    sql = """        SELECT DISTINCT slug
    FROM odor_facts
    ORDER BY slug
    """
    rows = conn.execute(sql).fetchall()
    return [r[0] for r in rows]


def descriptor_search(
    conn: sqlite3.Connection,
    text: str,
    dataset: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[OdorRow]:
    """Search odors by descriptor text in descriptor-like columns.

    This finds odors where:
    - odor_facts.column contains 'descriptor' (case-insensitive), AND
    - odor_facts.value_text contains the given text (case-insensitive).

    Optionally restricted to a specific dataset slug.
    """
    q_value = f"%{text.strip()}%"
    sql = """        SELECT DISTINCT o.unified_odor_id, o.name, o.cid, o.cas, o.smiles, o.slug_first, o.stimulus_first
    FROM odors o
    JOIN odor_facts f ON f.unified_odor_id = o.unified_odor_id
    WHERE LOWER(f.column) LIKE '%descriptor%'
      AND COALESCE(LOWER(f.value_text), '') LIKE LOWER(?)
    """
    params: list[object] = [q_value]

    if dataset:
        sql += " AND f.slug = ?"
        params.append(dataset)

    sql += " ORDER BY COALESCE(o.name, o.stimulus_first, o.unified_odor_id) ASC"

    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(sql, tuple(params)).fetchall()
    return [
        OdorRow(
            unified_odor_id=row[0],
            name=row[1],
            cid=row[2],
            cas=row[3],
            smiles=row[4],
            slug_first=row[5],
            stimulus_first=row[6],
        )
        for row in rows
    ]
