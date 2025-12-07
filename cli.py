from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import sqlite3

from .config import get_default_db_path
from .schema import get_connection, initialize_database
from .ingest import ingest_all
from .query import OdorRow, get_odor_facts, list_odors, search_odors


def _open_db(path: Optional[str]) -> sqlite3.Connection:
    db_path = Path(path) if path is not None else get_default_db_path()
    conn = get_connection(db_path)
    initialize_database(conn)
    return conn


def cmd_init(args: argparse.Namespace) -> None:
    conn = _open_db(args.db)
    conn.close()
    print(f"[ok] Initialized database at: {args.db or get_default_db_path()}")


def cmd_ingest(args: argparse.Namespace) -> None:
    datasets_root = Path(args.datasets)
    conn = _open_db(args.db)
    ingest_all(conn, datasets_root=datasets_root)
    conn.close()
    print("[ok] Ingestion complete.")


def _print_odors(rows: list[OdorRow]) -> None:
    if not rows:
        print("No odors found.")
        return
    print(f"Listing {len(rows)} odor(s):")
    for r in rows:
        name = r.name or ""
        cid = r.cid or ""
        cas = r.cas or ""
        print(
            f"- {r.unified_odor_id:30s} "
            f"name={name!r} "
            f"cid={cid!r} "
            f"cas={cas!r}"
        )


def cmd_list(args: argparse.Namespace) -> None:
    conn = _open_db(args.db)
    rows = list_odors(conn, limit=args.limit)
    conn.close()
    _print_odors(rows)


def cmd_search(args: argparse.Namespace) -> None:
    conn = _open_db(args.db)
    rows = search_odors(conn, query=args.query, limit=args.limit)
    conn.close()
    print(f"[info] Search results for {args.query!r}:")
    _print_odors(rows)


def cmd_show(args: argparse.Namespace) -> None:
    conn = _open_db(args.db)
    odor_id = args.id
    rows = list(search_odors(conn, query=odor_id, limit=None))

    target_id = odor_id
    exact = [r for r in rows if r.unified_odor_id == odor_id]
    if exact:
        target_id = exact[0].unified_odor_id
    else:
        if not rows:
            print(f"[error] No odor found matching {odor_id!r}.")
            conn.close()
            return
        target_id = rows[0].unified_odor_id
        print(
            f"[info] No exact match for {odor_id!r}, "
            f"using closest candidate {target_id!r}."
        )

    basic = [r for r in rows if r.unified_odor_id == target_id]
    if not basic:
        cur = conn.execute(
            """                SELECT unified_odor_id, name, cid, cas, smiles, slug_first, stimulus_first
            FROM odors WHERE unified_odor_id = ?
            """,
            (target_id,),
        )
        row = cur.fetchone()
        if not row:
            print(f"[error] Odor {target_id!r} not found.")
            conn.close()
            return
        b = OdorRow(
            unified_odor_id=row[0],
            name=row[1],
            cid=row[2],
            cas=row[3],
            smiles=row[4],
            slug_first=row[5],
            stimulus_first=row[6],
        )
    else:
        b = basic[0]

    print("Odor")
    print("----")
    print(f"  unified_odor_id : {b.unified_odor_id}")
    print(f"  name            : {b.name or ''}")
    print(f"  cid             : {b.cid or ''}")
    print(f"  cas             : {b.cas or ''}")
    print(f"  smiles          : {b.smiles or ''}")

    print("\nFacts")
    print("-----")
    any_fact = False
    current_slug_file = None
    for fact in get_odor_facts(conn, target_id):
        slug = fact["slug"]
        file = fact["file"]
        key = (slug, file)
        if key != current_slug_file:
            current_slug_file = key
            print(f"\n[{slug} / {file}]")
        print(f"  {fact['column']}: {fact['value']}")
        any_fact = True

    if not any_fact:
        print("  (no facts found for this odor)")

    conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="odordb",
        description="Simple unified odor DB over local datasets/",
    )
    parser.add_argument(
        "--db",
        help="Path to SQLite DB file (default: ./.odordb_simple/odors.db)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    p_init = subparsers.add_parser(
        "init",
        help="Initialize the database file (create tables if needed)",
    )
    p_init.set_defaults(func=cmd_init)

    p_ingest = subparsers.add_parser(
        "ingest",
        help="Ingest all datasets from a root folder",
    )
    p_ingest.add_argument(
        "--datasets",
        required=True,
        help="Path to datasets root (folder with subfolders per dataset)",
    )
    p_ingest.set_defaults(func=cmd_ingest)

    p_list = subparsers.add_parser("list", help="List odors")
    p_list.add_argument(
        "--limit",
        type=int,
        help="Limit number of odors shown (default: all)",
    )
    p_list.set_defaults(func=cmd_list)

    p_search = subparsers.add_parser("search", help="Search odors by text/id")
    p_search.add_argument(
        "--query",
        required=True,
        help="Text to search in name, CID, CAS, unified ID, or first stimulus.",
    )
    p_search.add_argument(
        "--limit",
        type=int,
        help="Limit number of results (default: all)",
    )
    p_search.set_defaults(func=cmd_search)

    p_show = subparsers.add_parser(
        "show", help="Show all values attached to an odor across datasets."
    )
    p_show.add_argument(
        "--id",
        required=True,
        help="Unified ID (like CID:xxxx, CAS:xxxx, or any matching text).",
    )
    p_show.set_defaults(func=cmd_show)

    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
