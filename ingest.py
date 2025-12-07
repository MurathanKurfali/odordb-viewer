from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Sequence

import pandas as pd
import sqlite3

from .schema import initialize_database


# Datasets with more rows than this (sum across behavior*.csv) are skipped.
MAX_ROWS_PER_DATASET = 5000


@dataclass
class Identity:
    unified_odor_id: str
    slug: str
    stimulus: str
    name: Optional[str] = None
    cid: Optional[str] = None
    cas: Optional[str] = None
    smiles: Optional[str] = None


# --- helpers -----------------------------------------------------------

def _safe_str(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        if pd.isna(value):  # type: ignore[arg-type]
            return None
    except Exception:
        pass
    text = str(value).strip()
    return text or None


def _pick_first_present(row: pd.Series, candidates: Sequence[str]) -> Optional[str]:
    for col in candidates:
        if col in row.index:
            val = _safe_str(row[col])
            if val is not None:
                return val
    return None


def _find_stimulus_column(df: pd.DataFrame) -> Optional[str]:
    for col in df.columns:
        if col.strip().lower() == "stimulus":
            return col
    return None


# --- loading stimuli / molecules --------------------------------------

def _load_stimuli(slug_dir: Path) -> Optional[pd.DataFrame]:
    stim_path = slug_dir / "stimuli.csv"
    if not stim_path.exists():
        return None
    try:
        df = pd.read_csv(stim_path)
    except Exception:
        print(f"[warn] {slug_dir.name}: could not read stimuli.csv, ignoring.")
        return None
    if df.empty:
        return None
    stim_col = _find_stimulus_column(df)
    if stim_col is None:
        # e.g. NatGeo has a typo "Simuli"; we skip that special case.
        return None
    return df


def _load_molecules(slug_dir: Path) -> Optional[pd.DataFrame]:
    mol_path = slug_dir / "molecules.csv"
    if not mol_path.exists():
        return None
    try:
        df = pd.read_csv(mol_path)
    except Exception:
        print(f"[warn] {slug_dir.name}: could not read molecules.csv, ignoring.")
        return None
    if df.empty:
        return None
    if "CID" not in df.columns:
        return None
    df = df.copy()
    df["CID"] = df["CID"].astype(str)
    return df


def _get_stimulus_row(stim_df: Optional[pd.DataFrame], stimulus_value: str) -> Optional[pd.Series]:
    if stim_df is None:
        return None
    stim_col = _find_stimulus_column(stim_df)
    if stim_col and stim_col in stim_df.columns:
        matches = stim_df[stim_col].astype(str) == str(stimulus_value)
        matches_df = stim_df[matches]
        if not matches_df.empty:
            return matches_df.iloc[0]
    return None


def _get_molecule_row(mol_df: Optional[pd.DataFrame], cid: Optional[str]) -> Optional[pd.Series]:
    if mol_df is None or cid is None:
        return None
    matches = mol_df["CID"].astype(str) == str(cid)
    matches_df = mol_df[matches]
    if not matches_df.empty:
        return matches_df.iloc[0]
    return None


# --- identity building -------------------------------------------------

CID_CANDIDATES_STIM = ["CID", "cid", "PubChemCID", "pubchem_cid", "new_CID"]
CAS_CANDIDATES = ["CAS", "cas", "CASNo", "CASNo.", "cas_number", "C.A.S."]
NAME_CANDIDATES = [
    "name",
    "Name",
    "OdorName",
    "Odorant name",
    "Odorant",
    "OdorantName",
    "ChemicalName",
]
SMILES_CANDIDATES = ["IsomericSMILES", "SMILES", "CanonicalSMILES"]


def _identity_from_rows(
    slug: str,
    stimulus: str,
    stim_row: Optional[pd.Series],
    mol_row: Optional[pd.Series],
) -> Identity:
    """Build Identity from both stimuli and molecules rows (if present)."""
    cid = None
    cas = None
    name = None
    smiles = None

    # CID priority: molecules.CID then stimuli CID
    if mol_row is not None and "CID" in mol_row.index:
        cid = _safe_str(mol_row["CID"])
    if cid is None and stim_row is not None:
        cid = _pick_first_present(stim_row, CID_CANDIDATES_STIM)

    # CAS from stimuli first, then molecules (if any)
    if stim_row is not None:
        cas = _pick_first_present(stim_row, CAS_CANDIDATES)
    if cas is None and mol_row is not None:
        cas = _pick_first_present(mol_row, CAS_CANDIDATES)

    # Name from molecules first, then stimuli
    if mol_row is not None:
        name = _pick_first_present(mol_row, NAME_CANDIDATES)
    if name is None and stim_row is not None:
        name = _pick_first_present(stim_row, NAME_CANDIDATES)

    # SMILES from molecules first, then stimuli
    if mol_row is not None:
        smiles = _pick_first_present(mol_row, SMILES_CANDIDATES)
    if smiles is None and stim_row is not None:
        smiles = _pick_first_present(stim_row, SMILES_CANDIDATES)

    if cid is not None:
        uid = f"CID:{cid}"
    elif cas is not None:
        uid = f"CAS:{cas}"
    else:
        uid = f"OID:{slug}:{stimulus}"

    return Identity(
        unified_odor_id=uid,
        slug=slug,
        stimulus=stimulus,
        name=name or stimulus,
        cid=cid,
        cas=cas,
        smiles=smiles,
    )


# --- odor + facts writing ----------------------------------------------

def _upsert_odor(conn: sqlite3.Connection, ident: Identity) -> None:
    with conn:
        conn.execute(
            """                INSERT OR IGNORE INTO odors (
                unified_odor_id, slug_first, stimulus_first, name, cid, cas, smiles
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ident.unified_odor_id,
                ident.slug,
                ident.stimulus,
                ident.name,
                ident.cid,
                ident.cas,
                ident.smiles,
            ),
        )
        for col, value in [
            ("name", ident.name),
            ("cid", ident.cid),
            ("cas", ident.cas),
            ("smiles", ident.smiles),
        ]:
            if value is None:
                continue
            conn.execute(
                f"""                    UPDATE odors
                SET {col} = ?
                WHERE unified_odor_id = ?
                  AND ({col} IS NULL OR TRIM({col}) = '')
                """,
                (value, ident.unified_odor_id),
            )


def _insert_fact(
    conn: sqlite3.Connection,
    ident: Identity,
    slug: str,
    filename: str,
    column: str,
    value: object,
) -> None:
    if value is None:
        return
    if isinstance(value, float) and math.isnan(value):
        return

    value_text: Optional[str] = None
    value_num: Optional[float] = None

    if isinstance(value, (int, float)):
        value_num = float(value)
    else:
        text = _safe_str(value)
        if text is None:
            return
        try:
            num = float(text)
            if not math.isnan(num):
                value_num = num
            else:
                value_text = text
        except Exception:
            value_text = text

    with conn:
        conn.execute(
            """                INSERT INTO odor_facts (
                unified_odor_id, slug, file, column, value_text, value_num
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                ident.unified_odor_id,
                slug,
                filename,
                column,
                value_text,
                value_num,
            ),
        )


def _insert_row_as_facts(
    conn: sqlite3.Connection,
    ident: Identity,
    slug: str,
    filename: str,
    row: Optional[pd.Series],
    skip_cols: Sequence[str],
) -> None:
    if row is None:
        return
    skip = {c.strip().lower() for c in skip_cols}
    for col in row.index:
        if col.strip().lower() in skip:
            continue
        _insert_fact(conn, ident, slug, filename, col, row[col])


# --- dataset-level helpers ---------------------------------------------

def _count_behavior_rows(slug_dir: Path) -> int:
    """Count total rows across all behavior*.csv in this dataset."""
    total = 0
    for fname in os.listdir(slug_dir):
        if not fname.lower().startswith("behavior") or not fname.lower().endswith(".csv"):
            continue
        fpath = slug_dir / fname
        try:
            df = pd.read_csv(fpath)
        except Exception:
            continue
        total += len(df)
    return total


# --- main ingest -------------------------------------------------------

def ingest_dataset(conn: sqlite3.Connection, datasets_root: Path, slug: str) -> None:
    slug_dir = datasets_root / slug
    if not slug_dir.is_dir():
        print(f"[warn] {slug}: not a directory, skipping.")
        return

    print(f"[info] Ingesting dataset: {slug}")
    initialize_database(conn)

    total_rows = _count_behavior_rows(slug_dir)
    if total_rows > MAX_ROWS_PER_DATASET:
        print(
            f"[info] {slug}: has {total_rows} rows in behavior*.csv "
            f"(>{MAX_ROWS_PER_DATASET}), skipping this dataset."
        )
        return

    stim_df = _load_stimuli(slug_dir)
    mol_df = _load_molecules(slug_dir)
    if stim_df is None:
        print(f"[info] {slug}: no usable stimuli.csv found (identity may be limited).")
    if mol_df is None:
        print(f"[info] {slug}: no usable molecules.csv found (molecule features may be limited).")

    odors_seen = 0
    facts_seen = 0
    identity_cache: Dict[str, Identity] = {}

    for fname in sorted(os.listdir(slug_dir)):
        if not fname.lower().startswith("behavior") or not fname.lower().endswith(".csv"):
            continue
        fpath = slug_dir / fname
        try:
            df = pd.read_csv(fpath)
        except Exception as exc:
            print(f"[warn] {slug}: could not read {fname} ({exc}), skipping.")
            continue

        if df.empty:
            print(f"[info] {slug}: {fname} is empty, skipping.")
            continue

        stim_col = _find_stimulus_column(df)
        if stim_col is None:
            print(
                f"[info] {slug}: {fname} has no 'Stimulus' column, "
                "skipping this file (likely pairwise or special format)."
            )
            continue

        df = df.copy()
        df[stim_col] = df[stim_col].astype(str)

        print(f"[info] {slug}: processing {fname}, {len(df)} rows.")
        for _, row in df.iterrows():
            stim_value = _safe_str(row[stim_col])
            if stim_value is None:
                continue

            if stim_value in identity_cache:
                ident = identity_cache[stim_value]
            else:
                stim_row = _get_stimulus_row(stim_df, stim_value)
                cid_from_stim = None
                if stim_row is not None:
                    cid_from_stim = _pick_first_present(stim_row, CID_CANDIDATES_STIM)
                mol_row = _get_molecule_row(mol_df, cid_from_stim)
                ident = _identity_from_rows(slug, stim_value, stim_row, mol_row)
                identity_cache[stim_value] = ident
                _upsert_odor(conn, ident)
                _insert_row_as_facts(conn, ident, slug, "stimuli.csv", stim_row, skip_cols=[stim_col])
                _insert_row_as_facts(conn, ident, slug, "molecules.csv", mol_row, skip_cols=["CID"])
                odors_seen += 1

            for col in df.columns:
                if col == stim_col:
                    continue
                _insert_fact(conn, ident, slug, fname, col, row[col])
                facts_seen += 1

    print(
        f"[ok] {slug}: ingested approximately {odors_seen} odors "
        f"and {facts_seen} fact values."
    )


def ingest_all(conn: sqlite3.Connection, datasets_root: Path) -> None:
    datasets_root = Path(datasets_root)
    if not datasets_root.is_dir():
        print(f"[error] datasets root {datasets_root} does not exist or is not a directory.")
        return

    slugs = sorted(d.name for d in datasets_root.iterdir() if d.is_dir())
    if not slugs:
        print(f"[warn] no subfolders found under {datasets_root}, nothing to ingest.")
        return

    print(f"[info] Found {len(slugs)} dataset(s) under {datasets_root}.")
    for slug in slugs:
        ingest_dataset(conn, datasets_root, slug)
