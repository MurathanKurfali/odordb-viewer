from __future__ import annotations

import sqlite3
from typing import Optional, List

import pandas as pd
import streamlit as st

from config import get_default_db_path
from schema import get_connection, initialize_database
from query import (
    OdorRow,
    get_odor_facts,
    list_odors,
    search_odors,
    descriptor_search,
    list_all_datasets,
    get_datasets_for_odor,
)


@st.cache_resource(show_spinner=False)
def _get_conn_cached() -> sqlite3.Connection:
    """Return a cached connection to the default DB path."""
    path = str(get_default_db_path())
    conn = get_connection(path)
    initialize_database(conn)
    return conn


def get_connection_for_ui() -> sqlite3.Connection:
    """UI-friendly wrapper (no arguments, no controls)."""
    return _get_conn_cached()


def page_header() -> None:
    st.title("odorDB – simple odor browser")
    st.caption("Unified view of odors across multiple datasets (local CSVs).")


def _datasets_for_rows(conn: sqlite3.Connection, rows: List[OdorRow]) -> dict[str, List[str]]:
    """Return a mapping unified_odor_id -> sorted list of dataset slugs."""
    out: dict[str, List[str]] = {}
    for r in rows:
        out[r.unified_odor_id] = get_datasets_for_odor(conn, r.unified_odor_id)
    return out


def _select_row_via_table(df: pd.DataFrame, table_key: str) -> Optional[str]:
    """Render a data_editor with a selectable checkbox column and return the selected ID.

    The user can click the checkbox in the row they want. If none are selected,
    we fall back to the first row.
    """
    if df.empty:
        return None

    df_view = df.copy()
    select_col = "_select"
    df_view.insert(0, select_col, False)

    edited = st.data_editor(
        df_view,
        use_container_width=True,
        num_rows="fixed",
        key=table_key,
    )

    if select_col in edited.columns:
        selected_rows = edited[edited[select_col] == True]
        if not selected_rows.empty:
            return str(selected_rows["unified_odor_id"].iloc[0])

    return str(df["unified_odor_id"].iloc[0])


def odor_search_tab(conn: sqlite3.Connection) -> Optional[OdorRow]:
    st.subheader("Search by odor ID / name / CID / CAS")

    all_datasets = list_all_datasets(conn)
    dataset_filter = st.selectbox(
        "Filter to dataset (optional)",
        options=["(all datasets)"] + all_datasets,
        index=0,
    )
    dataset_filter_val = None if dataset_filter == "(all datasets)" else dataset_filter

    query = st.text_input(
        "Search text",
        placeholder="e.g. coffee, 1234, CAS:123-45-6, OID:abraham_2012:1",
    )
    col1, col2 = st.columns([1, 3])
    with col1:
        limit = st.number_input(
            "Max results",
            min_value=1,
            max_value=500,
            value=50,
            step=1,
        )
    with col2:
        list_all = st.checkbox("List all odors (ignore search text)", value=False)

    if list_all:
        rows = list_odors(conn, limit=int(limit))
    elif query:
        rows = search_odors(conn, query=query, limit=int(limit))
    else:
        st.info("Enter a search query or tick 'List all odors'.")
        return None

    if not rows:
        st.warning("No matching odors found.")
        return None

    ds_map = _datasets_for_rows(conn, rows)
    if dataset_filter_val is not None:
        rows = [r for r in rows if dataset_filter_val in ds_map.get(r.unified_odor_id, [])]
        if not rows:
            st.warning("No odors found in that dataset with the given search.")
            return None

    data = [
        {
            "unified_odor_id": r.unified_odor_id,
            "name": r.name,
            "cid": r.cid,
            "cas": r.cas,
            "smiles": r.smiles,
            "datasets": ", ".join(ds_map.get(r.unified_odor_id, [])),
        }
        for r in rows
    ]
    df = pd.DataFrame(data)
    st.write("Results (click the checkbox in a row to select an odor):")
    selected_id = _select_row_via_table(df, table_key="odor_search_table")

    if selected_id is None:
        return None

    selected_row = next((r for r in rows if r.unified_odor_id == selected_id), None)
    return selected_row


def descriptor_search_tab(conn: sqlite3.Connection) -> Optional[OdorRow]:
    st.subheader("Search by descriptor text")

    all_datasets = list_all_datasets(conn)
    dataset_filter = st.selectbox(
        "Restrict to dataset (optional)",
        options=["(all datasets)"] + all_datasets,
        index=0,
        key="descriptor_dataset_filter",
    )
    dataset_filter_val = None if dataset_filter == "(all datasets)" else dataset_filter

    text = st.text_input(
        "Descriptor text to search for",
        placeholder="e.g. sour, citrus, smoky",
    )
    limit = st.number_input(
        "Max results",
        min_value=1,
        max_value=500,
        value=100,
        step=1,
        key="descriptor_max_results",
    )

    if not text:
        st.info("Enter descriptor text (e.g. sour) to search in descriptor-like columns.")
        return None

    rows = descriptor_search(conn, text=text, dataset=dataset_filter_val, limit=int(limit))
    if not rows:
        st.warning("No odors found with that descriptor text in descriptor columns.")
        return None

    ds_map = _datasets_for_rows(conn, rows)
    data = [
        {
            "unified_odor_id": r.unified_odor_id,
            "name": r.name,
            "cid": r.cid,
            "cas": r.cas,
            "smiles": r.smiles,
            "datasets": ", ".join(ds_map.get(r.unified_odor_id, [])),
        }
        for r in rows
    ]
    df = pd.DataFrame(data)
    st.write("Results (click the checkbox in a row to select an odor):")
    selected_id = _select_row_via_table(df, table_key="descriptor_search_table")

    if selected_id is None:
        return None

    selected_row = next((r for r in rows if r.unified_odor_id == selected_id), None)
    return selected_row


def odors_overview_tab(conn: sqlite3.Connection) -> None:
    st.subheader("Odor overview (ID, name, molecules)")

    limit = st.number_input(
        "Max odors to show",
        min_value=10,
        max_value=2000,
        value=200,
        step=10,
        key="overview_max_odors",
    )

    rows = list_odors(conn, limit=int(limit))
    if not rows:
        st.info("No odors in the database yet. Did you run 'odordb ingest'?")
        return

    ds_map = _datasets_for_rows(conn, rows)
    base_data = [
        {
            "unified_odor_id": r.unified_odor_id,
            "name": r.name,
            "cid": r.cid,
            "cas": r.cas,
            "smiles": r.smiles,
            "datasets": ", ".join(ds_map.get(r.unified_odor_id, [])),
        }
        for r in rows
    ]
    df = pd.DataFrame(base_data)

    # Attach chemical properties from stimuli.csv and molecules.csv (no behavioral files).
    ids = [r.unified_odor_id for r in rows]
    if ids:
        placeholders = ",".join("?" * len(ids))
        sql = f"""
        SELECT unified_odor_id, file, column, value_text, value_num
        FROM odor_facts
        WHERE unified_odor_id IN ({placeholders})
          AND file IN ('stimuli.csv', 'molecules.csv')
        """
        props_df = pd.read_sql_query(sql, conn, params=ids)

        if not props_df.empty:
            props_df["value"] = props_df["value_text"]
            mask_num = props_df["value_num"].notna()
            props_df.loc[mask_num, "value"] = props_df.loc[mask_num, "value_num"].astype(str)

            # Drop purely ID-like fields; keep chemical/physical/meta fields.
            drop_cols_lower = {
                "cid",
                "cas",
                "casno",
                "casno.",
                "cas_number",
                "c.a.s.",
                "stimulus",
            }
            props_df = props_df[~props_df["column"].str.lower().isin(drop_cols_lower)]

            if not props_df.empty:
                pivot = props_df.pivot_table(
                    index="unified_odor_id",
                    columns="column",
                    values="value",
                    aggfunc=lambda x: next(v for v in x if v is not None),
                )
                pivot = pivot.reset_index()
                df = df.merge(pivot, on="unified_odor_id", how="left")

    st.write(
        "This view lists odors, molecule-level identifiers (CID, CAS, SMILES, name), "
        "and chemical properties aggregated from stimuli/molecules tables "
        "(no behavioral measurements)."
    )
    st.dataframe(df, use_container_width=True)


def details_section(conn: sqlite3.Connection, odor: OdorRow) -> None:
    st.subheader("Odor details")

    datasets = get_datasets_for_odor(conn, odor.unified_odor_id)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Unified ID", odor.unified_odor_id)
        st.write(f"**Name**: {odor.name or ''}")
    with c2:
        st.write(f"**CID**: {odor.cid or ''}")
        st.write(f"**CAS**: {odor.cas or ''}")
    with c3:
        st.write(f"**SMILES**: {odor.smiles or ''}")
        st.write("**Datasets**:")
        if datasets:
            st.write(", ".join(datasets))
        else:
            st.write("(none)")

    st.markdown("---")
    st.subheader("Values across datasets")

    facts = list(get_odor_facts(conn, odor.unified_odor_id))
    if not facts:
        st.info("No facts found for this odor.")
        return

    df = pd.DataFrame(facts)
    st.write("All values (long table):")
    st.dataframe(df, use_container_width=True)

    st.markdown("#### Optional wide view")
    grouped = df.groupby(["slug", "file", "column"])
    rows_agg = []
    for (slug, file, column), sub in grouped:
        vals = list(sub["value"])
        numeric_vals = [v for v in vals if isinstance(v, (int, float))]
        if numeric_vals and len(numeric_vals) == len(vals):
            agg = sum(float(v) for v in numeric_vals) / len(numeric_vals)
        else:
            text_vals = [str(v) for v in vals if v is not None]
            agg = "; ".join(sorted(set(text_vals)))
        rows_agg.append(
            {
                "slug": slug,
                "file": file,
                "column": column,
                "value": agg,
            }
        )
    df_agg = pd.DataFrame(rows_agg)
    if not df_agg.empty:
        df_pivot = df_agg.pivot_table(
            index=["slug", "file"],
            columns="column",
            values="value",
            aggfunc="first",
        )
        df_pivot = df_pivot.reset_index()
        st.dataframe(df_pivot, use_container_width=True)
    else:
        st.info("No aggregated data available.")


def main() -> None:
    page_header()
    # No sidebar, no DB path control: always use default DB
    conn = get_connection_for_ui()

    tab1, tab2, tab3 = st.tabs(
        ["Odor search", "Descriptor search", "Odor overview"]
    )

    selected_odor: Optional[OdorRow] = None
    with tab1:
        selected_odor = odor_search_tab(conn)
    with tab2:
        selected_odor_desc = descriptor_search_tab(conn)
        if selected_odor_desc is not None:
            selected_odor = selected_odor_desc
    with tab3:
        odors_overview_tab(conn)

    if selected_odor is not None:
        st.markdown("---")
        details_section(conn, selected_odor)


if __name__ == "__main__":
    main()
