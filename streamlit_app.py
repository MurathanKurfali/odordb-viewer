from __future__ import annotations

import sqlite3
from typing import Optional, List, Dict

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


def _datasets_for_rows(conn: sqlite3.Connection, rows: List[OdorRow]) -> Dict[str, List[str]]:
    """Return a mapping unified_odor_id -> sorted list of dataset slugs."""
    out: Dict[str, List[str]] = {}
    for r in rows:
        out[r.unified_odor_id] = get_datasets_for_odor(conn, r.unified_odor_id)
    return out


def _select_rows_via_table(df: pd.DataFrame, table_key: str) -> List[str]:
    """Render a data_editor with a selectable checkbox column and return selected IDs.

    The user can click the checkbox in any number of rows.
    If none are selected, we fall back to the first row.
    """
    if df.empty:
        return []

    df_view = df.copy()
    select_col = "_select"
    df_view.insert(0, select_col, False)

    edited = st.data_editor(
        df_view,
        use_container_width=True,
        num_rows="fixed",
        key=table_key,
    )

    selected_ids: List[str] = []
    if select_col in edited.columns:
        selected_rows = edited[edited[select_col] == True]
        if not selected_rows.empty:
            selected_ids = [str(v) for v in selected_rows["unified_odor_id"].tolist()]

    if not selected_ids:
        # Fallback: first row
        selected_ids = [str(df["unified_odor_id"].iloc[0])]

    return selected_ids


def descriptor_search_tab(conn: sqlite3.Connection) -> List[OdorRow]:
    st.subheader("Search by descriptor text")
    st.markdown("""
_Search odors by words in descriptor-like fields (e.g. **sweat**, **citrus**, **smoky**).  
Select one or more rows in the results table to include them in the aggregated view below._
""")

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
        st.info("Enter descriptor text (e.g. 'sweat') to search in descriptor-like columns.")
        return []

    rows = descriptor_search(conn, text=text, dataset=dataset_filter_val, limit=int(limit))
    if not rows:
        st.warning("No odors found with that descriptor text in descriptor columns.")
        return []

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
    st.write("Results (tick the checkbox in one or more rows to select odors):")
    selected_ids = _select_rows_via_table(df, table_key="descriptor_search_table")

    if not selected_ids:
        return []

    selected_rows = [r for r in rows if r.unified_odor_id in selected_ids]
    return selected_rows


def odor_search_tab(conn: sqlite3.Connection) -> List[OdorRow]:
    """Compound search tab (by ID / name / CID / CAS)."""
    st.subheader("Search by compound identifiers")
    st.markdown("""
_Search by **name**, **CID**, **CAS**, or unified odor ID (e.g. `OID:abraham_2012:1`).  
Select one or more rows in the results table to include them in the aggregated view below._
""")

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
        return []

    if not rows:
        st.warning("No matching odors found.")
        return []

    ds_map = _datasets_for_rows(conn, rows)
    if dataset_filter_val is not None:
        rows = [r for r in rows if dataset_filter_val in ds_map.get(r.unified_odor_id, [])]
        if not rows:
            st.warning("No odors found in that dataset with the given search.")
            return []

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
    st.write("Results (tick the checkbox in one or more rows to select odors):")
    selected_ids = _select_rows_via_table(df, table_key="odor_search_table")

    if not selected_ids:
        return []

    selected_rows = [r for r in rows if r.unified_odor_id in selected_ids]
    return selected_rows


def odors_overview_tab(conn: sqlite3.Connection) -> None:
    st.subheader("Odor overview (ID, name, molecules)")
    st.markdown("""
_Browse a list of odors with identifiers (name, CID, CAS, SMILES) and  
chemical properties aggregated from stimulus/molecule tables (no behavioral data)._
""")

    limit = st.number_input(
        "Max odors to show",
        min_value=10,
        max_value=20000,
        value=200,
        step=10,
        key="overview_max_odors",
    )

    rows = list_odors(conn, limit=int(limit))
    if not rows:
        st.info("No odors in the database yet. Did you run `odordb ingest`?")
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

    st.dataframe(df, use_container_width=True)


def details_section_multi(conn: sqlite3.Connection, odors: List[OdorRow]) -> None:
    """Show a single combined 'Values across datasets' table for all selected odors."""
    if not odors:
        return

    # --- Combined wide view for all selected odors ---

    all_facts = []
    for o in odors:
        these_facts = list(get_odor_facts(conn, o.unified_odor_id))
        for f in these_facts:
            # Ensure we have a mutable dict
            if not isinstance(f, dict):
                f = dict(f)
            # Attach the unified_odor_id so we can group by it
            f["unified_odor_id"] = o.unified_odor_id
            all_facts.append(f)

    if not all_facts:
        st.info("No facts found for the selected odors.")
        return

    df = pd.DataFrame(all_facts)

    # Be defensive: make sure required columns exist
    required_cols = ["unified_odor_id", "slug", "file", "column", "value"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        st.error(f"Missing expected columns in facts: {missing}")
        st.dataframe(df, use_container_width=True)
        return

    grouped = df.groupby(["unified_odor_id", "slug", "file", "column"])
    rows_agg = []
    for (unified_odor_id, slug, file, column), sub in grouped:
        vals = list(sub["value"])
        numeric_vals = [v for v in vals if isinstance(v, (int, float))]
        if numeric_vals and len(numeric_vals) == len(vals):
            agg = sum(float(v) for v in numeric_vals) / len(numeric_vals)
        else:
            text_vals = [str(v) for v in vals if v is not None]
            agg = "; ".join(sorted(set(text_vals)))
        rows_agg.append(
            {
                "unified_odor_id": unified_odor_id,
                "slug": slug,
                "file": file,
                "column": column,
                "value": agg,
            }
        )

    df_agg = pd.DataFrame(rows_agg)
    if df_agg.empty:
        st.info("No aggregated data available.")
        return

    df_pivot = df_agg.pivot_table(
        index=["unified_odor_id", "slug", "file"],
        columns="column",
        values="value",
        aggfunc="first",
    )
    df_pivot = df_pivot.reset_index()

    st.markdown("""
### Values across datasets

This table shows all available (non-behavioral) information for **all selected odors**,  
merged across datasets and files. Use the download button below to export it as CSV.
""")
    st.dataframe(df_pivot, use_container_width=True)

    # Export combined wide view as a single CSV
    csv_data = df_pivot.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download values as CSV",
        data=csv_data,
        file_name="selected_odors_values.csv",
        mime="text/csv",
    )


def main() -> None:
    page_header()

    # Global usage instructions (top of page)
    st.markdown("""
### Mini Readme

1. Use the **Descriptor search** or **Compund search** tab to find odors of interest in the database.
2. In the results table, tick the checkbox for one or more rows to select odors.  
3. Scroll down to see **Values across datasets**, which aggregates information for all selected odors.  
4. Click **Download values as CSV** to export the combined table.  
5. Use **Odor overview** to browse the full set of odors and their basic chemical identifiers.
The database currently consists of 27 datasets from the Pyrfume project (https://pyrfume.org/).
""")

    conn = get_connection_for_ui()

    # TAB ORDER: descriptor search first, then compound search, then overview
    tab1, tab2, tab3 = st.tabs(
        ["Descriptor search", "Compund search", "Odor overview"]
    )

    selected_odors_desc: List[OdorRow] = []
    selected_odors_search: List[OdorRow] = []

    with tab1:
        selected_odors_desc = descriptor_search_tab(conn)
    with tab2:
        selected_odors_search = odor_search_tab(conn)
    with tab3:
        odors_overview_tab(conn)

    # Combine selections from both tabs, de-duplicate by unified_odor_id
    combined: Dict[str, OdorRow] = {}
    for o in selected_odors_desc + selected_odors_search:
        combined[o.unified_odor_id] = o
    selected_odors: List[OdorRow] = list(combined.values())

    if selected_odors:
        st.markdown("---")
        details_section_multi(conn, selected_odors)


if __name__ == "__main__":
    main()
