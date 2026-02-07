import streamlit as st
import pandas as pd
from pathlib import Path

from src.config import DB_PATH, ENDPOINTS
from src.database import get_connection, init_db
from src.sync import run_full_sync, sync_crimes, sync_calls_for_service, sync_shotspotter, sync_boundaries


def render(db_path: Path = DB_PATH):
    st.header("Data Sources & Sync")

    init_db(db_path)

    # Current status
    st.subheader("Data Status")
    conn = get_connection(db_path)

    tables = {
        "Crimes": "crimes",
        "Calls for Service": "calls_for_service",
        "ShotSpotter": "shotspotter",
        "Boundaries": "boundaries",
    }

    cols = st.columns(len(tables))
    for i, (label, table) in enumerate(tables.items()):
        count = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        with cols[i]:
            st.metric(label, f"{count:,}")

    # Last sync info
    st.subheader("Sync History")
    sync_df = pd.read_sql_query(
        "SELECT source, table_name, records_fetched, started_at, completed_at, status "
        "FROM sync_log ORDER BY id DESC LIMIT 20",
        conn,
    )
    conn.close()

    if not sync_df.empty:
        st.dataframe(sync_df, use_container_width=True)
    else:
        st.info("No sync history yet.")

    # Sync controls
    st.subheader("Sync Controls")
    st.caption("Pull latest data from Peoria PD ArcGIS services.")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Full Sync (All Sources)", type="primary"):
            with st.spinner("Syncing all data sources... This may take several minutes."):
                result = run_full_sync(db_path)
            st.success(
                f"Sync complete! Crimes: {result['crimes']:,}, "
                f"Calls: {result['calls_for_service']:,}, "
                f"ShotSpotter: {result['shotspotter']:,}, "
                f"Boundaries: {result['boundaries']:,}"
            )
            st.rerun()

    with col2:
        source_choice = st.selectbox("Or sync a single source:", list(tables.keys()))
        if st.button("Sync Selected"):
            sync_fn = {
                "Crimes": sync_crimes,
                "Calls for Service": sync_calls_for_service,
                "ShotSpotter": sync_shotspotter,
                "Boundaries": sync_boundaries,
            }[source_choice]
            with st.spinner(f"Syncing {source_choice}..."):
                count = sync_fn(db_path)
            st.success(f"Synced {count:,} {source_choice} records.")
            st.rerun()

    # Data sources transparency
    st.subheader("Data Sources")
    st.markdown("""
**Primary: Peoria Police Department (ArcGIS Open Data)**
- Crimes, Calls for Service, ShotSpotter, Police Boundaries
- Updated regularly by the Peoria Police Department
- No authentication required — public data

**Endpoints:**
    """)
    for name, url in ENDPOINTS.items():
        st.code(url, language=None)

    st.subheader("Score Methodology")
    st.markdown("""
The **Crime Severity Score** uses weighted crime counts:

| Category | Weight |
|----------|--------|
| Homicide | 10 |
| Robbery, Kidnapping | 7 |
| Sex Offenses | 6 |
| Assault, Arson | 5 |
| Burglary, Weapons | 4 |
| Vehicle Theft | 3 |
| Theft, Drugs, Fraud | 2 |
| Vandalism, Trespass, Disorderly | 1 |

Scores are compared against the city-wide average per district to produce
a relative rating (Low / Moderate / High / Very High).
    """)
