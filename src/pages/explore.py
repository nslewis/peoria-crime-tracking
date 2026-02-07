import streamlit as st
import pandas as pd
from pathlib import Path
from streamlit_folium import st_folium

from src.config import DB_PATH
from src.database import get_connection
from src.map_utils import create_base_map, add_crime_markers
from src.queries import get_area_options


def render(db_path: Path = DB_PATH):
    st.header("Explore Crime Data")

    # Source selector
    source = st.radio("Data Source", ["Crimes", "Calls for Service", "ShotSpotter"], horizontal=True)
    table = {"Crimes": "crimes", "Calls for Service": "calls_for_service", "ShotSpotter": "shotspotter"}[source]

    conn = get_connection(db_path)
    total = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    conn.close()

    if total == 0:
        st.warning(f"No {source} data loaded. Go to **Data Sources & Sync** to pull data.")
        return

    st.caption(f"{total:,} total records")

    # Filters
    col1, col2 = st.columns(2)

    if table == "crimes":
        with col1:
            crime_types = get_area_options(db_path, "nibrs_offense")
            selected_type = st.selectbox("Crime Type", ["All"] + crime_types)
        with col2:
            districts = get_area_options(db_path, "district")
            selected_district = st.selectbox("District", ["All"] + districts, key="explore_district")

        conditions = []
        params = []
        if selected_type != "All":
            conditions.append("nibrs_offense = ?")
            params.append(selected_type)
        if selected_district != "All":
            conditions.append("district = ?")
            params.append(selected_district)

        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        conn = get_connection(db_path)
        df = pd.read_sql_query(
            f"SELECT offense_id, nibrs_offense, nibrs_description, address, district, beat, "
            f"neighborhood, report_date, latitude, longitude FROM {table}{where} "
            f"ORDER BY report_date DESC LIMIT 5000",
            conn, params=params,
        )
        conn.close()

    elif table == "calls_for_service":
        conn = get_connection(db_path)
        df = pd.read_sql_query(
            f"SELECT call_id, call_type, priority, disposition, address, district, beat, "
            f"call_date, latitude, longitude FROM {table} ORDER BY call_date DESC LIMIT 5000",
            conn,
        )
        conn.close()

    else:  # shotspotter
        conn = get_connection(db_path)
        df = pd.read_sql_query(
            f"SELECT incident_id, rounds_fired, event_type, address, district, beat, "
            f"event_date, latitude, longitude FROM {table} ORDER BY event_date DESC LIMIT 5000",
            conn,
        )
        conn.close()

    st.subheader(f"{len(df):,} records shown")

    # Map
    if not df.empty and "latitude" in df.columns:
        m = create_base_map()
        crimes_list = df.dropna(subset=["latitude", "longitude"]).to_dict("records")
        m = add_crime_markers(m, crimes_list, max_markers=500)
        st_folium(m, width=None, height=400, use_container_width=True)

    # Table
    st.dataframe(df, use_container_width=True, height=400)

    # CSV export
    csv = df.to_csv(index=False)
    st.download_button("Download CSV", csv, f"peoria_{table}_export.csv", "text/csv")
