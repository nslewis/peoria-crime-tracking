import streamlit as st
import plotly.express as px
import pandas as pd
from pathlib import Path
from streamlit_folium import st_folium

from src.config import DB_PATH
from src.database import get_connection
from src.queries import search_streets, get_street_crime_summary
from src.map_utils import create_base_map, add_crime_markers


def render(db_path: Path = DB_PATH):
    st.header("Street Search")
    st.caption("Search by street name to see crime activity in your area")

    conn = get_connection(db_path)
    total = conn.execute("SELECT count(*) FROM crimes").fetchone()[0]
    conn.close()

    if total == 0:
        st.warning("No crime data loaded yet. Go to **Data Sources & Sync** to pull data.")
        return

    # Search box
    street_query = st.text_input(
        "Enter a street name (e.g., 'MAIN ST', 'ADAMS', 'WAR MEMORIAL')",
        placeholder="Type a street name...",
    )

    if not street_query or len(street_query) < 3:
        st.info("Enter at least 3 characters to search.")
        return

    # Search results
    results = search_streets(db_path, street_query)

    if not results:
        st.warning(f"No crimes found matching '{street_query}'.")
        return

    st.subheader(f"{len(results)} locations matching '{street_query}'")

    # Show results as selectable table
    results_df = pd.DataFrame(results)
    results_df = results_df[["address", "crime_count", "earliest", "latest"]]
    results_df.columns = ["Address", "Crimes", "First Report", "Last Report"]

    # Let user select an address for detail
    selected_address = st.selectbox(
        "Select an address for detailed view:",
        [r["address"] for r in results],
        format_func=lambda x: f"{x} ({next(r['crime_count'] for r in results if r['address'] == x)} crimes)",
    )

    if selected_address:
        _render_street_detail(db_path, selected_address)


def _render_street_detail(db_path: Path, address: str):
    st.divider()
    st.subheader(f"Crime Detail: {address}")

    summary = get_street_crime_summary(db_path, address)

    # Metrics row
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Crimes", f"{summary['total']:,}")
    with col2:
        st.metric("Severity Score", f"{summary['severity_score']:.0f}")
    with col3:
        if summary["by_year"]:
            years_span = f"{summary['by_year'][0]['year']} - {summary['by_year'][-1]['year']}"
            st.metric("Years Covered", years_span)

    # Crime types breakdown
    col_chart, col_map = st.columns([1, 1])

    with col_chart:
        if summary["by_type"]:
            df_types = pd.DataFrame(summary["by_type"])
            fig = px.pie(df_types, values="count", names="type",
                         title="Crime Types at This Location")
            st.plotly_chart(fig, use_container_width=True)

    with col_map:
        if summary["recent"]:
            m = create_base_map()
            m = add_crime_markers(m, summary["recent"], max_markers=50)
            st_folium(m, width=None, height=350, use_container_width=True)

    # Year-over-year trend
    if summary["by_year"]:
        df_years = pd.DataFrame(summary["by_year"])
        fig2 = px.bar(df_years, x="year", y="count", title="Crimes by Year")
        fig2.update_layout(xaxis_title="Year", yaxis_title="Crime Count")
        st.plotly_chart(fig2, use_container_width=True)

    # Recent crimes table
    if summary["recent"]:
        st.subheader("Recent Crimes")
        recent_df = pd.DataFrame(summary["recent"])
        display_cols = [c for c in ["report_date", "nibrs_offense", "nibrs_description", "address"] if c in recent_df.columns]
        st.dataframe(recent_df[display_cols], use_container_width=True)
