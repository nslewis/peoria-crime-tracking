import streamlit as st
import plotly.express as px
import pandas as pd
from pathlib import Path
from streamlit_folium import st_folium

from src.config import DB_PATH, DISTRICT_NAMES
from src.database import init_db, get_connection
from src.queries import (
    get_crime_counts_by_area,
    get_crime_trend,
    compute_severity_score,
    get_top_crime_types,
    get_area_options,
    get_yoy_change,
)
from src.map_utils import create_base_map, add_crime_heatmap, add_crime_markers, add_boundary_overlay


def _score_to_rating(score: float, city_avg: float) -> tuple[str, str]:
    if city_avg == 0:
        return "N/A", "gray"
    ratio = score / city_avg
    if ratio < 0.5:
        return "Low", "#4caf50"
    elif ratio < 1.0:
        return "Moderate", "#ff9800"
    elif ratio < 1.5:
        return "High", "#f44336"
    else:
        return "Very High", "#b71c1c"


def render(db_path: Path = DB_PATH):
    st.header("Crime Dashboard")

    init_db(db_path)
    conn = get_connection(db_path)
    total = conn.execute("SELECT count(*) FROM crimes").fetchone()[0]
    conn.close()

    if total == 0:
        st.warning("No crime data loaded yet. Go to **Data Sources & Sync** to pull data.")
        return

    # Filters
    col1, col2, col3, col4 = st.columns(4)

    years = get_area_options(db_path, "report_year")
    with col1:
        selected_year = st.selectbox("Year", ["All"] + [str(y) for y in years])

    districts = get_area_options(db_path, "district")
    district_display = {d: DISTRICT_NAMES.get(d, d) for d in districts}
    with col2:
        selected_district_label = st.selectbox(
            "District",
            ["All"] + [f"{district_display[d]}" for d in districts],
        )
    # Reverse-map the display label back to the district number
    label_to_district = {v: k for k, v in district_display.items()}
    selected_district = label_to_district.get(selected_district_label, selected_district_label)

    beats = get_area_options(db_path, "beat")
    with col3:
        if beats:
            selected_beat = st.selectbox("Beat", ["All"] + beats)
        else:
            selected_beat = "All"
            st.selectbox("Beat", ["All"], disabled=True)

    neighborhoods = get_area_options(db_path, "neighborhood")
    with col4:
        if neighborhoods:
            selected_neighborhood = st.selectbox("Neighborhood", ["All"] + neighborhoods)
        else:
            selected_neighborhood = "All"
            st.selectbox("Neighborhood", ["All"], disabled=True)

    # Build filter kwargs
    filters = {}
    if selected_year != "All":
        filters["year"] = int(selected_year)
    if selected_district != "All":
        filters["district"] = selected_district
    if selected_beat != "All":
        filters["beat"] = selected_beat
    if selected_neighborhood != "All":
        filters["neighborhood"] = selected_neighborhood

    # Severity score
    area_score = compute_severity_score(db_path, **filters)

    # City average for comparison
    year_filter = {"year": filters["year"]} if "year" in filters else {}
    city_total_score = compute_severity_score(db_path, **year_filter)
    n_districts = len(districts) if districts else 1
    city_avg = city_total_score / n_districts

    rating, color = _score_to_rating(area_score, city_avg)

    # Year-over-year change
    area_filters = {k: v for k, v in filters.items() if k != "year"}
    yoy = get_yoy_change(db_path, **area_filters)

    # Score card + summary
    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)

    with metric_col1:
        st.markdown(
            f'<div style="text-align:center; padding:20px; background:{color}20; '
            f'border-left:5px solid {color}; border-radius:5px;">'
            f'<h2 style="color:{color}; margin:0;">{rating}</h2>'
            f'<p style="margin:0;">Crime Severity</p>'
            f'<p style="margin:0; font-size:0.8em;">Score: {area_score:.0f}</p></div>',
            unsafe_allow_html=True,
        )

    top_crimes = get_top_crime_types(db_path, limit=5, **{
        k: v for k, v in filters.items() if k != "year"
    })

    with metric_col2:
        conn = get_connection(db_path)
        conditions = []
        params = []
        if "year" in filters:
            conditions.append("report_year = ?")
            params.append(filters["year"])
        if "district" in filters:
            conditions.append("district = ?")
            params.append(filters["district"])
        if "beat" in filters:
            conditions.append("beat = ?")
            params.append(filters["beat"])
        if "neighborhood" in filters:
            conditions.append("neighborhood = ?")
            params.append(filters["neighborhood"])
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        filtered_total = conn.execute(
            f"SELECT count(*) FROM crimes{where}", params
        ).fetchone()[0]
        conn.close()
        st.metric("Total Crimes", f"{filtered_total:,}")

    with metric_col3:
        if yoy["change_pct"] is not None:
            delta_str = f"{yoy['change_pct']:+.1f}%"
            st.metric(
                f"vs {yoy.get('previous_year', 'prev')}",
                f"{yoy['current']:,}",
                delta=delta_str,
                delta_color="inverse",
            )
        else:
            st.metric("Year Trend", "N/A")

    with metric_col4:
        if top_crimes:
            st.markdown("**Top Crime Types**")
            for tc in top_crimes[:3]:
                st.markdown(f"- {tc['type']}: **{tc['count']:,}**")

    # Map
    st.subheader("Crime Map")
    conn = get_connection(db_path)
    conditions = []
    params = []
    if "year" in filters:
        conditions.append("report_year = ?")
        params.append(filters["year"])
    if "district" in filters:
        conditions.append("district = ?")
        params.append(filters["district"])
    if "beat" in filters:
        conditions.append("beat = ?")
        params.append(filters["beat"])
    if "neighborhood" in filters:
        conditions.append("neighborhood = ?")
        params.append(filters["neighborhood"])
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    rows = conn.execute(
        f"SELECT * FROM crimes{where} ORDER BY report_date DESC LIMIT 2000", params
    ).fetchall()
    conn.close()
    crimes = [dict(r) for r in rows]

    m = create_base_map()
    m = add_crime_heatmap(m, crimes)
    m = add_crime_markers(m, crimes, max_markers=300)
    m = add_boundary_overlay(m, db_path, "district")
    st_folium(m, width=None, height=500, use_container_width=True)

    # Trend chart
    st.subheader("Crime Trend")
    trend_data = get_crime_trend(db_path, **filters)
    if trend_data:
        df = pd.DataFrame(trend_data)
        df["period"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)
        fig = px.bar(df, x="period", y="count", title="Crimes by Month")
        fig.update_layout(xaxis_title="Month", yaxis_title="Crime Count")
        st.plotly_chart(fig, use_container_width=True)
