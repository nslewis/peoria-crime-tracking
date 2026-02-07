import streamlit as st
import plotly.express as px
import pandas as pd
from pathlib import Path

from src.config import DB_PATH, DISTRICT_NAMES
from src.database import get_connection
from src.queries import get_crime_trend, get_area_options, compute_severity_score


def render(db_path: Path = DB_PATH):
    st.header("Trends & Comparison")

    conn = get_connection(db_path)
    total = conn.execute("SELECT count(*) FROM crimes").fetchone()[0]
    conn.close()

    if total == 0:
        st.warning("No crime data loaded yet.")
        return

    tab1, tab2, tab3 = st.tabs(["Monthly Trends", "Area Comparison", "Time Patterns"])

    with tab1:
        _render_monthly_trends(db_path)

    with tab2:
        _render_area_comparison(db_path)

    with tab3:
        _render_time_patterns(db_path)


def _render_monthly_trends(db_path: Path):
    st.subheader("Crime Trends Over Time")

    years = get_area_options(db_path, "report_year")
    selected_years = st.multiselect(
        "Years to show", years,
        default=years[-2:] if len(years) >= 2 else years
    )

    conn = get_connection(db_path)
    if selected_years:
        placeholders = ",".join("?" * len(selected_years))
        df = pd.read_sql_query(
            f"SELECT report_year, report_month, nibrs_offense, COUNT(*) as count "
            f"FROM crimes WHERE report_year IN ({placeholders}) "
            f"GROUP BY report_year, report_month, nibrs_offense "
            f"ORDER BY report_year, report_month",
            conn, params=selected_years,
        )
    else:
        df = pd.DataFrame()
    conn.close()

    if df.empty:
        st.info("No data for selected years.")
        return

    # Overall trend
    monthly = df.groupby(["report_year", "report_month"])["count"].sum().reset_index()
    monthly["period"] = (
        monthly["report_year"].astype(str) + "-" +
        monthly["report_month"].astype(str).str.zfill(2)
    )

    fig = px.line(monthly, x="period", y="count", title="Total Crimes by Month", markers=True)
    fig.update_layout(xaxis_title="Month", yaxis_title="Crime Count")
    st.plotly_chart(fig, use_container_width=True)

    # Stacked by type
    top_types = df.groupby("nibrs_offense")["count"].sum().nlargest(8).index.tolist()
    df_top = df[df["nibrs_offense"].isin(top_types)]
    df_top_monthly = (
        df_top.groupby(["report_year", "report_month", "nibrs_offense"])["count"]
        .sum().reset_index()
    )
    df_top_monthly["period"] = (
        df_top_monthly["report_year"].astype(str) + "-" +
        df_top_monthly["report_month"].astype(str).str.zfill(2)
    )

    fig2 = px.area(
        df_top_monthly, x="period", y="count", color="nibrs_offense",
        title="Crime Types Over Time"
    )
    st.plotly_chart(fig2, use_container_width=True)


def _render_area_comparison(db_path: Path):
    st.subheader("Compare Two Areas")

    col1, col2 = st.columns(2)
    districts = get_area_options(db_path, "district")
    district_display = {d: DISTRICT_NAMES.get(d, d) for d in districts}
    display_list = [district_display[d] for d in districts]
    label_to_district = {v: k for k, v in district_display.items()}

    if len(districts) < 2:
        st.info("Need at least 2 districts to compare.")
        return

    with col1:
        label1 = st.selectbox("Area 1", display_list, key="cmp1")
    with col2:
        label2 = st.selectbox("Area 2", display_list, key="cmp2",
                              index=min(1, len(display_list) - 1))

    area1 = label_to_district[label1]
    area2 = label_to_district[label2]

    trend1 = get_crime_trend(db_path, district=area1)
    trend2 = get_crime_trend(db_path, district=area2)

    df1 = pd.DataFrame(trend1)
    df2 = pd.DataFrame(trend2)

    if not df1.empty:
        df1["period"] = df1["year"].astype(str) + "-" + df1["month"].astype(str).str.zfill(2)
        df1["area"] = label1
    if not df2.empty:
        df2["period"] = df2["year"].astype(str) + "-" + df2["month"].astype(str).str.zfill(2)
        df2["area"] = label2

    combined = pd.concat([df1, df2], ignore_index=True)
    if not combined.empty:
        fig = px.line(
            combined, x="period", y="count", color="area",
            title=f"{label1} vs {label2}", markers=True
        )
        st.plotly_chart(fig, use_container_width=True)

    # Score comparison
    s1 = compute_severity_score(db_path, district=area1)
    s2 = compute_severity_score(db_path, district=area2)
    mc1, mc2 = st.columns(2)
    with mc1:
        st.metric(f"{label1} Score", f"{s1:.0f}")
    with mc2:
        st.metric(f"{label2} Score", f"{s2:.0f}")


def _render_time_patterns(db_path: Path):
    st.subheader("When Do Crimes Happen?")

    conn = get_connection(db_path)
    df = pd.read_sql_query("""
        SELECT report_dow, report_hour, COUNT(*) as count
        FROM crimes
        WHERE report_dow IS NOT NULL AND report_hour IS NOT NULL
        GROUP BY report_dow, report_hour
    """, conn)
    conn.close()

    if df.empty:
        st.info("No time pattern data available.")
        return

    dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    pivot = df.pivot_table(values="count", index="report_dow", columns="report_hour", fill_value=0)
    pivot = pivot.reindex([d for d in dow_order if d in pivot.index])

    fig = px.imshow(
        pivot,
        labels=dict(x="Hour of Day", y="Day of Week", color="Crimes"),
        title="Crime Frequency by Day and Hour",
        color_continuous_scale="YlOrRd",
        aspect="auto",
    )
    st.plotly_chart(fig, use_container_width=True)
