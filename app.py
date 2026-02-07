import streamlit as st
from src.config import DB_PATH
from src.database import init_db

st.set_page_config(
    page_title="Peoria Crime Tracker",
    page_icon=":mag:",
    layout="wide",
)

init_db(DB_PATH)

st.title("Peoria Crime Tracker")
st.caption("Aggregating multiple data sources for a complete picture of crime in Peoria, IL")

page = st.sidebar.radio("Navigate", [
    "Dashboard",
    "Explore Data",
    "Trends & Comparison",
    "Data Sources & Sync",
])

if page == "Dashboard":
    from src.pages.dashboard import render
    render()
elif page == "Explore Data":
    from src.pages.explore import render
    render()
elif page == "Trends & Comparison":
    from src.pages.trends import render
    render()
elif page == "Data Sources & Sync":
    from src.pages.sync_page import render
    render()
