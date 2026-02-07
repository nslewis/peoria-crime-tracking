import folium
from folium.plugins import HeatMap
import json
from pathlib import Path
from src.database import get_connection


def create_base_map(center=(40.6936, -89.5890), zoom=12):
    m = folium.Map(location=center, zoom_start=zoom, tiles="CartoDB positron")
    return m


def add_crime_heatmap(m: folium.Map, crimes: list[dict]) -> folium.Map:
    heat_data = [
        [c["latitude"], c["longitude"]]
        for c in crimes
        if c.get("latitude") and c.get("longitude")
    ]
    if heat_data:
        HeatMap(heat_data, radius=15, blur=20, max_zoom=15).add_to(m)
    return m


def add_crime_markers(m: folium.Map, crimes: list[dict], max_markers: int = 500) -> folium.Map:
    for c in crimes[:max_markers]:
        if not c.get("latitude") or not c.get("longitude"):
            continue
        popup_text = (
            f"<b>{c.get('nibrs_offense', 'Unknown')}</b><br>"
            f"{c.get('nibrs_description', '')}<br>"
            f"{c.get('address', '')}<br>"
            f"{str(c.get('report_date', ''))[:10]}"
        )
        folium.CircleMarker(
            location=[c["latitude"], c["longitude"]],
            radius=5,
            color=_crime_color(c.get("nibrs_offense", "")),
            fill=True,
            fill_opacity=0.7,
            popup=folium.Popup(popup_text, max_width=300),
        ).add_to(m)
    return m


def add_boundary_overlay(m: folium.Map, db_path: Path, boundary_type: str) -> folium.Map:
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT name, geometry_geojson FROM boundaries WHERE boundary_type = ?",
        (boundary_type,)
    ).fetchall()
    conn.close()

    for row in rows:
        try:
            geom = json.loads(row["geometry_geojson"])
            geojson = {"type": "Feature", "geometry": geom, "properties": {"name": row["name"]}}
            folium.GeoJson(
                geojson,
                style_function=lambda x: {"fillColor": "transparent", "color": "#3388ff", "weight": 2},
                tooltip=row["name"],
            ).add_to(m)
        except (json.JSONDecodeError, TypeError):
            continue
    return m


def _crime_color(offense: str) -> str:
    colors = {
        "Homicide Offenses": "#d32f2f",
        "Assault Offenses": "#f44336",
        "Robbery": "#e91e63",
        "Weapon Law Violations": "#ff5722",
        "Burglary/Breaking & Entering": "#ff9800",
        "Motor Vehicle Theft": "#ffc107",
        "Larceny/Theft Offenses": "#2196f3",
        "Drug/Narcotic Offenses": "#9c27b0",
        "Destruction/Damage/Vandalism": "#607d8b",
    }
    return colors.get(offense, "#757575")
