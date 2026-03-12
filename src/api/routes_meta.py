import json

from fastapi import APIRouter, Depends
from pathlib import Path

from src.api.deps import get_db_path
from src.database import get_connection
from src.queries import get_area_options
from src.config import DISTRICT_NAMES

router = APIRouter()


@router.get("/options")
def options(db_path: Path = Depends(get_db_path)):
    years = get_area_options(db_path, "report_year")
    districts = get_area_options(db_path, "district")
    beats = get_area_options(db_path, "beat")
    neighborhoods = get_area_options(db_path, "neighborhood")
    return {
        "years": years,
        "districts": [
            {"id": d, "name": DISTRICT_NAMES.get(d, d)} for d in districts
        ],
        "beats": beats,
        "neighborhoods": neighborhoods,
    }


@router.get("/boundaries")
def boundaries(type: str = "districts", db_path: Path = Depends(get_db_path)):
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT name, geometry_geojson FROM boundaries WHERE boundary_type = ?",
        (type,),
    ).fetchall()
    conn.close()
    features = []
    for row in rows:
        try:
            geom = json.loads(row["geometry_geojson"])
            # Convert Esri ring format to standard GeoJSON
            if "rings" in geom and "type" not in geom:
                geom = {"type": "Polygon", "coordinates": geom["rings"]}
            features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {"name": row["name"]},
            })
        except (json.JSONDecodeError, TypeError):
            continue
    return {"type": "FeatureCollection", "features": features}


@router.get("/table-counts")
def table_counts(db_path: Path = Depends(get_db_path)):
    conn = get_connection(db_path)
    crimes = conn.execute("SELECT COUNT(*) FROM crimes").fetchone()[0]
    calls = conn.execute("SELECT COUNT(*) FROM calls_for_service").fetchone()[0]
    shotspotter = conn.execute("SELECT COUNT(*) FROM shotspotter").fetchone()[0]
    boundaries = conn.execute("SELECT COUNT(*) FROM boundaries").fetchone()[0]
    conn.close()
    return {
        "crimes": crimes,
        "calls_for_service": calls,
        "shotspotter": shotspotter,
        "boundaries": boundaries,
    }
