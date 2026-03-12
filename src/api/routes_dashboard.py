from fastapi import APIRouter, Depends, Query
from pathlib import Path

from src.api.deps import get_db_path
from src.database import get_connection
from src.config import FEAR_INDEX, DEFAULT_FEAR
from src.queries import (
    compute_severity_score,
    get_top_crime_types,
    get_yoy_change,
    get_crime_trend,
    get_area_options,
)

router = APIRouter()


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


@router.get("/severity")
def severity(
    year: int | None = None,
    district: str | None = None,
    beat: str | None = None,
    neighborhood: str | None = None,
    db_path: Path = Depends(get_db_path),
):
    area_score = compute_severity_score(
        db_path, district=district, beat=beat, neighborhood=neighborhood, year=year,
    )
    year_filter = {"year": year} if year else {}
    city_total = compute_severity_score(db_path, **year_filter)
    districts = get_area_options(db_path, "district")
    n_districts = len(districts) if districts else 1
    city_avg = city_total / n_districts
    rating, color = _score_to_rating(area_score, city_avg)
    return {
        "score": round(area_score, 1),
        "rating": rating,
        "color": color,
        "city_avg": round(city_avg, 1),
    }


@router.get("/summary")
def summary(
    year: int | None = None,
    district: str | None = None,
    beat: str | None = None,
    neighborhood: str | None = None,
    db_path: Path = Depends(get_db_path),
):
    conn = get_connection(db_path)
    conditions: list[str] = []
    params: list = []
    if year:
        conditions.append("report_year = ?")
        params.append(year)
    if district:
        conditions.append("district = ?")
        params.append(district)
    if beat:
        conditions.append("beat = ?")
        params.append(beat)
    if neighborhood:
        conditions.append("neighborhood = ?")
        params.append(neighborhood)
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    total = conn.execute(f"SELECT COUNT(*) FROM crimes{where}", params).fetchone()[0]
    conn.close()

    area_filters = {}
    if district:
        area_filters["district"] = district
    if beat:
        area_filters["beat"] = beat
    if neighborhood:
        area_filters["neighborhood"] = neighborhood

    top_crimes = get_top_crime_types(db_path, limit=5, **area_filters)
    yoy = get_yoy_change(db_path, **area_filters)

    # Crime types ranked by public fear index
    all_types = get_top_crime_types(db_path, limit=50, **area_filters)
    by_fear = sorted(
        all_types,
        key=lambda t: FEAR_INDEX.get(t["type"], DEFAULT_FEAR),
        reverse=True,
    )
    for item in by_fear:
        item["fear_level"] = FEAR_INDEX.get(item["type"], DEFAULT_FEAR)

    return {
        "total": total,
        "top_crimes": top_crimes,
        "yoy": yoy,
        "by_fear": by_fear,
    }


@router.get("/crimes")
def crimes(
    year: int | None = None,
    district: str | None = None,
    beat: str | None = None,
    neighborhood: str | None = None,
    limit: int = Query(default=2000, le=5000),
    db_path: Path = Depends(get_db_path),
):
    conn = get_connection(db_path)
    conditions: list[str] = []
    params: list = []
    if year:
        conditions.append("report_year = ?")
        params.append(year)
    if district:
        conditions.append("district = ?")
        params.append(district)
    if beat:
        conditions.append("beat = ?")
        params.append(beat)
    if neighborhood:
        conditions.append("neighborhood = ?")
        params.append(neighborhood)
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    rows = conn.execute(
        f"SELECT * FROM crimes{where} ORDER BY report_date DESC LIMIT ?",
        params + [limit],
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.get("/trend")
def trend(
    year: int | None = None,
    district: str | None = None,
    beat: str | None = None,
    neighborhood: str | None = None,
    db_path: Path = Depends(get_db_path),
):
    data = get_crime_trend(
        db_path, year=year, district=district, beat=beat, neighborhood=neighborhood,
    )
    return data
