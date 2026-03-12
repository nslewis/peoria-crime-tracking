from fastapi import APIRouter, Depends, Query
from pathlib import Path

from src.api.deps import get_db_path
from src.database import get_connection
from src.queries import get_crime_trend, compute_severity_score

router = APIRouter()


@router.get("/monthly")
def monthly(
    years: str = Query(default="", description="Comma-separated years"),
    district: str | None = None,
    beat: str | None = None,
    neighborhood: str | None = None,
    db_path: Path = Depends(get_db_path),
):
    if years:
        year_list = [int(y.strip()) for y in years.split(",") if y.strip()]
    else:
        year_list = []

    results = {}
    if year_list:
        for yr in year_list:
            data = get_crime_trend(
                db_path, year=yr, district=district, beat=beat, neighborhood=neighborhood,
            )
            results[str(yr)] = data
    else:
        data = get_crime_trend(
            db_path, district=district, beat=beat, neighborhood=neighborhood,
        )
        results["all"] = data

    # Also get per-type breakdown
    conn = get_connection(db_path)
    conditions: list[str] = []
    params: list = []
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
    type_rows = conn.execute(
        f"SELECT nibrs_offense, report_year, report_month, COUNT(*) as cnt "
        f"FROM crimes{where} AND nibrs_offense IS NOT NULL "
        f"GROUP BY nibrs_offense, report_year, report_month "
        f"ORDER BY cnt DESC"
        if conditions else
        f"SELECT nibrs_offense, report_year, report_month, COUNT(*) as cnt "
        f"FROM crimes WHERE nibrs_offense IS NOT NULL "
        f"GROUP BY nibrs_offense, report_year, report_month "
        f"ORDER BY cnt DESC",
        params,
    ).fetchall()
    conn.close()

    by_type = {}
    for row in type_rows:
        t = row[0]
        if t not in by_type:
            by_type[t] = []
        by_type[t].append({"year": row[1], "month": row[2], "count": row[3]})

    return {"monthly": results, "by_type": by_type}


@router.get("/compare")
def compare(
    area1_type: str = Query(default="district"),
    area1_value: str = Query(default=""),
    area2_type: str = Query(default="district"),
    area2_value: str = Query(default=""),
    db_path: Path = Depends(get_db_path),
):
    def get_area_data(area_type: str, area_value: str):
        kwargs = {area_type: area_value} if area_value else {}
        trend = get_crime_trend(db_path, **kwargs)
        score = compute_severity_score(db_path, **kwargs)
        return {"trend": trend, "severity": round(score, 1)}

    return {
        "area1": get_area_data(area1_type, area1_value),
        "area2": get_area_data(area2_type, area2_value),
    }


@router.get("/time-patterns")
def time_patterns(
    district: str | None = None,
    beat: str | None = None,
    neighborhood: str | None = None,
    year: int | None = None,
    db_path: Path = Depends(get_db_path),
):
    conn = get_connection(db_path)
    conditions: list[str] = []
    params: list = []
    if district:
        conditions.append("district = ?")
        params.append(district)
    if beat:
        conditions.append("beat = ?")
        params.append(beat)
    if neighborhood:
        conditions.append("neighborhood = ?")
        params.append(neighborhood)
    if year:
        conditions.append("report_year = ?")
        params.append(year)
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    rows = conn.execute(
        f"SELECT report_dow, report_hour, COUNT(*) as cnt FROM crimes{where} "
        f"WHERE report_dow IS NOT NULL AND report_hour IS NOT NULL "
        f"GROUP BY report_dow, report_hour"
        if not conditions else
        f"SELECT report_dow, report_hour, COUNT(*) as cnt FROM crimes{where} "
        f"AND report_dow IS NOT NULL AND report_hour IS NOT NULL "
        f"GROUP BY report_dow, report_hour",
        params,
    ).fetchall()
    conn.close()

    # Build matrix: {dow: {hour: count}}
    matrix = {}
    for row in rows:
        dow = row[0]
        hour = row[1]
        cnt = row[2]
        if dow not in matrix:
            matrix[dow] = {}
        matrix[dow][str(hour)] = cnt
    return {"matrix": matrix}
