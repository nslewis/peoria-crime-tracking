from pathlib import Path

from src.database import get_connection
from src.config import CRIME_WEIGHTS, DEFAULT_WEIGHT


def get_crime_counts_by_area(db_path: Path, area_type: str, year: int | None = None) -> dict[str, int]:
    """Returns {area_name: crime_count} for the given area_type column (district, beat, neighborhood)."""
    conn = get_connection(db_path)
    query = f"SELECT {area_type}, COUNT(*) as cnt FROM crimes"
    params: list = []
    if year:
        query += " WHERE report_year = ?"
        params.append(year)
    query += f" GROUP BY {area_type}"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return {row[0]: row[1] for row in rows if row[0]}


def get_crime_trend(db_path: Path, year: int | None = None, district: str | None = None,
                    beat: str | None = None, neighborhood: str | None = None) -> list[dict]:
    """Returns list of {year, month, count} for crime trend over time."""
    conn = get_connection(db_path)
    conditions: list[str] = []
    params: list = []

    if year is not None:
        conditions.append("report_year = ?")
        params.append(year)
    if district is not None:
        conditions.append("district = ?")
        params.append(district)
    if beat is not None:
        conditions.append("beat = ?")
        params.append(beat)
    if neighborhood is not None:
        conditions.append("neighborhood = ?")
        params.append(neighborhood)

    query = "SELECT report_year, report_month, COUNT(*) as cnt FROM crimes"
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " GROUP BY report_year, report_month ORDER BY report_year, report_month"

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [{"year": row[0], "month": row[1], "count": row[2]} for row in rows]


def get_crimes_near_address(db_path: Path, lat: float, lon: float, radius_miles: float = 0.5) -> list[dict]:
    """Returns crimes within approximate radius of lat/lon using bounding box (1 degree ~ 69 miles)."""
    conn = get_connection(db_path)
    degree_offset = radius_miles / 69.0
    min_lat = lat - degree_offset
    max_lat = lat + degree_offset
    min_lon = lon - degree_offset
    max_lon = lon + degree_offset

    query = """
        SELECT id, offense_id, nibrs_offense, nibrs_description, address,
               district, beat, neighborhood, report_date, report_year,
               report_month, latitude, longitude
        FROM crimes
        WHERE latitude BETWEEN ? AND ?
          AND longitude BETWEEN ? AND ?
    """
    params = [min_lat, max_lat, min_lon, max_lon]
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def compute_severity_score(db_path: Path, district: str | None = None, beat: str | None = None,
                           neighborhood: str | None = None, year: int | None = None) -> float:
    """Computes weighted severity score: sum of (CRIME_WEIGHTS[offense] * count) for each offense type."""
    conn = get_connection(db_path)
    conditions: list[str] = []
    params: list = []

    if district is not None:
        conditions.append("district = ?")
        params.append(district)
    if beat is not None:
        conditions.append("beat = ?")
        params.append(beat)
    if neighborhood is not None:
        conditions.append("neighborhood = ?")
        params.append(neighborhood)
    if year is not None:
        conditions.append("report_year = ?")
        params.append(year)

    query = "SELECT nibrs_offense, COUNT(*) as cnt FROM crimes"
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " GROUP BY nibrs_offense"

    rows = conn.execute(query, params).fetchall()
    conn.close()

    score = 0.0
    for row in rows:
        offense = row[0]
        count = row[1]
        weight = CRIME_WEIGHTS.get(offense, DEFAULT_WEIGHT) if offense else DEFAULT_WEIGHT
        score += weight * count
    return score


def get_top_crime_types(db_path: Path, limit: int = 10, district: str | None = None,
                        beat: str | None = None, neighborhood: str | None = None) -> list[dict]:
    """Returns [{type, count}] ordered by count desc."""
    conn = get_connection(db_path)
    conditions: list[str] = []
    params: list = []

    if district is not None:
        conditions.append("district = ?")
        params.append(district)
    if beat is not None:
        conditions.append("beat = ?")
        params.append(beat)
    if neighborhood is not None:
        conditions.append("neighborhood = ?")
        params.append(neighborhood)

    query = "SELECT nibrs_offense, COUNT(*) as cnt FROM crimes"
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " GROUP BY nibrs_offense ORDER BY cnt DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [{"type": row[0], "count": row[1]} for row in rows]


def get_area_options(db_path: Path, area_type: str) -> list[str]:
    """Returns sorted distinct values for an area column (district, beat, neighborhood, report_year)."""
    conn = get_connection(db_path)
    query = f"SELECT DISTINCT {area_type} FROM crimes WHERE {area_type} IS NOT NULL ORDER BY {area_type}"
    rows = conn.execute(query).fetchall()
    conn.close()
    return [str(row[0]) for row in rows]
