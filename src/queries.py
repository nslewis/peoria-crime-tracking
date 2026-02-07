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

    conditions.append("nibrs_offense IS NOT NULL")
    query = "SELECT nibrs_offense, COUNT(*) as cnt FROM crimes"
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


def get_yoy_change(db_path: Path, district: str | None = None,
                    beat: str | None = None, neighborhood: str | None = None) -> dict:
    """Returns year-over-year crime count change between the two most recent full years.

    Skips the current calendar year if it has fewer than 12 months of data,
    so that a partial year (e.g. Jan-Feb 2026) doesn't distort the comparison.
    """
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

    # Get all years with their counts and month spans
    rows = conn.execute(
        f"SELECT report_year, COUNT(*), COUNT(DISTINCT report_month) FROM crimes{where} "
        f"GROUP BY report_year ORDER BY report_year DESC",
        params,
    ).fetchall()
    conn.close()

    # Filter to years with at least 10 months of data (substantially complete years)
    full_years = [(r[0], r[1]) for r in rows if r[2] >= 10]

    # If we don't have 2 full years, fall back to the two most recent years overall
    if len(full_years) < 2:
        candidates = [(r[0], r[1]) for r in rows]
    else:
        candidates = full_years

    if len(candidates) < 2:
        return {"current_year": candidates[0][0] if candidates else None,
                "current": candidates[0][1] if candidates else 0,
                "previous": 0, "change_pct": None}

    current_year, current_count = candidates[0]
    prev_year, prev_count = candidates[1]
    change_pct = ((current_count - prev_count) / prev_count * 100) if prev_count else None
    return {
        "current_year": current_year, "current": current_count,
        "previous_year": prev_year, "previous": prev_count,
        "change_pct": round(change_pct, 1) if change_pct is not None else None,
    }


def search_streets(db_path: Path, street_query: str, limit: int = 20) -> list[dict]:
    """Search for streets matching a query and return crime summary per street."""
    conn = get_connection(db_path)
    rows = conn.execute("""
        SELECT address, COUNT(*) as crime_count,
               AVG(latitude) as avg_lat, AVG(longitude) as avg_lon,
               GROUP_CONCAT(DISTINCT nibrs_offense) as crime_types,
               MIN(report_date) as earliest, MAX(report_date) as latest
        FROM crimes
        WHERE address LIKE ?
        GROUP BY address
        ORDER BY crime_count DESC
        LIMIT ?
    """, (f"%{street_query.upper()}%", limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_street_crime_summary(db_path: Path, street_name: str) -> dict:
    """Get detailed crime summary for a specific street."""
    conn = get_connection(db_path)
    total = conn.execute(
        "SELECT COUNT(*) FROM crimes WHERE address LIKE ?",
        (f"%{street_name.upper()}%",),
    ).fetchone()[0]

    by_type = conn.execute(
        "SELECT nibrs_offense, COUNT(*) as cnt FROM crimes "
        "WHERE address LIKE ? GROUP BY nibrs_offense ORDER BY cnt DESC",
        (f"%{street_name.upper()}%",),
    ).fetchall()

    by_year = conn.execute(
        "SELECT report_year, COUNT(*) as cnt FROM crimes "
        "WHERE address LIKE ? GROUP BY report_year ORDER BY report_year",
        (f"%{street_name.upper()}%",),
    ).fetchall()

    recent = conn.execute(
        "SELECT * FROM crimes WHERE address LIKE ? ORDER BY report_date DESC LIMIT 20",
        (f"%{street_name.upper()}%",),
    ).fetchall()
    conn.close()

    score = sum(
        CRIME_WEIGHTS.get(r[0], DEFAULT_WEIGHT) * r[1]
        for r in by_type if r[0]
    )

    return {
        "total": total,
        "by_type": [{"type": r[0], "count": r[1]} for r in by_type],
        "by_year": [{"year": r[0], "count": r[1]} for r in by_year],
        "recent": [dict(r) for r in recent],
        "severity_score": score,
    }


def get_recent_crimes(db_path: Path, limit: int = 50,
                       district: str | None = None, beat: str | None = None,
                       neighborhood: str | None = None) -> list[dict]:
    """Returns the most recent crimes, optionally filtered by area."""
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
    rows = conn.execute(
        f"SELECT * FROM crimes{where} ORDER BY report_date DESC LIMIT ?",
        params + [limit],
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
