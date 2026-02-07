"""Smoke test: ensures all modules import and core flow works end-to-end."""
import pytest
from src.database import init_db, get_connection
from src.queries import (
    get_crime_counts_by_area,
    get_crime_trend,
    compute_severity_score,
    get_top_crime_types,
    get_area_options,
)
from src.map_utils import create_base_map, add_crime_heatmap


@pytest.fixture
def populated_db(tmp_path):
    path = tmp_path / "smoke.db"
    init_db(path)
    conn = get_connection(path)
    for i in range(50):
        conn.execute("""
            INSERT INTO crimes (offense_id, nibrs_offense, nibrs_description,
                district, beat, neighborhood, address,
                report_date, report_year, report_month, report_hour, report_dow,
                latitude, longitude)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            f"SMOKE-{i}",
            "Assault Offenses" if i % 3 == 0 else "Larceny/Theft Offenses",
            "TEST",
            "10" if i < 25 else "13",
            "1A" if i < 25 else "3B",
            "Downtown" if i < 25 else "East Bluff",
            f"{100 + i} TEST ST",
            f"2025-{(i % 12) + 1:02d}-15T12:00:00+00:00",
            2025, (i % 12) + 1, 14, "Tuesday",
            40.69 + i * 0.001, -89.59 + i * 0.001,
        ))
    conn.commit()
    conn.close()
    return path


def test_full_query_flow(populated_db):
    counts = get_crime_counts_by_area(populated_db, "district")
    assert len(counts) == 2

    trend = get_crime_trend(populated_db, district="10")
    assert len(trend) > 0

    score = compute_severity_score(populated_db, district="10")
    assert score > 0

    top = get_top_crime_types(populated_db, limit=5)
    assert len(top) > 0

    options = get_area_options(populated_db, "district")
    assert "10" in options


def test_map_creation(populated_db):
    m = create_base_map()
    assert m is not None

    conn = get_connection(populated_db)
    rows = conn.execute("SELECT * FROM crimes LIMIT 10").fetchall()
    conn.close()
    crimes = [dict(r) for r in rows]

    m = add_crime_heatmap(m, crimes)
    assert m is not None
