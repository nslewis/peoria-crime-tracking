"""Tests for new query functions: YoY change, street search, recent crimes."""
import pytest
from src.database import init_db, get_connection
from src.queries import get_yoy_change, search_streets, get_street_crime_summary, get_recent_crimes


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "test_new.db"
    init_db(path)
    conn = get_connection(path)
    # Insert records across 2 years and 2 streets
    records = [
        # 2024 - 8 crimes on MAIN ST
        ("Y1-01", "Assault Offenses", "100 MAIN ST PEORIA", "10", "1A", "Downtown", 2024, 6, 40.69, -89.59),
        ("Y1-02", "Assault Offenses", "100 MAIN ST PEORIA", "10", "1A", "Downtown", 2024, 7, 40.69, -89.59),
        ("Y1-03", "Larceny/Theft Offenses", "100 MAIN ST PEORIA", "10", "1A", "Downtown", 2024, 8, 40.69, -89.59),
        ("Y1-04", "Larceny/Theft Offenses", "200 MAIN ST PEORIA", "10", "1A", "Downtown", 2024, 9, 40.691, -89.591),
        ("Y1-05", "Assault Offenses", "200 MAIN ST PEORIA", "10", "1A", "Downtown", 2024, 10, 40.691, -89.591),
        ("Y1-06", "Drug/Narcotic Offenses", "300 ADAMS ST PEORIA", "10", "1A", "Downtown", 2024, 11, 40.688, -89.596),
        ("Y1-07", "Assault Offenses", "300 ADAMS ST PEORIA", "10", "1A", "Downtown", 2024, 11, 40.688, -89.596),
        ("Y1-08", "Robbery", "300 ADAMS ST PEORIA", "10", "1A", "Downtown", 2024, 12, 40.688, -89.596),
        # 2025 - 5 crimes (fewer than 2024)
        ("Y2-01", "Assault Offenses", "100 MAIN ST PEORIA", "10", "1A", "Downtown", 2025, 1, 40.69, -89.59),
        ("Y2-02", "Larceny/Theft Offenses", "100 MAIN ST PEORIA", "10", "1A", "Downtown", 2025, 2, 40.69, -89.59),
        ("Y2-03", "Assault Offenses", "200 MAIN ST PEORIA", "10", "1A", "Downtown", 2025, 3, 40.691, -89.591),
        ("Y2-04", "Drug/Narcotic Offenses", "300 ADAMS ST PEORIA", "10", "1A", "Downtown", 2025, 4, 40.688, -89.596),
        ("Y2-05", "Homicide Offenses", "300 ADAMS ST PEORIA", "10", "1A", "Downtown", 2025, 5, 40.688, -89.596),
    ]
    for r in records:
        conn.execute(
            """INSERT INTO crimes (offense_id, nibrs_offense, address, district, beat,
               neighborhood, report_year, report_month, latitude, longitude,
               report_date)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
               printf('%d-%02d-15T12:00:00+00:00', ?, ?))""",
            r + (r[6], r[7]),
        )
    conn.commit()
    conn.close()
    return path


def test_yoy_change_returns_decrease(db_path):
    result = get_yoy_change(db_path)
    assert result["current_year"] == 2025
    assert result["current"] == 5
    assert result["previous"] == 8
    assert result["change_pct"] < 0  # crime went down


def test_yoy_change_with_district_filter(db_path):
    result = get_yoy_change(db_path, district="10")
    assert result["current_year"] == 2025
    assert result["current"] + result["previous"] == 13


def test_search_streets_main(db_path):
    results = search_streets(db_path, "MAIN")
    assert len(results) == 2  # 100 MAIN ST and 200 MAIN ST
    # Most crimes first
    assert results[0]["crime_count"] >= results[1]["crime_count"]


def test_search_streets_adams(db_path):
    results = search_streets(db_path, "ADAMS")
    assert len(results) == 1
    assert results[0]["crime_count"] == 5


def test_search_streets_no_results(db_path):
    results = search_streets(db_path, "NONEXISTENT")
    assert len(results) == 0


def test_street_crime_summary(db_path):
    summary = get_street_crime_summary(db_path, "MAIN")
    assert summary["total"] == 8  # 5 on 100 MAIN + 3 on 200 MAIN
    assert summary["severity_score"] > 0
    assert len(summary["by_type"]) > 0
    assert len(summary["by_year"]) == 2  # 2024 and 2025


def test_get_recent_crimes(db_path):
    recent = get_recent_crimes(db_path, limit=5)
    assert len(recent) == 5
    # Should be ordered by date descending
    dates = [r["report_date"] for r in recent if r["report_date"]]
    assert dates == sorted(dates, reverse=True)


def test_get_recent_crimes_with_filter(db_path):
    recent = get_recent_crimes(db_path, limit=50, district="10")
    assert len(recent) == 13  # all records are district 10
