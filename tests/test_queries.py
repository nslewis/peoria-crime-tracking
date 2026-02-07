import pytest
from src.database import init_db, get_connection
from src.queries import (
    get_crime_counts_by_area,
    get_crime_trend,
    get_top_crime_types,
    compute_severity_score,
    get_area_options,
    get_crimes_near_address,
)


@pytest.fixture
def db_path(tmp_path):
    """Create a tmp db, call init_db, and insert 10 test crime records with known values."""
    path = tmp_path / "test_queries.db"
    init_db(path)
    conn = get_connection(path)
    # Insert 6 Assault Offenses and 4 Larceny/Theft Offenses
    # All in district "10", beat "1A", neighborhood "Downtown", year 2025
    # Months spread across 1-3
    records = [
        # 6 Assault Offenses: 3 in month 1, 2 in month 2, 1 in month 3
        ("OFF001", "Assault Offenses", "Simple Assault", "10", "1A", "Downtown", 2025, 1, 40.6936, -89.5890),
        ("OFF002", "Assault Offenses", "Simple Assault", "10", "1A", "Downtown", 2025, 1, 40.6940, -89.5895),
        ("OFF003", "Assault Offenses", "Aggravated Assault", "10", "1A", "Downtown", 2025, 1, 40.6945, -89.5885),
        ("OFF004", "Assault Offenses", "Simple Assault", "10", "1A", "Downtown", 2025, 2, 40.6950, -89.5880),
        ("OFF005", "Assault Offenses", "Simple Assault", "10", "1A", "Downtown", 2025, 2, 40.6955, -89.5875),
        ("OFF006", "Assault Offenses", "Aggravated Assault", "10", "1A", "Downtown", 2025, 3, 40.6960, -89.5870),
        # 4 Larceny/Theft Offenses: 2 in month 1, 1 in month 2, 1 in month 3
        ("OFF007", "Larceny/Theft Offenses", "Shoplifting", "10", "1A", "Downtown", 2025, 1, 40.6965, -89.5865),
        ("OFF008", "Larceny/Theft Offenses", "Shoplifting", "10", "1A", "Downtown", 2025, 1, 40.6970, -89.5860),
        ("OFF009", "Larceny/Theft Offenses", "Theft from Building", "10", "1A", "Downtown", 2025, 2, 40.6975, -89.5855),
        ("OFF010", "Larceny/Theft Offenses", "Theft from Vehicle", "10", "1A", "Downtown", 2025, 3, 40.6980, -89.5850),
    ]
    for r in records:
        conn.execute(
            """INSERT INTO crimes
               (offense_id, nibrs_offense, nibrs_description, district, beat, neighborhood,
                report_year, report_month, latitude, longitude)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            r,
        )
    conn.commit()
    conn.close()
    return path


def test_get_crime_counts_by_area_district(db_path):
    """Test that get_crime_counts_by_area returns correct counts for district."""
    result = get_crime_counts_by_area(db_path, "district")
    assert result == {"10": 10}


def test_get_crime_counts_by_area_neighborhood(db_path):
    """Test that get_crime_counts_by_area returns correct counts for neighborhood."""
    result = get_crime_counts_by_area(db_path, "neighborhood")
    assert result == {"Downtown": 10}


def test_get_crime_counts_by_area_with_year_filter(db_path):
    """Test that year filter works correctly."""
    result = get_crime_counts_by_area(db_path, "district", year=2025)
    assert result == {"10": 10}
    # Non-existent year should return empty dict
    result_empty = get_crime_counts_by_area(db_path, "district", year=1999)
    assert result_empty == {}


def test_get_crime_trend(db_path):
    """Test that get_crime_trend returns the monthly breakdown."""
    result = get_crime_trend(db_path, year=2025)
    assert len(result) == 3  # 3 months
    # Month 1: 3 assaults + 2 thefts = 5
    assert result[0] == {"year": 2025, "month": 1, "count": 5}
    # Month 2: 2 assaults + 1 theft = 3
    assert result[1] == {"year": 2025, "month": 2, "count": 3}
    # Month 3: 1 assault + 1 theft = 2
    assert result[2] == {"year": 2025, "month": 3, "count": 2}


def test_get_crime_trend_with_filters(db_path):
    """Test that get_crime_trend correctly applies district filter."""
    result = get_crime_trend(db_path, district="10")
    assert len(result) == 3
    total = sum(r["count"] for r in result)
    assert total == 10


def test_get_top_crime_types(db_path):
    """Test that Assault is first with count 6."""
    result = get_top_crime_types(db_path)
    assert len(result) == 2
    assert result[0]["type"] == "Assault Offenses"
    assert result[0]["count"] == 6
    assert result[1]["type"] == "Larceny/Theft Offenses"
    assert result[1]["count"] == 4


def test_get_top_crime_types_with_limit(db_path):
    """Test that the limit parameter works."""
    result = get_top_crime_types(db_path, limit=1)
    assert len(result) == 1
    assert result[0]["type"] == "Assault Offenses"


def test_compute_severity_score(db_path):
    """Test that compute_severity_score returns a positive number."""
    score = compute_severity_score(db_path)
    assert score > 0
    # Assault Offenses weight = 5 (from CRIME_WEIGHTS), count = 6 -> 30
    # Larceny/Theft Offenses weight = 2 (from CRIME_WEIGHTS), count = 4 -> 8
    # Total = 38
    assert score == 38.0


def test_compute_severity_score_with_filter(db_path):
    """Test severity score with district filter."""
    score = compute_severity_score(db_path, district="10")
    assert score > 0
    assert score == 38.0


def test_get_area_options_district(db_path):
    """Test that get_area_options returns district '10'."""
    result = get_area_options(db_path, "district")
    assert "10" in result


def test_get_area_options_beat(db_path):
    """Test that get_area_options returns beat '1A'."""
    result = get_area_options(db_path, "beat")
    assert "1A" in result


def test_get_area_options_neighborhood(db_path):
    """Test that get_area_options returns neighborhood 'Downtown'."""
    result = get_area_options(db_path, "neighborhood")
    assert "Downtown" in result


def test_get_area_options_report_year(db_path):
    """Test that get_area_options returns report_year '2025'."""
    result = get_area_options(db_path, "report_year")
    assert "2025" in result


def test_get_crimes_near_address(db_path):
    """Test that crimes near a known location are returned."""
    # Center of our test data is roughly 40.696, -89.587
    result = get_crimes_near_address(db_path, lat=40.696, lon=-89.587, radius_miles=1.0)
    assert len(result) == 10


def test_get_crimes_near_address_no_results(db_path):
    """Test that a distant location returns no crimes."""
    result = get_crimes_near_address(db_path, lat=0.0, lon=0.0, radius_miles=0.5)
    assert len(result) == 0
