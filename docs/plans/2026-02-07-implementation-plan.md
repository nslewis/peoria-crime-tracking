# Peoria Crime Tracking Platform — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Streamlit app that ingests Peoria IL crime data from multiple public sources into SQLite and lets residents explore crime by area with maps, trends, and a composite severity score.

**Architecture:** Data sync module pulls from ArcGIS REST APIs (crimes, calls for service, ShotSpotter, boundaries) into SQLite. A service layer handles queries, scoring, and trend analysis. Streamlit frontend presents 4 pages: Dashboard, Explore, Trends, and Data Sources.

**Tech Stack:** Python 3.10, Streamlit, Folium, streamlit-folium, Plotly, Pandas, SQLite3, Requests

---

## Task 0: Project Setup

**Files:**
- Create: `requirements.txt`
- Create: `src/__init__.py`
- Create: `src/config.py`
- Create: `tests/__init__.py`
- Create: `.gitignore`

**Step 1: Initialize git repo**

```bash
cd /home/nolu/Desktop/peoria_crime_tracking
git init
```

**Step 2: Create .gitignore**

```
__pycache__/
*.pyc
*.pyo
.env
*.db
*.sqlite
.venv/
venv/
.streamlit/secrets.toml
data/
```

**Step 3: Create requirements.txt**

```
streamlit>=1.30.0
folium>=0.15.0
streamlit-folium>=0.17.0
plotly>=5.18.0
pandas>=2.0.0
requests>=2.31.0
```

**Step 4: Create virtual environment and install dependencies**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install pytest
```

**Step 5: Create src/config.py**

```python
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "peoria_crime.db"

ARCGIS_BASE = "https://services1.arcgis.com/Vm4J3EDyqMzmDYgP/arcgis/rest/services"

ENDPOINTS = {
    "crimes": f"{ARCGIS_BASE}/Crimes_public_b259ad13665440579e8fa083818cdd9f/FeatureServer/0",
    "calls_for_service": f"{ARCGIS_BASE}/CallsForService_cc05ba2862d74015aea976e3aefe4f1f/FeatureServer/0",
    "shotspotter": f"{ARCGIS_BASE}/ShotSpotter_Dashboard_Data/FeatureServer/0",
    "beats": f"{ARCGIS_BASE}/Beat_cc05ba2862d74015aea976e3aefe4f1f/FeatureServer/0",
    "districts": f"{ARCGIS_BASE}/District_cc05ba2862d74015aea976e3aefe4f1f/FeatureServer/0",
    "community_policing": f"{ARCGIS_BASE}/CommunityPolicingAreas_public_b259ad13665440579e8fa083818cdd9f/FeatureServer/0",
}

PAGE_SIZE = 2000

CRIME_WEIGHTS = {
    "Homicide Offenses": 10,
    "Robbery": 7,
    "Kidnapping/Abduction": 7,
    "Sex Offenses": 6,
    "Assault Offenses": 5,
    "Arson": 5,
    "Burglary/Breaking & Entering": 4,
    "Weapon Law Violations": 4,
    "Motor Vehicle Theft": 3,
    "Larceny/Theft Offenses": 2,
    "Drug/Narcotic Offenses": 2,
    "Fraud Offenses": 2,
    "Stolen Property Offenses": 2,
    "Destruction/Damage/Vandalism": 1,
    "Trespass of Real Property": 1,
    "Disorderly Conduct": 1,
}
DEFAULT_WEIGHT = 1
```

**Step 6: Create src/__init__.py and tests/__init__.py**

Both empty files.

**Step 7: Commit**

```bash
git add .gitignore requirements.txt src/__init__.py src/config.py tests/__init__.py
git commit -m "chore: project setup with config and dependencies"
```

---

## Task 1: Database Layer

**Files:**
- Create: `src/database.py`
- Create: `tests/test_database.py`

**Step 1: Write failing tests for database initialization**

```python
# tests/test_database.py
import sqlite3
import os
import tempfile
import pytest
from src.database import init_db, get_connection


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "test_crime.db"


def test_init_db_creates_file(db_path):
    init_db(db_path)
    assert db_path.exists()


def test_init_db_creates_crimes_table(db_path):
    init_db(db_path)
    conn = get_connection(db_path)
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='crimes'"
    )
    assert cursor.fetchone() is not None
    conn.close()


def test_init_db_creates_calls_table(db_path):
    init_db(db_path)
    conn = get_connection(db_path)
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='calls_for_service'"
    )
    assert cursor.fetchone() is not None
    conn.close()


def test_init_db_creates_shotspotter_table(db_path):
    init_db(db_path)
    conn = get_connection(db_path)
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='shotspotter'"
    )
    assert cursor.fetchone() is not None
    conn.close()


def test_init_db_creates_boundaries_table(db_path):
    init_db(db_path)
    conn = get_connection(db_path)
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='boundaries'"
    )
    assert cursor.fetchone() is not None
    conn.close()


def test_init_db_creates_sync_log_table(db_path):
    init_db(db_path)
    conn = get_connection(db_path)
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='sync_log'"
    )
    assert cursor.fetchone() is not None
    conn.close()


def test_init_db_is_idempotent(db_path):
    init_db(db_path)
    init_db(db_path)  # should not raise
    conn = get_connection(db_path)
    cursor = conn.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='table'"
    )
    count = cursor.fetchone()[0]
    assert count == 5
    conn.close()
```

**Step 2: Run tests to verify they fail**

```bash
cd /home/nolu/Desktop/peoria_crime_tracking
.venv/bin/pytest tests/test_database.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'src.database'`

**Step 3: Write minimal implementation**

```python
# src/database.py
import sqlite3
from pathlib import Path


def get_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Path) -> None:
    conn = get_connection(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS crimes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            offense_id TEXT UNIQUE,
            call_id TEXT,
            statute TEXT,
            nibrs_code TEXT,
            nibrs_offense TEXT,
            nibrs_description TEXT,
            crime_against TEXT,
            attempt_completed TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            beat TEXT,
            district TEXT,
            neighborhood TEXT,
            weapon_category TEXT,
            weapon_description TEXT,
            report_date TEXT,
            report_year INTEGER,
            report_month INTEGER,
            report_hour INTEGER,
            report_dow TEXT,
            latitude REAL,
            longitude REAL,
            source TEXT DEFAULT 'peoria_pd_arcgis',
            synced_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS calls_for_service (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            call_id TEXT UNIQUE,
            call_type TEXT,
            priority TEXT,
            disposition TEXT,
            address TEXT,
            beat TEXT,
            district TEXT,
            call_date TEXT,
            latitude REAL,
            longitude REAL,
            source TEXT DEFAULT 'peoria_pd_arcgis',
            synced_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS shotspotter (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            incident_id TEXT UNIQUE,
            rounds_fired INTEGER,
            event_type TEXT,
            address TEXT,
            beat TEXT,
            district TEXT,
            event_date TEXT,
            latitude REAL,
            longitude REAL,
            source TEXT DEFAULT 'peoria_pd_arcgis',
            synced_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS boundaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            boundary_type TEXT,
            name TEXT,
            geometry_geojson TEXT,
            UNIQUE(boundary_type, name)
        );

        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            table_name TEXT,
            records_fetched INTEGER,
            started_at TEXT DEFAULT (datetime('now')),
            completed_at TEXT,
            status TEXT DEFAULT 'running'
        );

        CREATE INDEX IF NOT EXISTS idx_crimes_report_date ON crimes(report_date);
        CREATE INDEX IF NOT EXISTS idx_crimes_report_year ON crimes(report_year);
        CREATE INDEX IF NOT EXISTS idx_crimes_district ON crimes(district);
        CREATE INDEX IF NOT EXISTS idx_crimes_beat ON crimes(beat);
        CREATE INDEX IF NOT EXISTS idx_crimes_neighborhood ON crimes(neighborhood);
        CREATE INDEX IF NOT EXISTS idx_crimes_nibrs_offense ON crimes(nibrs_offense);
        CREATE INDEX IF NOT EXISTS idx_crimes_coords ON crimes(latitude, longitude);
        CREATE INDEX IF NOT EXISTS idx_calls_call_date ON calls_for_service(call_date);
        CREATE INDEX IF NOT EXISTS idx_calls_district ON calls_for_service(district);
        CREATE INDEX IF NOT EXISTS idx_shotspotter_event_date ON shotspotter(event_date);
        CREATE INDEX IF NOT EXISTS idx_boundaries_type ON boundaries(boundary_type);
    """)
    conn.commit()
    conn.close()
```

**Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_database.py -v
```

Expected: All 7 tests PASS.

**Step 5: Commit**

```bash
git add src/database.py tests/test_database.py
git commit -m "feat: database layer with schema and initialization"
```

---

## Task 2: ArcGIS Data Sync Module

**Files:**
- Create: `src/sync.py`
- Create: `tests/test_sync.py`

**Step 1: Write failing tests for ArcGIS fetching and data insertion**

```python
# tests/test_sync.py
import json
import pytest
from unittest.mock import patch, MagicMock
from src.database import init_db, get_connection
from src.sync import (
    fetch_arcgis_page,
    fetch_all_records,
    sync_crimes,
    sync_calls_for_service,
    sync_shotspotter,
    sync_boundaries,
    run_full_sync,
)


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "test_crime.db"
    init_db(path)
    return path


def _mock_arcgis_response(features, exceeded=False):
    return {
        "features": features,
        "exceededTransferLimit": exceeded,
    }


def _crime_feature(offense_id="23-001", nibrs_offense="Assault Offenses",
                    address="100 MAIN ST", lat=40.7, lon=-89.6,
                    report_date=1700000000000, report_year=2024):
    return {
        "attributes": {
            "offenseid": offense_id,
            "callid": f"PA{offense_id}",
            "statute": "720-5/12-3.05(a)(1)",
            "nibrscode": "13A",
            "nibrsoffense": nibrs_offense,
            "nibrsdesc": "AGGRAVATED ASSAULT",
            "nibrscrimeag": "Person",
            "nibrsgroup": "Group A",
            "attemptcompleted": "C",
            "fulladdr": address,
            "city": "PEORIA",
            "state": "IL",
            "zip5": "61602",
            "beat": "1A",
            "district": "10",
            "neighborhood": "Downtown",
            "weaponcat": "Firearm",
            "weapondesc": "HANDGUN",
            "reportdate": report_date,
            "reportyear": report_year,
            "reportmonth": 11,
            "reporthour": 14,
            "reportdow": "Tuesday",
        },
        "geometry": {"x": lon, "y": lat},
    }


class TestFetchArcgisPage:
    @patch("src.sync.requests.get")
    def test_returns_features(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _mock_arcgis_response(
            [_crime_feature()], exceeded=False
        )
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        features, has_more = fetch_arcgis_page("http://fake", offset=0)
        assert len(features) == 1
        assert has_more is False


class TestSyncCrimes:
    @patch("src.sync.fetch_all_records")
    def test_inserts_crime_records(self, mock_fetch, db_path):
        mock_fetch.return_value = [_crime_feature()]
        count = sync_crimes(db_path)
        assert count == 1

        conn = get_connection(db_path)
        row = conn.execute("SELECT * FROM crimes WHERE offense_id = '23-001'").fetchone()
        assert row is not None
        assert row["nibrs_offense"] == "Assault Offenses"
        assert row["latitude"] == 40.7
        conn.close()

    @patch("src.sync.fetch_all_records")
    def test_skips_duplicates(self, mock_fetch, db_path):
        mock_fetch.return_value = [_crime_feature(), _crime_feature()]
        count = sync_crimes(db_path)
        assert count == 1  # second one skipped


class TestRunFullSync:
    @patch("src.sync.sync_boundaries")
    @patch("src.sync.sync_shotspotter")
    @patch("src.sync.sync_calls_for_service")
    @patch("src.sync.sync_crimes")
    def test_calls_all_syncs(self, mock_crimes, mock_calls, mock_shot, mock_bounds, db_path):
        mock_crimes.return_value = 10
        mock_calls.return_value = 20
        mock_shot.return_value = 5
        mock_bounds.return_value = 3
        result = run_full_sync(db_path)
        assert result["crimes"] == 10
        assert result["calls_for_service"] == 20
        assert result["shotspotter"] == 5
        assert result["boundaries"] == 3
```

**Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_sync.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'src.sync'`

**Step 3: Write implementation**

```python
# src/sync.py
import requests
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from src.config import ENDPOINTS, PAGE_SIZE
from src.database import get_connection, init_db

logger = logging.getLogger(__name__)


def fetch_arcgis_page(url: str, offset: int = 0, where: str = "1=1") -> tuple[list, bool]:
    params = {
        "where": where,
        "outFields": "*",
        "outSR": 4326,
        "f": "json",
        "resultRecordCount": PAGE_SIZE,
        "resultOffset": offset,
    }
    resp = requests.get(url + "/query", params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    features = data.get("features", [])
    has_more = data.get("exceededTransferLimit", False)
    return features, has_more


def fetch_all_records(url: str, where: str = "1=1") -> list:
    all_features = []
    offset = 0
    while True:
        features, has_more = fetch_arcgis_page(url, offset=offset, where=where)
        all_features.extend(features)
        logger.info(f"Fetched {len(features)} records at offset {offset}")
        if not has_more or len(features) == 0:
            break
        offset += PAGE_SIZE
    return all_features


def _ts_to_iso(ms_timestamp) -> str | None:
    if ms_timestamp is None:
        return None
    try:
        return datetime.fromtimestamp(ms_timestamp / 1000, tz=timezone.utc).isoformat()
    except (ValueError, TypeError, OSError):
        return None


def sync_crimes(db_path: Path) -> int:
    features = fetch_all_records(ENDPOINTS["crimes"])
    conn = get_connection(db_path)
    count = 0
    for f in features:
        a = f.get("attributes", {})
        g = f.get("geometry", {})
        try:
            conn.execute("""
                INSERT OR IGNORE INTO crimes (
                    offense_id, call_id, statute, nibrs_code, nibrs_offense,
                    nibrs_description, crime_against, attempt_completed,
                    address, city, state, zip, beat, district, neighborhood,
                    weapon_category, weapon_description,
                    report_date, report_year, report_month, report_hour, report_dow,
                    latitude, longitude
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                a.get("offenseid"), a.get("callid"), a.get("statute"),
                a.get("nibrscode"), a.get("nibrsoffense"), a.get("nibrsdesc"),
                a.get("nibrscrimeag"), a.get("attemptcompleted"),
                a.get("fulladdr"), a.get("city"), a.get("state"), a.get("zip5"),
                a.get("beat"), a.get("district"), a.get("neighborhood"),
                a.get("weaponcat"), a.get("weapondesc"),
                _ts_to_iso(a.get("reportdate")), a.get("reportyear"),
                a.get("reportmonth"), a.get("reporthour"), a.get("reportdow"),
                g.get("y"), g.get("x"),
            ))
            count += conn.total_changes  # track via rowcount
        except Exception as e:
            logger.warning(f"Skipping crime record: {e}")
    conn.commit()
    actual = conn.execute("SELECT count(*) FROM crimes").fetchone()[0]
    conn.close()
    _log_sync(db_path, "peoria_pd_arcgis", "crimes", actual)
    return actual


def sync_calls_for_service(db_path: Path) -> int:
    features = fetch_all_records(ENDPOINTS["calls_for_service"])
    conn = get_connection(db_path)
    for f in features:
        a = f.get("attributes", {})
        g = f.get("geometry", {})
        try:
            conn.execute("""
                INSERT OR IGNORE INTO calls_for_service (
                    call_id, call_type, priority, disposition,
                    address, beat, district, call_date,
                    latitude, longitude
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                a.get("callid") or a.get("CALLID") or a.get("CallID"),
                a.get("calltype") or a.get("CALLTYPE") or a.get("calltypedesc"),
                a.get("priority") or a.get("PRIORITY"),
                a.get("disposition") or a.get("DISPOSITION") or a.get("dispositiondesc"),
                a.get("fulladdr") or a.get("FULLADDR") or a.get("address"),
                a.get("beat") or a.get("BEAT"),
                a.get("district") or a.get("DISTRICT"),
                _ts_to_iso(a.get("calldate") or a.get("CALLDATE") or a.get("calldatetime")),
                g.get("y") if g else None,
                g.get("x") if g else None,
            ))
        except Exception as e:
            logger.warning(f"Skipping call record: {e}")
    conn.commit()
    count = conn.execute("SELECT count(*) FROM calls_for_service").fetchone()[0]
    conn.close()
    _log_sync(db_path, "peoria_pd_arcgis", "calls_for_service", count)
    return count


def sync_shotspotter(db_path: Path) -> int:
    features = fetch_all_records(ENDPOINTS["shotspotter"])
    conn = get_connection(db_path)
    for f in features:
        a = f.get("attributes", {})
        g = f.get("geometry", {})
        try:
            conn.execute("""
                INSERT OR IGNORE INTO shotspotter (
                    incident_id, rounds_fired, event_type,
                    address, beat, district, event_date,
                    latitude, longitude
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(a.get("incidentid") or a.get("INCIDENTID") or a.get("OBJECTID", "")),
                a.get("rounds") or a.get("ROUNDS") or a.get("roundsfired"),
                a.get("type") or a.get("TYPE") or a.get("eventtype"),
                a.get("fulladdr") or a.get("address") or a.get("ADDRESS"),
                a.get("beat") or a.get("BEAT"),
                a.get("district") or a.get("DISTRICT"),
                _ts_to_iso(a.get("eventdate") or a.get("datetime") or a.get("DATETIME")),
                g.get("y") if g else None,
                g.get("x") if g else None,
            ))
        except Exception as e:
            logger.warning(f"Skipping shotspotter record: {e}")
    conn.commit()
    count = conn.execute("SELECT count(*) FROM shotspotter").fetchone()[0]
    conn.close()
    _log_sync(db_path, "peoria_pd_arcgis", "shotspotter", count)
    return count


def sync_boundaries(db_path: Path) -> int:
    conn = get_connection(db_path)
    total = 0
    for btype, key in [("beat", "beats"), ("district", "districts"),
                        ("community_policing", "community_policing")]:
        try:
            features = fetch_all_records(ENDPOINTS[key])
            for f in features:
                a = f.get("attributes", {})
                g = f.get("geometry", {})
                name = (a.get("name") or a.get("NAME") or a.get("Beat")
                        or a.get("District") or a.get("AREA") or str(a.get("OBJECTID", "")))
                conn.execute("""
                    INSERT OR REPLACE INTO boundaries (boundary_type, name, geometry_geojson)
                    VALUES (?, ?, ?)
                """, (btype, str(name), json.dumps(g)))
                total += 1
        except Exception as e:
            logger.warning(f"Failed to sync {btype} boundaries: {e}")
    conn.commit()
    conn.close()
    _log_sync(db_path, "peoria_pd_arcgis", "boundaries", total)
    return total


def _log_sync(db_path: Path, source: str, table: str, count: int) -> None:
    conn = get_connection(db_path)
    conn.execute("""
        INSERT INTO sync_log (source, table_name, records_fetched, completed_at, status)
        VALUES (?, ?, ?, datetime('now'), 'completed')
    """, (source, table, count))
    conn.commit()
    conn.close()


def run_full_sync(db_path: Path) -> dict:
    init_db(db_path)
    return {
        "crimes": sync_crimes(db_path),
        "calls_for_service": sync_calls_for_service(db_path),
        "shotspotter": sync_shotspotter(db_path),
        "boundaries": sync_boundaries(db_path),
    }
```

**Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_sync.py -v
```

Expected: All tests PASS.

**Step 5: Commit**

```bash
git add src/sync.py tests/test_sync.py
git commit -m "feat: ArcGIS data sync module with pagination"
```

---

## Task 3: Query Engine & Score Calculator

**Files:**
- Create: `src/queries.py`
- Create: `tests/test_queries.py`

**Step 1: Write failing tests**

```python
# tests/test_queries.py
import pytest
from src.database import init_db, get_connection
from src.queries import (
    get_crime_counts_by_area,
    get_crime_trend,
    get_crimes_near_address,
    compute_severity_score,
    get_top_crime_types,
    get_area_options,
)


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "test.db"
    init_db(path)
    conn = get_connection(path)
    # Insert test data
    for i in range(10):
        conn.execute("""
            INSERT INTO crimes (offense_id, nibrs_offense, district, beat, neighborhood,
                report_date, report_year, report_month, latitude, longitude, address)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            f"OFF-{i}", "Assault Offenses" if i < 6 else "Larceny/Theft Offenses",
            "10", "1A", "Downtown",
            f"2025-0{(i % 3) + 1}-15T12:00:00+00:00", 2025, (i % 3) + 1,
            40.69 + i * 0.001, -89.59 + i * 0.001,
            f"{100 + i} MAIN ST PEORIA",
        ))
    conn.commit()
    conn.close()
    return path


def test_get_crime_counts_by_area(db_path):
    result = get_crime_counts_by_area(db_path, "district")
    assert "10" in result
    assert result["10"] == 10


def test_get_crime_trend(db_path):
    result = get_crime_trend(db_path, year=2025)
    assert len(result) > 0
    assert sum(r["count"] for r in result) == 10


def test_get_top_crime_types(db_path):
    result = get_top_crime_types(db_path)
    assert result[0]["type"] == "Assault Offenses"
    assert result[0]["count"] == 6


def test_compute_severity_score(db_path):
    score = compute_severity_score(db_path, district="10")
    assert score > 0


def test_get_area_options(db_path):
    options = get_area_options(db_path, "district")
    assert "10" in options
```

**Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_queries.py -v
```

Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write implementation**

```python
# src/queries.py
from pathlib import Path
from src.database import get_connection
from src.config import CRIME_WEIGHTS, DEFAULT_WEIGHT


def get_crime_counts_by_area(db_path: Path, area_type: str,
                              year: int | None = None) -> dict[str, int]:
    conn = get_connection(db_path)
    query = f"SELECT {area_type}, COUNT(*) as cnt FROM crimes"
    params = []
    if year:
        query += " WHERE report_year = ?"
        params.append(year)
    query += f" GROUP BY {area_type}"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return {row[0]: row[1] for row in rows if row[0]}


def get_crime_trend(db_path: Path, year: int | None = None,
                     district: str | None = None, beat: str | None = None,
                     neighborhood: str | None = None) -> list[dict]:
    conn = get_connection(db_path)
    conditions = []
    params = []
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
    query = f"""
        SELECT report_year, report_month, COUNT(*) as count
        FROM crimes {where}
        GROUP BY report_year, report_month
        ORDER BY report_year, report_month
    """
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [{"year": r[0], "month": r[1], "count": r[2]} for r in rows]


def get_crimes_near_address(db_path: Path, lat: float, lon: float,
                             radius_miles: float = 0.5) -> list[dict]:
    degree_approx = radius_miles / 69.0
    conn = get_connection(db_path)
    rows = conn.execute("""
        SELECT * FROM crimes
        WHERE latitude BETWEEN ? AND ?
          AND longitude BETWEEN ? AND ?
        ORDER BY report_date DESC
    """, (lat - degree_approx, lat + degree_approx,
          lon - degree_approx, lon + degree_approx)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def compute_severity_score(db_path: Path, district: str | None = None,
                            beat: str | None = None,
                            neighborhood: str | None = None,
                            year: int | None = None) -> float:
    conn = get_connection(db_path)
    conditions = []
    params = []
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
        f"SELECT nibrs_offense, COUNT(*) as cnt FROM crimes {where} GROUP BY nibrs_offense",
        params
    ).fetchall()
    conn.close()

    score = sum(
        CRIME_WEIGHTS.get(row[0], DEFAULT_WEIGHT) * row[1]
        for row in rows
    )
    return float(score)


def get_top_crime_types(db_path: Path, limit: int = 10,
                         district: str | None = None, beat: str | None = None,
                         neighborhood: str | None = None) -> list[dict]:
    conn = get_connection(db_path)
    conditions = []
    params = []
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
    rows = conn.execute(f"""
        SELECT nibrs_offense, COUNT(*) as cnt FROM crimes {where}
        GROUP BY nibrs_offense ORDER BY cnt DESC LIMIT ?
    """, params + [limit]).fetchall()
    conn.close()
    return [{"type": r[0], "count": r[1]} for r in rows]


def get_area_options(db_path: Path, area_type: str) -> list[str]:
    conn = get_connection(db_path)
    rows = conn.execute(
        f"SELECT DISTINCT {area_type} FROM crimes WHERE {area_type} IS NOT NULL ORDER BY {area_type}"
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]
```

**Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_queries.py -v
```

Expected: All tests PASS.

**Step 5: Commit**

```bash
git add src/queries.py tests/test_queries.py
git commit -m "feat: query engine with severity scoring and trend analysis"
```

---

## Task 4: Streamlit App — Dashboard Page

**Files:**
- Create: `app.py`
- Create: `src/pages/__init__.py`
- Create: `src/pages/dashboard.py`
- Create: `src/map_utils.py`

**Step 1: Create map utility module**

```python
# src/map_utils.py
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
        popup_text = f"""
            <b>{c.get('nibrs_offense', 'Unknown')}</b><br>
            {c.get('nibrs_description', '')}<br>
            {c.get('address', '')}<br>
            {c.get('report_date', '')[:10] if c.get('report_date') else ''}
        """
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
```

**Step 2: Create dashboard page**

```python
# src/pages/dashboard.py
import streamlit as st
import plotly.express as px
import pandas as pd
from pathlib import Path
from streamlit_folium import st_folium

from src.config import DB_PATH
from src.database import init_db, get_connection
from src.queries import (
    get_crime_counts_by_area,
    get_crime_trend,
    compute_severity_score,
    get_top_crime_types,
    get_area_options,
    get_crimes_near_address,
)
from src.map_utils import create_base_map, add_crime_heatmap, add_crime_markers, add_boundary_overlay


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


def render(db_path: Path = DB_PATH):
    st.header("Crime Dashboard")

    init_db(db_path)
    conn = get_connection(db_path)
    total = conn.execute("SELECT count(*) FROM crimes").fetchone()[0]
    conn.close()

    if total == 0:
        st.warning("No crime data loaded yet. Go to **Data Sources & Sync** to pull data.")
        return

    # Filters
    col1, col2, col3, col4 = st.columns(4)

    years = get_area_options(db_path, "report_year")
    with col1:
        selected_year = st.selectbox("Year", ["All"] + [str(y) for y in years])

    districts = get_area_options(db_path, "district")
    with col2:
        selected_district = st.selectbox("District", ["All"] + districts)

    beats = get_area_options(db_path, "beat")
    with col3:
        selected_beat = st.selectbox("Beat", ["All"] + beats)

    neighborhoods = get_area_options(db_path, "neighborhood")
    with col4:
        selected_neighborhood = st.selectbox("Neighborhood", ["All"] + neighborhoods)

    # Build filter kwargs
    filters = {}
    if selected_year != "All":
        filters["year"] = int(selected_year)
    if selected_district != "All":
        filters["district"] = selected_district
    if selected_beat != "All":
        filters["beat"] = selected_beat
    if selected_neighborhood != "All":
        filters["neighborhood"] = selected_neighborhood

    # Severity score
    area_score = compute_severity_score(db_path, **filters)

    # City average for comparison
    year_filter = {"year": filters["year"]} if "year" in filters else {}
    city_total_score = compute_severity_score(db_path, **year_filter)
    n_districts = len(districts) if districts else 1
    city_avg = city_total_score / n_districts

    rating, color = _score_to_rating(area_score, city_avg)

    # Score card + summary
    metric_col1, metric_col2, metric_col3 = st.columns(3)

    with metric_col1:
        st.markdown(
            f'<div style="text-align:center; padding:20px; background:{color}20; '
            f'border-left:5px solid {color}; border-radius:5px;">'
            f'<h2 style="color:{color}; margin:0;">{rating}</h2>'
            f'<p style="margin:0;">Crime Severity</p>'
            f'<p style="margin:0; font-size:0.8em;">Score: {area_score:.0f}</p></div>',
            unsafe_allow_html=True,
        )

    top_crimes = get_top_crime_types(db_path, limit=5, **{
        k: v for k, v in filters.items() if k != "year"
    })

    with metric_col2:
        conn = get_connection(db_path)
        conditions = []
        params = []
        if "year" in filters:
            conditions.append("report_year = ?")
            params.append(filters["year"])
        if "district" in filters:
            conditions.append("district = ?")
            params.append(filters["district"])
        if "beat" in filters:
            conditions.append("beat = ?")
            params.append(filters["beat"])
        if "neighborhood" in filters:
            conditions.append("neighborhood = ?")
            params.append(filters["neighborhood"])
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        filtered_total = conn.execute(
            f"SELECT count(*) FROM crimes{where}", params
        ).fetchone()[0]
        conn.close()
        st.metric("Total Crimes", f"{filtered_total:,}")

    with metric_col3:
        if top_crimes:
            st.markdown("**Top Crime Types**")
            for tc in top_crimes[:3]:
                st.markdown(f"- {tc['type']}: **{tc['count']:,}**")

    # Map
    st.subheader("Crime Map")
    conn = get_connection(db_path)
    conditions = []
    params = []
    if "year" in filters:
        conditions.append("report_year = ?")
        params.append(filters["year"])
    if "district" in filters:
        conditions.append("district = ?")
        params.append(filters["district"])
    if "beat" in filters:
        conditions.append("beat = ?")
        params.append(filters["beat"])
    if "neighborhood" in filters:
        conditions.append("neighborhood = ?")
        params.append(filters["neighborhood"])
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    rows = conn.execute(
        f"SELECT * FROM crimes{where} ORDER BY report_date DESC LIMIT 2000", params
    ).fetchall()
    conn.close()
    crimes = [dict(r) for r in rows]

    m = create_base_map()
    m = add_crime_heatmap(m, crimes)
    m = add_crime_markers(m, crimes, max_markers=300)
    m = add_boundary_overlay(m, db_path, "district")
    st_folium(m, width=None, height=500, use_container_width=True)

    # Trend chart
    st.subheader("Crime Trend")
    trend_data = get_crime_trend(db_path, **filters)
    if trend_data:
        df = pd.DataFrame(trend_data)
        df["period"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)
        fig = px.bar(df, x="period", y="count", title="Crimes by Month")
        fig.update_layout(xaxis_title="Month", yaxis_title="Crime Count")
        st.plotly_chart(fig, use_container_width=True)
```

**Step 3: Create empty __init__.py**

`src/pages/__init__.py` — empty file.

**Step 4: Create main app.py**

```python
# app.py
import streamlit as st
from src.config import DB_PATH
from src.database import init_db

st.set_page_config(
    page_title="Peoria Crime Tracker",
    page_icon="🔍",
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
```

**Step 5: Commit**

```bash
git add app.py src/map_utils.py src/pages/__init__.py src/pages/dashboard.py
git commit -m "feat: dashboard page with map, score card, and trend chart"
```

---

## Task 5: Explore Data Page

**Files:**
- Create: `src/pages/explore.py`

**Step 1: Write the page**

```python
# src/pages/explore.py
import streamlit as st
import pandas as pd
from pathlib import Path
from streamlit_folium import st_folium

from src.config import DB_PATH
from src.database import get_connection
from src.map_utils import create_base_map, add_crime_markers
from src.queries import get_area_options


def render(db_path: Path = DB_PATH):
    st.header("Explore Crime Data")

    # Source selector
    source = st.radio("Data Source", ["Crimes", "Calls for Service", "ShotSpotter"], horizontal=True)
    table = {"Crimes": "crimes", "Calls for Service": "calls_for_service", "ShotSpotter": "shotspotter"}[source]

    conn = get_connection(db_path)
    total = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    conn.close()

    if total == 0:
        st.warning(f"No {source} data loaded. Go to **Data Sources & Sync** to pull data.")
        return

    st.caption(f"{total:,} total records")

    # Filters
    col1, col2 = st.columns(2)

    if table == "crimes":
        with col1:
            crime_types = get_area_options(db_path, "nibrs_offense")
            selected_type = st.selectbox("Crime Type", ["All"] + crime_types)
        with col2:
            districts = get_area_options(db_path, "district")
            selected_district = st.selectbox("District", ["All"] + districts, key="explore_district")

        conditions = []
        params = []
        if selected_type != "All":
            conditions.append("nibrs_offense = ?")
            params.append(selected_type)
        if selected_district != "All":
            conditions.append("district = ?")
            params.append(selected_district)

        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        conn = get_connection(db_path)
        df = pd.read_sql_query(
            f"SELECT offense_id, nibrs_offense, nibrs_description, address, district, beat, "
            f"neighborhood, report_date, latitude, longitude FROM {table}{where} "
            f"ORDER BY report_date DESC LIMIT 5000",
            conn, params=params,
        )
        conn.close()

    elif table == "calls_for_service":
        conn = get_connection(db_path)
        df = pd.read_sql_query(
            f"SELECT call_id, call_type, priority, disposition, address, district, beat, "
            f"call_date, latitude, longitude FROM {table} ORDER BY call_date DESC LIMIT 5000",
            conn,
        )
        conn.close()

    else:  # shotspotter
        conn = get_connection(db_path)
        df = pd.read_sql_query(
            f"SELECT incident_id, rounds_fired, event_type, address, district, beat, "
            f"event_date, latitude, longitude FROM {table} ORDER BY event_date DESC LIMIT 5000",
            conn,
        )
        conn.close()

    st.subheader(f"{len(df):,} records shown")

    # Map
    if not df.empty and "latitude" in df.columns:
        m = create_base_map()
        crimes_list = df.dropna(subset=["latitude", "longitude"]).to_dict("records")
        m = add_crime_markers(m, crimes_list, max_markers=500)
        st_folium(m, width=None, height=400, use_container_width=True)

    # Table
    st.dataframe(df, use_container_width=True, height=400)

    # CSV export
    csv = df.to_csv(index=False)
    st.download_button("Download CSV", csv, f"peoria_{table}_export.csv", "text/csv")
```

**Step 2: Commit**

```bash
git add src/pages/explore.py
git commit -m "feat: explore data page with filters, map, table, and CSV export"
```

---

## Task 6: Trends & Comparison Page

**Files:**
- Create: `src/pages/trends.py`

**Step 1: Write the page**

```python
# src/pages/trends.py
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pathlib import Path

from src.config import DB_PATH
from src.database import get_connection
from src.queries import get_crime_trend, get_area_options, compute_severity_score


def render(db_path: Path = DB_PATH):
    st.header("Trends & Comparison")

    conn = get_connection(db_path)
    total = conn.execute("SELECT count(*) FROM crimes").fetchone()[0]
    conn.close()

    if total == 0:
        st.warning("No crime data loaded yet.")
        return

    tab1, tab2, tab3 = st.tabs(["Monthly Trends", "Area Comparison", "Time Patterns"])

    with tab1:
        _render_monthly_trends(db_path)

    with tab2:
        _render_area_comparison(db_path)

    with tab3:
        _render_time_patterns(db_path)


def _render_monthly_trends(db_path: Path):
    st.subheader("Crime Trends Over Time")

    years = get_area_options(db_path, "report_year")
    selected_years = st.multiselect("Years to show", years, default=years[-2:] if len(years) >= 2 else years)

    conn = get_connection(db_path)
    if selected_years:
        placeholders = ",".join("?" * len(selected_years))
        df = pd.read_sql_query(
            f"SELECT report_year, report_month, nibrs_offense, COUNT(*) as count "
            f"FROM crimes WHERE report_year IN ({placeholders}) "
            f"GROUP BY report_year, report_month, nibrs_offense "
            f"ORDER BY report_year, report_month",
            conn, params=selected_years,
        )
    else:
        df = pd.DataFrame()
    conn.close()

    if df.empty:
        st.info("No data for selected years.")
        return

    # Overall trend
    monthly = df.groupby(["report_year", "report_month"])["count"].sum().reset_index()
    monthly["period"] = monthly["report_year"].astype(str) + "-" + monthly["report_month"].astype(str).str.zfill(2)

    fig = px.line(monthly, x="period", y="count", title="Total Crimes by Month",
                  markers=True)
    fig.update_layout(xaxis_title="Month", yaxis_title="Crime Count")
    st.plotly_chart(fig, use_container_width=True)

    # Stacked by type
    top_types = df.groupby("nibrs_offense")["count"].sum().nlargest(8).index.tolist()
    df_top = df[df["nibrs_offense"].isin(top_types)]
    df_top_monthly = df_top.groupby(["report_year", "report_month", "nibrs_offense"])["count"].sum().reset_index()
    df_top_monthly["period"] = df_top_monthly["report_year"].astype(str) + "-" + df_top_monthly["report_month"].astype(str).str.zfill(2)

    fig2 = px.area(df_top_monthly, x="period", y="count", color="nibrs_offense",
                   title="Crime Types Over Time")
    st.plotly_chart(fig2, use_container_width=True)


def _render_area_comparison(db_path: Path):
    st.subheader("Compare Two Areas")

    col1, col2 = st.columns(2)
    districts = get_area_options(db_path, "district")

    with col1:
        area1 = st.selectbox("Area 1 (District)", districts, key="cmp1")
    with col2:
        area2 = st.selectbox("Area 2 (District)", districts, key="cmp2",
                              index=min(1, len(districts) - 1))

    if area1 and area2:
        trend1 = get_crime_trend(db_path, district=area1)
        trend2 = get_crime_trend(db_path, district=area2)

        df1 = pd.DataFrame(trend1)
        df2 = pd.DataFrame(trend2)

        if not df1.empty:
            df1["period"] = df1["year"].astype(str) + "-" + df1["month"].astype(str).str.zfill(2)
            df1["area"] = f"District {area1}"
        if not df2.empty:
            df2["period"] = df2["year"].astype(str) + "-" + df2["month"].astype(str).str.zfill(2)
            df2["area"] = f"District {area2}"

        combined = pd.concat([df1, df2], ignore_index=True)
        if not combined.empty:
            fig = px.line(combined, x="period", y="count", color="area",
                         title=f"District {area1} vs District {area2}", markers=True)
            st.plotly_chart(fig, use_container_width=True)

        # Score comparison
        s1 = compute_severity_score(db_path, district=area1)
        s2 = compute_severity_score(db_path, district=area2)
        mc1, mc2 = st.columns(2)
        with mc1:
            st.metric(f"District {area1} Score", f"{s1:.0f}")
        with mc2:
            st.metric(f"District {area2} Score", f"{s2:.0f}")


def _render_time_patterns(db_path: Path):
    st.subheader("When Do Crimes Happen?")

    conn = get_connection(db_path)
    # Day of week / hour heatmap
    df = pd.read_sql_query("""
        SELECT report_dow, report_hour, COUNT(*) as count
        FROM crimes
        WHERE report_dow IS NOT NULL AND report_hour IS NOT NULL
        GROUP BY report_dow, report_hour
    """, conn)
    conn.close()

    if df.empty:
        st.info("No time pattern data available.")
        return

    dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    df["dow_num"] = df["report_dow"].map({d: i for i, d in enumerate(dow_order)})
    df = df.dropna(subset=["dow_num"])

    pivot = df.pivot_table(values="count", index="report_dow", columns="report_hour", fill_value=0)
    # Reorder rows
    pivot = pivot.reindex([d for d in dow_order if d in pivot.index])

    fig = px.imshow(
        pivot,
        labels=dict(x="Hour of Day", y="Day of Week", color="Crimes"),
        title="Crime Frequency by Day and Hour",
        color_continuous_scale="YlOrRd",
        aspect="auto",
    )
    st.plotly_chart(fig, use_container_width=True)
```

**Step 2: Commit**

```bash
git add src/pages/trends.py
git commit -m "feat: trends page with monthly, comparison, and time pattern views"
```

---

## Task 7: Data Sources & Sync Page

**Files:**
- Create: `src/pages/sync_page.py`

**Step 1: Write the page**

```python
# src/pages/sync_page.py
import streamlit as st
import pandas as pd
from pathlib import Path

from src.config import DB_PATH, ENDPOINTS
from src.database import get_connection, init_db
from src.sync import run_full_sync, sync_crimes, sync_calls_for_service, sync_shotspotter, sync_boundaries


def render(db_path: Path = DB_PATH):
    st.header("Data Sources & Sync")

    init_db(db_path)

    # Current status
    st.subheader("Data Status")
    conn = get_connection(db_path)

    tables = {
        "Crimes": "crimes",
        "Calls for Service": "calls_for_service",
        "ShotSpotter": "shotspotter",
        "Boundaries": "boundaries",
    }

    cols = st.columns(len(tables))
    for i, (label, table) in enumerate(tables.items()):
        count = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        with cols[i]:
            st.metric(label, f"{count:,}")

    # Last sync info
    st.subheader("Sync History")
    sync_df = pd.read_sql_query(
        "SELECT source, table_name, records_fetched, started_at, completed_at, status "
        "FROM sync_log ORDER BY id DESC LIMIT 20",
        conn,
    )
    conn.close()

    if not sync_df.empty:
        st.dataframe(sync_df, use_container_width=True)
    else:
        st.info("No sync history yet.")

    # Sync controls
    st.subheader("Sync Controls")
    st.caption("Pull latest data from Peoria PD ArcGIS services.")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Full Sync (All Sources)", type="primary"):
            with st.spinner("Syncing all data sources... This may take several minutes."):
                result = run_full_sync(db_path)
            st.success(
                f"Sync complete! Crimes: {result['crimes']:,}, "
                f"Calls: {result['calls_for_service']:,}, "
                f"ShotSpotter: {result['shotspotter']:,}, "
                f"Boundaries: {result['boundaries']:,}"
            )
            st.rerun()

    with col2:
        source_choice = st.selectbox("Or sync a single source:", list(tables.keys()))
        if st.button("Sync Selected"):
            sync_fn = {
                "Crimes": sync_crimes,
                "Calls for Service": sync_calls_for_service,
                "ShotSpotter": sync_shotspotter,
                "Boundaries": sync_boundaries,
            }[source_choice]
            with st.spinner(f"Syncing {source_choice}..."):
                count = sync_fn(db_path)
            st.success(f"Synced {count:,} {source_choice} records.")
            st.rerun()

    # Data sources transparency
    st.subheader("Data Sources")
    st.markdown("""
    **Primary: Peoria Police Department (ArcGIS Open Data)**
    - Crimes, Calls for Service, ShotSpotter, Police Boundaries
    - Updated regularly by the Peoria Police Department
    - No authentication required — public data

    **Endpoints:**
    """)
    for name, url in ENDPOINTS.items():
        st.code(url, language=None)

    st.subheader("Score Methodology")
    st.markdown("""
    The **Crime Severity Score** uses weighted crime counts:

    | Category | Weight |
    |----------|--------|
    | Homicide | 10 |
    | Robbery, Kidnapping | 7 |
    | Sex Offenses | 6 |
    | Assault, Arson | 5 |
    | Burglary, Weapons | 4 |
    | Vehicle Theft | 3 |
    | Theft, Drugs, Fraud | 2 |
    | Vandalism, Trespass, Disorderly | 1 |

    Scores are compared against the city-wide average per district to produce
    a relative rating (Low / Moderate / High / Very High).
    """)
```

**Step 2: Commit**

```bash
git add src/pages/sync_page.py
git commit -m "feat: data sources page with sync controls and methodology"
```

---

## Task 8: Integration Test — Full App Smoke Test

**Files:**
- Create: `tests/test_app_smoke.py`

**Step 1: Write smoke test**

```python
# tests/test_app_smoke.py
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
```

**Step 2: Run all tests**

```bash
.venv/bin/pytest tests/ -v
```

Expected: All tests PASS.

**Step 3: Commit**

```bash
git add tests/test_app_smoke.py
git commit -m "test: add integration smoke test for full query and map flow"
```

---

## Task 9: Final Polish & Run

**Step 1: Verify the app starts**

```bash
cd /home/nolu/Desktop/peoria_crime_tracking
source .venv/bin/activate
streamlit run app.py --server.headless true
```

Verify no import errors. Stop with Ctrl+C.

**Step 2: Run full test suite one final time**

```bash
.venv/bin/pytest tests/ -v --tb=short
```

Expected: All tests PASS.

**Step 3: Final commit**

```bash
git add -A
git commit -m "chore: final integration and polish"
```

---

## Summary

| Task | What | Files |
|------|------|-------|
| 0 | Project setup | requirements.txt, config.py, .gitignore |
| 1 | Database layer | src/database.py, tests/test_database.py |
| 2 | ArcGIS sync module | src/sync.py, tests/test_sync.py |
| 3 | Query engine + scoring | src/queries.py, tests/test_queries.py |
| 4 | Dashboard page | app.py, src/pages/dashboard.py, src/map_utils.py |
| 5 | Explore data page | src/pages/explore.py |
| 6 | Trends page | src/pages/trends.py |
| 7 | Sync page | src/pages/sync_page.py |
| 8 | Smoke tests | tests/test_app_smoke.py |
| 9 | Final polish & run | — |
