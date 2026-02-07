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
