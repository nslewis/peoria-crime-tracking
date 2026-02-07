import json
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path

from src.config import ENDPOINTS, PAGE_SIZE
from src.database import get_connection, init_db

logger = logging.getLogger(__name__)


def fetch_arcgis_page(
    url: str, offset: int = 0, where: str = "1=1"
) -> tuple[list, bool]:
    """Fetch one page from an ArcGIS REST API query endpoint."""
    params = {
        "where": where,
        "outFields": "*",
        "outSR": 4326,
        "f": "json",
        "resultRecordCount": PAGE_SIZE,
        "resultOffset": offset,
    }
    resp = requests.get(f"{url}/query", params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    features = data.get("features", [])
    # ArcGIS signals more data via exceededTransferLimit
    has_more = data.get("exceededTransferLimit", False)
    return features, has_more


def fetch_all_records(url: str, where: str = "1=1") -> list:
    """Paginate through all pages of an ArcGIS feature layer."""
    all_features: list = []
    offset = 0
    while True:
        features, has_more = fetch_arcgis_page(url, offset, where)
        all_features.extend(features)
        if not has_more or len(features) == 0:
            break
        offset += len(features)
    return all_features


def _ts_to_iso(ms_timestamp: int | None) -> str | None:
    """Convert a millisecond unix timestamp to an ISO 8601 string."""
    if ms_timestamp is None:
        return None
    dt = datetime.fromtimestamp(ms_timestamp / 1000, tz=timezone.utc)
    return dt.isoformat()


def _get_attr(attributes: dict, *keys: str):
    """Try multiple key variants and return the first match, or None."""
    for key in keys:
        if key in attributes:
            return attributes[key]
    return None


def sync_crimes(db_path: Path) -> int:
    """Fetch all crime records and insert into the crimes table."""
    url = ENDPOINTS["crimes"]
    features = fetch_all_records(url)
    conn = get_connection(db_path)
    for feat in features:
        attrs = feat.get("attributes", {})
        geom = feat.get("geometry", {})
        conn.execute(
            """INSERT OR IGNORE INTO crimes (
                offense_id, call_id, statute, nibrs_code, nibrs_offense,
                nibrs_description, crime_against, attempt_completed,
                address, city, state, zip, beat, district, neighborhood,
                weapon_category, weapon_description, report_date,
                report_year, report_month, report_hour, report_dow,
                latitude, longitude
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                attrs.get("offenseid"),
                attrs.get("callid"),
                attrs.get("statute"),
                attrs.get("nibrscode"),
                attrs.get("nibrsoffense"),
                attrs.get("nibrsdesc"),
                attrs.get("nibrscrimeag"),
                attrs.get("attemptcompleted"),
                attrs.get("fulladdr"),
                attrs.get("city"),
                attrs.get("state"),
                attrs.get("zip5"),
                attrs.get("beat"),
                attrs.get("district"),
                attrs.get("neighborhood"),
                attrs.get("weaponcat"),
                attrs.get("weapondesc"),
                _ts_to_iso(attrs.get("reportdate")),
                attrs.get("reportyear"),
                attrs.get("reportmonth"),
                attrs.get("reporthour"),
                attrs.get("reportdow"),
                geom.get("y"),
                geom.get("x"),
            ),
        )
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM crimes").fetchone()[0]
    conn.close()
    _log_sync(db_path, "peoria_pd_arcgis", "crimes", count)
    logger.info("Synced %d crime records", count)
    return count


def sync_calls_for_service(db_path: Path) -> int:
    """Fetch all calls-for-service records and insert into the table."""
    url = ENDPOINTS["calls_for_service"]
    features = fetch_all_records(url)
    conn = get_connection(db_path)
    for feat in features:
        attrs = feat.get("attributes", {})
        geom = feat.get("geometry", {})
        conn.execute(
            """INSERT OR IGNORE INTO calls_for_service (
                call_id, call_type, priority, disposition,
                address, beat, district, call_date,
                latitude, longitude
            ) VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                _get_attr(attrs, "callid", "CallID", "CALLID", "call_id"),
                _get_attr(attrs, "calltype", "CallType", "CALLTYPE", "call_type"),
                _get_attr(attrs, "priority", "Priority", "PRIORITY"),
                _get_attr(attrs, "disposition", "Disposition", "DISPOSITION"),
                _get_attr(attrs, "fulladdr", "FullAddr", "FULLADDR", "address"),
                _get_attr(attrs, "beat", "Beat", "BEAT"),
                _get_attr(attrs, "district", "District", "DISTRICT"),
                _ts_to_iso(
                    _get_attr(attrs, "calldate", "CallDate", "CALLDATE", "call_date")
                ),
                geom.get("y") if geom else None,
                geom.get("x") if geom else None,
            ),
        )
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM calls_for_service").fetchone()[0]
    conn.close()
    _log_sync(db_path, "peoria_pd_arcgis", "calls_for_service", count)
    logger.info("Synced %d calls-for-service records", count)
    return count


def sync_shotspotter(db_path: Path) -> int:
    """Fetch all ShotSpotter records and insert into the table."""
    url = ENDPOINTS["shotspotter"]
    features = fetch_all_records(url)
    conn = get_connection(db_path)
    for feat in features:
        attrs = feat.get("attributes", {})
        geom = feat.get("geometry", {})
        conn.execute(
            """INSERT OR IGNORE INTO shotspotter (
                incident_id, rounds_fired, event_type,
                address, beat, district, event_date,
                latitude, longitude
            ) VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                _get_attr(
                    attrs, "ShotSpotter_ID", "incidentid", "IncidentID",
                    "INCIDENTID", "incident_id",
                ),
                _get_attr(
                    attrs, "Rounds", "roundsfired", "RoundsFired",
                    "ROUNDSFIRED", "rounds_fired",
                ),
                _get_attr(
                    attrs, "Type", "eventtype", "EventType",
                    "EVENTTYPE", "event_type",
                ),
                _get_attr(attrs, "Address", "fulladdr", "FullAddr", "FULLADDR", "address"),
                _get_attr(attrs, "Beat", "beat", "BEAT"),
                _get_attr(attrs, "District", "district", "DISTRICT"),
                _ts_to_iso(
                    _get_attr(
                        attrs, "Date", "eventdate", "EventDate",
                        "EVENTDATE", "event_date",
                    )
                ),
                geom.get("y") if geom else None,
                geom.get("x") if geom else None,
            ),
        )
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM shotspotter").fetchone()[0]
    conn.close()
    _log_sync(db_path, "peoria_pd_arcgis", "shotspotter", count)
    logger.info("Synced %d shotspotter records", count)
    return count


def sync_boundaries(db_path: Path) -> int:
    """Fetch beat, district, and community policing boundary layers."""
    boundary_layers = {
        "beats": ENDPOINTS["beats"],
        "districts": ENDPOINTS["districts"],
        "community_policing": ENDPOINTS["community_policing"],
    }
    conn = get_connection(db_path)
    total = 0
    for boundary_type, url in boundary_layers.items():
        features = fetch_all_records(url)
        for feat in features:
            attrs = feat.get("attributes", {})
            geom = feat.get("geometry", {})
            name = _get_attr(
                attrs, "beat", "district", "name", "Name", "NAME",
                "BEAT", "DISTRICT", "AREA",
            )
            if name is None:
                name = str(attrs.get("OBJECTID", "unknown"))
            geometry_geojson = json.dumps(geom) if geom else None
            conn.execute(
                """INSERT OR REPLACE INTO boundaries (
                    boundary_type, name, geometry_geojson
                ) VALUES (?,?,?)""",
                (boundary_type, name, geometry_geojson),
            )
        total += len(features)
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM boundaries").fetchone()[0]
    conn.close()
    _log_sync(db_path, "peoria_pd_arcgis", "boundaries", count)
    logger.info("Synced %d boundary records", count)
    return count


def _log_sync(db_path: Path, source: str, table: str, count: int) -> None:
    """Insert a record into the sync_log table."""
    conn = get_connection(db_path)
    conn.execute(
        """INSERT INTO sync_log (source, table_name, records_fetched, completed_at, status)
           VALUES (?, ?, ?, datetime('now'), 'completed')""",
        (source, table, count),
    )
    conn.commit()
    conn.close()


def run_full_sync(db_path: Path | None = None) -> dict:
    """Run a full sync of all data sources."""
    if db_path is None:
        from src.config import DB_PATH

        db_path = DB_PATH
    init_db(db_path)
    counts = {
        "crimes": sync_crimes(db_path),
        "calls_for_service": sync_calls_for_service(db_path),
        "shotspotter": sync_shotspotter(db_path),
        "boundaries": sync_boundaries(db_path),
    }
    logger.info("Full sync complete: %s", counts)
    return counts
