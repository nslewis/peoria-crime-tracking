import json
from unittest.mock import patch, MagicMock

import pytest

from src.database import init_db, get_connection
from src.sync import (
    fetch_arcgis_page,
    fetch_all_records,
    _ts_to_iso,
    sync_crimes,
    sync_calls_for_service,
    sync_shotspotter,
    sync_boundaries,
    run_full_sync,
    _log_sync,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "test_sync.db"
    init_db(path)
    return path


# ---------------------------------------------------------------------------
# Helpers to build fake ArcGIS feature dicts
# ---------------------------------------------------------------------------

def _make_crime_feature(offense_id="OFF-001", call_id="CALL-001", lat=40.7, lon=-89.6):
    return {
        "attributes": {
            "offenseid": offense_id,
            "callid": call_id,
            "statute": "720-5/12-3.05(a)(1)",
            "nibrscode": "13A",
            "nibrsoffense": "Assault Offenses",
            "nibrsdesc": "Aggravated Assault",
            "nibrscrimeag": "Person",
            "attemptcompleted": "Completed",
            "fulladdr": "123 MAIN ST",
            "city": "PEORIA",
            "state": "IL",
            "zip5": "61602",
            "beat": "1A",
            "district": "1",
            "neighborhood": "Downtown",
            "weaponcat": "Firearm",
            "weapondesc": "Handgun",
            "reportdate": 1700000000000,  # 2023-11-14T22:13:20+00:00
            "reportyear": 2023,
            "reportmonth": 11,
            "reporthour": 22,
            "reportdow": "Tuesday",
        },
        "geometry": {"x": lon, "y": lat},
    }


def _make_call_feature(call_id="CFS-001"):
    return {
        "attributes": {
            "callid": call_id,
            "calltype": "DISTURBANCE",
            "priority": "P1",
            "disposition": "REPORT",
            "fulladdr": "456 ELM ST",
            "beat": "2B",
            "district": "2",
            "calldate": 1700000000000,
        },
        "geometry": {"x": -89.5, "y": 40.6},
    }


def _make_shotspotter_feature(incident_id="SS-001"):
    return {
        "attributes": {
            "incidentid": incident_id,
            "roundsfired": 5,
            "eventtype": "Multiple Gunshots",
            "fulladdr": "789 OAK AVE",
            "beat": "3C",
            "district": "3",
            "eventdate": 1700000000000,
        },
        "geometry": {"x": -89.55, "y": 40.65},
    }


def _make_boundary_feature(name="Beat 1A"):
    return {
        "attributes": {"name": name},
        "geometry": {
            "rings": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
        },
    }


# ---------------------------------------------------------------------------
# Tests for _ts_to_iso
# ---------------------------------------------------------------------------

class TestTsToIso:
    def test_converts_millisecond_timestamp(self):
        result = _ts_to_iso(1700000000000)
        assert result is not None
        assert "2023" in result

    def test_returns_none_for_none(self):
        assert _ts_to_iso(None) is None

    def test_zero_timestamp(self):
        result = _ts_to_iso(0)
        assert result is not None
        assert "1970" in result


# ---------------------------------------------------------------------------
# Tests for fetch_arcgis_page
# ---------------------------------------------------------------------------

class TestFetchArcgisPage:
    @patch("src.sync.requests.get")
    def test_returns_features_and_has_more_true(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "features": [{"attributes": {"id": 1}}],
            "exceededTransferLimit": True,
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        features, has_more = fetch_arcgis_page("http://example.com/layer", offset=0)

        assert len(features) == 1
        assert has_more is True
        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args
        assert call_kwargs[1]["params"]["outFields"] == "*"
        assert call_kwargs[1]["params"]["f"] == "json"
        assert call_kwargs[1]["params"]["outSR"] == 4326

    @patch("src.sync.requests.get")
    def test_returns_features_and_has_more_false(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "features": [{"attributes": {"id": 1}}],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        features, has_more = fetch_arcgis_page("http://example.com/layer", offset=0)

        assert len(features) == 1
        assert has_more is False

    @patch("src.sync.requests.get")
    def test_empty_response(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"features": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        features, has_more = fetch_arcgis_page("http://example.com/layer", offset=0)

        assert features == []
        assert has_more is False

    @patch("src.sync.requests.get")
    def test_passes_where_clause(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"features": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        fetch_arcgis_page("http://example.com/layer", offset=0, where="status='open'")

        call_kwargs = mock_get.call_args
        assert call_kwargs[1]["params"]["where"] == "status='open'"


# ---------------------------------------------------------------------------
# Tests for fetch_all_records
# ---------------------------------------------------------------------------

class TestFetchAllRecords:
    @patch("src.sync.fetch_arcgis_page")
    def test_single_page(self, mock_page):
        mock_page.return_value = ([{"id": 1}], False)
        result = fetch_all_records("http://example.com/layer")
        assert len(result) == 1
        assert mock_page.call_count == 1

    @patch("src.sync.fetch_arcgis_page")
    def test_multiple_pages(self, mock_page):
        mock_page.side_effect = [
            ([{"id": 1}, {"id": 2}], True),
            ([{"id": 3}], False),
        ]
        result = fetch_all_records("http://example.com/layer")
        assert len(result) == 3
        assert mock_page.call_count == 2


# ---------------------------------------------------------------------------
# Tests for sync_crimes
# ---------------------------------------------------------------------------

class TestSyncCrimes:
    @patch("src.sync.fetch_all_records")
    def test_inserts_records(self, mock_fetch, db_path):
        mock_fetch.return_value = [
            _make_crime_feature("OFF-001"),
            _make_crime_feature("OFF-002"),
        ]
        count = sync_crimes(db_path)
        assert count == 2

        conn = get_connection(db_path)
        rows = conn.execute("SELECT * FROM crimes").fetchall()
        assert len(rows) == 2
        assert rows[0]["offense_id"] == "OFF-001"
        assert rows[0]["address"] == "123 MAIN ST"
        assert rows[0]["latitude"] == 40.7
        assert rows[0]["report_year"] == 2023
        conn.close()

    @patch("src.sync.fetch_all_records")
    def test_skips_duplicates(self, mock_fetch, db_path):
        mock_fetch.return_value = [
            _make_crime_feature("OFF-001"),
            _make_crime_feature("OFF-001"),  # duplicate
        ]
        count = sync_crimes(db_path)
        assert count == 1

    @patch("src.sync.fetch_all_records")
    def test_report_date_converted(self, mock_fetch, db_path):
        mock_fetch.return_value = [_make_crime_feature("OFF-003")]
        sync_crimes(db_path)

        conn = get_connection(db_path)
        row = conn.execute("SELECT report_date FROM crimes").fetchone()
        assert row["report_date"] is not None
        assert "2023" in row["report_date"]
        conn.close()

    @patch("src.sync.fetch_all_records")
    def test_logs_sync(self, mock_fetch, db_path):
        mock_fetch.return_value = [_make_crime_feature("OFF-010")]
        sync_crimes(db_path)

        conn = get_connection(db_path)
        log = conn.execute(
            "SELECT * FROM sync_log WHERE table_name='crimes'"
        ).fetchone()
        assert log is not None
        assert log["records_fetched"] == 1
        assert log["status"] == "completed"
        conn.close()


# ---------------------------------------------------------------------------
# Tests for sync_calls_for_service
# ---------------------------------------------------------------------------

class TestSyncCallsForService:
    @patch("src.sync.fetch_all_records")
    def test_inserts_records(self, mock_fetch, db_path):
        mock_fetch.return_value = [_make_call_feature("CFS-001")]
        count = sync_calls_for_service(db_path)
        assert count == 1

        conn = get_connection(db_path)
        row = conn.execute("SELECT * FROM calls_for_service").fetchone()
        assert row["call_id"] == "CFS-001"
        assert row["call_type"] == "DISTURBANCE"
        conn.close()

    @patch("src.sync.fetch_all_records")
    def test_skips_duplicates(self, mock_fetch, db_path):
        mock_fetch.return_value = [
            _make_call_feature("CFS-001"),
            _make_call_feature("CFS-001"),
        ]
        count = sync_calls_for_service(db_path)
        assert count == 1


# ---------------------------------------------------------------------------
# Tests for sync_shotspotter
# ---------------------------------------------------------------------------

class TestSyncShotspotter:
    @patch("src.sync.fetch_all_records")
    def test_inserts_records(self, mock_fetch, db_path):
        mock_fetch.return_value = [_make_shotspotter_feature("SS-001")]
        count = sync_shotspotter(db_path)
        assert count == 1

        conn = get_connection(db_path)
        row = conn.execute("SELECT * FROM shotspotter").fetchone()
        assert row["incident_id"] == "SS-001"
        assert row["rounds_fired"] == 5
        conn.close()


# ---------------------------------------------------------------------------
# Tests for sync_boundaries
# ---------------------------------------------------------------------------

class TestSyncBoundaries:
    @patch("src.sync.fetch_all_records")
    def test_inserts_boundaries(self, mock_fetch, db_path):
        mock_fetch.return_value = [_make_boundary_feature("Beat 1A")]
        count = sync_boundaries(db_path)
        # Called 3 times (beats, districts, community_policing), 1 feature each
        assert count == 3

        conn = get_connection(db_path)
        rows = conn.execute("SELECT * FROM boundaries").fetchall()
        assert len(rows) == 3
        # Verify geometry stored as JSON string
        geom = json.loads(rows[0]["geometry_geojson"])
        assert "rings" in geom
        conn.close()


# ---------------------------------------------------------------------------
# Tests for _log_sync
# ---------------------------------------------------------------------------

class TestLogSync:
    def test_inserts_log_entry(self, db_path):
        _log_sync(db_path, "test_source", "test_table", 42)
        conn = get_connection(db_path)
        row = conn.execute("SELECT * FROM sync_log").fetchone()
        assert row["source"] == "test_source"
        assert row["table_name"] == "test_table"
        assert row["records_fetched"] == 42
        assert row["status"] == "completed"
        conn.close()


# ---------------------------------------------------------------------------
# Tests for run_full_sync
# ---------------------------------------------------------------------------

class TestRunFullSync:
    @patch("src.sync.sync_boundaries")
    @patch("src.sync.sync_shotspotter")
    @patch("src.sync.sync_calls_for_service")
    @patch("src.sync.sync_crimes")
    @patch("src.sync.init_db")
    def test_calls_all_sync_functions(
        self, mock_init, mock_crimes, mock_calls, mock_ss, mock_bounds
    ):
        mock_crimes.return_value = 100
        mock_calls.return_value = 50
        mock_ss.return_value = 10
        mock_bounds.return_value = 5

        result = run_full_sync(db_path="/tmp/test.db")

        mock_init.assert_called_once_with("/tmp/test.db")
        mock_crimes.assert_called_once_with("/tmp/test.db")
        mock_calls.assert_called_once_with("/tmp/test.db")
        mock_ss.assert_called_once_with("/tmp/test.db")
        mock_bounds.assert_called_once_with("/tmp/test.db")

        assert result == {
            "crimes": 100,
            "calls_for_service": 50,
            "shotspotter": 10,
            "boundaries": 5,
        }

    @patch("src.sync.sync_boundaries")
    @patch("src.sync.sync_shotspotter")
    @patch("src.sync.sync_calls_for_service")
    @patch("src.sync.sync_crimes")
    @patch("src.sync.init_db")
    def test_returns_count_dict(
        self, mock_init, mock_crimes, mock_calls, mock_ss, mock_bounds
    ):
        mock_crimes.return_value = 0
        mock_calls.return_value = 0
        mock_ss.return_value = 0
        mock_bounds.return_value = 0

        result = run_full_sync(db_path="/tmp/test2.db")

        assert isinstance(result, dict)
        assert set(result.keys()) == {
            "crimes",
            "calls_for_service",
            "shotspotter",
            "boundaries",
        }
