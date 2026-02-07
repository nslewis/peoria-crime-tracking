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
    init_db(db_path)
    conn = get_connection(db_path)
    expected_tables = {"crimes", "calls_for_service", "shotspotter", "boundaries", "sync_log"}
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    tables = {row[0] for row in cursor.fetchall()}
    assert tables == expected_tables
    conn.close()
