from pathlib import Path

from src.config import DB_PATH


def get_db_path() -> Path:
    return DB_PATH
