from fastapi import APIRouter, Depends, Query
from pathlib import Path

from src.api.deps import get_db_path
from src.queries import search_streets, get_street_crime_summary

router = APIRouter()


@router.get("/search")
def search(q: str = Query(..., min_length=2), db_path: Path = Depends(get_db_path)):
    return search_streets(db_path, q)


@router.get("/detail")
def detail(address: str = Query(...), db_path: Path = Depends(get_db_path)):
    return get_street_crime_summary(db_path, address)
