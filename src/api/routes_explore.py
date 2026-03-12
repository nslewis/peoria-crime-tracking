from fastapi import APIRouter, Depends, Query
from pathlib import Path

from src.api.deps import get_db_path
from src.database import get_connection

router = APIRouter()

ALLOWED_SOURCES = {"crimes", "calls_for_service", "shotspotter"}


@router.get("/{source}")
def explore(
    source: str,
    district: str | None = None,
    beat: str | None = None,
    limit: int = Query(default=200, le=2000),
    offset: int = 0,
    db_path: Path = Depends(get_db_path),
):
    if source not in ALLOWED_SOURCES:
        return {"error": f"Unknown source: {source}"}

    conn = get_connection(db_path)
    conditions: list[str] = []
    params: list = []
    if district:
        conditions.append("district = ?")
        params.append(district)
    if beat:
        conditions.append("beat = ?")
        params.append(beat)
    where = " WHERE " + " AND ".join(conditions) if conditions else ""

    total = conn.execute(f"SELECT COUNT(*) FROM {source}{where}", params).fetchone()[0]

    date_col = {"crimes": "report_date", "calls_for_service": "call_date", "shotspotter": "event_date"}[source]
    rows = conn.execute(
        f"SELECT * FROM {source}{where} ORDER BY {date_col} DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()
    conn.close()
    return {"total": total, "records": [dict(r) for r in rows]}
