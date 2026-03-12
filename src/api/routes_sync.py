import logging
from fastapi import APIRouter, Depends, BackgroundTasks
from pathlib import Path

from src.api.deps import get_db_path
from src.database import get_connection
from src.sync import run_full_sync, sync_crimes, sync_calls_for_service, sync_shotspotter, sync_boundaries

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/history")
def history(db_path: Path = Depends(get_db_path)):
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT * FROM sync_log ORDER BY started_at DESC LIMIT 20"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.post("/full")
def full_sync(background_tasks: BackgroundTasks, db_path: Path = Depends(get_db_path)):
    background_tasks.add_task(_run_full_sync, db_path)
    return {"status": "started", "message": "Full sync started in background"}


@router.post("/source")
def source_sync(
    source: str,
    background_tasks: BackgroundTasks,
    db_path: Path = Depends(get_db_path),
):
    sync_fns = {
        "crimes": sync_crimes,
        "calls_for_service": sync_calls_for_service,
        "shotspotter": sync_shotspotter,
        "boundaries": sync_boundaries,
    }
    fn = sync_fns.get(source)
    if not fn:
        return {"error": f"Unknown source: {source}"}
    background_tasks.add_task(fn, db_path)
    return {"status": "started", "source": source}


def _run_full_sync(db_path: Path):
    try:
        counts = run_full_sync(db_path)
        logger.info("Full sync completed: %s", counts)
    except Exception:
        logger.exception("Full sync failed")
