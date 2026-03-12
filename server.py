import logging

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from src.config import DB_PATH
from src.database import init_db
from src.api.routes_meta import router as meta_router
from src.api.routes_dashboard import router as dashboard_router
from src.api.routes_streets import router as streets_router
from src.api.routes_explore import router as explore_router
from src.api.routes_trends import router as trends_router
from src.api.routes_sync import router as sync_router

logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Peoria Crime Tracker")


@app.on_event("startup")
def startup():
    init_db(DB_PATH)


app.include_router(meta_router, prefix="/api/meta", tags=["meta"])
app.include_router(dashboard_router, prefix="/api/dashboard", tags=["dashboard"])
app.include_router(streets_router, prefix="/api/streets", tags=["streets"])
app.include_router(explore_router, prefix="/api/explore", tags=["explore"])
app.include_router(trends_router, prefix="/api/trends", tags=["trends"])
app.include_router(sync_router, prefix="/api/sync", tags=["sync"])

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def index():
    return FileResponse("static/index.html")
