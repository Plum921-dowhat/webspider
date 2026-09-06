"""FastAPI read-only dashboard for crawler results.

Run:
    cd D:/src/WebSpider/py-processor
    uvicorn dashboard.app:app --host 0.0.0.0 --port 8000
"""
import os
import sys

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from queries import articles, daily, health, lang_distribution, pipeline, source_distribution, summary

app = FastAPI(title="Crawler Dashboard", version="1.0.0")


@app.get("/")
def index():
    return FileResponse(os.path.join(_HERE, "index.html"))


@app.get("/api/summary")
def api_summary():
    try:
        return summary()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/lang-dist")
def api_lang_dist():
    try:
        return lang_distribution()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/daily")
def api_daily(limit: int = Query(30, ge=1, le=365)):
    try:
        return daily(limit)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/pipeline")
def api_pipeline():
    """Crawler pipeline health: produced/stored/rejected counters, DLQ and
    stream lengths, pending messages. 502 when Redis is unreachable so the
    frontend can show the panel as degraded instead of silently stale."""
    try:
        return pipeline()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"redis unavailable: {exc}") from exc


@app.get("/api/source-dist")
def api_source_dist():
    try:
        return source_distribution()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/health")
def api_health(hours: int = Query(24, ge=1, le=168)):
    """Crawler activity monitoring: per-source run heartbeats + snapshot series."""
    try:
        return health(hours)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/articles")
def api_articles(
    lang: str | None = Query(None),
    q: str | None = Query(None),
    source: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    try:
        return articles(lang=lang, q=q, source=source, page=page, page_size=page_size)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
