"""FastAPI read-only dashboard for crawler results.

Run:
    cd D:/src/WebSpider/py-processor
    uvicorn dashboard.app:app --host 0.0.0.0 --port 8000

Auth (optional): set DASH_TOKEN to require `Authorization: Bearer <token>` on
all /api/* routes; unset (default) leaves the dashboard open.
"""
import os
import sys

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from queries import article_by_id, articles, daily, health, lang_distribution, pipeline, source_distribution, summary
from dashboard import rag_search

app = FastAPI(title="Crawler Dashboard", version="1.1.0")

_DASH_TOKEN = os.getenv("DASH_TOKEN", "")


@app.middleware("http")
async def auth_guard(request, call_next):
    """Optional bearer guard for /api/*. The static page itself stays open so
    the frontend can prompt for the token and store it."""
    if _DASH_TOKEN and request.url.path.startswith("/api/"):
        if request.headers.get("authorization", "") != f"Bearer {_DASH_TOKEN}":
            return JSONResponse({"detail": "unauthorized"}, status_code=401)
    return await call_next(request)


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


@app.get("/api/search")
def api_search(
    q: str = Query(..., min_length=2),
    k: int = Query(8, ge=1, le=32),
    source: str | None = Query(None),
    min_quality: float | None = Query(None, ge=0, le=1),
    since: str | None = Query(None),
):
    """Semantic Top-K retrieval over the pgvector corpus. 429 when the query
    rate exceeds RAG_QPS, 503 when the embedding backend is not configured."""
    try:
        return rag_search.semantic_search(q, k=k, source=source,
                                          min_quality=min_quality, since=since)
    except rag_search.RateLimited as exc:
        return JSONResponse(
            {"detail": str(exc)},
            status_code=429,
            headers={"Retry-After": str(int(exc.retry_after) + 1)},
        )
    except rag_search.SearchUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/articles/{article_id}")
def api_article_detail(article_id: int):
    try:
        article = article_by_id(article_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if article is None:
        raise HTTPException(status_code=404, detail="article not found")
    return article


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
