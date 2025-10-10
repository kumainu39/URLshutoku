from __future__ import annotations

import asyncio
import sys
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import FileResponse, HTMLResponse, Response, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

from . import db
from .config import Settings, get_settings
from .services.pipeline import CrawlPipeline, JobConfig, JobManager, JobState
from .prefectures import REGION_GROUPS


# Configure app
app = FastAPI(title="URL取得支援ツール")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")


# Ensure Loguru outputs UTF-8 on Windows consoles
logger.remove()
try:
    sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass
logger.add(sys.stderr)


# Managers
job_manager = JobManager()
pipeline = CrawlPipeline()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    settings: Settings = get_settings()
    available = set(db.fetch_prefectures(pipeline.engine))
    prefecture_groups = []
    for region, prefs in REGION_GROUPS:
        group_prefs = [p for p in prefs if not available or p in available]
        if group_prefs:
            prefecture_groups.append((region, group_prefs))
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "jobs": job_manager.jobs,
            "settings": settings,
            "prefecture_groups": prefecture_groups,
        },
    )


@app.post("/jobs", response_class=HTMLResponse)
async def create_job(
    request: Request,
    prefecture: Optional[str] = Form(default=None),
    limit: Optional[int] = Form(default=None),
    chunk_size: int = Form(default=100),
    concurrency: int = Form(default=5),
    skip_existing: Optional[str] = Form(default=None),
) -> HTMLResponse:
    job_id = str(uuid.uuid4())
    chunk_size = max(1, chunk_size)
    concurrency = max(1, concurrency)
    config = JobConfig(
        prefecture=prefecture or None,
        limit=limit,
        chunk_size=chunk_size,
        concurrency=concurrency,
        skip_existing=bool(skip_existing),
    )
    settings = get_settings()
    settings.concurrency_limit = concurrency  # type: ignore[attr-defined]
    state = JobState(job_id=job_id, config=config)
    await job_manager.create_job(state)

    async def _run_job() -> None:
        logger.info("ジョブ開始 {job_id}", job_id=job_id)
        await pipeline.run(state, on_update=lambda _: None)
        logger.info("ジョブ終了 {job_id}", job_id=job_id)

    asyncio.create_task(_run_job())

    return templates.TemplateResponse(
        "job.html",
        {"request": request, "job": state},
    )


@app.get("/jobs/{job_id}", response_class=HTMLResponse)
async def job_detail(request: Request, job_id: str) -> HTMLResponse:
    job = job_manager.get(job_id)
    if job is None:
        return HTMLResponse(content="ジョブが見つかりません", status_code=404)
    return templates.TemplateResponse("job.html", {"request": request, "job": job})


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    svg_path = Path(__file__).parent / "static" / "favicon.svg"
    if svg_path.exists():
        return FileResponse(svg_path, media_type="image/svg+xml")
    return Response(status_code=204)


@app.get("/stats", include_in_schema=False)
def stats(prefecture: Optional[str] = None) -> JSONResponse:
    missing, total = db.count_missing_by_prefecture(pipeline.engine, prefecture)
    return JSONResponse({"prefecture": prefecture, "missing": missing, "total": total})
