from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .ai_client import DeepSeekClient
from .models import TaskCreateRequest, TaskDetail
from .pipeline import PipelineRunner
from .storage import TaskStore


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(
    title="RivalFlow Demo",
    description="AI 驱动竞品分析 Agent 协作系统 Demo",
    version="0.2.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

store = TaskStore()
deepseek_client = DeepSeekClient()
runner = PipelineRunner(store=store, ai_client=deepseek_client)


@app.get("/api/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/tasks", response_model=TaskDetail)
async def create_task(payload: TaskCreateRequest) -> TaskDetail:
    task = await store.create_task(payload)
    asyncio.create_task(runner.run(task.task_id))
    return task


@app.get("/api/tasks/{task_id}", response_model=TaskDetail)
async def get_task(task_id: str) -> TaskDetail:
    task = await store.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")
    return task


@app.get("/")
async def index() -> FileResponse:
    index_path = STATIC_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="前端页面不存在")
    return FileResponse(index_path)


if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
