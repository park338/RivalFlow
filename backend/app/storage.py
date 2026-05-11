from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Callable
from uuid import uuid4

from .models import PipelineNode, TaskCreateRequest, TaskDetail, TaskEvent


PIPELINE_NODES: list[tuple[str, str]] = [
    ("planner", "Planner 任务拆解"),
    ("collector", "Collector 信息采集"),
    ("structurer", "Structurer 数据结构化"),
    ("analyst", "Analyst AI 分析"),
    ("reviewer", "Reviewer 交叉审查"),
    ("reporter", "Reporter 报告生成"),
]


class TaskStore:
    def __init__(self) -> None:
        self._tasks: dict[str, TaskDetail] = {}
        self._lock = asyncio.Lock()

    async def create_task(self, payload: TaskCreateRequest) -> TaskDetail:
        now = datetime.utcnow()
        task_id = uuid4().hex[:10]
        task = TaskDetail(
            task_id=task_id,
            status="pending",
            created_at=now,
            updated_at=now,
            input=payload,
            nodes=[PipelineNode(key=key, label=label) for key, label in PIPELINE_NODES],
        )
        task.events.append(
            TaskEvent(
                at=now,
                level="info",
                stage="task_created",
                message="任务已创建，等待执行",
                context={
                    "project_name": payload.project_name,
                    "industry": payload.industry,
                    "competitors": payload.competitors,
                    "focus_areas": payload.focus_areas,
                    "time_range": payload.time_range,
                },
            )
        )
        async with self._lock:
            self._tasks[task_id] = task
        return task.model_copy(deep=True)

    async def get_task(self, task_id: str) -> TaskDetail | None:
        async with self._lock:
            task = self._tasks.get(task_id)
            return task.model_copy(deep=True) if task else None

    async def mutate_task(
        self,
        task_id: str,
        mutator: Callable[[TaskDetail], None],
    ) -> TaskDetail | None:
        async with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return None
            mutator(task)
            task.updated_at = datetime.utcnow()
            return task.model_copy(deep=True)
