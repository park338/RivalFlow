from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


TaskStatus = Literal["pending", "running", "completed", "failed"]
NodeStatus = Literal["pending", "running", "completed", "failed"]
EventLevel = Literal["info", "warning", "error"]


DEFAULT_FOCUS_AREAS = [
    "产品定位",
    "用户体验",
    "商业化能力",
    "增长策略",
]


class TaskCreateRequest(BaseModel):
    project_name: str = Field(
        default="2026 短视频电商竞品分析（抖音 vs 快手 vs 小红书）",
        min_length=1,
        max_length=100,
    )
    industry: str = Field(..., min_length=1, max_length=100)
    competitors: list[str] = Field(..., min_length=1)
    focus_areas: list[str] = Field(default_factory=lambda: DEFAULT_FOCUS_AREAS.copy())
    source_urls: list[str] = Field(default_factory=list)
    time_range: str = Field(default="近 12 个月", max_length=50)

    @field_validator("competitors", mode="before")
    @classmethod
    def normalize_competitors(cls, value: list[str] | str) -> list[str]:
        raw_items: list[str]
        if isinstance(value, str):
            raw_items = [item.strip() for item in value.replace("，", ",").split(",")]
        else:
            raw_items = [str(item).strip() for item in value]
        cleaned = [item for item in raw_items if item]
        unique = list(dict.fromkeys(cleaned))
        if not unique:
            raise ValueError("competitors 不能为空")
        return unique

    @field_validator("focus_areas", "source_urls", mode="before")
    @classmethod
    def normalize_optional_list(cls, value: list[str] | str | None) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            raw_items = [item.strip() for item in value.replace("，", ",").split(",")]
        else:
            raw_items = [str(item).strip() for item in value]
        unique = list(dict.fromkeys([item for item in raw_items if item]))
        return unique


class PipelineNode(BaseModel):
    key: str
    label: str
    status: NodeStatus = "pending"
    summary: str = ""
    started_at: datetime | None = None
    finished_at: datetime | None = None
    context: dict[str, Any] = Field(default_factory=dict)


class TaskEvent(BaseModel):
    at: datetime
    level: EventLevel = "info"
    message: str
    node_key: str | None = None
    stage: str = "progress"
    context: dict[str, Any] = Field(default_factory=dict)


class EvidenceItem(BaseModel):
    evidence_id: str
    competitor: str
    focus_area: str
    source_name: str
    source_url: str
    snippet: str
    confidence: float = Field(ge=0, le=1)


class ClaimItem(BaseModel):
    claim_id: str
    title: str
    detail: str
    confidence: float = Field(ge=0, le=1)
    evidence_ids: list[str] = Field(default_factory=list)


class TaskResult(BaseModel):
    plan: list[str] = Field(default_factory=list)
    scorecard: dict[str, dict[str, int]] = Field(default_factory=dict)
    claims: list[ClaimItem] = Field(default_factory=list)
    reviewer_notes: list[str] = Field(default_factory=list)
    recommendations: list[str] = Field(default_factory=list)
    model_info: dict[str, str] = Field(default_factory=dict)
    markdown_report: str = ""


class TaskDetail(BaseModel):
    task_id: str
    status: TaskStatus
    created_at: datetime
    updated_at: datetime
    error_message: str | None = None
    input: TaskCreateRequest
    nodes: list[PipelineNode]
    events: list[TaskEvent] = Field(default_factory=list)
    evidence: list[EvidenceItem] = Field(default_factory=list)
    result: TaskResult = Field(default_factory=TaskResult)
