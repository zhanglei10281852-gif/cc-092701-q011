from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class TemplateCreate(BaseModel):
    code: str = Field(min_length=2, max_length=64, pattern=r"^[a-z0-9][a-z0-9._-]+$")
    name: str = Field(min_length=2, max_length=120)
    algorithm: str = Field(min_length=2, max_length=120)
    parameter_schema: dict[str, dict[str, Any]]
    default_parameters: dict[str, Any] = Field(default_factory=dict)
    max_runtime_seconds: int = Field(default=600, ge=1, le=86400)
    max_attempts: int = Field(default=3, ge=1, le=20)


class QuotaSet(BaseModel):
    subject_type: Literal["user", "role", "project"]
    subject_key: str = Field(min_length=1, max_length=120)
    max_queued: int = Field(default=20, ge=0, le=100000)
    max_running: int = Field(default=4, ge=0, le=10000)
    daily_submissions: int = Field(default=200, ge=0, le=1000000)


class TaskSubmit(BaseModel):
    template_code: str = Field(min_length=2, max_length=64)
    project_code: str = Field(min_length=1, max_length=80)
    requested_by: str = Field(min_length=1, max_length=80)
    parameters: dict[str, Any]
    priority: int = Field(default=50, ge=0, le=100)
    idempotency_key: str = Field(min_length=6, max_length=160)


class TaskClaim(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    capabilities: list[str] = Field(default_factory=list, max_length=100)
    lease_seconds: int = Field(default=60, ge=5, le=3600)


class TaskResult(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    result: dict[str, Any]
    metrics: dict[str, Any] = Field(default_factory=dict)


class TaskFailure(BaseModel):
    worker_id: str = Field(min_length=1, max_length=120)
    error_code: str = Field(min_length=1, max_length=120)
    message: str = Field(min_length=1, max_length=2000)
    retryable: bool = True


class CancelRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)


class RetryRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)
    priority: int | None = Field(default=None, ge=0, le=100)


class PriorityRequest(BaseModel):
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)
    priority: int = Field(ge=0, le=100)


class BatchOperation(BaseModel):
    task_ids: list[int] = Field(min_length=1, max_length=200)
    operation: Literal["cancel", "retry", "priority"]
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)
    priority: int | None = Field(default=None, ge=0, le=100)

    @model_validator(mode="after")
    def validate_priority(self) -> "BatchOperation":
        if self.operation == "priority" and self.priority is None:
            raise ValueError("批量调整优先级时必须提供 priority")
        return self


class BatchProtocolFilter(BaseModel):
    """预览时固定下来的任务筛选条件。"""

    status: str | None = Field(default=None, min_length=1, max_length=40)
    project_code: str | None = Field(default=None, min_length=1, max_length=80)
    requested_by: str | None = Field(default=None, min_length=1, max_length=80)
    limit: int = Field(default=100, ge=1, le=200)


class BatchProtocolPreviewRequest(BaseModel):
    operation: Literal["cancel", "retry", "priority"]
    actor: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=2, max_length=1000)
    priority: int | None = Field(default=None, ge=0, le=100)
    # 显式任务列表与筛选条件二选一；命中的任务集合在预览时被冻结。
    task_ids: list[int] | None = Field(default=None, min_length=1, max_length=200)
    filter: BatchProtocolFilter | None = None
    # abort：全部允许项都成功才提交；accept_partial：明确接受逐条结果。
    execution_mode: Literal["abort", "accept_partial"] = "abort"
    ttl_seconds: int = Field(default=600, ge=30, le=3600)

    @model_validator(mode="after")
    def validate_request(self) -> "BatchProtocolPreviewRequest":
        if self.operation == "priority" and self.priority is None:
            raise ValueError("批量调整优先级时必须提供 priority")
        if (self.task_ids is None) == (self.filter is None):
            raise ValueError("task_ids 与 filter 必须且只能提供一个")
        return self


class BatchProtocolConfirmRequest(BaseModel):
    token: str = Field(min_length=32, max_length=128)
    preview_digest: str = Field(min_length=8, max_length=128)
    actor: str = Field(min_length=1, max_length=120)
