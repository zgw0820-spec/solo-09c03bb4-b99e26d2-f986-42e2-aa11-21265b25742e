"""Request/response schemas surfaced in the OpenAPI (Swagger) document."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    sql: str = Field(
        ...,
        description=(
            "单条只读 SELECT 语句（允许 WITH/VALUES/TABLE，"
            "禁止多语句、写操作、SELECT INTO 等）"
        ),
        examples=["SELECT id, device_id, severity FROM fault_logs LIMIT 5"],
    )
    timeout_ms: int = Field(
        ...,
        ge=1,
        le=60_000,
        description=(
            "总截止时间（毫秒），从请求进入起计时，包含排队等连接的时间"
        ),
    )


class QueryResponse(BaseModel):
    status: Literal["ok", "timeout"]
    executed: bool = Field(
        description="语句是否真正发送到了数据库后端"
    )
    wait_ms: float | None = None
    execution_ms: float | None = None
    row_count: int | None = None
    rows: list[list[Any]] | None = None
    cancelled: bool | None = None
    connection_discarded: bool | None = None
    late_result_ignored: bool | None = None
    detail: str | None = None


class ErrorResponse(BaseModel):
    error: dict[str, str]


class HealthResponse(BaseModel):
    status: Literal["healthy"]
    pool: dict[str, Any]
