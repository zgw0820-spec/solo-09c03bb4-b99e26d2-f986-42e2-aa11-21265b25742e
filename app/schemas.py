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
    pagination: dict[str, Any] | None = None


class PagedOpenRequest(BaseModel):
    """Open a snapshot pagination session and fetch its first page."""

    sql: str = Field(
        ...,
        description=(
            "单条只读 SELECT 语句；会话期间所有页都来自开启时的同一数据库快照"
        ),
        examples=["SELECT id, device_id, severity, occurred_at FROM fault_logs ORDER BY id"],
    )
    page_size: int = Field(
        ...,
        ge=1,
        le=1000,
        description="每页行数（服务端游标逐页 FETCH，不会一次加载全部结果）",
    )
    timeout_ms: int = Field(
        ...,
        ge=1,
        le=60_000,
        description="本次请求总截止时间（毫秒），排队等连接的时间计入预算",
    )


class PagedNextRequest(BaseModel):
    page_token: str = Field(
        ...,
        min_length=8,
        max_length=128,
        description="上一页响应返回的取页令牌；会话结束时令牌失效",
    )
    timeout_ms: int = Field(
        ...,
        ge=1,
        le=60_000,
        description="本次取页的总截止时间（毫秒），排队时间计入预算",
    )


class PagedCloseRequest(BaseModel):
    page_token: str = Field(..., min_length=8, max_length=128)


class PagedPageResponse(BaseModel):
    status: Literal["ok"]
    has_more: bool = Field(description="是否还有后续页；末页时会话随即关闭")
    page_token: str | None = Field(
        default=None, description="取下一页所需的令牌；末页时为 null"
    )
    row_count: int
    rows: list[list[Any]]
    wait_ms: float = Field(description="本次请求排队耗时（计入总期限）")
    execution_ms: float
    page_number: int


class PagedCloseResponse(BaseModel):
    status: Literal["closed"]
    detail: str | None = None
