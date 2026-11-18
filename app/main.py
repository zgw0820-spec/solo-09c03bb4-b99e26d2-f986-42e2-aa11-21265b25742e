"""FastAPI application: a two-connection read-only fault-log query API.

Run locally::

    uvicorn app.main:app --host 0.0.0.0 --port 8000

Swagger UI is served at ``/docs``, ReDoc at ``/redoc``.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .config import settings
from .errors import InvalidDeadlineError, QueryServiceError
from .executor import QueryExecutor
from .paging import ClientAborted, PaginationManager
from .pool import QueryConnectionPool
from .schemas import (
    HealthResponse,
    PagedCloseRequest,
    PagedCloseResponse,
    PagedNextRequest,
    PagedOpenRequest,
    PagedPageResponse,
    QueryRequest,
    QueryResponse,
)
from .validation import validate_single_select

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("api")

pool = QueryConnectionPool(settings)
executor = QueryExecutor(pool, settings)
paging = PaginationManager(pool, settings)


async def _reaper_loop() -> None:
    """Background sweeper: release paging sessions idle past their limit."""
    while True:
        await asyncio.sleep(settings.paged_reaper_interval_ms / 1000.0)
        try:
            await paging.reap_idle()
        except Exception:
            logger.exception("paged-session reaper failed")


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Eagerly open both connections at startup; close them on shutdown.
    await pool.open()
    reaper = asyncio.create_task(_reaper_loop())
    try:
        yield
    finally:
        reaper.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await reaper
        # End any lingering paging session before draining the pool.
        await paging.shutdown()
        await pool.close()


app = FastAPI(
    title="机房设备故障日志只读查询 API",
    description=(
        "值班分析员共用的只读 SQL 查询入口。固定两条 PostgreSQL 连接，"
        "提交单条 SELECT 与总截止时间；排队时间计入预算，超时立即取消语句，"
        "确认事务结束才复用连接，无法确认则弃置并补建。"
        "另提供分页快照接口：同一数据库快照逐页取数，新入库数据不会混入，"
        "同一时刻仅一个分页会话，末页或闲置 30 秒自动释放。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


@app.exception_handler(QueryServiceError)
async def _service_error_handler(_: Request, exc: QueryServiceError):
    return JSONResponse(
        status_code=exc.http_status,
        content={
            "error": {"code": exc.code, "message": str(exc)},
            **exc.extra,
        },
    )


@app.exception_handler(Exception)
async def _unexpected_handler(_: Request, exc: Exception):
    logger.exception("unhandled error")
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "internal_error",
                "message": f"{type(exc).__name__}: {exc}",
            }
        },
    )


@app.get("/health", response_model=HealthResponse, tags=["运维"])
async def health() -> HealthResponse:
    """连接池健康状态：容量、空闲/占用数、累计创建与弃置数、分页会话状态。"""
    status = await pool.status()
    return HealthResponse(
        status="healthy",
        pool={
            "size": status.size,
            "idle": status.idle,
            "in_use": status.in_use,
            "total_created": status.total_created,
            "total_discarded": status.total_discarded,
            "closed": status.closed,
        },
        pagination=paging.status(),
    )


@app.post(
    "/query",
    response_model=QueryResponse,
    tags=["查询"],
    responses={
        400: {"description": "SQL 不符合单条只读 SELECT 约束"},
        422: {"description": "请求体或截止时间不合法"},
        503: {"description": "排队超时 / 连接被弃置"},
        504: {"description": "语句执行超过总截止时间"},
    },
)
async def query(body: QueryRequest) -> QueryResponse:
    """执行一条只读 SELECT，``timeout_ms`` 是包含排队时间的总预算。"""
    if body.timeout_ms <= 0:
        raise InvalidDeadlineError("timeout_ms 必须为正整数")

    sql = validate_single_select(body.sql, settings.max_sql_chars)

    # Absolute monotonic deadline: waiting + execution share one budget.
    deadline = time.monotonic() + body.timeout_ms / 1000.0
    # A deadline that expires while queued surfaces as a 503
    # acquire_timeout; an execution deadline as a 504 query_timeout.
    result = await executor.execute(sql, deadline)
    return QueryResponse(**result)


# --------------------------------------------------------------------- #
# Snapshot pagination: one session at a time, same snapshot for all pages
# --------------------------------------------------------------------- #
def _disconnect_watch(request: Request) -> tuple[asyncio.Event, asyncio.Task]:
    """Arm a poll on the ASGI disconnect channel for the running request.

    When the client goes away mid-request the event is set and the paging
    layer runs the same cancel-and-terminate protocol as for a timeout.
    """
    aborted = asyncio.Event()

    async def _poll() -> None:
        try:
            while not aborted.is_set():
                if await request.is_disconnected():
                    aborted.set()
                    return
                await asyncio.sleep(0.05)
        except Exception:  # pragma: no cover - best-effort watcher
            return

    return aborted, asyncio.create_task(_poll())


async def _stop_watch(watcher: asyncio.Task) -> None:
    watcher.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await watcher


@app.post(
    "/paged/query",
    response_model=PagedPageResponse,
    tags=["分页快照"],
    responses={
        400: {"description": "SQL 不符合单条只读 SELECT 约束"},
        409: {"description": "已有进行中的分页会话（同一时刻仅允许一个）"},
        422: {"description": "请求体或截止时间不合法"},
        503: {"description": "排队等连接超过总截止时间，语句未执行"},
        504: {"description": "开启会话/取首页超过总截止时间"},
    },
)
async def paged_open(body: PagedOpenRequest, request: Request):
    """开启分页快照会话并取回首页。

    在 ``REPEATABLE READ READ ONLY`` 事务中声明服务端游标：后续各页沿用
    同一数据库快照，新入库日志不会混入；响应附取页令牌。若首页即末页，
    会话不保留、不发放令牌。
    """
    if body.timeout_ms <= 0:
        raise InvalidDeadlineError("timeout_ms 必须为正整数")

    sql = validate_single_select(body.sql, settings.max_sql_chars)
    deadline = time.monotonic() + body.timeout_ms / 1000.0
    aborted, watcher = _disconnect_watch(request)
    try:
        page = await paging.open(sql, body.page_size, deadline, aborted)
    except ClientAborted:
        return JSONResponse(
            status_code=499,
            content={
                "error": {
                    "code": "client_disconnected",
                    "message": "客户端在请求完成前断开连接，分页会话未建立",
                }
            },
        )
    finally:
        await _stop_watch(watcher)
    return PagedPageResponse(**page)


@app.post(
    "/paged/next",
    response_model=PagedPageResponse,
    tags=["分页快照"],
    responses={
        410: {"description": "取页令牌无效或会话已结束（末页/超时/闲置/断连）"},
        422: {"description": "请求体或截止时间不合法"},
        503: {"description": "等待会话占用锁超过总截止时间，语句未执行"},
        504: {"description": "取页超过总截止时间，会话已终止"},
    },
)
async def paged_next(body: PagedNextRequest, request: Request):
    """凭令牌取下一页；同一令牌的并发请求被串行化，不会重复推进。

    返回行数少于页大小即为末页：事务与连接立即释放，令牌失效。
    """
    if body.timeout_ms <= 0:
        raise InvalidDeadlineError("timeout_ms 必须为正整数")

    deadline = time.monotonic() + body.timeout_ms / 1000.0
    aborted, watcher = _disconnect_watch(request)
    try:
        page = await paging.fetch_next(body.page_token, deadline, aborted)
    except ClientAborted:
        return JSONResponse(
            status_code=499,
            content={
                "error": {
                    "code": "client_disconnected",
                    "message": "客户端在取页完成前断开连接，分页会话已终止",
                }
            },
        )
    finally:
        await _stop_watch(watcher)
    return PagedPageResponse(**page)


@app.post(
    "/paged/close",
    response_model=PagedCloseResponse,
    tags=["分页快照"],
    responses={
        410: {"description": "取页令牌无效或会话已结束"},
    },
)
async def paged_close(body: PagedCloseRequest):
    """提前关闭分页会话：回滚事务、确认连接空闲后归还连接池。"""
    await paging.close(body.page_token)
    return PagedCloseResponse(
        status="closed", detail="分页会话已关闭，事务与连接已释放"
    )
