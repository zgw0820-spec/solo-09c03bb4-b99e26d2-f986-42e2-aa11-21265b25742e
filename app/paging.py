"""Single-session snapshot pagination on top of the two-connection pool.

A pagination session pins one pooled connection for its whole lifetime: the
validated ``SELECT`` runs inside a ``REPEATABLE READ READ ONLY`` transaction
as a **server-side cursor**, so every page comes from the *same* database
snapshot and rows inserted after the first page can never leak in.  Pages are
pulled with ``FETCH FORWARD n`` — the API never materialises the whole
result set in memory.

Guarantees, mapped to the operational requirements:

* at most one session exists at any moment (``SessionBusyError`` otherwise);
  the second pooled connection keeps serving ordinary ``/query`` traffic;
* the page token serialises concurrent use through a per-session lock, so
  each fetch advances the cursor exactly once — no duplicated or skipped
  pages and no interleaved protocol on the connection;
* every request carries its own total deadline and queueing counts against
  it (pool acquire for ``open``, the session lock for ``next``);
* the session ends — transaction rolled back, token invalidated — on the
  last page, on request timeout, on client disconnect, on any database
  failure, or after ``paged_idle_timeout_ms`` of idleness; the connection is
  reused only after the backend is confirmed IDLE, otherwise it is discarded
  and rebuilt by the pool.
"""
from __future__ import annotations

import asyncio
import logging
import secrets
import time
from dataclasses import dataclass
from typing import Any

import psycopg

from .config import Settings
from .errors import (
    InvalidPageTokenError,
    PoolAcquireTimeout,
    QueryTimeout,
    SessionBusyError,
)
from .executor import QueryExecutor
from .pool import QueryConnectionPool

logger = logging.getLogger("paging")

_CANCEL_GRACE_S = 5.0  # max wait for a cancelled statement task to finish
_POLL_SLICE_S = 0.05   # deadline/disconnect polling granularity


class ClientAborted(Exception):
    """The HTTP client went away mid-request; the session was terminated."""


@dataclass
class _Session:
    """One pinned connection + open cursor.  Guarded by ``lock``."""

    token: str
    conn: psycopg.AsyncConnection
    cursor_name: str
    page_size: int
    lock: asyncio.Lock
    created_at: float
    last_touch: float
    pages_fetched: int = 0
    closed: bool = False


class PaginationManager:
    def __init__(self, pool: QueryConnectionPool, settings: Settings) -> None:
        self._pool = pool
        self._settings = settings
        self._current: _Session | None = None
        self._opening = False
        self._gate = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #
    def status(self) -> dict[str, Any]:
        session = self._current
        base: dict[str, Any] = {
            "idle_timeout_ms": self._settings.paged_idle_timeout_ms,
        }
        if session is None or session.closed:
            return {"active": False, **base}
        return {
            "active": True,
            "idle_ms": round((time.monotonic() - session.last_touch) * 1000, 1),
            "page_size": session.page_size,
            "pages_fetched": session.pages_fetched,
            **base,
        }

    # ------------------------------------------------------------------ #
    # Open a session (first page included)
    # ------------------------------------------------------------------ #
    async def open(
        self,
        sql: str,
        page_size: int,
        deadline: float,
        abort: asyncio.Event | None = None,
    ) -> dict[str, Any]:
        """Open the single allowed session and return its first page.

        Queueing for a pooled connection consumes the caller's deadline.  A
        dead-on-arrival backend is discarded and the open retried once within
        the same budget, mirroring the plain-query executor.
        """
        async with self._gate:
            if self._opening or (
                self._current is not None and not self._current.closed
            ):
                raise SessionBusyError(
                    "已有进行中的分页会话；同一时刻仅允许一个，"
                    "请等其结束或调用 /paged/close 后重试"
                )
            self._opening = True
        try:
            return await self._open_inner(sql, page_size, deadline, abort)
        finally:
            self._opening = False

    async def _open_inner(
        self,
        sql: str,
        page_size: int,
        deadline: float,
        abort: asyncio.Event | None,
    ) -> dict[str, Any]:
        last_conn_error: psycopg.Error | None = None
        for _attempt in (1, 2):
            wait_started = time.monotonic()
            # Propagates PoolAcquireTimeout (503): nothing was ever sent.
            conn = await self._pool.acquire(deadline)
            waited_ms = round((time.monotonic() - wait_started) * 1000, 3)
            acquired_at = time.monotonic()
            cursor_name = f"snap_{secrets.token_hex(8)}"

            async def _run() -> list[tuple[Any, ...]]:
                backstop_ms = max(
                    1,
                    int(
                        (deadline - time.monotonic()) * 1000
                        + self._settings.statement_timeout_grace_ms
                    ),
                )
                async with conn.cursor() as cur:
                    # REPEATABLE READ pins one snapshot for the whole
                    # session; READ ONLY is defence in depth.
                    await cur.execute(
                        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"
                    )
                    await cur.execute(
                        "SELECT set_config('statement_timeout', %s, true)",
                        (str(backstop_ms),),
                    )
                    # Force the transaction snapshot to be taken now, at
                    # session open, not lazily at some later statement.
                    await cur.execute("SELECT 1")
                    await cur.execute(
                        f"DECLARE {cursor_name} NO SCROLL CURSOR FOR {sql}"
                    )
                    await cur.execute(
                        f"FETCH FORWARD {page_size} FROM {cursor_name}"
                    )
                    return await cur.fetchall()

            task = asyncio.ensure_future(_run())
            try:
                rows = await self._await_statement(task, deadline, abort)
            except ClientAborted:
                await self._cancel_and_wait(conn, task)
                await self._release_or_discard(conn)
                raise
            except (asyncio.TimeoutError, TimeoutError):
                raise await self._open_timeout_receipt(
                    conn, task, waited_ms, acquired_at
                )
            except psycopg.errors.QueryCanceled as exc:
                # Server-side statement_timeout backstop fired first.
                logger.warning("open backstop fired: %s", exc)
                raise await self._open_timeout_receipt(
                    conn, task, waited_ms, acquired_at
                )
            except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                # Connection-level failure before the session existed:
                # discard, rebuild, retry once within the same budget.
                logger.warning("connection-level error during paged open: %s", exc)
                await self._pool.discard_and_replace(conn)
                last_conn_error = exc
                if deadline - time.monotonic() <= 0:
                    raise
                continue
            except psycopg.Error:
                # Statement-level error (e.g. undefined table): roll back,
                # verify IDLE, reuse; no session was established.
                await self._release_or_discard(conn)
                raise

            execution_ms = round((time.monotonic() - acquired_at) * 1000, 3)
            if len(rows) < page_size:
                # The first page is also the last: nothing to keep open.
                await self._release_or_discard(conn)
                return self._page(rows, False, None, waited_ms, execution_ms, 1)

            session = _Session(
                token=secrets.token_urlsafe(24),
                conn=conn,
                cursor_name=cursor_name,
                page_size=page_size,
                lock=asyncio.Lock(),
                created_at=time.monotonic(),
                last_touch=time.monotonic(),
                pages_fetched=1,
            )
            self._current = session
            logger.info(
                "paged session %s... opened (page_size=%d)",
                session.token[:8],
                page_size,
            )
            return self._page(rows, True, session.token, waited_ms, execution_ms, 1)

        raise last_conn_error  # pragma: no cover - loop returns or raises

    # ------------------------------------------------------------------ #
    # Fetch the next page of an existing session
    # ------------------------------------------------------------------ #
    async def fetch_next(
        self,
        token: str,
        deadline: float,
        abort: asyncio.Event | None = None,
    ) -> dict[str, Any]:
        session = self._current
        if (
            session is None
            or session.closed
            or not secrets.compare_digest(session.token, token)
        ):
            raise InvalidPageTokenError("取页令牌无效或分页会话已结束")

        # Waiting for the session lock (concurrent use of the same token)
        # counts against this request's total deadline.
        wait_started = time.monotonic()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise PoolAcquireTimeout("总截止时间已在取页前耗尽，语句未执行")
        try:
            await asyncio.wait_for(session.lock.acquire(), timeout=remaining)
        except (asyncio.TimeoutError, TimeoutError) as exc:
            raise PoolAcquireTimeout(
                "等待分页会话占用锁超过总截止时间，语句未执行"
            ) from exc
        waited_ms = round((time.monotonic() - wait_started) * 1000, 3)
        try:
            if session.closed:
                # Terminated (idle reaper / close) while we queued.
                raise InvalidPageTokenError("取页令牌无效或分页会话已结束")
            if deadline - time.monotonic() <= 0:
                raise PoolAcquireTimeout("排队期间总期限已耗尽，语句未执行")
            session.last_touch = time.monotonic()
            acquired_at = time.monotonic()
            conn = session.conn

            async def _run() -> list[tuple[Any, ...]]:
                # Re-arm the server-side backstop for *this* request's
                # deadline (SET LOCAL persists inside the session txn).
                backstop_ms = max(
                    1,
                    int(
                        (deadline - time.monotonic()) * 1000
                        + self._settings.statement_timeout_grace_ms
                    ),
                )
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT set_config('statement_timeout', %s, true)",
                        (str(backstop_ms),),
                    )
                    await cur.execute(
                        f"FETCH FORWARD {session.page_size} "
                        f"FROM {session.cursor_name}"
                    )
                    return await cur.fetchall()

            task = asyncio.ensure_future(_run())
            try:
                rows = await self._await_statement(task, deadline, abort)
            except ClientAborted:
                await self._cancel_and_wait(conn, task)
                await self._terminate(session, "client disconnected")
                raise
            except (asyncio.TimeoutError, TimeoutError):
                raise await self._session_timeout_receipt(
                    session, task, waited_ms, acquired_at
                )
            except psycopg.errors.QueryCanceled as exc:
                logger.warning("fetch backstop fired: %s", exc)
                raise await self._session_timeout_receipt(
                    session, task, waited_ms, acquired_at
                )
            except psycopg.Error:
                # Statement- or connection-level failure: the snapshot is
                # lost.  Terminate; _terminate reuses the connection only
                # when the backend is confirmed IDLE, else discards it.
                await self._terminate(session, "database error during fetch")
                raise

            session.pages_fetched += 1
            execution_ms = round((time.monotonic() - acquired_at) * 1000, 3)
            if len(rows) < session.page_size:
                # Short page (including an empty one) means the cursor is
                # exhausted: release transaction and connection now.
                await self._terminate(session, "last page fetched")
                return self._page(
                    rows, False, None, waited_ms, execution_ms,
                    session.pages_fetched,
                )
            session.last_touch = time.monotonic()
            return self._page(
                rows, True, session.token, waited_ms, execution_ms,
                session.pages_fetched,
            )
        finally:
            session.lock.release()

    # ------------------------------------------------------------------ #
    # Explicit close / idle reaping / shutdown
    # ------------------------------------------------------------------ #
    async def close(self, token: str) -> None:
        session = self._current
        if (
            session is None
            or session.closed
            or not secrets.compare_digest(session.token, token)
        ):
            raise InvalidPageTokenError("取页令牌无效或分页会话已结束")
        async with session.lock:
            await self._terminate(session, "closed by client")

    async def reap_idle(self) -> None:
        """Terminate the session if it has been idle past the limit."""
        session = self._current
        if session is None or session.closed or session.lock.locked():
            return
        idle_ms = (time.monotonic() - session.last_touch) * 1000
        if idle_ms <= self._settings.paged_idle_timeout_ms:
            return
        async with session.lock:
            if session.closed:
                return
            idle_ms = (time.monotonic() - session.last_touch) * 1000
            if idle_ms <= self._settings.paged_idle_timeout_ms:
                return
            await self._terminate(session, f"idle for {int(idle_ms)} ms")

    async def shutdown(self) -> None:
        session = self._current
        if session is None:
            return
        async with session.lock:
            await self._terminate(session, "service shutdown")

    # ------------------------------------------------------------------ #
    # Termination and recovery primitives
    # ------------------------------------------------------------------ #
    async def _terminate(self, session: _Session, reason: str) -> bool:
        """End the session exactly once; return True iff the conn was reused.

        Caller must hold ``session.lock`` (or the session must not be
        registered yet).  ROLLBACK both ends the transaction (dropping the
        server-side cursor) and proves the backend responds; only a
        confirmed-IDLE connection goes back to the pool.
        """
        if session.closed:
            return True
        session.closed = True
        if self._current is session:
            self._current = None
        logger.warning(
            "paged session %s... terminated: %s", session.token[:8], reason
        )
        return await self._release_or_discard(session.conn)

    async def _release_or_discard(self, conn: psycopg.AsyncConnection) -> bool:
        idle = await self._pool.confirm_idle_or_discard(conn)
        if idle:
            await self._pool.release_healthy(conn)
            return True
        await self._pool.discard_and_replace(conn)
        return False

    async def _cancel_and_wait(
        self, conn: psycopg.AsyncConnection, task: asyncio.Task
    ) -> tuple[bool, list[tuple] | None]:
        """PQcancel the running statement and await its task to the end.

        Returns ``(cancel_ok, late_rows)``; late rows are audit-logged only
        and must never replace an already-decided receipt.
        """
        cancel_ok = False
        try:
            # PQcancel uses a separate connection and blocks; run off-loop.
            await asyncio.to_thread(conn.cancel)
            cancel_ok = True
        except Exception:
            logger.exception("PQcancel failed; connection will be discarded")

        late_rows: list[tuple] | None = None
        try:
            late_rows = await asyncio.wait_for(task, timeout=_CANCEL_GRACE_S)
            logger.warning(
                "statement completed before cancel took effect; late result "
                "with %d rows ignored",
                len(late_rows),
            )
        except (asyncio.TimeoutError, TimeoutError):
            logger.error("statement task did not stop after cancel")
        except psycopg.errors.QueryCanceled:
            logger.info("server confirmed cancellation (QueryCanceled)")
        except psycopg.Error as exc:
            logger.info("statement ended after cancel: %s", exc.sqlstate)
        return cancel_ok, late_rows

    async def _await_statement(
        self,
        task: asyncio.Task,
        deadline: float,
        abort: asyncio.Event | None,
    ) -> list[tuple[Any, ...]]:
        """Await a shielded statement task bounded by ``deadline``.

        Polls in short slices so a client disconnect (``abort`` set by the
        endpoint's watcher) pre-empts the wait and triggers the same
        cancel-and-terminate protocol as a timeout.
        """
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError
            try:
                return await asyncio.wait_for(
                    asyncio.shield(task),
                    timeout=min(remaining, _POLL_SLICE_S),
                )
            except (asyncio.TimeoutError, TimeoutError):
                if abort is not None and abort.is_set():
                    raise ClientAborted
                if deadline - time.monotonic() <= 0:
                    raise TimeoutError

    # ------------------------------------------------------------------ #
    # Timeout receipts
    # ------------------------------------------------------------------ #
    async def _open_timeout_receipt(
        self,
        conn: psycopg.AsyncConnection,
        task: asyncio.Task,
        waited_ms: float,
        acquired_at: float,
    ) -> QueryTimeout:
        cancel_ok, late_rows = await self._cancel_and_wait(conn, task)
        reused = await self._release_or_discard(conn)
        return QueryTimeout(
            "开启分页会话超过总截止时间，已请求取消；会话未建立"
            + ("" if reused else "；无法确认后端空闲，连接已弃置并补建"),
            extra={
                "status": "timeout",
                "executed": True,
                "paged": True,
                "session_established": False,
                "cancelled": cancel_ok,
                "connection_discarded": not reused,
                "wait_ms": waited_ms,
                "execution_ms": round(
                    (time.monotonic() - acquired_at) * 1000, 3
                ),
                "late_result_ignored": late_rows is not None,
            },
        )

    async def _session_timeout_receipt(
        self,
        session: _Session,
        task: asyncio.Task,
        waited_ms: float,
        acquired_at: float,
    ) -> QueryTimeout:
        cancel_ok, late_rows = await self._cancel_and_wait(session.conn, task)
        reused = await self._terminate(session, "page fetch exceeded deadline")
        return QueryTimeout(
            "取页超过总截止时间，分页会话已终止，令牌已失效"
            + ("" if reused else "；无法确认后端空闲，连接已弃置并补建"),
            extra={
                "status": "timeout",
                "executed": True,
                "paged": True,
                "session_terminated": True,
                "token_invalidated": True,
                "cancelled": cancel_ok,
                "connection_discarded": not reused,
                "wait_ms": waited_ms,
                "execution_ms": round(
                    (time.monotonic() - acquired_at) * 1000, 3
                ),
                "late_result_ignored": late_rows is not None,
            },
        )

    # ------------------------------------------------------------------ #
    @staticmethod
    def _page(
        rows: list[tuple],
        has_more: bool,
        token: str | None,
        waited_ms: float,
        execution_ms: float,
        page_number: int,
    ) -> dict[str, Any]:
        return {
            "status": "ok",
            "has_more": has_more,
            "page_token": token,
            "row_count": len(rows),
            "rows": [QueryExecutor._jsonable(row) for row in rows],
            "wait_ms": waited_ms,
            "execution_ms": execution_ms,
            "page_number": page_number,
        }
