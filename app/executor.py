"""Request execution: deadline accounting, statement cancellation, recovery.

The crucial timeout protocol is::

    1. run the statement as a *shielded* task so a late result can never
       overwrite the timeout receipt;
    2. on deadline, call libpq ``PQcancel`` to ask the server to abort the
       statement;
    3. await the shielded task (the cancellation error) so no coroutine is
       left running against this connection;
    4. prove the backend is IDLE (ROLLBACK + transaction_status check);
    5. only then return the connection to the pool, otherwise discard it
       and build a replacement.

Transactions are committed *inside* the cursor block on purpose: psycopg's
``async with conn.cursor()`` rolls back any transaction still open when the
block exits, so committing afterwards would silently leave the backend in
an aborted/in-transaction state.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import psycopg

from .config import Settings
from .errors import QueryTimeout
from .pool import QueryConnectionPool

logger = logging.getLogger("executor")


class QueryExecutor:
    def __init__(self, pool: QueryConnectionPool, settings: Settings) -> None:
        self._pool = pool
        self._settings = settings

    async def execute(self, sql: str, deadline: float) -> dict[str, Any]:
        """Execute one validated read-only statement against the pool.

        A dead-on-arrival backend (e.g. terminated out-of-band while idle)
        is discarded and rebuilt, then the read-only statement is retried
        once within the same total deadline.
        """
        last_conn_error: psycopg.Error | None = None
        for attempt in (1, 2):
            wait_started = time.monotonic()
            # Propagate PoolAcquireTimeout as a 503 receipt.  When this
            # raises, no connection was borrowed and no statement was sent.
            conn = await self._pool.acquire(deadline)
            waited_ms = round((time.monotonic() - wait_started) * 1000, 3)
            acquired_at = time.monotonic()

            async def _run() -> list[tuple[Any, ...]]:
                # Server-side backstop, deliberately *after* the request
                # deadline: application cancellation normally wins;
                # statement_timeout only intervenes if PQcancel is delayed.
                backstop_ms = max(
                    1,
                    int(
                        (deadline - time.monotonic()) * 1000
                        + self._settings.statement_timeout_grace_ms
                    ),
                )
                async with conn.cursor() as cur:
                    await cur.execute("BEGIN READ ONLY")
                    await cur.execute(
                        "SELECT set_config('statement_timeout', %s, true)",
                        (str(backstop_ms),),
                    )
                    await cur.execute(sql)
                    rows = await cur.fetchall()
                    # Commit before the cursor block exits, or psycopg's
                    # cursor teardown would implicitly ROLLBACK this txn.
                    await cur.execute("COMMIT")
                    return rows

            task = asyncio.ensure_future(_run())
            try:
                rows = await asyncio.wait_for(
                    asyncio.shield(task),
                    timeout=max(0.0, deadline - time.monotonic()),
                )
            except (asyncio.TimeoutError, TimeoutError):
                raise await self._handle_timeout(
                    conn, task, waited_ms, acquired_at
                )
            except psycopg.errors.QueryCanceled as exc:
                # The server-side statement_timeout backstop fired first.
                # Same recovery protocol, same 504 receipt.
                logger.warning("statement_timeout backstop fired: %s", exc)
                raise await self._handle_timeout(
                    conn, task, waited_ms, acquired_at, backstop=True
                )
            except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                # Connection-level failure (backend terminated/restarted):
                # discard it, rebuild via the pool, retry once if budget
                # remains.  Retry is safe because the statement is read-only
                # and never committed anything on a dead connection.
                logger.warning("connection-level error, discarding: %s", exc)
                await self._pool.discard_and_replace(conn)
                last_conn_error = exc
                if deadline - time.monotonic() <= 0:
                    raise
                continue
            except psycopg.Error:
                # Statement-level error: roll back, verify IDLE, then reuse.
                await self._rollback_or_discard(conn)
                raise

            elapsed_ms = round((time.monotonic() - acquired_at) * 1000, 3)
            await self._pool.release_healthy(conn)
            return {
                "status": "ok",
                "executed": True,
                "wait_ms": waited_ms,
                "execution_ms": elapsed_ms,
                "row_count": len(rows),
                "rows": [self._jsonable(row) for row in rows],
            }

        # Defensive: loop should either return or raise earlier.
        raise last_conn_error  # pragma: no cover

    # ------------------------------------------------------------------ #
    async def _handle_timeout(
        self,
        conn: psycopg.AsyncConnection,
        task: asyncio.Task,
        waited_ms: float,
        acquired_at: float,
        backstop: bool = False,
    ) -> QueryTimeout:
        logger.warning(
            "deadline reached (%s), cancelling statement on backend",
            "server backstop" if backstop else "client deadline",
        )

        # 1) libpq cancel request.  PQcancel opens a separate connection to
        #    the server and is a blocking socket call; run it off the loop.
        cancel_ok = False
        try:
            await asyncio.to_thread(conn.cancel)
            cancel_ok = True
        except Exception:
            logger.exception("PQcancel failed; connection will be discarded")

        # 2) Wait for the shielded statement task to actually finish so no
        #    coroutine keeps touching this connection.
        late_rows: list[tuple] | None = None
        try:
            late_rows = await asyncio.wait_for(task, timeout=5.0)
            # The statement finished before the cancel took effect.  Its
            # result must NOT replace the timeout receipt; audit-log only.
            logger.warning(
                "statement completed before cancel took effect; late result "
                "with %d rows ignored, timeout receipt stands",
                len(late_rows),
            )
        except asyncio.TimeoutError:
            logger.error("statement task did not stop after cancel")
        except psycopg.errors.QueryCanceled:
            logger.info("server confirmed cancellation (QueryCanceled)")
        except psycopg.Error as exc:
            logger.info("statement ended after cancel: %s", exc.sqlstate)

        # 3) Prove transaction end.  If we cannot, drop the backend.
        idle = await self._pool.confirm_idle_or_discard(conn)
        discarded = False
        if idle:
            await self._pool.release_healthy(conn)
        else:
            discarded = True
            await self._pool.discard_and_replace(conn)

        elapsed_ms = round((time.monotonic() - acquired_at) * 1000, 3)
        return QueryTimeout(
            "语句执行超过总截止时间，已请求取消"
            + ("；无法确认后端空闲，连接已弃置并补建" if discarded else ""),
            extra={
                "status": "timeout",
                "executed": True,
                "cancelled": cancel_ok,
                "connection_discarded": discarded,
                "wait_ms": waited_ms,
                "execution_ms": elapsed_ms,
                "late_result_ignored": late_rows is not None,
            },
        )

    async def _rollback_or_discard(self, conn: psycopg.AsyncConnection) -> None:
        idle = await self._pool.confirm_idle_or_discard(conn)
        if idle:
            await self._pool.release_healthy(conn)
        else:
            await self._pool.discard_and_replace(conn)

    @staticmethod
    def _jsonable(row: tuple) -> list[Any]:
        out: list[Any] = []
        for value in row:
            # Common non-JSON types from fault logs.
            if hasattr(value, "isoformat"):
                out.append(value.isoformat())
            elif isinstance(value, (bytes, memoryview)):
                out.append(bytes(value).hex())
            else:
                out.append(value)
        return out
