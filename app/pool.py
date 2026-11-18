"""A fixed-size pool of **two** read-only PostgreSQL connections.

Design goals, mapped to the operational requirements:

* ``POOL_SIZE`` (default 2) physical connections at most.  A bounded
  semaphore serialises requests so a third request *waits* instead of
  opening another backend.
* Waiting for a free connection consumes the same deadline budget as
  execution (:meth:`QueryConnectionPool.acquire` takes an absolute
  monotonic deadline).  If the deadline passes while queued, no statement
  is ever sent on any connection.
* After a timeout the handler must prove the backend is idle again
  (``transaction_status == IDLE`` after a successful ``ROLLBACK``) before
  the connection is returned to the pool.  If that cannot be confirmed the
  connection is closed and discarded, and a replacement is built so the
  pool keeps its capacity of two.
* Every connection is opened in a read-only, read-committed session.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass

from psycopg import AsyncConnection
from psycopg.pq import TransactionStatus

from .config import Settings
from .errors import PoolAcquireTimeout

logger = logging.getLogger("querypool")


@dataclass
class PoolStatus:
    size: int
    idle: int
    in_use: int
    total_created: int
    total_discarded: int
    closed: bool


class QueryConnectionPool:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # One slot per *borrower*.  Acquiring a slot guarantees the right to
        # use one physical backend, even if that backend must be (re)built.
        self._slots = asyncio.BoundedSemaphore(settings.pool_size)
        self._free: asyncio.Queue[AsyncConnection] = asyncio.Queue()
        # Number of physical backends currently tracked (idle + in use).
        # It can temporarily fall below pool_size when a rebuild fails.
        self._backends = 0
        self._closed = False
        self._lock = asyncio.Lock()
        self._total_created = 0
        self._total_discarded = 0

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def open(self) -> None:
        """Eagerly establish the full pool so failures surface at startup."""
        async with self._lock:
            for _ in range(self._settings.pool_size):
                conn = await self._make_connection()
                await self._free.put(conn)
        logger.info(
            "pool opened: size=%d created=%d",
            self._settings.pool_size,
            self._total_created,
        )

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            while True:
                try:
                    conn = self._free.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._backends -= 1
                await self._safe_close(conn)
            logger.info("pool closed")

    async def status(self) -> PoolStatus:
        idle = self._free.qsize()
        return PoolStatus(
            size=self._settings.pool_size,
            idle=idle,
            in_use=max(0, self._backends - idle),
            total_created=self._total_created,
            total_discarded=self._total_discarded,
            closed=self._closed,
        )

    # ------------------------------------------------------------------ #
    # Connection factory
    # ------------------------------------------------------------------ #
    async def _make_connection(self) -> AsyncConnection:
        """Open a fresh connection pinned to a read-only session."""
        # Start in autocommit so the session SETs below do not leave an
        # implicit transaction open (psycopg opens one on the first command
        # of a non-autocommit connection).  Switch back afterwards so every
        # request manages explicit BEGIN/COMMIT itself.
        conn = await AsyncConnection.connect(
            self._settings.database_dsn,
            connect_timeout=self._settings.connect_timeout,
            autocommit=True,
        )
        # Session-level safety net: even an accidental COMMIT cannot write.
        await conn.execute("SET default_transaction_read_only = on")
        await conn.execute(
            "SET default_transaction_isolation = 'read committed'"
        )
        # Async connections expose a setter coroutine, not a writable property.
        await conn.set_autocommit(False)
        assert conn.info.transaction_status == TransactionStatus.IDLE
        self._backends += 1
        self._total_created += 1
        logger.info(
            "connection created (backends=%d total_created=%d)",
            self._backends,
            self._total_created,
        )
        return conn

    async def _safe_close(self, conn: AsyncConnection) -> None:
        with contextlib.suppress(Exception):
            await conn.close()

    @staticmethod
    def _is_idle(conn: AsyncConnection) -> bool:
        """True only when the backend has no open/in-progress transaction."""
        return (not conn.closed) and (
            conn.info.transaction_status == TransactionStatus.IDLE
        )

    # ------------------------------------------------------------------ #
    # Acquire / release
    # ------------------------------------------------------------------ #
    async def acquire(self, deadline: float) -> AsyncConnection:
        """Reserve one connection, waiting at most until ``deadline``.

        Time spent here counts against the request's total deadline.  On
        timeout the semaphore slot is released and *nothing has been sent
        to the database* for this request.
        """
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise PoolAcquireTimeout("总截止时间已在排队前耗尽，语句未执行")

        try:
            await asyncio.wait_for(self._slots.acquire(), timeout=remaining)
        except TimeoutError as exc:
            raise PoolAcquireTimeout(
                "等待连接超过总截止时间，排队期间已过期，语句未执行"
            ) from exc

        try:
            return await self._take_idle_or_build(deadline)
        except Exception:
            # Any failure to obtain a physical backend frees the borrower
            # slot so the pool cannot deadlock after a failed rebuild.
            self._slots.release()
            raise

    async def _take_idle_or_build(self, deadline: float) -> AsyncConnection:
        # No lock across awaits: all mutations happen on one event loop and
        # each step is atomic between awaits, so no interleaving can corrupt
        # the counters or the queue.
        if self._free.qsize() > 0:
            conn = self._free.get_nowait()
        else:
            # Pool shrank after an earlier discarded connection: rebuild
            # lazily within the caller's remaining deadline budget.
            logger.info("no idle backend; lazily rebuilding within pool")
            conn = await self._make_connection()

        if not self._is_idle(conn):
            # An idle backend turned stale; replace it before handing out.
            logger.warning(
                "idle connection not healthy (closed=%s tx=%s), replacing",
                conn.closed,
                conn.info.transaction_status,
            )
            await self._discard_locked(conn)
            conn = await self._make_connection()
        return conn

    async def release_healthy(self, conn: AsyncConnection) -> None:
        """Return a connection that has been confirmed IDLE to the pool."""
        if not self._is_idle(conn):
            # Never put an unverified connection back; rebuild instead.
            await self.discard_and_replace(conn)
            return
        await self._free.put(conn)
        self._slots.release()

    async def discard_and_replace(self, conn: AsyncConnection) -> None:
        """Drop an unverifiable connection and build its replacement.

        The replacement is built *before* the slot is released so the pool
        usually offers the full capacity of two healthy backends afterwards.
        If the replacement cannot be built the slot is released anyway and
        the pool temporarily shrinks; the next acquire lazily restores it.
        """
        logger.warning(
            "discarding connection (closed=%s tx=%s) and rebuilding",
            conn.closed,
            conn.info.transaction_status,
        )
        await self._discard_locked(conn)
        try:
            fresh = await self._make_connection()
            await self._free.put(fresh)
        except Exception:
            logger.exception("failed to rebuild discarded connection; "
                             "pool will lazily rebuild on next acquire")
        finally:
            self._slots.release()

    async def _discard_locked(self, conn: AsyncConnection) -> None:
        await self._safe_close(conn)
        self._backends -= 1
        self._total_discarded += 1

    # ------------------------------------------------------------------ #
    # Timeout recovery primitives used by the request handler
    # ------------------------------------------------------------------ #
    async def confirm_idle_or_discard(self, conn: AsyncConnection) -> bool:
        """Best-effort proof that no statement is running anymore.

        Returns ``True`` only when the backend reports the IDLE transaction
        state.  Any failure means the connection must be discarded.
        """
        try:
            if conn.closed:
                return False
            status = conn.info.transaction_status
            if status != TransactionStatus.IDLE:
                # A cancelled statement leaves an aborted transaction;
                # ROLLBACK both ends it and proves the backend responds.
                await conn.execute("ROLLBACK")
                status = conn.info.transaction_status
            return (not conn.closed) and status == TransactionStatus.IDLE
        except Exception:
            logger.exception("could not confirm connection idle")
            return False
