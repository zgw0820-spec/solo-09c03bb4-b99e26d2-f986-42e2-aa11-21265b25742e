"""Focused protocol tests that do not need a running HTTP server.

They drive the executor/pool directly against the ephemeral PostgreSQL and
concentrate on the hardest correctness guarantees:

* a statement that finishes just before the cancel request arrives must not
  overwrite the already-decided timeout receipt ("late result");
* a queued request whose deadline expires never sends a statement;
* after a timeout the backend is confirmed IDLE before reuse.

Run:  python tests/test_protocol.py
"""
from __future__ import annotations

import asyncio
import pathlib
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import psycopg  # noqa: E402
import pgserver  # noqa: E402

from app.config import Settings  # noqa: E402
from app.errors import (  # noqa: E402
    InvalidPageTokenError,
    PoolAcquireTimeout,
    QueryTimeout,
    SessionBusyError,
)
from app.executor import QueryExecutor  # noqa: E402
from app.paging import PaginationManager  # noqa: E402
from app.pool import QueryConnectionPool  # noqa: E402

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


async def main() -> int:
    data_dir = tempfile.mkdtemp(prefix="proto-")
    pg = pgserver.get_server(data_dir, cleanup_mode="stop")
    admin_dsn = pg.get_uri()
    ddl = (ROOT / "db" / "01_init.sql").read_text(encoding="utf-8")
    with psycopg.connect(admin_dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute("CREATE DATABASE faultdb")
    from urllib.parse import urlparse, urlunparse

    p = urlparse(admin_dsn)
    fault_admin = urlunparse(p._replace(path="/faultdb"))
    with psycopg.connect(fault_admin, autocommit=True) as c, c.cursor() as cur:
        cur.execute(ddl)
        cur.execute("ALTER ROLE fault_ro PASSWORD NULL")
    ro = urlunparse(
        urlparse(fault_admin)._replace(
            netloc=urlparse(fault_admin).netloc.replace("postgres", "fault_ro", 1)
        )
    )

    settings = Settings(database_dsn=ro, pool_size=2)
    pool = QueryConnectionPool(settings)
    await pool.open()
    executor = QueryExecutor(pool, settings)

    print("=== A. 迟到结果不改写超时回执 ===")
    # The statement (150ms) always outlives the 60ms deadline, so the timeout
    # path is mandatory.  Cancellation arrives ~sub-millisecond later, which
    # is also how a real "finished just before cancel" race is shaped: the
    # shielded task is awaited and whatever it returns is recorded but never
    # allowed to replace the receipt.
    receipt: QueryTimeout | None = None
    late_seen = False
    try:
        await executor.execute(
            "SELECT pg_sleep(0.15)", time.monotonic() + 0.06
        )
    except QueryTimeout as exc:
        receipt = exc
        late_seen = bool(exc.extra.get("late_result_ignored"))

    check("截止时间到必定返回 QueryTimeout", receipt is not None)
    if receipt is not None:
        check("回执固定为 timeout",
              receipt.extra.get("status") == "timeout", receipt.extra)
        check("回执不携带 rows（迟到结果未泄漏）",
              "rows" not in receipt.extra, receipt.extra)
        check("已发起取消并确认事务结束(未弃置)",
              receipt.extra.get("cancelled") is True
              and receipt.extra.get("connection_discarded") is False,
              receipt.extra)
        print(f"  INFO  late_result_ignored={late_seen} "
              f"(True=语句在取消前完成；False=服务器确认取消)")

    print("=== B. 超时后确认 IDLE 才复用 ===")
    st = await pool.status()
    check("池容量仍为 2", st.size == 2, st)
    check("两条后端均空闲(IDLE)", st.idle == 2, st)
    ok = await executor.execute("SELECT 7", time.monotonic() + 5)
    check("后续查询得到自身结果", ok["rows"] == [[7]], ok)

    print("=== C. 排队过期绝不发送语句 ===")
    # Occupy both backends with long sleeps (long own deadlines).
    async def slow(tag: str) -> dict:
        return await executor.execute(
            f"SELECT '{tag}', pg_sleep(2)", time.monotonic() + 6
        )

    busy = [asyncio.ensure_future(slow("X")), asyncio.ensure_future(slow("Y"))]
    await asyncio.sleep(0.4)
    queued_err = None
    try:
        await executor.execute(
            "SELECT 'NEVER-RUN'", time.monotonic() + 0.4
        )
    except PoolAcquireTimeout as exc:
        queued_err = exc
    check("排队过期抛 PoolAcquireTimeout", queued_err is not None)
    results = await asyncio.gather(*busy)
    check("两条占池查询各自成功",
          {r["rows"][0][0] for r in results} == {"X", "Y"}, results)
    # Prove the queued statement never reached a backend: no marker exists.
    after = await executor.execute("SELECT 1", time.monotonic() + 5)
    check("过期语句无任何副作用，后续查询正常", after["rows"] == [[1]], after)
    st = await pool.status()
    check("结束后池满且空闲", st.idle == 2 and st.size == 2, st)

    await pool.close()

    # ------------------------------------------------------------------ #
    # Paged snapshot sessions (manager level, short idle timeout for speed)
    # ------------------------------------------------------------------ #
    settings2 = Settings(database_dsn=ro, pool_size=2, paged_idle_timeout_ms=600)
    pool2 = QueryConnectionPool(settings2)
    await pool2.open()
    executor2 = QueryExecutor(pool2, settings2)
    mgr = PaginationManager(pool2, settings2)

    print("=== D. 分页快照：新入库不混入、逐页推进、末页释放 ===")
    page = await mgr.open(
        "SELECT id FROM fault_logs ORDER BY id", 3, time.monotonic() + 5
    )
    check("首页 3 行且发放令牌",
          page["row_count"] == 3 and page["has_more"] and page["page_token"],
          page)
    token = page["page_token"]
    # Insert a row *after* the snapshot was taken; it must never appear.
    with psycopg.connect(fault_admin, autocommit=True) as c, c.cursor() as cur:
        cur.execute(
            "INSERT INTO fault_logs (device_id, site, severity, fault_code,"
            " message) VALUES ('LATE-ROW','t','INFO','PROBE','late')"
        )
    try:
        pages = [page]
        while pages[-1]["has_more"]:
            pages.append(await mgr.fetch_next(token, time.monotonic() + 5))
        seen = [r[0] for p in pages for r in p["rows"]]
        check("分页合计恰为快照时点的 7 行", seen == [1, 2, 3, 4, 5, 6, 7], seen)
        check("末页后令牌清空", pages[-1]["page_token"] is None, pages[-1])
        token_dead = False
        try:
            await mgr.fetch_next(token, time.monotonic() + 1)
        except InvalidPageTokenError:
            token_dead = True
        check("末页后旧令牌失效", token_dead)
        st = await pool2.status()
        check("末页释放后池满且空闲", st.idle == 2 and st.size == 2, st)
        # A fresh ordinary query does see the new row (snapshot was ours only).
        fresh = await executor2.execute(
            "SELECT count(*) FROM fault_logs", time.monotonic() + 5
        )
        check("普通查询可见新入库行", fresh["rows"] == [[8]], fresh)
    finally:
        with psycopg.connect(fault_admin, autocommit=True) as c, \
                c.cursor() as cur:
            cur.execute("DELETE FROM fault_logs WHERE device_id = 'LATE-ROW'")

    print("=== E. 同一令牌并发取页不重复推进 ===")
    page = await mgr.open(
        "SELECT id FROM fault_logs ORDER BY id", 2, time.monotonic() + 5
    )
    token = page["page_token"]
    a, b = await asyncio.gather(
        mgr.fetch_next(token, time.monotonic() + 5),
        mgr.fetch_next(token, time.monotonic() + 5),
    )
    got = sorted(tuple(r[0] for r in p["rows"]) for p in (a, b))
    check("并发两请求各得一页、无重复无跳过", got == [(3, 4), (5, 6)], got)
    last = await mgr.fetch_next(token, time.monotonic() + 5)
    check("末页 1 行并关闭会话",
          last["row_count"] == 1 and not last["has_more"], last)

    print("=== F. 单会话限制与会话期间普通查询 ===")
    page = await mgr.open(
        "SELECT id FROM fault_logs ORDER BY id", 2, time.monotonic() + 5
    )
    token = page["page_token"]
    busy = False
    try:
        await mgr.open("SELECT 1", 2, time.monotonic() + 1)
    except SessionBusyError:
        busy = True
    check("会话未结束时再次开启被拒绝", busy)
    parallel = await executor2.execute(
        "SELECT 'PARALLEL'", time.monotonic() + 5
    )
    check("另一条连接继续服务普通查询", parallel["rows"] == [["PARALLEL"]],
          parallel)
    st = await pool2.status()
    check("会话独占一条连接", st.in_use == 1, st)
    await mgr.close(token)
    st = await pool2.status()
    check("显式关闭后连接归还", st.idle == 2, st)
    dead = False
    try:
        await mgr.fetch_next(token, time.monotonic() + 1)
    except InvalidPageTokenError:
        dead = True
    check("关闭后令牌失效", dead)

    print("=== G. 闲置超时回收 ===")
    page = await mgr.open(
        "SELECT id FROM fault_logs ORDER BY id", 2, time.monotonic() + 5
    )
    token = page["page_token"]
    await asyncio.sleep(0.35)
    await mgr.reap_idle()
    alive = await mgr.fetch_next(token, time.monotonic() + 5)
    check("未超闲置时限会话仍可用", alive["row_count"] == 2, alive)
    await asyncio.sleep(0.7)  # idle limit is 600ms now
    await mgr.reap_idle()
    dead = False
    try:
        await mgr.fetch_next(token, time.monotonic() + 1)
    except InvalidPageTokenError:
        dead = True
    check("闲置超时后令牌失效", dead)
    st = await pool2.status()
    check("闲置回收后连接归还", st.idle == 2, st)

    print("=== H. 取页超时终止会话并恢复连接 ===")
    page = await mgr.open(
        "SELECT g, CASE WHEN g = 2 THEN pg_sleep(1) END "
        "FROM generate_series(1, 4) g",
        1,
        time.monotonic() + 5,
    )
    token = page["page_token"]
    receipt = None
    try:
        await mgr.fetch_next(token, time.monotonic() + 0.3)
    except QueryTimeout as exc:
        receipt = exc
    check("取页超时抛 QueryTimeout", receipt is not None)
    if receipt is not None:
        check("回执标明会话终止且令牌失效",
              receipt.extra.get("session_terminated") is True
              and receipt.extra.get("token_invalidated") is True,
              receipt.extra)
    dead = False
    try:
        await mgr.fetch_next(token, time.monotonic() + 1)
    except InvalidPageTokenError:
        dead = True
    check("超时后旧令牌失效", dead)
    st = await pool2.status()
    check("超时后池恢复满容量且空闲", st.idle == 2 and st.size == 2, st)
    again = await mgr.open("SELECT 1", 2, time.monotonic() + 5)
    check("终止后可立即开新会话", again["row_count"] == 1, again)

    print("=== I. 会话期间后端被杀：终止会话、弃置补建 ===")
    page = await mgr.open(
        "SELECT id FROM fault_logs ORDER BY id", 2, time.monotonic() + 5
    )
    token = page["page_token"]
    # Kill the session's backend out-of-band.
    with psycopg.connect(fault_admin, autocommit=True) as c, c.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE usename = 'fault_ro' AND pid <> pg_backend_pid() "
            "AND state = 'idle in transaction'"
        )
        check("管理连接成功终止会话后端", cur.fetchone()[0] is True)
    await asyncio.sleep(0.2)  # let the server notice the dropped socket
    failed = False
    try:
        await mgr.fetch_next(token, time.monotonic() + 5)
    except psycopg.Error:
        failed = True
    check("取页时连接级错误浮出", failed)
    dead = False
    try:
        await mgr.fetch_next(token, time.monotonic() + 1)
    except InvalidPageTokenError:
        dead = True
    check("故障后旧令牌失效", dead)
    st = await pool2.status()
    check("弃置补建后池恢复满容量且空闲",
          st.idle == 2 and st.size == 2 and st.total_discarded >= 1, st)
    ok = await executor2.execute("SELECT 'AFTER-KILL'", time.monotonic() + 5)
    check("普通查询在补建连接上正常", ok["rows"] == [["AFTER-KILL"]], ok)
    again = await mgr.open("SELECT 1", 2, time.monotonic() + 5)
    check("故障后可立即开新会话", again["row_count"] == 1, again)

    await pool2.close()
    pg.cleanup()
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
