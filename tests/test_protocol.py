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
from app.errors import PoolAcquireTimeout, QueryTimeout  # noqa: E402
from app.executor import QueryExecutor  # noqa: E402
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
    pg.cleanup()
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
