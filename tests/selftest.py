"""End-to-end self-test for the read-only query API.

Runs entirely on the Python standard library so it works inside the slim
application container as well as on a developer machine:

    python -m tests.selftest

Scenarios:
  1. health + normal SELECT returns the analyst's own rows
  2. read-only guard rails (multi-statement, INSERT, non-SELECT rejected)
  3. single slow query times out, is cancelled, receipt is final
  4. a queued request whose budget expires while waiting never executes
  5. after two concurrent slow queries, later requests still get their own
     results (connection reuse after confirmed rollback)
  6. pool capacity really is two (observed via pg_stat_activity)
  7. paged snapshot: page through all rows, token dies on the last page
  8. paged snapshot isolation: rows inserted mid-session never leak in
  9. one session at a time; explicit close; ordinary /query unaffected
 10. concurrent use of one token serialises without double-advancing
 11. page-fetch timeout terminates the session and recovers the connection
 12. open-time timeout and queue expiry leave no session behind
 13. idle session is reaped after the configured 30s limit
 14. client disconnect mid-fetch terminates the session
"""
from __future__ import annotations

import http.client
import json
import os
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8000"

PASS = 0
FAIL = 0


def check(name: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def post(path: str, payload: dict, timeout: float = 30.0):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        BASE + path,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())
    except urllib.error.URLError as exc:
        return -1, {"error": {"code": "connection_failed", "message": str(exc)}}


def get(path: str, timeout: float = 10.0):
    try:
        with urllib.request.urlopen(BASE + path, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())
    except urllib.error.URLError as exc:
        return -1, {"error": {"code": "connection_failed", "message": str(exc)}}


def run_case(title: str, fn) -> None:
    print(f"\n=== {title} ===")
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 - test harness must keep going
        check(f"{title} 未抛异常", False, repr(exc))


def case_health_and_basic() -> None:
    status, body = get("/health")
    check("健康检查 200", status == 200, body)
    check("连接池容量为 2", body["pool"]["size"] == 2, body)
    check("启动即建满两条连接", body["pool"]["idle"] == 2, body)

    status, body = post(
        "/query",
        {"sql": "SELECT count(*) AS n FROM fault_logs", "timeout_ms": 5000},
    )
    check("普通 SELECT 200", status == 200, body)
    check("返回 ok 且已执行", body["status"] == "ok" and body["executed"], body)
    check("预置 7 条故障日志", body["rows"][0][0] == 7, body)

    status, body = post(
        "/query",
        {
            "sql": "SELECT device_id, severity FROM fault_logs "
                   "WHERE severity = 'CRITICAL' ORDER BY id",
            "timeout_ms": 5000,
        },
    )
    check("过滤查询返回自身结果", status == 200 and body["row_count"] == 2, body)
    check(
        "结果内容正确",
        [r[0] for r in body["rows"]] == ["SW-CORE-01", "SRV-DB-07"],
        body,
    )


def case_read_only_guards() -> None:
    for name, sql in [
        ("多语句", "SELECT 1; SELECT 2"),
        ("INSERT", "INSERT INTO fault_logs (device_id) VALUES ('x')"),
        ("UPDATE", "UPDATE fault_logs SET site = 'z'"),
        ("非 SELECT", "SHOW transaction_read_only"),
        ("SELECT INTO", "SELECT 1 INTO TEMP t"),
    ]:
        status, body = post("/query", {"sql": sql, "timeout_ms": 5000})
        check(f"{name} 被拒绝(400)", status == 400, body)
        check(f"{name} 错误码 invalid_sql",
              body.get("error", {}).get("code") == "invalid_sql", body)

    # pg_sleep 是允许的只读函数，用于慢查询演示。
    status, body = post(
        "/query", {"sql": "SELECT pg_sleep(0.1)", "timeout_ms": 5000}
    )
    check("pg_sleep 只读函数允许执行", status == 200 and body["status"] == "ok", body)


def case_single_timeout() -> None:
    _, before = get("/health")
    discarded_before = before["pool"]["total_discarded"]
    t0 = time.monotonic()
    status, body = post(
        "/query",
        {"sql": "SELECT pg_sleep(3)", "timeout_ms": 800},
        timeout=15,
    )
    elapsed = time.monotonic() - t0
    check("超时返回 504", status == 504, body)
    check("错误码 query_timeout",
          body.get("error", {}).get("code") == "query_timeout", body)
    check("回执 status=timeout", body.get("status") == "timeout", body)
    check("语句确实已发送执行", body.get("executed") is True, body)
    check("已发起取消", body.get("cancelled") is True, body)
    check("及时返回 (<2.5s)", elapsed < 2.5, f"elapsed={elapsed:.2f}")

    # Give the server a moment, then prove the slow statement is gone and
    # the connection was reused for a fresh, independent query.
    time.sleep(0.5)
    status, body = post(
        "/query", {"sql": "SELECT 42 AS answer", "timeout_ms": 5000}
    )
    check("超时后连接可复用且返回自身结果",
          status == 200 and body["rows"] == [[42]], body)

    status, health = get("/health")
    check("池容量仍为 2", health["pool"]["size"] == 2, health)
    check("确认事务结束后复用，本用例无新增弃置",
          health["pool"]["total_discarded"] == discarded_before, health)


def case_queued_expiry() -> None:
    results: list[tuple] = []

    def slow(marker: str, sleep_s: float, budget_ms: int) -> None:
        t0 = time.monotonic()
        status, body = post(
            "/query",
            {"sql": f"SELECT '{marker}', pg_sleep({sleep_s})",
             "timeout_ms": budget_ms},
            timeout=20,
        )
        results.append((marker, status, body, round(time.monotonic() - t0, 2)))

    # Two slow queries occupy both connections for ~2.5s.
    t1 = threading.Thread(target=slow, args=("A", 2.5, 5000))
    t2 = threading.Thread(target=slow, args=("B", 2.5, 5000))
    t1.start()
    time.sleep(0.3)
    t2.start()
    time.sleep(0.3)

    # Third request: only 500ms total budget, both backends are busy.
    t0 = time.monotonic()
    status, body = post(
        "/query",
        {"sql": "SELECT 'SHOULD-NOT-RUN', pg_sleep(0)", "timeout_ms": 500},
        timeout=10,
    )
    waited = time.monotonic() - t0
    check("排队过期返回 503", status == 503, body)
    check("错误码 acquire_timeout",
          body.get("error", {}).get("code") == "acquire_timeout", body)
    check("排队等待本身计入预算 (~0.5s)", 0.4 < waited < 1.5,
          f"waited={waited:.2f}")

    t1.join()
    t2.join()
    ok = [r for r in results if r[1] == 200 and r[2]["status"] == "ok"]
    check("两条占池慢查询最终各自成功", len(ok) == 2, results)
    check(
        "两条慢查询拿到各自的标记结果",
        {r[2]["rows"][0][0] for r in ok} == {"A", "B"},
        results,
    )

    # After the backlog drains, pool must be fully reusable.
    status, body = post(
        "/query", {"sql": "SELECT 'AFTER-BACKLOG'", "timeout_ms": 5000}
    )
    check("积压清空后新查询正常",
          status == 200 and body["rows"][0][0] == "AFTER-BACKLOG", body)


def case_capacity_and_reuse() -> None:
    barrier = threading.Barrier(3)
    finished: list = []

    def worker(i: int) -> None:
        barrier.wait()
        status, body = post(
            "/query",
            {"sql": f"SELECT {i}, pg_sleep(1.2)", "timeout_ms": 8000},
            timeout=15,
        )
        finished.append((i, status, body))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
    _, before = get("/health")
    discarded_before = before["pool"]["total_discarded"]
    for t in threads:
        t.start()
    t0 = time.monotonic()
    for t in threads:
        t.join()
    wall = time.monotonic() - t0

    check("三个并发请求全部成功",
          len(finished) == 3 and all(s == 200 for _, s, _ in finished),
          finished)
    check(
        "每个请求拿到自身结果(无串结果)",
        {b["rows"][0][0] for _, _, b in finished} == {0, 1, 2},
        finished,
    )
    # Capacity 2 -> three 1.2s sleeps must take > 1.8s (two waves).
    check("容量 2 导致第三个请求排队 (>1.8s)", wall > 1.8, f"wall={wall:.2f}")

    status, health = get("/health")
    check("结束后两条连接全部空闲", health["pool"]["idle"] == 2, health)
    check("容量保持 2、本用例无新增弃置",
          health["pool"]["size"] == 2
          and health["pool"]["total_discarded"] == discarded_before, health)


def _admin_dsn() -> str | None:
    """Admin DSN for chaos/snapshot-injection scenarios, if available."""
    dsn = os.environ.get("ADMIN_DSN")
    if not dsn:
        dsn_file = os.path.join(tempfile.gettempdir(), "faultdb_admin_dsn")
        if os.path.exists(dsn_file):
            with open(dsn_file, encoding="utf-8") as fh:
                dsn = fh.read().strip()
    return dsn


def case_discard_and_rebuild() -> None:
    """Kill a pooled backend out-of-band; the API must discard and rebuild."""
    admin_dsn = _admin_dsn()
    if not admin_dsn:
        print("  SKIP  弃置补建混沌测试（未设置 ADMIN_DSN）")
        return
    try:
        import psycopg
    except ImportError:
        print("  SKIP  弃置补建混沌测试（无 psycopg）")
        return

    with psycopg.connect(admin_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pid FROM pg_stat_activity "
            "WHERE usename = 'fault_ro' AND pid <> pg_backend_pid()"
        )
        pids = [r[0] for r in cur.fetchall()]
        check("池内存在两条 fault_ro 后端", len(pids) == 2, pids)
        victim = pids[0]
        cur.execute("SELECT pg_terminate_backend(%s)", (victim,))
        check("管理连接成功终止一个后端", cur.fetchone()[0] is True)

    # Run several sequential queries so both pooled backends are cycled;
    # whichever request draws the dead backend must transparently discard,
    # rebuild and retry — every client call still succeeds with its own rows.
    statuses, markers = [], []
    for i in range(4):
        status, body = post(
            "/query",
            {"sql": f"SELECT 'CYCLE-{i}'", "timeout_ms": 8000},
        )
        statuses.append(status)
        if status == 200:
            markers.append(body["rows"][0][0])
    check("循环借出期间所有请求均成功(客户端无感)",
          statuses == [200] * 4, statuses)
    check("每次都返回自身结果",
          markers == [f"CYCLE-{i}" for i in range(4)], markers)

    status, health = get("/health")
    check("弃置计数增加",
          health["pool"]["total_discarded"] >= 1, health)
    check("补建后容量恢复为 2 且全空闲",
          health["pool"]["size"] == 2 and health["pool"]["idle"] == 2, health)

    # Physical capacity check: while two slow queries run, the database must
    # see at most two fault_ro backends — no third connection is ever opened.
    observed: list[int] = []
    barrier = threading.Barrier(2)

    def slow(i: int) -> None:
        barrier.wait()
        post("/query",
             {"sql": f"SELECT {i}, pg_sleep(2)", "timeout_ms": 8000},
             timeout=15)

    threads = [threading.Thread(target=slow, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    deadline = time.monotonic() + 3
    with psycopg.connect(admin_dsn) as conn, conn.cursor() as cur:
        while time.monotonic() < deadline:
            cur.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE usename='fault_ro'"
            )
            observed.append(cur.fetchone()[0])
            time.sleep(0.1)
    for t in threads:
        t.join()
    check("数据库侧 fault_ro 后端数从未超过 2",
          observed and max(observed) <= 2, f"max={max(observed)}")


def case_paged_happy_path() -> None:
    status, body = post(
        "/paged/query",
        {"sql": "SELECT id, device_id FROM fault_logs ORDER BY id",
         "page_size": 3, "timeout_ms": 5000},
    )
    check("开启分页会话 200", status == 200, body)
    check("首页 3 行且附令牌",
          body["row_count"] == 3 and body["has_more"] and body["page_token"],
          body)
    token = body["page_token"]

    # The session pins one connection; ordinary queries use the other one.
    status, q = post(
        "/query", {"sql": "SELECT 'STILL-WORKS'", "timeout_ms": 5000}
    )
    check("会话期间普通 /query 仍可用",
          status == 200 and q["rows"] == [["STILL-WORKS"]], q)
    _, health = get("/health")
    check("健康检查显示分页会话活跃、独占一条连接",
          health["pagination"]["active"] is True
          and health["pool"]["in_use"] == 1, health)

    seen = [r[0] for r in body["rows"]]
    status, p2 = post("/paged/next", {"page_token": token, "timeout_ms": 5000})
    check("第二页 3 行", status == 200 and p2["row_count"] == 3
          and p2["has_more"] and p2["page_number"] == 2, p2)
    seen += [r[0] for r in p2["rows"]]
    status, p3 = post("/paged/next", {"page_token": token, "timeout_ms": 5000})
    check("第三页 1 行且为末页", status == 200 and p3["row_count"] == 1
          and not p3["has_more"], p3)
    check("末页后不再发放令牌", p3["page_token"] is None, p3)
    seen += [r[0] for r in p3["rows"]]
    check("7 行齐全且顺序正确、无重复", seen == [1, 2, 3, 4, 5, 6, 7], seen)

    status, body = post("/paged/next", {"page_token": token, "timeout_ms": 5000})
    check("末页后旧令牌失效(410)",
          status == 410
          and body.get("error", {}).get("code") == "invalid_page_token", body)
    _, health = get("/health")
    check("末页释放事务与连接、会话清空",
          health["pool"]["idle"] == 2
          and health["pagination"]["active"] is False, health)

    # Exact multiple: an empty final page marks the end.
    status, body = post(
        "/paged/query",
        {"sql": "SELECT id FROM fault_logs ORDER BY id",
         "page_size": 7, "timeout_ms": 5000},
    )
    check("页大小=总量时首页仍有更多", status == 200 and body["has_more"], body)
    status, p2 = post(
        "/paged/next", {"page_token": body["page_token"], "timeout_ms": 5000}
    )
    check("下一页 0 行即末页并关闭",
          status == 200 and p2["row_count"] == 0 and not p2["has_more"], p2)

    # Guard rails apply to paged open as well.
    status, body = post(
        "/paged/query",
        {"sql": "DELETE FROM fault_logs", "page_size": 5, "timeout_ms": 5000},
    )
    check("分页开启拒绝写语句(400)",
          status == 400
          and body.get("error", {}).get("code") == "invalid_sql", body)
    status, body = post(
        "/paged/next", {"page_token": "no-such-token", "timeout_ms": 1000}
    )
    check("未知令牌 410", status == 410, body)


def case_paged_snapshot_isolation() -> None:
    admin_dsn = _admin_dsn()
    if not admin_dsn:
        print("  SKIP  快照隔离注入测试（未设置 ADMIN_DSN）")
        return
    try:
        import psycopg
    except ImportError:
        print("  SKIP  快照隔离注入测试（无 psycopg）")
        return

    status, body = post(
        "/paged/query",
        {"sql": "SELECT id FROM fault_logs ORDER BY id",
         "page_size": 2, "timeout_ms": 5000},
    )
    check("开启会话成功", status == 200 and body["has_more"], body)
    token = body["page_token"]
    # A new log arrives AFTER the snapshot was taken.
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO fault_logs (device_id, site, severity, fault_code,"
            " message) VALUES ('INJECTED-NEW','测试','INFO','PROBE','分页期间新入库')"
        )
    try:
        seen = [r[0] for r in body["rows"]]
        while True:
            status, page = post(
                "/paged/next", {"page_token": token, "timeout_ms": 5000}
            )
            check("翻页成功", status == 200, page)
            seen += [r[0] for r in page["rows"]]
            if not page["has_more"]:
                break
        check("新入库行未混入快照（仍为 7 行）",
              seen == [1, 2, 3, 4, 5, 6, 7], seen)
        status, q = post(
            "/query",
            {"sql": "SELECT count(*) FROM fault_logs", "timeout_ms": 5000},
        )
        check("会话外的新查询能看到新入库行", q["rows"][0][0] == 8, q)
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            conn.execute(
                "DELETE FROM fault_logs WHERE device_id = 'INJECTED-NEW'"
            )


def case_paged_single_session() -> None:
    status, a = post(
        "/paged/query",
        {"sql": "SELECT id FROM fault_logs ORDER BY id",
         "page_size": 2, "timeout_ms": 5000},
    )
    token_a = a["page_token"]
    status, b = post(
        "/paged/query",
        {"sql": "SELECT id FROM fault_logs ORDER BY id",
         "page_size": 2, "timeout_ms": 5000},
    )
    check("会话进行中再次开启被拒绝(409)",
          status == 409
          and b.get("error", {}).get("code") == "session_busy", b)
    status, p = post("/paged/next", {"page_token": token_a, "timeout_ms": 5000})
    check("原会话不受影响", status == 200 and p["row_count"] == 2, p)

    status, c = post("/paged/close", {"page_token": token_a})
    check("显式关闭会话", status == 200 and c["status"] == "closed", c)
    status, d = post("/paged/next", {"page_token": token_a, "timeout_ms": 5000})
    check("关闭后令牌失效(410)", status == 410, d)
    _, health = get("/health")
    check("关闭后连接归还", health["pool"]["idle"] == 2, health)

    status, e = post(
        "/paged/query",
        {"sql": "SELECT id FROM fault_logs ORDER BY id",
         "page_size": 5, "timeout_ms": 5000},
    )
    check("关闭后可立即开新会话", status == 200 and e["row_count"] == 5, e)
    post("/paged/close", {"page_token": e["page_token"]})


def case_paged_concurrent_token() -> None:
    status, body = post(
        "/paged/query",
        {"sql": "SELECT id FROM fault_logs ORDER BY id",
         "page_size": 2, "timeout_ms": 5000},
    )
    token = body["page_token"]
    results: list[tuple] = []
    barrier = threading.Barrier(2)

    def nxt() -> None:
        barrier.wait()
        results.append(post("/paged/next",
                            {"page_token": token, "timeout_ms": 8000}))

    threads = [threading.Thread(target=nxt) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    oks = [b for s, b in results if s == 200]
    check("同令牌并发两请求都成功", len(oks) == 2, results)
    pages = {tuple(r[0] for r in b["rows"]) for b in oks}
    check("各拿一页、无重复推进", pages == {(3, 4), (5, 6)}, results)
    # Drain the last page so the session is released.
    status, last = post("/paged/next", {"page_token": token, "timeout_ms": 5000})
    check("收尾页关闭会话", status == 200 and not last["has_more"], last)


def case_paged_fetch_timeout() -> None:
    # Page 1 is instant; fetching the page containing g=2 sleeps 1s.
    sql = ("SELECT g, CASE WHEN g = 2 THEN pg_sleep(1) END "
           "FROM generate_series(1, 4) g")
    status, body = post(
        "/paged/query", {"sql": sql, "page_size": 1, "timeout_ms": 5000}
    )
    check("开启成功(首页快)", status == 200 and body["has_more"], body)
    token = body["page_token"]
    _, before = get("/health")
    discarded_before = before["pool"]["total_discarded"]

    t0 = time.monotonic()
    status, body = post("/paged/next",
                        {"page_token": token, "timeout_ms": 300}, timeout=15)
    elapsed = time.monotonic() - t0
    check("取页超时 504", status == 504
          and body.get("error", {}).get("code") == "query_timeout", body)
    check("回执标明会话终止、令牌失效",
          body.get("session_terminated") is True
          and body.get("token_invalidated") is True, body)
    check("及时返回 (<2.5s)", elapsed < 2.5, f"elapsed={elapsed:.2f}")

    status, again = post("/paged/next",
                         {"page_token": token, "timeout_ms": 5000})
    check("超时后旧令牌失效(410)", status == 410, again)
    time.sleep(0.3)
    _, health = get("/health")
    check("确认事务结束后复用连接、无弃置",
          health["pool"]["idle"] == 2
          and health["pool"]["total_discarded"] == discarded_before, health)
    status, s2 = post(
        "/paged/query",
        {"sql": "SELECT id FROM fault_logs ORDER BY id",
         "page_size": 4, "timeout_ms": 5000},
    )
    check("超时终止后可开新会话", status == 200 and s2["row_count"] == 4, s2)
    post("/paged/close", {"page_token": s2["page_token"]})


def case_paged_open_timeout_and_queue() -> None:
    status, body = post(
        "/paged/query",
        {"sql": "SELECT pg_sleep(3)", "page_size": 5, "timeout_ms": 400},
        timeout=15,
    )
    check("开启超时 504", status == 504
          and body.get("error", {}).get("code") == "query_timeout", body)
    time.sleep(0.3)
    _, health = get("/health")
    check("开启超时后无残留会话且连接恢复",
          health["pagination"]["active"] is False
          and health["pool"]["idle"] == 2, health)

    # Occupy both pooled connections; an open must queue and expire as 503.
    def slow() -> None:
        post("/query", {"sql": "SELECT pg_sleep(2)", "timeout_ms": 6000},
             timeout=15)

    threads = [threading.Thread(target=slow) for _ in range(2)]
    for t in threads:
        t.start()
    time.sleep(0.4)
    t0 = time.monotonic()
    status, body = post(
        "/paged/query",
        {"sql": "SELECT 1", "page_size": 5, "timeout_ms": 400},
        timeout=10,
    )
    waited = time.monotonic() - t0
    check("开启排队过期 503", status == 503
          and body.get("error", {}).get("code") == "acquire_timeout", body)
    check("排队时间计入总期限 (~0.4s)", 0.3 < waited < 1.5,
          f"waited={waited:.2f}")
    for t in threads:
        t.join()
    _, health = get("/health")
    check("排队过期后无残留会话", health["pagination"]["active"] is False,
          health)


def case_paged_idle_expiry() -> None:
    _, health = get("/health")
    idle_ms = health["pagination"]["idle_timeout_ms"]
    status, body = post(
        "/paged/query",
        {"sql": "SELECT id FROM fault_logs ORDER BY id",
         "page_size": 2, "timeout_ms": 5000},
    )
    token = body["page_token"]
    wait_s = idle_ms / 1000.0 + 2.5
    print(f"  INFO  等待闲置回收 ({wait_s:.1f}s, 服务端时限 {idle_ms}ms)...")
    time.sleep(wait_s)
    status, body = post("/paged/next", {"page_token": token, "timeout_ms": 5000})
    check("闲置超时后令牌失效(410)", status == 410, body)
    _, health = get("/health")
    check("闲置回收后事务与连接释放",
          health["pool"]["idle"] == 2
          and health["pagination"]["active"] is False, health)


def case_paged_disconnect() -> None:
    # Fetching the page with g=2 sleeps 30s unless cancelled; the client
    # vanishes mid-fetch and the session must be terminated server-side.
    sql = ("SELECT g, CASE WHEN g = 2 THEN pg_sleep(30) END "
           "FROM generate_series(1, 4) g")
    status, body = post(
        "/paged/query", {"sql": sql, "page_size": 1, "timeout_ms": 5000}
    )
    check("开启成功", status == 200 and body["has_more"], body)
    token = body["page_token"]

    conn = http.client.HTTPConnection("127.0.0.1", 8000, timeout=30)
    payload = json.dumps({"page_token": token, "timeout_ms": 8000})
    conn.request("POST", "/paged/next", body=payload,
                 headers={"Content-Type": "application/json"})
    time.sleep(0.5)
    conn.close()  # abrupt disconnect mid-fetch

    # Fast path: the server notices the disconnect and cancels.  Backstop:
    # the request's own deadline cancels at ~8s.  Either way the token dies.
    deadline = time.monotonic() + 15
    gone = False
    while time.monotonic() < deadline:
        time.sleep(0.5)
        status, _ = post("/paged/next",
                         {"page_token": token, "timeout_ms": 1000})
        if status == 410:
            gone = True
            break
    check("断连后会话被终止、令牌失效", gone)
    _, health = get("/health")
    check("断连恢复后连接归还或补建",
          health["pool"]["idle"] == 2
          and health["pagination"]["active"] is False, health)


def main() -> int:
    print(f"self-test against {BASE}")
    run_case("1. 健康检查与普通查询", case_health_and_basic)
    run_case("2. 只读约束", case_read_only_guards)
    run_case("3. 单条慢查询超时与取消", case_single_timeout)
    run_case("4. 排队期间过期不执行", case_queued_expiry)
    run_case("5. 容量为 2 与接续查询", case_capacity_and_reuse)
    run_case("6. 弃置并补建 / 物理容量", case_discard_and_rebuild)
    run_case("7. 分页快照逐页取数与末页释放", case_paged_happy_path)
    run_case("8. 分页快照隔离(新入库不混入)", case_paged_snapshot_isolation)
    run_case("9. 单会话限制与显式关闭", case_paged_single_session)
    run_case("10. 同令牌并发不重复推进", case_paged_concurrent_token)
    run_case("11. 取页超时终止会话", case_paged_fetch_timeout)
    run_case("12. 开启超时与排队过期", case_paged_open_timeout_and_queue)
    run_case("13. 闲置 30s 自动释放", case_paged_idle_expiry)
    run_case("14. 客户端断连终止会话", case_paged_disconnect)
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
