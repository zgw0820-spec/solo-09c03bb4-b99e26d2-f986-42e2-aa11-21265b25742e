# 机房设备故障日志只读查询 API

值班分析员共用的**只读 SQL 入口**：固定两条 PostgreSQL 连接，提交单条
`SELECT` 与**总截止时间**，排队等待也计入预算；超时立即取消数据库语句，
确认事务结束才复用连接，无法确认则弃置连接并补建；迟到结果不会改写超时
回执。后续查询始终拿到属于自己的结果。

新增**分页快照**接口：开启会话时提交单条 `SELECT` 与页大小，首页响应附
取页令牌；后续各页沿用**同一数据库快照**（`REPEATABLE READ` 事务 +
服务端游标），逐页 `FETCH`，新入库日志不会混入，API 不会一次加载全部
结果。同一时刻最多一个分页会话，另一条连接继续服务普通查询；末页、
显式关闭、超时、断连或闲置 30 秒都会释放事务与连接，旧令牌立即失效。

## 技术栈

- FastAPI（Swagger UI：`/docs`，ReDoc：`/redoc`）
- psycopg 3（异步连接 + libpq `PQcancel` + 服务端游标）
- PostgreSQL 16
- Docker Compose 编排，预置 7 条故障日志与只读角色 `fault_ro`

## 快速开始（Docker Compose）

```bash
docker compose up --build
# API:    http://localhost:8000
# Swagger: http://localhost:8000/docs
```

自测（另开终端，容器内或本机均可）：

```bash
docker compose exec api python -m tests.selftest
```

## 接口

### `POST /query`

```json
{
  "sql": "SELECT id, device_id, severity FROM fault_logs LIMIT 5",
  "timeout_ms": 2000
}
```

`timeout_ms` 是**包含排队时间**的总预算（单调时钟），从请求进入即计时。

| 场景 | HTTP | 回执要点 |
| --- | --- | --- |
| 成功 | 200 | `status=ok`，`rows` 为本次查询自身结果，附 `wait_ms`/`execution_ms` |
| 执行超时 | 504 | `error.code=query_timeout`，`status=timeout`，`cancelled`、`connection_discarded`、`late_result_ignored` |
| 排队期间预算耗尽 | 503 | `error.code=acquire_timeout`，语句**从未发送**到数据库 |
| SQL 不合法 | 400 | `error.code=invalid_sql` |

### `POST /paged/query` — 开启分页快照会话

```json
{
  "sql": "SELECT id, device_id, severity, occurred_at FROM fault_logs ORDER BY id",
  "page_size": 3,
  "timeout_ms": 2000
}
```

在 `REPEATABLE READ READ ONLY` 事务中声明服务端游标并取回首页：

```json
{
  "status": "ok",
  "has_more": true,
  "page_token": "9x...",
  "row_count": 3,
  "rows": [[1, "SW-CORE-01", "CRITICAL", "..."], ...],
  "wait_ms": 0.4,
  "execution_ms": 3.1,
  "page_number": 1
}
```

- 快照在会话开启时固定：之后新入库的日志**不会**出现在任何后续页。
- 逐页 `FETCH FORWARD n`，服务端不物化全量结果，API 不一次加载全部行。
- 同一时刻仅允许一个会话；会话进行中再次开启返回 **409** `session_busy`。
- 若首页即末页（返回行数 < 页大小），会话不保留、不发放令牌。
- 建议 `ORDER BY` 主键以获得稳定的页边界（正确性不依赖它：游标本身
  保证每行恰好交付一次）。

### `POST /paged/next` — 取下一页

```json
{ "page_token": "9x...", "timeout_ms": 2000 }
```

- 每个请求自带总期限；等待会话占用锁的时间也计入本次预算（503）。
- 同一令牌的并发请求被**串行化**：各自取走相继的一页，不会重复推进、
  不会跳过或重复交付任何一页。
- 返回行数少于页大小（含 0 行）即为末页：事务与连接立即释放，
  `page_token=null`，旧令牌失效（再取返回 **410** `invalid_page_token`）。
- 取页超时返回 **504** 且 `session_terminated=true`、`token_invalidated=true`。

### `POST /paged/close` — 提前关闭会话

```json
{ "page_token": "9x..." }
```

回滚事务、确认连接空闲后归还连接池，令牌失效。

### `GET /health`

返回连接池容量、空闲/占用数、累计创建与弃置数，以及分页会话状态
（`pagination.active`/`idle_ms`/`idle_timeout_ms` 等）。

## 关键设计

### 1. 容量恒为 2，排队计入总预算

`QueryConnectionPool` 用容量为 2 的信号量限制**物理后端数**，第三个请求
排队等待而非新开连接。`acquire(deadline)` 接收绝对单调时钟截止时间，
等待与执行共用同一预算；排队期间过期则直接返回 503，不发送任何语句。
分页会话从池中借走一条连接并持有整个会话期，剩余一条继续服务 `/query`。

### 2. 单条只读 SELECT 的三层防护

1. `app/validation.py` 仅允许单条 `SELECT`/`WITH`/`TABLE`/`VALUES`，拒绝
   多语句（额外分号）、写关键字、`SELECT ... INTO` 等。
2. 每个请求在显式 `BEGIN READ ONLY` 事务中执行，会话默认
   `default_transaction_read_only=on`，并设置局部 `statement_timeout` 兜底。
3. 数据库角色 `fault_ro` 仅有 `SELECT` 权限，且角色默认只读；直连也无法
   写入（`InsufficientPrivilege`）。

### 3. 超时处理协议（`app/executor.py`，分页取页复用同一协议）

1. 语句作为 **shielded task** 运行，超时回执一旦确定就不会被迟到结果覆盖。
2. 截止时间到，调用 libpq `PQcancel`（经独立取消连接）请求服务器中止语句。
3. `await` 该 shielded 任务直至真正结束（收到 `QueryCanceled` 或拿到迟到
   结果），杜绝协程残留。
4. 执行 `ROLLBACK` 并核对 `transaction_status == IDLE`，**确认事务结束**才
   把连接放回池；无法确认则关闭弃置并补建，保持容量为 2。
5. 迟到完成的结果只做审计日志，回执固定为 504 且不携带 `rows`。

### 4. 分页快照会话（`app/paging.py`）

- **快照隔离**：`BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` 后先
  `SELECT 1` 固定快照，再 `DECLARE <cur> NO SCROLL CURSOR FOR <sql>`；
  每页 `FETCH FORWARD n`。会话期间新入库行对游标不可见。
- **单会话**：开启前经门闩检查，已有活跃会话（或正在开启）即 409。
- **令牌**：`secrets.token_urlsafe` 随机令牌，响应首页发放；末页、显式
  关闭、超时、断连、数据库故障、闲置 30s（`PAGED_IDLE_TIMEOUT_MS`）后
  立即失效（410）。
- **并发串行化**：同令牌请求经会话级互斥锁串行执行，锁等待计入本次
  总期限；每次取页恰好推进一次。
- **断连处理**：端点轮询 ASGI 断连通道，客户端断开即走与超时相同的
  `PQcancel` + 终止会话协议；另有 `statement_timeout` 兜底，保证会话
  终将终止。
- **闲置回收**：后台任务每秒扫描，闲置超限时回滚事务、确认 IDLE 后
  归还连接；无法确认则弃置补建。

### 5. 连接级故障的弃置补建与无感重试

后端被带外终止（如 `pg_terminate_backend`）时，连接级错误触发弃置并由池
补建；只读语句在同一总预算内安全重试一次，客户端无感知（分页开启同样
重试一次；会话建立后快照无法重建，故障即终止会话并失效令牌）。若补建
暂时失败，池临时缩水并在下一次获取时懒建补回，不会死锁。

## 无 Docker 的本地运行与自测

本机若无容器运行时，可用 `pgserver`（PyPI 上的免 root PostgreSQL 16）：

```bash
pip install -r requirements.txt pgserver
python tests/local_run.py        # 拉起 PG16 + uvicorn
python -m tests.selftest         # 端到端自测（92 项，含分页 8 组场景）
python tests/test_protocol.py    # 超时/迟到结果/排队/分页协议直测（32 项）
```

自测的第 13 组会真实等待闲置回收（默认约 32s）；若以
`PAGED_IDLE_TIMEOUT_MS=3000` 启动服务，自测会按 `/health` 报告的时限
自动缩短等待。

## 目录结构

```
app/            FastAPI 应用（config/pool/executor/paging/validation/schemas/main）
db/01_init.sql  建表、预置日志、只读角色（Compose 首次初始化执行）
tests/          selftest.py（HTTP 端到端）、test_protocol.py（协议直测）、
                local_run.py（免 Docker 启动器）
Dockerfile / docker-compose.yml / requirements.txt
```
