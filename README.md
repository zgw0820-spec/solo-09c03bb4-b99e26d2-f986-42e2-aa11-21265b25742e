# 机房设备故障日志只读查询 API

值班分析员共用的**只读 SQL 入口**：固定两条 PostgreSQL 连接，提交单条
`SELECT` 与**总截止时间**，排队等待也计入预算；超时立即取消数据库语句，
确认事务结束才复用连接，无法确认则弃置连接并补建；迟到结果不会改写超时
回执。后续查询始终拿到属于自己的结果。

## 技术栈

- FastAPI（Swagger UI：`/docs`，ReDoc：`/redoc`）
- psycopg 3（异步连接 + libpq `PQcancel`）
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

### `GET /health`

返回连接池容量、空闲/占用数、累计创建与弃置数。

## 关键设计

### 1. 容量恒为 2，排队计入总预算

`QueryConnectionPool` 用容量为 2 的信号量限制**物理后端数**，第三个请求
排队等待而非新开连接。`acquire(deadline)` 接收绝对单调时钟截止时间，
等待与执行共用同一预算；排队期间过期则直接返回 503，不发送任何语句。

### 2. 单条只读 SELECT 的三层防护

1. `app/validation.py` 仅允许单条 `SELECT`/`WITH`/`TABLE`/`VALUES`，拒绝
   多语句（额外分号）、写关键字、`SELECT ... INTO` 等。
2. 每个请求在显式 `BEGIN READ ONLY` 事务中执行，会话默认
   `default_transaction_read_only=on`，并设置局部 `statement_timeout` 兜底。
3. 数据库角色 `fault_ro` 仅有 `SELECT` 权限，且角色默认只读；直连也无法
   写入（`InsufficientPrivilege`）。

### 3. 超时处理协议（`app/executor.py`）

1. 语句作为 **shielded task** 运行，超时回执一旦确定就不会被迟到结果覆盖。
2. 截止时间到，调用 libpq `PQcancel`（经独立取消连接）请求服务器中止语句。
3. `await` 该 shielded 任务直至真正结束（收到 `QueryCanceled` 或拿到迟到
   结果），杜绝协程残留。
4. 执行 `ROLLBACK` 并核对 `transaction_status == IDLE`，**确认事务结束**才
   把连接放回池；无法确认则关闭弃置并补建，保持容量为 2。
5. 迟到完成的结果只做审计日志，回执固定为 504 且不携带 `rows`。

### 4. 连接级故障的弃置补建与无感重试

后端被带外终止（如 `pg_terminate_backend`）时，连接级错误触发弃置并由池
补建；只读语句在同一总预算内安全重试一次，客户端无感知。若补建暂时失败，
池临时缩水并在下一次获取时懒建补回，不会死锁。

## 无 Docker 的本地运行与自测

本机若无容器运行时，可用 `pgserver`（PyPI 上的免 root PostgreSQL 16）：

```bash
pip install -r requirements.txt pgserver
python tests/local_run.py        # 拉起 PG16 + uvicorn
python -m tests.selftest         # 端到端自测（46 项）
python tests/test_protocol.py    # 超时/迟到结果/排队协议直测（11 项）
```

## 目录结构

```
app/            FastAPI 应用（config/pool/executor/validation/schemas/main）
db/01_init.sql  建表、预置日志、只读角色（Compose 首次初始化执行）
tests/          selftest.py（HTTP 端到端）、test_protocol.py（协议直测）、
                local_run.py（免 Docker 启动器）
Dockerfile / docker-compose.yml / requirements.txt
```
