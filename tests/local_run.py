#!/usr/bin/env python3
"""Local-only launcher: run the full stack without Docker.

Boots an ephemeral PostgreSQL 16 via the ``pgserver`` PyPI package, applies
the same schema/role DDL as db/01_init.sql, then starts uvicorn with the
two-connection pool.  Docker Compose remains the canonical deployment;
this script exists so the service can be exercised on machines without
a container runtime.

    python tests/local_run.py
"""
from __future__ import annotations

import os
import pathlib
import sys
import tempfile
import time
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pgserver  # noqa: E402
import psycopg  # noqa: E402
import uvicorn  # noqa: E402

DDL = (ROOT / "db" / "01_init.sql").read_text(encoding="utf-8")


def _with_dbname(dsn: str, dbname: str) -> str:
    """Replace only the database path component of a libpq URL."""
    parts = urlparse(dsn)
    return urlunparse(
        parts._replace(
            path=f"/{dbname}",
            query=urlencode(parse_qsl(parts.query)),
        )
    )


def main() -> None:
    data_dir = tempfile.mkdtemp(prefix="faultdb-")
    print(f"[local_run] starting ephemeral PostgreSQL in {data_dir}")
    pg = pgserver.get_server(data_dir, cleanup_mode="stop")
    admin_dsn = pg.get_uri()
    print(f"[local_run] postgres up: {admin_dsn}")

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE DATABASE faultdb")

    fault_admin_dsn = _with_dbname(admin_dsn, "faultdb")
    with psycopg.connect(fault_admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
    print("[local_run] schema and fault_ro role ready")

    # pgserver authenticates over a unix socket; point the API at the same
    # socket but under the read-only role (password cleared for local use).
    ro_parts = urlparse(fault_admin_dsn)
    ro_dsn = urlunparse(ro_parts._replace(netloc=ro_parts.netloc.replace(
        ro_parts.username or "postgres", "fault_ro", 1
    )))
    with psycopg.connect(fault_admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("ALTER ROLE fault_ro PASSWORD NULL")
    print(f"[local_run] read-only DSN={ro_dsn}")

    os.environ["DATABASE_DSN"] = ro_dsn
    os.environ["POOL_SIZE"] = "2"
    # Exposed for tests/selftest.py's discard/rebuild chaos scenario.
    admin_dsn_path = pathlib.Path(tempfile.gettempdir()) / "faultdb_admin_dsn"
    admin_dsn_path.write_text(fault_admin_dsn, encoding="utf-8")
    print(f"[local_run] admin DSN for self-test written to {admin_dsn_path}")

    print("[local_run] uvicorn on http://127.0.0.1:8000 (Swagger: /docs)")
    try:
        uvicorn.run(
            "app.main:app",
            host="127.0.0.1",
            port=8000,
            log_level="info",
        )
    finally:
        time.sleep(0.2)
        pg.cleanup()


if __name__ == "__main__":
    main()
