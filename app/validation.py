"""Validation of analyst-submitted SQL.

The entry point is strictly read-only.  Defence in depth is applied on three
layers:

1. This module accepts only a *single* ``SELECT`` statement (no semicolon
   batches, no data-modifying commands, no ``SELECT ... INTO``).
2. Every pooled session is opened with ``default_transaction_read_only=on``
   and each request runs inside an explicit ``BEGIN READ ONLY`` transaction.
3. The database login role (``fault_ro``) has no write privileges.
"""
from __future__ import annotations

import re

from .errors import InvalidSqlError

# A trailing semicolon followed only by whitespace is harmless and allowed;
# any other semicolon means more than one statement -> reject.
_TRAILING_SEMICOLON = re.compile(r";\s*$")
_SEMICOLON = re.compile(r";")
_KEYWORDS = re.compile(
    r"\b("
    r"INSERT|UPDATE|DELETE|MERGE|TRUNCATE|DROP|ALTER|CREATE|REPLACE|"
    r"GRANT|REVOKE|COMMENT|REFRESH|COPY|VACUUM|REINDEX|CLUSTER|"
    r"CALL|DO|LOCK|PREPARE|EXECUTE|DISCARD|SET|RESET|LISTEN|NOTIFY|"
    r"INTO|TEMP|TEMPORARY|UNLOGGED|MATERIALIZED|SECURITY|EXTENSION|"
    r"EVENT|TRIGGER|RULE|POLICY|PUBLICATION|SUBSCRIPTION"
    r")\b",
    re.IGNORECASE,
)
# WITH ... SELECT is fine; WITH ... INSERT/UPDATE/DELETE is not.  The keyword
# check above already catches the latter, this is kept for readable errors.
_START_SELECT = re.compile(r"^\s*(SELECT|WITH|TABLE|VALUES)\b", re.IGNORECASE)
_COMMENT_LINE = re.compile(r"--[^\n]*")
_COMMENT_BLOCK = re.compile(r"/\*.*?\*/", re.DOTALL)


def _strip_comments(sql: str) -> str:
    sql = _COMMENT_BLOCK.sub(" ", sql)
    sql = _COMMENT_LINE.sub(" ", sql)
    return sql


def validate_single_select(sql: str, max_chars: int) -> str:
    """Return the normalised SQL or raise :class:`InvalidSqlError`."""
    if not isinstance(sql, str):
        raise InvalidSqlError("sql 必须是字符串")
    if not sql.strip():
        raise InvalidSqlError("sql 不能为空")
    if len(sql) > max_chars:
        raise InvalidSqlError(f"SQL 超过 {max_chars} 字符上限")

    visible = _strip_comments(sql).strip()

    if not _START_SELECT.match(visible):
        raise InvalidSqlError("只允许单条 SELECT（或以 WITH 开头的只读查询）语句")

    # Normalise: allow exactly one trailing semicolon, reject any other.
    without_trailing = _TRAILING_SEMICOLON.sub("", visible).rstrip()
    if _SEMICOLON.search(without_trailing):
        raise InvalidSqlError("禁止在一个请求中提交多条语句（检测到额外的分号）")

    match = _KEYWORDS.search(without_trailing)
    if match:
        raise InvalidSqlError(
            f"检测到禁止的关键字 {match.group(1).upper()}，本入口仅允许只读 SELECT"
        )
    return without_trailing
