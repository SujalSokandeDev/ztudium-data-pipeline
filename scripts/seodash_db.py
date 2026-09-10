"""Small Supabase-style database adapter backed by SEODASH_DATABASE_URL."""

from __future__ import annotations

import json
import os
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg2
from psycopg2.extras import Json, RealDictCursor


class QueryResult:
    def __init__(self, data: list[dict[str, Any]] | None = None):
        self.data = data or []


class PostgresQuery:
    def __init__(self, database_url: str, table: str):
        self.database_url = database_url
        self.table = table
        self.select_clause = "*"
        self.filters: list[tuple[str, str, Any]] = []
        self.orders: list[tuple[str, bool]] = []
        self.row_limit: int | None = None
        self.insert_rows: list[dict[str, Any]] | None = None
        self.update_values: dict[str, Any] | None = None
        self.upsert_rows: list[dict[str, Any]] | None = None
        self.upsert_conflict: str | None = None

    def select(self, select_clause: str):
        self.select_clause = select_clause or "*"
        return self

    def eq(self, column: str, value: Any):
        self.filters.append((column, "=", value))
        return self

    def neq(self, column: str, value: Any):
        self.filters.append((column, "<>", value))
        return self

    def gte(self, column: str, value: Any):
        self.filters.append((column, ">=", value))
        return self

    def gt(self, column: str, value: Any):
        self.filters.append((column, ">", value))
        return self

    def lte(self, column: str, value: Any):
        self.filters.append((column, "<=", value))
        return self

    def lt(self, column: str, value: Any):
        self.filters.append((column, "<", value))
        return self

    def in_(self, column: str, values: list[Any]):
        self.filters.append((column, "IN", values or []))
        return self

    def order(self, column: str, desc: bool = False):
        self.orders.append((column, bool(desc)))
        return self

    def limit(self, value: int):
        self.row_limit = int(value)
        return self

    def insert(self, rows: dict[str, Any] | list[dict[str, Any]]):
        self.insert_rows = rows if isinstance(rows, list) else [rows]
        return self

    def update(self, values: dict[str, Any]):
        self.update_values = values or {}
        return self

    def upsert(self, rows: dict[str, Any] | list[dict[str, Any]], on_conflict: str | None = None):
        self.upsert_rows = rows if isinstance(rows, list) else [rows]
        self.upsert_conflict = on_conflict
        return self

    def execute(self):
        with psycopg2.connect(self.database_url) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if self.insert_rows is not None:
                    return self._execute_insert(cur)
                if self.upsert_rows is not None:
                    return self._execute_upsert(cur)
                if self.update_values is not None:
                    return self._execute_update(cur)
                return self._execute_select(cur)

    def _execute_select(self, cur):
        params: list[Any] = []
        where_sql = self._where_sql(params)
        order_sql = ""
        if self.orders:
            order_sql = " ORDER BY " + ", ".join(
                f"{self._ident(column)} {'DESC' if desc else 'ASC'}" for column, desc in self.orders
            )
        limit_sql = ""
        if self.row_limit:
            limit_sql = " LIMIT %s"
            params.append(self.row_limit)
        cur.execute(
            f"SELECT {self._select_sql()} FROM {self._ident(self.table)}{where_sql}{order_sql}{limit_sql}",
            params,
        )
        return QueryResult([self._normalize_row(dict(row)) for row in cur.fetchall()])

    def _execute_insert(self, cur):
        rows = [row for row in (self.insert_rows or []) if row]
        if not rows:
            return QueryResult([])
        columns = list(rows[0].keys())
        sql = (
            f"INSERT INTO {self._ident(self.table)} ({', '.join(self._ident(column) for column in columns)}) "
            f"VALUES ({', '.join(['%s'] * len(columns))}) RETURNING *"
        )
        data = []
        for row in rows:
            cur.execute(sql, [self._adapt_value(row.get(column)) for column in columns])
            data.extend(self._normalize_row(dict(item)) for item in cur.fetchall())
        return QueryResult(data)

    def _execute_update(self, cur):
        if not self.update_values:
            return QueryResult([])
        params = [self._adapt_value(value) for value in self.update_values.values()]
        assignments = ", ".join(f"{self._ident(column)} = %s" for column in self.update_values)
        where_sql = self._where_sql(params)
        cur.execute(f"UPDATE {self._ident(self.table)} SET {assignments}{where_sql} RETURNING *", params)
        return QueryResult([self._normalize_row(dict(row)) for row in cur.fetchall()])

    def _execute_upsert(self, cur):
        rows = [row for row in (self.upsert_rows or []) if row]
        if not rows:
            return QueryResult([])
        columns = list(rows[0].keys())
        conflict_columns = [column.strip() for column in (self.upsert_conflict or columns[0]).split(",") if column.strip()]
        update_columns = [column for column in columns if column not in conflict_columns]
        conflict_sql = "DO NOTHING"
        if update_columns:
            update_sql = ", ".join(f"{self._ident(column)} = EXCLUDED.{self._ident(column)}" for column in update_columns)
            conflict_sql = f"DO UPDATE SET {update_sql}"
        sql = (
            f"INSERT INTO {self._ident(self.table)} ({', '.join(self._ident(column) for column in columns)}) "
            f"VALUES ({', '.join(['%s'] * len(columns))}) "
            f"ON CONFLICT ({', '.join(self._ident(column) for column in conflict_columns)}) {conflict_sql} RETURNING *"
        )
        data = []
        for row in rows:
            cur.execute(sql, [self._adapt_value(row.get(column)) for column in columns])
            data.extend(self._normalize_row(dict(item)) for item in cur.fetchall())
        return QueryResult(data)

    def _where_sql(self, params: list[Any]) -> str:
        if not self.filters:
            return ""
        clauses = []
        for column, operator, value in self.filters:
            if operator == "IN":
                if not value:
                    clauses.append("FALSE")
                    continue
                clauses.append(f"{self._ident(column)} = ANY(%s)")
                params.append(list(value))
                continue
            clauses.append(f"{self._ident(column)} {operator} %s")
            params.append(self._adapt_value(value))
        return " WHERE " + " AND ".join(clauses)

    def _select_sql(self) -> str:
        if self.select_clause.strip() == "*":
            return "*"
        return ", ".join(self._ident(part.strip()) for part in self.select_clause.split(",") if part.strip())

    @staticmethod
    def _ident(value: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(value)):
            raise ValueError(f"Unsafe SQL identifier: {value}")
        return f'"{value}"'

    @staticmethod
    def _adapt_value(value: Any):
        if isinstance(value, (dict, list)):
            return Json(value, dumps=lambda obj: json.dumps(obj, default=str))
        return value

    @classmethod
    def _normalize_row(cls, row: dict[str, Any]) -> dict[str, Any]:
        return {key: cls._normalize_value(value) for key, value in row.items()}

    @classmethod
    def _normalize_value(cls, value: Any) -> Any:
        if isinstance(value, Decimal):
            return int(value) if value == value.to_integral_value() else float(value)
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, dict):
            return {key: cls._normalize_value(item) for key, item in value.items()}
        if isinstance(value, list):
            return [cls._normalize_value(item) for item in value]
        return value


class PostgresClient:
    def __init__(self, database_url: str):
        self.database_url = database_url

    def table(self, table: str) -> PostgresQuery:
        return PostgresQuery(self.database_url, table)


def create_seodash_client():
    database_url = os.getenv("SEODASH_DATABASE_URL", "").strip()
    if database_url:
        return PostgresClient(database_url)

    from supabase import create_client

    supabase_url = os.getenv("SUPABASE_URL", "")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY", "")
    if not supabase_url or not supabase_key:
        raise RuntimeError("SEODASH_DATABASE_URL must be set, or legacy SUPABASE_URL/SUPABASE_SERVICE_KEY must be available")
    return create_client(supabase_url, supabase_key)
