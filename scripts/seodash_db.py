"""
Small Postgres helper for SEODash pipeline scripts.

The pipeline now writes to the dedicated SEODASH_DATABASE_URL database. A few
scripts still keep Supabase fallback paths for local compatibility, but GitHub
Actions should prefer these helpers whenever SEODASH_DATABASE_URL is present.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values


def database_url() -> str:
    return os.getenv("SEODASH_DATABASE_URL", "").strip()


def enabled() -> bool:
    return bool(database_url())


def connect():
    value = database_url()
    if not value:
        raise RuntimeError("SEODASH_DATABASE_URL must be set")
    return psycopg2.connect(value)


def normalize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return value
    return value


def adapt_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return Json(value)
    return value


def normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: normalize_value(value) for key, value in row.items()}


def table_columns(table: str) -> set[str]:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                """,
                (table,),
            )
            return {row[0] for row in cur.fetchall()}


def supports_column(table: str, column: str) -> bool:
    return column in table_columns(table)


def filter_supported_columns(table: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return rows
    columns = table_columns(table)
    return [{key: value for key, value in row.items() if key in columns} for row in rows]


def upsert_rows(
    table: str,
    rows: list[dict[str, Any]],
    conflict_cols: str | list[str],
    chunk_size: int = 500,
    filter_columns: bool = True,
) -> int:
    if not rows:
        return 0
    if isinstance(conflict_cols, str):
        conflict = [col.strip() for col in conflict_cols.split(",") if col.strip()]
    else:
        conflict = list(conflict_cols)

    clean_rows = [normalize_row(row) for row in rows]
    if filter_columns:
        clean_rows = filter_supported_columns(table, clean_rows)
    clean_rows = [row for row in clean_rows if all(row.get(col) is not None for col in conflict)]
    if not clean_rows:
        return 0

    all_keys = sorted({key for row in clean_rows for key in row.keys()})
    update_keys = [key for key in all_keys if key not in set(conflict) and key != "id"]
    columns_sql = ", ".join(f'"{key}"' for key in all_keys)
    conflict_sql = ", ".join(f'"{key}"' for key in conflict)
    if update_keys:
        update_sql = ", ".join(f'"{key}" = EXCLUDED."{key}"' for key in update_keys)
        on_conflict = f"DO UPDATE SET {update_sql}"
    else:
        on_conflict = "DO NOTHING"
    sql = f'INSERT INTO "{table}" ({columns_sql}) VALUES %s ON CONFLICT ({conflict_sql}) {on_conflict}'
    values = [tuple(adapt_value(row.get(key)) for key in all_keys) for row in clean_rows]

    with connect() as conn:
        with conn.cursor() as cur:
            for start in range(0, len(values), chunk_size):
                execute_values(cur, sql, values[start:start + chunk_size], page_size=chunk_size)
        conn.commit()
    return len(clean_rows)


def insert_rows(table: str, rows: list[dict[str, Any]], returning: str | None = None) -> list[dict[str, Any]]:
    if not rows:
        return []
    clean_rows = filter_supported_columns(table, [normalize_row(row) for row in rows])
    all_keys = sorted({key for row in clean_rows for key in row.keys()})
    columns_sql = ", ".join(f'"{key}"' for key in all_keys)
    returning_sql = f" RETURNING {returning}" if returning else ""
    sql = f'INSERT INTO "{table}" ({columns_sql}) VALUES %s{returning_sql}'
    values = [tuple(adapt_value(row.get(key)) for key in all_keys) for row in clean_rows]

    with connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            execute_values(cur, sql, values, page_size=500)
            result = [dict(row) for row in cur.fetchall()] if returning else []
        conn.commit()
    return [normalize_row(row) for row in result]


def update_where(table: str, payload: dict[str, Any], filters: dict[str, Any]) -> int:
    clean_payload = filter_supported_columns(table, [normalize_row(payload)])[0]
    if not clean_payload or not filters:
        return 0
    set_sql = ", ".join(f'"{key}" = %s' for key in clean_payload)
    where_sql = " AND ".join(f'"{key}" = %s' for key in filters)
    values = [adapt_value(value) for value in clean_payload.values()] + [adapt_value(value) for value in filters.values()]
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f'UPDATE "{table}" SET {set_sql} WHERE {where_sql}', values)
            count = cur.rowcount or 0
        conn.commit()
    return count


def delete_where(table: str, filters: dict[str, Any]) -> int:
    if not filters:
        return 0
    where_sql = " AND ".join(f'"{key}" = %s' for key in filters)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(f'DELETE FROM "{table}" WHERE {where_sql}', list(filters.values()))
            count = cur.rowcount or 0
        conn.commit()
    return count


def select_rows(
    table: str,
    columns: str = "*",
    filters: dict[str, Any] | None = None,
    in_filters: dict[str, list[Any]] | None = None,
    order_by: str | None = None,
    desc: bool = False,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    where_parts = []
    values: list[Any] = []
    for key, value in (filters or {}).items():
        where_parts.append(f'"{key}" = %s')
        values.append(value)
    for key, items in (in_filters or {}).items():
        if not items:
            return []
        where_parts.append(f'"{key}" = ANY(%s)')
        values.append(items)
    sql = f'SELECT {columns} FROM "{table}"'
    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)
    if order_by:
        sql += f' ORDER BY "{order_by}" {"DESC" if desc else "ASC"}'
    if limit:
        sql += " LIMIT %s"
        values.append(limit)
    with connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, values)
            return [normalize_row(dict(row)) for row in cur.fetchall()]


def start_ingestion_run(source: str, websites_attempted: list[str]) -> str:
    run_id = str(uuid.uuid4())
    insert_rows(
        "ingestion_runs",
        [{
            "id": run_id,
            "source": source,
            "status": "running",
            "websites_attempted": websites_attempted,
        }],
    )
    return run_id


def finish_ingestion_run(
    run_id: str | None,
    status: str,
    websites_succeeded: list[str],
    websites_failed: list[str],
    error_details: dict[str, Any],
    duration_seconds: int,
) -> None:
    if not run_id:
        return
    update_where(
        "ingestion_runs",
        {
            "status": status,
            "websites_succeeded": websites_succeeded,
            "websites_failed": websites_failed,
            "error_details": error_details,
            "duration_seconds": duration_seconds,
            "completed_at": datetime.utcnow().isoformat(),
        },
        {"id": run_id},
    )


def start_pipeline_run(pipeline: str) -> str:
    run_id = str(uuid.uuid4())
    insert_rows("pipeline_runs", [{"id": run_id, "pipeline": pipeline, "status": "running"}])
    return run_id


def finish_pipeline_run(run_id: str | None, status: str, duration_seconds: int, error_details: dict[str, Any] | None = None) -> None:
    if not run_id:
        return
    update_where(
        "pipeline_runs",
        {
            "status": "completed" if status in {"success", "partial"} else "failed",
            "completed_at": datetime.utcnow().isoformat(),
            "error_message": None if status in {"success", "partial"} else json.dumps(error_details or {})[:1000],
            "details": {"duration_seconds": duration_seconds},
        },
        {"id": run_id},
    )
