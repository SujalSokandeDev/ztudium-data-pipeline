"""
Postgres-backed storage adapter for SEODash pipeline artifacts.

It preserves the previous bucket/path model while storing artifact bytes in
the dedicated SEODASH_DATABASE_URL database.
"""

from __future__ import annotations

import json
import logging
import mimetypes
import os
from pathlib import Path
from typing import Iterable

import psycopg2
from psycopg2.extras import Json

logger = logging.getLogger(__name__)


def _database_url() -> str:
    value = os.getenv("SEODASH_DATABASE_URL", "").strip()
    if not value:
        raise RuntimeError("SEODASH_DATABASE_URL must be set for SEODash storage")
    return value


def _connect():
    return psycopg2.connect(_database_url())


def safe_storage_path(path: str) -> str:
    parts = [part for part in path.replace("\\", "/").split("/") if part and part != "."]
    if any(part == ".." for part in parts):
        raise ValueError(f"Unsafe storage path: {path}")
    return "/".join(parts)


def guess_content_type(path: str) -> str:
    guessed, _ = mimetypes.guess_type(path)
    return guessed or "application/octet-stream"


def upload_object(
    bucket: str,
    path: str,
    content: bytes,
    content_type: str | None = None,
    metadata: dict | None = None,
) -> dict:
    path = safe_storage_path(path)
    filename = Path(path).name
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO storage_objects
                  (bucket, path, filename, content_type, size_bytes, content, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (bucket, path) DO UPDATE SET
                  filename = EXCLUDED.filename,
                  content_type = EXCLUDED.content_type,
                  size_bytes = EXCLUDED.size_bytes,
                  content = EXCLUDED.content,
                  metadata = EXCLUDED.metadata,
                  updated_at = NOW()
                RETURNING bucket, path, filename, content_type, size_bytes
                """,
                (
                    bucket,
                    path,
                    filename,
                    content_type or guess_content_type(path),
                    len(content),
                    psycopg2.Binary(content),
                    Json(metadata or {}),
                ),
            )
            row = cur.fetchone()
    return {
        "bucket": row[0],
        "path": row[1],
        "filename": row[2],
        "content_type": row[3],
        "size_bytes": int(row[4] or 0),
    }


def upload_file(bucket: str, file_path: str, storage_path: str | None = None, metadata: dict | None = None) -> dict:
    path = storage_path or os.path.basename(file_path)
    with open(file_path, "rb") as handle:
        content = handle.read()
    return upload_object(bucket, path, content, guess_content_type(path), metadata)


def clear_bucket(bucket: str) -> int:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM storage_objects WHERE bucket = %s", (bucket,))
            return cur.rowcount or 0


def list_objects(
    bucket: str,
    prefix: str = "",
    extensions: Iterable[str] | None = None,
    limit: int = 1000,
) -> list[dict]:
    prefix = safe_storage_path(prefix) if prefix else ""
    normalized_extensions = tuple(ext.lower() for ext in (extensions or ()))
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT path, filename, content_type, size_bytes, metadata, updated_at
                FROM storage_objects
                WHERE bucket = %s AND path LIKE %s
                ORDER BY path ASC
                LIMIT %s
                """,
                (bucket, f"{prefix}%", limit),
            )
            rows = cur.fetchall()

    objects = []
    for path, filename, content_type, size_bytes, metadata, updated_at in rows:
        if normalized_extensions and not path.lower().endswith(normalized_extensions):
            continue
        objects.append(
            {
                "path": path,
                "name": filename or Path(path).name,
                "content_type": content_type,
                "size_bytes": int(size_bytes or 0),
                "metadata": metadata if isinstance(metadata, dict) else json.loads(metadata or "{}"),
                "updated_at": updated_at,
            }
        )
    return objects


def download_object(bucket: str, path: str) -> bytes | None:
    path = safe_storage_path(path)
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT content FROM storage_objects WHERE bucket = %s AND path = %s LIMIT 1",
                (bucket, path),
            )
            row = cur.fetchone()
    if not row:
        return None
    return bytes(row[0])


def download_bucket_files(
    bucket: str,
    date_folder: str,
    temp_dir: str,
    extensions: Iterable[str],
    limit: int = 1000,
) -> list[str]:
    root_items = [
        item
        for item in list_objects(bucket, "", extensions=extensions, limit=limit)
        if "/" not in item["path"]
    ]
    prefix = ""
    items = root_items

    if not items and date_folder:
        prefix = f"{safe_storage_path(date_folder)}/"
        logger.info("No root-level files, trying prefix: %s", prefix)
        items = list_objects(bucket, prefix, extensions=extensions, limit=limit)

    logger.info("Found %d files in storage bucket '%s'", len(items), bucket)
    downloaded = []
    for item in items:
        path = item["path"]
        content = download_object(bucket, path)
        if content is None:
            logger.warning("Failed to download %s", path)
            continue
        name = Path(path).name if prefix else item["name"]
        local_path = os.path.join(temp_dir, name)
        with open(local_path, "wb") as handle:
            handle.write(content)
        downloaded.append(local_path)
        logger.info("  OK  %s (%.2f MB)", name, len(content) / 1024 / 1024)
    return downloaded
