"""Run SQL migrations in database/migrations against SUPABASE_DB_URL."""

from __future__ import annotations

import os
from pathlib import Path

import psycopg2


ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = ROOT / "database" / "migrations"


def main() -> None:
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL is not set")

    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not files:
        raise RuntimeError(f"No migration files found in {MIGRATIONS_DIR}")

    print(f"Applying {len(files)} migrations to {db_url.split('@')[-1]}")

    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            for migration in files:
                sql = migration.read_text(encoding="utf-8")
                print(f"  -> {migration.name}")
                cur.execute(sql)
        conn.commit()

    print("Migrations complete.")


if __name__ == "__main__":
    main()
