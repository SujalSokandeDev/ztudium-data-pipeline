"""
Summarize and prune granular SEODash rows older than the retention window.

Default behavior is dry-run. Live mode requires DRY_RUN=false.

Daily metrics are intentionally excluded and kept long-term.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import Json, RealDictCursor

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("data_retention_cleanup")


@dataclass(frozen=True)
class RetentionTable:
    table: str
    summary_table: str
    date_column: str
    website_column: str
    metrics_sql: str


TABLES = [
    RetentionTable(
        table="website_pages",
        summary_table="website_pages_summary",
        date_column="date",
        website_column="website",
        metrics_sql="""
          jsonb_build_object(
            'total_clicks', COALESCE(SUM(clicks), 0),
            'total_impressions', COALESCE(SUM(impressions), 0),
            'avg_ctr', ROUND(AVG(ctr)::numeric, 4),
            'avg_position', ROUND(AVG(position)::numeric, 4),
            'total_ga_sessions', COALESCE(SUM(ga_sessions), 0),
            'total_ga_pageviews', COALESCE(SUM(ga_pageviews), 0),
            'total_ahrefs_traffic', COALESCE(SUM(traffic_ahrefs), 0),
            'avg_keywords_count', ROUND(AVG(keywords_count)::numeric, 4),
            'distinct_urls', COUNT(DISTINCT url)
          )
        """,
    ),
    RetentionTable(
        table="website_keywords",
        summary_table="website_keywords_summary",
        date_column="date",
        website_column="website",
        metrics_sql="""
          jsonb_build_object(
            'total_clicks', COALESCE(SUM(clicks), 0),
            'total_impressions', COALESCE(SUM(impressions), 0),
            'avg_ctr', ROUND(AVG(ctr)::numeric, 4),
            'avg_position', ROUND(AVG(position)::numeric, 4),
            'avg_search_volume', ROUND(AVG(search_volume)::numeric, 4),
            'avg_keyword_difficulty', ROUND(AVG(keyword_difficulty)::numeric, 4),
            'total_traffic_estimate', COALESCE(SUM(traffic_estimate), 0),
            'distinct_keywords', COUNT(DISTINCT keyword)
          )
        """,
    ),
    RetentionTable(
        table="ahrefs_overview",
        summary_table="ahrefs_overview_summary",
        date_column="date",
        website_column="website",
        metrics_sql="""
          jsonb_build_object(
            'avg_dr', ROUND(AVG(dr)::numeric, 4),
            'avg_ur', ROUND(AVG(ur)::numeric, 4),
            'avg_ahrefs_rank', ROUND(AVG(ahrefs_rank)::numeric, 4),
            'max_top3_keywords', MAX(top3_keywords),
            'total_ai_overview', COALESCE(SUM(ai_overview), 0),
            'total_ai_chatgpt', COALESCE(SUM(ai_chatgpt), 0),
            'total_ai_perplexity', COALESCE(SUM(ai_perplexity), 0),
            'total_ai_gemini', COALESCE(SUM(ai_gemini), 0),
            'total_ai_copilot', COALESCE(SUM(ai_copilot), 0)
          )
        """,
    ),
    RetentionTable(
        table="ahrefs_referring_domains",
        summary_table="ahrefs_referring_domains_summary",
        date_column="date",
        website_column="website",
        metrics_sql="""
          jsonb_build_object(
            'distinct_domains', COUNT(DISTINCT domain),
            'avg_dr', ROUND(AVG(dr)::numeric, 4),
            'spam_domains', COUNT(*) FILTER (WHERE is_spam IS TRUE),
            'total_dofollow_links', COALESCE(SUM(dofollow_links), 0),
            'total_links_to_target', COALESCE(SUM(links_to_target), 0)
          )
        """,
    ),
    RetentionTable(
        table="ahrefs_broken_backlinks",
        summary_table="ahrefs_broken_backlinks_summary",
        date_column="date",
        website_column="website",
        metrics_sql="""
          jsonb_build_object(
            'distinct_referring_pages', COUNT(DISTINCT referring_page),
            'distinct_target_urls', COUNT(DISTINCT target_url),
            'avg_ref_domain_dr', ROUND(AVG(ref_domain_dr)::numeric, 4),
            'pending_count', COUNT(*) FILTER (WHERE validation_status = 'pending'),
            'resolved_count', COUNT(*) FILTER (WHERE validation_status = 'resolved'),
            'confirmed_broken_count', COUNT(*) FILTER (WHERE validation_status = 'confirmed_broken'),
            'needs_review_count', COUNT(*) FILTER (WHERE validation_status = 'needs_review')
          )
        """,
    ),
    RetentionTable(
        table="ahrefs_lost_backlinks",
        summary_table="ahrefs_lost_backlinks_summary",
        date_column="lost_date",
        website_column="website",
        metrics_sql="""
          jsonb_build_object(
            'distinct_referring_pages', COUNT(DISTINCT referring_page_url),
            'distinct_target_urls', COUNT(DISTINCT target_url),
            'avg_domain_rating', ROUND(AVG(domain_rating)::numeric, 4),
            'pending_count', COUNT(*) FILTER (WHERE validation_status = 'pending'),
            'resolved_count', COUNT(*) FILTER (WHERE validation_status = 'resolved'),
            'confirmed_broken_count', COUNT(*) FILTER (WHERE validation_status = 'confirmed_broken'),
            'needs_review_count', COUNT(*) FILTER (WHERE validation_status = 'needs_review')
          )
        """,
    ),
    RetentionTable(
        table="ahrefs_competitors",
        summary_table="ahrefs_competitors_summary",
        date_column="date",
        website_column="website",
        metrics_sql="""
          jsonb_build_object(
            'distinct_competitors', COUNT(DISTINCT competitor_domain),
            'avg_keyword_overlap', ROUND(AVG(keyword_overlap)::numeric, 4),
            'avg_competitor_keywords', ROUND(AVG(competitor_keywords)::numeric, 4)
          )
        """,
    ),
    RetentionTable(
        table="content_gap_keywords",
        summary_table="content_gap_keywords_summary",
        date_column="date",
        website_column="website",
        metrics_sql="""
          jsonb_build_object(
            'distinct_keywords', COUNT(DISTINCT keyword),
            'total_volume', COALESCE(SUM(volume), 0),
            'avg_kd', ROUND(AVG(kd)::numeric, 4),
            'avg_cpc', ROUND(AVG(cpc)::numeric, 4),
            'avg_opportunity_score', ROUND(AVG(opportunity_score)::numeric, 4),
            'easy_win_count', COUNT(*) FILTER (WHERE is_easy_win IS TRUE),
            'distinct_clusters', COUNT(DISTINCT cluster)
          )
        """,
    ),
]


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def connect():
    database_url = os.getenv("SEODASH_DATABASE_URL", "")
    if not database_url:
        raise RuntimeError("SEODASH_DATABASE_URL is required")
    return psycopg2.connect(database_url)


def month_period_expression(date_column: str) -> str:
    quoted = quote(date_column)
    return f"date_trunc('month', {quoted})::date"


def find_periods(conn, table: RetentionTable, retention_days: int, limit: int | None = None) -> list[dict[str, Any]]:
    limit_sql = " LIMIT %s" if limit else ""
    params: list[Any] = [retention_days]
    if limit:
        params.append(limit)

    sql = f"""
      WITH cutoff AS (
        SELECT (CURRENT_DATE - (%s::int * INTERVAL '1 day'))::date AS cutoff_date
      )
      SELECT
        {quote(table.website_column)} AS website,
        {month_period_expression(table.date_column)} AS period_start,
        LEAST(
          ({month_period_expression(table.date_column)} + INTERVAL '1 month - 1 day')::date,
          (SELECT cutoff_date - 1 FROM cutoff)
        ) AS period_end,
        COUNT(*)::int AS source_row_count
      FROM {quote(table.table)}
      WHERE {quote(table.date_column)} < (SELECT cutoff_date FROM cutoff)
      GROUP BY 1, 2, 3
      ORDER BY 2 ASC, 1 ASC
      {limit_sql}
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]


def build_summary(conn, table: RetentionTable, website: str, period_start: date, period_end: date) -> dict[str, Any]:
    sql = f"""
      SELECT
        COUNT(*)::int AS source_row_count,
        {table.metrics_sql} AS metrics
      FROM {quote(table.table)}
      WHERE {quote(table.website_column)} = %s
        AND {quote(table.date_column)} >= %s
        AND {quote(table.date_column)} <= %s
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(sql, [website, period_start, period_end])
        row = dict(cursor.fetchone() or {})
    metrics = row.get("metrics") or {}
    return {
        "source_row_count": int(row.get("source_row_count") or 0),
        "metrics": metrics,
    }


def upsert_summary(cursor, table: RetentionTable, website: str, period_start: date, period_end: date, summary: dict[str, Any]):
    cursor.execute(
        f"""
        INSERT INTO {quote(table.summary_table)}
          (period_start, period_end, website, source_row_count, metrics)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (period_start, period_end, website)
        DO UPDATE SET
          source_row_count = EXCLUDED.source_row_count,
          metrics = EXCLUDED.metrics,
          updated_at = NOW()
        """,
        [
            period_start,
            period_end,
            website,
            summary["source_row_count"],
            Json(summary["metrics"], dumps=json_dumps),
        ],
    )


def json_default(value: Any):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def json_dumps(value: Any) -> str:
    return json.dumps(value, default=json_default)


def fetch_delete_batch(cursor, table: RetentionTable, website: str, period_start: date, period_end: date, batch_size: int) -> list[str]:
    cursor.execute(
        f"""
        SELECT id::text
        FROM {quote(table.table)}
        WHERE {quote(table.website_column)} = %s
          AND {quote(table.date_column)} >= %s
          AND {quote(table.date_column)} <= %s
        ORDER BY {quote(table.date_column)} ASC, id ASC
        LIMIT %s
        FOR UPDATE SKIP LOCKED
        """,
        [website, period_start, period_end, batch_size],
    )
    return [row[0] for row in cursor.fetchall()]


def delete_batch(cursor, table: RetentionTable, ids: list[str]) -> int:
    if not ids:
        return 0
    cursor.execute(
        f"DELETE FROM {quote(table.table)} WHERE id::text = ANY(%s)",
        [ids],
    )
    return cursor.rowcount


def insert_log(
    conn,
    *,
    run_id: str,
    table: RetentionTable,
    website: str | None,
    period_start: date | None,
    period_end: date | None,
    dry_run: bool,
    source_row_count: int,
    summarized_rows: int,
    deleted_rows: int,
    status: str,
    message: str,
    started_at: datetime,
):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO retention_log
              (run_id, table_name, summary_table_name, period_start, period_end, website,
               dry_run, source_row_count, summarized_rows, deleted_rows, status, message,
               started_at, completed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            [
                run_id,
                table.table,
                table.summary_table,
                period_start,
                period_end,
                website,
                dry_run,
                source_row_count,
                summarized_rows,
                deleted_rows,
                status,
                message[:1000] if message else "",
                started_at,
            ],
        )
    conn.commit()


def process_period(
    conn,
    *,
    run_id: str,
    table: RetentionTable,
    website: str,
    period_start: date,
    period_end: date,
    source_row_count: int,
    batch_size: int,
    dry_run: bool,
) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    summary = build_summary(conn, table, website, period_start, period_end)

    if dry_run:
        insert_log(
            conn,
            run_id=run_id,
            table=table,
            website=website,
            period_start=period_start,
            period_end=period_end,
            dry_run=True,
            source_row_count=source_row_count,
            summarized_rows=0,
            deleted_rows=0,
            status="dry_run",
            message="Would summarize and delete old detailed rows.",
            started_at=started_at,
        )
        return {"summarized": 0, "deleted": 0}

    deleted_total = 0
    summarized_rows = 0
    try:
        while True:
            with conn.cursor() as cursor:
                cursor.execute("BEGIN")
                upsert_summary(cursor, table, website, period_start, period_end, summary)
                ids = fetch_delete_batch(cursor, table, website, period_start, period_end, batch_size)
                deleted = delete_batch(cursor, table, ids)
                cursor.execute("COMMIT")

            if summarized_rows == 0:
                summarized_rows = 1
            deleted_total += deleted
            if deleted == 0 or len(ids) < batch_size:
                break

        insert_log(
            conn,
            run_id=run_id,
            table=table,
            website=website,
            period_start=period_start,
            period_end=period_end,
            dry_run=False,
            source_row_count=source_row_count,
            summarized_rows=summarized_rows,
            deleted_rows=deleted_total,
            status="completed",
            message="Summary saved before detailed rows were deleted.",
            started_at=started_at,
        )
        return {"summarized": summarized_rows, "deleted": deleted_total}
    except Exception as exc:
        conn.rollback()
        insert_log(
            conn,
            run_id=run_id,
            table=table,
            website=website,
            period_start=period_start,
            period_end=period_end,
            dry_run=False,
            source_row_count=source_row_count,
            summarized_rows=summarized_rows,
            deleted_rows=deleted_total,
            status="failed",
            message=str(exc),
            started_at=started_at,
        )
        raise


def main():
    parser = argparse.ArgumentParser(description="Summarize and prune granular SEODash data older than the retention window")
    parser.add_argument("--retention-days", type=int, default=int(os.getenv("RETENTION_DAYS", "90")))
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("RETENTION_BATCH_SIZE", "500")))
    parser.add_argument("--max-periods-per-table", type=int, default=int(os.getenv("RETENTION_MAX_PERIODS_PER_TABLE", "0")))
    parser.add_argument("--tables", default=os.getenv("RETENTION_TABLES", ""))
    parser.add_argument("--dry-run", action="store_true", default=env_bool("DRY_RUN", True))
    parser.add_argument("--live", action="store_true", help="Run live even if DRY_RUN defaults to true")
    args = parser.parse_args()

    dry_run = False if args.live else args.dry_run
    selected = {item.strip() for item in args.tables.split(",") if item.strip()}
    tables = [table for table in TABLES if not selected or table.table in selected]
    if not tables:
        raise RuntimeError("No retention tables selected")

    run_id = str(uuid.uuid4())
    logger.info(
        "Retention cleanup started run_id=%s dry_run=%s retention_days=%s batch_size=%s tables=%s",
        run_id,
        dry_run,
        args.retention_days,
        args.batch_size,
        ",".join(table.table for table in tables),
    )

    totals = {"periods": 0, "source_rows": 0, "summaries": 0, "deleted": 0}
    with connect() as conn:
        conn.autocommit = True
        for table in tables:
            periods = find_periods(
                conn,
                table,
                args.retention_days,
                limit=args.max_periods_per_table or None,
            )
            if not periods:
                logger.info("%s: no rows older than %d days", table.table, args.retention_days)
                insert_log(
                    conn,
                    run_id=run_id,
                    table=table,
                    website=None,
                    period_start=None,
                    period_end=None,
                    dry_run=dry_run,
                    source_row_count=0,
                    summarized_rows=0,
                    deleted_rows=0,
                    status="no_op",
                    message=f"No rows older than {args.retention_days} days.",
                    started_at=datetime.now(timezone.utc),
                )
                continue

            for period in periods:
                logger.info(
                    "%s/%s/%s..%s: source_rows=%s dry_run=%s",
                    table.table,
                    period["website"],
                    period["period_start"],
                    period["period_end"],
                    period["source_row_count"],
                    dry_run,
                )
                result = process_period(
                    conn,
                    run_id=run_id,
                    table=table,
                    website=period["website"],
                    period_start=period["period_start"],
                    period_end=period["period_end"],
                    source_row_count=period["source_row_count"],
                    batch_size=args.batch_size,
                    dry_run=dry_run,
                )
                totals["periods"] += 1
                totals["source_rows"] += period["source_row_count"]
                totals["summaries"] += result["summarized"]
                totals["deleted"] += result["deleted"]

    logger.info(
        "Retention cleanup finished run_id=%s periods=%s source_rows=%s summaries=%s deleted=%s dry_run=%s",
        run_id,
        totals["periods"],
        totals["source_rows"],
        totals["summaries"],
        totals["deleted"],
        dry_run,
    )

    print(json.dumps({"run_id": run_id, "dry_run": dry_run, **totals}, indent=2))


if __name__ == "__main__":
    main()
