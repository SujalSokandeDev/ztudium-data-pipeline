"""
Fetch GSC + GA4 data and upload to Supabase.
Self-contained — no dependency on the dashboard backend.

Usage:
    python scripts/fetch_google.py
"""

import os
import sys
import logging
import time
from datetime import date, timedelta
from collections import defaultdict

# Add scripts dir to path
sys.path.insert(0, os.path.dirname(__file__))
from config import WEBSITES, SUPABASE_URL, SUPABASE_SERVICE_KEY, setup_google_credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fetch_google")
_UPSERT_SUPPORTS_DEFAULT_TO_NULL = None

END_DATE = date.today() - timedelta(days=2)
START_DATE = END_DATE - timedelta(days=30)


# ══════════════════════════════════════════════════════════════
#  Google API clients
# ══════════════════════════════════════════════════════════════

gsc_service = None
ga4_client = None


def init_google_apis():
    """Initialize Google API clients."""
    global gsc_service, ga4_client

    creds_path = setup_google_credentials()

    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    # GSC
    try:
        gsc_creds = service_account.Credentials.from_service_account_file(
            creds_path, scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
        )
        gsc_service = build("searchconsole", "v1", credentials=gsc_creds, cache_discovery=False)
        logger.info("GSC API initialized")
    except Exception as e:
        logger.error("GSC init failed: %s", e)

    # GA4
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient

        ga4_creds = service_account.Credentials.from_service_account_file(
            creds_path, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
        )
        ga4_client = BetaAnalyticsDataClient(credentials=ga4_creds)
        logger.info("GA4 API initialized")
    except Exception as e:
        logger.error("GA4 init failed: %s", e)


# ══════════════════════════════════════════════════════════════
#  GSC: Daily metrics
# ══════════════════════════════════════════════════════════════

def fetch_gsc_daily(name, gsc_prop):
    """Fetch daily GSC metrics (clicks, impressions, CTR, position)."""
    if not gsc_service or not gsc_prop:
        return []
    try:
        resp = gsc_service.searchanalytics().query(
            siteUrl=gsc_prop,
            body={
                "startDate": START_DATE.isoformat(),
                "endDate": END_DATE.isoformat(),
                "dimensions": ["date"],
                "rowLimit": 500,
            },
        ).execute()
        rows = []
        for row in resp.get("rows", []):
            rows.append({
                "date": row["keys"][0],
                "website": name,
                "gsc_clicks": int(row.get("clicks", 0)),
                "gsc_impressions": int(row.get("impressions", 0)),
                "gsc_ctr": round(row.get("ctr", 0) * 100, 2),
                "gsc_position": round(row.get("position", 0), 2),
            })
        logger.info("  GSC daily: %d rows for %s", len(rows), name)
        return rows
    except Exception as e:
        logger.error("  GSC daily failed for %s: %s", name, e)
        return []


def fetch_gsc_keywords(name, gsc_prop):
    """Fetch top 100 keywords by clicks."""
    if not gsc_service or not gsc_prop:
        return []
    try:
        resp = gsc_service.searchanalytics().query(
            siteUrl=gsc_prop,
            body={
                "startDate": START_DATE.isoformat(),
                "endDate": END_DATE.isoformat(),
                "dimensions": ["query"],
                "rowLimit": 100,
            },
        ).execute()
        keywords = []
        for row in resp.get("rows", []):
            keywords.append({
                "keyword": row["keys"][0],
                "clicks": int(row.get("clicks", 0)),
                "impressions": int(row.get("impressions", 0)),
                "ctr": round(row.get("ctr", 0) * 100, 2),
                "position": round(row.get("position", 0), 2),
            })
        logger.info("  GSC keywords: %d for %s", len(keywords), name)
        return keywords
    except Exception as e:
        logger.error("  GSC keywords failed for %s: %s", name, e)
        return []


def fetch_gsc_pages(name, gsc_prop):
    """Fetch top 100 pages by clicks."""
    if not gsc_service or not gsc_prop:
        return []
    try:
        resp = gsc_service.searchanalytics().query(
            siteUrl=gsc_prop,
            body={
                "startDate": START_DATE.isoformat(),
                "endDate": END_DATE.isoformat(),
                "dimensions": ["page"],
                "rowLimit": 100,
            },
        ).execute()
        pages = []
        for row in resp.get("rows", []):
            pages.append({
                "url": row["keys"][0],
                "clicks": int(row.get("clicks", 0)),
                "impressions": int(row.get("impressions", 0)),
                "ctr": round(row.get("ctr", 0) * 100, 2),
                "position": round(row.get("position", 0), 2),
            })
        logger.info("  GSC pages: %d for %s", len(pages), name)
        return pages
    except Exception as e:
        logger.error("  GSC pages failed for %s: %s", name, e)
        return []


# ══════════════════════════════════════════════════════════════
#  GA4: Daily metrics
# ══════════════════════════════════════════════════════════════

def fetch_ga4_daily(name, ga4_id):
    """Fetch daily GA4 metrics (sessions, users, organic, engagement)."""
    if not ga4_client or not ga4_id:
        return []
    try:
        from google.analytics.data_v1beta.types import (
            RunReportRequest, DateRange, Dimension, Metric,
        )

        # All-traffic request
        req_all = RunReportRequest(
            property=f"properties/{ga4_id}",
            date_ranges=[DateRange(start_date=START_DATE.isoformat(), end_date=END_DATE.isoformat())],
            dimensions=[Dimension(name="date")],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="userEngagementDuration"),
                Metric(name="bounceRate"),
            ],
        )
        resp_all = ga4_client.run_report(req_all)

        all_by_date = {}
        for row in resp_all.rows:
            raw = row.dimension_values[0].value
            d = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
            all_by_date[d] = {
                "date": d,
                "website": name,
                "ga_sessions": int(row.metric_values[0].value),
                "ga_users": int(row.metric_values[1].value),
                "ga_engagement_time": round(float(row.metric_values[2].value), 1),
                "ga_bounce_rate": round(float(row.metric_values[3].value) * 100, 2),
            }

        # Organic-only request
        from google.analytics.data_v1beta.types import FilterExpression, Filter
        req_organic = RunReportRequest(
            property=f"properties/{ga4_id}",
            date_ranges=[DateRange(start_date=START_DATE.isoformat(), end_date=END_DATE.isoformat())],
            dimensions=[Dimension(name="date")],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalUsers"),
            ],
            dimension_filter=FilterExpression(
                filter=Filter(
                    field_name="sessionDefaultChannelGroup",
                    string_filter=Filter.StringFilter(value="Organic Search"),
                )
            ),
        )
        try:
            resp_org = ga4_client.run_report(req_organic)
            for row in resp_org.rows:
                raw = row.dimension_values[0].value
                d = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
                if d in all_by_date:
                    all_by_date[d]["ga_organic_sessions"] = int(row.metric_values[0].value)
                    all_by_date[d]["ga_organic_users"] = int(row.metric_values[1].value)
        except Exception:
            pass  # Organic filter may not be available

        rows = sorted(all_by_date.values(), key=lambda r: r["date"])
        logger.info("  GA4 daily: %d rows for %s", len(rows), name)
        return rows

    except Exception as e:
        logger.error("  GA4 daily failed for %s: %s", name, e)
        return []


# ══════════════════════════════════════════════════════════════
#  Supabase upload
# ══════════════════════════════════════════════════════════════

def batch_upsert(client, table, rows, conflict_cols):
    """Batch upsert with key normalization."""
    if not rows:
        return 0
    all_keys = set()
    for row in rows:
        all_keys.update(row.keys())
    normalized = [{k: row.get(k) for k in all_keys} for row in rows]

    upserted = 0
    chunk_size = 50
    chunk_retries = 3
    for i in range(0, len(normalized), chunk_size):
        chunk = normalized[i : i + chunk_size]
        chunk_ok = False

        for attempt in range(1, chunk_retries + 1):
            try:
                _upsert_chunk(client, table, chunk, conflict_cols)
                upserted += len(chunk)
                chunk_ok = True
                break
            except Exception as e:
                msg = str(e)[:200]
                if attempt < chunk_retries and _is_retryable_upsert_error(e):
                    wait_s = 1.0 * attempt
                    logger.warning(
                        "  %s chunk retry %d/%d after transient error: %s",
                        table, attempt, chunk_retries, msg
                    )
                    time.sleep(wait_s)
                    continue
                logger.error("  %s batch error (chunk %d): %s", table, i, msg)
                break

        if not chunk_ok:
            for row in chunk:
                try:
                    client.table(table).upsert([row], on_conflict=conflict_cols).execute()
                    upserted += 1
                except Exception:
                    pass
                time.sleep(0.05)

        time.sleep(0.2)
    return upserted


def _is_retryable_upsert_error(exc: Exception) -> bool:
    """Detect transient transport/rate-limit errors that benefit from retry."""
    msg = str(exc).lower()
    retry_markers = (
        "429",
        "rate limit",
        "timed out",
        "timeout",
        "connection reset",
        "temporarily unavailable",
        "503",
        "502",
        "504",
    )
    return any(m in msg for m in retry_markers)


def _upsert_chunk(client, table: str, chunk: list, conflict_cols: str):
    """Upsert one chunk, with compatibility handling for older Supabase clients."""
    global _UPSERT_SUPPORTS_DEFAULT_TO_NULL

    base_kwargs = {"on_conflict": conflict_cols}

    if _UPSERT_SUPPORTS_DEFAULT_TO_NULL is False:
        client.table(table).upsert(chunk, **base_kwargs).execute()
        return

    try:
        client.table(table).upsert(chunk, on_conflict=conflict_cols, default_to_null=True).execute()
        _UPSERT_SUPPORTS_DEFAULT_TO_NULL = True
    except Exception as e:
        msg = str(e).lower()
        unsupported_default_to_null = (
            "default_to_null" in msg and
            ("unexpected keyword" in msg or "got an unexpected keyword argument" in msg)
        )
        if not unsupported_default_to_null:
            raise

        _UPSERT_SUPPORTS_DEFAULT_TO_NULL = False
        logger.warning(
            "Supabase client does not support default_to_null; using compatible upsert mode."
        )
        client.table(table).upsert(chunk, **base_kwargs).execute()


def store_in_supabase(all_data, keywords_data, pages_data):
    """Upload all fetched data to Supabase."""
    from supabase import create_client

    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.warning("Supabase credentials not set, skipping upload")
        return
    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    today_str = date.today().isoformat()

    # Daily metrics
    dm_rows = []
    for name, rows in all_data.items():
        for row in rows:
            db_row = {
                "date": row.get("date"),
                "website": name,
                "gsc_clicks": row.get("gsc_clicks"),
                "gsc_impressions": row.get("gsc_impressions"),
                "gsc_ctr": row.get("gsc_ctr"),
                "gsc_position": row.get("gsc_position"),
                "ga_sessions": row.get("ga_sessions"),
                "ga_users": row.get("ga_users"),
                "ga_organic_sessions": row.get("ga_organic_sessions"),
                "ga_organic_users": row.get("ga_organic_users"),
                "ga_engagement_time": row.get("ga_engagement_time"),
                "ga_bounce_rate": row.get("ga_bounce_rate"),
                "data_source": "api",
            }
            dm_rows.append({k: v for k, v in db_row.items() if v is not None})
    c = batch_upsert(client, "daily_metrics", dm_rows, "date,website")
    logger.info("  daily_metrics: %d/%d upserted", c, len(dm_rows))

    # Keywords
    kw_rows = []
    for name, kws in keywords_data.items():
        for kw in kws[:100]:
            kw_rows.append({
                "date": today_str, "website": name, "keyword": kw.get("keyword"),
                "clicks": kw.get("clicks"), "impressions": kw.get("impressions"),
                "ctr": kw.get("ctr"), "position": kw.get("position"), "source": "gsc",
            })
    c = batch_upsert(client, "website_keywords", kw_rows, "date,website,keyword,source")
    logger.info("  website_keywords (GSC): %d/%d upserted", c, len(kw_rows))

    # Pages
    pg_rows = []
    for name, pgs in pages_data.items():
        for pg in pgs[:50]:
            pg_rows.append({
                "date": today_str, "website": name, "url": pg.get("url"),
                "clicks": pg.get("clicks"), "impressions": pg.get("impressions"),
                "ctr": pg.get("ctr"), "position": pg.get("position"), "source": "gsc",
            })
    c = batch_upsert(client, "website_pages", pg_rows, "date,website,url,source")
    logger.info("  website_pages (GSC): %d/%d upserted", c, len(pg_rows))


# ══════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════

def main():
    print()
    print("=" * 60)
    print("  DAILY GOOGLE FETCH (GSC + GA4)")
    print(f"  Date: {date.today().isoformat()}")
    print(f"  Range: {START_DATE} to {END_DATE}")
    print("=" * 60)

    init_google_apis()

    all_data = {}
    keywords_data = {}
    pages_data = {}

    for ws in WEBSITES:
        name = ws["name"]
        gsc_prop = ws.get("gsc_property", "")
        ga4_id = ws.get("ga4_property_id", "")

        print(f"\n  {name}:")
        merged = defaultdict(lambda: {"website": name})

        for row in fetch_gsc_daily(name, gsc_prop):
            merged[row["date"]].update(row)
        for row in fetch_ga4_daily(name, ga4_id):
            merged[row["date"]].update(row)

        if merged:
            all_data[name] = sorted(merged.values(), key=lambda r: r.get("date", ""))

        kws = fetch_gsc_keywords(name, gsc_prop)
        if kws:
            keywords_data[name] = kws

        pgs = fetch_gsc_pages(name, gsc_prop)
        if pgs:
            pages_data[name] = pgs

    # Upload to Supabase
    print("\n--- Uploading to Supabase ---")
    store_in_supabase(all_data, keywords_data, pages_data)

    # Summary
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"  Websites with data: {len(all_data)}")
    print(f"  Websites with keywords: {len(keywords_data)}")
    print(f"  Websites with pages: {len(pages_data)}")
    for name in sorted(all_data.keys()):
        days = len(all_data.get(name, []))
        kws = len(keywords_data.get(name, []))
        pgs = len(pages_data.get(name, []))
        print(f"  {name:15s} | {days:3d} days | {kws:3d} keywords | {pgs:3d} pages")
    print()


if __name__ == "__main__":
    main()
