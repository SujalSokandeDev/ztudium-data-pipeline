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
import uuid
import re
from datetime import date, timedelta
from collections import defaultdict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

# Add scripts dir to path
sys.path.insert(0, os.path.dirname(__file__))
from config import WEBSITES, SUPABASE_URL, SUPABASE_SERVICE_KEY, setup_google_credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fetch_google")

END_DATE = date.today() - timedelta(days=2)
START_DATE = END_DATE - timedelta(days=30)
TRANSIENT_STATUS_CODES = {408, 429, 500, 502, 503, 504}


# ══════════════════════════════════════════════════════════════
#  Google API clients
# ══════════════════════════════════════════════════════════════

gsc_service = None
ga4_client = None


def _extract_status_code(exc: Exception):
    """Best-effort extraction of HTTP/gRPC status code from API exceptions."""
    resp = getattr(exc, "resp", None)
    if resp is not None:
        status = getattr(resp, "status", None) or getattr(resp, "status_code", None)
        if isinstance(status, int):
            return status
        try:
            return int(status)
        except Exception:
            pass

    code_attr = getattr(exc, "code", None)
    if code_attr is not None:
        try:
            code_val = code_attr() if callable(code_attr) else code_attr
        except Exception:
            code_val = code_attr
        if isinstance(code_val, int):
            return code_val

        code_text = str(code_val).upper()
        if "PERMISSION_DENIED" in code_text:
            return 403
        if "INVALID_ARGUMENT" in code_text:
            return 400
        if "UNAVAILABLE" in code_text:
            return 503
        if "DEADLINE_EXCEEDED" in code_text:
            return 504

    match = re.search(r"\b([45]\d{2})\b", str(exc))
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def _is_transient_error(exc: Exception) -> bool:
    code = _extract_status_code(exc)
    if code in TRANSIENT_STATUS_CODES:
        return True

    msg = str(exc).lower()
    retry_markers = (
        "rate limit",
        "timed out",
        "timeout",
        "connection reset",
        "temporarily unavailable",
        "service unavailable",
        "internal server error",
        "deadline exceeded",
        "connection aborted",
    )
    return any(marker in msg for marker in retry_markers)


def _is_retryable_api_error(exc: Exception) -> bool:
    # Retry only for transient API/transport conditions.
    return _is_transient_error(exc)


def _classify_api_error(exc: Exception):
    """Return (kind, summary_message) for logging and reporting."""
    code = _extract_status_code(exc)
    if code == 403:
        return "permission", "access denied"
    if code == 400:
        return "config", "invalid request or property configuration"
    if code in TRANSIENT_STATUS_CODES or _is_transient_error(exc):
        if code:
            return "transient", f"temporary API issue (HTTP {code})"
        return "transient", "temporary API issue"
    return "unexpected", str(exc).splitlines()[0][:180]


def _to_int(value, default=0):
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_float(value, default=0.0):
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _validate_ga4_property_id(raw_id):
    prop = str(raw_id or "").strip()
    if not prop:
        return None, "missing property ID"
    if not prop.isdigit():
        return None, f"invalid property ID '{prop}' (must be numeric)"
    return prop, None


def _status_text(status, reason=""):
    if status == "ok":
        return "OK"
    if status == "partial":
        return f"PARTIAL ({reason})" if reason else "PARTIAL"
    if status == "skip":
        return f"SKIP ({reason})" if reason else "SKIP"
    return f"FAIL ({reason})" if reason else "FAIL"


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception(_is_retryable_api_error),
)
def _execute_gsc_query(site_url: str, body: dict):
    return gsc_service.searchanalytics().query(siteUrl=site_url, body=body).execute()


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception(_is_retryable_api_error),
)
def _execute_ga4_report(req):
    return ga4_client.run_report(req)


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
    if not gsc_service:
        return [], {"kind": "config", "message": "GSC client unavailable"}
    if not str(gsc_prop or "").strip():
        return [], {"kind": "config", "message": "missing GSC property"}

    try:
        resp = _execute_gsc_query(
            gsc_prop,
            {
                "startDate": START_DATE.isoformat(),
                "endDate": END_DATE.isoformat(),
                "dimensions": ["date"],
                "rowLimit": 500,
            },
        )
        rows = []
        for row in resp.get("rows", []):
            rows.append(
                {
                    "date": row["keys"][0],
                    "website": name,
                    "gsc_clicks": _to_int(row.get("clicks", 0)),
                    "gsc_impressions": _to_int(row.get("impressions", 0)),
                    "gsc_ctr": round(_to_float(row.get("ctr", 0)) * 100, 2),
                    "gsc_position": round(_to_float(row.get("position", 0)), 2),
                }
            )
        logger.info("  GSC daily: %d rows for %s", len(rows), name)
        return rows, None
    except Exception as exc:
        kind, msg = _classify_api_error(exc)
        level = logging.WARNING if kind in {"permission", "config", "transient"} else logging.ERROR
        logger.log(level, "  GSC daily skipped for %s: %s", name, msg)
        return [], {"kind": kind, "message": msg}


def fetch_gsc_keywords(name, gsc_prop):
    """Fetch top 100 keywords by clicks."""
    if not gsc_service:
        return [], {"kind": "config", "message": "GSC client unavailable"}
    if not str(gsc_prop or "").strip():
        return [], {"kind": "config", "message": "missing GSC property"}

    try:
        resp = _execute_gsc_query(
            gsc_prop,
            {
                "startDate": START_DATE.isoformat(),
                "endDate": END_DATE.isoformat(),
                "dimensions": ["query"],
                "rowLimit": 100,
            },
        )
        keywords = []
        for row in resp.get("rows", []):
            keywords.append(
                {
                    "keyword": row["keys"][0],
                    "clicks": _to_int(row.get("clicks", 0)),
                    "impressions": _to_int(row.get("impressions", 0)),
                    "ctr": round(_to_float(row.get("ctr", 0)) * 100, 2),
                    "position": round(_to_float(row.get("position", 0)), 2),
                }
            )
        logger.info("  GSC keywords: %d for %s", len(keywords), name)
        return keywords, None
    except Exception as exc:
        kind, msg = _classify_api_error(exc)
        level = logging.WARNING if kind in {"permission", "config", "transient"} else logging.ERROR
        logger.log(level, "  GSC keywords skipped for %s: %s", name, msg)
        return [], {"kind": kind, "message": msg}


def fetch_gsc_pages(name, gsc_prop):
    """Fetch top 100 pages by clicks."""
    if not gsc_service:
        return [], {"kind": "config", "message": "GSC client unavailable"}
    if not str(gsc_prop or "").strip():
        return [], {"kind": "config", "message": "missing GSC property"}

    try:
        resp = _execute_gsc_query(
            gsc_prop,
            {
                "startDate": START_DATE.isoformat(),
                "endDate": END_DATE.isoformat(),
                "dimensions": ["page"],
                "rowLimit": 100,
            },
        )
        pages = []
        for row in resp.get("rows", []):
            pages.append(
                {
                    "url": row["keys"][0],
                    "clicks": _to_int(row.get("clicks", 0)),
                    "impressions": _to_int(row.get("impressions", 0)),
                    "ctr": round(_to_float(row.get("ctr", 0)) * 100, 2),
                    "position": round(_to_float(row.get("position", 0)), 2),
                }
            )
        logger.info("  GSC pages: %d for %s", len(pages), name)
        return pages, None
    except Exception as exc:
        kind, msg = _classify_api_error(exc)
        level = logging.WARNING if kind in {"permission", "config", "transient"} else logging.ERROR
        logger.log(level, "  GSC pages skipped for %s: %s", name, msg)
        return [], {"kind": kind, "message": msg}


# ══════════════════════════════════════════════════════════════
#  GA4: Daily metrics
# ══════════════════════════════════════════════════════════════

def fetch_ga4_daily(name, ga4_id):
    """Fetch daily GA4 metrics (sessions, users, organic, engagement)."""
    if not ga4_client:
        return [], {"kind": "config", "message": "GA4 client unavailable"}

    normalized_id, validation_error = _validate_ga4_property_id(ga4_id)
    if validation_error:
        logger.warning("  GA4 skipped for %s: %s", name, validation_error)
        return [], {"kind": "config", "message": validation_error}

    try:
        from google.analytics.data_v1beta.types import (
            RunReportRequest, DateRange, Dimension, Metric,
        )

        req_all = RunReportRequest(
            property=f"properties/{normalized_id}",
            date_ranges=[DateRange(start_date=START_DATE.isoformat(), end_date=END_DATE.isoformat())],
            dimensions=[Dimension(name="date")],
            metrics=[
                Metric(name="sessions"),
                Metric(name="totalUsers"),
                Metric(name="userEngagementDuration"),
                Metric(name="bounceRate"),
            ],
        )
        resp_all = _execute_ga4_report(req_all)

        all_by_date = {}
        for row in resp_all.rows:
            raw = row.dimension_values[0].value
            d = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
            all_by_date[d] = {
                "date": d,
                "website": name,
                "ga_sessions": _to_int(row.metric_values[0].value),
                "ga_users": _to_int(row.metric_values[1].value),
                "ga_engagement_time": round(_to_float(row.metric_values[2].value), 1),
                "ga_bounce_rate": round(_to_float(row.metric_values[3].value) * 100, 2),
            }

        from google.analytics.data_v1beta.types import FilterExpression, Filter
        req_organic = RunReportRequest(
            property=f"properties/{normalized_id}",
            date_ranges=[DateRange(start_date=START_DATE.isoformat(), end_date=END_DATE.isoformat())],
            dimensions=[Dimension(name="date")],
            metrics=[Metric(name="sessions"), Metric(name="totalUsers")],
            dimension_filter=FilterExpression(
                filter=Filter(
                    field_name="sessionDefaultChannelGroup",
                    string_filter=Filter.StringFilter(value="Organic Search"),
                )
            ),
        )
        try:
            resp_org = _execute_ga4_report(req_organic)
            for row in resp_org.rows:
                raw = row.dimension_values[0].value
                d = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
                if d in all_by_date:
                    all_by_date[d]["ga_organic_sessions"] = _to_int(row.metric_values[0].value)
                    all_by_date[d]["ga_organic_users"] = _to_int(row.metric_values[1].value)
        except Exception as exc:
            kind, msg = _classify_api_error(exc)
            # Organic segment is non-critical; warn and continue with base GA4 data.
            logger.warning("  GA4 organic segment unavailable for %s: %s", name, msg if kind != "unexpected" else "unexpected error")

        rows = sorted(all_by_date.values(), key=lambda r: r["date"])
        logger.info("  GA4 daily: %d rows for %s", len(rows), name)
        return rows, None

    except Exception as exc:
        kind, msg = _classify_api_error(exc)
        level = logging.WARNING if kind in {"permission", "config", "transient"} else logging.ERROR
        logger.log(level, "  GA4 daily skipped for %s: %s", name, msg)
        return [], {"kind": kind, "message": msg}


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
                except Exception as e:
                    logger.warning("  %s row upsert failed: %s", table, str(e)[:180])
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
    """Upsert one chunk using supabase-py compatible parameters."""
    client.table(table).upsert(chunk, on_conflict=conflict_cols).execute()


def _start_ingestion_run(source: str, websites_attempted: list[str]) -> str | None:
    """Create a run record; do not fail pipeline if tracking table is unavailable."""
    from supabase import create_client

    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return None

    try:
        client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        run_id = str(uuid.uuid4())
        payload = {
            "id": run_id,
            "source": source,
            "status": "running",
            "websites_attempted": websites_attempted,
        }
        client.table("ingestion_runs").insert(payload).execute()
        return run_id
    except Exception as e:
        logger.warning("Could not start ingestion run tracking: %s", str(e)[:200])
        return None


def _finish_ingestion_run(
    run_id: str | None,
    status: str,
    websites_succeeded: list[str],
    websites_failed: list[str],
    error_details: dict,
    duration_seconds: int,
):
    """Finalize run tracking; best effort only."""
    if not run_id or not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return

    from supabase import create_client

    try:
        client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        payload = {
            "status": status,
            "websites_succeeded": websites_succeeded,
            "websites_failed": websites_failed,
            "error_details": error_details,
            "duration_seconds": duration_seconds,
        }
        client.table("ingestion_runs").update(payload).eq("id", run_id).execute()
    except Exception as e:
        logger.warning("Could not finalize ingestion run tracking: %s", str(e)[:200])


_COLUMN_SUPPORT_CACHE = {}


def _supports_column(client, table: str, column: str) -> bool:
    key = f"{table}.{column}"
    if key in _COLUMN_SUPPORT_CACHE:
        return _COLUMN_SUPPORT_CACHE[key]
    try:
        client.table(table).select(column).limit(1).execute()
        _COLUMN_SUPPORT_CACHE[key] = True
    except Exception as e:
        msg = str(e).lower()
        _COLUMN_SUPPORT_CACHE[key] = not ("column" in msg and "does not exist" in msg)
    return _COLUMN_SUPPORT_CACHE[key]


def store_in_supabase(all_data, keywords_data, pages_data, run_id: str | None = None):
    """Upload all fetched data to Supabase."""
    from supabase import create_client

    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.warning("Supabase credentials not set, skipping upload")
        return
    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    daily_metrics_has_ingestion_run_id = _supports_column(client, "daily_metrics", "ingestion_run_id")
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
            if run_id and daily_metrics_has_ingestion_run_id:
                db_row["ingestion_run_id"] = run_id
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
    run_started = time.time()
    all_website_names = [ws["name"] for ws in WEBSITES]
    run_id = _start_ingestion_run("google", all_website_names)

    all_data = {}
    keywords_data = {}
    pages_data = {}
    websites_succeeded = []
    websites_failed = []
    error_details = {}
    site_report = {}

    for ws in WEBSITES:
        name = ws["name"]
        gsc_prop = (ws.get("gsc_property", "") or "").strip()
        ga4_id = ws.get("ga4_property_id", "")

        print(f"\n  {name}:")
        merged = defaultdict(lambda: {"website": name})
        rep = {
            "gsc_status": "fail",
            "gsc_reason": "no GSC payload",
            "ga4_status": "fail",
            "ga4_reason": "no GA4 payload",
            "days": 0,
            "keywords": 0,
            "pages": 0,
        }

        # GSC calls
        gsc_daily_rows, gsc_daily_err = fetch_gsc_daily(name, gsc_prop)
        for row in gsc_daily_rows:
            merged[row["date"]].update(row)

        skip_remaining_gsc = bool(
            gsc_daily_err and gsc_daily_err.get("kind") in {"permission", "config"}
        )
        if skip_remaining_gsc:
            kws, kws_err = [], None
            pgs, pgs_err = [], None
        else:
            kws, kws_err = fetch_gsc_keywords(name, gsc_prop)
            pgs, pgs_err = fetch_gsc_pages(name, gsc_prop)

        if kws:
            keywords_data[name] = kws
        if pgs:
            pages_data[name] = pgs

        gsc_success_sections = 0
        gsc_failed_parts = []
        if gsc_daily_rows:
            gsc_success_sections += 1
        elif gsc_daily_err:
            gsc_failed_parts.append(f"daily:{gsc_daily_err.get('message')}")
        if kws:
            gsc_success_sections += 1
        elif kws_err:
            gsc_failed_parts.append(f"keywords:{kws_err.get('message')}")
        if pgs:
            gsc_success_sections += 1
        elif pgs_err:
            gsc_failed_parts.append(f"pages:{pgs_err.get('message')}")
        gsc_failed_parts = list(dict.fromkeys(gsc_failed_parts))

        if gsc_success_sections == 3:
            rep["gsc_status"] = "ok"
            rep["gsc_reason"] = ""
        elif gsc_success_sections > 0:
            rep["gsc_status"] = "partial"
            rep["gsc_reason"] = "; ".join(gsc_failed_parts[:2])
        elif gsc_daily_err and gsc_daily_err.get("kind") == "config" and "missing GSC property" in gsc_daily_err.get("message", ""):
            rep["gsc_status"] = "skip"
            rep["gsc_reason"] = gsc_daily_err.get("message")
        else:
            rep["gsc_status"] = "fail"
            rep["gsc_reason"] = "; ".join(gsc_failed_parts[:2]) or "no GSC payload"

        # GA4 call
        ga4_daily_rows, ga4_err = fetch_ga4_daily(name, ga4_id)
        for row in ga4_daily_rows:
            merged[row["date"]].update(row)
        if ga4_daily_rows:
            rep["ga4_status"] = "ok"
            rep["ga4_reason"] = ""
        elif ga4_err and ga4_err.get("kind") == "config":
            rep["ga4_status"] = "skip"
            rep["ga4_reason"] = ga4_err.get("message", "GA4 not configured")
        elif ga4_err:
            rep["ga4_status"] = "fail"
            rep["ga4_reason"] = ga4_err.get("message", "GA4 failed")

        if merged:
            all_data[name] = sorted(merged.values(), key=lambda r: r.get("date", ""))
        rep["days"] = len(all_data.get(name, []))
        rep["keywords"] = len(keywords_data.get(name, []))
        rep["pages"] = len(pages_data.get(name, []))
        site_report[name] = rep

        has_any_payload = bool(gsc_daily_rows or ga4_daily_rows or kws or pgs)
        if has_any_payload:
            websites_succeeded.append(name)
        else:
            websites_failed.append(name)

        if rep["gsc_status"] in {"fail", "partial", "skip"} or rep["ga4_status"] in {"fail", "skip"}:
            error_details[name] = {
                "gsc": _status_text(rep["gsc_status"], rep["gsc_reason"]),
                "ga4": _status_text(rep["ga4_status"], rep["ga4_reason"]),
            }

        logger.info(
            "  %s status | GSC: %s | GA4: %s",
            name,
            _status_text(rep["gsc_status"], rep["gsc_reason"]),
            _status_text(rep["ga4_status"], rep["ga4_reason"]),
        )

    # Upload to Supabase
    print("\n--- Uploading to Supabase ---")
    status = "success"
    upload_failed = False
    try:
        store_in_supabase(all_data, keywords_data, pages_data, run_id=run_id)
        if websites_failed and websites_succeeded:
            status = "partial"
        elif websites_failed and not websites_succeeded:
            status = "failed"
    except Exception as e:
        status = "failed"
        error_details["upload"] = str(e)
        upload_failed = True
        logger.error("Supabase upload failed: %s", str(e)[:220])
    finally:
        duration_seconds = int(time.time() - run_started)
        _finish_ingestion_run(
            run_id=run_id,
            status=status,
            websites_succeeded=websites_succeeded,
            websites_failed=websites_failed,
            error_details=error_details,
            duration_seconds=duration_seconds,
        )

    # Summary
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"  Websites with data: {len(all_data)}")
    print(f"  Websites with keywords: {len(keywords_data)}")
    print(f"  Websites with pages: {len(pages_data)}")
    print("")
    print("  Per-website source status:")
    for name in all_website_names:
        rep = site_report.get(name, {})
        print(
            f"  {name:15s} | "
            f"GSC: {_status_text(rep.get('gsc_status', 'fail'), rep.get('gsc_reason', '')):35s} | "
            f"GA4: {_status_text(rep.get('ga4_status', 'fail'), rep.get('ga4_reason', '')):35s} | "
            f"{rep.get('days', 0):3d} days | {rep.get('keywords', 0):3d} keywords | {rep.get('pages', 0):3d} pages"
        )

    gsc_ok = sum(1 for r in site_report.values() if r.get("gsc_status") == "ok")
    ga4_ok = sum(1 for r in site_report.values() if r.get("ga4_status") == "ok")
    print("")
    print(f"  GSC success: {gsc_ok}/{len(all_website_names)}")
    print(f"  GA4 success: {ga4_ok}/{len(all_website_names)}")
    print()

    if upload_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
