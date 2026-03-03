"""
Process Ahrefs CSVs from Supabase Storage and upload parsed data to Supabase DB.

This runs in GitHub Actions after CSVs are uploaded to storage.

Usage:
    python scripts/process_ahrefs.py --date-folder 2026-03-03

What it does:
  1. Downloads CSV/TXT files from Supabase Storage (ahrefs-exports bucket)
  2. Parses overview .txt files (domain metrics, DR, backlinks, etc.)
  3. Parses 5 CSV types (organic keywords, referring domains, top pages,
     broken backlinks, organic competitors)
  4. Uploads all parsed data to Supabase database tables
"""

import os
import sys
import re
import csv
import io
import time
import json
import uuid
import logging
import argparse
import tempfile
import requests
from datetime import date
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

sys.path.insert(0, os.path.dirname(__file__))
from config import SUPABASE_URL, SUPABASE_SERVICE_KEY, AHREFS_BUCKET

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("process_ahrefs")

# ── Domain mapping ────────────────────────────────────────────
WEBSITE_MAP = {
    "businessabc": "BusinessABC",
    "citiesabc": "CitiesABC",
    "fashionabc": "FashionABC",
    "freedomx": "FreedomX",
    "hedgethink": "HedgeThink",
    "intelligenthq": "IntelligentHQ",
    "sportsdna": "SportsDNA",
    "tradersdna": "TradersDNA",
    "wisdomia": "Wisdomia",
}

_CANONICAL_NAME_LOOKUP = {v.lower(): v for v in WEBSITE_MAP.values()}
_NORMALIZED_LOOKUP = {
    re.sub(r"[^a-z0-9]+", "", k.lower()): v
    for k, v in {**WEBSITE_MAP, **_CANONICAL_NAME_LOOKUP}.items()
}


def detect_website(filename):
    """Detect website from filename."""
    f = filename.lower()
    for slug, name in WEBSITE_MAP.items():
        if slug in f:
            return name

    # Ahrefs_Overview_<WebsiteName>_YYYY-MM-DD.txt
    m = re.search(r"ahrefs_overview_([^_]+)_\d{4}-\d{2}-\d{2}\.txt$", f)
    if m:
        key = re.sub(r"[^a-z0-9]+", "", m.group(1))
        if key in _NORMALIZED_LOOKUP:
            return _NORMALIZED_LOOKUP[key]

    # Domain-based filenames (e.g. www.businessabc.net-...)
    m = re.match(r"(?:www\.)?([a-z0-9-]+)\.(?:com|net|org|ai|io)", f)
    if m:
        key = re.sub(r"[^a-z0-9]+", "", m.group(1))
        if key in _NORMALIZED_LOOKUP:
            return _NORMALIZED_LOOKUP[key]

    return None


def categorize_file(filename):
    """Determine the file type from its name."""
    f = filename.lower()
    if f.startswith("ahrefs_overview") and f.endswith(".txt"):
        return "overview"
    if "broken-backlinks" in f:
        return "broken_backlinks"
    if "refdomains" in f:
        return "referring_domains"
    if "organic-keywords" in f:
        return "organic_keywords"
    if "top-pages" in f:
        return "top_pages"
    if "orgcompetitors" in f and "map" not in f:
        return "organic_competitors"
    return None


def extract_snapshot_date(filename: str) -> str:
    """Extract YYYY-MM-DD snapshot date from filename.

    This date is the data snapshot date and must be preserved across reruns to
    keep upserts idempotent. It is intentionally different from ingestion date.
    """
    m = re.search(r"(20\d{2}-\d{2}-\d{2})", filename)
    return m.group(1) if m else date.today().isoformat()


def _is_retryable_api_error(exc: Exception) -> bool:
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
    return any(marker in msg for marker in retry_markers)


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception(_is_retryable_api_error),
)
def _http_post(url: str, **kwargs):
    return requests.post(url, **kwargs)


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception(_is_retryable_api_error),
)
def _http_get(url: str, **kwargs):
    return requests.get(url, **kwargs)


# ══════════════════════════════════════════════════════════════
#  Download from Supabase Storage
# ══════════════════════════════════════════════════════════════

def download_from_storage(date_folder: str, temp_dir: str) -> list:
    """Download all files from Supabase Storage bucket.
    
    Tries flat (root-level) files first (current upload format),
    then falls back to date-prefixed folders (legacy format).
    """
    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "apikey": SUPABASE_SERVICE_KEY,
    }
    list_url = f"{SUPABASE_URL}/storage/v1/object/list/{AHREFS_BUCKET}"

    # Try root-level first (flat upload)
    resp = _http_post(
        list_url,
        headers=headers,
        json={"prefix": "", "limit": 200},
        timeout=30,
    )

    if resp.status_code != 200:
        logger.error("Failed to list files: HTTP %d — %s", resp.status_code, resp.text[:100])
        return []

    items = resp.json()
    # Filter to actual files (have metadata/id, not just folder entries)
    file_items = [f for f in items if f.get("name", "").endswith((".csv", ".txt"))]

    # If no flat files, try date-prefixed folder
    if not file_items:
        logger.info("No root-level files, trying prefix: %s/", date_folder)
        resp = _http_post(
            list_url,
            headers=headers,
            json={"prefix": f"{date_folder}/", "limit": 200},
            timeout=30,
        )
        if resp.status_code == 200:
            file_items = resp.json()
            prefix = f"{date_folder}/"
        else:
            return []
    else:
        prefix = ""

    logger.info("Found %d files in storage bucket", len(file_items))

    downloaded = []
    for file_info in file_items:
        name = file_info.get("name", "")
        if not name:
            continue

        storage_path = f"{prefix}{name}" if prefix else name
        download_url = f"{SUPABASE_URL}/storage/v1/object/{AHREFS_BUCKET}/{storage_path}"

        try:
            resp = _http_get(download_url, headers=headers, timeout=120)
            if resp.status_code == 200:
                local_path = os.path.join(temp_dir, name)
                with open(local_path, "wb") as f:
                    f.write(resp.content)
                downloaded.append(local_path)
                mb = len(resp.content) / 1024 / 1024
                logger.info("  ✅ %s (%.2f MB)", name, mb)
            else:
                logger.error("  ❌ %s — HTTP %d", name, resp.status_code)
        except Exception as e:
            logger.error("  ❌ %s — %s", name, str(e)[:100], exc_info=True)

    return downloaded


# ══════════════════════════════════════════════════════════════
#  Parsers (adapted from ahrefs_processor.py)
# ══════════════════════════════════════════════════════════════

def _parse_number(text):
    """Convert '25.3K' → 25300, '86.5K' → 86500, '1.1K' → 1100."""
    if not text:
        return None
    text = str(text).strip().replace(",", "").replace("$", "")
    if not text or text == "-":
        return None
    multiplier = 1
    if text.upper().endswith("K"):
        multiplier = 1000
        text = text[:-1]
    elif text.upper().endswith("M"):
        multiplier = 1000000
        text = text[:-1]
    elif text.upper().endswith("B"):
        multiplier = 1000000000
        text = text[:-1]
    try:
        return int(float(text) * multiplier)
    except ValueError:
        return None


def _read_ahrefs_csv(filepath):
    """Read Ahrefs CSV (UTF-16 tab-separated)."""
    encodings = ["utf-16", "utf-16-le", "utf-8-sig", "utf-8"]
    for enc in encodings:
        try:
            with open(filepath, "r", encoding=enc) as f:
                content = f.read()
            reader = csv.DictReader(io.StringIO(content), delimiter="\t")
            return list(reader)
        except (UnicodeError, UnicodeDecodeError):
            continue
    return []


def parse_overview_txt(filepath, website_name):
    """Parse an overview .txt file. Returns a dict."""
    text = None
    for enc in ("utf-8", "utf-8-sig", "utf-16", "latin-1"):
        try:
            with open(filepath, "r", encoding=enc) as f:
                text = f.read()
            break
        except Exception:
            continue
    if text is None:
        return None

    def extract(pattern, group=1):
        m = re.search(pattern, text, re.IGNORECASE)
        return m.group(group).strip() if m else None

    def extract_delta(label):
        m = re.search(rf"{label}:\s*([\d,.KMBkmb]+)\s*\(delta:\s*([+-]?[\d,.KMBkmb]+)\)", text, re.IGNORECASE)
        if m:
            return _parse_number(m.group(1)), _parse_number(m.group(2))
        m = re.search(rf"{label}:\s*([\d,.KMBkmb]+)", text, re.IGNORECASE)
        if m:
            return _parse_number(m.group(1)), None
        return None, None

    # Extract date from filename
    file_date = extract_snapshot_date(os.path.basename(filepath))

    dr_val, dr_delta = extract_delta("DR")
    ur_val, ur_delta = extract_delta("UR")
    ref_val, ref_delta = extract_delta("Referring domains")
    traffic_val, traffic_delta = extract_delta("Organic traffic")

    return {
        "date": file_date,
        "website": website_name,
        "source_file": os.path.basename(filepath),
        "domain": extract(r"Domain:\s*(\S+)"),
        "dr": dr_val,
        "dr_delta": dr_delta,
        "ur": ur_val,
        "ur_delta": ur_delta,
        "ahrefs_rank": _parse_number(
            extract(r"Ahrefs Rank:\s*([\d,.]+)") or
            extract(r"AR:\s*([\d,.]+)")
        ),
        "backlinks": _parse_number(extract(r"Backlinks:\s*([\d,.KMBkmb]+)")),
        "ref_domains": ref_val,
        "ref_domains_delta": ref_delta,
        "organic_keywords": _parse_number(extract(r"Organic keywords:\s*([\d,.KMBkmb]+)")),
        "organic_traffic": traffic_val,
        "organic_traffic_delta": traffic_delta,
        "traffic_value": _parse_number(extract(r"Traffic value:\s*\$?([\d,.KMBkmb]+)")),
    }


def parse_organic_keywords(filepath, website):
    """Parse organic keywords CSV. Returns top 200 keywords."""
    rows = _read_ahrefs_csv(filepath)
    snapshot_date = extract_snapshot_date(os.path.basename(filepath))
    keywords = []
    for row in rows[:200]:
        keywords.append({
            "keyword": row.get("Keyword", row.get("keyword", "")),
            "volume": _parse_number(row.get("Volume", row.get("Search volume", 0))),
            "kd": _parse_number(row.get("KD", row.get("Keyword Difficulty", 0))),
            "position": _parse_number(row.get("Position", row.get("Current position", 0))),
            "traffic": _parse_number(row.get("Traffic", row.get("Estimated traffic", 0))),
        })
    return {
        "website": website,
        "date": snapshot_date,
        "source_file": os.path.basename(filepath),
        "total": len(rows),
        "keywords": keywords,
    }


def parse_referring_domains(filepath, website):
    """Parse referring domains CSV. Returns top 500 domains."""
    rows = _read_ahrefs_csv(filepath)
    snapshot_date = extract_snapshot_date(os.path.basename(filepath))
    domains = []
    for row in rows[:500]:
        domains.append({
            "domain": row.get("Referring domain", row.get("Domain", "")),
            "dr": _parse_number(row.get("Domain Rating", row.get("DR", 0))),
            "dofollow_links": _parse_number(row.get("Dofollow ref. domains", row.get("Dofollow links", 0))),
            "links_to_target": _parse_number(row.get("Links to target", 0)),
            "first_seen": row.get("First seen", ""),
        })
    return {
        "website": website,
        "date": snapshot_date,
        "source_file": os.path.basename(filepath),
        "total": len(rows),
        "domains": domains,
    }


def parse_top_pages(filepath, website):
    """Parse top pages CSV. Returns top 100 pages."""
    rows = _read_ahrefs_csv(filepath)
    snapshot_date = extract_snapshot_date(os.path.basename(filepath))
    pages = []
    for row in rows[:100]:
        pages.append({
            "url": row.get("URL", row.get("Page", "")),
            "traffic": _parse_number(row.get("Traffic", row.get("Organic traffic", 0))),
            "keywords_count": _parse_number(row.get("Keywords", row.get("Number of keywords", 0))),
            "top_keyword": row.get("Top keyword", ""),
        })
    return {
        "website": website,
        "date": snapshot_date,
        "source_file": os.path.basename(filepath),
        "total": len(rows),
        "pages": pages,
    }


def parse_broken_backlinks(filepath, website):
    """Parse broken backlinks CSV. Returns top 200 links."""
    rows = _read_ahrefs_csv(filepath)
    snapshot_date = extract_snapshot_date(os.path.basename(filepath))
    links = []
    for row in rows[:200]:
        links.append({
            "referring_url": row.get("Referring page URL", row.get("Source URL", "")),
            "target_url": row.get("URL (target link)", row.get("Target URL", "")),
            "http_code": _parse_number(row.get("HTTP code", row.get("Response code", 0))),
            "anchor": row.get("Anchor", row.get("Link anchor", "")),
            "dr": _parse_number(row.get("Domain Rating", row.get("DR", 0))),
        })
    return {
        "website": website,
        "date": snapshot_date,
        "source_file": os.path.basename(filepath),
        "total": len(rows),
        "links": links,
    }


def parse_competitors(filepath, website):
    """Parse organic competitors CSV."""
    rows = _read_ahrefs_csv(filepath)
    snapshot_date = extract_snapshot_date(os.path.basename(filepath))
    competitors = []
    for row in rows[:50]:
        competitors.append({
            "domain": row.get("Competitor", row.get("Domain", "")),
            "common_keywords": _parse_number(row.get("Common keywords", 0)),
            "share": row.get("Share", ""),
            "competitor_keywords": _parse_number(row.get("SE keywords", row.get("Keywords", 0))),
        })
    return {
        "website": website,
        "date": snapshot_date,
        "source_file": os.path.basename(filepath),
        "total": len(rows),
        "competitors": competitors,
    }


# ══════════════════════════════════════════════════════════════
#  Supabase DB upload
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
                logger.error("  %s batch error: %s", table, msg)
                break

        if not chunk_ok:
            # Fallback: row-wise upsert for resilience, still rate-limited.
            for row in chunk:
                try:
                    client.table(table).upsert([row], on_conflict=conflict_cols).execute()
                    upserted += 1
                except Exception as e:
                    logger.warning("  %s row upsert failed: %s", table, str(e)[:180])
                time.sleep(0.05)

        # Steady pacing to avoid hitting API limits.
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
    """Create a run record; best effort only."""
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


def upload_parsed_data(parsed_data, run_id: str | None = None):
    """Upload all parsed Ahrefs data to Supabase."""
    from supabase import create_client

    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.error("Supabase credentials not set")
        return
    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    overview_has_source_file = _supports_column(client, "ahrefs_overview", "source_file")
    ref_domains_has_source_file = _supports_column(client, "ahrefs_referring_domains", "source_file")
    broken_backlinks_has_source_file = _supports_column(client, "ahrefs_broken_backlinks", "source_file")
    overview_has_ingestion_run_id = _supports_column(client, "ahrefs_overview", "ingestion_run_id")
    keyword_has_ingestion_run_id = _supports_column(client, "website_keywords", "ingestion_run_id")
    pages_has_ingestion_run_id = _supports_column(client, "website_pages", "ingestion_run_id")
    ref_domains_has_ingestion_run_id = _supports_column(client, "ahrefs_referring_domains", "ingestion_run_id")
    broken_backlinks_has_ingestion_run_id = _supports_column(client, "ahrefs_broken_backlinks", "ingestion_run_id")
    competitors_has_ingestion_run_id = _supports_column(client, "ahrefs_competitors", "ingestion_run_id")

    # Overviews
    ov_rows = []
    for ws, data in parsed_data.items():
        ov = data.get("overview")
        if ov:
            snapshot_date = ov.get("date")
            if not snapshot_date:
                logger.warning("  ahrefs_overview skipped for %s (missing snapshot date)", ws)
                continue
            row = {k: v for k, v in ov.items() if v is not None}
            if run_id and overview_has_ingestion_run_id:
                row["ingestion_run_id"] = run_id
            if not overview_has_source_file:
                row.pop("source_file", None)
            ov_rows.append(row)
    c = batch_upsert(client, "ahrefs_overview", ov_rows, "date,website")
    logger.info("  ahrefs_overview: %d upserted", c)

    # Keywords
    kw_rows = []
    for ws, data in parsed_data.items():
        ok = data.get("organic_keywords")
        if ok:
            snapshot_date = ok.get("date")
            if not snapshot_date:
                logger.warning("  ahrefs_keywords skipped for %s (missing snapshot date)", ws)
                continue
            for kw in ok.get("keywords", [])[:200]:
                kw_rows.append({k: v for k, v in {
                    "date": snapshot_date, "website": ws, "keyword": kw.get("keyword"),
                    "clicks": kw.get("traffic") or 0, "impressions": kw.get("volume") or 0,
                    "position": kw.get("position"), "search_volume": kw.get("volume"),
                    "keyword_difficulty": kw.get("kd"), "traffic_estimate": kw.get("traffic"),
                    "source": "ahrefs",
                }.items() if v is not None})
                if run_id and keyword_has_ingestion_run_id:
                    kw_rows[-1]["ingestion_run_id"] = run_id
    c = batch_upsert(client, "website_keywords", kw_rows, "date,website,keyword,source")
    logger.info("  ahrefs_keywords: %d upserted", c)

    # Top pages
    pg_rows = []
    for ws, data in parsed_data.items():
        tp = data.get("top_pages")
        if tp:
            snapshot_date = tp.get("date")
            if not snapshot_date:
                logger.warning("  ahrefs_pages skipped for %s (missing snapshot date)", ws)
                continue
            for pg in tp.get("pages", [])[:100]:
                pg_rows.append({k: v for k, v in {
                    "date": snapshot_date, "website": ws, "url": pg.get("url"),
                    "clicks": pg.get("traffic") or 0, "traffic_ahrefs": pg.get("traffic"),
                    "keywords_count": pg.get("keywords_count"), "top_keyword": pg.get("top_keyword"),
                    "source": "ahrefs",
                }.items() if v is not None})
                if run_id and pages_has_ingestion_run_id:
                    pg_rows[-1]["ingestion_run_id"] = run_id
    c = batch_upsert(client, "website_pages", pg_rows, "date,website,url,source")
    logger.info("  ahrefs_pages: %d upserted", c)

    # Referring domains
    rd_rows = []
    for ws, data in parsed_data.items():
        rd = data.get("referring_domains")
        if rd:
            snapshot_date = rd.get("date")
            if not snapshot_date:
                logger.warning("  ahrefs_referring_domains skipped for %s (missing snapshot date)", ws)
                continue
            for dom in rd.get("domains", [])[:500]:
                rd_row = {k: v for k, v in {
                    "date": snapshot_date, "website": ws, "domain": dom.get("domain"),
                    "dr": dom.get("dr"), "dofollow_links": dom.get("dofollow_links"),
                    "links_to_target": dom.get("links_to_target"), "first_seen": dom.get("first_seen"),
                    "source_file": rd.get("source_file"),
                }.items() if v is not None}
                if not ref_domains_has_source_file:
                    rd_row.pop("source_file", None)
                if run_id and ref_domains_has_ingestion_run_id:
                    rd_row["ingestion_run_id"] = run_id
                rd_rows.append(rd_row)
    c = batch_upsert(client, "ahrefs_referring_domains", rd_rows, "date,website,domain")
    logger.info("  ahrefs_referring_domains: %d upserted", c)

    # Broken backlinks
    bb_rows = []
    for ws, data in parsed_data.items():
        bb = data.get("broken_backlinks")
        if bb:
            snapshot_date = bb.get("date")
            if not snapshot_date:
                logger.warning("  ahrefs_broken_backlinks skipped for %s (missing snapshot date)", ws)
                continue
            for link in bb.get("links", [])[:200]:
                http_code = None
                try:
                    http_code = int(link.get("http_code", 0))
                except (ValueError, TypeError):
                    pass
                bb_row = {k: v for k, v in {
                    "date": snapshot_date, "website": ws,
                    "referring_page": link.get("referring_url"), "target_url": link.get("target_url"),
                    "http_code": http_code, "anchor_text": link.get("anchor"),
                    "ref_domain_dr": link.get("dr"),
                    "source_file": bb.get("source_file"),
                }.items() if v is not None}
                if not broken_backlinks_has_source_file:
                    bb_row.pop("source_file", None)
                if run_id and broken_backlinks_has_ingestion_run_id:
                    bb_row["ingestion_run_id"] = run_id
                bb_rows.append(bb_row)
    c = batch_upsert(client, "ahrefs_broken_backlinks", bb_rows, "date,website,referring_page")
    logger.info("  ahrefs_broken_backlinks: %d upserted", c)

    # Competitors
    comp_rows = []
    for ws, data in parsed_data.items():
        comp = data.get("organic_competitors")
        if comp:
            snapshot_date = comp.get("date")
            if not snapshot_date:
                logger.warning("  ahrefs_competitors skipped for %s (missing snapshot date)", ws)
                continue
            for rank, c_item in enumerate(comp.get("competitors", []), 1):
                comp_rows.append({k: v for k, v in {
                    "date": snapshot_date, "website": ws,
                    "competitor_domain": c_item.get("domain"),
                    "keyword_overlap": c_item.get("common_keywords"),
                    "share_pct": c_item.get("share"),
                    "competitor_keywords": c_item.get("competitor_keywords"),
                    "rank_order": rank,
                }.items() if v is not None})
                if run_id and competitors_has_ingestion_run_id:
                    comp_rows[-1]["ingestion_run_id"] = run_id
    c = batch_upsert(client, "ahrefs_competitors", comp_rows, "date,website,competitor_domain")
    logger.info("  ahrefs_competitors: %d upserted", c)


# ══════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Process Ahrefs CSVs from Supabase Storage")
    parser.add_argument("--date-folder", default=date.today().isoformat(),
                        help="Date folder in storage (default: today)")
    parser.add_argument("--local-dir", default=None,
                        help="Process from local directory instead of downloading from storage")
    args = parser.parse_args()

    print()
    print("=" * 60)
    print("  PROCESS AHREFS CSVs")
    print(f"  Date folder: {args.date_folder}")
    print("=" * 60)
    run_started = time.time()

    # Get files
    if args.local_dir:
        temp_dir = args.local_dir
        files = [os.path.join(temp_dir, f) for f in os.listdir(temp_dir)
                 if f.endswith(".csv") or f.endswith(".txt")]
        logger.info("Processing %d files from local: %s", len(files), temp_dir)
    else:
        temp_dir = tempfile.mkdtemp(prefix="ahrefs_")
        logger.info("Downloading files from storage...")
        files = download_from_storage(args.date_folder, temp_dir)

    if not files:
        logger.error("No files to process")
        sys.exit(1)

    # Parse files
    print("\n--- Parsing files ---")
    parsed_data = {}
    parse_errors = {}

    for filepath in sorted(files):
        filename = os.path.basename(filepath)
        website = detect_website(filename)
        category = categorize_file(filename)

        if not website or not category:
            logger.debug("Skipping unrecognized: %s", filename)
            continue

        if website not in parsed_data:
            parsed_data[website] = {}

        try:
            if category == "overview":
                parsed_data[website]["overview"] = parse_overview_txt(filepath, website)
            elif category == "organic_keywords":
                parsed_data[website]["organic_keywords"] = parse_organic_keywords(filepath, website)
            elif category == "referring_domains":
                parsed_data[website]["referring_domains"] = parse_referring_domains(filepath, website)
            elif category == "top_pages":
                parsed_data[website]["top_pages"] = parse_top_pages(filepath, website)
            elif category == "broken_backlinks":
                parsed_data[website]["broken_backlinks"] = parse_broken_backlinks(filepath, website)
            elif category == "organic_competitors":
                parsed_data[website]["organic_competitors"] = parse_competitors(filepath, website)

            logger.info("  ✅ %s → %s / %s", filename, website, category)
        except Exception as e:
            logger.error("  ❌ %s — %s", filename, str(e)[:80], exc_info=True)
            parse_errors[filename] = str(e)

    # Summary
    print(f"\nParsed data for {len(parsed_data)} websites:")
    for ws in sorted(parsed_data.keys()):
        sections = list(parsed_data[ws].keys())
        print(f"  {ws}: {', '.join(sections)}")
    websites_attempted = sorted(parsed_data.keys())
    run_id = _start_ingestion_run("ahrefs", websites_attempted)

    # Upload to Supabase
    print("\n--- Uploading to Supabase ---")
    status = "success"
    websites_succeeded = []
    websites_failed = []
    try:
        upload_parsed_data(parsed_data, run_id=run_id)
        for ws, sections in parsed_data.items():
            if sections:
                websites_succeeded.append(ws)
            else:
                websites_failed.append(ws)
        if websites_failed and websites_succeeded:
            status = "partial"
        elif websites_failed and not websites_succeeded:
            status = "failed"
    except Exception as e:
        status = "failed"
        parse_errors["upload"] = str(e)
        raise
    finally:
        duration_seconds = int(time.time() - run_started)
        _finish_ingestion_run(
            run_id=run_id,
            status=status,
            websites_succeeded=sorted(websites_succeeded),
            websites_failed=sorted(websites_failed),
            error_details=parse_errors,
            duration_seconds=duration_seconds,
        )

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
