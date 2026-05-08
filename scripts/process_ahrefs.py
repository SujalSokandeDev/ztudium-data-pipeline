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
import hashlib
import logging
import argparse
import tempfile
import requests
from datetime import date, datetime
from urllib.parse import urlparse
from collections import defaultdict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

sys.path.insert(0, os.path.dirname(__file__))
from config import SUPABASE_URL, SUPABASE_SERVICE_KEY, AHREFS_BUCKET, WEBSITES

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - available in normal runtime
    load_dotenv = None

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover - available in normal runtime
    OpenAI = None

from ai_client import get_ai_client, ai_json_response as _ai_json_response_fallback, ai_chat_completion

if load_dotenv:
    load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("process_ahrefs")

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
INTERNAL_LINKING_MODEL = "gpt-4o"
INTERNAL_LINKING_BYPASS_LAYER2 = os.getenv("INTERNAL_LINKING_BYPASS_LAYER2", "").strip().lower() in {"1", "true", "yes"}
INTERNAL_LINKING_DEBUG_PAYLOAD = os.getenv("INTERNAL_LINKING_DEBUG_PAYLOAD", "").strip().lower() in {"1", "true", "yes"}
# AI client with automatic Gemini fallback (managed by ai_client module)
openai_client = get_ai_client()
_layer1_debug_logged = False

SITE_CONTEXT = {
    site["name"]: {
        "category": site.get("category", ""),
        "audience": site.get("audience", ""),
    }
    for site in WEBSITES
}

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
    if "internal-links" in f:
        return "internal_links"
    if "backlinks" in f and "broken-backlinks" not in f:
        return "backlinks"
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


def _parse_date_text(value):
    """Parse Ahrefs date/datetime text into YYYY-MM-DD."""
    raw = str(value or "").strip()
    if not raw:
        return None
    raw = raw.replace("/", "-")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", "%d-%m-%Y %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    if re.match(r"^\d{4}-\d{2}-\d{2}", raw):
        return raw[:10]
    return None


def _normalize_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    scheme = parsed.scheme.lower() if parsed.scheme else "https"
    host = parsed.netloc.lower().replace("www.", "")
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")
    return f"{scheme}://{host}{path}"


def _url_path_tokens(url: str) -> set[str]:
    path = urlparse(url).path.lower()
    parts = [part for part in re.split(r"[/\-_]+", path) if part]
    stop = {"page", "pages", "blog", "wiki", "news", "article", "category", "tag"}
    return {part for part in parts if len(part) > 2 and part not in stop}


def _text_tokens(text: str) -> set[str]:
    raw = (text or "").lower()
    stop = {
        "the", "and", "for", "with", "from", "that", "this", "into", "your",
        "what", "when", "where", "which", "about", "their", "have", "will",
        "after", "before", "over", "under", "than", "then", "them", "they",
    }
    return {
        token for token in re.split(r"[^a-z0-9]+", raw)
        if len(token) > 2 and token not in stop
    }


def _first_path_segment(url: str) -> str:
    parts = [part for part in urlparse(url).path.lower().split("/") if part]
    return parts[0] if parts else ""


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
    """Parse organic keywords CSV."""
    rows = _read_ahrefs_csv(filepath)
    snapshot_date = extract_snapshot_date(os.path.basename(filepath))
    keywords = []
    for row in rows:
        keywords.append({
            "keyword": row.get("Keyword", row.get("keyword", "")),
            "volume": _parse_number(row.get("Volume", row.get("Search volume", 0))),
            "kd": _parse_number(row.get("KD", row.get("Keyword Difficulty", 0))),
            "position": _parse_number(row.get("Position", row.get("Current position", 0))),
            "traffic": _parse_number(row.get("Organic traffic", row.get("Traffic", row.get("Estimated traffic", 0)))),
            "url": row.get("Current URL", row.get("URL", "")),
        })
    return {
        "website": website,
        "date": snapshot_date,
        "source_file": os.path.basename(filepath),
        "total": len(rows),
        "keywords": keywords,
    }


def parse_referring_domains(filepath, website):
    """Parse referring domains CSV."""
    rows = _read_ahrefs_csv(filepath)
    snapshot_date = extract_snapshot_date(os.path.basename(filepath))
    domains = []
    for row in rows:
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
    """Parse top pages CSV."""
    rows = _read_ahrefs_csv(filepath)
    snapshot_date = extract_snapshot_date(os.path.basename(filepath))
    pages = []
    for row in rows:
        pages.append({
            "url": row.get("URL", row.get("Page", "")),
            "traffic": _parse_number(row.get("Current traffic", row.get("Traffic", row.get("Organic traffic", 0)))),
            "keywords_count": _parse_number(row.get("Current # of keywords", row.get("Keywords", row.get("Number of keywords", 0)))),
            "top_keyword": row.get("Current top keyword", row.get("Top keyword", "")),
            "top_keyword_volume": _parse_number(row.get("Current top keyword: Volume", row.get("Top keyword: Volume", 0))),
            "title": row.get("Title", row.get("Page title", "")),
            "meta_description": row.get("Meta description", row.get("Description", "")),
            "content_summary": row.get(
                "Content summary",
                row.get(
                    "Summary",
                    row.get(
                        "Excerpt",
                        row.get("First paragraph", row.get("Introduction", row.get("Snippet", ""))),
                    ),
                ),
            ),
            "ur": _parse_number(row.get("UR", 0)),
            "referring_domains": _parse_number(row.get("Current referring domains", 0)),
        })
    return {
        "website": website,
        "date": snapshot_date,
        "source_file": os.path.basename(filepath),
        "total": len(rows),
        "pages": pages,
    }


def parse_broken_backlinks(filepath, website):
    """Parse broken backlinks CSV."""
    rows = _read_ahrefs_csv(filepath)
    snapshot_date = extract_snapshot_date(os.path.basename(filepath))
    links = []
    for row in rows:
        links.append({
            "referring_url": row.get("Referring page URL", row.get("Source URL", "")),
            "target_url": row.get("URL (target link)", row.get("Target URL", "")),
            "http_code": _parse_number(row.get("Target page HTTP code", row.get("HTTP code", row.get("Response code", 0)))),
            "target_http_code": _parse_number(row.get("Target page HTTP code", row.get("HTTP code", row.get("Response code", 0)))),
            "referring_page_http_code": _parse_number(row.get("Referring page HTTP code", 0)),
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
    for row in rows:
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


def parse_backlinks(filepath, website):
    """Parse backlinks CSV and keep only rows with a lost date."""
    rows = _read_ahrefs_csv(filepath)
    snapshot_date = extract_snapshot_date(os.path.basename(filepath))
    lost_links = []
    for row in rows:
        lost_date = _parse_date_text(row.get("Lost"))
        if not lost_date:
            continue
        target_url = row.get("Target URL", row.get("URL (target link)", ""))
        referring_page_url = row.get("Referring page URL", "")
        if not target_url or not referring_page_url:
            continue
        lost_links.append({
            "website": website,
            "referring_page_url": referring_page_url,
            "domain_rating": _parse_number(row.get("Domain rating", row.get("Domain Rating", row.get("DR", 0)))),
            "target_url": target_url,
            "anchor": row.get("Anchor", ""),
            "first_seen": _parse_date_text(row.get("First seen")),
            "last_seen": _parse_date_text(row.get("Last seen")),
            "lost_date": lost_date,
            "drop_reason": (row.get("Lost status", "") or "").strip(),
        })
    return {
        "website": website,
        "date": snapshot_date,
        "source_file": os.path.basename(filepath),
        "total": len(rows),
        "lost_links": lost_links,
    }


def parse_internal_links(filepath, website):
    """Parse grouped-similar internal links CSV."""
    rows = _read_ahrefs_csv(filepath)
    snapshot_date = extract_snapshot_date(os.path.basename(filepath))
    internal_links = []
    for row in rows:
        source_url = row.get("Referring page URL", "")
        target_url = row.get("Target URL", "")
        if not source_url or not target_url:
            continue
        internal_links.append({
            "source_page": source_url,
            "target_url": target_url,
            "anchor": row.get("Anchor", ""),
            "page_traffic": _parse_number(row.get("Page traffic", 0)),
            "first_seen": _parse_date_text(row.get("First seen")),
            "last_seen": _parse_date_text(row.get("Last seen")),
        })
    return {
        "website": website,
        "date": snapshot_date,
        "source_file": os.path.basename(filepath),
        "total": len(rows),
        "internal_links": internal_links,
    }


def _select_target_candidates_for_thresholds(
    organic_keywords: dict,
    *,
    min_position: int,
    max_position: int,
    min_volume: int,
) -> list[dict]:
    """Select one best target keyword per target URL for the given thresholds."""
    best_by_url = {}
    for row in organic_keywords.get("keywords", []):
        url = _normalize_url(row.get("url", ""))
        position = row.get("position")
        volume = row.get("volume")
        keyword = (row.get("keyword") or "").strip()
        if not url or not keyword or position is None or volume is None:
            continue
        if position < min_position or position > max_position or volume < min_volume:
            continue

        current = best_by_url.get(url)
        candidate = {
            "target_page": url,
            "target_page_keyword": keyword,
            "target_page_position": int(position),
            "target_page_volume": int(volume),
        }
        if current is None:
            best_by_url[url] = candidate
            continue
        if candidate["target_page_volume"] > current["target_page_volume"]:
            best_by_url[url] = candidate
        elif candidate["target_page_volume"] == current["target_page_volume"] and candidate["target_page_position"] < current["target_page_position"]:
            best_by_url[url] = candidate
    return list(best_by_url.values())


def _select_target_candidates(organic_keywords: dict, site_name: str | None = None) -> list[dict]:
    """Select target candidates, with a low-data fallback for thin sites."""
    primary = _select_target_candidates_for_thresholds(
        organic_keywords,
        min_position=4,
        max_position=10,
        min_volume=1000,
    )
    if len(primary) >= 3:
        return primary

    fallback = _select_target_candidates_for_thresholds(
        organic_keywords,
        min_position=4,
        max_position=20,
        min_volume=500,
    )
    if site_name:
        logger.info(
            "  internal_linking[%s]: widened target eligibility from standard to fallback thresholds (primary=%d, fallback=%d)",
            site_name,
            len(primary),
            len(fallback),
        )
    return fallback


def _select_source_candidates(top_pages: dict, min_traffic: int = 500) -> list[dict]:
    """Select donor pages by traffic threshold with a fallback for lower-traffic sites."""
    all_pages = []
    seen = set()
    for row in top_pages.get("pages", []):
        url = _normalize_url(row.get("url", ""))
        traffic = row.get("traffic") or 0
        if not url or url in seen or traffic <= 0:
            continue
        seen.add(url)
        all_pages.append({
            "source_page": url,
            "source_page_traffic": int(traffic),
            "source_top_keyword": (row.get("top_keyword") or "").strip(),
        })

    all_pages.sort(key=lambda row: row["source_page_traffic"], reverse=True)

    strong_donors = [row for row in all_pages if row["source_page_traffic"] >= min_traffic]
    if len(strong_donors) >= 5:
        return strong_donors[:40]

    medium_donors = [row for row in all_pages if row["source_page_traffic"] >= 100]
    if medium_donors:
        return medium_donors[:40]

    return all_pages[:20]


def _humanize_token_text(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[-_/]+", " ", text or "")).strip().title()


def _infer_page_title(url: str, fallback_keyword: str = "") -> str:
    if fallback_keyword:
        return _humanize_token_text(fallback_keyword)
    parsed = urlparse(url)
    segments = [segment for segment in parsed.path.split("/") if segment]
    if segments:
        return _humanize_token_text(segments[-1])
    hostname = parsed.netloc.replace("www.", "")
    return _humanize_token_text(hostname.split(".")[0])


def _build_keyword_map(organic_keywords: dict) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for row in organic_keywords.get("keywords", []):
        url = _normalize_url(row.get("url", ""))
        keyword = (row.get("keyword") or "").strip()
        if not url or not keyword:
            continue
        grouped.setdefault(url, []).append({
            "keyword": keyword,
            "volume": int(row.get("volume") or 0),
            "traffic": int(row.get("traffic") or 0),
            "position": int(row.get("position") or 0),
        })

    for url, keywords in grouped.items():
        grouped[url] = sorted(
            keywords,
            key=lambda item: (
                item.get("volume") or 0,
                -(item.get("position") or 999),
                item.get("traffic") or 0,
                item.get("keyword") or "",
            ),
            reverse=True,
        )[:5]
    return grouped


def _build_page_enrichment(site_name: str, site_data: dict) -> dict[str, dict]:
    organic_keywords = site_data.get("organic_keywords") or {}
    top_pages = site_data.get("top_pages") or {}
    keywords_by_url = _build_keyword_map(organic_keywords)
    top_pages_by_url = {
        _normalize_url(row.get("url", "")): row
        for row in top_pages.get("pages", [])
        if _normalize_url(row.get("url", ""))
    }

    enriched: dict[str, dict] = {}
    site_context = SITE_CONTEXT.get(site_name, {"category": "", "audience": ""})
    for url in sorted(set(keywords_by_url.keys()) | set(top_pages_by_url.keys())):
        top_page = top_pages_by_url.get(url, {})
        top_keywords = keywords_by_url.get(url, [])
        primary_keyword = (
            (top_keywords[0]["keyword"] if top_keywords else "")
            or (top_page.get("top_keyword") or "").strip()
        )
        traffic = int(
            top_page.get("traffic")
            or (top_keywords[0].get("traffic") if top_keywords else 0)
            or 0
        )
        title = (top_page.get("title") or "").strip() or _infer_page_title(url, primary_keyword)
        meta_description = (top_page.get("meta_description") or "").strip()
        content_summary = (
            (top_page.get("content_summary") or "").strip()
            or meta_description
        )
        section = _first_path_segment(url)
        keyword_summaries = [
            {
                "keyword": item.get("keyword", ""),
                "volume": int(item.get("volume") or 0),
                "position": int(item.get("position") or 0),
            }
            for item in top_keywords[:5]
            if item.get("keyword")
        ]
        if not keyword_summaries and top_page.get("top_keyword"):
            keyword_summaries = [{
                "keyword": top_page.get("top_keyword", ""),
                "volume": int(top_page.get("top_keyword_volume") or 0),
                "position": 0,
            }]
        topic_tokens = (
            _url_path_tokens(url)
            | _text_tokens(title)
            | _text_tokens(meta_description)
            | _text_tokens(content_summary)
            | _text_tokens(primary_keyword)
        )
        for item in keyword_summaries:
            topic_tokens |= _text_tokens(item["keyword"])
        topic_tokens |= _text_tokens(site_context.get("category", ""))

        enriched[url] = {
            "website": site_name,
            "url": url,
            "title": title,
            "meta_description": meta_description,
            "content_summary": content_summary,
            "top_keywords": keyword_summaries,
            "traffic": traffic,
            "section": section,
            "topic_tokens": topic_tokens,
            "primary_keyword": primary_keyword,
            "site_category": site_context.get("category", ""),
            "site_audience": site_context.get("audience", ""),
        }

    return enriched


def _topic_related(source_page: str, target_page: str, target_keyword: str, source_top_keyword: str) -> bool:
    source_tokens = _url_path_tokens(source_page)
    target_tokens = _url_path_tokens(target_page)
    target_keyword_tokens = _text_tokens(target_keyword)
    source_keyword_tokens = _text_tokens(source_top_keyword)

    if source_tokens & target_tokens:
        return True
    if source_tokens & target_keyword_tokens:
        return True
    if source_keyword_tokens & target_keyword_tokens:
        return True
    if source_keyword_tokens & target_tokens:
        return True

    source_segment = _first_path_segment(source_page)
    target_segment = _first_path_segment(target_page)
    if source_segment and target_segment and source_segment == target_segment:
        return True
    return False


def _build_existing_link_pairs(internal_links: dict) -> set[tuple[str, str]]:
    pairs = set()
    for row in internal_links.get("internal_links", []):
        source = _normalize_url(row.get("source_page", ""))
        target = _normalize_url(row.get("target_url", ""))
        if source and target:
            pairs.add((source, target))
    return pairs


def _coerce_confidence(value) -> int | None:
    try:
        confidence = int(round(float(value)))
    except (TypeError, ValueError):
        return None
    return max(0, min(100, confidence))


def _split_reason_sentences(reason: str) -> list[str]:
    return [part.strip() for part in re.split(r"(?<=[.!?])\s+", (reason or "").strip()) if part.strip()]


def _is_meaningful_reason(reason: str) -> bool:
    text = (reason or "").strip()
    if not text or len(text) < 140:
        return False

    sentences = _split_reason_sentences(text)
    if len(sentences) < 3:
        return False

    lowered = text.lower()
    generic_phrases = [
        "both pages are related",
        "same broad topic",
        "same general theme",
        "same industry",
        "topically related",
        "would support that topic",
        "this link makes sense",
        "would be useful",
    ]
    return not any(phrase in lowered for phrase in generic_phrases)


def _service_tokens_from_url(url: str) -> set[str]:
    generic = {
        "login", "signin", "sign", "in", "account", "access", "portal", "customer",
        "service", "services", "phone", "number", "contact", "support", "help",
        "hours", "hour", "page", "pages", "the", "and", "for", "near", "me",
    }
    return {token for token in (_url_path_tokens(url) | _text_tokens(urlparse(url).netloc)) if token not in generic}


def _violates_hard_internal_link_rules(
    *,
    source_site: str,
    target_site: str,
    scope: str,
    source_page: str,
    target_page: str,
) -> bool:
    source_lower = (source_page or "").lower()
    target_lower = (target_page or "").lower()

    if (
        scope == "within_site"
        and source_site == "BusinessABC"
        and "/wiki/" in source_lower
        and "/wiki/" in target_lower
    ):
        return True

    source_has_login = "login" in source_lower
    target_has_contact_pattern = any(pattern in target_lower for pattern in ("phone", "contact", "customer"))
    if source_has_login and target_has_contact_pattern:
        if source_site != target_site:
            return True
        if not (_service_tokens_from_url(source_page) & _service_tokens_from_url(target_page)):
            return True

    return False


def _light_prefilter_donors(
    donors: list[dict],
    target: dict,
    page_map: dict[str, dict],
    max_candidates: int = 25,
) -> list[dict]:
    target_profile = page_map.get(target["target_page"]) or {}
    target_tokens = set(target_profile.get("topic_tokens") or set()) | _text_tokens(target["target_page_keyword"])
    target_section = target_profile.get("section") or _first_path_segment(target["target_page"])

    ranked = []
    for donor in donors:
        donor_profile = page_map.get(donor["source_page"]) or {}
        donor_tokens = set(donor_profile.get("topic_tokens") or set()) | _text_tokens(donor.get("source_top_keyword", ""))
        overlap = len(donor_tokens & target_tokens)
        same_section = bool(target_section and donor_profile.get("section") == target_section)
        if overlap == 0 and not same_section:
            continue
        heuristic = (overlap * 3) + (4 if same_section else 0) + min(int(donor.get("source_page_traffic") or 0) // 250, 8)
        ranked.append((heuristic, donor))

    ranked.sort(key=lambda item: item[0], reverse=True)
    filtered = [item[1] for item in ranked[:max_candidates]]
    if len(filtered) >= min(max_candidates, 8):
        return filtered

    seen = {row["source_page"] for row in filtered}
    for donor in donors:
        if donor["source_page"] in seen:
            continue
        filtered.append(donor)
        seen.add(donor["source_page"])
        if len(filtered) >= min(max_candidates, len(donors)):
            break
    return filtered[:max_candidates]


def _coerce_confidence(val: any) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _openai_json_response(system_prompt: str, payload: dict) -> dict:
    if not get_ai_client():
        raise RuntimeError("OPENAI_API_KEY or GEMINI_API_KEY is required for AI internal linking suggestions")
    return _ai_json_response_fallback(
        system_prompt,
        payload,
        model=INTERNAL_LINKING_MODEL,
        temperature=0.1,
    )


def _build_target_payload(target: dict, page_map: dict[str, dict], target_site: str) -> dict:
    profile = page_map.get(target["target_page"], {})
    return {
        "website": target_site,
        "url": target["target_page"],
        "title": profile.get("title") or _infer_page_title(target["target_page"], target["target_page_keyword"]),
        "meta_description": profile.get("meta_description") or "",
        "content_summary": profile.get("content_summary") or "",
        "top_keywords": profile.get("top_keywords") or [{"keyword": target["target_page_keyword"], "volume": target["target_page_volume"]}],
        "traffic": profile.get("traffic") or 0,
        "section": profile.get("section") or _first_path_segment(target["target_page"]),
        "primary_keyword": target["target_page_keyword"],
        "position": target["target_page_position"],
        "volume": target["target_page_volume"],
        "site_category": profile.get("site_category") or SITE_CONTEXT.get(target_site, {}).get("category", ""),
        "site_audience": profile.get("site_audience") or SITE_CONTEXT.get(target_site, {}).get("audience", ""),
    }


def _build_donor_payload(donor: dict, page_map: dict[str, dict], source_site: str) -> dict:
    profile = page_map.get(donor["source_page"], {})
    return {
        "website": source_site,
        "url": donor["source_page"],
        "title": profile.get("title") or _infer_page_title(donor["source_page"], donor.get("source_top_keyword", "")),
        "meta_description": profile.get("meta_description") or "",
        "content_summary": profile.get("content_summary") or "",
        "top_keywords": profile.get("top_keywords") or (
            [{"keyword": donor.get("source_top_keyword", ""), "volume": 0}] if donor.get("source_top_keyword") else []
        ),
        "traffic": donor.get("source_page_traffic") or profile.get("traffic") or 0,
        "section": profile.get("section") or _first_path_segment(donor["source_page"]),
        "primary_keyword": profile.get("primary_keyword") or donor.get("source_top_keyword", ""),
        "site_category": profile.get("site_category") or SITE_CONTEXT.get(source_site, {}).get("category", ""),
        "site_audience": profile.get("site_audience") or SITE_CONTEXT.get(source_site, {}).get("audience", ""),
    }


def _log_layer1_payload_once(source_site: str, target_site: str, target_payload: dict, donor_payloads: list[dict], scope: str) -> None:
    global _layer1_debug_logged
    if not INTERNAL_LINKING_DEBUG_PAYLOAD or _layer1_debug_logged:
        return

    sample_payload = {
        "scope": scope,
        "source_site": source_site,
        "target_site": target_site,
        "target_page": target_payload,
        "candidate_donor_pages": donor_payloads,
    }
    logger.info(
        "LAYER1 DEBUG PAYLOAD [%s] %s -> %s:\n%s",
        scope,
        source_site,
        target_site,
        json.dumps(sample_payload, ensure_ascii=False, indent=2),
    )
    _layer1_debug_logged = True


def _layer1_generate_internal_links(
    source_site: str,
    target_site: str,
    target_payload: dict,
    donor_payloads: list[dict],
    scope: str,
) -> list[dict]:
    payload = {
        "scope": scope,
        "source_site": source_site,
        "target_site": target_site,
        "target_page": target_payload,
        "candidate_donor_pages": donor_payloads,
        "instructions": {
            "max_suggestions": min(15, len(donor_payloads)),
            "confidence_scale": "0-100 integer",
            "anchor_rule": "Anchor text must be natural, descriptive, and fit the donor page context.",
            "reason_rule": (
                "Reason must be 3 to 4 sentences. Sentence 1 = exact topical connection. "
                "Sentences 2 to 4 = concrete reader benefit, content/keyword overlap, and editorial justification."
            ),
            "site_rule": "Respect what the source site and target site are about and who they serve.",
        },
    }
    _log_layer1_payload_once(source_site, target_site, target_payload, donor_payloads, scope)
    result = _openai_json_response(
        system_prompt=(
            "You are an internal linking strategist. "
            f"The donor pages are from {source_site} and the target page is on {target_site}. "
            "For cross-platform suggestions, the donor and target are on different sites — respect what each site is about and who it serves. "
            "You will be given full enriched context for one target page and candidate donor pages. "
            "Use the site niche, audience, page title, meta description, content summary, top keywords, traffic, and section context. "
            "Only suggest links that are editorially useful for a real reader. "
            "Do not guess what pages are about. Use only the evidence provided. "
            "If a connection relies on a clearly incorrect factual claim, such as assuming one company owns another when it does not, do not suggest it. "
            "For every suggestion, write a strong detailed reason. "
            "Sentence one must state the exact topical connection between the donor page and the target page. "
            "Then write two or three more sentences explaining why a real reader on the donor page would benefit from clicking through, "
            "what specific value they get, which keyword or content overlap supports the suggestion, "
            "and why the link is editorially justified in context. "
            "Never use vague phrases like 'both pages are related' or 'same broad topic'. "
            "Return JSON with a `suggestions` array only. "
            "Each suggestion must include: source_page, anchor_text, reason, confidence."
        ),
        payload=payload,
    )
    suggestions = result.get("suggestions") or []
    return suggestions if isinstance(suggestions, list) else []


def _layer2_validate_internal_links_batch(
    source_site: str,
    target_site: str,
    target_payload: dict,
    donor_payloads: dict[str, dict],
    candidates: list[dict],
    scope: str,
) -> list[dict]:
    review_payload = []
    for candidate in candidates:
        source_page = _normalize_url(candidate.get("source_page", ""))
        donor_payload = donor_payloads.get(source_page)
        if not donor_payload:
            continue
        review_payload.append({
            "donor_page": donor_payload,
            "candidate": candidate,
        })

    if not review_payload:
        return []

    result = _openai_json_response(
        system_prompt=(
            f"You are a skeptical SEO reviewer validating internal linking suggestions for the {source_site} website. "
            "Reject anything where the connection is only the same broad category, same industry, or same vague theme. "
            "Use your world knowledge about the companies, brands, people, and topics involved as an active fact-checking tool. "
            "If the suggested reason contains speculation, assumption, or any claim that cannot be verified from the page data plus your world knowledge, reject it immediately. "
            "If the suggested connection is factually wrong based on what you know about the real world, reject it immediately. "
            "Broad overlap in investment, startups, business, innovation, leadership, finance, or technology is not enough on its own. "
            "A contrast between two topics is also not enough to justify an internal link. "
            "Approve only when there is a specific shared entity, company, person, product, event, service, or a very clear reader need that is evident from the donor context. "
            "Reject any suggestion where the donor page URL contains 'login' and the target page is a phone, contact, or customer-service page, "
            "unless both pages are explicitly about the exact same domain and the exact same service. "
            "Reject any suggestion where the connection relies only on both pages being in the investment, tech, or business sector without a specific shared entity, person, or product. "
            "Reject any suggestion where the donor page is fundamentally a login, sign-in, account access, portal, phone number, hours, contact, or customer-service page "
            "and the target page is a different login/contact/help page, unless both pages are explicitly about the same service or same company. "
            "Approve only if there is a clear, specific, editorial reason a reader would benefit from following the link from this donor page to this target page. "
            "Ask yourself: would a senior editor at a real publication approve this link? "
            "If the answer is anything other than a clear yes, reject it. "
            "Only approve if confidence is 75 or above. If your confidence is below 75, reject the suggestion. "
            "If the anchor feels generic, forced, or weak, reject it. "
            "For cross-platform suggestions, if there is a clear shared audience or clear topic overlap across Ztudium ecosystem sites, "
            "that can be enough to approve even when the sites serve different verticals, but the editorial fit still has to be real. "
            "For approved suggestions, the reason must be a strong 3 to 4 sentence explanation. "
            "Sentence one must state the exact topical connection. "
            "The remaining sentences must explain the concrete reader benefit, the exact keyword or content overlap, "
            "and why the link fits naturally in the donor-page context. "
            "Do not approve generic language like 'both pages are related' or 'same broad topic'. "
            "Review every suggestion independently. "
            "Return JSON with a `results` array only. "
            "Each result must include: source_page, approved, confidence, reason, anchor_text. "
            "For rejected suggestions, one clear sentence is enough. For approved suggestions, use the full detailed format."
        ),
        payload={
            "scope": scope,
            "source_site": source_site,
            "target_site": target_site,
            "target_page": target_payload,
            "suggestions_to_review": review_payload,
            "instructions": {
                "task": "Review every suggestion in suggestions_to_review independently.",
                "confidence_scale": "0-100 integer",
                "approval_threshold": "Only approve if confidence is 75 or above.",
                "reason_rule": (
                    "Approved suggestions must have a 3 to 4 sentence reason with exact topical connection, "
                    "reader benefit, content overlap, and editorial fit. Rejected suggestions can use one sentence."
                ),
                "rejection_rule": (
                    "Reject if the only connection is that both pages are in the same industry, "
                    "same broad topic, or same general theme. "
                    "Reject any suggestion where the donor page URL contains 'login' and the target page is a phone, contact, or customer-service page, "
                    "unless both pages are explicitly about the exact same domain and the exact same service. "
                    "Reject login/account-access/portal pages linking to phone-number/contact/customer-service pages unless both are explicitly about the same service or company. "
                    "Reject investment/startup/business matches where the only connection is broad finance or investing language without a specific shared company, entity, event, or reader need. "
                    "Reject suggestions where the connection relies only on both pages being in the investment, tech, or business sector without a specific shared entity, person, or product. "
                    "Reject reasons that rely on contrast alone, such as saying one page offers a different perspective on the same broad business area. "
                    "Reject immediately if the reason relies on speculation, assumption, invented ownership, "
                    "invented product relationships, or any fact claim not supported by the page data or your world knowledge. "
                    "A valid approval requires a specific, clear reason why a real reader "
                    "on the donor page would benefit from clicking through to the target page. "
                    "If you cannot state that specific reason, reject it."
                ),
                "output_format": "Return JSON with a `results` array. Each item must include: source_page, approved, confidence, reason, anchor_text.",
            },
        },
    )
    results = result.get("results") or []
    return results if isinstance(results, list) else []


def _save_layer1_candidates_direct(
    *,
    source_site: str,
    target_site: str,
    target: dict,
    target_page: str,
    donor_rows: dict[str, dict],
    layer1_suggestions: list[dict],
    scope: str,
    seen: set,
    suggestions: list[dict],
) -> int:
    saved = 0
    for candidate in layer1_suggestions:
        source_page = _normalize_url(candidate.get("source_page", ""))
        donor_row = donor_rows.get(source_page)
        if not donor_row:
            continue

        dedupe_key = (scope, source_site, source_page, target_site, target_page, target["target_page_keyword"])
        if dedupe_key in seen:
            continue

        final_reason = (candidate.get("reason") or "").strip()
        final_anchor = (candidate.get("anchor_text") or target["target_page_keyword"]).strip()
        if not final_reason or not final_anchor:
            continue
        if _violates_hard_internal_link_rules(
            source_site=source_site,
            target_site=target_site,
            scope=scope,
            source_page=source_page,
            target_page=target_page,
        ):
            continue

        seen.add(dedupe_key)
        score = int(
            (donor_row["source_page_traffic"] / 100)
            + (100 - target["target_page_position"])
            + (target["target_page_volume"] / 1000)
        )
        suggestions.append({
            "website": source_site,
            "source_website": source_site,
            "target_website": target_site,
            "suggestion_scope": scope,
            "source_page": source_page,
            "source_page_traffic": donor_row["source_page_traffic"],
            "target_page": target_page,
            "target_page_keyword": target["target_page_keyword"],
            "target_page_position": target["target_page_position"],
            "target_page_volume": target["target_page_volume"],
            "suggested_anchor": final_anchor,
            "existing_link": False,
            "score": score,
            "status": "pending",
            "reason": final_reason,
            "ai_confidence": _coerce_confidence(candidate.get("confidence")),
        })
        saved += 1
    return saved


def _build_rule_based_suggestions(
    site_name: str,
    targets: list[dict],
    donors: list[dict],
    existing_pairs: set[tuple[str, str]] | None,
    target_site_name: str,
    scope: str,
    limit: int,
) -> list[dict]:
    suggestions = []
    seen = set()
    for target in targets:
        target_page = target["target_page"]
        for donor in donors:
            source_page = donor["source_page"]
            if source_page == target_page:
                continue
            if existing_pairs is not None and (source_page, target_page) in existing_pairs:
                continue
            if not _topic_related(
                source_page,
                target_page,
                target["target_page_keyword"],
                donor.get("source_top_keyword", ""),
            ):
                continue

            dedupe_key = (
                scope,
                site_name,
                source_page,
                target_site_name,
                target_page,
                target["target_page_keyword"],
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            score = int(
                (donor["source_page_traffic"] / 100)
                + (100 - target["target_page_position"])
                + (target["target_page_volume"] / 1000)
            )
            suggestions.append({
                "website": site_name,
                "source_website": site_name,
                "target_website": target_site_name,
                "suggestion_scope": scope,
                "source_page": source_page,
                "source_page_traffic": donor["source_page_traffic"],
                "target_page": target_page,
                "target_page_keyword": target["target_page_keyword"],
                "target_page_position": target["target_page_position"],
                "target_page_volume": target["target_page_volume"],
                "suggested_anchor": target["target_page_keyword"],
                "existing_link": False,
                "score": score,
                "status": "pending",
                "reason": (
                    f"Both pages align around {target['target_page_keyword']}, "
                    f"so linking {source_page} to {target_page} would support that topic."
                ),
                "ai_confidence": None,
            })
    suggestions.sort(key=lambda row: (row["score"], row["source_page_traffic"], row["target_page_volume"]), reverse=True)
    return suggestions[:limit]


def _generate_ai_suggestions_for_scope(
    source_site: str,
    target_site: str,
    targets: list[dict],
    donors: list[dict],
    source_page_map: dict[str, dict],
    target_page_map: dict[str, dict],
    scope: str,
    limit: int,
    existing_pairs: set[tuple[str, str]] | None = None,
) -> list[dict]:
    if not get_ai_client():
        raise RuntimeError("OPENAI_API_KEY or GEMINI_API_KEY is required for internal linking generation")

    logger.info(
        "  internal_linking[%s]: %s -> %s | %d targets, %d donor pages",
        scope,
        source_site,
        target_site,
        len(targets),
        len(donors),
    )
    if INTERNAL_LINKING_BYPASS_LAYER2:
        logger.info("  internal_linking[%s]: layer2 bypass enabled, saving raw layer1 output", scope)
    suggestions = []
    seen = set()
    minimum_scope_suggestions = min(4, limit)
    for index, target in enumerate(targets, start=1):
        target_page = target["target_page"]
        target_payload = _build_target_payload(target, target_page_map, target_site)
        candidate_donors = []
        for donor in _light_prefilter_donors(donors, target, source_page_map):
            source_page = donor["source_page"]
            if source_page == target_page:
                continue
            if existing_pairs is not None and (source_page, target_page) in existing_pairs:
                continue
            candidate_donors.append(donor)

        if not candidate_donors:
            logger.info(
                "    target %d/%d skipped (%s): no donor candidates after prefilter",
                index,
                len(targets),
                target_page,
            )
            continue

        donor_payloads = [_build_donor_payload(donor, source_page_map, source_site) for donor in candidate_donors]
        donor_lookup = {payload["url"]: payload for payload in donor_payloads}
        donor_rows = {donor["source_page"]: donor for donor in candidate_donors}
        logger.info(
            "    target %d/%d | %s | %d candidate donors",
            index,
            len(targets),
            target_page,
            len(candidate_donors),
        )

        try:
            layer1_suggestions = _layer1_generate_internal_links(
                source_site=source_site,
                target_site=target_site,
                target_payload=target_payload,
                donor_payloads=donor_payloads,
                scope=scope,
            )
        except Exception as exc:
            logger.warning(
                "AI internal linking layer 1 failed for %s → %s (%s): %s",
                source_site,
                target_site,
                target_page,
                str(exc)[:160],
            )
            continue
        logger.info(
            "      layer1 returned %d suggestions for %s",
            len(layer1_suggestions),
            target_page,
        )

        if INTERNAL_LINKING_BYPASS_LAYER2:
            saved_from_layer1 = _save_layer1_candidates_direct(
                source_site=source_site,
                target_site=target_site,
                target=target,
                target_page=target_page,
                donor_rows=donor_rows,
                layer1_suggestions=layer1_suggestions,
                scope=scope,
                seen=seen,
                suggestions=suggestions,
            )
            logger.info(
                "      layer2 bypass active: saved %d layer1 suggestions for %s",
                saved_from_layer1,
                target_page,
            )
            continue

        approved_for_target = 0
        try:
            validation_results = _layer2_validate_internal_links_batch(
                source_site=source_site,
                target_site=target_site,
                target_payload=target_payload,
                donor_payloads=donor_lookup,
                candidates=layer1_suggestions,
                scope=scope,
            )
        except Exception as exc:
            logger.warning(
                "AI internal linking layer 2 failed for %s → %s (%s): %s",
                source_site,
                target_site,
                target_page,
                str(exc)[:160],
            )
            continue

        layer1_lookup = {
            _normalize_url(candidate.get("source_page", "")): candidate
            for candidate in layer1_suggestions
            if _normalize_url(candidate.get("source_page", ""))
        }

        for validation in validation_results:
            source_page = _normalize_url(validation.get("source_page", ""))
            donor_row = donor_rows.get(source_page)
            candidate = layer1_lookup.get(source_page)
            if not donor_row or not candidate:
                continue

            dedupe_key = (scope, source_site, source_page, target_site, target_page, target["target_page_keyword"])
            if dedupe_key in seen:
                continue

            final_confidence = _coerce_confidence(validation.get("confidence"))
            if final_confidence is None:
                final_confidence = _coerce_confidence(candidate.get("confidence"))
            if final_confidence is None or final_confidence < 75:
                continue
            if not validation.get("approved"):
                continue

            final_reason = (validation.get("reason") or candidate.get("reason") or "").strip()
            final_anchor = (validation.get("anchor_text") or candidate.get("anchor_text") or target["target_page_keyword"]).strip()
            if not final_anchor or not _is_meaningful_reason(final_reason):
                continue
            if _violates_hard_internal_link_rules(
                source_site=source_site,
                target_site=target_site,
                scope=scope,
                source_page=source_page,
                target_page=target_page,
            ):
                continue

            seen.add(dedupe_key)
            score = int(
                (donor_row["source_page_traffic"] / 100)
                + (100 - target["target_page_position"])
                + (target["target_page_volume"] / 1000)
            )
            suggestions.append({
                "website": source_site,
                "source_website": source_site,
                "target_website": target_site,
                "suggestion_scope": scope,
                "source_page": source_page,
                "source_page_traffic": donor_row["source_page_traffic"],
                "target_page": target_page,
                "target_page_keyword": target["target_page_keyword"],
                "target_page_position": target["target_page_position"],
                "target_page_volume": target["target_page_volume"],
                "suggested_anchor": final_anchor,
                "existing_link": False,
                "score": score,
                "status": "pending",
                "reason": final_reason,
                "ai_confidence": final_confidence,
            })
            approved_for_target += 1

        logger.info(
            "      layer2 approved %d suggestions for %s",
            approved_for_target,
            target_page,
        )

    if len(suggestions) < minimum_scope_suggestions and donors:
        fallback_limit = min(40, max(25, len(donors)))
        logger.info(
            "  internal_linking[%s]: %s -> %s below floor (%d/%d). Running broader fallback pass with %d donors.",
            scope,
            source_site,
            target_site,
            len(suggestions),
            minimum_scope_suggestions,
            fallback_limit,
        )
        for index, target in enumerate(targets, start=1):
            if len(suggestions) >= minimum_scope_suggestions:
                break

            target_page = target["target_page"]
            target_payload = _build_target_payload(target, target_page_map, target_site)
            candidate_donors = []
            for donor in _light_prefilter_donors(donors, target, source_page_map, max_candidates=fallback_limit):
                source_page = donor["source_page"]
                if source_page == target_page:
                    continue
                if existing_pairs is not None and (source_page, target_page) in existing_pairs:
                    continue
                candidate_donors.append(donor)

            if not candidate_donors:
                continue

            donor_payloads = [_build_donor_payload(donor, source_page_map, source_site) for donor in candidate_donors]
            donor_lookup = {payload["url"]: payload for payload in donor_payloads}
            donor_rows = {donor["source_page"]: donor for donor in candidate_donors}
            logger.info(
                "    [fallback] target %d/%d | %s | %d candidate donors",
                index,
                len(targets),
                target_page,
                len(candidate_donors),
            )

            try:
                layer1_suggestions = _layer1_generate_internal_links(
                    source_site=source_site,
                    target_site=target_site,
                    target_payload=target_payload,
                    donor_payloads=donor_payloads,
                    scope=scope,
                )
                validation_results = _layer2_validate_internal_links_batch(
                    source_site=source_site,
                    target_site=target_site,
                    target_payload=target_payload,
                    donor_payloads=donor_lookup,
                    candidates=layer1_suggestions,
                    scope=scope,
                )
            except Exception as exc:
                logger.warning(
                    "AI internal linking fallback failed for %s -> %s (%s): %s",
                    source_site,
                    target_site,
                    target_page,
                    str(exc)[:160],
                )
                continue

            if INTERNAL_LINKING_BYPASS_LAYER2:
                saved_from_layer1 = _save_layer1_candidates_direct(
                    source_site=source_site,
                    target_site=target_site,
                    target=target,
                    target_page=target_page,
                    donor_rows=donor_rows,
                    layer1_suggestions=layer1_suggestions,
                    scope=scope,
                    seen=seen,
                    suggestions=suggestions,
                )
                logger.info(
                    "      [fallback] layer2 bypass active: saved %d layer1 suggestions for %s",
                    saved_from_layer1,
                    target_page,
                )
                continue

            layer1_lookup = {
                _normalize_url(candidate.get("source_page", "")): candidate
                for candidate in layer1_suggestions
                if _normalize_url(candidate.get("source_page", ""))
            }

            approved_for_target = 0
            for validation in validation_results:
                source_page = _normalize_url(validation.get("source_page", ""))
                donor_row = donor_rows.get(source_page)
                candidate = layer1_lookup.get(source_page)
                if not donor_row or not candidate:
                    continue

                dedupe_key = (scope, source_site, source_page, target_site, target_page, target["target_page_keyword"])
                if dedupe_key in seen:
                    continue

                final_confidence = _coerce_confidence(validation.get("confidence"))
                if final_confidence is None:
                    final_confidence = _coerce_confidence(candidate.get("confidence"))
                if final_confidence is None or final_confidence < 75:
                    continue
                if not validation.get("approved"):
                    continue

                final_reason = (validation.get("reason") or candidate.get("reason") or "").strip()
                final_anchor = (validation.get("anchor_text") or candidate.get("anchor_text") or target["target_page_keyword"]).strip()
                if not final_anchor or not _is_meaningful_reason(final_reason):
                    continue
                if _violates_hard_internal_link_rules(
                    source_site=source_site,
                    target_site=target_site,
                    scope=scope,
                    source_page=source_page,
                    target_page=target_page,
                ):
                    continue

                seen.add(dedupe_key)
                score = int(
                    (donor_row["source_page_traffic"] / 100)
                    + (100 - target["target_page_position"])
                    + (target["target_page_volume"] / 1000)
                )
                suggestions.append({
                    "website": source_site,
                    "source_website": source_site,
                    "target_website": target_site,
                    "suggestion_scope": scope,
                    "source_page": source_page,
                    "source_page_traffic": donor_row["source_page_traffic"],
                    "target_page": target_page,
                    "target_page_keyword": target["target_page_keyword"],
                    "target_page_position": target["target_page_position"],
                    "target_page_volume": target["target_page_volume"],
                    "suggested_anchor": final_anchor,
                    "existing_link": False,
                    "score": score,
                    "status": "pending",
                    "reason": final_reason,
                    "ai_confidence": final_confidence,
                })
                approved_for_target += 1

            logger.info(
                "      [fallback] layer2 approved %d suggestions for %s",
                approved_for_target,
                target_page,
            )

    suggestions.sort(
        key=lambda row: (
            row["score"],
            row.get("ai_confidence") or 0,
            row["source_page_traffic"],
            row["target_page_volume"],
        ),
        reverse=True,
    )
    return suggestions[:limit]


# ══════════════════════════════════════════════════════════════
#  Cluster-First Internal Linking Engine v2
# ══════════════════════════════════════════════════════════════

CLUSTER_SIMILARITY_THRESHOLD = 6
CLUSTER_MIN_SIZE = 3
CLUSTER_MAX_SIZE = 12


def _compute_page_similarity(page_a: dict, page_b: dict) -> float:
    """Compute weighted similarity score between two enriched pages."""
    score = 0.0
    tokens_a = set(page_a.get("topic_tokens") or set())
    tokens_b = set(page_b.get("topic_tokens") or set())
    if tokens_a and tokens_b:
        union = tokens_a | tokens_b
        intersection = tokens_a & tokens_b
        if union:
            score += (len(intersection) / len(union)) * 5
    section_a = page_a.get("section", "")
    section_b = page_b.get("section", "")
    if section_a and section_b and section_a == section_b:
        score += 4
    keywords_a = {
        (kw.get("keyword") or "").lower().strip()
        for kw in (page_a.get("top_keywords") or [])
        if kw.get("keyword")
    }
    keywords_b = {
        (kw.get("keyword") or "").lower().strip()
        for kw in (page_b.get("top_keywords") or [])
        if kw.get("keyword")
    }
    if keywords_a & keywords_b:
        score += 3
    url_tokens_a = _url_path_tokens(page_a.get("url", ""))
    url_tokens_b = _url_path_tokens(page_b.get("url", ""))
    if url_tokens_a & url_tokens_b:
        score += 2
    return score


def _detect_topic_clusters(enriched_pages: list[dict], site_name: str) -> list[dict]:
    """Group enriched pages into topic clusters using heuristic similarity BEFORE linking."""
    if len(enriched_pages) < CLUSTER_MIN_SIZE:
        logger.info("  cluster_detect[%s]: too few pages (%d), skipping", site_name, len(enriched_pages))
        return []

    pages = list(enriched_pages)
    n = len(pages)
    adjacency: dict[int, set[int]] = defaultdict(set)
    for i in range(n):
        for j in range(i + 1, n):
            sim = _compute_page_similarity(pages[i], pages[j])
            if sim >= CLUSTER_SIMILARITY_THRESHOLD:
                adjacency[i].add(j)
                adjacency[j].add(i)

    visited: set[int] = set()
    raw_components: list[list[int]] = []
    for start in range(n):
        if start in visited:
            continue
        stack = [start]
        component: set[int] = set()
        visited.add(start)
        while stack:
            current = stack.pop()
            component.add(current)
            for neighbor in adjacency.get(current, set()):
                if neighbor not in visited:
                    visited.add(neighbor)
                    stack.append(neighbor)
        if len(component) >= 2:
            raw_components.append(sorted(component))

    valid_clusters: list[list[int]] = []
    small_clusters: list[list[int]] = []
    for component in raw_components:
        if len(component) >= CLUSTER_MIN_SIZE:
            valid_clusters.append(component)
        else:
            small_clusters.append(component)

    for small in small_clusters:
        small_tokens: set[str] = set()
        for idx in small:
            small_tokens |= set(pages[idx].get("topic_tokens") or set())
        best_target_idx = None
        best_overlap = 0
        for ci, cluster in enumerate(valid_clusters):
            cluster_tokens: set[str] = set()
            for idx in cluster:
                cluster_tokens |= set(pages[idx].get("topic_tokens") or set())
            overlap = len(small_tokens & cluster_tokens)
            if overlap > best_overlap:
                best_overlap = overlap
                best_target_idx = ci
        if best_target_idx is not None and best_overlap >= 2:
            valid_clusters[best_target_idx].extend(small)

    result: list[dict] = []
    for ci, component in enumerate(valid_clusters):
        cluster_pages = [pages[idx] for idx in component]
        cluster_pages.sort(key=lambda p: p.get("traffic", 0), reverse=True)
        cluster_pages = cluster_pages[:CLUSTER_MAX_SIZE]
        if len(cluster_pages) < CLUSTER_MIN_SIZE:
            continue
        topic_tokens: set[str] = set()
        for p in cluster_pages:
            topic_tokens |= set(p.get("topic_tokens") or set())
        result.append({
            "cluster_index": ci,
            "pages": cluster_pages,
            "topic_tokens": topic_tokens,
        })

    logger.info(
        "  cluster_detect[%s]: detected %d clusters from %d pages",
        site_name, len(result), n,
    )
    return result


def _ai_layer1_validate_cluster(cluster_pages: list[dict], site_name: str) -> tuple[list[dict], int]:
    """AI Layer 1: Validate cluster semantic consistency and prune outlier pages."""
    if not get_ai_client():
        return cluster_pages, 50

    try:
        result = _openai_json_response(
            system_prompt=(
                "You are an SEO cluster architect. "
                "Given a list of pages, determine if they form a coherent topic cluster. "
                "Remove any outlier pages that clearly do not fit the dominant theme. "
                "Be conservative: only remove pages that are genuinely unrelated. "
                "Return JSON with: "
                "refined_pages (list of URL strings to KEEP), "
                "removed_pages (list of URL strings to REMOVE), "
                "consistency_score (integer 0-100)."
            ),
            payload={
                "pages": [
                    {
                        "url": p.get("url", ""),
                        "title": p.get("title", ""),
                        "primary_keyword": p.get("primary_keyword", ""),
                        "top_keywords": [kw.get("keyword", "") for kw in (p.get("top_keywords") or [])[:3]],
                    }
                    for p in cluster_pages
                ],
            },
        )
    except Exception as exc:
        logger.warning("  cluster_validate[%s]: AI Layer 1 failed: %s", site_name, str(exc)[:120])
        return cluster_pages, 50

    refined_urls = set(result.get("refined_pages") or [])
    consistency = int(result.get("consistency_score") or 50)
    if not refined_urls:
        return cluster_pages, consistency

    refined = [p for p in cluster_pages if p.get("url", "") in refined_urls]
    removed_count = len(cluster_pages) - len(refined)
    if removed_count > 0:
        logger.info(
            "  cluster_validate[%s]: AI removed %d outliers, consistency=%d",
            site_name, removed_count, consistency,
        )
    return refined if len(refined) >= CLUSTER_MIN_SIZE else cluster_pages, consistency


def _assign_pillar_and_supporting(cluster_pages: list[dict]) -> tuple[dict, list[dict]]:
    """Select pillar page by weighted scoring; all others are supporting."""
    if not cluster_pages:
        return {}, []

    max_traffic = max((p.get("traffic") or 0) for p in cluster_pages) or 1
    max_keywords = max(len(p.get("top_keywords") or []) for p in cluster_pages) or 1

    def _pillar_score(page: dict) -> float:
        traffic_score = (page.get("traffic") or 0) / max_traffic
        best_pos = 50
        for kw in (page.get("top_keywords") or []):
            pos = kw.get("position") or 50
            if pos < best_pos:
                best_pos = pos
        position_score = max(0.0, 1.0 - (best_pos / 50.0))
        breadth_score = len(page.get("top_keywords") or []) / max_keywords
        return 0.4 * traffic_score + 0.3 * position_score + 0.3 * breadth_score

    scored = sorted(cluster_pages, key=_pillar_score, reverse=True)
    pillar = scored[0]
    supporting = scored[1:]
    return pillar, supporting


def _ai_layer2_normalize_topic(cluster_pages: list[dict], pillar_url: str, site_name: str) -> dict:
    """AI Layer 2: Generate canonical topic, intent, and cluster reason."""
    if not get_ai_client():
        primary_kw = ""
        for p in cluster_pages:
            if p.get("url") == pillar_url:
                primary_kw = p.get("primary_keyword", "")
                break
        return {
            "cluster_topic": primary_kw or "Content cluster",
            "cluster_intent": "informational",
            "cluster_reason": "",
        }

    try:
        result = _openai_json_response(
            system_prompt=(
                "You are an SEO strategist. Given a topic cluster of pages with a designated pillar page, "
                "determine: (1) a canonical topic name (2-5 words, clean and specific), "
                "(2) the primary user intent (informational, commercial, transactional, or navigational), "
                "(3) a detailed cluster reason (35-70 words prose) explaining how these pages support each other. "
                "Return JSON with: cluster_topic, cluster_intent, cluster_reason."
            ),
            payload={
                "pillar_page": pillar_url,
                "pages": [
                    {
                        "url": p.get("url", ""),
                        "title": p.get("title", ""),
                        "primary_keyword": p.get("primary_keyword", ""),
                        "traffic": p.get("traffic", 0),
                    }
                    for p in cluster_pages
                ],
            },
        )
    except Exception as exc:
        logger.warning("  cluster_normalize[%s]: AI Layer 2 failed: %s", site_name, str(exc)[:120])
        return {"cluster_topic": "Content cluster", "cluster_intent": "informational", "cluster_reason": ""}

    return {
        "cluster_topic": (result.get("cluster_topic") or "Content cluster").strip(),
        "cluster_intent": (result.get("cluster_intent") or "informational").strip(),
        "cluster_reason": (result.get("cluster_reason") or "").strip(),
    }


def _compute_cluster_hash(page_urls: list[str]) -> str:
    """Deterministic hash of sorted page URLs for cluster identity."""
    canonical = "\n".join(sorted(set(url.strip().lower().rstrip("/") for url in page_urls if url.strip())))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _load_existing_clusters(client, domain: str) -> dict[str, dict]:
    """Load all existing clusters for a domain from the memory table."""
    try:
        resp = client.table("internal_linking_clusters").select("*").eq("domain", domain).execute()
        rows = resp.data or []
    except Exception:
        return {}
    return {row["cluster_hash"]: row for row in rows}


def _load_link_history(client, domain: str) -> set[tuple[str, str]]:
    """Load existing link pairs from history to avoid duplicates."""
    try:
        resp = (
            client.table("internal_linking_history")
            .select("source_url,target_url")
            .execute()
        )
        rows = resp.data or []
    except Exception:
        return set()
    return {(_normalize_url(r["source_url"]), _normalize_url(r["target_url"])) for r in rows}


def _process_cluster_lifecycle(
    cluster_pages: list[dict],
    domain: str,
    site_name: str,
    existing_clusters: dict[str, dict],
) -> dict | None:
    """Check cluster memory; return enriched cluster data or None to skip."""
    page_urls = [p.get("url", "") for p in cluster_pages if p.get("url")]
    cluster_hash = _compute_cluster_hash(page_urls)

    if cluster_hash in existing_clusters:
        cached = existing_clusters[cluster_hash]
        logger.info(
            "  cluster_lifecycle[%s]: CACHE HIT hash=%s, cluster_id=%s — skipping AI",
            site_name, cluster_hash[:8], cached.get("cluster_id"),
        )
        return {
            "cluster_id": cached["cluster_id"],
            "cluster_hash": cluster_hash,
            "pillar_page": cached["pillar_page"],
            "supporting_pages": cached.get("supporting_pages") or [],
            "cluster_topic": cached.get("cluster_topic") or "Content cluster",
            "cluster_intent": cached.get("cluster_intent") or "informational",
            "cluster_reason": cached.get("cluster_reason") or "",
            "pages": cluster_pages,
            "is_cached": True,
        }

    # New cluster: assign pillar
    pillar, supporting = _assign_pillar_and_supporting(cluster_pages)
    pillar_url = pillar.get("url", "")
    supporting_urls = [p.get("url", "") for p in supporting]

    # AI Layer 2: normalize topic
    normalized = _ai_layer2_normalize_topic(cluster_pages, pillar_url, site_name)

    cluster_id = f"{domain.lower()}-{cluster_hash[:8]}"
    logger.info(
        "  cluster_lifecycle[%s]: NEW cluster_id=%s, pillar=%s, %d supporting, topic='%s'",
        site_name, cluster_id, pillar_url[:60], len(supporting_urls), normalized["cluster_topic"],
    )

    return {
        "cluster_id": cluster_id,
        "cluster_hash": cluster_hash,
        "pillar_page": pillar_url,
        "supporting_pages": supporting_urls,
        "cluster_topic": normalized["cluster_topic"],
        "cluster_intent": normalized["cluster_intent"],
        "cluster_reason": normalized["cluster_reason"],
        "pages": cluster_pages,
        "is_cached": False,
    }


def _ai_layer3_generate_links_batch(
    links_to_generate: list[dict],
    cluster_topic: str,
    site_name: str,
) -> list[dict]:
    """AI Layer 3: Generate anchor text for a batch of links within one cluster."""
    if not links_to_generate:
        return []
    if not get_ai_client():
        return [
            {**link, "anchor_text": link.get("target_keyword") or cluster_topic, "confidence": 60}
            for link in links_to_generate
        ]

    try:
        result = _openai_json_response(
            system_prompt=(
                f"You are an internal linking specialist for the topic cluster '{cluster_topic}'. "
                "For each link in the batch, suggest a natural, descriptive anchor text that fits "
                "naturally in the source page's content. Also provide a brief reason (1-2 sentences) "
                "and a confidence score (0-100). "
                "Return JSON with: suggestions (array of objects with source_url, target_url, "
                "anchor_text, reason, confidence)."
            ),
            payload={
                "cluster_topic": cluster_topic,
                "links": [
                    {
                        "source_url": link["source_url"],
                        "source_title": link.get("source_title", ""),
                        "target_url": link["target_url"],
                        "target_title": link.get("target_title", ""),
                        "target_keyword": link.get("target_keyword", ""),
                        "link_type": link["link_type"],
                    }
                    for link in links_to_generate
                ],
            },
        )
    except Exception as exc:
        logger.warning("  layer3[%s]: AI batch generation failed: %s", site_name, str(exc)[:120])
        return [
            {**link, "anchor_text": link.get("target_keyword") or cluster_topic, "confidence": None}
            for link in links_to_generate
        ]

    suggestions = result.get("suggestions") if isinstance(result, dict) else result
    if not isinstance(suggestions, list):
        suggestions = []
    ai_lookup = {}
    for s in suggestions:
        key = (_normalize_url(s.get("source_url", "")), _normalize_url(s.get("target_url", "")))
        ai_lookup[key] = s

    enriched = []
    for link in links_to_generate:
        key = (_normalize_url(link["source_url"]), _normalize_url(link["target_url"]))
        ai_data = ai_lookup.get(key, {})
        enriched.append({
            **link,
            "anchor_text": (ai_data.get("anchor_text") or link.get("target_keyword") or cluster_topic).strip(),
            "reason": (ai_data.get("reason") or "").strip(),
            "confidence": _coerce_confidence(ai_data.get("confidence")),
        })
    return enriched


def _ai_layer4_validate_links(
    links: list[dict],
    cluster_topic: str,
    site_name: str,
) -> list[dict]:
    """AI Layer 4: Skeptical editor validates link quality and rejects weak ones."""
    if not links:
        return []
    if not get_ai_client():
        return links

    try:
        result = _openai_json_response(
            system_prompt=(
                "You are a skeptical SEO editor reviewing internal link suggestions. "
                "For each link, score relevance (0-100), contextual fit (0-100), and anchor quality (0-100). "
                "Reject if: overall confidence < 75, anchor is too generic, or the anchor duplicates another in the batch. "
                "Approve only links with clear editorial value. "
                "Return JSON with: results (array of objects with source_url, target_url, approved (boolean), "
                "overall_confidence (0-100), reason (1 sentence))."
            ),
            payload={
                "cluster_topic": cluster_topic,
                "links_to_review": [
                    {
                        "source_url": link["source_url"],
                        "target_url": link["target_url"],
                        "link_type": link["link_type"],
                        "anchor_text": link.get("anchor_text", ""),
                        "reason": link.get("reason", ""),
                    }
                    for link in links
                ],
            },
        )
    except Exception as exc:
        logger.warning("  layer4[%s]: AI validation failed: %s", site_name, str(exc)[:120])
        return links

    validation_results = result.get("results") if isinstance(result, dict) else result
    if not isinstance(validation_results, list):
        validation_results = []
    val_lookup = {}
    for v in validation_results:
        key = (_normalize_url(v.get("source_url", "")), _normalize_url(v.get("target_url", "")))
        val_lookup[key] = v

    validated = []
    for link in links:
        key = (_normalize_url(link["source_url"]), _normalize_url(link["target_url"]))
        val = val_lookup.get(key, {})
        confidence = _coerce_confidence(val.get("overall_confidence")) or link.get("confidence")
        approved = val.get("approved", True)
        is_mandatory = link["link_type"] in ("support_to_pillar", "pillar_to_support")

        if is_mandatory:
            link["confidence"] = confidence
            if val.get("reason"):
                link["reason"] = val["reason"]
            validated.append(link)
        elif approved and confidence is not None and confidence >= 75:
            link["confidence"] = confidence
            if val.get("reason"):
                link["reason"] = val["reason"]
            validated.append(link)

    return validated


def _generate_structured_links(
    cluster_data: dict,
    page_map: dict[str, dict],
    existing_pairs: set[tuple[str, str]],
    link_history_pairs: set[tuple[str, str]],
    site_name: str,
) -> list[dict]:
    """Generate structured links within a single cluster following pillar/spoke rules."""
    cluster_id = cluster_data["cluster_id"]
    cluster_topic = cluster_data["cluster_topic"]
    pillar_url = cluster_data["pillar_page"]
    supporting_urls = cluster_data["supporting_pages"]
    pillar_profile = page_map.get(pillar_url, {})
    all_links: list[dict] = []

    def _should_skip(source: str, target: str) -> bool:
        s, t = _normalize_url(source), _normalize_url(target)
        if not s or not t or s == t:
            return True
        if (s, t) in existing_pairs or (s, t) in link_history_pairs:
            return True
        return False

    def _make_link_request(source_url: str, target_url: str, link_type: str) -> dict:
        source_profile = page_map.get(source_url, {})
        target_profile = page_map.get(target_url, {})
        return {
            "source_url": source_url,
            "target_url": target_url,
            "link_type": link_type,
            "source_title": source_profile.get("title") or _infer_page_title(source_url),
            "target_title": target_profile.get("title") or _infer_page_title(target_url),
            "target_keyword": target_profile.get("primary_keyword") or "",
        }

    # 1. MANDATORY: Supporting -> Pillar
    s2p_requests = []
    for sup_url in supporting_urls:
        if not _should_skip(sup_url, pillar_url):
            s2p_requests.append(_make_link_request(sup_url, pillar_url, "support_to_pillar"))
    if s2p_requests:
        s2p_links = _ai_layer3_generate_links_batch(s2p_requests, cluster_topic, site_name)
        all_links.extend(s2p_links)

    # 2. MANDATORY: Pillar -> Supporting
    p2s_requests = []
    for sup_url in supporting_urls:
        if not _should_skip(pillar_url, sup_url):
            p2s_requests.append(_make_link_request(pillar_url, sup_url, "pillar_to_support"))
    if p2s_requests:
        p2s_links = _ai_layer3_generate_links_batch(p2s_requests, cluster_topic, site_name)
        all_links.extend(p2s_links)

    # 3. OPTIONAL: Supporting <-> Supporting (only if topic token overlap >= 3)
    s2s_requests = []
    for i, url_a in enumerate(supporting_urls):
        profile_a = page_map.get(url_a, {})
        tokens_a = set(profile_a.get("topic_tokens") or set())
        for url_b in supporting_urls[i + 1:]:
            profile_b = page_map.get(url_b, {})
            tokens_b = set(profile_b.get("topic_tokens") or set())
            if len(tokens_a & tokens_b) >= 3:
                if not _should_skip(url_a, url_b):
                    s2s_requests.append(_make_link_request(url_a, url_b, "support_to_support"))
                if not _should_skip(url_b, url_a):
                    s2s_requests.append(_make_link_request(url_b, url_a, "support_to_support"))
    if s2s_requests:
        s2s_links = _ai_layer3_generate_links_batch(s2s_requests, cluster_topic, site_name)
        all_links.extend(s2s_links)

    # Layer 4: Validate all links
    validated = _ai_layer4_validate_links(all_links, cluster_topic, site_name)

    logger.info(
        "  structured_links[%s][%s]: %d generated, %d validated (s2p=%d, p2s=%d, s2s=%d)",
        site_name, cluster_id,
        len(all_links), len(validated),
        len([l for l in validated if l["link_type"] == "support_to_pillar"]),
        len([l for l in validated if l["link_type"] == "pillar_to_support"]),
        len([l for l in validated if l["link_type"] == "support_to_support"]),
    )
    return validated


def generate_internal_link_suggestions(site_name: str, site_data: dict, limit: int = 30) -> list[dict]:
    """Build cluster-first internal-link suggestions for one website (v2 engine)."""
    organic_keywords = site_data.get("organic_keywords")
    top_pages = site_data.get("top_pages")
    internal_links = site_data.get("internal_links")
    if not organic_keywords or not top_pages or not internal_links:
        logger.info(
            "  internal_linking[%s]: skipped (organic_keywords=%s, top_pages=%s, internal_links=%s)",
            site_name,
            bool(organic_keywords),
            bool(top_pages),
            bool(internal_links),
        )
        return []

    targets = _select_target_candidates(organic_keywords, site_name=site_name)
    donors = _select_source_candidates(top_pages, min_traffic=500)
    if site_name == "BusinessABC":
        targets = [t for t in targets if "/wiki/" not in (t.get("target_page") or "").lower()]
        donors = [d for d in donors if "/wiki/" not in (d.get("source_page") or "").lower()]
    if not targets or not donors:
        logger.info("  internal_linking[%s]: no eligible pages (targets=%d, donors=%d)", site_name, len(targets), len(donors))
        return []

    existing_pairs = _build_existing_link_pairs(internal_links)
    page_map = _build_page_enrichment(site_name, site_data)

    # Build unified enriched page list from page_map
    enriched_pages = list(page_map.values())
    logger.info("  internal_linking[%s]: %d enriched pages available for clustering", site_name, len(enriched_pages))

    # Step 1: Detect clusters (heuristic)
    cluster_candidates = _detect_topic_clusters(enriched_pages, site_name)
    if not cluster_candidates:
        logger.info("  internal_linking[%s]: no clusters detected, skipping", site_name)
        return []

    # Load memory tables (graceful if tables don't exist yet)
    try:
        from supabase import create_client
        client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        existing_clusters = _load_existing_clusters(client, site_name)
        link_history_pairs = _load_link_history(client, site_name)
    except Exception:
        existing_clusters = {}
        link_history_pairs = set()
        client = None

    all_suggestions: list[dict] = []
    new_clusters_to_save: list[dict] = []
    new_history_links: list[dict] = []

    for candidate in cluster_candidates:
        cluster_pages = candidate["pages"]

        # Step 2: AI Layer 1 — Semantic Validation
        refined_pages, consistency_score = _ai_layer1_validate_cluster(cluster_pages, site_name)
        if len(refined_pages) < CLUSTER_MIN_SIZE:
            continue

        # Step 3: Lifecycle — check memory or create new cluster
        cluster_data = _process_cluster_lifecycle(refined_pages, site_name, site_name, existing_clusters)
        if cluster_data is None:
            continue

        # Step 4: Generate structured links within cluster
        validated_links = _generate_structured_links(
            cluster_data=cluster_data,
            page_map=page_map,
            existing_pairs=existing_pairs,
            link_history_pairs=link_history_pairs,
            site_name=site_name,
        )

        # Convert validated links to suggestion row format
        for link in validated_links:
            source_url = _normalize_url(link["source_url"])
            target_url = _normalize_url(link["target_url"])
            source_profile = page_map.get(source_url, {})
            target_profile = page_map.get(target_url, {})
            score = int(
                ((source_profile.get("traffic") or 0) / 100)
                + (100 - (target_profile.get("top_keywords", [{}])[0].get("position", 50) if target_profile.get("top_keywords") else 50))
                + ((target_profile.get("top_keywords", [{}])[0].get("volume", 0) if target_profile.get("top_keywords") else 0) / 1000)
            )
            all_suggestions.append({
                "website": site_name,
                "source_website": site_name,
                "target_website": site_name,
                "suggestion_scope": "within_site",
                "source_page": source_url,
                "source_page_traffic": source_profile.get("traffic") or 0,
                "target_page": target_url,
                "target_page_keyword": target_profile.get("primary_keyword") or link.get("target_keyword", ""),
                "target_page_position": (target_profile.get("top_keywords", [{}])[0].get("position") if target_profile.get("top_keywords") else None),
                "target_page_volume": (target_profile.get("top_keywords", [{}])[0].get("volume") if target_profile.get("top_keywords") else None),
                "suggested_anchor": link.get("anchor_text", ""),
                "existing_link": False,
                "score": score,
                "status": "pending",
                "reason": link.get("reason", ""),
                "ai_confidence": link.get("confidence"),
                "link_type": link["link_type"],
                "cluster_id": cluster_data["cluster_id"],
                "pillar_page": cluster_data["pillar_page"],
                "cluster_topic": cluster_data["cluster_topic"],
                "cluster_reason": cluster_data.get("cluster_reason", ""),
            })
            # Track for history
            new_history_links.append({
                "source_url": source_url,
                "target_url": target_url,
                "link_type": link["link_type"],
                "cluster_id": cluster_data["cluster_id"],
                "anchor_text": link.get("anchor_text", ""),
                "ai_confidence": link.get("confidence"),
            })
            # Add to existing_pairs to prevent duplicates within same run
            existing_pairs.add((source_url, target_url))

        # Track cluster for memory save
        if not cluster_data.get("is_cached"):
            avg_traffic = sum(p.get("traffic", 0) for p in refined_pages) / max(len(refined_pages), 1)
            new_clusters_to_save.append({
                "cluster_id": cluster_data["cluster_id"],
                "domain": site_name,
                "cluster_topic": cluster_data["cluster_topic"],
                "cluster_intent": cluster_data.get("cluster_intent", "informational"),
                "pillar_page": cluster_data["pillar_page"],
                "supporting_pages": cluster_data["supporting_pages"],
                "cluster_hash": cluster_data["cluster_hash"],
                "cluster_status": "new",
                "cluster_reason": cluster_data.get("cluster_reason", ""),
                "baseline_metrics": json.dumps({"avg_traffic": round(avg_traffic, 1), "page_count": len(refined_pages)}),
            })

    # Persist memory tables
    if client:
        try:
            if new_clusters_to_save:
                for cluster_row in new_clusters_to_save:
                    cluster_row["supporting_pages"] = json.dumps(cluster_row["supporting_pages"])
                batch_upsert(client, "internal_linking_clusters", new_clusters_to_save, "cluster_id")
                logger.info("  memory[%s]: saved %d new clusters", site_name, len(new_clusters_to_save))
            if new_history_links:
                for hl in new_history_links:
                    hl.setdefault("link_applied", False)
                batch_upsert(client, "internal_linking_history", new_history_links, "source_url,target_url")
                logger.info("  memory[%s]: saved %d link history entries", site_name, len(new_history_links))
        except Exception as exc:
            logger.warning("  memory[%s]: failed to persist memory tables: %s", site_name, str(exc)[:120])

    all_suggestions.sort(
        key=lambda row: (row["score"], row.get("ai_confidence") or 0, row["source_page_traffic"]),
        reverse=True,
    )
    logger.info("  internal_linking[%s]: v2 engine produced %d suggestions (cap %d)", site_name, len(all_suggestions), limit)
    return all_suggestions[:limit]



def generate_cross_platform_link_suggestions(parsed_data: dict, limit_per_source: int = 30) -> list[dict]:
    """Build AI-validated cross-platform link suggestions across the Ztudium ecosystem."""
    site_inputs = {}
    for site_name, site_data in parsed_data.items():
        organic_keywords = site_data.get("organic_keywords")
        top_pages = site_data.get("top_pages")
        if not organic_keywords or not top_pages:
            continue
        targets = _select_target_candidates(organic_keywords, site_name=site_name)
        donors = _select_source_candidates(top_pages, min_traffic=500)
        if not targets or not donors:
            logger.info(
                "  internal_linking[cross_platform][%s]: excluded from source/target pool (targets=%d, donors=%d, organic_keywords=%d, top_pages=%d)",
                site_name,
                len(targets),
                len(donors),
                len((organic_keywords or {}).get("keywords", [])),
                len((top_pages or {}).get("pages", [])),
            )
            continue
        site_inputs[site_name] = {
            "targets": targets,
            "donors": donors,
            "page_map": _build_page_enrichment(site_name, site_data),
        }

    suggestions = []
    for source_site, source_data in site_inputs.items():
        source_suggestions = []
        for target_site, target_data in site_inputs.items():
            if source_site == target_site:
                continue
            source_suggestions.extend(
                _generate_ai_suggestions_for_scope(
                    source_site=source_site,
                    target_site=target_site,
                    targets=target_data["targets"],
                    donors=source_data["donors"],
                    source_page_map=source_data["page_map"],
                    target_page_map=target_data["page_map"],
                    scope="cross_platform",
                    limit=limit_per_source,
                    existing_pairs=None,
                )
            )

        source_suggestions.sort(
            key=lambda row: (
                row["score"],
                row.get("ai_confidence") or 0,
                row["source_page_traffic"],
                row["target_page_volume"],
            ),
            reverse=True,
        )
        suggestions.extend(source_suggestions[:limit_per_source])

    return suggestions


# ══════════════════════════════════════════════════════════════
#  Supabase DB upload
# ══════════════════════════════════════════════════════════════

def _internal_link_cluster_group_key(row: dict) -> tuple[str, str]:
    scope = row.get("suggestion_scope") or "within_site"
    if scope == "cross_platform":
        site = row.get("source_website") or row.get("website") or "Unknown"
    else:
        site = row.get("website") or row.get("source_website") or "Unknown"
    return scope, site


def _build_internal_link_cluster_components(rows: list[dict]) -> list[list[int]]:
    grouped: dict[tuple[str, str], list[int]] = {}
    for index, row in enumerate(rows):
        source = _normalize_url(row.get("source_page", ""))
        target = _normalize_url(row.get("target_page", ""))
        if not source or not target or source == target:
            continue
        grouped.setdefault(_internal_link_cluster_group_key(row), []).append(index)

    components: list[list[int]] = []
    for indices in grouped.values():
        adjacency: dict[str, set[str]] = {}
        node_edges: dict[str, list[int]] = {}
        for index in indices:
            row = rows[index]
            source = _normalize_url(row.get("source_page", ""))
            target = _normalize_url(row.get("target_page", ""))
            adjacency.setdefault(source, set()).add(target)
            adjacency.setdefault(target, set()).add(source)
            node_edges.setdefault(source, []).append(index)
            node_edges.setdefault(target, []).append(index)

        visited: set[str] = set()
        for start in adjacency.keys():
            if start in visited:
                continue
            stack = [start]
            component_nodes: set[str] = set()
            visited.add(start)
            while stack:
                current = stack.pop()
                component_nodes.add(current)
                for next_node in adjacency.get(current, set()):
                    if next_node in visited:
                        continue
                    visited.add(next_node)
                    stack.append(next_node)

            component_indices = sorted({
                edge_index
                for node in component_nodes
                for edge_index in node_edges.get(node, [])
            })
            if component_indices:
                components.append(component_indices)
    return components


def _generate_internal_link_cluster_reason(component_rows: list[dict]) -> str | None:
    if not get_ai_client():
        return None

    page_stats: dict[str, dict] = {}
    for row in component_rows:
        source = _normalize_url(row.get("source_page", ""))
        target = _normalize_url(row.get("target_page", ""))
        if not source or not target:
            continue
        source_stats = page_stats.setdefault(source, {
            "url": source,
            "title": _infer_page_title(source),
            "in_degree": 0,
            "out_degree": 0,
            "keywords": set(),
        })
        target_stats = page_stats.setdefault(target, {
            "url": target,
            "title": _infer_page_title(target, row.get("target_page_keyword", "")),
            "in_degree": 0,
            "out_degree": 0,
            "keywords": set(),
        })
        source_stats["out_degree"] += 1
        target_stats["in_degree"] += 1
        keyword = (row.get("target_page_keyword") or "").strip()
        if keyword:
            target_stats["keywords"].add(keyword)

    pages = sorted(
        page_stats.values(),
        key=lambda item: (
            item["in_degree"] + item["out_degree"],
            item["out_degree"],
            item["title"],
        ),
        reverse=True,
    )
    links = sorted(
        component_rows,
        key=lambda row: (row.get("score") or 0, row.get("ai_confidence") or 0),
        reverse=True,
    )

    result = _openai_json_response(
        system_prompt=(
            "You are an SEO editor explaining an internal-linking topic cluster. "
            "Write one concise, factual prose explanation of why these pages belong in the same cluster. "
            "Base the explanation only on the page titles, keywords, and suggested links provided. "
            "Do not list links, do not invent facts, and do not say the pages are related only because they are in the same broad category. "
            "Return JSON with a `cluster_reason` string only."
        ),
        payload={
            "scope": component_rows[0].get("suggestion_scope") or "within_site",
            "source_site": component_rows[0].get("source_website") or component_rows[0].get("website"),
            "target_sites": sorted({
                row.get("target_website") or row.get("website") or ""
                for row in component_rows
                if row.get("target_website") or row.get("website")
            }),
            "pages": [
                {
                    "url": page["url"],
                    "title": page["title"],
                    "in_degree": page["in_degree"],
                    "out_degree": page["out_degree"],
                    "keywords": sorted(page["keywords"])[:5],
                }
                for page in pages[:12]
            ],
            "suggested_links": [
                {
                    "source_page": row.get("source_page"),
                    "target_page": row.get("target_page"),
                    "target_keyword": row.get("target_page_keyword"),
                    "anchor": row.get("suggested_anchor"),
                    "reason": row.get("reason"),
                    "score": row.get("score"),
                }
                for row in links[:18]
            ],
            "instructions": {
                "length": "35 to 70 words",
                "style": "plain prose for a client-facing SEO dashboard",
                "must_explain": "the shared topic or reader journey connecting the pillar and supporting pages",
            },
        },
    )
    reason = (result.get("cluster_reason") or "").strip()
    if len(reason) < 40:
        return None
    return reason[:700]


def populate_internal_link_cluster_reasons(rows: list[dict]) -> None:
    if not rows:
        return
    if not get_ai_client():
        logger.info("  internal_linking: skipped cluster_reason generation (no AI provider configured)")
        return

    components = _build_internal_link_cluster_components(rows)
    generated = 0
    for component_indices in components:
        component_rows = [rows[index] for index in component_indices]
        if len(component_rows) == 1:
            continue
        try:
            reason = _generate_internal_link_cluster_reason(component_rows)
        except Exception as exc:
            logger.warning("  internal_linking: cluster_reason generation failed: %s", str(exc)[:160])
            continue
        if not reason:
            continue
        for index in component_indices:
            rows[index]["cluster_reason"] = reason
        generated += 1

    logger.info("  internal_linking: generated cluster_reason for %d clusters", generated)


def classify_authority_problem(row: dict) -> dict:
    """Classify the SEO authority issue solved by an internal-link suggestion."""
    scope = row.get("suggestion_scope") or "within_site"
    link_type = row.get("link_type") or ""
    score = int(row.get("score") or 0)
    source_traffic = int(row.get("source_page_traffic") or 0)
    position = float(row.get("target_page_position") or 999)
    keyword = row.get("target_page_keyword") or "the target keyword"
    target_page = row.get("target_page") or "the target page"
    source_page = row.get("source_page") or "the source page"
    cluster_topic = row.get("cluster_topic") or keyword

    if scope == "cross_platform":
        problem_type = "Cross-Platform Opportunity"
        reason = (
            f"{source_page} and {target_page} cover overlapping Ztudium ecosystem topics. "
            f"A contextual link would transfer topical relevance between properties for '{keyword}'."
        )
        priority = "high" if score >= 100 else "medium" if score >= 80 else "low"
        action = f"Add a cross-platform contextual link using '{keyword}' or a close variant as anchor text."
    elif link_type == "support_to_pillar":
        problem_type = "Inverted Cluster"
        reason = (
            f"The supporting page should reinforce the pillar for the '{cluster_topic}' cluster. "
            "Without support-to-pillar links, Google may not identify the main authority hub."
        )
        priority = "high" if score >= 90 else "medium"
        action = f"Link this supporting page back to the pillar page using '{keyword}' as the anchor."
    elif link_type == "pillar_to_support" and source_traffic >= 500:
        problem_type = "Authority Hoarding"
        reason = (
            f"The source page has meaningful traffic ({source_traffic:,}) but is not distributing authority "
            f"to {target_page}, which targets '{keyword}'."
        )
        priority = "high" if source_traffic >= 2000 or score >= 100 else "medium"
        action = "Add 3-5 contextual links from this high-authority page to relevant supporting pages."
    elif 8 <= position <= 15:
        problem_type = "Link-Starved Page"
        reason = (
            f"The target page already ranks on page 1/2 at position {position:g} for '{keyword}', "
            "so internal authority is likely the fastest lever to push it into the top results."
        )
        priority = "high" if score >= 90 else "medium"
        action = f"Add this link and 2-4 related internal links to push '{keyword}' toward the top 5."
    elif row.get("cluster_id"):
        problem_type = "Orphaned Cluster Page"
        reason = (
            f"The page belongs to the '{cluster_topic}' cluster but is under-connected to related pages. "
            "A cluster link helps consolidate topical authority."
        )
        priority = "medium" if score >= 70 else "low"
        action = f"Connect this page inside the '{cluster_topic}' cluster with descriptive anchor text."
    else:
        problem_type = "Link-Starved Page"
        reason = (
            f"The target page needs more internal authority for '{keyword}'. "
            "This suggestion connects it from a relevant source page."
        )
        priority = "medium" if score >= 70 else "low"
        action = f"Add a contextual link from the source page to the target page using '{keyword}'."

    return {
        "authority_problem_type": problem_type,
        "authority_problem_reason": reason,
        "authority_priority": priority,
        "authority_action": action,
    }


def populate_authority_problem_fields(rows: list[dict]) -> None:
    for row in rows:
        row.update(classify_authority_problem(row))


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


def _dedupe_rows_by_keys(rows: list[dict], keys: list[str]) -> list[dict]:
    """Keep only the last row for each unique key combination within a batch."""
    deduped: dict[tuple, dict] = {}
    for row in rows:
        key = tuple(row.get(k) for k in keys)
        if any(value is None for value in key):
            continue
        deduped[key] = row
    return list(deduped.values())


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


def delete_where(client, table: str, filters: dict, chunk_size: int = 1000) -> int:
    """Delete rows matching filters. Returns deleted row count on best effort."""
    if not filters:
        return 0
    deleted = 0
    while True:
        query = client.table(table).select("id")
        for column, value in filters.items():
            query = query.eq(column, value)
        response = query.limit(chunk_size).execute()
        rows = response.data or []
        if not rows:
            break
        ids = [row["id"] for row in rows if row.get("id")]
        if not ids:
            break
        client.table(table).delete().in_("id", ids).execute()
        deleted += len(ids)
        if len(ids) < chunk_size:
            break
    return deleted


def replace_snapshot_rows(client, table: str, scopes: list[dict], label: str) -> int:
    """Delete existing rows for the current snapshot scopes so latest exports fully replace them."""
    deleted = 0
    for scope in scopes:
        if not scope:
            continue
        deleted += delete_where(client, table, scope)
    if deleted:
        logger.info("  %s: removed %d stale rows before insert", label, deleted)
    return deleted


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


def upload_parsed_data(parsed_data, run_id: str | None = None, internal_linking_only: bool = False):
    """Upload all parsed Ahrefs data to Supabase."""
    from supabase import create_client

    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.error("Supabase credentials not set")
        return
    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    overview_has_source_file = _supports_column(client, "ahrefs_overview", "source_file")
    ref_domains_has_source_file = _supports_column(client, "ahrefs_referring_domains", "source_file")
    broken_backlinks_has_source_file = _supports_column(client, "ahrefs_broken_backlinks", "source_file")
    broken_backlinks_has_target_http_code = _supports_column(client, "ahrefs_broken_backlinks", "target_http_code")
    broken_backlinks_has_referring_page_http_code = _supports_column(client, "ahrefs_broken_backlinks", "referring_page_http_code")
    overview_has_ingestion_run_id = _supports_column(client, "ahrefs_overview", "ingestion_run_id")
    keyword_has_ingestion_run_id = _supports_column(client, "website_keywords", "ingestion_run_id")
    pages_has_ingestion_run_id = _supports_column(client, "website_pages", "ingestion_run_id")
    ref_domains_has_ingestion_run_id = _supports_column(client, "ahrefs_referring_domains", "ingestion_run_id")
    broken_backlinks_has_ingestion_run_id = _supports_column(client, "ahrefs_broken_backlinks", "ingestion_run_id")
    competitors_has_ingestion_run_id = _supports_column(client, "ahrefs_competitors", "ingestion_run_id")
    internal_links_has_reason = _supports_column(client, "internal_linking_suggestions", "reason")
    internal_links_has_ai_confidence = _supports_column(client, "internal_linking_suggestions", "ai_confidence")
    internal_links_has_cluster_reason = _supports_column(client, "internal_linking_suggestions", "cluster_reason")
    internal_links_has_link_type = _supports_column(client, "internal_linking_suggestions", "link_type")
    internal_links_has_cluster_id = _supports_column(client, "internal_linking_suggestions", "cluster_id")
    internal_links_has_pillar_page = _supports_column(client, "internal_linking_suggestions", "pillar_page")
    internal_links_has_cluster_topic = _supports_column(client, "internal_linking_suggestions", "cluster_topic")
    internal_links_has_authority_problem_type = _supports_column(client, "internal_linking_suggestions", "authority_problem_type")
    internal_links_has_authority_problem_reason = _supports_column(client, "internal_linking_suggestions", "authority_problem_reason")
    internal_links_has_authority_priority = _supports_column(client, "internal_linking_suggestions", "authority_priority")
    internal_links_has_authority_action = _supports_column(client, "internal_linking_suggestions", "authority_action")
    websites_in_run = sorted(parsed_data.keys())

    def _strip_unsupported_columns(rows: list[dict]) -> None:
        """Remove columns that the DB doesn't support yet."""
        for row in rows:
            if not internal_links_has_reason:
                row.pop("reason", None)
            if not internal_links_has_ai_confidence:
                row.pop("ai_confidence", None)
            if not internal_links_has_cluster_reason:
                row.pop("cluster_reason", None)
            if not internal_links_has_link_type:
                row.pop("link_type", None)
            if not internal_links_has_cluster_id:
                row.pop("cluster_id", None)
            if not internal_links_has_pillar_page:
                row.pop("pillar_page", None)
            if not internal_links_has_cluster_topic:
                row.pop("cluster_topic", None)
            if not internal_links_has_authority_problem_type:
                row.pop("authority_problem_type", None)
            if not internal_links_has_authority_problem_reason:
                row.pop("authority_problem_reason", None)
            if not internal_links_has_authority_priority:
                row.pop("authority_priority", None)
            if not internal_links_has_authority_action:
                row.pop("authority_action", None)

    if internal_linking_only:
        logger.info("  internal_linking_only mode: rebuilding internal_linking_suggestions only")
        for ws in websites_in_run:
            deleted = delete_where(client, "internal_linking_suggestions", {"source_website": ws})
            if deleted:
                logger.info("  internal_linking_suggestions: removed %d stale rows for %s", deleted, ws)
        suggestion_rows = []
        for ws, data in parsed_data.items():
            if not data.get("organic_keywords") or not data.get("top_pages") or not data.get("internal_links"):
                logger.info("  internal_linking[%s]: skipped (missing one of organic keywords, top pages, or internal links)", ws)
                continue
            suggestion_rows.extend(generate_internal_link_suggestions(ws, data, limit=30))
        cross_platform_rows = generate_cross_platform_link_suggestions(parsed_data, limit_per_source=30)
        if internal_links_has_cluster_reason:
            populate_internal_link_cluster_reasons(cross_platform_rows)
        suggestion_rows.extend(cross_platform_rows)
        populate_authority_problem_fields(suggestion_rows)
        _strip_unsupported_columns(suggestion_rows)
        c = batch_upsert(
            client,
            "internal_linking_suggestions",
            suggestion_rows,
            "suggestion_scope,source_website,source_page,target_website,target_page,target_page_keyword",
        )
        logger.info("  internal_linking_suggestions: %d upserted", c)
        return

    # Overviews
    ov_rows = []
    ov_scopes = []
    for ws, data in parsed_data.items():
        ov = data.get("overview")
        if ov:
            snapshot_date = ov.get("date")
            if not snapshot_date:
                logger.warning("  ahrefs_overview skipped for %s (missing snapshot date)", ws)
                continue
            ov_scopes.append({"website": ws, "date": snapshot_date})
            row = {k: v for k, v in ov.items() if v is not None}
            if run_id and overview_has_ingestion_run_id:
                row["ingestion_run_id"] = run_id
            if not overview_has_source_file:
                row.pop("source_file", None)
            ov_rows.append(row)
    replace_snapshot_rows(client, "ahrefs_overview", ov_scopes, "ahrefs_overview")
    c = batch_upsert(client, "ahrefs_overview", ov_rows, "date,website")
    logger.info("  ahrefs_overview: %d upserted", c)

    # Keywords
    kw_rows = []
    kw_scopes = []
    for ws, data in parsed_data.items():
        ok = data.get("organic_keywords")
        if ok:
            snapshot_date = ok.get("date")
            if not snapshot_date:
                logger.warning("  ahrefs_keywords skipped for %s (missing snapshot date)", ws)
                continue
            kw_scopes.append({"website": ws, "date": snapshot_date, "source": "ahrefs"})
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
    replace_snapshot_rows(client, "website_keywords", kw_scopes, "ahrefs_keywords")
    c = batch_upsert(client, "website_keywords", kw_rows, "date,website,keyword,source")
    logger.info("  ahrefs_keywords: %d upserted", c)

    # Top pages
    pg_rows = []
    pg_scopes = []
    for ws, data in parsed_data.items():
        tp = data.get("top_pages")
        if tp:
            snapshot_date = tp.get("date")
            if not snapshot_date:
                logger.warning("  ahrefs_pages skipped for %s (missing snapshot date)", ws)
                continue
            pg_scopes.append({"website": ws, "date": snapshot_date, "source": "ahrefs"})
            for pg in tp.get("pages", [])[:100]:
                pg_rows.append({k: v for k, v in {
                    "date": snapshot_date, "website": ws, "url": pg.get("url"),
                    "clicks": pg.get("traffic") or 0, "traffic_ahrefs": pg.get("traffic"),
                    "keywords_count": pg.get("keywords_count"), "top_keyword": pg.get("top_keyword"),
                    "source": "ahrefs",
                }.items() if v is not None})
                if run_id and pages_has_ingestion_run_id:
                    pg_rows[-1]["ingestion_run_id"] = run_id
    replace_snapshot_rows(client, "website_pages", pg_scopes, "ahrefs_pages")
    c = batch_upsert(client, "website_pages", pg_rows, "date,website,url,source")
    logger.info("  ahrefs_pages: %d upserted", c)

    # Referring domains
    rd_rows = []
    rd_scopes = []
    for ws, data in parsed_data.items():
        rd = data.get("referring_domains")
        if rd:
            snapshot_date = rd.get("date")
            if not snapshot_date:
                logger.warning("  ahrefs_referring_domains skipped for %s (missing snapshot date)", ws)
                continue
            rd_scopes.append({"website": ws, "date": snapshot_date})
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
    replace_snapshot_rows(client, "ahrefs_referring_domains", rd_scopes, "ahrefs_referring_domains")
    c = batch_upsert(client, "ahrefs_referring_domains", rd_rows, "date,website,domain")
    logger.info("  ahrefs_referring_domains: %d upserted", c)

    # Broken backlinks
    bb_rows = []
    bb_scopes = []
    for ws, data in parsed_data.items():
        bb = data.get("broken_backlinks")
        if bb:
            snapshot_date = bb.get("date")
            if not snapshot_date:
                logger.warning("  ahrefs_broken_backlinks skipped for %s (missing snapshot date)", ws)
                continue
            bb_scopes.append({"website": ws, "date": snapshot_date})
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
                    "target_http_code": link.get("target_http_code"),
                    "referring_page_http_code": link.get("referring_page_http_code"),
                    "ref_domain_dr": link.get("dr"),
                    "source_file": bb.get("source_file"),
                }.items() if v is not None}
                if not broken_backlinks_has_target_http_code:
                    bb_row.pop("target_http_code", None)
                if not broken_backlinks_has_referring_page_http_code:
                    bb_row.pop("referring_page_http_code", None)
                if not broken_backlinks_has_source_file:
                    bb_row.pop("source_file", None)
                if run_id and broken_backlinks_has_ingestion_run_id:
                    bb_row["ingestion_run_id"] = run_id
                bb_rows.append(bb_row)
    pre_dedupe_count = len(bb_rows)
    bb_rows = _dedupe_rows_by_keys(bb_rows, ["date", "website", "referring_page"])
    duplicate_count = pre_dedupe_count - len(bb_rows)
    if duplicate_count > 0:
        logger.info("  ahrefs_broken_backlinks: removed %d duplicate rows before upsert", duplicate_count)
    replace_snapshot_rows(client, "ahrefs_broken_backlinks", bb_scopes, "ahrefs_broken_backlinks")
    c = batch_upsert(client, "ahrefs_broken_backlinks", bb_rows, "date,website,referring_page")
    logger.info("  ahrefs_broken_backlinks: %d upserted", c)

    # Competitors
    comp_rows = []
    comp_scopes = []
    for ws, data in parsed_data.items():
        comp = data.get("organic_competitors")
        if comp:
            snapshot_date = comp.get("date")
            if not snapshot_date:
                logger.warning("  ahrefs_competitors skipped for %s (missing snapshot date)", ws)
                continue
            comp_scopes.append({"website": ws, "date": snapshot_date})
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
    replace_snapshot_rows(client, "ahrefs_competitors", comp_scopes, "ahrefs_competitors")
    c = batch_upsert(client, "ahrefs_competitors", comp_rows, "date,website,competitor_domain")
    logger.info("  ahrefs_competitors: %d upserted", c)

    # Lost backlinks
    lost_rows = []
    for ws, data in parsed_data.items():
        backlinks = data.get("backlinks")
        if not backlinks:
            continue
        for link in backlinks.get("lost_links", []):
            lost_row = {
                "website": ws,
                "referring_page_url": link.get("referring_page_url"),
                "domain_rating": link.get("domain_rating"),
                "target_url": link.get("target_url"),
                "anchor": link.get("anchor"),
                "first_seen": link.get("first_seen"),
                "last_seen": link.get("last_seen"),
                "lost_date": link.get("lost_date"),
                "drop_reason": link.get("drop_reason", ""),
            }
            lost_rows.append({
                k: v for k, v in lost_row.items()
                if v is not None and (v != "" or k == "drop_reason")
            })
    c = batch_upsert(
        client,
        "ahrefs_lost_backlinks",
        lost_rows,
        "website,referring_page_url,target_url,lost_date",
    )
    logger.info("  ahrefs_lost_backlinks: %d upserted", c)

    # Internal linking suggestions
    for ws in websites_in_run:
        deleted = delete_where(client, "internal_linking_suggestions", {"source_website": ws})
        if deleted:
            logger.info("  internal_linking_suggestions: removed %d stale rows for %s", deleted, ws)
    suggestion_rows = []
    for ws, data in parsed_data.items():
        suggestion_rows.extend(generate_internal_link_suggestions(ws, data, limit=30))
    cross_platform_rows = generate_cross_platform_link_suggestions(parsed_data, limit_per_source=30)
    if internal_links_has_cluster_reason:
        populate_internal_link_cluster_reasons(cross_platform_rows)
    suggestion_rows.extend(cross_platform_rows)
    populate_authority_problem_fields(suggestion_rows)
    _strip_unsupported_columns(suggestion_rows)
    c = batch_upsert(
        client,
        "internal_linking_suggestions",
        suggestion_rows,
        "suggestion_scope,source_website,source_page,target_website,target_page,target_page_keyword",
    )
    logger.info("  internal_linking_suggestions: %d upserted", c)


# ══════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Process Ahrefs CSVs from Supabase Storage")
    parser.add_argument("--date-folder", default=date.today().isoformat(),
                        help="Date folder in storage (default: today)")
    parser.add_argument("--local-dir", default=None,
                        help="Process from local directory instead of downloading from storage")
    parser.add_argument(
        "--internal-linking-only",
        action="store_true",
        help="Only rebuild internal_linking_suggestions from Ahrefs organic keywords, top pages, and internal links",
    )
    args = parser.parse_args()

    print()
    print("=" * 60)
    print("  PROCESS AHREFS CSVs")
    print(f"  Date folder: {args.date_folder}")
    if args.internal_linking_only:
        print("  Mode: internal-linking-only")
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
        if args.internal_linking_only and category not in {"organic_keywords", "top_pages", "internal_links"}:
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
            elif category == "backlinks":
                parsed_data[website]["backlinks"] = parse_backlinks(filepath, website)
            elif category == "broken_backlinks":
                parsed_data[website]["broken_backlinks"] = parse_broken_backlinks(filepath, website)
            elif category == "organic_competitors":
                parsed_data[website]["organic_competitors"] = parse_competitors(filepath, website)
            elif category == "internal_links":
                parsed_data[website]["internal_links"] = parse_internal_links(filepath, website)

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

    # Track in pipeline_runs
    pipeline_run_id = None
    if SUPABASE_URL and SUPABASE_SERVICE_KEY:
        from supabase import create_client as _pr_create_client
        try:
            _pr_client = _pr_create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
            res = _pr_client.table("pipeline_runs").insert({
                "pipeline": "ahrefs",
                "status": "running"
            }).execute()
            if res.data and len(res.data) > 0:
                pipeline_run_id = res.data[0]["id"]
        except Exception as e:
            logger.warning("Could not track pipeline_run start: %s", str(e)[:200])

    # Upload to Supabase
    print("\n--- Uploading to Supabase ---")
    status = "success"
    websites_succeeded = []
    websites_failed = []
    try:
        upload_parsed_data(parsed_data, run_id=run_id, internal_linking_only=args.internal_linking_only)
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

        # Track pipeline_runs completion
        if pipeline_run_id and SUPABASE_URL and SUPABASE_SERVICE_KEY:
            from supabase import create_client as _pr_create_client2
            try:
                _pr_client2 = _pr_create_client2(SUPABASE_URL, SUPABASE_SERVICE_KEY)
                from datetime import datetime as _dt, timezone as _tz
                _pr_client2.table("pipeline_runs").update({
                    "status": "completed" if status in ("success", "partial") else "failed",
                    "completed_at": _dt.now(_tz.utc).isoformat(),
                    "details": {"duration_seconds": duration_seconds}
                }).eq("id", pipeline_run_id).execute()
            except Exception as e:
                logger.warning("Could not track pipeline_run completion: %s", str(e)[:200])

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
