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
from datetime import date, datetime
from urllib.parse import urlparse
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

sys.path.insert(0, os.path.dirname(__file__))
from config import SUPABASE_URL, SUPABASE_SERVICE_KEY, AHREFS_BUCKET

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - available in normal runtime
    load_dotenv = None

try:
    from openai import OpenAI
except ImportError:  # pragma: no cover - available in normal runtime
    OpenAI = None

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
INTERNAL_LINKING_MODEL = os.getenv("INTERNAL_LINKING_MODEL", "gpt-4o")
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OpenAI and OPENAI_API_KEY else None

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


def _select_target_candidates(organic_keywords: dict) -> list[dict]:
    """Select one best target keyword per target URL."""
    best_by_url = {}
    for row in organic_keywords.get("keywords", []):
        url = _normalize_url(row.get("url", ""))
        position = row.get("position")
        volume = row.get("volume")
        keyword = (row.get("keyword") or "").strip()
        if not url or not keyword or position is None or volume is None:
            continue
        if position < 4 or position > 10 or volume < 1000:
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
        return strong_donors[:25]

    medium_donors = [row for row in all_pages if row["source_page_traffic"] >= 100]
    if medium_donors:
        return medium_donors[:25]

    return all_pages[:10]


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
        })

    for url, keywords in grouped.items():
        grouped[url] = sorted(
            keywords,
            key=lambda item: (item.get("volume") or 0, item.get("traffic") or 0, item.get("keyword") or ""),
            reverse=True,
        )[:3]
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
        section = _first_path_segment(url)
        keyword_summaries = [
            {
                "keyword": item.get("keyword", ""),
                "volume": int(item.get("volume") or 0),
            }
            for item in top_keywords[:3]
            if item.get("keyword")
        ]
        if not keyword_summaries and top_page.get("top_keyword"):
            keyword_summaries = [{
                "keyword": top_page.get("top_keyword", ""),
                "volume": int(top_page.get("top_keyword_volume") or 0),
            }]
        topic_tokens = (
            _url_path_tokens(url)
            | _text_tokens(title)
            | _text_tokens(meta_description)
            | _text_tokens(primary_keyword)
        )
        for item in keyword_summaries:
            topic_tokens |= _text_tokens(item["keyword"])

        enriched[url] = {
            "website": site_name,
            "url": url,
            "title": title,
            "meta_description": meta_description,
            "top_keywords": keyword_summaries,
            "traffic": traffic,
            "section": section,
            "topic_tokens": topic_tokens,
            "primary_keyword": primary_keyword,
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


def _light_prefilter_donors(
    donors: list[dict],
    target: dict,
    page_map: dict[str, dict],
    max_candidates: int = 15,
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


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception(lambda exc: True),
)
def _openai_json_response(system_prompt: str, payload: dict) -> dict:
    if not openai_client:
        raise RuntimeError("OPENAI_API_KEY is required for AI internal linking suggestions")
    response = openai_client.chat.completions.create(
        model=INTERNAL_LINKING_MODEL,
        temperature=0.1,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ],
    )
    content = response.choices[0].message.content or "{}"
    return json.loads(content)


def _build_target_payload(target: dict, page_map: dict[str, dict], target_site: str) -> dict:
    profile = page_map.get(target["target_page"], {})
    return {
        "website": target_site,
        "url": target["target_page"],
        "title": profile.get("title") or _infer_page_title(target["target_page"], target["target_page_keyword"]),
        "meta_description": profile.get("meta_description") or "",
        "top_keywords": profile.get("top_keywords") or [{"keyword": target["target_page_keyword"], "volume": target["target_page_volume"]}],
        "traffic": profile.get("traffic") or 0,
        "section": profile.get("section") or _first_path_segment(target["target_page"]),
        "primary_keyword": target["target_page_keyword"],
        "position": target["target_page_position"],
        "volume": target["target_page_volume"],
    }


def _build_donor_payload(donor: dict, page_map: dict[str, dict], source_site: str) -> dict:
    profile = page_map.get(donor["source_page"], {})
    return {
        "website": source_site,
        "url": donor["source_page"],
        "title": profile.get("title") or _infer_page_title(donor["source_page"], donor.get("source_top_keyword", "")),
        "meta_description": profile.get("meta_description") or "",
        "top_keywords": profile.get("top_keywords") or (
            [{"keyword": donor.get("source_top_keyword", ""), "volume": 0}] if donor.get("source_top_keyword") else []
        ),
        "traffic": donor.get("source_page_traffic") or profile.get("traffic") or 0,
        "section": profile.get("section") or _first_path_segment(donor["source_page"]),
        "primary_keyword": profile.get("primary_keyword") or donor.get("source_top_keyword", ""),
    }


def _layer1_generate_internal_links(
    source_site: str,
    target_site: str,
    target_payload: dict,
    donor_payloads: list[dict],
    scope: str,
) -> list[dict]:
    result = _openai_json_response(
        system_prompt=(
            "You are an internal linking strategist for SEO. "
            "Given one target page and a set of candidate donor pages, choose only the donor pages that should naturally link to the target. "
            "Use topic overlap, reader usefulness, keyword alignment, and site section context. "
            "Do not force weak matches. Return JSON with a `suggestions` array only. "
            "Each suggestion must include: source_page, anchor_text, reason, confidence."
        ),
        payload={
            "scope": scope,
            "source_site": source_site,
            "target_site": target_site,
            "target_page": target_payload,
            "candidate_donor_pages": donor_payloads,
            "instructions": {
                "max_suggestions": min(5, len(donor_payloads)),
                "confidence_scale": "0-100 integer",
                "anchor_rule": "Anchor text must be natural, descriptive, and fit the donor page context.",
                "reason_rule": "Reason must be one sentence in plain English and reference the actual page topic or keyword overlap.",
            },
        },
    )
    suggestions = result.get("suggestions") or []
    return suggestions if isinstance(suggestions, list) else []


def _layer2_validate_internal_link(
    source_site: str,
    target_site: str,
    target_payload: dict,
    donor_payload: dict,
    candidate: dict,
    scope: str,
) -> dict:
    return _openai_json_response(
        system_prompt=(
            "You are a skeptical SEO reviewer validating internal linking suggestions. "
            "Reject any stretch, weak topic match, vague anchor, or suggestion that feels only loosely related. "
            "Approve only if the donor page would genuinely help a reader navigate to the target page. "
            "Return JSON only with: approved, confidence, reason, anchor_text."
        ),
        payload={
            "scope": scope,
            "source_site": source_site,
            "target_site": target_site,
            "target_page": target_payload,
            "donor_page": donor_payload,
            "candidate_suggestion": candidate,
            "instructions": {
                "confidence_scale": "0-100 integer",
                "reason_rule": "Keep reason to one sentence and explain the actual topical connection.",
            },
        },
    )


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
    if not openai_client:
        logger.warning(
            "OPENAI_API_KEY missing; falling back to deterministic internal linking for %s (%s)",
            source_site,
            scope,
        )
        return _build_rule_based_suggestions(
            site_name=source_site,
            targets=targets,
            donors=donors,
            existing_pairs=existing_pairs,
            target_site_name=target_site,
            scope=scope,
            limit=limit,
        )

    logger.info(
        "  internal_linking[%s]: %s -> %s | %d targets, %d donor pages",
        scope,
        source_site,
        target_site,
        len(targets),
        len(donors),
    )
    suggestions = []
    seen = set()
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

        approved_for_target = 0
        for candidate in layer1_suggestions:
            source_page = _normalize_url(candidate.get("source_page", ""))
            donor_row = donor_rows.get(source_page)
            donor_payload = donor_lookup.get(source_page)
            if not donor_row or not donor_payload:
                continue

            dedupe_key = (scope, source_site, source_page, target_site, target_page, target["target_page_keyword"])
            if dedupe_key in seen:
                continue

            try:
                validation = _layer2_validate_internal_link(
                    source_site=source_site,
                    target_site=target_site,
                    target_payload=target_payload,
                    donor_payload=donor_payload,
                    candidate=candidate,
                    scope=scope,
                )
            except Exception as exc:
                logger.warning(
                    "AI internal linking layer 2 failed for %s → %s (%s): %s",
                    source_site,
                    target_site,
                    source_page,
                    str(exc)[:160],
                )
                continue

            if not validation.get("approved"):
                continue

            final_confidence = _coerce_confidence(validation.get("confidence"))
            if final_confidence is None:
                final_confidence = _coerce_confidence(candidate.get("confidence"))
            if final_confidence is None:
                continue

            final_reason = (validation.get("reason") or candidate.get("reason") or "").strip()
            final_anchor = (validation.get("anchor_text") or candidate.get("anchor_text") or target["target_page_keyword"]).strip()
            if not final_reason or not final_anchor:
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


def generate_internal_link_suggestions(site_name: str, site_data: dict, limit: int = 30) -> list[dict]:
    """Build AI-validated internal-link suggestions for one website."""
    organic_keywords = site_data.get("organic_keywords")
    top_pages = site_data.get("top_pages")
    internal_links = site_data.get("internal_links")
    if not organic_keywords or not top_pages or not internal_links:
        return []

    targets = _select_target_candidates(organic_keywords)
    donors = _select_source_candidates(top_pages, min_traffic=500)
    existing_pairs = _build_existing_link_pairs(internal_links)
    page_map = _build_page_enrichment(site_name, site_data)
    return _generate_ai_suggestions_for_scope(
        source_site=site_name,
        target_site=site_name,
        targets=targets,
        donors=donors,
        source_page_map=page_map,
        target_page_map=page_map,
        scope="within_site",
        limit=limit,
        existing_pairs=existing_pairs,
    )


def generate_cross_platform_link_suggestions(parsed_data: dict, limit_per_source: int = 30) -> list[dict]:
    """Build AI-validated cross-platform link suggestions across the Ztudium ecosystem."""
    site_inputs = {}
    for site_name, site_data in parsed_data.items():
        organic_keywords = site_data.get("organic_keywords")
        top_pages = site_data.get("top_pages")
        if not organic_keywords or not top_pages:
            continue
        targets = _select_target_candidates(organic_keywords)
        donors = _select_source_candidates(top_pages, min_traffic=500)
        if not targets or not donors:
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
    websites_in_run = sorted(parsed_data.keys())

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
    suggestion_rows.extend(generate_cross_platform_link_suggestions(parsed_data, limit_per_source=30))
    if not internal_links_has_reason or not internal_links_has_ai_confidence:
        for row in suggestion_rows:
            if not internal_links_has_reason:
                row.pop("reason", None)
            if not internal_links_has_ai_confidence:
                row.pop("ai_confidence", None)
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
