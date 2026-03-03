"""
Process keyword gap CSVs from Supabase Storage and upsert to content_gap_keywords.

Usage:
    python scripts/process_keyword_gap.py --date-folder 2026-03-03
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import re
import tempfile
import time
import uuid
from collections import Counter
from datetime import date

import requests
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

sys_path = os.path.dirname(__file__)
import sys

sys.path.insert(0, sys_path)
from config import KEYWORD_GAP_BUCKET, SUPABASE_SERVICE_KEY, SUPABASE_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("process_keyword_gap")

DOMAIN_TO_WEBSITE = {
    "citiesabc.com": "CitiesABC",
    "www.citiesabc.com": "CitiesABC",
    "businessabc.net": "BusinessABC",
    "www.businessabc.net": "BusinessABC",
    "hedgethink.com": "HedgeThink",
    "www.hedgethink.com": "HedgeThink",
    "fashionabc.org": "FashionABC",
    "www.fashionabc.org": "FashionABC",
    "tradersdna.com": "TradersDNA",
    "www.tradersdna.com": "TradersDNA",
    "freedomx.com": "FreedomX",
    "www.freedomx.com": "FreedomX",
    "wisdomia.ai": "Wisdomia",
    "www.wisdomia.ai": "Wisdomia",
    "sportsabc.org": "SportsDNA",
    "www.sportsabc.org": "SportsDNA",
    "sportsdna.ai": "SportsDNA",
    "www.sportsdna.ai": "SportsDNA",
    "intelligenthq.com": "IntelligentHQ",
    "www.intelligenthq.com": "IntelligentHQ",
}

SLUG_TO_WEBSITE = {
    "citiesabc": "CitiesABC",
    "businessabc": "BusinessABC",
    "hedgethink": "HedgeThink",
    "fashionabc": "FashionABC",
    "tradersdna": "TradersDNA",
    "freedomx": "FreedomX",
    "wisdomia": "Wisdomia",
    "sportsdna": "SportsDNA",
    "sportsabc": "SportsDNA",
    "intelligenthq": "IntelligentHQ",
}

EXCLUDE_TERMS = (
    "porn",
    "adult",
    "sex",
    "xxx",
    "nude",
    "escort",
    "casino",
    "bet",
    "gambl",
    "slot",
    "poker",
    "roblox",
    "minecraft",
)

INTENT_SIGNALS = {
    "informational": {
        "what",
        "how",
        "who",
        "why",
        "when",
        "where",
        "guide",
        "tutorial",
        "meaning",
        "definition",
        "review",
        "vs",
        "versus",
        "best",
        "top",
    },
    "commercial": {"price", "cost", "buy", "deal", "discount", "software", "service"},
}

STOP_WORDS = {
    "the",
    "a",
    "an",
    "of",
    "in",
    "to",
    "for",
    "and",
    "or",
    "is",
    "are",
    "was",
    "were",
    "it",
    "its",
    "on",
    "at",
    "by",
    "with",
    "from",
    "as",
    "be",
    "this",
    "that",
    "which",
    "what",
    "how",
    "who",
    "where",
    "when",
    "why",
    "vs",
}


def _is_retryable_api_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    markers = (
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
    return any(m in msg for m in markers)


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


def extract_snapshot_date(filename: str) -> str:
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", filename)
    return match.group(1) if match else date.today().isoformat()


def detect_website(filename: str) -> str | None:
    lower = filename.lower()

    domain_match = re.match(r"(?:www\.)?([a-z0-9-]+\.(?:com|net|org|ai|io))", lower)
    if domain_match:
        domain = domain_match.group(1)
        if domain in DOMAIN_TO_WEBSITE:
            return DOMAIN_TO_WEBSITE[domain]

    for slug, website in SLUG_TO_WEBSITE.items():
        if slug in lower:
            return website

    return None


def _as_int(value, default=0):
    if value in (None, "", "-"):
        return default
    try:
        txt = str(value).replace(",", "").strip()
        return int(float(txt))
    except Exception:
        return default


def _as_float(value, default=0.0):
    if value in (None, "", "-"):
        return default
    try:
        txt = str(value).replace(",", "").replace("$", "").strip()
        return float(txt)
    except Exception:
        return default


def _sanitize_keyword_payload(row: dict) -> dict:
    """Normalize payload types so inserts work against strict/int drifted schemas."""
    return {
        "date": row["date"],
        "website": row["website"],
        "keyword": row["keyword"],
        # Keep integer-safe fields as integers to avoid 22P02 on legacy integer schemas.
        "volume": _as_int(row.get("volume"), 0),
        "kd": _as_int(row.get("kd"), 0),
        "cpc": round(_as_float(row.get("cpc"), 0.0), 2),
        "serp_features": row.get("serp_features"),
        "intent": row.get("intent"),
        "is_easy_win": bool(row.get("is_easy_win", False)),
        "opportunity_score": round(_as_float(row.get("opportunity_score"), 0.0), 1),
        "cluster": row.get("cluster"),
        "competitors": row.get("competitors") or [],
    }


def _detect_intent(keyword: str) -> str:
    tokens = set(re.findall(r"[a-z0-9]+", keyword.lower()))
    if tokens.intersection(INTENT_SIGNALS["commercial"]):
        return "commercial"
    if tokens.intersection(INTENT_SIGNALS["informational"]) or keyword.endswith("?"):
        return "informational"
    return "other"


def _calc_opportunity_score(volume: int, kd: float, comp_count: int) -> float:
    vol_norm = min(max(volume, 0) / 50000.0, 1.0)
    kd_norm = max(0.0, (100.0 - max(kd, 0.0)) / 100.0)
    coverage = min(max(comp_count, 0) / 3.0, 1.0)
    return round((vol_norm * 50) + (kd_norm * 30) + (coverage * 20), 1)


def _detect_clusters(rows: list[dict], top_n: int = 12) -> dict[str, str]:
    words = Counter()
    for row in rows:
        tokens = [t for t in re.findall(r"[a-z0-9]+", row["keyword"].lower()) if t not in STOP_WORDS and len(t) > 2]
        words.update(tokens)
    top_words = [w for w, count in words.most_common(top_n) if count >= 3]

    assigned = {}
    for row in rows:
        kw = row["keyword"].lower()
        label = "Other"
        for word in top_words:
            if word in kw:
                label = word.title()
                break
        assigned[row["keyword"]] = label
    return assigned


def _read_csv_rows(filepath: str) -> list[dict]:
    encodings = ("utf-16", "utf-16-le", "utf-8-sig", "utf-8")
    for enc in encodings:
        try:
            with open(filepath, "r", encoding=enc) as f:
                content = f.read()
            reader = csv.DictReader(io.StringIO(content), delimiter="\t")
            if reader.fieldnames:
                return [{(k or "").strip(): (v or "").strip() for k, v in row.items()} for row in reader]
        except Exception:
            continue
    return []


def _detect_competitor_domains(headers: list[str], our_domain: str | None) -> list[str]:
    domains = []
    seen = set()
    for header in headers:
        match = re.match(r"(.+?)/:\s*Organic Position", header)
        if not match:
            continue
        domain = match.group(1).strip()
        if our_domain and our_domain in domain:
            continue
        if domain not in seen:
            seen.add(domain)
            domains.append(domain)
    return domains


def parse_keyword_gap_file(filepath: str) -> tuple[str | None, list[dict]]:
    filename = os.path.basename(filepath)
    website = detect_website(filename)
    if not website:
        logger.warning("Skipping file (website not detected): %s", filename)
        return None, []

    rows = _read_csv_rows(filepath)
    if not rows:
        logger.warning("Skipping unreadable or empty CSV: %s", filename)
        return website, []

    headers = list(rows[0].keys())
    domain_match = re.match(r"(?:www\.)?([a-z0-9-]+\.(?:com|net|org|ai|io))", filename.lower())
    our_domain = domain_match.group(1) if domain_match else None
    competitors = _detect_competitor_domains(headers, our_domain)
    snapshot_date = extract_snapshot_date(filename)

    parsed = []
    for row in rows:
        keyword = (row.get("Keyword") or row.get("keyword") or "").strip()
        if not keyword:
            continue
        if any(term in keyword.lower() for term in EXCLUDE_TERMS):
            continue

        volume = _as_int(row.get("Volume"), 0)
        kd = _as_int(row.get("KD"), 100)
        cpc = _as_float(row.get("CPC"), 0.0)
        serp_features = row.get("SERP features") or row.get("SERP Features") or None
        intent = _detect_intent(keyword)

        comp_rows = []
        comp_count = 0
        for domain in competitors:
            pos = row.get(f"{domain}/: Organic Position", "")
            if not pos:
                continue
            comp_count += 1
            comp_rows.append(
                {
                    "domain": domain,
                    "position": _as_float(pos, 0.0),
                    "traffic": _as_float(row.get(f"{domain}/: Organic Traffic"), 0.0),
                    "url": row.get(f"{domain}/: URL") or None,
                }
            )

        is_easy_win = volume >= 1000 and kd < 5
        score = _calc_opportunity_score(volume, float(kd), comp_count)
        parsed.append(
            {
                "date": snapshot_date,
                "website": website,
                "keyword": keyword,
                "volume": volume,
                "kd": kd,
                "cpc": cpc,
                "serp_features": serp_features,
                "intent": intent,
                "is_easy_win": is_easy_win,
                "opportunity_score": score,
                "competitors": comp_rows,
                "source_file": filename,
            }
        )

    cluster_map = _detect_clusters(parsed)
    for row in parsed:
        row["cluster"] = cluster_map.get(row["keyword"], "Other")

    logger.info(
        "Parsed %s -> website=%s rows=%d competitors=%d snapshot=%s",
        filename,
        website,
        len(parsed),
        len(competitors),
        snapshot_date,
    )
    return website, parsed


def dedupe_rows(rows: list[dict]) -> list[dict]:
    deduped = {}
    for row in rows:
        key = (row["date"], row["website"], row["keyword"].strip().lower())
        existing = deduped.get(key)
        if not existing:
            deduped[key] = row
            continue
        existing_score = existing.get("opportunity_score") or 0
        new_score = row.get("opportunity_score") or 0
        if new_score > existing_score:
            deduped[key] = row
        elif new_score == existing_score:
            if (row.get("volume") or 0) > (existing.get("volume") or 0):
                deduped[key] = row
    return list(deduped.values())


def download_from_storage(date_folder: str, temp_dir: str) -> list[str]:
    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "apikey": SUPABASE_SERVICE_KEY,
    }
    list_url = f"{SUPABASE_URL}/storage/v1/object/list/{KEYWORD_GAP_BUCKET}"

    resp = _http_post(
        list_url,
        headers=headers,
        json={"prefix": "", "limit": 500},
        timeout=30,
    )
    if resp.status_code != 200:
        logger.error("Failed to list bucket files (HTTP %d)", resp.status_code)
        return []

    items = [item for item in resp.json() if item.get("name", "").lower().endswith(".csv")]
    prefix = ""
    if not items:
        resp = _http_post(
            list_url,
            headers=headers,
            json={"prefix": f"{date_folder}/", "limit": 500},
            timeout=30,
        )
        if resp.status_code == 200:
            items = [item for item in resp.json() if item.get("name", "").lower().endswith(".csv")]
            prefix = f"{date_folder}/"

    downloaded = []
    for item in items:
        name = item.get("name", "")
        if not name:
            continue
        storage_path = f"{prefix}{name}" if prefix else name
        url = f"{SUPABASE_URL}/storage/v1/object/{KEYWORD_GAP_BUCKET}/{storage_path}"
        r = _http_get(url, headers=headers, timeout=120)
        if r.status_code != 200:
            logger.warning("Failed to download %s (HTTP %d)", storage_path, r.status_code)
            continue
        local_path = os.path.join(temp_dir, name)
        with open(local_path, "wb") as f:
            f.write(r.content)
        downloaded.append(local_path)
    return downloaded


def _is_retryable_upsert_error(exc: Exception) -> bool:
    return _is_retryable_api_error(exc)


def _upsert_chunk(client, table: str, chunk: list[dict], on_conflict: str):
    client.table(table).upsert(chunk, on_conflict=on_conflict).execute()


def _supports_column(client, table: str, column: str) -> bool:
    try:
        client.table(table).select(column).limit(1).execute()
        return True
    except Exception as exc:
        msg = str(exc).lower()
        return not ("column" in msg and "does not exist" in msg)


def batch_upsert(client, rows: list[dict], run_id: str | None = None) -> int:
    if not rows:
        return 0

    has_source_file = _supports_column(client, "content_gap_keywords", "source_file")
    has_run_id = _supports_column(client, "content_gap_keywords", "ingestion_run_id")
    normalized = []
    for row in rows:
        payload = _sanitize_keyword_payload(row)
        if has_source_file:
            payload["source_file"] = row.get("source_file")
        if run_id and has_run_id:
            payload["ingestion_run_id"] = run_id
        normalized.append(payload)

    inserted = 0
    chunk_size = 100
    for i in range(0, len(normalized), chunk_size):
        chunk = normalized[i : i + chunk_size]
        ok = False
        for attempt in range(1, 4):
            try:
                _upsert_chunk(client, "content_gap_keywords", chunk, "date,website,keyword")
                inserted += len(chunk)
                ok = True
                break
            except Exception as exc:
                if attempt < 3 and _is_retryable_upsert_error(exc):
                    time.sleep(attempt)
                    continue
                logger.warning("Batch upsert fallback triggered: %s", str(exc)[:200])
                break
        if not ok:
            for row in chunk:
                try:
                    client.table("content_gap_keywords").upsert(
                        [row],
                        on_conflict="date,website,keyword",
                    ).execute()
                    inserted += 1
                except Exception as exc:
                    logger.error(
                        "Row upsert failed for keyword=%s (volume=%s kd=%s cpc=%s): %s",
                        row.get("keyword"),
                        row.get("volume"),
                        row.get("kd"),
                        row.get("cpc"),
                        str(exc)[:200],
                    )
                time.sleep(0.04)
        time.sleep(0.15)
    return inserted


def _start_ingestion_run(source: str, websites_attempted: list[str]) -> str | None:
    from supabase import create_client

    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return None
    try:
        client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        run_id = str(uuid.uuid4())
        client.table("ingestion_runs").insert(
            {
                "id": run_id,
                "source": source,
                "status": "running",
                "websites_attempted": websites_attempted,
            }
        ).execute()
        return run_id
    except Exception as exc:
        logger.warning("Run tracking start failed: %s", str(exc)[:200])
        return None


def _finish_ingestion_run(
    run_id: str | None,
    status: str,
    websites_succeeded: list[str],
    websites_failed: list[str],
    error_details: dict,
    duration_seconds: int,
):
    from supabase import create_client

    if not run_id or not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return
    try:
        client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        client.table("ingestion_runs").update(
            {
                "status": status,
                "websites_succeeded": websites_succeeded,
                "websites_failed": websites_failed,
                "error_details": error_details,
                "duration_seconds": duration_seconds,
            }
        ).eq("id", run_id).execute()
    except Exception as exc:
        logger.warning("Run tracking finish failed: %s", str(exc)[:200])


def main():
    parser = argparse.ArgumentParser(description="Process keyword gap CSV files")
    parser.add_argument("--date-folder", default=date.today().isoformat())
    parser.add_argument("--local-dir", default=None)
    args = parser.parse_args()

    print()
    print("=" * 56)
    print("  PROCESS KEYWORD GAP CSVs")
    print(f"  Bucket: {KEYWORD_GAP_BUCKET}")
    print("=" * 56)
    started = time.time()

    if args.local_dir:
        files = [
            os.path.join(args.local_dir, f)
            for f in os.listdir(args.local_dir)
            if f.lower().endswith(".csv")
        ]
    else:
        temp_dir = tempfile.mkdtemp(prefix="keyword_gap_")
        files = download_from_storage(args.date_folder, temp_dir)

    if not files:
        logger.error("No CSV files to process.")
        sys.exit(1)

    all_rows = []
    website_errors = {}
    website_seen = set()
    for filepath in sorted(files):
        try:
            website, rows = parse_keyword_gap_file(filepath)
            if website:
                website_seen.add(website)
            all_rows.extend(rows)
        except Exception as exc:
            name = os.path.basename(filepath)
            website_errors[name] = str(exc)
            logger.error("Failed parsing %s: %s", name, str(exc)[:200], exc_info=True)

    if not all_rows:
        logger.warning("Parsed 0 rows from %d files.", len(files))

    deduped_rows = dedupe_rows(all_rows)
    logger.info("Rows: parsed=%d deduped=%d", len(all_rows), len(deduped_rows))

    from supabase import create_client

    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    run_id = _start_ingestion_run("keyword_gap", sorted(website_seen))
    status = "success"
    succeeded = []
    failed = []
    try:
        upserted = batch_upsert(client, deduped_rows, run_id=run_id)
        logger.info("Upserted rows: %d", upserted)
        succeeded = sorted(website_seen)
        if website_errors:
            status = "partial"
            failed = sorted(set(website_errors.keys()))
    except Exception as exc:
        status = "failed"
        website_errors["upload"] = str(exc)
        failed = sorted(website_seen)
        raise
    finally:
        _finish_ingestion_run(
            run_id=run_id,
            status=status,
            websites_succeeded=succeeded,
            websites_failed=failed,
            error_details=website_errors,
            duration_seconds=int(time.time() - started),
        )

    print("\nDone.")


if __name__ == "__main__":
    main()
