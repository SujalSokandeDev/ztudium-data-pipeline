"""
Validate URLs stored in backlink-related tables and mark resolved rows.

Usage:
    python scripts/validate_backlink_urls.py
    python scripts/validate_backlink_urls.py --batch-size 50 --batch-delay 2
"""

import argparse
import html
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Iterable
from urllib.parse import urlparse

import requests
from requests import Response
from supabase import create_client

from config import SUPABASE_SERVICE_KEY, SUPABASE_URL


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("validate_backlink_urls")

TRANSIENT_STATUS_CODES = {429, 502, 503, 504}
SOFT_404_PATTERNS = (
    "page not found",
    "not found",
    "does not exist",
    "no longer available",
    "404",
    "error page",
)


def normalize_url(raw_url: str) -> str | None:
    raw = (raw_url or "").strip()
    if not raw:
        return None
    if "://" not in raw:
        raw = f"https://{raw}"
    parsed = urlparse(raw)
    if parsed.scheme.lower() not in {"http", "https"}:
        return None
    if not parsed.netloc:
        return None
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")
    normalized = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.strip().lower(),
        path=path,
        fragment="",
    )
    return normalized.geturl()


def extract_visible_text(content: str) -> str:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", content)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:2000]


def extract_title(content: str) -> str:
    match = re.search(r"(?is)<title[^>]*>(.*?)</title>", content)
    if not match:
        return ""
    return html.unescape(re.sub(r"\s+", " ", match.group(1))).strip()[:300]


def looks_generic_redirect(original_url: str, final_url: str) -> bool:
    original = urlparse(original_url)
    final = urlparse(final_url)
    if original.netloc != final.netloc:
        return False
    original_path = (original.path or "/").rstrip("/") or "/"
    final_path = (final.path or "/").rstrip("/") or "/"
    return original_path not in {"", "/"} and final_path in {"", "/search", "/search/"}


def detect_soft_404(original_url: str, response: Response, body_text: str, title: str) -> str | None:
    if 200 <= response.status_code < 300:
        combined = f"{title} {body_text}".lower()
        if any(pattern in combined for pattern in SOFT_404_PATTERNS):
            return "soft-404 phrase detected"
        if looks_generic_redirect(original_url, response.url):
            return "redirected to generic landing/search page"
        if len(body_text.strip()) < 80:
            return "very short page body"
    return None


def request_with_fallback(session: requests.Session, url: str, timeout: int = 10) -> tuple[Response | None, str]:
    last_error = ""
    for attempt in range(3):
        try:
            head = session.head(url, allow_redirects=True, timeout=timeout)
            if head.status_code < 400 and head.status_code not in {204, 405}:
                return head, "HEAD"
            if head.status_code in TRANSIENT_STATUS_CODES:
                retry_after = head.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    time.sleep(min(int(retry_after), 30))
                else:
                    time.sleep(2 ** attempt)
            get = session.get(url, allow_redirects=True, timeout=timeout)
            return get, "GET"
        except requests.RequestException as exc:
            last_error = str(exc)
            if attempt < 2:
                time.sleep(2 ** attempt)
                continue
    return None, last_error


def classify_url(url: str) -> dict:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Ztudium-URL-Validator/1.0 (+https://ztudium.com)",
        "Accept-Language": "en-US,en;q=0.9",
    })

    response, method_or_error = request_with_fallback(session, url)
    if response is None:
        return {
            "result": "blocked",
            "confidence": "medium",
            "notes": f"request failed after retries: {method_or_error}",
            "final_url": url,
            "final_status": None,
            "body": "",
        }

    final_status = response.status_code
    final_url = response.url
    body = ""
    title = ""
    notes = [f"{method_or_error} {final_status}", f"final_url={final_url}"]

    if method_or_error == "HEAD" and 200 <= final_status < 300:
        try:
            response = session.get(url, allow_redirects=True, timeout=10)
            final_status = response.status_code
            final_url = response.url
            notes = [f"HEAD {notes[0].split()[-1]}", f"GET {final_status}", f"final_url={final_url}"]
        except requests.RequestException as exc:
            notes.append(f"GET fallback failed: {str(exc)[:180]}")

    if method_or_error == "GET" or response.request.method.upper() == "GET" or final_status in {200, 204}:
        body = response.text[:25000]
        title = extract_title(body)
        text = extract_visible_text(body)
        soft_404_reason = detect_soft_404(url, response, text, title)
        if soft_404_reason:
            return {
                "result": "needs_review",
                "confidence": "medium",
                "notes": "; ".join(notes + [soft_404_reason]),
                "final_url": final_url,
                "final_status": final_status,
                "body": body,
            }

    if 200 <= final_status < 300:
        result = "redirected" if normalize_url(final_url) != normalize_url(url) else "ok"
        confidence = "high" if result in {"ok", "redirected"} else "medium"
        return {
            "result": result,
            "confidence": confidence,
            "notes": "; ".join(notes),
            "final_url": final_url,
            "final_status": final_status,
            "body": body,
        }
    if final_status in {401, 403}:
        return {
            "result": "blocked",
            "confidence": "high",
            "notes": "; ".join(notes + ["access denied"]),
            "final_url": final_url,
            "final_status": final_status,
            "body": body,
        }
    if final_status in TRANSIENT_STATUS_CODES:
        return {
            "result": "blocked",
            "confidence": "medium",
            "notes": "; ".join(notes + ["transient upstream status"]),
            "final_url": final_url,
            "final_status": final_status,
            "body": body,
        }
    return {
        "result": "broken",
        "confidence": "high",
        "notes": "; ".join(notes),
        "final_url": final_url,
        "final_status": final_status,
        "body": body,
    }


def backlink_present(page_html: str, target_url: str) -> bool:
    normalized_target = normalize_url(target_url)
    if not normalized_target:
        return False
    parsed_target = urlparse(normalized_target)
    escaped_target = re.escape(normalized_target)
    escaped_host = re.escape(parsed_target.netloc)
    escaped_path = re.escape(parsed_target.path.rstrip("/"))
    patterns = [
        escaped_target,
        rf'href=["\']https?://(?:www\.)?{escaped_host}{escaped_path}/?["\']',
        rf'href=["\']/(?:[^"\']*){escaped_path.lstrip("/")}/?["\']',
    ]
    lowered_html = page_html.lower()
    return any(re.search(pattern.lower(), lowered_html) for pattern in patterns if pattern)


def fetch_all_rows(client, table: str, page_size: int = 1000) -> list[dict]:
    rows = []
    start = 0
    while True:
        response = client.table(table).select("*").range(start, start + page_size - 1).execute()
        chunk = response.data or []
        rows.extend(chunk)
        if len(chunk) < page_size:
            break
        start += page_size
    return rows


def fetch_rows_by_status(client, table: str, status: str, page_size: int = 1000) -> list[dict]:
    rows = []
    start = 0
    while True:
        response = (
            client.table(table)
            .select("*")
            .eq("validation_status", status)
            .range(start, start + page_size - 1)
            .execute()
        )
        chunk = response.data or []
        rows.extend(chunk)
        if len(chunk) < page_size:
            break
        start += page_size
    return rows


def fetch_rows_for_validation(client, table: str, recheck_resolved_after_hours: int, page_size: int = 1000) -> list[dict]:
    pending = fetch_rows_by_status(client, table, "pending", page_size=page_size)
    needs_review = fetch_rows_by_status(client, table, "needs_review", page_size=page_size)
    confirmed_broken = fetch_rows_by_status(client, table, "confirmed_broken", page_size=page_size)
    resolved = fetch_rows_by_status(client, table, "resolved", page_size=page_size)

    if recheck_resolved_after_hours > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=recheck_resolved_after_hours)
        resolved = [
            row for row in resolved
            if (parsed := parse_iso_datetime(row.get("last_validated_at"))) is None
            or parsed <= cutoff
        ]
    else:
        resolved = []

    return pending + needs_review + confirmed_broken + resolved


def parse_iso_datetime(value: str | None) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def chunked(items: list[dict], size: int) -> Iterable[list[dict]]:
    for index in range(0, len(items), size):
        yield items[index:index + size]


def apply_validation_update(client, table: str, row_id: str, status: str, notes: str):
    payload = {
        "validation_status": status,
        "validation_notes": notes[:1000],
        "last_validated_at": datetime.now(timezone.utc).isoformat(),
    }
    if status == "resolved":
        payload["resolved_at"] = datetime.now(timezone.utc).isoformat()
    else:
        payload["resolved_at"] = None
    client.table(table).update(payload).eq("id", row_id).execute()


def validate_broken_backlink_row(row: dict) -> tuple[str, str]:
    normalized_url = normalize_url(row.get("target_url"))
    if not normalized_url:
        return "needs_review", "invalid or unsupported target URL"

    validation = classify_url(normalized_url)
    result = validation["result"]
    confidence = validation["confidence"]
    notes = f"{result}/{confidence}: {validation['notes']}"

    if result in {"ok", "redirected"} and confidence == "high":
        return "resolved", notes
    if result in {"broken", "blocked"}:
        return "confirmed_broken", notes
    return "needs_review", notes


def validate_lost_backlink_row(row: dict) -> tuple[str, str]:
    referring_url = normalize_url(row.get("referring_page_url"))
    if not referring_url:
        return "needs_review", "invalid or unsupported referring page URL"

    validation = classify_url(referring_url)
    result = validation["result"]
    confidence = validation["confidence"]
    notes = [f"{result}/{confidence}: {validation['notes']}"]

    if result in {"broken", "blocked"}:
        return "confirmed_broken", "; ".join(notes)
    if result == "needs_review":
        return "needs_review", "; ".join(notes)

    target_url = row.get("target_url") or ""
    page_html = validation.get("body") or ""
    if page_html and backlink_present(page_html, target_url):
        notes.append("target link detected on referring page")
        return "resolved", "; ".join(notes)

    notes.append("target link not detected on live referring page")
    if confidence == "high":
        return "confirmed_broken", "; ".join(notes)
    return "needs_review", "; ".join(notes)


def run_validation(batch_size: int, batch_delay: float, recheck_resolved_after_hours: int):
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_KEY are required")

    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    broken_rows = fetch_rows_for_validation(
        client,
        "ahrefs_broken_backlinks",
        recheck_resolved_after_hours=recheck_resolved_after_hours,
    )
    lost_rows = fetch_rows_for_validation(
        client,
        "ahrefs_lost_backlinks",
        recheck_resolved_after_hours=recheck_resolved_after_hours,
    )
    all_items = [("ahrefs_broken_backlinks", row) for row in broken_rows] + [
        ("ahrefs_lost_backlinks", row) for row in lost_rows
    ]

    summary = {
        "checked": 0,
        "resolved": 0,
        "confirmed_broken": 0,
        "needs_review": 0,
    }

    logger.info(
        "Validating %d backlink rows (%d broken + %d lost) in batches of %d (resolved recheck window: %dh)",
        len(all_items),
        len(broken_rows),
        len(lost_rows),
        batch_size,
        recheck_resolved_after_hours,
    )

    for batch_number, batch in enumerate(chunked(all_items, batch_size), start=1):
        logger.info("Processing batch %d (%d rows)", batch_number, len(batch))
        for table, row in batch:
            try:
                if table == "ahrefs_broken_backlinks":
                    status, notes = validate_broken_backlink_row(row)
                else:
                    status, notes = validate_lost_backlink_row(row)
                apply_validation_update(client, table, row["id"], status, notes)
                summary["checked"] += 1
                summary[status] += 1
            except Exception as exc:
                logger.warning("%s row %s failed validation: %s", table, row.get("id"), str(exc)[:200])
                apply_validation_update(
                    client,
                    table,
                    row["id"],
                    "needs_review",
                    f"validation exception: {str(exc)[:900]}",
                )
                summary["checked"] += 1
                summary["needs_review"] += 1

        if batch_delay > 0:
            time.sleep(batch_delay)

    logger.info(
        "Validation summary: checked=%d resolved=%d confirmed_broken=%d needs_review=%d",
        summary["checked"],
        summary["resolved"],
        summary["confirmed_broken"],
        summary["needs_review"],
    )


def main():
    parser = argparse.ArgumentParser(description="Validate backlink URLs and mark resolved rows")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--batch-delay", type=float, default=2.0)
    parser.add_argument("--recheck-resolved-after-hours", type=int, default=168)
    args = parser.parse_args()
    run_validation(
        batch_size=args.batch_size,
        batch_delay=args.batch_delay,
        recheck_resolved_after_hours=args.recheck_resolved_after_hours,
    )


if __name__ == "__main__":
    main()
