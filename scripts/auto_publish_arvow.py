"""Automatically dispatch top daily content opportunities to Arvow for selected sites."""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from dotenv import load_dotenv
from supabase import create_client

from config import WEBSITES
from generate_daily_content_opportunities import (
    OPENAI_API_KEY,
    SUPABASE_KEY,
    SUPABASE_URL,
    build_site_arvow_config,
    clean_text,
    clean_url,
    jaccard_similarity,
    normalize_key,
    require_env,
    slugify,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("auto_publish_arvow")

AUTO_STATUSES = {"pending", "approved"}
ATTEMPTED_STATUSES = {
    "queued_for_arvow",
    "sent_to_arvow",
    "verification_pending",
    "published",
    "verification_failed",
}


def configure_logging(debug: bool = False) -> None:
    if debug:
        logger.setLevel(logging.DEBUG)


def redact_value(value: Any) -> Any:
    if isinstance(value, dict):
        redacted = {}
        for key, item in value.items():
            if key.lower() in {"key", "api_key", "authorization", "token"} and isinstance(item, str):
                redacted[key] = f"{item[:4]}...{item[-4:]}" if len(item) > 8 else "***"
            else:
                redacted[key] = redact_value(item)
        return redacted
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    return value


def response_error_text(response: dict[str, Any] | str | None, status_code: int) -> str:
    if isinstance(response, str):
        return response
    if isinstance(response, dict):
        for key in ("error", "message", "detail", "details"):
            message = clean_text(response.get(key))
            if message:
                return message
        return json.dumps(response)
    return f"HTTP {status_code} with empty response"


def build_history_payload(
    *,
    request_payload: dict[str, Any] | None,
    status_code: int,
    response_body: dict[str, Any] | str | None,
) -> dict[str, Any]:
    return {
        "http_status": status_code,
        "request": redact_value(request_payload) if request_payload else None,
        "response_body": response_body,
    }


def auto_publish_sites() -> list[dict[str, Any]]:
    return [site for site in WEBSITES if bool(site.get("auto_publish_enabled"))]


def today_iso() -> str:
    return date.today().isoformat()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def extract_arvow_payload(row: dict[str, Any], site_name: str) -> dict[str, Any]:
    payload = row.get("arvow_payload") or {}
    if isinstance(payload, str):
        payload = json.loads(payload)
    body = payload.get("body") or {}
    config = build_site_arvow_config(site_name)
    formula = body.get("formula") or {}
    internal_linking = formula.get("internalLinking") or {}
    external_linking = formula.get("externalLinking") or {}
    videos = formula.get("videos") or {}

    body["key"] = os.getenv("ARVOW_API_KEY") or body.get("key")
    body["integrationId"] = config["integration_id"] or body.get("integrationId")
    formula["internalLinking"] = {
        **internal_linking,
        "sitemaps": [{"url": url} for url in config["sitemaps"]],
    }
    formula["externalLinking"] = {
        **external_linking,
        "includeSources": config["external_sources"],
        "automateExternalLinks": True,
    }
    formula["videos"] = {
        **videos,
        "youtubeLinks": config["youtube_links"],
        "automateYoutubeLinks": True,
    }
    body["formula"] = formula
    endpoint = clean_text(payload.get("endpoint")) or "https://api.arvow.com/api/v0.1/batch"
    method = clean_text(payload.get("method")).upper() or "POST"

    if not body.get("key"):
        raise RuntimeError(f"{site_name}: missing ARVOW_API_KEY")
    if not body.get("integrationId"):
        raise RuntimeError(f"{site_name}: missing integration id")
    if not body.get("formula"):
        raise RuntimeError(f"{site_name}: payload formula missing")

    return {
        "endpoint": endpoint,
        "method": method,
        "body": body,
    }


def fetch_site_candidates(client, site_name: str) -> list[dict[str, Any]]:
    rows = (
        client.table("daily_content_opportunities")
        .select("*")
        .eq("site", site_name)
        .eq("generated_date", today_iso())
        .order("priority_score", desc=True)
        .order("updated_at", desc=True)
        .limit(50)
        .execute()
        .data
        or []
    )
    return [row for row in rows if clean_text(row.get("status")).lower() in AUTO_STATUSES]


def count_attempted_today(client, site_name: str) -> int:
    rows = (
        client.table("daily_content_opportunities")
        .select("id, status")
        .eq("site", site_name)
        .eq("generated_date", today_iso())
        .in_("status", list(ATTEMPTED_STATUSES))
        .execute()
        .data
        or []
    )
    return len(rows)


def load_cannibalization_context(client, site_name: str) -> dict[str, list[dict[str, Any]]]:
    since = (date.today() - timedelta(days=45)).isoformat()
    pages = (
        client.table("website_pages")
        .select("url, clicks, impressions, ga_sessions, position, date")
        .eq("website", site_name)
        .gte("date", since)
        .order("date", desc=True)
        .limit(500)
        .execute()
        .data
        or []
    )
    keywords = (
        client.table("website_keywords")
        .select("keyword, clicks, impressions, position, date")
        .eq("website", site_name)
        .gte("date", since)
        .order("date", desc=True)
        .limit(500)
        .execute()
        .data
        or []
    )
    existing_published = (
        client.table("daily_content_opportunities")
        .select("title, primary_keyword, published_url")
        .eq("site", site_name)
        .eq("status", "published")
        .limit(500)
        .execute()
        .data
        or []
    )
    return {
        "pages": pages,
        "keywords": keywords,
        "published": existing_published,
    }


def url_signature(url: str) -> str:
    cleaned = clean_url(url)
    if not cleaned:
        return ""
    path = cleaned.split("://", 1)[-1].split("/", 1)
    slug = path[1] if len(path) > 1 else cleaned
    return normalize_key(slug.replace("-", " "))


def has_cannibalization_conflict(opportunity: dict[str, Any], context: dict[str, list[dict[str, Any]]]) -> tuple[bool, str]:
    title = clean_text(opportunity.get("title"))
    keyword = clean_text(opportunity.get("primary_keyword"))
    keyword_key = normalize_key(keyword)

    for row in context["keywords"]:
        existing_keyword = clean_text(row.get("keyword"))
        position = row.get("position")
        if normalize_key(existing_keyword) == keyword_key and (position is None or float(position or 0) <= 30):
            return True, f"Existing ranking keyword conflict: {existing_keyword}"

    for row in context["pages"]:
        signature = url_signature(clean_text(row.get("url")))
        if not signature:
            continue
        if jaccard_similarity(signature, keyword) >= 0.85:
            return True, f"Existing URL already targets similar keyword: {row.get('url')}"
        if jaccard_similarity(signature, title) >= 0.68:
            return True, f"Existing URL already matches similar title: {row.get('url')}"

    for row in context["published"]:
        published_title = clean_text(row.get("title"))
        published_keyword = clean_text(row.get("primary_keyword"))
        published_url = clean_text(row.get("published_url"))
        if normalize_key(published_keyword) == keyword_key:
            return True, f"Already published via automation: {published_url or published_title}"
        if jaccard_similarity(published_title, title) >= 0.8:
            return True, f"Already published similar title: {published_url or published_title}"

    return False, ""


def update_opportunity(client, opportunity_id: str, **patch: Any) -> None:
    patch["updated_at"] = now_iso()
    client.table("daily_content_opportunities").update(patch).eq("id", opportunity_id).execute()


def insert_publish_history(
    client,
    *,
    site: str,
    opportunity_id: str,
    arvow_batch_id: str | None,
    arvow_response: dict[str, Any] | None,
    published_url: str | None,
    status: str,
    error_message: str | None = None,
    sent_at: str | None = None,
    verified_at: str | None = None,
) -> None:
    client.table("arvow_publish_history").insert(
        {
            "site": site,
            "opportunity_id": opportunity_id,
            "arvow_batch_id": arvow_batch_id,
            "arvow_response": arvow_response,
            "published_url": published_url,
            "status": status,
            "error_message": error_message,
            "sent_at": sent_at,
            "verified_at": verified_at,
        }
    ).execute()


def dispatch_payload(payload: dict[str, Any], *, debug: bool = False) -> tuple[bool, int, dict[str, Any] | str | None]:
    if debug:
        logger.debug("Arvow request: %s", json.dumps(redact_value(payload), ensure_ascii=False))
    response = requests.request(
        payload["method"],
        payload["endpoint"],
        json=payload["body"],
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        timeout=90,
    )
    text = response.text
    parsed: dict[str, Any] | str | None
    try:
        parsed = response.json() if text else None
    except Exception:
        parsed = text
    if debug:
        logger.debug("Arvow response HTTP %s: %s", response.status_code, json.dumps(parsed, ensure_ascii=False) if isinstance(parsed, dict) else parsed)
    return response.ok, response.status_code, parsed


def process_site(client, site: dict[str, Any], dry_run: bool = False, debug: bool = False) -> int:
    site_name = site["name"]
    config = build_site_arvow_config(site_name)
    limit = int(site.get("daily_publish_limit") or 0)
    if not limit:
        return 0

    attempted = count_attempted_today(client, site_name)
    remaining = max(0, limit - attempted)
    if remaining == 0:
        logger.info("[%s] already attempted %d item(s) today; skipping", site_name, attempted)
        return 0

    candidates = fetch_site_candidates(client, site_name)
    if not candidates:
        logger.info("[%s] no pending candidates for today", site_name)
        return 0

    context = load_cannibalization_context(client, site_name)
    selected: list[dict[str, Any]] = []
    for row in candidates:
        conflict, reason = has_cannibalization_conflict(row, context)
        if conflict:
            logger.info("[%s] cannibalization skip: %s", site_name, clean_text(row.get("title")))
            if not dry_run:
                update_opportunity(
                    client,
                    row["id"],
                    status="ignored",
                    verification_notes=f"Skipped by auto-publish cannibalization check: {reason}",
                )
            continue
        selected.append(row)
        if len(selected) >= remaining:
            break

    if not selected:
        logger.info("[%s] no rows survived cannibalization checks", site_name)
        return 0

    if dry_run:
        for row in selected:
            payload = extract_arvow_payload(row, site_name)
            if debug:
                logger.debug("[%s] dry-run payload for '%s': %s", site_name, clean_text(row.get("title")), json.dumps(redact_value(payload), ensure_ascii=False))
        logger.info("[%s] dry-run would dispatch %d row(s)", site_name, len(selected))
        return len(selected)

    logger.info(
        "[%s] auto-publishing %d/%d row(s) with integration %s",
        site_name,
        len(selected),
        limit,
        config["integration_id"] or "missing",
    )

    sent_count = 0
    for row in selected:
        queued_at = now_iso()
        update_opportunity(
            client,
            row["id"],
            status="queued_for_arvow",
            verification_notes="Queued for automated Arvow dispatch.",
        )
        insert_publish_history(
            client,
            site=site_name,
            opportunity_id=row["id"],
            arvow_batch_id=None,
            arvow_response=None,
            published_url=None,
            status="queued_for_arvow",
            sent_at=queued_at,
        )

        try:
            payload = extract_arvow_payload(row, site_name)
            ok, status_code, response = dispatch_payload(payload, debug=debug)
            batch_id = None
            job_id = None
            if isinstance(response, dict):
                batch_id = clean_text(response.get("batch_id") or response.get("batchId")) or None
                job_id = clean_text(response.get("job_id") or response.get("jobId") or response.get("id")) or None
            history_payload = build_history_payload(
                request_payload=payload,
                status_code=status_code,
                response_body=response,
            )

            if ok and not (batch_id or job_id):
                ok = False
                response = (
                    response if isinstance(response, dict) else {"raw": response}
                ) or {}
                if isinstance(response, dict):
                    response["integration_error"] = "Arvow accepted the request but did not return a trackable batch or job id."
                history_payload = build_history_payload(
                    request_payload=payload,
                    status_code=status_code,
                    response_body=response,
                )

            if ok:
                dispatched_at = now_iso()
                update_opportunity(
                    client,
                    row["id"],
                    status="sent_to_arvow",
                    arvow_batch_id=batch_id,
                    arvow_job_id=job_id,
                    sent_to_arvow_at=dispatched_at,
                    verification_notes="Sent to Arvow successfully. Awaiting live verification.",
                )
                insert_publish_history(
                    client,
                    site=site_name,
                    opportunity_id=row["id"],
                    arvow_batch_id=batch_id or job_id,
                    arvow_response=history_payload,
                    published_url=None,
                    status="sent_to_arvow",
                    sent_at=dispatched_at,
                )
                sent_count += 1
                logger.info("[%s] sent '%s' to Arvow", site_name, clean_text(row.get("title")))
            else:
                error_message = response_error_text(response, status_code)
                update_opportunity(
                    client,
                    row["id"],
                    status="verification_failed",
                    arvow_batch_id=batch_id,
                    arvow_job_id=job_id,
                    sent_to_arvow_at=now_iso(),
                    verification_notes=f"Arvow dispatch failed with HTTP {status_code}.",
                )
                insert_publish_history(
                    client,
                    site=site_name,
                    opportunity_id=row["id"],
                    arvow_batch_id=batch_id or job_id,
                    arvow_response=history_payload,
                    published_url=None,
                    status="verification_failed",
                    error_message=error_message,
                    sent_at=now_iso(),
                )
                logger.error("[%s] failed to send '%s' (HTTP %s)", site_name, clean_text(row.get("title")), status_code)
        except Exception as exc:
            update_opportunity(
                client,
                row["id"],
                status="verification_failed",
                verification_notes=f"Arvow automation error: {exc}",
            )
            insert_publish_history(
                client,
                site=site_name,
                opportunity_id=row["id"],
                arvow_batch_id=None,
                arvow_response=None,
                published_url=None,
                status="verification_failed",
                error_message=str(exc),
                sent_at=now_iso(),
            )
            logger.error("[%s] automation error for '%s': %s", site_name, clean_text(row.get("title")), exc)

    return sent_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", help="Limit automation to one site")
    parser.add_argument("--dry-run", action="store_true", help="Do not write or send requests")
    parser.add_argument("--debug", action="store_true", help="Log redacted request/response details")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(args.debug)
    require_env("SUPABASE_URL", SUPABASE_URL)
    require_env("SUPABASE_SERVICE_KEY", SUPABASE_KEY)
    require_env("ARVOW_API_KEY", os.getenv("ARVOW_API_KEY", ""))
    if not OPENAI_API_KEY:
        logger.info("OPENAI_API_KEY is not required for auto-publish; continuing without it")

    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    sites = auto_publish_sites()
    if args.site:
        sites = [site for site in sites if site["name"] == args.site]
        if not sites:
            raise RuntimeError(f"Unknown or non-auto site: {args.site}")

    total_sent = 0
    for site in sites:
        total_sent += process_site(client, site, dry_run=args.dry_run, debug=args.debug)

    if args.dry_run:
        logger.info("Auto-publish dry-run completed. %d item(s) ready.", total_sent)
    else:
        logger.info("Auto-publish completed. %d item(s) sent.", total_sent)


if __name__ == "__main__":
    main()
