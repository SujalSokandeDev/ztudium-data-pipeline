"""Verify automated Arvow publishing and sync AI content clusters."""

from __future__ import annotations

import argparse
import json
import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from supabase import create_client
from postgrest.exceptions import APIError

from config import WEBSITES
from generate_daily_content_opportunities import (
    SUPABASE_KEY,
    SUPABASE_URL,
    build_site_arvow_config,
    clean_text,
    clean_url,
    jaccard_similarity,
    normalize_key,
    require_env,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("verify_arvow_publish")

VERIFYABLE_STATUSES = {"sent_to_arvow", "verification_pending"}
VERIFY_LOOKBACK_DAYS = int(os.getenv("ARVOW_VERIFY_LOOKBACK_DAYS", "7"))
MIN_VERIFY_DELAY_MINUTES = int(os.getenv("ARVOW_VERIFY_MIN_DELAY_MINUTES", "30"))
MAX_VERIFICATION_ATTEMPTS = int(os.getenv("ARVOW_VERIFY_MAX_ATTEMPTS", "4"))
VERIFY_ATTEMPT_WINDOWS = [
    max(MIN_VERIFY_DELAY_MINUTES, int(value.strip()))
    for value in os.getenv("ARVOW_VERIFY_ATTEMPT_WINDOWS", "30,60,120,360").split(",")
    if value.strip()
]
ARVOW_STATUS_URL_TEMPLATE = clean_text(os.getenv("ARVOW_STATUS_URL_TEMPLATE", "https://api.arvow.com/api/v0.1/batch/{id}"))
ARVOW_API_KEY = clean_text(os.getenv("ARVOW_API_KEY", ""))


def configure_logging(debug: bool = False) -> None:
    if debug:
        logger.setLevel(logging.DEBUG)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def auto_publish_sites() -> list[dict[str, Any]]:
    return [site for site in WEBSITES if bool(site.get("auto_publish_enabled"))]


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


def opportunity_rows(client, site_name: str) -> list[dict[str, Any]]:
    since = datetime.now(timezone.utc) - timedelta(days=VERIFY_LOOKBACK_DAYS)
    try:
        rows = (
            client.table("daily_content_opportunities")
            .select("*")
            .eq("site", site_name)
            .in_("status", list(VERIFYABLE_STATUSES))
            .order("updated_at", desc=False)
            .limit(50)
            .execute()
            .data
            or []
        )
        filtered: list[dict[str, Any]] = []
        for row in rows:
            reference = clean_text(row.get("sent_to_arvow_at")) or clean_text(row.get("updated_at"))
            if not reference:
                filtered.append(row)
                continue
            try:
                reference_dt = datetime.fromisoformat(reference.replace("Z", "+00:00"))
            except ValueError:
                filtered.append(row)
                continue
            if reference_dt >= since:
                filtered.append(row)
        return filtered
    except APIError as exc:
        if "sent_to_arvow_at" in str(exc):
            raise RuntimeError(
                "The V3 automation migration has not been applied yet. Run "
                "data-consolidation-dashboard/database/upgrade_arvow_automation_v3.sql first."
            ) from exc
        raise


def latest_publish_history(client, opportunity_id: str) -> dict[str, Any] | None:
    rows = (
        client.table("arvow_publish_history")
        .select("*")
        .eq("opportunity_id", opportunity_id)
        .order("created_at", desc=True)
        .limit(10)
        .execute()
        .data
        or []
    )
    if not rows:
        return None
    for row in rows:
        payload = row.get("arvow_response")
        if payload:
            return row
    return rows[0]


def verification_attempts(client, opportunity_id: str) -> int:
    rows = (
        client.table("arvow_publish_history")
        .select("id, status")
        .eq("opportunity_id", opportunity_id)
        .in_("status", ["verification_pending", "verification_failed"])
        .execute()
        .data
        or []
    )
    attempts = 0
    for row in rows:
        message = clean_text(row.get("error_message")).lower()
        if message.startswith("verification skipped:"):
            continue
        attempts += 1
    return attempts


def response_body_from_history(history_row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not history_row:
        return None
    payload = history_row.get("arvow_response")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            return None
    if not isinstance(payload, dict):
        return None
    body = payload.get("response_body") or payload.get("responseBody")
    if isinstance(body, dict):
        return body
    return payload


def candidate_url_from_response(response: dict[str, Any] | None) -> str | None:
    if not response:
        return None

    def walk(value: Any) -> str | None:
        if isinstance(value, dict):
            for key in ("publishedUrl", "published_url", "url", "postUrl", "post_url", "targetUrl", "draftUrl", "draft_url", "previewUrl", "preview_url"):
                candidate = clean_url(value.get(key))
                if candidate:
                    return candidate
            for nested in value.values():
                candidate = walk(nested)
                if candidate:
                    return candidate
        elif isinstance(value, list):
            for nested in value:
                candidate = walk(nested)
                if candidate:
                    return candidate
        return None

    return walk(response)


def fetch_arvow_status(batch_id: str, *, debug: bool = False) -> tuple[int | None, dict[str, Any] | str | None]:
    if not batch_id or not ARVOW_STATUS_URL_TEMPLATE or not ARVOW_API_KEY:
        return None, None
    endpoint = ARVOW_STATUS_URL_TEMPLATE.replace("{id}", batch_id)
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {ARVOW_API_KEY}",
        "x-api-key": ARVOW_API_KEY,
    }
    try:
        response = requests.get(endpoint, headers=headers, timeout=45)
        parsed: dict[str, Any] | str | None
        try:
            parsed = response.json() if response.text else None
        except Exception:
            parsed = response.text
        if debug:
            logger.debug("Arvow status HTTP %s for %s: %s", response.status_code, batch_id, json.dumps(parsed, ensure_ascii=False) if isinstance(parsed, dict) else parsed)
        return response.status_code, parsed
    except Exception as exc:
        if debug:
            logger.debug("Arvow status poll failed for %s: %s", batch_id, exc)
        return None, {"poll_error": str(exc)}


def status_failure_message(status_payload: dict[str, Any] | str | None) -> str | None:
    if isinstance(status_payload, dict):
        text = " ".join(
            clean_text(status_payload.get(key))
            for key in ("status", "state", "error", "message", "detail")
            if clean_text(status_payload.get(key))
        ).lower()
        if any(token in text for token in ("failed", "error", "rejected", "cancelled", "canceled")):
            return clean_text(status_payload.get("error")) or clean_text(status_payload.get("message")) or clean_text(status_payload.get("detail")) or clean_text(status_payload.get("status")) or "Arvow reported a failure state."
    return None


def url_is_live(url: str) -> bool:
    try:
        response = requests.get(url, timeout=30, allow_redirects=True)
        return response.status_code == 200
    except Exception:
        return False


def parse_xml_locs(xml_text: str) -> list[str]:
    locs: list[str] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return locs
    for loc in root.findall(".//{*}loc"):
        value = clean_url(loc.text)
        if value:
            locs.append(value)
    return locs


def parse_feed_links(xml_text: str) -> list[str]:
    links: list[str] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return links
    for node in root.findall(".//item/link"):
        value = clean_url(node.text)
        if value:
            links.append(value)
    for node in root.findall(".//{*}entry/{*}link"):
        value = clean_url(node.attrib.get("href"))
        if value:
            links.append(value)
    return links


def sitemap_candidates(sitemap_url: str) -> list[str]:
    try:
        response = requests.get(sitemap_url, timeout=45)
        response.raise_for_status()
    except Exception:
        return []

    locs = parse_xml_locs(response.text)
    expanded: list[str] = []
    nested = [loc for loc in locs if loc.endswith(".xml")]
    direct_urls = [loc for loc in locs if not loc.endswith(".xml")]
    preferred_nested = sorted(
        nested,
        key=lambda value: (
            0
            if any(token in value for token in ("post-sitemap", "page-sitemap", "post-", "page-", "article", "news"))
            else 1,
            value,
        ),
    )

    for nested_url in preferred_nested[:15]:
        try:
            nested_response = requests.get(nested_url, timeout=45)
            nested_response.raise_for_status()
            expanded.extend([loc for loc in parse_xml_locs(nested_response.text) if not loc.endswith(".xml")])
        except Exception:
            continue

    return direct_urls + expanded


def match_published_url(opportunity: dict[str, Any], urls: list[str]) -> str | None:
    title = clean_text(opportunity.get("title"))
    keyword = clean_text(opportunity.get("primary_keyword"))
    for url in urls:
        signature = normalize_key(url.split("://", 1)[-1].replace("/", " ").replace("-", " "))
        if jaccard_similarity(signature, keyword) >= 0.8:
            return url
        if jaccard_similarity(signature, title) >= 0.62:
            return url
    return None


def wordpress_candidate(site_domain: str, keyword: str, title: str) -> str | None:
    base = f"https://{site_domain}"
    search_terms = [keyword, title]
    endpoints = ("posts", "pages")
    for endpoint in endpoints:
        for term in search_terms:
            query = quote(term[:60])
            url = f"{base}/wp-json/wp/v2/{endpoint}?search={query}&per_page=5&_fields=link,title,slug,status,date"
            try:
                response = requests.get(url, timeout=30)
                if response.status_code != 200:
                    continue
                rows = response.json()
                for row in rows or []:
                    link = clean_url(row.get("link"))
                    rendered_title = clean_text((row.get("title") or {}).get("rendered"))
                    signature = normalize_key((link or "").replace("https://", " ").replace("http://", " ").replace("/", " ").replace("-", " "))
                    if link and (
                        jaccard_similarity(signature, keyword) >= 0.72
                        or jaccard_similarity(signature, title) >= 0.62
                        or jaccard_similarity(rendered_title, title) >= 0.72
                        or jaccard_similarity(rendered_title, keyword) >= 0.55
                    ):
                        return link
            except Exception:
                continue
    return None


def rss_candidate(site_domain: str, keyword: str, title: str) -> str | None:
    feed_url = f"https://{site_domain}/feed"
    try:
        response = requests.get(feed_url, timeout=30)
        response.raise_for_status()
    except Exception:
        return None

    for url in parse_feed_links(response.text):
        signature = normalize_key(url.replace("https://", " ").replace("http://", " ").replace("/", " ").replace("-", " "))
        if jaccard_similarity(signature, keyword) >= 0.72 or jaccard_similarity(signature, title) >= 0.62:
            return url
    return None


def sync_cluster_coverage(client, opportunity: dict[str, Any], published_url: str) -> bool:
    rows = (
        client.table("daily_insights")
        .select("id, content_plan")
        .order("date", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not rows:
        return False

    row = rows[0]
    content_plan = row.get("content_plan") or {"sites": []}
    if isinstance(content_plan, str):
        content_plan = json.loads(content_plan)
    if not isinstance(content_plan, dict):
        return False

    updated = False
    site_name = clean_text(opportunity.get("site"))
    title = clean_text(opportunity.get("title"))
    keyword = clean_text(opportunity.get("primary_keyword"))
    cluster_id = normalize_key(clean_text(opportunity.get("cluster_id")))
    covered_at = now_iso()

    for site in content_plan.get("sites", []):
        if clean_text(site.get("website")) != site_name:
            continue
        for cluster in site.get("clusters", []) or []:
            primary = cluster.get("primary_keyword") or {}
            cluster_key = normalize_key(clean_text(cluster.get("cluster_id"))) or normalize_key(
                clean_text(cluster.get("cluster_topic")) or clean_text(cluster.get("hub_article_title"))
            )
            if cluster_id and cluster_key == cluster_id:
                pass
            elif normalize_key(clean_text(primary.get("keyword"))) == normalize_key(keyword):
                pass
            elif jaccard_similarity(clean_text(cluster.get("hub_article_title")), title) >= 0.72:
                pass
            else:
                continue

            cluster["status"] = "covered"
            cluster["covered_at"] = covered_at
            cluster["covered_by_opportunity_id"] = opportunity.get("id")
            cluster["published_url"] = published_url
            updated = True

    if updated:
        client.table("daily_insights").update({"content_plan": content_plan}).eq("id", row["id"]).execute()
    return updated


def verify_one(
    client,
    site_name: str,
    row: dict[str, Any],
    *,
    dry_run: bool = False,
    force_verify: bool = False,
    debug: bool = False,
) -> str:
    status = clean_text(row.get("status")).lower()
    title = clean_text(row.get("title"))
    keyword = clean_text(row.get("primary_keyword"))
    sent_at_raw = clean_text(row.get("sent_to_arvow_at"))
    sent_at = None
    if sent_at_raw:
        try:
            sent_at = datetime.fromisoformat(sent_at_raw.replace("Z", "+00:00"))
        except ValueError:
            sent_at = None
    if sent_at is None:
        fallback = clean_text(row.get("updated_at")) or clean_text(row.get("created_at"))
        try:
            sent_at = datetime.fromisoformat(fallback.replace("Z", "+00:00")) if fallback else None
        except ValueError:
            sent_at = None

    current_attempts = verification_attempts(client, row["id"]) if not dry_run else 0
    required_age = VERIFY_ATTEMPT_WINDOWS[min(current_attempts, len(VERIFY_ATTEMPT_WINDOWS) - 1)] if VERIFY_ATTEMPT_WINDOWS else MIN_VERIFY_DELAY_MINUTES

    if sent_at is not None and not force_verify:
        age_minutes = (datetime.now(timezone.utc) - sent_at).total_seconds() / 60
        if age_minutes < required_age:
            note = (
                f"Verification deferred: only {age_minutes:.0f} minute(s) since dispatch. "
                f"Next attempt starts after {required_age} minutes."
            )
            if not dry_run:
                update_opportunity(
                    client,
                    row["id"],
                    status="verification_pending",
                    verification_notes=note,
                )
            logger.info("[%s] delaying verification for '%s' (%0.f min old)", site_name, title, age_minutes)
            return "pending"

    if status == "sent_to_arvow" and not dry_run:
        update_opportunity(
            client,
            row["id"],
            status="verification_pending",
            verification_notes="Verification in progress after automated Arvow dispatch.",
        )

    history_row = latest_publish_history(client, row["id"])
    response = response_body_from_history(history_row)

    site_config = build_site_arvow_config(site_name)
    batch_id = clean_text(row.get("arvow_batch_id")) or None
    job_id = clean_text(row.get("arvow_job_id")) or None

    status_code, status_payload = fetch_arvow_status(batch_id or "", debug=debug)
    status_failure = status_failure_message(status_payload)
    if status_failure:
        verified_at = now_iso()
        if not dry_run:
            update_opportunity(
                client,
                row["id"],
                status="verification_failed",
                verified_at=verified_at,
                verification_notes=f"Arvow status endpoint reported a failure: {status_failure}",
            )
            insert_publish_history(
                client,
                site=site_name,
                opportunity_id=row["id"],
                arvow_batch_id=batch_id or job_id,
                arvow_response={
                    "http_status": status_code,
                    "response_body": status_payload,
                },
                published_url=None,
                status="verification_failed",
                error_message=status_failure,
                sent_at=sent_at_raw or None,
                verified_at=verified_at,
            )
        logger.warning("[%s] Arvow reported failure for '%s': %s", site_name, title, status_failure)
        return "failed"

    candidate_url = candidate_url_from_response(response if isinstance(response, dict) else None)
    if not candidate_url and isinstance(status_payload, dict):
        candidate_url = candidate_url_from_response(status_payload)
    published_url = None

    if candidate_url and url_is_live(candidate_url):
        published_url = candidate_url
    else:
        for sitemap_url in site_config.get("sitemaps") or []:
            sitemap_url = clean_text(sitemap_url)
            if not sitemap_url:
                continue
            sitemap_urls = sitemap_candidates(sitemap_url)
            published_url = match_published_url(row, sitemap_urls)
            if published_url:
                break
        if not published_url:
            published_url = wordpress_candidate(site_config["domain"], keyword, title)
        if not published_url:
            published_url = rss_candidate(site_config["domain"], keyword, title)

    if published_url:
        verified_at = now_iso()
        if not dry_run:
            update_opportunity(
                client,
                row["id"],
                status="published",
                published_url=published_url,
                verified_at=verified_at,
                verification_notes="Verified live via URL/sitemap/WordPress feed check.",
            )
            insert_publish_history(
                client,
                site=site_name,
                opportunity_id=row["id"],
                arvow_batch_id=batch_id or job_id,
                arvow_response={
                    "http_status": status_code,
                    "response_body": status_payload if isinstance(status_payload, dict) else response if isinstance(response, dict) else None,
                },
                published_url=published_url,
                status="published",
                sent_at=clean_text(row.get("sent_to_arvow_at")) or None,
                verified_at=verified_at,
            )
            sync_cluster_coverage(client, row, published_url)
        logger.info("[%s] verified publish for '%s' -> %s", site_name, title, published_url)
        return "published"

    attempt_number = current_attempts + 1
    should_fail = attempt_number >= MAX_VERIFICATION_ATTEMPTS
    failure_note = (
        f"Verification attempt {attempt_number}/{MAX_VERIFICATION_ATTEMPTS} did not find a live URL yet. "
        "Checked Arvow status (if configured), direct URL response, sitemap, WordPress REST, and RSS feed."
    )
    if not dry_run:
        update_opportunity(
            client,
            row["id"],
            status="verification_failed" if should_fail else "verification_pending",
            verified_at=now_iso() if should_fail else None,
            verification_notes=failure_note,
        )
        insert_publish_history(
            client,
            site=site_name,
            opportunity_id=row["id"],
            arvow_batch_id=batch_id or job_id,
            arvow_response={
                "http_status": status_code,
                "response_body": status_payload if isinstance(status_payload, dict) else response if isinstance(response, dict) else None,
            },
            published_url=None,
            status="verification_failed" if should_fail else "verification_pending",
            error_message=failure_note,
            sent_at=clean_text(row.get("sent_to_arvow_at")) or None,
            verified_at=now_iso() if should_fail else None,
        )
    if should_fail:
        logger.warning("[%s] verification failed for '%s' after %d attempts", site_name, title, attempt_number)
        return "failed"
    else:
        logger.info("[%s] verification pending for '%s' after attempt %d", site_name, title, attempt_number)
        return "pending"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", help="Limit verification to one site")
    parser.add_argument("--dry-run", action="store_true", help="Run verification without writing updates")
    parser.add_argument("--force-verify", action="store_true", help="Bypass minimum delay windows for testing")
    parser.add_argument("--debug", action="store_true", help="Log Arvow status polling and verifier details")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(args.debug)
    require_env("SUPABASE_URL", SUPABASE_URL)
    require_env("SUPABASE_SERVICE_KEY", SUPABASE_KEY)

    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    sites = auto_publish_sites()
    if args.site:
        sites = [site for site in sites if site["name"] == args.site]
        if not sites:
            raise RuntimeError(f"Unknown or non-auto site: {args.site}")

    verified = 0
    pending = 0
    failed = 0
    for site in sites:
        site_name = site["name"]
        rows = opportunity_rows(client, site_name)
        if not rows:
            logger.info("[%s] nothing pending verification", site_name)
            continue
        for row in rows:
            result = verify_one(
                client,
                site_name,
                row,
                dry_run=args.dry_run,
                force_verify=args.force_verify,
                debug=args.debug,
            )
            if result == "published":
                verified += 1
            elif result == "failed":
                failed += 1
            else:
                pending += 1

    logger.info("Verification complete. published=%d pending=%d failed=%d", verified, pending, failed)


if __name__ == "__main__":
    main()
