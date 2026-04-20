"""
generate_daily_content_opportunities.py

Daily pipeline step that creates a clean, deduplicated content queue per site and stores it in
daily_content_opportunities for the Arvow dashboard.

Usage:
    python scripts/generate_daily_content_opportunities.py
    python scripts/generate_daily_content_opportunities.py --dry-run
    python scripts/generate_daily_content_opportunities.py --use-sample-data --dry-run --site FreedomX
"""

from __future__ import annotations

import argparse
import html
import json
import logging
import os
import re
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

try:
    from openai import OpenAI
    from supabase import create_client
except ImportError as exc:  # pragma: no cover
    raise RuntimeError(f"Missing dependency: {exc}") from exc

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from config import WEBSITES  # noqa: E402
from daily_content_prompt_config import (  # noqa: E402
    ARVOW_ENRICHMENT_PROMPT,
    CONTENT_GENERATION_PROMPT,
    SITE_ANALYSIS_PROMPT,
    VALIDATION_PROMPT,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("generate_daily_content_opportunities")

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("supabase").setLevel(logging.WARNING)


SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

ACTIVE_QUEUE_TARGET = 6
RECENT_DEDUPE_DAYS = 7
MIN_SIGNIFICANT_SCORE_DELTA = 12
OPENAI_MODEL = "gpt-4o-mini"

CONTROL_CHAR_RE = re.compile(r"[\u0000-\u001F\u007F-\u009F\uFFFD]")
RAW_URL_RE = re.compile(r"(https?://|www\.)", re.IGNORECASE)
HTML_ENTITY_RE = re.compile(r"&(?:[a-zA-Z]+|#\d+|#x[0-9a-fA-F]+);")
ALLOWED_KEYWORD_RE = re.compile(r"^[A-Za-z0-9\s&'\".,:;!?()/+\-_%#@$\[\]]+$")
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
SITE_DOMAINS = {
    "CitiesABC": "citiesabc.com",
    "BusinessABC": "businessabc.net",
    "HedgeThink": "hedgethink.com",
    "FashionABC": "fashionabc.org",
    "TradersDNA": "tradersdna.com",
    "FreedomX": "freedomx.com",
    "Wisdomia": "wisdomia.ai",
    "SportsDNA": "sportsdna.ai",
    "IntelligentHQ": "intelligenthq.com",
}


def require_env(name: str, value: str) -> str:
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", CONTROL_CHAR_RE.sub(" ", html.unescape(str(value or "")))).strip()


def clean_url(value: Any) -> str:
    raw = clean_text(value)
    if not raw:
        return ""
    raw = raw if raw.startswith(("http://", "https://")) else f"https://{raw}"
    try:
        from urllib.parse import urlparse, urlunparse

        parsed = urlparse(raw)
        path = parsed.path.rstrip("/") if parsed.path not in {"", "/"} else "/"
        return urlunparse(
            (
                parsed.scheme.lower(),
                parsed.netloc.lower(),
                path,
                "",
                parsed.query,
                "",
            )
        )
    except Exception:
        return raw


def is_noise_keyword(keyword: str) -> bool:
    text = clean_text(keyword)
    if not text:
        return True

    compact = re.sub(r"\s+", "", text)
    if len(compact) < 3:
        return True
    if RAW_URL_RE.search(text):
        return True
    if HTML_ENTITY_RE.search(text):
        return True
    if not ALLOWED_KEYWORD_RE.match(text):
        return True

    letters = len(re.findall(r"[A-Za-z]", text))
    if compact and (letters / len(compact)) < 0.5:
        return True

    punctuation = len(re.findall(r"[^A-Za-z0-9\s]", text))
    if compact and (punctuation / len(compact)) > 0.35:
        return True

    if re.search(r"([A-Za-z])\1{3,}", text):
        return True

    return False


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", clean_text(value).lower()).strip()


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", normalize_key(value)).strip("-")


def token_set(value: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9]+", normalize_key(value)) if token not in STOP_WORDS}


def jaccard_similarity(left: str, right: str) -> float:
    left_tokens = token_set(left)
    right_tokens = token_set(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens.intersection(right_tokens)) / len(left_tokens.union(right_tokens))


def priority_label(score: int) -> str:
    if score >= 72:
        return "high"
    if score >= 48:
        return "medium"
    return "low"


def today_iso() -> str:
    return date.today().isoformat()


def get_site_profile(site_name: str) -> dict[str, str]:
    for site in WEBSITES:
        if site["name"] == site_name:
            return {
                "name": site["name"],
                "slug": site["slug"],
                "category": site["category"],
                "audience": site["audience"],
                "domain": SITE_DOMAINS.get(site["name"], ""),
            }
    return {
        "name": site_name,
        "slug": slugify(site_name),
        "category": "",
        "audience": "",
        "domain": SITE_DOMAINS.get(site_name, ""),
    }


def build_site_arvow_config(site_name: str) -> dict[str, Any]:
    profile = get_site_profile(site_name)
    integration_id = os.getenv(f"ARVOW_INTEGRATION_ID_{profile['slug'].upper()}", "")
    domain = profile["domain"]
    external_sources = [
        f"https://{candidate['domain']}/"
        for candidate in (get_site_profile(site["name"]) for site in WEBSITES)
        if candidate["name"] != site_name and candidate["domain"]
    ]
    return {
        "site": site_name,
        "slug": profile["slug"],
        "domain": domain,
        "integration_id": integration_id or None,
        "sitemaps": [f"https://{domain}/api/sitemaps/index.xml"] if domain else [],
        "external_sources": external_sources,
        "youtube_links": ["https://www.youtube.com/@DinisGuarda"],
    }


def fallback_openai_json(system_prompt: str, payload: dict[str, Any]) -> dict[str, Any]:
    site_payload = payload.get("site_payload") or payload
    candidates = site_payload.get("prioritized_candidates") or []
    insights = site_payload.get("insights") or []
    support_signals = site_payload.get("support_signals") or {}

    if system_prompt == SITE_ANALYSIS_PROMPT:
        themes = []
        for candidate in candidates[:4]:
            themes.append(
                {
                    "theme": candidate.get("cluster_topic") or candidate.get("primary_keyword"),
                    "primary_keyword": candidate.get("primary_keyword"),
                    "priority_score": int(candidate.get("priority_score") or 0),
                    "why_now": candidate.get("strategy")
                    or f"{site_payload.get('site')} has fresh demand around this theme.",
                }
            )
        return {
            "site": site_payload.get("site"),
            "themes": themes,
            "signals": {
                "internal_link_count": support_signals.get("internal_link_count", 0),
                "broken_backlink_count": len(support_signals.get("broken_backlinks", [])),
                "lost_backlink_count": len(support_signals.get("lost_backlinks", [])),
            },
        }

    if system_prompt == CONTENT_GENERATION_PROMPT:
        opportunities = []
        for candidate in candidates[:ACTIVE_QUEUE_TARGET]:
            opportunities.append(
                {
                    "title": clean_text(candidate.get("title_hint"))
                    or clean_text(candidate.get("primary_keyword")).title(),
                    "primary_keyword": clean_text(candidate.get("primary_keyword")),
                    "cluster_id": clean_text(candidate.get("cluster_id")) or None,
                    "reasoning": clean_text(candidate.get("strategy"))
                    or f"High-priority theme for {site_payload.get('site')} based on the latest content gap and cluster inputs.",
                    "priority_score": max(1, min(100, int(candidate.get("priority_score") or 0))),
                    "intent": clean_text(candidate.get("intent")) or "informational",
                }
            )
        return {"opportunities": opportunities}

    if system_prompt == VALIDATION_PROMPT:
        approved = []
        seen_keys: set[str] = set()
        generated = (payload.get("generated") or {}).get("opportunities") or []
        for item in generated:
            key = f"{normalize_key(item.get('title', ''))}::{normalize_key(item.get('primary_keyword', ''))}"
            if key in seen_keys:
                continue
            seen_keys.add(key)
            approved.append(item)
            if len(approved) >= int(payload.get("max_keep") or ACTIVE_QUEUE_TARGET):
                break
        return {"approved": approved}

    if system_prompt == ARVOW_ENRICHMENT_PROMPT:
        entries = []
        for item in payload.get("approved_items") or []:
            entries.append(
                {
                    "title": clean_text(item.get("title")),
                    "primary_keyword": clean_text(item.get("primary_keyword")),
                    "cluster_id": clean_text(item.get("cluster_id")) or None,
                    "reasoning": clean_text(item.get("reasoning")),
                    "priority_score": int(item.get("priority_score") or 0),
                    "intent": item.get("intent") or "informational",
                    "content_brief": f"Create a clear, authoritative article around {clean_text(item.get('primary_keyword'))} tailored to {site_payload.get('profile', {}).get('audience', 'the site audience')}.",
                    "internal_linking_notes": [
                        f"Reference current {site_payload.get('site')} topic pages that overlap with {clean_text(item.get('primary_keyword'))}."
                    ],
                    "supporting_insights": [
                        clean_text((insights[0] or {}).get("analysis")) if insights else "",
                    ],
                }
            )
        return {"entries": entries}

    return {}


def openai_json(client: OpenAI | None, system_prompt: str, payload: dict[str, Any]) -> dict[str, Any]:
    if client is None:
        return fallback_openai_json(system_prompt, payload)
    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        temperature=0.2,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ],
    )
    content = response.choices[0].message.content or "{}"
    return json.loads(content)


def safe_query(client, table: str, select: str, *, filters: list[tuple[str, tuple[Any, ...]]] | None = None, order: tuple[str, bool] | None = None, limit: int = 5000):
    query = client.table(table).select(select)
    if filters:
        for method, args in filters:
            query = getattr(query, method)(*args)
    if order:
        query = query.order(order[0], desc=order[1])
    response = query.limit(limit).execute()
    return response.data or []


def fetch_latest_content_plan(client) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = (
        client.table("daily_insights")
        .select("date, insights, content_plan")
        .order("date", desc=True)
        .limit(1)
        .execute()
        .data
        or []
    )
    if not rows:
        return [], []

    row = rows[0]
    insights = row.get("insights") or []
    if isinstance(insights, str):
        insights = json.loads(insights)
    content_plan = row.get("content_plan") or {"sites": []}
    if isinstance(content_plan, str):
        content_plan = json.loads(content_plan)
    return insights if isinstance(insights, list) else [], content_plan.get("sites", []) if isinstance(content_plan, dict) else []


def latest_date_by_site(rows: list[dict[str, Any]], site_field: str, date_field: str) -> dict[str, str]:
    latest: dict[str, str] = {}
    for row in rows:
        site = clean_text(row.get(site_field))
        snapshot = clean_text(row.get(date_field))
        if not site or not snapshot:
            continue
        if site not in latest or snapshot > latest[site]:
            latest[site] = snapshot
    return latest


def load_live_dataset(client) -> dict[str, Any]:
    insights, content_plan_sites = fetch_latest_content_plan(client)
    content_gap_rows = safe_query(
        client,
        "content_gap_keywords",
        "id, date, website, keyword, volume, kd, opportunity_score, intent, cluster, is_easy_win, competitors",
        order=("date", True),
        limit=7000,
    )
    internal_link_rows = safe_query(
        client,
        "internal_linking_suggestions",
        "id, website, source_website, target_website, source_page, target_page, target_page_keyword, score, ai_confidence, reason, status",
        order=("created_at", True),
        limit=5000,
    )
    broken_rows = safe_query(
        client,
        "ahrefs_broken_backlinks",
        "id, date, website, referring_page, target_url, ref_domain_dr, validation_status, validation_notes, anchor_text, target_http_code, http_code",
        order=("date", True),
        limit=5000,
    )
    lost_rows = safe_query(
        client,
        "ahrefs_lost_backlinks",
        "id, website, referring_page_url, target_url, domain_rating, validation_status, validation_notes, anchor, lost_date, drop_reason",
        order=("lost_date", True),
        limit=5000,
    )
    metric_rows = safe_query(
        client,
        "daily_metrics",
        "date, website, gsc_clicks, gsc_impressions, gsc_ctr, gsc_position, ga_sessions, ga_organic_sessions, ga_bounce_rate",
        filters=[("gte", ("date", (date.today() - timedelta(days=21)).isoformat()))],
        order=("date", True),
        limit=2000,
    )
    active_rows = safe_query(
        client,
        "daily_content_opportunities",
        "*",
        order=("updated_at", True),
        limit=2000,
    )
    history_rows = safe_query(
        client,
        "content_generation_history",
        "*",
        filters=[("gte", ("generated_date", (date.today() - timedelta(days=RECENT_DEDUPE_DAYS)).isoformat()))],
        order=("completed_at", True),
        limit=2000,
    )

    return {
        "insights": insights,
        "content_plan_sites": content_plan_sites,
        "content_gap_rows": content_gap_rows,
        "internal_link_rows": internal_link_rows,
        "broken_rows": broken_rows,
        "lost_rows": lost_rows,
        "metric_rows": metric_rows,
        "active_rows": active_rows,
        "history_rows": history_rows,
    }


def load_sample_dataset() -> dict[str, Any]:
    return {
        "insights": [
            {
                "category": "content_gap",
                "severity": "high",
                "title": "DeFi explainers continue to outperform broad crypto news",
                "analysis": "Users are finding structured educational content more often than generic market commentary.",
                "action": "Expand editorial depth around DeFi and exchange explainers.",
                "impact": "Improves non-brand search growth for crypto topics.",
                "related_website": "FreedomX",
            }
        ],
        "content_plan_sites": [
            {
                "website": "FreedomX",
                "clusters": [
                    {
                        "cluster_topic": "Decentralized Finance",
                        "hub_article_title": "DeFi Explained for New Crypto Investors",
                        "primary_keyword": {"keyword": "defi explained", "volume": 2400, "kd": 6, "intent": "informational"},
                        "related_keywords": [
                            {"keyword": "what is defi", "volume": 6600, "kd": 8, "intent": "informational"},
                            {"keyword": "defi platforms", "volume": 1800, "kd": 9, "intent": "commercial"},
                        ],
                        "total_cluster_volume": 10800,
                        "estimated_traffic": 1600,
                        "strategy": "Own beginner-friendly DeFi education before users compare platforms.",
                    }
                ],
            }
        ],
        "content_gap_rows": [
            {
                "id": str(uuid.uuid4()),
                "date": today_iso(),
                "website": "FreedomX",
                "keyword": "defi explained",
                "volume": 2400,
                "kd": 6,
                "opportunity_score": 78,
                "intent": "informational",
                "cluster": "Decentralized Finance",
                "is_easy_win": False,
                "competitors": [],
            },
            {
                "id": str(uuid.uuid4()),
                "date": today_iso(),
                "website": "FreedomX",
                "keyword": "best crypto wallet for beginners",
                "volume": 3200,
                "kd": 7,
                "opportunity_score": 82,
                "intent": "commercial",
                "cluster": "Crypto Wallets",
                "is_easy_win": True,
                "competitors": [],
            },
        ],
        "internal_link_rows": [
            {
                "id": str(uuid.uuid4()),
                "website": "FreedomX",
                "source_website": "FreedomX",
                "target_website": "FreedomX",
                "source_page": "https://freedomx.com/blog/bitcoin-basics",
                "target_page": "https://freedomx.com/blog/defi-platforms",
                "target_page_keyword": "defi platforms",
                "score": 88,
                "ai_confidence": 83,
                "reason": "Readers learning bitcoin basics also look for the next step into DeFi platforms.",
                "status": "pending",
            }
        ],
        "broken_rows": [],
        "lost_rows": [],
        "metric_rows": [
            {
                "date": today_iso(),
                "website": "FreedomX",
                "gsc_clicks": 420,
                "gsc_impressions": 9800,
                "gsc_ctr": 4.2,
                "gsc_position": 18.4,
                "ga_sessions": 1080,
                "ga_organic_sessions": 760,
                "ga_bounce_rate": 41.2,
            }
        ],
        "active_rows": [],
        "history_rows": [],
    }


def build_site_datasets(dataset: dict[str, Any], *, site_filter: str | None = None) -> list[dict[str, Any]]:
    content_gap_rows = dataset["content_gap_rows"]
    latest_gap_dates = latest_date_by_site(content_gap_rows, "website", "date")
    broken_rows = dataset["broken_rows"]
    latest_broken_dates = latest_date_by_site(broken_rows, "website", "date")
    content_plan_by_site = {
        clean_text(site.get("website")): site.get("clusters", [])
        for site in dataset["content_plan_sites"]
        if clean_text(site.get("website"))
    }

    all_sites = [site["name"] for site in WEBSITES]
    if site_filter:
        all_sites = [site for site in all_sites if site == site_filter]

    sites: list[dict[str, Any]] = []
    for site_name in all_sites:
        profile = get_site_profile(site_name)
        keywords = []
        for row in content_gap_rows:
            if clean_text(row.get("website")) != site_name:
                continue
            if clean_text(row.get("date")) != latest_gap_dates.get(site_name):
                continue
            keyword = clean_text(row.get("keyword"))
            if is_noise_keyword(keyword):
                continue
            keywords.append(
                {
                    "id": clean_text(row.get("id")),
                    "keyword": keyword,
                    "volume": int(row.get("volume") or 0),
                    "kd": int(float(row.get("kd") or 0)),
                    "opportunity_score": int(float(row.get("opportunity_score") or 0)),
                    "intent": clean_text(row.get("intent")) or "informational",
                    "cluster": clean_text(row.get("cluster")) or None,
                    "is_easy_win": bool(row.get("is_easy_win")),
                }
            )
        keywords.sort(key=lambda row: (row["opportunity_score"], row["volume"]), reverse=True)

        clusters = []
        for cluster in content_plan_by_site.get(site_name, []):
            primary = cluster.get("primary_keyword") or {}
            primary_keyword = clean_text(primary.get("keyword"))
            if primary_keyword and is_noise_keyword(primary_keyword):
                continue
            related = []
            for item in cluster.get("related_keywords", []) or []:
                related_keyword = clean_text(item.get("keyword"))
                if not related_keyword or is_noise_keyword(related_keyword):
                    continue
                related.append(
                    {
                        "keyword": related_keyword,
                        "volume": int(item.get("volume") or 0),
                        "kd": int(float(item.get("kd") or 0)),
                        "intent": clean_text(item.get("intent")) or "informational",
                    }
                )
            clusters.append(
                {
                    "cluster_id": slugify(clean_text(cluster.get("cluster_topic")) or clean_text(cluster.get("hub_article_title"))),
                    "cluster_topic": clean_text(cluster.get("cluster_topic")) or "Content Opportunity",
                    "hub_article_title": clean_text(cluster.get("hub_article_title")) or primary_keyword,
                    "strategy": clean_text(cluster.get("strategy")),
                    "primary_keyword": {
                        "keyword": primary_keyword,
                        "volume": int(primary.get("volume") or 0),
                        "kd": int(float(primary.get("kd") or 0)),
                        "intent": clean_text(primary.get("intent")) or "informational",
                    }
                    if primary_keyword
                    else None,
                    "related_keywords": related,
                    "total_cluster_volume": int(cluster.get("total_cluster_volume") or 0),
                    "estimated_traffic": int(cluster.get("estimated_traffic") or 0),
                }
            )

        internal_links = [
            {
                "source_page": clean_url(row.get("source_page")),
                "target_page": clean_url(row.get("target_page")),
                "target_page_keyword": clean_text(row.get("target_page_keyword")),
                "score": int(row.get("score") or 0),
                "ai_confidence": int(row.get("ai_confidence") or 0),
                "reason": clean_text(row.get("reason")),
            }
            for row in dataset["internal_link_rows"]
            if clean_text(row.get("source_website") or row.get("website")) == site_name
        ]
        internal_links.sort(key=lambda row: (row["score"], row["ai_confidence"]), reverse=True)

        broken = [
            {
                "referring_page": clean_url(row.get("referring_page")),
                "target_url": clean_url(row.get("target_url")),
                "domain_rating": int(row.get("ref_domain_dr") or 0),
                "status_code": int(row.get("target_http_code") or row.get("http_code") or 0) or None,
                "validation_status": clean_text(row.get("validation_status")),
                "validation_notes": clean_text(row.get("validation_notes")),
            }
            for row in broken_rows
            if clean_text(row.get("website")) == site_name
            and clean_text(row.get("date")) == latest_broken_dates.get(site_name)
        ]
        lost = [
            {
                "referring_page": clean_url(row.get("referring_page_url")),
                "target_url": clean_url(row.get("target_url")),
                "domain_rating": int(row.get("domain_rating") or 0),
                "lost_date": clean_text(row.get("lost_date")),
                "drop_reason": clean_text(row.get("drop_reason")),
                "validation_status": clean_text(row.get("validation_status")),
                "validation_notes": clean_text(row.get("validation_notes")),
            }
            for row in dataset["lost_rows"]
            if clean_text(row.get("website")) == site_name
        ]

        metrics = [
            row
            for row in dataset["metric_rows"]
            if clean_text(row.get("website")) == site_name
        ]
        metrics.sort(key=lambda row: clean_text(row.get("date")), reverse=True)

        site_insights = [
            insight
            for insight in dataset["insights"]
            if clean_text(insight.get("related_website")) in {"", "all", site_name}
        ]

        active_rows = [
            row
            for row in dataset["active_rows"]
            if clean_text(row.get("site")) == site_name
        ]
        history_rows = [
            row
            for row in dataset["history_rows"]
            if clean_text(row.get("site")) == site_name
        ]

        sites.append(
            {
                "site": site_name,
                "profile": profile,
                "arvow": build_site_arvow_config(site_name),
                "keywords": keywords,
                "clusters": clusters,
                "internal_links": internal_links,
                "broken_backlinks": broken,
                "lost_backlinks": lost,
                "metrics": metrics,
                "insights": site_insights,
                "active_rows": active_rows,
                "history_rows": history_rows,
            }
        )

    return sites


def summarize_performance(site_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not site_rows:
        return {
            "latest_clicks": 0,
            "latest_sessions": 0,
            "latest_position": None,
            "seven_day_clicks": 0,
            "seven_day_sessions": 0,
            "momentum": "unknown",
        }

    latest = site_rows[0]
    recent = site_rows[:7]
    previous = site_rows[7:14]
    recent_clicks = sum(int(row.get("gsc_clicks") or 0) for row in recent)
    previous_clicks = sum(int(row.get("gsc_clicks") or 0) for row in previous)
    recent_sessions = sum(int(row.get("ga_sessions") or 0) for row in recent)
    previous_sessions = sum(int(row.get("ga_sessions") or 0) for row in previous)
    momentum = "flat"
    if recent_clicks > previous_clicks * 1.08 or recent_sessions > previous_sessions * 1.08:
        momentum = "up"
    elif previous_clicks > 0 and recent_clicks < previous_clicks * 0.92:
        momentum = "down"
    return {
        "latest_clicks": int(latest.get("gsc_clicks") or 0),
        "latest_sessions": int(latest.get("ga_sessions") or 0),
        "latest_position": latest.get("gsc_position"),
        "seven_day_clicks": recent_clicks,
        "seven_day_sessions": recent_sessions,
        "momentum": momentum,
    }


def score_keyword_candidate(keyword: dict[str, Any]) -> int:
    volume = int(keyword.get("volume") or 0)
    kd = int(keyword.get("kd") or 0)
    opportunity = int(keyword.get("opportunity_score") or 0)
    easy_win = 12 if keyword.get("is_easy_win") else 0
    score = min(volume / 90, 34) + max(0, 24 - kd * 2) + min(opportunity / 3, 28) + easy_win
    return max(0, min(100, round(score)))


def score_cluster_candidate(cluster: dict[str, Any]) -> int:
    primary = cluster.get("primary_keyword") or {}
    volume = int(cluster.get("total_cluster_volume") or 0)
    kd = int(primary.get("kd") or 0)
    est = int(cluster.get("estimated_traffic") or 0)
    score = min(volume / 80, 38) + max(0, 22 - kd * 2.2) + min(est / 45, 22) + (10 if cluster.get("strategy") else 0)
    return max(0, min(100, round(score)))


def build_prioritized_candidates(site_data: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen_keywords: set[str] = set()

    for cluster in site_data["clusters"]:
        primary = cluster.get("primary_keyword") or {}
        primary_keyword = clean_text(primary.get("keyword"))
        if not primary_keyword:
            continue
        seen_keywords.add(normalize_key(primary_keyword))
        candidates.append(
            {
                "source": "cluster",
                "title_hint": clean_text(cluster.get("hub_article_title")) or primary_keyword.title(),
                "primary_keyword": primary_keyword,
                "cluster_id": cluster.get("cluster_id"),
                "cluster_topic": clean_text(cluster.get("cluster_topic")),
                "strategy": clean_text(cluster.get("strategy")),
                "priority_score": score_cluster_candidate(cluster),
                "intent": clean_text(primary.get("intent")) or "informational",
                "support_keywords": [item["keyword"] for item in cluster.get("related_keywords", [])[:4]],
            }
        )

    for keyword in site_data["keywords"]:
        keyword_key = normalize_key(keyword["keyword"])
        if keyword_key in seen_keywords:
            continue
        candidates.append(
            {
                "source": "keyword_gap",
                "title_hint": keyword["keyword"].title(),
                "primary_keyword": keyword["keyword"],
                "cluster_id": slugify(keyword.get("cluster") or keyword["keyword"]),
                "cluster_topic": keyword.get("cluster") or "Standalone opportunity",
                "strategy": "",
                "priority_score": score_keyword_candidate(keyword),
                "intent": keyword.get("intent") or "informational",
                "support_keywords": [],
            }
        )

    candidates.sort(key=lambda row: row["priority_score"], reverse=True)
    return candidates[:12]


def recent_duplicate(existing_rows: list[dict[str, Any]], title: str, keyword: str, score: int) -> bool:
    title_key = normalize_key(title)
    keyword_key = normalize_key(keyword)
    cutoff = date.today() - timedelta(days=RECENT_DEDUPE_DAYS)
    for row in existing_rows:
        generated_date = clean_text(row.get("generated_date"))
        if generated_date and generated_date < cutoff.isoformat():
            continue
        existing_title = clean_text(row.get("title"))
        existing_keyword = clean_text(row.get("primary_keyword"))
        existing_score = int(row.get("priority_score") or 0)
        if normalize_key(existing_keyword) == keyword_key and score <= existing_score + MIN_SIGNIFICANT_SCORE_DELTA:
            return True
        if jaccard_similarity(existing_title, title) >= 0.82 and score <= existing_score + MIN_SIGNIFICANT_SCORE_DELTA:
            return True
        if jaccard_similarity(existing_keyword, keyword) >= 0.9 and score <= existing_score + MIN_SIGNIFICANT_SCORE_DELTA:
            return True
        if title_key == normalize_key(existing_title) and score <= existing_score + MIN_SIGNIFICANT_SCORE_DELTA:
            return True
    return False


def build_site_payload(site_data: dict[str, Any]) -> dict[str, Any]:
    performance = summarize_performance(site_data["metrics"])
    support_signals = {
        "internal_link_count": len(site_data["internal_links"]),
        "top_internal_links": site_data["internal_links"][:3],
        "broken_backlinks": sorted(site_data["broken_backlinks"], key=lambda row: row["domain_rating"], reverse=True)[:3],
        "lost_backlinks": sorted(site_data["lost_backlinks"], key=lambda row: row["domain_rating"], reverse=True)[:3],
    }
    return {
        "site": site_data["site"],
        "profile": site_data["profile"],
        "performance": performance,
        "prioritized_candidates": build_prioritized_candidates(site_data),
        "existing_active_titles": [
            {
                "title": clean_text(row.get("title")),
                "primary_keyword": clean_text(row.get("primary_keyword")),
                "status": clean_text(row.get("status")),
            }
            for row in site_data["active_rows"]
        ],
        "recent_history_titles": [
            {
                "title": clean_text(row.get("title")),
                "primary_keyword": clean_text(row.get("primary_keyword")),
                "action_taken": clean_text(row.get("action_taken")),
            }
            for row in site_data["history_rows"]
        ],
        "insights": site_data["insights"][:4],
        "support_signals": support_signals,
    }


def analyze_site(client: OpenAI | None, site_payload: dict[str, Any]) -> dict[str, Any]:
    return openai_json(client, SITE_ANALYSIS_PROMPT, site_payload)


def generate_titles(client: OpenAI | None, site_payload: dict[str, Any], analysis: dict[str, Any]) -> dict[str, Any]:
    return openai_json(
        client,
        CONTENT_GENERATION_PROMPT,
        {
            "site_payload": site_payload,
            "analysis": analysis,
            "target_count": max(6, ACTIVE_QUEUE_TARGET),
        },
    )


def validate_titles(client: OpenAI | None, site_payload: dict[str, Any], analysis: dict[str, Any], generated: dict[str, Any]) -> dict[str, Any]:
    return openai_json(
        client,
        VALIDATION_PROMPT,
        {
            "site_payload": site_payload,
            "analysis": analysis,
            "generated": generated,
            "max_keep": ACTIVE_QUEUE_TARGET,
        },
    )


def enrich_for_arvow(client: OpenAI | None, site_payload: dict[str, Any], approved_items: list[dict[str, Any]]) -> dict[str, Any]:
    return openai_json(
        client,
        ARVOW_ENRICHMENT_PROMPT,
        {
            "site_payload": site_payload,
            "approved_items": approved_items,
        },
    )


def build_stored_arvow_payload(site_name: str, enriched: dict[str, Any]) -> dict[str, Any]:
    config = build_site_arvow_config(site_name)
    body = {
        "key": os.getenv("ARVOW_API_KEY") or None,
        "formula": {
            "generation": {
                "entries": [
                    {
                        "title": enriched["title"],
                        "keyword": enriched["primary_keyword"],
                    }
                ]
            },
            "content": {
                "languageCode": "en",
                "formality": "formal",
                "tone": "neutral, educative, easy to understand, formal, professional, engaging",
            },
            "knowledge": {"serp": True},
            "formatting": {
                "bold": True,
                "tables": True,
                "quotes": True,
                "lists": True,
                "headingCase": "title",
            },
            "structure": {
                "faq": True,
                "size": "md",
                "keyTakeaways": True,
                "conclusion": True,
            },
            "internalLinking": {
                "sitemaps": [{"url": url} for url in config["sitemaps"]],
            },
            "externalLinking": {
                "automateExternalLinks": True,
                "includeSources": config["external_sources"],
            },
            "images": {"inArticleImages": True, "featuredImage": True},
            "videos": {
                "youtubeLinks": config["youtube_links"],
                "automateYoutubeLinks": True,
            },
        },
        "quantity": 1,
        "integrationId": config["integration_id"],
    }
    return {
        "endpoint": "https://api.arvow.com/api/v0.1/batch",
        "method": "POST",
        "body": body,
        "metadata": {
            "site": site_name,
            "cluster_id": enriched.get("cluster_id"),
            "reasoning": enriched.get("reasoning"),
            "intent": enriched.get("intent"),
            "content_brief": enriched.get("content_brief"),
            "internal_linking_notes": enriched.get("internal_linking_notes", []),
            "supporting_insights": enriched.get("supporting_insights", []),
            "priority_score": enriched.get("priority_score"),
            "priority_label": priority_label(int(enriched.get("priority_score") or 0)),
        },
    }


def make_row(site_name: str, item: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "id": str(uuid.uuid4()),
        "site": site_name,
        "title": clean_text(item["title"]),
        "primary_keyword": clean_text(item["primary_keyword"]),
        "cluster_id": clean_text(item.get("cluster_id")) or None,
        "reasoning": clean_text(item.get("reasoning")),
        "priority_score": int(item.get("priority_score") or 0),
        "status": "pending",
        "arvow_payload": item.get("arvow_payload"),
        "created_at": now,
        "updated_at": now,
        "generated_date": today_iso(),
    }


def save_rows(client, rows: list[dict[str, Any]], dry_run: bool) -> int:
    if not rows:
        return 0
    if dry_run:
        logger.info("  dry_run enabled: skipping insert of %d rows", len(rows))
        return len(rows)
    response = client.table("daily_content_opportunities").upsert(
        rows,
        on_conflict="site,title,primary_keyword,generated_date",
    ).execute()
    return len(response.data or rows)


def process_site(site_data: dict[str, Any], openai_client: OpenAI | None, *, dry_run: bool = False) -> list[dict[str, Any]]:
    active_count = len(site_data["active_rows"])
    slots_available = max(0, ACTIVE_QUEUE_TARGET - active_count)
    if slots_available == 0:
        logger.info("[%s] active queue already has %d items; no new rows generated", site_data["site"], active_count)
        return []

    site_payload = build_site_payload(site_data)
    if not site_payload["prioritized_candidates"]:
        logger.info("[%s] no valid candidates after cleaning", site_data["site"])
        return []

    logger.info("[%s] stage 1/4 analyze site themes", site_data["site"])
    analysis = analyze_site(openai_client, site_payload)

    logger.info("[%s] stage 2/4 generate titles", site_data["site"])
    generated = generate_titles(openai_client, site_payload, analysis)
    generated_items = generated.get("opportunities") or []

    logger.info("[%s] stage 3/4 validate titles", site_data["site"])
    validation = validate_titles(openai_client, site_payload, analysis, generated)
    approved_items = validation.get("approved") or generated_items

    recent_rows = site_data["active_rows"] + site_data["history_rows"]
    filtered_items: list[dict[str, Any]] = []
    seen_local: set[str] = set()
    for item in approved_items:
        title = clean_text(item.get("title"))
        keyword = clean_text(item.get("primary_keyword"))
        reasoning = clean_text(item.get("reasoning"))
        if not title or not keyword or is_noise_keyword(keyword) or is_noise_keyword(title):
            continue
        item_key = f"{normalize_key(title)}::{normalize_key(keyword)}"
        if item_key in seen_local:
            continue
        seen_local.add(item_key)
        score = int(item.get("priority_score") or 0)
        if recent_duplicate(recent_rows, title, keyword, score):
            continue
        filtered_items.append(
            {
                "title": title,
                "primary_keyword": keyword,
                "cluster_id": clean_text(item.get("cluster_id")) or None,
                "reasoning": reasoning,
                "priority_score": max(1, min(100, score)),
            }
        )
        if len(filtered_items) >= slots_available + 2:
            break

    if not filtered_items:
        logger.info("[%s] validation produced no new rows after dedupe", site_data["site"])
        return []

    logger.info("[%s] stage 4/4 enrich payload metadata", site_data["site"])
    enriched_response = enrich_for_arvow(openai_client, site_payload, filtered_items)
    enriched_items = enriched_response.get("entries") or []

    final_rows: list[dict[str, Any]] = []
    for item in enriched_items:
        title = clean_text(item.get("title"))
        keyword = clean_text(item.get("primary_keyword"))
        if not title or not keyword:
            continue
        matched = next(
            (
                source
                for source in filtered_items
                if normalize_key(source["title"]) == normalize_key(title)
                or normalize_key(source["primary_keyword"]) == normalize_key(keyword)
            ),
            None,
        )
        score = int(item.get("priority_score") or (matched or {}).get("priority_score") or 0)
        stored_item = {
            "title": title,
            "primary_keyword": keyword,
            "cluster_id": clean_text(item.get("cluster_id") or (matched or {}).get("cluster_id")) or None,
            "reasoning": clean_text(item.get("reasoning") or (matched or {}).get("reasoning")),
            "priority_score": max(1, min(100, score)),
            "arvow_payload": build_stored_arvow_payload(
                site_data["site"],
                {
                    **item,
                    "priority_score": score,
                    "cluster_id": clean_text(item.get("cluster_id") or (matched or {}).get("cluster_id")) or None,
                    "reasoning": clean_text(item.get("reasoning") or (matched or {}).get("reasoning")),
                },
            ),
        }
        if recent_duplicate(recent_rows, stored_item["title"], stored_item["primary_keyword"], stored_item["priority_score"]):
            continue
        final_rows.append(make_row(site_data["site"], stored_item))
        if len(final_rows) >= slots_available:
            break

    logger.info("[%s] prepared %d new active rows", site_data["site"], len(final_rows))
    return final_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Run analysis without inserting rows")
    parser.add_argument("--use-sample-data", action="store_true", help="Use built-in test data instead of Supabase")
    parser.add_argument("--site", help="Generate for one site only")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.site and args.site not in {site["name"] for site in WEBSITES}:
        raise RuntimeError(f"Unknown site: {args.site}")

    if not args.use_sample_data:
        require_env("SUPABASE_URL", SUPABASE_URL)
        require_env("SUPABASE_SERVICE_KEY", SUPABASE_KEY)
    if not OPENAI_API_KEY and not args.use_sample_data:
        require_env("OPENAI_API_KEY", OPENAI_API_KEY)

    openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY) if not args.use_sample_data else None

    logger.info("Loading %s dataset", "sample" if args.use_sample_data else "live")
    if args.use_sample_data and openai_client is None:
        logger.info("OPENAI_API_KEY not set in sample mode; using deterministic local fallback")
    dataset = load_sample_dataset() if args.use_sample_data else load_live_dataset(supabase)
    sites = build_site_datasets(dataset, site_filter=args.site)

    all_rows: list[dict[str, Any]] = []
    for site_data in sites:
        try:
            all_rows.extend(process_site(site_data, openai_client, dry_run=args.dry_run))
        except Exception as exc:
            logger.error("[%s] generation failed: %s", site_data["site"], str(exc))

    if not all_rows:
        logger.info("No new rows generated.")
        return

    inserted = save_rows(supabase, all_rows, args.dry_run) if supabase else len(all_rows)
    logger.info("Generated %d content opportunity row(s)", inserted)

    if args.dry_run:
        print(json.dumps({"rows": all_rows}, indent=2))


if __name__ == "__main__":
    main()
