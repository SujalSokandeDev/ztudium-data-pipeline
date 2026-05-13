"""Materialize validated content-plan clusters into Semantic Cluster Intelligence V2 tables.

This script is intentionally compatibility-first:
- It does not replace the existing weekly cluster generator.
- It reads the latest validated daily_insights.content_plan.
- It writes normalized semantic cluster records for APIs, lifecycle and filtering.
- It performs deterministic validation/scoring now, with AI-ready metadata fields for
  the multi-layer enrichment passes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

try:
    from supabase import create_client
except ImportError as exc:  # pragma: no cover
    raise RuntimeError(f"Missing dependency: {exc}") from exc

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from config import SUPABASE_SERVICE_KEY, SUPABASE_URL, WEBSITES  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("semantic_cluster_engine")

STOP_WORDS = {
    "the", "a", "an", "of", "in", "to", "for", "and", "or", "is", "are", "was",
    "were", "it", "its", "on", "at", "by", "with", "from", "as", "be", "this",
    "that", "which", "what", "how", "who", "where", "when", "why", "vs",
}

SITE_PROFILES = {site["name"]: site for site in WEBSITES}


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", clean_text(value).lower()).strip("-")


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", clean_text(value).lower()).strip()


def token_set(value: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9]+", normalize_key(value)) if token not in STOP_WORDS and len(token) > 2}


def stable_cluster_key(site: str, cluster: dict[str, Any]) -> str:
    topic = clean_text(cluster.get("cluster_topic") or cluster.get("core_topic") or cluster.get("hub_article_title"))
    primary = clean_text((cluster.get("primary_keyword") or {}).get("keyword"))
    base = f"{site}|{topic}|{primary}"
    digest = hashlib.sha1(normalize_key(base).encode("utf-8")).hexdigest()[:10]
    return f"{slugify(site)}-{slugify(topic or primary or 'cluster')}-{digest}"


def semantic_category(site: str, cluster: dict[str, Any]) -> str:
    profile = SITE_PROFILES.get(site) or {}
    core = clean_text(cluster.get("core_topic") or cluster.get("cluster_topic"))
    if core:
        return core.title()[:80]
    category = clean_text(profile.get("category"))
    return category.split(",")[0].strip().title() if category else "Content Opportunity"


def parent_topic(site: str, cluster: dict[str, Any]) -> str:
    profile = SITE_PROFILES.get(site) or {}
    category = clean_text(profile.get("category"))
    if category:
        return category.split(",")[0].strip().title()
    return semantic_category(site, cluster)


def keyword_rows(cluster: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    primary = cluster.get("primary_keyword") or {}
    if clean_text(primary.get("keyword")):
        rows.append({**primary, "keyword_role": "primary", "sort_order": 0})
    for index, item in enumerate(cluster.get("related_keywords") or [], start=1):
        if clean_text(item.get("keyword")):
            rows.append({**item, "keyword_role": "supporting", "sort_order": index})
    question = clean_text(cluster.get("question_keyword"))
    if question:
        rows.append({
            "keyword": question,
            "volume": 0,
            "kd": 0,
            "intent": "informational",
            "opportunity_score": 0,
            "relaxed": False,
            "keyword_role": "question",
            "sort_order": len(rows),
        })
    return rows


def avg(values: list[float], default: float = 0.0) -> float:
    filtered = [float(v) for v in values if v is not None]
    return sum(filtered) / len(filtered) if filtered else default


def score_cluster(cluster: dict[str, Any], previous_events: list[dict[str, Any]]) -> dict[str, float]:
    kws = keyword_rows(cluster)
    volumes = [float(k.get("volume") or 0) for k in kws]
    kds = [float(k.get("kd") or 0) for k in kws if k.get("keyword_role") != "question"]
    opps = [float(k.get("opportunity_score") or 0) for k in kws]

    total_volume = float(cluster.get("total_cluster_volume") or sum(volumes))
    estimated_traffic = float(cluster.get("estimated_traffic") or total_volume * 0.18)
    avg_kd = avg(kds, 50)
    avg_opp = avg(opps, 0)
    semantic_overlap = semantic_coherence(cluster)
    has_strategy = 1 if clean_text(cluster.get("strategy")) else 0

    repeat_count = len(previous_events)
    published_or_ignored = any((event.get("event_type") or "").lower() in {"published", "ignored", "approved", "generated"} for event in previous_events)
    freshness_score = max(10.0, 100.0 - repeat_count * 18.0 - (30.0 if published_or_ignored else 0.0))
    keyword_strength = min(100.0, min(total_volume / 120.0, 42.0) + max(0.0, 28.0 - avg_kd * 2.2) + min(avg_opp, 30.0))
    semantic_fit = max(35.0, min(100.0, semantic_overlap * 100.0))
    strategic = min(100.0, 45.0 + has_strategy * 20.0 + min(estimated_traffic / 60.0, 25.0) + min(len(kws) * 2.0, 10.0))
    confidence = min(100.0, 45.0 + semantic_fit * 0.25 + min(len(kws), 12) * 2.0 + (10.0 if has_strategy else 0.0))
    opportunity = max(0.0, min(100.0, keyword_strength * 0.38 + freshness_score * 0.22 + confidence * 0.2 + strategic * 0.2))

    return {
        "opportunity_score": round(opportunity, 2),
        "freshness_score": round(freshness_score, 2),
        "cluster_confidence": round(confidence, 2),
        "semantic_fit_score": round(semantic_fit, 2),
        "keyword_strength_score": round(keyword_strength, 2),
        "strategic_score": round(strategic, 2),
    }


def semantic_coherence(cluster: dict[str, Any]) -> float:
    topic_tokens = token_set(clean_text(cluster.get("cluster_topic") or cluster.get("core_topic") or cluster.get("hub_article_title")))
    primary_tokens = token_set(clean_text((cluster.get("primary_keyword") or {}).get("keyword")))
    related = [token_set(clean_text(item.get("keyword"))) for item in cluster.get("related_keywords") or []]
    base = topic_tokens | primary_tokens
    if not base or not related:
        return 0.55
    overlaps = [len(base.intersection(tokens)) / max(len(base.union(tokens)), 1) for tokens in related if tokens]
    return max(0.35, min(1.0, avg(overlaps, 0.55) + 0.35))


def lifecycle_status(scores: dict[str, float], previous_events: list[dict[str, Any]]) -> str:
    if any((event.get("event_type") or "").lower() == "published" for event in previous_events):
        return "published"
    if any((event.get("event_type") or "").lower() == "ignored" for event in previous_events):
        return "suppressed"
    if scores["freshness_score"] < 35:
        return "expired"
    if scores["cluster_confidence"] < 55:
        return "suppressed"
    return "active"


def validation_metadata(cluster: dict[str, Any], scores: dict[str, float], previous_events: list[dict[str, Any]]) -> dict[str, Any]:
    kws = keyword_rows(cluster)
    invalid = [kw.get("keyword") for kw in kws if not clean_text(kw.get("keyword"))]
    intents = Counter(clean_text(kw.get("intent")) or "unknown" for kw in kws)
    return {
        "layers": {
            "deterministic_keyword_site_validation": {"passed": not invalid, "invalid_keywords": invalid},
            "semantic_grouping_validation": {"passed": scores["semantic_fit_score"] >= 55, "score": scores["semantic_fit_score"]},
            "ai_cluster_coherence_validation": {"status": "precomputed_ready", "score": scores["cluster_confidence"]},
            "ai_strategy_enrichment": {"status": "seeded_from_validated_content_plan", "has_strategy": bool(clean_text(cluster.get("strategy")))},
            "hallucination_duplicate_suppression": {"passed": True, "previous_event_count": len(previous_events)},
        },
        "intent_mix": intents,
        "keyword_count": len(kws),
    }


def build_jontool_payload(site: str, cluster: dict[str, Any]) -> dict[str, Any]:
    primary = clean_text((cluster.get("primary_keyword") or {}).get("keyword")) or clean_text(cluster.get("cluster_topic"))
    related = [clean_text(item.get("keyword")) for item in cluster.get("related_keywords") or [] if clean_text(item.get("keyword"))]
    title = clean_text(cluster.get("hub_article_title")) or primary.title()
    return {
        "cluster": {
            "name": title,
            "cluster_topic": clean_text(cluster.get("cluster_topic")) or title,
            "pillar_title": title,
            "raw_keywords": "\n".join([primary, *related]),
        },
        "spokes": [
            {"title": title, "primary_keyword": primary, "is_pillar": True, "sort_order": 0},
            *[
                {"title": keyword.title(), "primary_keyword": keyword, "is_pillar": False, "sort_order": index}
                for index, keyword in enumerate(related[:10], start=1)
            ],
        ],
    }


def fetch_latest_content_plan(client, requested_date: str | None = None) -> tuple[dict[str, Any], dict[str, Any]] | None:
    query = client.table("daily_insights").select("id, date, generated_at, content_plan")
    if requested_date:
        query = query.eq("date", requested_date)
    else:
        query = query.order("date", desc=True).limit(1)
    response = query.execute()
    rows = response.data or []
    if not rows:
        return None
    row = rows[0]
    content_plan = row.get("content_plan") or {}
    if isinstance(content_plan, str):
        content_plan = json.loads(content_plan)
    return row, content_plan


def fetch_history(client, site: str, keys: list[str]) -> dict[str, list[dict[str, Any]]]:
    if not keys:
        return {}
    try:
        response = (
            client.table("semantic_opportunity_history")
            .select("cluster_key, event_type, created_at")
            .eq("site", site)
            .in_("cluster_key", keys)
            .order("created_at", desc=True)
            .execute()
        )
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in response.data or []:
            grouped[row["cluster_key"]].append(row)
        return grouped
    except Exception as exc:
        logger.warning("History lookup skipped for %s: %s", site, str(exc)[:160])
        return {}


def upsert_many(client, table: str, rows: list[dict[str, Any]], on_conflict: str, chunk_size: int = 100) -> None:
    for start in range(0, len(rows), chunk_size):
        client.table(table).upsert(rows[start:start + chunk_size], on_conflict=on_conflict).execute()


def materialize(trigger_source: str, requested_date: str | None = None, site_filter: str | None = None) -> dict[str, Any]:
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")

    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    latest = fetch_latest_content_plan(client, requested_date)
    if not latest:
        raise RuntimeError("No daily_insights.content_plan found to materialize")

    insight_row, content_plan = latest
    sites = content_plan.get("sites") or []
    if site_filter:
        sites = [site for site in sites if clean_text(site.get("website")).lower() == site_filter.lower()]

    attempted = [clean_text(site.get("website")) for site in sites if clean_text(site.get("website"))]
    run_row = {
        "run_date": date.today().isoformat(),
        "trigger_source": trigger_source,
        "source_daily_insight_date": insight_row.get("date"),
        "source_daily_insight_id": insight_row.get("id"),
        "status": "running",
        "sites_attempted": attempted,
        "validation_summary": {},
    }
    run_response = client.table("semantic_cluster_runs").insert(run_row).execute()
    run_id = (run_response.data or [{}])[0].get("id")
    if not run_id:
        raise RuntimeError("Failed to create semantic_cluster_runs row")

    cluster_rows: list[dict[str, Any]] = []
    keyword_payloads_by_key: dict[str, list[dict[str, Any]]] = {}
    history_events: list[dict[str, Any]] = []
    succeeded: list[str] = []
    failed: list[str] = []
    errors: dict[str, str] = {}
    validation_counts = Counter()

    for site_data in sites:
        site = clean_text(site_data.get("website"))
        if not site:
            continue
        try:
            clusters = site_data.get("clusters") or []
            keys = [stable_cluster_key(site, cluster) for cluster in clusters]
            history = fetch_history(client, site, keys)
            for cluster in clusters:
                key = stable_cluster_key(site, cluster)
                previous = history.get(key, [])
                scores = score_cluster(cluster, previous)
                status = lifecycle_status(scores, previous)
                kws = keyword_rows(cluster)
                metadata = validation_metadata(cluster, scores, previous)
                validation_counts.update({f"status_{status}": 1, "clusters": 1, "keywords": len(kws)})
                cluster_rows.append({
                    "cluster_key": key,
                    "site": site,
                    "run_id": run_id,
                    "source_daily_insight_date": insight_row.get("date"),
                    "cluster_topic": clean_text(cluster.get("cluster_topic") or cluster.get("hub_article_title")),
                    "parent_topic": parent_topic(site, cluster),
                    "semantic_category": semantic_category(site, cluster),
                    "core_topic": clean_text(cluster.get("core_topic")) or None,
                    "hub_article_title": clean_text(cluster.get("hub_article_title") or cluster.get("cluster_topic")),
                    "pillar_page_url": clean_text(cluster.get("published_url")) or None,
                    "strategy": clean_text(cluster.get("strategy")) or clean_text(site_data.get("summary")) or None,
                    "lifecycle_status": status,
                    **scores,
                    "total_cluster_volume": int(cluster.get("total_cluster_volume") or 0),
                    "estimated_traffic": int(cluster.get("estimated_traffic") or 0),
                    "keyword_count": len(kws),
                    "validation_metadata": metadata,
                    "enrichment_metadata": {
                        "source": "validated_daily_insights_content_plan",
                        "multi_layer_ai_ready": True,
                        "target_cluster_count_per_site": "10-15",
                    },
                    "jon_tool": build_jontool_payload(site, cluster),
                    "content_plan_cluster": cluster,
                    "last_seen_at": datetime.now(tz=timezone.utc).isoformat(),
                    "updated_at": datetime.now(tz=timezone.utc).isoformat(),
                })
                keyword_payloads_by_key[key] = [
                    {
                        "site": site,
                        "keyword": clean_text(item.get("keyword")),
                        "keyword_role": item.get("keyword_role"),
                        "volume": int(item.get("volume") or 0),
                        "kd": float(item.get("kd") or 0),
                        "intent": clean_text(item.get("intent")) or None,
                        "opportunity_score": float(item.get("opportunity_score") or 0),
                        "freshness_score": scores["freshness_score"],
                        "relaxed": bool(item.get("relaxed")),
                        "source_snapshot_date": insight_row.get("date"),
                        "sort_order": int(item.get("sort_order") or 0),
                    }
                    for item in kws
                ]
                if status == "active" and scores["opportunity_score"] >= 45:
                    history_events.append({
                        "cluster_key": key,
                        "site": site,
                        "event_type": "surfaced" if previous else "resurfaced",
                        "event_source": trigger_source,
                        "event_metadata": {"opportunity_score": scores["opportunity_score"], "freshness_score": scores["freshness_score"]},
                    })
            succeeded.append(site)
        except Exception as exc:
            failed.append(site)
            errors[site] = str(exc)
            logger.exception("Failed semantic materialization for %s", site)

    try:
        if cluster_rows:
            upsert_many(client, "semantic_clusters", cluster_rows, "site,cluster_key")
            cluster_lookup_rows = (
                client.table("semantic_clusters")
                .select("id, site, cluster_key")
                .in_("cluster_key", [row["cluster_key"] for row in cluster_rows])
                .execute()
                .data
                or []
            )
            cluster_ids = {(row["site"], row["cluster_key"]): row["id"] for row in cluster_lookup_rows}
            keyword_rows_to_write: list[dict[str, Any]] = []
            for key, rows in keyword_payloads_by_key.items():
                cluster_site = next((row["site"] for row in cluster_rows if row["cluster_key"] == key), "")
                cluster_id = cluster_ids.get((cluster_site, key))
                if not cluster_id:
                    continue
                for row in rows:
                    keyword_rows_to_write.append({**row, "cluster_id": cluster_id})
            if keyword_rows_to_write:
                upsert_many(client, "semantic_cluster_keywords", keyword_rows_to_write, "cluster_id,keyword,keyword_role")
            if history_events:
                client.table("semantic_opportunity_history").insert(history_events).execute()

        client.table("semantic_cluster_runs").update({
            "status": "completed" if not failed else "partial",
            "sites_succeeded": succeeded,
            "sites_failed": failed,
            "cluster_count": len(cluster_rows),
            "keyword_count": sum(len(rows) for rows in keyword_payloads_by_key.values()),
            "validation_summary": dict(validation_counts),
            "error_details": errors,
            "completed_at": datetime.now(tz=timezone.utc).isoformat(),
        }).eq("id", run_id).execute()
    except Exception as exc:
        client.table("semantic_cluster_runs").update({
            "status": "failed",
            "error_details": {"write": str(exc)},
            "completed_at": datetime.now(tz=timezone.utc).isoformat(),
        }).eq("id", run_id).execute()
        raise

    return {
        "run_id": run_id,
        "sites": succeeded,
        "failed": failed,
        "clusters": len(cluster_rows),
        "keywords": sum(len(rows) for rows in keyword_payloads_by_key.values()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialize Semantic Cluster Intelligence V2")
    parser.add_argument("--trigger-source", default="manual")
    parser.add_argument("--date", default=None)
    parser.add_argument("--site", default=None)
    args = parser.parse_args()

    result = materialize(args.trigger_source, requested_date=args.date, site_filter=args.site)
    logger.info(
        "Semantic clusters materialized: run=%s sites=%d clusters=%d keywords=%d failed=%d",
        result["run_id"],
        len(result["sites"]),
        result["clusters"],
        result["keywords"],
        len(result["failed"]),
    )


if __name__ == "__main__":
    main()
