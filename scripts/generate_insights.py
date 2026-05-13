"""
generate_insights.py — Strategic Insights Engine v4
Analyzes comprehensive dashboard data and generates executive-level
strategic recommendations with root cause analysis, keyword/page-level
drill-downs, competitor context, and quantified impacts.

Run after compute_trends.py: python scripts/generate_insights.py
"""
import os
import sys
import json
import re
import html
import hashlib
import logging
import time
import threading
import textwrap
from datetime import date, timedelta, datetime, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
load_dotenv()

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Suppress noisy HTTP request logs from httpx / openai / supabase
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("supabase").setLevel(logging.WARNING)

# ── Dependencies ────────────────────────────────────────────
try:
    from supabase import create_client
    from openai import OpenAI
except ImportError as e:
    logger.error(f"Missing package: {e}. Run: pip install supabase openai")
    sys.exit(1)

from ai_client import get_ai_client, ai_chat_completion_reliable, response_model_used, response_provider_used
from semantic_cluster_engine import materialize as materialize_semantic_clusters
try:
    from ai_client import _switch_to_gemini
except ImportError:
    _switch_to_gemini = None

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
    sys.exit(1)

if not OPENAI_API_KEY and not GEMINI_API_KEY:
    logger.error("At least one of OPENAI_API_KEY or GEMINI_API_KEY must be set")
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
# AI client with automatic Gemini fallback (managed by ai_client module)
openai_client = get_ai_client()

SITES = [
    "CitiesABC",
    "BusinessABC",
    "HedgeThink",
    "FashionABC",
    "TradersDNA",
    "FreedomX",
    "Wisdomia",
    "SportsDNA",
    "IntelligentHQ",
]

PRIMARY_MODEL = "gpt-4o"
GEMINI_MODEL = "gemini-3.1-pro-preview"
GEMINI_FALLBACK_MODEL = os.getenv("GEMINI_FALLBACK_MODEL", "gemini-2.5-pro")
OPENAI_SAFETY_MODEL = os.getenv("OPENAI_SAFETY_MODEL", "gpt-4o")

SILENT_DECAY_MIN_PREVIOUS_CLICKS = int(os.getenv("SILENT_DECAY_MIN_PREVIOUS_CLICKS", "20"))
SILENT_DECAY_MIN_DECLINE_PCT = float(os.getenv("SILENT_DECAY_MIN_DECLINE_PCT", "25"))
SILENT_DECAY_MIN_POSITION_DRIFT = float(os.getenv("SILENT_DECAY_MIN_POSITION_DRIFT", "3"))
DEAD_PAGE_LOOKBACK_DAYS = int(os.getenv("DEAD_PAGE_LOOKBACK_DAYS", "90"))
DEAD_PAGE_VISIBLE_MIN_IMPRESSIONS = int(os.getenv("DEAD_PAGE_VISIBLE_MIN_IMPRESSIONS", "25"))
DEAD_PAGE_VISIBLE_MAX_CLICKS = int(os.getenv("DEAD_PAGE_VISIBLE_MAX_CLICKS", "2"))


# ══════════════════════════════════════════════════════════════
#  TERMINAL OUTPUT HELPERS
# ══════════════════════════════════════════════════════════════

class Spinner:
    """Threaded spinner that shows progress during long-running API calls."""
    FRAMES = ["   ", ".  ", ".. ", "...", " ..", "  ."]

    def __init__(self, message):
        self.message = message
        self._stop = threading.Event()
        self._thread = None
        self._start_time = None

    def __enter__(self):
        self._start_time = time.time()
        self._thread = threading.Thread(target=self._spin, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *args):
        self._stop.set()
        self._thread.join()
        elapsed = time.time() - self._start_time
        print(f"\r  {self.message} done ({elapsed:.1f}s)               ")

    def _spin(self):
        idx = 0
        while not self._stop.is_set():
            elapsed = time.time() - self._start_time
            frame = self.FRAMES[idx % len(self.FRAMES)]
            print(f"\r  {self.message}{frame} ({elapsed:.0f}s)", end="", flush=True)
            idx += 1
            self._stop.wait(0.4)


def print_header(title):
    """Print a clean section header."""
    w = 60
    print()
    print(f"  {'-' * w}")
    print(f"  {title}")
    print(f"  {'-' * w}")


def print_box(title, subtitle=""):
    """Print a prominent title box."""
    w = 60
    print(f"\n  {'=' * w}")
    print(f"   {title}")
    if subtitle:
        print(f"   {subtitle}")
    print(f"  {'=' * w}")


# ══════════════════════════════════════════════════════════════
#  DATA COLLECTION — Comprehensive context gathering
# ══════════════════════════════════════════════════════════════

def safe_query(table, select, filters=None, order=None, limit=500, label="query"):
    """Execute a Supabase query with error handling. Returns [] on failure."""
    try:
        q = supabase.table(table).select(select)
        if filters:
            for method, args in filters:
                q = getattr(q, method)(*args)
        if order:
            q = q.order(order[0], desc=order[1])
        q = q.limit(limit)
        result = q.execute()
        data = result.data or []
        logger.info(f"  [{label}] {len(data)} rows")
        return data
    except Exception as e:
        logger.warning(f"  [{label}] Query failed: {e}")
        return []


def gather_context():
    """Gather comprehensive dashboard data for AI analysis."""
    today = date.today()
    today_str = today.isoformat()
    week_ago = (today - timedelta(days=7)).isoformat()
    two_weeks_ago = (today - timedelta(days=14)).isoformat()
    month_ago = (today - timedelta(days=30)).isoformat()
    quarter_ago = (today - timedelta(days=90)).isoformat()

    context = {}

    # ── 1. Daily metrics: last 7 days (all sites) ───────────
    context["daily_metrics_7d"] = safe_query(
        "daily_metrics",
        "date, website, gsc_clicks, gsc_impressions, gsc_ctr, gsc_position, "
        "ga_sessions, ga_organic_sessions, ga_bounce_rate, domain_rating, "
        "ahrefs_keywords, ahrefs_top3, backlinks_total, ref_domains_total",
        filters=[("gte", ("date", week_ago))],
        order=("date", True),
        limit=500,
        label="daily_7d"
    )

    # ── 2. Daily metrics: previous week (for WoW comparison) ─
    context["daily_metrics_prev_week"] = safe_query(
        "daily_metrics",
        "date, website, gsc_clicks, gsc_impressions, ga_sessions, ga_organic_sessions",
        filters=[("gte", ("date", two_weeks_ago)), ("lt", ("date", week_ago))],
        order=("date", True),
        limit=500,
        label="daily_prev_week"
    )

    # ── 3. Calculated trends + anomalies (today) ────────────
    context["trends_today"] = safe_query(
        "calculated_metrics",
        "*",
        filters=[("eq", ("date", today_str))],
        limit=200,
        label="trends_today"
    )

    # ── 4. All anomalies from last 7 days ───────────────────
    context["anomalies_7d"] = safe_query(
        "calculated_metrics",
        "date, website, metric_name, day_over_day_pct, week_over_week_pct, "
        "severity, anomaly_description, site_wide_issue, cross_site_pattern, "
        "historical_percentile",
        filters=[("eq", ("is_anomaly", True)), ("gte", ("date", week_ago))],
        limit=100,
        label="anomalies_7d"
    )

    # ── 5. Ahrefs overview (latest per site) ────────────────
    ahrefs_raw = safe_query(
        "ahrefs_overview",
        "website, date, dr, dr_delta, ur, ur_delta, organic_traffic, "
        "organic_keywords, top3_keywords, top3_delta, backlinks, ref_domains, "
        "ref_domains_delta, traffic_value, paid_keywords, paid_traffic, "
        "ai_chatgpt, ai_perplexity, ai_gemini, ai_copilot, ai_overview",
        order=("date", True),
        limit=50,
        label="ahrefs_overview"
    )
    # Deduplicate per website (keep latest)
    seen = set()
    ahrefs = []
    for row in ahrefs_raw:
        if row.get("website") and row["website"] not in seen:
            seen.add(row["website"])
            ahrefs.append(row)
    context["ahrefs_overview"] = ahrefs

    # ── 6. Keyword position data (top movers, last 7 days) ──
    kw_recent = safe_query(
        "website_keywords",
        "date, website, keyword, clicks, impressions, position, search_volume",
        filters=[("gte", ("date", week_ago)), ("eq", ("source", "gsc"))],
        order=("clicks", True),
        limit=200,
        label="keywords_recent"
    )

    kw_previous = safe_query(
        "website_keywords",
        "date, website, keyword, clicks, impressions, position",
        filters=[
            ("gte", ("date", two_weeks_ago)),
            ("lt", ("date", week_ago)),
            ("eq", ("source", "gsc")),
        ],
        order=("clicks", True),
        limit=200,
        label="keywords_previous"
    )

    # Calculate keyword position changes (week-over-week)
    context["keyword_movers"] = compute_keyword_movers(kw_recent, kw_previous)

    # ── 7. Page performance (top pages, last 7 days) ────────
    pages_recent = safe_query(
        "website_pages",
        "date, website, url, clicks, impressions, ga_sessions",
        filters=[("gte", ("date", week_ago))],
        order=("clicks", True),
        limit=100,
        label="pages_recent"
    )

    pages_previous = safe_query(
        "website_pages",
        "date, website, url, clicks, impressions, ga_sessions",
        filters=[
            ("gte", ("date", two_weeks_ago)),
            ("lt", ("date", week_ago)),
        ],
        order=("clicks", True),
        limit=100,
        label="pages_previous"
    )

    context["page_movers"] = compute_page_movers(pages_recent, pages_previous)

    # ── 8. Competitor data (organic competitors) ────────────
    context["competitors"] = safe_query(
        "ahrefs_competitors",
        "website, competitor_domain, keyword_overlap, share_pct, competitor_keywords",
        order=("keyword_overlap", True),
        limit=50,
        label="competitors"
    )

    # ── 9. High-DR referring domains (new backlinks) ────────
    context["new_backlinks"] = safe_query(
        "ahrefs_referring_domains",
        "website, domain, dr, dofollow_links, first_seen",
        filters=[("gte", ("dr", 40))],
        order=("dr", True),
        limit=30,
        label="new_high_dr_backlinks"
    )

    # ── 10. Broken backlinks (high priority) ────────────────
    context["broken_backlinks"] = safe_query(
        "ahrefs_broken_backlinks",
        "website, referring_page, target_url, http_code, ref_domain_dr, anchor_text",
        filters=[("gte", ("ref_domain_dr", 40))],
        order=("ref_domain_dr", True),
        limit=15,
        label="broken_backlinks_high_dr"
    )

    # ── 11. Content gap easy wins ───────────────────────────
    context["easy_wins"] = safe_query(
        "content_gap_keywords",
        "website, keyword, volume, kd, opportunity_score, intent",
        filters=[("eq", ("is_easy_win", True))],
        order=("opportunity_score", True),
        limit=20,
        label="content_gap_easy_wins"
    )

    # ── 12. Historical baselines (30/90-day aggregates) ─────
    context["baselines"] = compute_baselines(month_ago, quarter_ago)

    return context


# ══════════════════════════════════════════════════════════════
#  ROOT CAUSE ANALYSIS — Keyword & Page movers
# ══════════════════════════════════════════════════════════════

def compute_keyword_movers(recent, previous):
    """
    Compare keyword positions week-over-week.
    Returns top gainers and losers with click/traffic impact.
    """
    if not recent or not previous:
        return {"gainers": [], "losers": []}

    # Aggregate recent week by website+keyword
    recent_map = {}
    for row in recent:
        key = f"{row['website']}|{row['keyword']}"
        if key not in recent_map:
            recent_map[key] = {
                "website": row["website"],
                "keyword": row["keyword"],
                "position": row.get("position"),
                "clicks": row.get("clicks", 0),
                "impressions": row.get("impressions", 0),
                "volume": row.get("search_volume"),
            }
        else:
            recent_map[key]["clicks"] = (recent_map[key]["clicks"] or 0) + (row.get("clicks") or 0)

    # Aggregate previous week
    prev_map = {}
    for row in previous:
        key = f"{row['website']}|{row['keyword']}"
        if key not in prev_map:
            prev_map[key] = {
                "position": row.get("position"),
                "clicks": row.get("clicks", 0),
            }
        else:
            prev_map[key]["clicks"] = (prev_map[key]["clicks"] or 0) + (row.get("clicks") or 0)

    # Calculate changes for shared keywords
    movers = []
    for key, curr in recent_map.items():
        prev = prev_map.get(key)
        if not prev or curr["position"] is None:
            continue
        prev_pos = prev.get("position")
        if prev_pos is None:
            continue

        pos_change = float(prev_pos) - float(curr["position"])  # Positive = improved
        click_change = (curr["clicks"] or 0) - (prev.get("clicks") or 0)

        if abs(pos_change) >= 2 or abs(click_change) >= 10:
            movers.append({
                "website": curr["website"],
                "keyword": curr["keyword"],
                "position_was": float(prev_pos),
                "position_now": float(curr["position"]),
                "position_change": round(pos_change, 1),
                "clicks_change": click_change,
                "current_clicks": curr["clicks"],
                "volume": curr.get("volume"),
            })

    # Sort: biggest losers first (most negative position change)
    movers.sort(key=lambda x: x["position_change"])

    losers = [m for m in movers if m["position_change"] < 0][:10]
    gainers = [m for m in movers if m["position_change"] > 0][-10:]
    gainers.reverse()

    return {"gainers": gainers, "losers": losers}


def compute_page_movers(recent, previous):
    """
    Compare page traffic week-over-week.
    Returns top gaining and losing pages.
    """
    if not recent or not previous:
        return {"rising": [], "falling": []}

    # Aggregate by website+url
    def aggregate(rows):
        agg = {}
        for row in rows:
            key = f"{row['website']}|{row['url']}"
            if key not in agg:
                agg[key] = {
                    "website": row["website"],
                    "url": row["url"],
                    "clicks": 0,
                    "sessions": 0,
                }
            agg[key]["clicks"] += (row.get("clicks") or 0)
            agg[key]["sessions"] += (row.get("ga_sessions") or 0)
        return agg

    recent_agg = aggregate(recent)
    prev_agg = aggregate(previous)

    movers = []
    for key, curr in recent_agg.items():
        prev = prev_agg.get(key)
        if not prev:
            continue
        click_change = curr["clicks"] - prev["clicks"]
        if abs(click_change) >= 10:
            # Shorten URL for readability
            path = curr["url"]
            if "://" in path:
                path = "/" + path.split("://", 1)[1].split("/", 1)[-1]
            if len(path) > 60:
                path = path[:57] + "..."

            movers.append({
                "website": curr["website"],
                "page": path,
                "clicks_now": curr["clicks"],
                "clicks_was": prev["clicks"],
                "change": click_change,
                "sessions_now": curr["sessions"],
            })

    movers.sort(key=lambda x: x["change"])

    falling = [m for m in movers if m["change"] < 0][:5]
    rising = [m for m in movers if m["change"] > 0][-5:]
    rising.reverse()

    return {"rising": rising, "falling": falling}


def compute_baselines(month_ago, quarter_ago):
    """Compute 30-day and 90-day baselines for key metrics per website."""
    try:
        result = supabase.table("daily_metrics") \
            .select("website, gsc_clicks, ga_sessions, domain_rating") \
            .gte("date", quarter_ago) \
            .limit(5000) \
            .execute()

        rows = result.data or []
        if not rows:
            return {}

        baselines = defaultdict(lambda: {
            "clicks_30d": [], "clicks_90d": [],
            "sessions_30d": [], "sessions_90d": [],
            "dr_values": [],
        })

        for row in rows:
            w = row["website"]
            baselines[w]["clicks_90d"].append(row.get("gsc_clicks"))
            baselines[w]["sessions_90d"].append(row.get("ga_sessions"))
            if row.get("domain_rating"):
                baselines[w]["dr_values"].append(row["domain_rating"])
            if row.get("date", "") >= month_ago:
                baselines[w]["clicks_30d"].append(row.get("gsc_clicks"))
                baselines[w]["sessions_30d"].append(row.get("ga_sessions"))

        # Compute min/max/avg per website
        summary = {}
        for website, data in baselines.items():
            def stats(values):
                valid = [v for v in values if v is not None]
                if not valid:
                    return None
                return {
                    "min": min(valid),
                    "max": max(valid),
                    "avg": round(sum(valid) / len(valid)),
                    "count": len(valid),
                }

            summary[website] = {
                "clicks_30d": stats(data["clicks_30d"]),
                "clicks_90d": stats(data["clicks_90d"]),
                "sessions_30d": stats(data["sessions_30d"]),
                "sessions_90d": stats(data["sessions_90d"]),
                "dr": stats(data["dr_values"]),
            }

        logger.info(f"  [baselines] Computed for {len(summary)} websites")
        return summary

    except Exception as e:
        logger.warning(f"  [baselines] Failed: {e}")
        return {}


# ══════════════════════════════════════════════════════════════
#  SMART DATA SUMMARIZATION — Prioritize signal over noise
# ══════════════════════════════════════════════════════════════

def summarize_context(context):
    """
    Build a prioritized context string for GPT.
    Order: Anomalies > Keyword movers > Page movers > Baselines > Everything else.
    This ensures GPT sees critical signals first.
    """
    sections = []

    # 1. ANOMALIES (full detail — highest priority)
    anomalies = context.get("anomalies_7d", [])
    if anomalies:
        sections.append("=== ANOMALIES & ALERTS (CRITICAL — analyze these first) ===")
        for a in anomalies:
            sections.append(json.dumps(a, default=str))
    else:
        sections.append("=== ANOMALIES: None detected in last 7 days ===")

    # 2. KEYWORD POSITION CHANGES (top movers)
    kw = context.get("keyword_movers", {})
    if kw.get("losers"):
        sections.append("\n=== KEYWORD RANKING LOSSES (investigate root cause) ===")
        for m in kw["losers"]:
            sections.append(
                f"  {m['website']}: '{m['keyword']}' dropped from #{m['position_was']:.0f} to "
                f"#{m['position_now']:.0f} ({m['position_change']:+.0f}), "
                f"clicks change: {m['clicks_change']:+d}"
                + (f", monthly volume: {m['volume']}" if m.get('volume') else "")
            )
    if kw.get("gainers"):
        sections.append("\n=== KEYWORD RANKING GAINS ===")
        for m in kw["gainers"]:
            sections.append(
                f"  {m['website']}: '{m['keyword']}' rose from #{m['position_was']:.0f} to "
                f"#{m['position_now']:.0f} ({m['position_change']:+.0f}), "
                f"clicks change: {m['clicks_change']:+d}"
            )

    # 3. PAGE TRAFFIC CHANGES
    pages = context.get("page_movers", {})
    if pages.get("falling"):
        sections.append("\n=== PAGES LOSING TRAFFIC ===")
        for p in pages["falling"]:
            sections.append(
                f"  {p['website']}: {p['page']} — clicks: {p['clicks_was']} -> {p['clicks_now']} "
                f"({p['change']:+d})"
            )
    if pages.get("rising"):
        sections.append("\n=== PAGES GAINING TRAFFIC ===")
        for p in pages["rising"]:
            sections.append(
                f"  {p['website']}: {p['page']} — clicks: {p['clicks_was']} -> {p['clicks_now']} "
                f"({p['change']:+d})"
            )

    # 4. HISTORICAL BASELINES
    baselines = context.get("baselines", {})
    if baselines:
        sections.append("\n=== HISTORICAL BASELINES (30/90-day) ===")
        for website, b in baselines.items():
            parts = [f"  {website}:"]
            if b.get("clicks_90d"):
                parts.append(f"clicks 90d avg={b['clicks_90d']['avg']}, "
                           f"min={b['clicks_90d']['min']}, max={b['clicks_90d']['max']}")
            if b.get("sessions_90d"):
                parts.append(f"sessions 90d avg={b['sessions_90d']['avg']}")
            if b.get("dr"):
                parts.append(f"DR range={b['dr']['min']}-{b['dr']['max']}")
            sections.append(" | ".join(parts))

    # 5. AHREFS OVERVIEW (per-site snapshot)
    ahrefs = context.get("ahrefs_overview", [])
    if ahrefs:
        sections.append("\n=== AHREFS OVERVIEW (latest snapshot per site) ===")
        for a in ahrefs:
            sections.append(json.dumps(a, default=str))

    # 6. DAILY METRICS SUMMARY (aggregate per website for last 7 days)
    daily = context.get("daily_metrics_7d", [])
    if daily:
        sections.append("\n=== DAILY METRICS SUMMARY (last 7 days, aggregated per site) ===")
        by_site = defaultdict(lambda: {"clicks": 0, "impressions": 0, "sessions": 0, "days": 0})
        for row in daily:
            w = row.get("website", "unknown")
            by_site[w]["clicks"] += (row.get("gsc_clicks") or 0)
            by_site[w]["impressions"] += (row.get("gsc_impressions") or 0)
            by_site[w]["sessions"] += (row.get("ga_sessions") or 0)
            by_site[w]["days"] += 1
        for site, d in sorted(by_site.items()):
            sections.append(
                f"  {site}: {d['days']}d total — "
                f"clicks={d['clicks']}, impressions={d['impressions']}, sessions={d['sessions']}"
            )

    # 7. COMPETITORS
    competitors = context.get("competitors", [])
    if competitors:
        sections.append("\n=== TOP ORGANIC COMPETITORS ===")
        by_site = defaultdict(list)
        for c in competitors:
            by_site[c["website"]].append(
                f"{c['competitor_domain']} (overlap={c.get('keyword_overlap')}, share={c.get('share_pct')})"
            )
        for site, comps in by_site.items():
            sections.append(f"  {site}: {', '.join(comps[:5])}")

    # 8. NEW HIGH-DR BACKLINKS
    backlinks = context.get("new_backlinks", [])
    if backlinks:
        sections.append("\n=== NEW HIGH-DR BACKLINKS (DR 40+) ===")
        for b in backlinks[:15]:
            sections.append(
                f"  {b.get('website')}: from {b.get('domain')} (DR={b.get('dr')}, "
                f"dofollow={b.get('dofollow_links')})"
            )

    # 9. BROKEN BACKLINKS (high DR)
    broken = context.get("broken_backlinks", [])
    if broken:
        sections.append("\n=== HIGH-DR BROKEN BACKLINKS (reclaim opportunities) ===")
        for b in broken[:10]:
            sections.append(
                f"  {b.get('website')}: DR={b.get('ref_domain_dr')} from {b.get('referring_page', '')[:80]} "
                f"-> {b.get('target_url', '')[:60]} (HTTP {b.get('http_code')})"
            )

    # 10. CONTENT GAP EASY WINS
    easy_wins = context.get("easy_wins", [])
    if easy_wins:
        sections.append("\n=== CONTENT GAP EASY WINS (top opportunities) ===")
        for ew in easy_wins[:10]:
            sections.append(
                f"  {ew.get('website')}: '{ew.get('keyword')}' "
                f"(vol={ew.get('volume')}, KD={ew.get('kd')}, "
                f"score={ew.get('opportunity_score')}, intent={ew.get('intent')})"
            )

    return "\n".join(sections)


# V2 DATA ENRICHMENT - deterministic signals before AI calls

def _num(value, default=0):
    """Coerce numeric strings / None safely for detector math."""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value
    try:
        return float(str(value).replace(",", "").replace("%", "").strip())
    except Exception:
        return default


def _pct_change(now, previous):
    now = _num(now)
    previous = _num(previous)
    if previous == 0:
        return 0
    return round(((now - previous) / previous) * 100, 1)


def _ensure_gemini_priority(reason="Gemini 3.1 Pro Preview is the primary insights model"):
    if _switch_to_gemini is not None:
        _switch_to_gemini(reason)


def _extract_json_text(raw: str) -> str:
    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
    if raw.endswith("```"):
        raw = raw[:-3]
    raw = raw.strip()
    if raw.startswith("json"):
        raw = raw[4:].strip()
    return raw


def _close_open_structures(raw: str) -> str:
    result = raw
    open_braces = result.count("{")
    close_braces = result.count("}")
    open_brackets = result.count("[")
    close_brackets = result.count("]")
    if open_brackets > close_brackets:
        result += "]" * (open_brackets - close_brackets)
    if open_braces > close_braces:
        result += "}" * (open_braces - close_braces)
    return result


def _truncate_to_last_json_boundary(raw: str) -> str:
    for idx in range(len(raw) - 1, -1, -1):
        if raw[idx] not in "}]":
            continue
        candidate = _close_open_structures(raw[: idx + 1].rstrip(", \n\r\t"))
        try:
            json.loads(candidate)
            return candidate
        except json.JSONDecodeError:
            continue
    return _close_open_structures(raw.rstrip(", \n\r\t"))


def _parse_json_response(raw: str):
    cleaned = _extract_json_text(raw)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as first_error:
        repaired = re.sub(r",(\s*[\]}])", r"\1", cleaned.rstrip())
        repaired = _close_open_structures(repaired)
        try:
            return json.loads(repaired)
        except json.JSONDecodeError:
            truncated = _truncate_to_last_json_boundary(repaired)
            return json.loads(truncated)


def _json_response_content(response):
    raw = response.choices[0].message.content or ""
    return _parse_json_response(raw)


def _call_gemini_chat_completion(**kwargs):
    _ensure_gemini_priority()
    return ai_chat_completion_reliable(
        model=PRIMARY_MODEL,
        primary_gemini_model=GEMINI_MODEL,
        secondary_gemini_model=GEMINI_FALLBACK_MODEL,
        openai_fallback_model=OPENAI_SAFETY_MODEL,
        **kwargs,
    )


def _site_rows(rows, website):
    return [r for r in rows or [] if r.get("website") == website]


def _latest_by_key(rows, key_field):
    latest = {}
    for row in sorted(rows or [], key=lambda r: str(r.get("date", ""))):
        key = row.get(key_field)
        if key:
            latest[key] = row
    return latest


def detect_zombie_pages(website):
    cutoff = (date.today() - timedelta(days=90)).isoformat()
    rows = safe_query(
        "website_pages",
        "date, website, url, clicks, impressions, ctr, position",
        filters=[("eq", ("website", website)), ("gte", ("date", cutoff))],
        order=("date", True),
        limit=5000,
        label=f"zombie_pages_{website}",
    )
    grouped = defaultdict(lambda: {"url": "", "impressions": 0, "clicks": 0, "best_position": None})
    for row in rows:
        url = row.get("url")
        if not url:
            continue
        item = grouped[url]
        item["url"] = url
        item["impressions"] += int(_num(row.get("impressions")))
        item["clicks"] += int(_num(row.get("clicks")))
        position = row.get("position")
        if position is not None:
            pos = float(_num(position))
            item["best_position"] = pos if item["best_position"] is None else min(item["best_position"], pos)

    zombies = []
    for item in grouped.values():
        if item["impressions"] > 500 and item["clicks"] < 10 and (
            item["best_position"] is None or item["best_position"] >= 20
        ):
            impressions = item["impressions"]
            clicks = item["clicks"]
            zombies.append({
                "url": item["url"],
                "impressions": impressions,
                "clicks": clicks,
                "ctr_pct": round((clicks / impressions) * 100, 2) if impressions else 0,
            })

    zombies.sort(key=lambda row: row["impressions"], reverse=True)
    return zombies[:5]


def detect_cannibalization(website):
    """
    Find keywords represented by multiple ranking URLs.

    website_keywords has no URL field in the current schema, so use
    website_pages.top_keyword as the page-level keyword source.
    """
    cutoff = (date.today() - timedelta(days=90)).isoformat()
    rows = safe_query(
        "website_pages",
        "date, website, url, top_keyword, position, clicks",
        filters=[("eq", ("website", website)), ("gte", ("date", cutoff))],
        order=("date", True),
        limit=5000,
        label=f"cannibalization_{website}",
    )
    latest = _latest_by_key(rows, "url")
    by_keyword = defaultdict(dict)
    for row in latest.values():
        keyword = clean_text(row.get("top_keyword"))
        url = row.get("url")
        position = row.get("position")
        if not keyword or not url or position is None or float(_num(position, 999)) > 20:
            continue
        by_keyword[keyword][url] = {
            "url": url,
            "position": round(float(_num(position)), 1),
            "clicks": int(_num(row.get("clicks"))),
        }

    flagged = []
    for keyword, urls in by_keyword.items():
        if len(urls) >= 2:
            url_rows = sorted(urls.values(), key=lambda item: item["position"])
            flagged.append({
                "keyword": keyword,
                "urls": url_rows,
                "total_clicks": sum(item["clicks"] for item in url_rows),
            })

    flagged.sort(key=lambda row: row["total_clicks"], reverse=True)
    return flagged[:5]


def detect_authority_leaks(website):
    """
    Find high-authority backlink targets losing clicks.

    ahrefs_referring_domains is domain-level in the current schema. The available
    page-level high-DR backlink signal is ahrefs_broken_backlinks.target_url.
    """
    broken = safe_query(
        "ahrefs_broken_backlinks",
        "website, target_url, ref_domain_dr",
        filters=[("eq", ("website", website)), ("gte", ("ref_domain_dr", 40))],
        order=("ref_domain_dr", True),
        limit=1000,
        label=f"authority_backlinks_{website}",
    )
    high_dr_counts = defaultdict(int)
    for row in broken:
        target = row.get("target_url")
        if target:
            high_dr_counts[target] += 1

    candidate_urls = {url for url, count in high_dr_counts.items() if count >= 3}
    if not candidate_urls:
        return []

    today = date.today()
    week_ago = (today - timedelta(days=7)).isoformat()
    two_weeks_ago = (today - timedelta(days=14)).isoformat()
    page_rows = safe_query(
        "website_pages",
        "date, website, url, clicks",
        filters=[("eq", ("website", website)), ("gte", ("date", two_weeks_ago))],
        order=("date", True),
        limit=5000,
        label=f"authority_pages_{website}",
    )
    grouped = defaultdict(lambda: {"now": 0, "prev": 0})
    for row in page_rows:
        url = row.get("url")
        if url not in candidate_urls:
            continue
        bucket = "now" if str(row.get("date", "")) >= week_ago else "prev"
        grouped[url][bucket] += int(_num(row.get("clicks")))

    leaks = []
    for url, values in grouped.items():
        drop = _pct_change(values["now"], values["prev"])
        if drop <= -20:
            leaks.append({
                "url": url,
                "high_dr_backlinks_count": high_dr_counts[url],
                "clicks_drop_pct": abs(drop),
                "clicks_now": values["now"],
                "clicks_prev": values["prev"],
            })

    leaks.sort(key=lambda row: (row["high_dr_backlinks_count"], row["clicks_drop_pct"]), reverse=True)
    return leaks[:5]


def detect_competitor_steal(website):
    rows = safe_query(
        "ahrefs_competitors",
        "date, website, competitor_domain, keyword_overlap, share_pct, competitor_keywords",
        filters=[("eq", ("website", website))],
        order=("date", True),
        limit=200,
        label=f"competitor_steal_{website}",
    )
    latest = _latest_by_key(rows, "competitor_domain")
    competitors = []
    for row in latest.values():
        competitors.append({
            "competitor_domain": row.get("competitor_domain"),
            "share_pct": row.get("share_pct"),
            "keyword_overlap": row.get("keyword_overlap"),
        })
    competitors.sort(key=lambda row: _num(row.get("keyword_overlap")), reverse=True)
    return competitors[:3]


def detect_internal_link_gaps(website):
    rows = safe_query(
        "internal_linking_suggestions",
        "website, source_page, target_page, score, target_page_keyword, status",
        filters=[("eq", ("website", website)), ("eq", ("status", "pending")), ("gte", ("score", 70))],
        order=("score", True),
        limit=200,
        label=f"internal_link_gaps_{website}",
    )
    return {
        "pending_count": len(rows),
        "top_suggestions": [
            {
                "source_page": row.get("source_page"),
                "target_page": row.get("target_page"),
                "score": row.get("score"),
                "target_page_keyword": row.get("target_page_keyword"),
            }
            for row in rows[:3]
        ],
    }


def _metric_totals(rows, website):
    site_rows = _site_rows(rows, website)
    return {
        "gsc_clicks": sum(_num(r.get("gsc_clicks")) for r in site_rows),
        "gsc_impressions": sum(_num(r.get("gsc_impressions")) for r in site_rows),
        "ga_sessions": sum(_num(r.get("ga_sessions")) for r in site_rows),
        "gsc_position": (
            sum(_num(r.get("gsc_position")) for r in site_rows if r.get("gsc_position") is not None)
            / max(1, len([r for r in site_rows if r.get("gsc_position") is not None]))
        ),
    }


def classify_alert_diagnosis(alert, website=None, context=None, network_event_type=None):
    """Assign a named diagnosis type to an alert from metric combinations."""
    allowed = {
        "CTR Issue",
        "Ranking Loss",
        "Tracking / Engagement Issue",
        "Algorithm Update",
        "Site-Specific Issue",
        "SERP Layout Change",
    }
    existing = alert.get("diagnosis_type")
    if existing in allowed:
        return existing

    if network_event_type == "network_wide":
        return "Algorithm Update"

    text = " ".join(str(alert.get(key, "")) for key in ("type", "title", "what_happened", "why_it_matters", "root_cause_hypothesis")).lower()
    if "ctr" in text or ("click" in text and "impression" in text and ("stable" in text or "flat" in text)):
        return "CTR Issue"
    if "tracking" in text or "analytics" in text or "server" in text or "ga session" in text:
        return "Tracking / Engagement Issue"
    if alert.get("type") in {"rank_loss", "competitor_steal"} or "position" in text or "ranking" in text:
        return "Ranking Loss"

    if website and context:
        now = _metric_totals(context.get("daily_metrics_7d"), website)
        prev = _metric_totals(context.get("daily_metrics_prev_week"), website)
        click_change = _pct_change(now["gsc_clicks"], prev["gsc_clicks"])
        impression_change = _pct_change(now["gsc_impressions"], prev["gsc_impressions"])
        session_change = _pct_change(now["ga_sessions"], prev["ga_sessions"])
        if click_change < -10 and abs(impression_change) <= 5:
            return "CTR Issue"
        if click_change < -10 and impression_change < -10:
            return "Ranking Loss"
        if abs(click_change) <= 5 and session_change < -15:
            return "Tracking / Engagement Issue"

    return "Site-Specific Issue"


def detect_silent_decay(website):
    """Find gradual page/keyword decline over 60-90 days that may not trigger anomaly rules."""
    cutoff = (date.today() - timedelta(days=90)).isoformat()
    page_rows = safe_query(
        "website_pages",
        "date, website, url, clicks, impressions, position",
        filters=[("eq", ("website", website)), ("gte", ("date", cutoff))],
        order=("date", True),
        limit=5000,
        label=f"silent_decay_pages_{website}",
    )
    today = date.today()
    recent_cutoff = today - timedelta(days=30)
    previous_cutoff = today - timedelta(days=60)
    grouped_pages = defaultdict(lambda: {"url": "", "recent": 0, "previous": 0, "older": 0, "impressions_recent": 0})
    for row in page_rows:
        url = row.get("url")
        if not url:
            continue
        try:
            row_date = datetime.fromisoformat(str(row.get("date"))).date()
        except Exception:
            continue
        bucket = grouped_pages[url]
        bucket["url"] = url
        clicks = _num(row.get("clicks"))
        if row_date >= recent_cutoff:
            bucket["recent"] += clicks
            bucket["impressions_recent"] += _num(row.get("impressions"))
        elif row_date >= previous_cutoff:
            bucket["previous"] += clicks
        else:
            bucket["older"] += clicks

    page_findings = []
    for row in grouped_pages.values():
        if row["previous"] < SILENT_DECAY_MIN_PREVIOUS_CLICKS:
            continue
        decline_pct = abs(_pct_change(row["recent"], row["previous"]))
        older_ok = row["older"] == 0 or row["previous"] <= row["older"] * 1.2
        if decline_pct >= SILENT_DECAY_MIN_DECLINE_PCT and row["recent"] < row["previous"] and older_ok:
            page_findings.append({
                "type": "page",
                "url": row["url"],
                "clicks_recent_30d": int(row["recent"]),
                "clicks_previous_30d": int(row["previous"]),
                "decline_pct": round(decline_pct, 1),
                "impressions_recent_30d": int(row["impressions_recent"]),
                "action": "Refresh the page and add internal links before the decline becomes a sharp drop.",
            })
    page_findings.sort(key=lambda item: (item["decline_pct"], item["clicks_previous_30d"]), reverse=True)

    keyword_rows = safe_query(
        "website_keywords",
        "date, website, keyword, clicks, impressions, position",
        filters=[("eq", ("website", website)), ("gte", ("date", cutoff))],
        order=("date", True),
        limit=5000,
        label=f"silent_decay_keywords_{website}",
    )
    grouped_keywords = defaultdict(list)
    for row in keyword_rows:
        kw = clean_text(row.get("keyword"))
        if kw and row.get("position") is not None:
            grouped_keywords[kw].append(row)

    keyword_findings = []
    for keyword, rows in grouped_keywords.items():
        rows = sorted(rows, key=lambda r: str(r.get("date", "")))
        if len(rows) < 4:
            continue
        first = rows[: max(2, len(rows) // 3)]
        last = rows[-max(2, len(rows) // 3):]
        first_pos = sum(_num(r.get("position")) for r in first) / len(first)
        last_pos = sum(_num(r.get("position")) for r in last) / len(last)
        if last_pos - first_pos >= SILENT_DECAY_MIN_POSITION_DRIFT and last_pos <= 30:
            keyword_findings.append({
                "type": "keyword",
                "keyword": keyword,
                "position_start": round(first_pos, 1),
                "position_now": round(last_pos, 1),
                "position_loss": round(last_pos - first_pos, 1),
                "action": "Refresh the ranking page and add supporting internal links this week.",
            })
    keyword_findings.sort(key=lambda item: item["position_loss"], reverse=True)
    return {"pages": page_findings[:5], "keywords": keyword_findings[:5]}


def detect_dead_pages(website):
    """Split low-value pages into visible-but-unclicked and invisible groups."""
    cutoff = (date.today() - timedelta(days=DEAD_PAGE_LOOKBACK_DAYS)).isoformat()
    rows = safe_query(
        "website_pages",
        "date, website, url, clicks, impressions, position, top_keyword",
        filters=[("eq", ("website", website)), ("gte", ("date", cutoff))],
        order=("date", True),
        limit=5000,
        label=f"dead_pages_{website}",
    )
    grouped = defaultdict(lambda: {"url": "", "clicks": 0, "impressions": 0, "best_position": None, "top_keyword": ""})
    for row in rows:
        url = row.get("url")
        if not url:
            continue
        item = grouped[url]
        item["url"] = url
        item["clicks"] += int(_num(row.get("clicks")))
        item["impressions"] += int(_num(row.get("impressions")))
        if row.get("top_keyword") and not item["top_keyword"]:
            item["top_keyword"] = clean_text(row.get("top_keyword"))
        if row.get("position") is not None:
            pos = _num(row.get("position"), 999)
            item["best_position"] = pos if item["best_position"] is None else min(item["best_position"], pos)

    visible_unclicked = []
    invisible = []
    for item in grouped.values():
        row = {
            "url": item["url"],
            "impressions": item["impressions"],
            "clicks": item["clicks"],
            "ctr_pct": round((item["clicks"] / item["impressions"]) * 100, 2) if item["impressions"] else 0,
            "best_position": item["best_position"],
            "top_keyword": item["top_keyword"],
        }
        if item["impressions"] >= DEAD_PAGE_VISIBLE_MIN_IMPRESSIONS and item["clicks"] <= DEAD_PAGE_VISIBLE_MAX_CLICKS:
            row["suggested_action"] = "Rewrite title/meta and compare the SERP above this result."
            visible_unclicked.append(row)
        elif item["impressions"] == 0 and item["clicks"] == 0:
            row["suggested_action"] = "Check indexation; redirect, consolidate, or remove if there is no demand."
            invisible.append(row)

    visible_unclicked.sort(key=lambda item: item["impressions"], reverse=True)
    invisible.sort(key=lambda item: str(item["url"]))
    return {
        "visible_unclicked_count": len(visible_unclicked),
        "invisible_count": len(invisible),
        "total_dead_pages": len(visible_unclicked) + len(invisible),
        "visible_unclicked": visible_unclicked[:10],
        "invisible": invisible[:10],
    }


def classify_network_event(site_reports):
    declining_sites = [
        site for site, report in site_reports.items()
        if report.get("health_direction") == "declining" or any(
            (alert.get("diagnosis_type") in {"Ranking Loss", "CTR Issue", "SERP Layout Change"})
            for alert in report.get("critical_alerts", [])
        )
    ]
    improving_sites = [site for site, report in site_reports.items() if report.get("health_direction") == "improving"]
    if len(declining_sites) >= 3:
        return {
            "network_event_type": "network_wide",
            "network_event_label": "Algorithm Update Detected",
            "network_event_summary": "Three or more sites show simultaneous search visibility pressure. Monitor for 48-72 hours before making broad content changes.",
            "affected_sites": declining_sites,
            "network_event_action": "Hold large content edits, validate tracking, and monitor rankings for 48-72 hours.",
        }
    if 1 <= len(declining_sites) <= 2:
        return {
            "network_event_type": "site_specific",
            "network_event_label": "Site-Specific Issue",
            "network_event_summary": f"Visibility pressure is isolated to {', '.join(declining_sites)}.",
            "affected_sites": declining_sites,
            "network_event_action": "Prioritize the affected site's deep dive and technical checks.",
        }
    if improving_sites and declining_sites:
        return {
            "network_event_type": "mixed_movement",
            "network_event_label": "Mixed Movement",
            "network_event_summary": "Sites are moving in opposite directions, which points to competitive displacement rather than one network-wide algorithm event.",
            "affected_sites": declining_sites + improving_sites,
            "network_event_action": "Compare winning site patterns against declining properties and transfer what is working.",
        }
    return {
        "network_event_type": "stable",
        "network_event_label": "Network Stable",
        "network_event_summary": "No synchronized multi-site pressure detected this week.",
        "affected_sites": [],
        "network_event_action": "Continue planned priority work.",
    }


def build_site_context(website, context):
    sections = [f"=== SITE: {website} ==="]

    daily = _site_rows(context.get("daily_metrics_7d"), website)
    if daily:
        clicks = sum(int(_num(r.get("gsc_clicks"))) for r in daily)
        impressions = sum(int(_num(r.get("gsc_impressions"))) for r in daily)
        sessions = sum(int(_num(r.get("ga_sessions"))) for r in daily)
        sections.append(
            "\n=== DAILY METRICS 7D ===\n"
            f"days={len(daily)}, gsc_clicks={clicks}, gsc_impressions={impressions}, ga_sessions={sessions}\n"
            + "\n".join(json.dumps(r, default=str) for r in daily[-7:])
        )
    else:
        sections.append("\n=== DAILY METRICS 7D ===\nNo rows available")

    anomalies = _site_rows(context.get("anomalies_7d"), website)
    sections.append("\n=== ANOMALIES 7D ===")
    sections.extend(json.dumps(r, default=str) for r in anomalies[:10]) if anomalies else sections.append("None")

    kw = context.get("keyword_movers", {})
    kw_losers = [m for m in kw.get("losers", []) if m.get("website") == website]
    kw_gainers = [m for m in kw.get("gainers", []) if m.get("website") == website]
    sections.append("\n=== KEYWORD MOVERS ===")
    sections.append("LOSERS:")
    sections.extend(json.dumps(m, default=str) for m in kw_losers[:10]) if kw_losers else sections.append("None")
    sections.append("GAINERS:")
    sections.extend(json.dumps(m, default=str) for m in kw_gainers[:10]) if kw_gainers else sections.append("None")

    pages = context.get("page_movers", {})
    falling = [m for m in pages.get("falling", []) if m.get("website") == website]
    rising = [m for m in pages.get("rising", []) if m.get("website") == website]
    sections.append("\n=== PAGE MOVERS ===")
    sections.append("FALLING:")
    sections.extend(json.dumps(m, default=str) for m in falling[:10]) if falling else sections.append("None")
    sections.append("RISING:")
    sections.extend(json.dumps(m, default=str) for m in rising[:10]) if rising else sections.append("None")

    for label, rows in [
        ("AHREFS OVERVIEW", _site_rows(context.get("ahrefs_overview"), website)),
        ("COMPETITORS", _site_rows(context.get("competitors"), website)),
        ("NEW HIGH-DR BACKLINKS", _site_rows(context.get("new_backlinks"), website)),
        ("BROKEN BACKLINKS", _site_rows(context.get("broken_backlinks"), website)),
        ("CONTENT GAP EASY WINS", _site_rows(context.get("easy_wins"), website)),
    ]:
        sections.append(f"\n=== {label} ===")
        sections.extend(json.dumps(r, default=str) for r in rows[:10]) if rows else sections.append("None")

    sections.append("\n=== BASELINES ===")
    sections.append(json.dumps(context.get("baselines", {}).get(website, {}), default=str))

    enrichment_blocks = [
        ("ZOMBIE PAGES", detect_zombie_pages(website)),
        ("CANNIBALIZATION", detect_cannibalization(website)),
        ("AUTHORITY LEAKS", detect_authority_leaks(website)),
        ("COMPETITOR STEAL SIGNALS", detect_competitor_steal(website)),
        ("SILENT DECAY", detect_silent_decay(website)),
        ("DEAD PAGES", detect_dead_pages(website)),
    ]
    for label, rows in enrichment_blocks:
        sections.append(f"\n=== {label} ===")
        if isinstance(rows, dict):
            sections.append(json.dumps(rows, default=str))
        elif rows:
            sections.extend(json.dumps(r, default=str) for r in rows)
        else:
            sections.append("None")

    sections.append("\n=== INTERNAL LINK GAPS ===")
    sections.append(json.dumps(detect_internal_link_gaps(website), default=str))

    site_context = "\n".join(sections)
    if len(site_context) > 8000:
        site_context = site_context[:8000] + "\n\n[...site context truncated at 8000 chars]"
    return site_context


# ══════════════════════════════════════════════════════════════
#  AI ANALYSIS — Enhanced GPT prompt
# ══════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """You are a world-class SEO strategist and data analyst working for Ztudium, 
a media company operating 9 websites: CitiesABC, BusinessABC, HedgeThink, FashionABC, TradersDNA, 
FreedomX, Wisdomia, SportsDNA, and IntelligentHQ.

You analyze dashboard data and generate 4-6 strategic insights that are SPECIFIC, ACTIONABLE, and 
QUANTIFIED. You have access to keyword-level data, page-level data, competitor data, and historical baselines.

CRITICAL REQUIREMENTS:

1. ROOT CAUSE ANALYSIS — Always explain WHY, not just WHAT
   BAD: "Traffic dropped 60%"
   GOOD: "Traffic dropped 60% driven by ranking losses on 3 high-volume keywords: 
          'enterprise AI' (position 3→15, -2.1K clicks), 'AI implementation' (position 2→12, -1.8K clicks)"

2. SPECIFIC PAGE/KEYWORD IDENTIFICATION — Cite exact keywords and pages from the data
   BAD: "Review recent content"
   GOOD: "Pages affected: /enterprise-guide (-1,200 clicks), /ai-tools (-800 clicks)"

3. QUANTIFIED IMPACT ESTIMATES — Use actual numbers from the data
   BAD: "Could improve traffic significantly"
   GOOD: "Reclaiming top 3 for these keywords = ~4,900 monthly clicks based on volume data"

4. COMPETITIVE CONTEXT — Reference competitor data when applicable
   Include which competitor domains are ranking, any new high-DR backlinks gained

5. HISTORICAL SEVERITY — Compare current metrics to 30/90-day baselines provided
   Example: "Worst daily clicks in 67 days" or "DR at 3-month high"

6. ACTIONABLE NEXT STEPS — Concrete, time-bound, specific
   BAD: "Optimize content"
   GOOD: "Publish updated guide within 48 hours. Include comparison table, FAQ schema markup.
          Target featured snippet for 'enterprise AI implementation'."

7. CROSS-SITE PATTERNS — If anomalies appear on 3+ sites simultaneously, flag as potential 
   algorithm update and recommend industry-wide response.

INSIGHT CATEGORIES (use exactly these):
- "urgent" — Traffic drops, ranking losses, broken link spikes, DR drops
- "momentum" — DR increases, traffic surges, keyword jumps, authority boosts
- "backlink" — High-DR broken link targets, competitor backlink gaps, new quality links
- "content_gap" — Easy-win keywords, featured snippet opportunities
- "ai_visibility" — AI citation changes, viral content detection

RESPOND WITH VALID JSON ONLY. No markdown, no code fences. Format:
{
  "insights": [
    {
      "category": "urgent|momentum|backlink|content_gap|ai_visibility",
      "severity": "high|medium|low",
      "title": "Short headline (max 12 words)",
      "analysis": "2-4 sentences with SPECIFIC numbers, keywords, and pages from the data",
      "action": "Concrete action with specific targets, timelines, and deliverables",
      "impact": "Quantified expected outcome using actual metric values from data",
      "related_website": "WebsiteName or 'all'"
    }
  ]
}

RULES:
- Generate 4-6 insights, ordered by severity (high first)
- Every insight MUST cite specific numbers from the provided data
- Never use vague phrases like "optimize content", "improve SEO", "conduct an audit"
- If data is limited, be transparent about it but still provide the best analysis possible
- Cross-reference anomalies with keyword/page movers to provide root cause analysis"""


def generate_insights(context):
    """Send prioritized context to GPT-4o-mini and get structured insights."""
    context_str = summarize_context(context)
    _ensure_gemini_priority("Legacy strategic insights should run on Gemini 3.1 Pro Preview")

    # Use higher token limit to accommodate richer analysis
    max_context = 20000
    if len(context_str) > max_context:
        context_str = context_str[:max_context] + "\n\n[...data truncated for token limits]"

    logger.info(f"  Sending {len(context_str)} chars of context for analysis")

    def _run_legacy_call(context_text, max_tokens=6000):
        return _call_gemini_chat_completion(
            temperature=0.3,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": (
                    "Analyze this comprehensive SEO dashboard data. The data is organized by priority: "
                    "anomalies first, then keyword/page changes, then baselines and context.\n\n"
                    f"{context_text}"
                )},
            ],
        )

    try:
        with Spinner("Analyzing strategic insights"):
            response = _call_gemini_chat_completion(
                temperature=0.3,
                max_tokens=6000,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": (
                        "Analyze this comprehensive SEO dashboard data. The data is organized by priority: "
                        "anomalies first, then keyword/page changes, then baselines and context.\n\n"
                        f"{context_str}"
                    )},
                ],
            )

        try:
            parsed = _json_response_content(response)
            if isinstance(parsed, dict):
                if "insights" in parsed and isinstance(parsed["insights"], list):
                    insights = parsed["insights"]
                else:
                    # Fallback: take the first list found in the dict
                    insights = next((v for v in parsed.values() if isinstance(v, list)), [])
            else:
                insights = parsed
            
            if not isinstance(insights, list):
                insights = [insights]
        except json.JSONDecodeError as e:
            logger.warning(f"  Legacy insights parse failed: {e}. Retrying with compact context.")
            retry_response = _run_legacy_call(context_str[:12000], max_tokens=6000)
            try:
                parsed = _json_response_content(retry_response)
                if isinstance(parsed, dict):
                    insights = parsed.get("insights") if isinstance(parsed.get("insights"), list) else next((v for v in parsed.values() if isinstance(v, list)), [])
                else:
                    insights = parsed
                if not isinstance(insights, list):
                    insights = [insights]
            except json.JSONDecodeError as retry_error:
                logger.error(f"  Failed to parse analysis response: {retry_error}")
                raw = retry_response.choices[0].message.content or ""
                logger.error(f"  Raw response preview: {_extract_json_text(raw)[:300]}...")
                return [{
                    "category": "urgent",
                    "severity": "low",
                    "title": "Legacy insight summary pending",
                    "analysis": "The v2 intelligence report was generated successfully, but the legacy compatibility summary needs a rerun.",
                    "action": "Use the Strategic Insights v2 sections for this week and rerun the legacy summary if required.",
                    "impact": "No impact on v2 AI Insights or PDF reporting.",
                    "related_website": "all",
                }]

        # Sort by severity
        severity_order = {"high": 0, "medium": 1, "low": 2}
        insights.sort(key=lambda x: severity_order.get(x.get("severity", "low"), 2))

        logger.info(f"  Generated {len(insights)} insights")
        return insights

    except json.JSONDecodeError as e:
        logger.error(f"  Failed to parse analysis response: {e}")
        logger.error(f"  Raw response: {_extract_json_text(response.choices[0].message.content or '')[:500]}")
        return [{
            "category": "urgent",
            "severity": "low",
            "title": "Insight generation encountered a parsing issue",
            "analysis": "Analysis ran but the response format was unexpected. This is typically temporary.",
            "action": "Re-run the insight generation script. If this persists, check OPENAI_API_KEY or GEMINI_API_KEY.",
            "impact": "No impact on dashboard functionality.",
            "related_website": "all"
        }]
    except Exception as e:
        logger.error(f"  Insights API call failed: {e}")
        return [{
            "category": "urgent",
            "severity": "low",
            "title": "Insights generation failed",
            "analysis": f"The analysis API call returned an error: {str(e)[:200]}",
            "action": "Check OPENAI_API_KEY or GEMINI_API_KEY is valid and has credits. Re-run the script.",
            "impact": "No insights generated for today. Previous insights remain visible.",
            "related_website": "all"
        }]


# ══════════════════════════════════════════════════════════════
#  TOPIC-BASED KEYWORD CLUSTERING (v4)
# ══════════════════════════════════════════════════════════════

def _fallback_site_report(website, reason="Insufficient data for this week's analysis."):
    return {
        "site": website,
        "health_score": 50,
        "health_direction": "stable",
        "headline": "Data analysis incomplete",
        "critical_alerts": [],
        "opportunities": [],
        "momentum_signals": [],
        "silent_decay_alerts": [],
        "dead_pages_summary": {
            "visible_unclicked_count": 0,
            "invisible_count": 0,
            "total_dead_pages": 0,
            "visible_unclicked": [],
            "invisible": [],
        },
        "limited_ai_insight": True,
        "ai_retry_pending": True,
        "ai_model_used": "fallback",
        "week_summary": reason,
    }


def _normalize_site_report(website, report):
    if not isinstance(report, dict):
        return _fallback_site_report(website)
    report["site"] = website
    report["health_score"] = max(0, min(100, int(_num(report.get("health_score"), 50))))
    if report.get("health_direction") not in {"improving", "stable", "declining"}:
        report["health_direction"] = "stable"
    report["headline"] = clean_text(report.get("headline")) or "Data analysis incomplete"
    report["critical_alerts"] = report.get("critical_alerts") if isinstance(report.get("critical_alerts"), list) else []
    report["opportunities"] = report.get("opportunities") if isinstance(report.get("opportunities"), list) else []
    report["momentum_signals"] = report.get("momentum_signals") if isinstance(report.get("momentum_signals"), list) else []
    report["silent_decay_alerts"] = report.get("silent_decay_alerts") if isinstance(report.get("silent_decay_alerts"), list) else []
    report["dead_pages_summary"] = report.get("dead_pages_summary") if isinstance(report.get("dead_pages_summary"), dict) else {
        "visible_unclicked_count": 0,
        "invisible_count": 0,
        "total_dead_pages": 0,
        "visible_unclicked": [],
        "invisible": [],
    }
    report["critical_alerts"] = report["critical_alerts"][:3]
    report["opportunities"] = report["opportunities"][:3]
    report["momentum_signals"] = report["momentum_signals"][:2]
    report["silent_decay_alerts"] = report["silent_decay_alerts"][:5]
    report["week_summary"] = clean_text(report.get("week_summary")) or "Insufficient data for this week's analysis."
    report["limited_ai_insight"] = bool(report.get("limited_ai_insight", False))
    report["ai_retry_pending"] = bool(report.get("ai_retry_pending", False))
    report["ai_model_used"] = clean_text(report.get("ai_model_used")) or ""
    for alert in report["critical_alerts"]:
        alert["estimated_traffic_impact"] = int(_num(alert.get("estimated_traffic_impact")))
        alert["diagnosis_type"] = classify_alert_diagnosis(alert, website=website)
    for opportunity in report["opportunities"]:
        opportunity["estimated_traffic_gain"] = int(_num(opportunity.get("estimated_traffic_gain")))
    return report


def _analyze_single_site(website_name, site_context):
    system_prompt = (
        "You are a senior SEO analyst reviewing one website's weekly performance data for Ztudium, "
        "a media company. You produce precise, quantified, analyst-grade intelligence reports. Every "
        "insight must cite specific numbers from the data. Never use vague language. Always name exact "
        "pages, keywords, and metrics."
    )
    def build_user_prompt(context_text):
        return f"""Site: {website_name}
Week of: {date.today().isoformat()}

DATA:
{context_text}

Generate a SITE INTELLIGENCE REPORT. Respond with VALID JSON ONLY. No markdown, no code fences.

{{
  "site": "{website_name}",
  "health_score": <integer 0-100>,
  "health_direction": "<improving|stable|declining>",
  "headline": "<one sentence, max 15 words, biggest story this week>",
  "critical_alerts": [
    {{
      "type": "<traffic_drop|rank_loss|competitor_steal|authority_leak|broken_backlinks>",
      "diagnosis_type": "<CTR Issue|Ranking Loss|Tracking / Engagement Issue|Algorithm Update|Site-Specific Issue|SERP Layout Change>",
      "title": "<max 10 words>",
      "what_happened": "<specific numbers, pages, keywords, exact deltas>",
      "why_it_matters": "<business impact, estimated sessions or clicks lost>",
      "root_cause_hypothesis": "<most likely cause based on available data signals>",
      "action": "<specific action, target page or keyword, timeframe>",
      "estimated_traffic_impact": <integer monthly clicks>
    }}
  ],
  "opportunities": [
    {{
      "type": "<keyword_gap|zombie_page|internal_link|cannibalization|content_refresh>",
      "title": "<short headline>",
      "insight": "<what data shows, specific pages or keywords>",
      "action": "<concrete next step>",
      "estimated_traffic_gain": <integer monthly clicks>
    }}
  ],
  "momentum_signals": [
    {{
      "title": "<what is winning>",
      "detail": "<specific metrics showing growth>"
    }}
  ],
  "silent_decay_alerts": [
    {{
      "type": "<page|keyword>",
      "title": "<slow decline headline>",
      "detail": "<specific gradual decline metrics>",
      "action": "<specific action>"
    }}
  ],
  "dead_pages_summary": {{
    "visible_unclicked_count": <integer>,
    "invisible_count": <integer>,
    "total_dead_pages": <integer>
  }},
  "week_summary": "<2-3 sentence analyst narrative>"
}}

Rules:
- Maximum 3 critical_alerts, maximum 3 opportunities, maximum 2 momentum_signals
- Maximum 3 silent_decay_alerts
- Every critical_alert must include diagnosis_type exactly from the allowed diagnosis list
- health_score: 70+ healthy, 50-69 needs attention, below 50 critical
- Every field must use numbers from the input data
- If data is sparse for this site, say so explicitly but still provide best analysis
- Never say "optimize content" - say exactly what to do"""
    def _run_single_site_call(context_text):
        return _call_gemini_chat_completion(
            temperature=0.2,
            max_tokens=4000,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": build_user_prompt(context_text)},
            ],
        )

    try:
        response = _run_single_site_call(site_context)
        try:
            report = _normalize_site_report(website_name, _json_response_content(response))
            report["ai_model_used"] = response_model_used(response)
            report["ai_provider_used"] = response_provider_used(response)
            report["limited_ai_insight"] = False
            report["ai_retry_pending"] = False
            return report
        except json.JSONDecodeError as parse_error:
            raw = response.choices[0].message.content or ""
            if "unterminated string" in str(parse_error).lower():
                logger.warning(f"  Retrying {website_name} with reduced context after truncation")
                retry_response = _run_single_site_call(site_context[:5000])
                try:
                    report = _normalize_site_report(website_name, _json_response_content(retry_response))
                    report["ai_model_used"] = response_model_used(retry_response)
                    report["ai_provider_used"] = response_provider_used(retry_response)
                    report["limited_ai_insight"] = False
                    report["ai_retry_pending"] = False
                    return report
                except json.JSONDecodeError as retry_error:
                    logger.error(f"  V2 site analysis failed for {website_name}: {retry_error}")
                    logger.error(f"  Raw response preview: {_extract_json_text(retry_response.choices[0].message.content or '')[:500]}")
                    return _fallback_site_report(website_name, "Limited AI insight - retry pending.")
            logger.error(f"  V2 site analysis failed for {website_name}: {parse_error}")
            logger.error(f"  Raw response preview: {_extract_json_text(raw)[:500]}")
            return _fallback_site_report(website_name, "Limited AI insight - retry pending.")
    except Exception as e:
        logger.error(f"  V2 site analysis failed for {website_name} at {datetime.now(tz=timezone.utc).isoformat()}: {e}")
        return _fallback_site_report(website_name, "Limited AI insight - retry pending.")


def _fallback_network_report(site_reports):
    scores = [int(_num(report.get("health_score"), 50)) for report in site_reports.values()]
    mean_score = round(sum(scores) / len(scores)) if scores else 50
    priority = min(site_reports.items(), key=lambda item: _num(item[1].get("health_score"), 50))[0] if site_reports else SITES[0]
    winner = max(site_reports.items(), key=lambda item: _num(item[1].get("health_score"), 50))[0] if site_reports else SITES[0]
    return {
        "network_health_score": mean_score,
        "network_trend": "stable",
        "algorithm_update_detected": False,
        "algorithm_update_confidence": "none",
        "algorithm_update_explanation": "",
        "top_priority_site": priority,
        "top_priority_reason": f"{priority} has the lowest current health score in the available v2 reports.",
        "network_winner": winner,
        "network_winner_reason": f"{winner} has the highest current health score in the available v2 reports.",
        "cross_site_patterns": [],
        "network_event_type": "stable",
        "network_event_label": "Network Stable",
        "network_event_summary": "No synchronized multi-site pressure detected this week.",
        "affected_sites": [],
        "network_event_action": "Continue planned priority work.",
        "resource_allocation_recommendation": f"Start with {priority}; review its alerts and highest-impact opportunities this week.",
        "limited_ai_insight": True,
        "ai_retry_pending": True,
        "ai_model_used": "fallback",
        "network_summary": "Weekly intelligence analysis is processing. Check back shortly for the full network summary.",
    }


def _analyze_network(site_reports):
    def build_site_summaries(compact=False):
        lines = []
        for site, report in site_reports.items():
            headline = clean_text(report.get("headline"))
            if compact:
                headline = headline[:80]
            lines.append(
                f"{site}: health_score={report.get('health_score')}, health_direction={report.get('health_direction')}, "
                f"headline={headline}, critical_alerts={len(report.get('critical_alerts', []))}, "
                f"opportunities={len(report.get('opportunities', []))}"
            )
        return "\n".join(lines)

    def build_user_prompt(all_site_summaries, compact=False):
        compact_rule = '\n- Keep every string concise; prefer one sentence per field when possible' if compact else ""
        return f"""Sites: {', '.join(SITES)}

WEEKLY SITE SUMMARIES:
{all_site_summaries}

Generate a NETWORK INTELLIGENCE REPORT. Respond with VALID JSON ONLY. No markdown, no code fences.

{{
  "network_health_score": <integer 0-100, weighted average of site health scores>,
  "network_trend": "<improving|stable|declining>",
  "algorithm_update_detected": <true|false>,
  "algorithm_update_confidence": "<high|medium|low|none>",
  "algorithm_update_explanation": "<if detected: which sites, what pattern, recommended response. If not: empty string>",
  "top_priority_site": "<site needing most urgent attention>",
  "top_priority_reason": "<one sentence why>",
  "network_winner": "<best performing site this week>",
  "network_winner_reason": "<one sentence why>",
  "cross_site_patterns": [
    {{
      "pattern": "<description of trend appearing on 3+ sites>",
      "sites_affected": ["Site1", "Site2"],
      "interpretation": "<what this likely means>",
      "recommended_response": "<network-wide action>"
    }}
  ],
  "network_event_type": "<network_wide|site_specific|mixed_movement|stable>",
  "network_event_label": "<Algorithm Update Detected|Site-Specific Issue|Mixed Movement|Network Stable>",
  "network_event_summary": "<one sentence explaining what the multi-site movement means>",
  "affected_sites": ["Site1"],
  "network_event_action": "<specific monitor/change recommendation>",
  "resource_allocation_recommendation": "<where should the team focus this week for max ROI, specific and actionable>",
  "network_summary": "<3-4 sentence executive briefing>"
}}

Rules:
- algorithm_update_detected = true only if 3+ sites show simultaneous drops in same metrics
- cross_site_patterns only if pattern appears on 3+ sites
- Be specific about site names, never vague{compact_rule}"""

    all_site_summaries = build_site_summaries()
    system_prompt = (
        "You are a chief SEO strategist reviewing 9 websites owned by the same media company "
        "(Ztudium). You identify cross-site patterns, algorithm signals, and network-wide priorities "
        "that no single site analyst would see. Produce precise, strategic, executive-level analysis."
    )
    try:
        def _run_network_call(compact=False):
            return _call_gemini_chat_completion(
                temperature=0.2,
                max_tokens=5000,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": build_user_prompt(build_site_summaries(compact=compact), compact=compact)},
                ],
            )

        response = _run_network_call()
        response_used = response
        try:
            report = _json_response_content(response)
        except json.JSONDecodeError as parse_error:
            if "unterminated string" in str(parse_error).lower():
                logger.warning("  Retrying network analysis with compact site summaries after truncation")
                retry_response = _run_network_call(compact=True)
                try:
                    report = _json_response_content(retry_response)
                    response_used = retry_response
                except json.JSONDecodeError as retry_error:
                    logger.error(f"  V2 network analysis failed: {retry_error}")
                    logger.error(f"  Raw response preview: {_extract_json_text(retry_response.choices[0].message.content or '')[:500]}")
                    return _fallback_network_report(site_reports)
            else:
                logger.error(f"  V2 network analysis failed: {parse_error}")
                logger.error(f"  Raw response preview: {_extract_json_text(response.choices[0].message.content or '')[:500]}")
                return _fallback_network_report(site_reports)
        fallback = _fallback_network_report(site_reports)
        if not isinstance(report, dict):
            return fallback
        merged = {**fallback, **report}
        merged["network_health_score"] = max(0, min(100, int(_num(merged.get("network_health_score"), fallback["network_health_score"]))))
        if merged.get("network_trend") not in {"improving", "stable", "declining"}:
            merged["network_trend"] = "stable"
        if merged.get("algorithm_update_confidence") not in {"high", "medium", "low", "none"}:
            merged["algorithm_update_confidence"] = "none"
        if not isinstance(merged.get("cross_site_patterns"), list):
            merged["cross_site_patterns"] = []
        merged["limited_ai_insight"] = False
        merged["ai_retry_pending"] = False
        merged["ai_model_used"] = response_model_used(response_used)
        merged["ai_provider_used"] = response_provider_used(response_used)
        return merged
    except Exception as e:
        logger.error(f"  V2 network analysis failed: {e}")
        return _fallback_network_report(site_reports)


def _enrich_site_reports(site_reports, context):
    network_event = classify_network_event(site_reports)
    for site in SITES:
        report = site_reports.get(site) or _fallback_site_report(site)
        silent_decay = detect_silent_decay(site)
        dead_pages = detect_dead_pages(site)
        report["silent_decay_alerts"] = [
            {
                "type": item.get("type", "page"),
                "title": (
                    f"Slow decline on {item.get('url', item.get('keyword', 'asset'))}"[:90]
                    if item.get("type") == "page"
                    else f"Keyword position decay: {item.get('keyword', 'keyword')}"[:90]
                ),
                "detail": (
                    f"Clicks fell {item.get('decline_pct')}% from {item.get('clicks_previous_30d')} to {item.get('clicks_recent_30d')} over the latest 30-day window."
                    if item.get("type") == "page"
                    else f"Average position moved from {item.get('position_start')} to {item.get('position_now')} over 60+ days."
                ),
                "action": item.get("action") or "Review the page and add supporting internal links.",
                **item,
            }
            for item in (silent_decay.get("pages", []) + silent_decay.get("keywords", []))[:5]
        ]
        report["dead_pages_summary"] = dead_pages
        for alert in report.get("critical_alerts", []):
            alert["diagnosis_type"] = classify_alert_diagnosis(
                alert,
                website=site,
                context=context,
                network_event_type=network_event.get("network_event_type"),
            )
        site_reports[site] = report
    return site_reports


def _alert_fingerprint(site, alert):
    base = "|".join([
        str(site or ""),
        str(alert.get("type") or ""),
        str(alert.get("diagnosis_type") or ""),
        str(alert.get("title") or ""),
    ]).lower()
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _parse_date(value, fallback=None):
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except Exception:
        return fallback or date.today()


def _track_alert_history(site_reports):
    """Persist alert age/recovery metadata without blocking report generation."""
    today = date.today()
    current_fingerprints = set()
    existing_by_fingerprint = {}
    tracking_available = True

    try:
        result = (
            supabase.table("ai_alert_tracking")
            .select("alert_fingerprint, site, first_seen, last_seen, recovery_status, last_impact, occurrences")
            .limit(2000)
            .execute()
        )
        existing_by_fingerprint = {
            row.get("alert_fingerprint"): row
            for row in (result.data or [])
            if row.get("alert_fingerprint")
        }
    except Exception as exc:
        tracking_available = False
        logger.warning(f"Alert history tracking unavailable; continuing without persisted recovery state: {exc}")

    rows_to_upsert = []
    for site, report in (site_reports or {}).items():
        alerts = report.get("critical_alerts") if isinstance(report.get("critical_alerts"), list) else []
        for alert in alerts:
            fingerprint = _alert_fingerprint(site, alert)
            current_fingerprints.add(fingerprint)
            existing = existing_by_fingerprint.get(fingerprint) or {}
            first_seen = _parse_date(existing.get("first_seen"), today)
            previous_impact = int(existing.get("last_impact") or 0)
            current_impact = int(_num(alert.get("estimated_traffic_impact"), 0))
            impact_delta = current_impact - previous_impact if existing else 0
            age_days = max(0, (today - first_seen).days)

            if not existing:
                recovery_status = "new"
            elif current_impact <= max(1, previous_impact * 0.75):
                recovery_status = "recovering"
            else:
                recovery_status = "active"

            alert["alert_fingerprint"] = fingerprint
            alert["first_seen"] = first_seen.isoformat()
            alert["last_seen"] = today.isoformat()
            alert["alert_age_days"] = age_days
            alert["recovery_status"] = recovery_status
            alert["previous_impact"] = previous_impact if existing else None
            alert["impact_delta"] = impact_delta

            if tracking_available:
                rows_to_upsert.append({
                    "alert_fingerprint": fingerprint,
                    "site": site,
                    "alert_type": alert.get("type"),
                    "diagnosis_type": alert.get("diagnosis_type"),
                    "title": alert.get("title"),
                    "first_seen": first_seen.isoformat(),
                    "last_seen": today.isoformat(),
                    "resolved_at": None,
                    "recovery_status": recovery_status,
                    "last_impact": current_impact,
                    "previous_impact": previous_impact if existing else None,
                    "impact_delta": impact_delta,
                    "occurrences": int(existing.get("occurrences") or 0) + 1,
                    "last_payload": alert,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })

    if not tracking_available:
        return site_reports

    try:
        if rows_to_upsert:
            supabase.table("ai_alert_tracking").upsert(rows_to_upsert, on_conflict="alert_fingerprint").execute()
        for fingerprint, row in existing_by_fingerprint.items():
            if fingerprint not in current_fingerprints and row.get("recovery_status") != "resolved":
                supabase.table("ai_alert_tracking").update({
                    "recovery_status": "resolved",
                    "resolved_at": today.isoformat(),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }).eq("alert_fingerprint", fingerprint).execute()
    except Exception as exc:
        logger.warning(f"Alert history update failed; reports were still generated: {exc}")

    return site_reports


def generate_v2_insights(context):
    """Run 9 site-level Gemini analyses plus one network meta-analysis."""
    _ensure_gemini_priority("AI Insights v2 requires Gemini")

    logger.info("  Building v2 site contexts")
    site_contexts = {site: build_site_context(site, context) for site in SITES}
    site_reports = {}

    with Spinner("Analyzing 9 site intelligence reports"):
        with ThreadPoolExecutor(max_workers=9) as executor:
            futures = {
                executor.submit(_analyze_single_site, site, site_contexts[site]): site
                for site in SITES
            }
            for future in as_completed(futures):
                site = futures[future]
                try:
                    site_reports[site] = future.result()
                except Exception as e:
                    logger.error(f"  V2 site worker failed for {site}: {e}")
                    site_reports[site] = _fallback_site_report(site)

    for site in SITES:
        if site not in site_reports:
            site_reports[site] = _fallback_site_report(site)

    site_reports = _enrich_site_reports(site_reports, context)
    site_reports = _track_alert_history(site_reports)

    with Spinner("Analyzing network intelligence report"):
        network_report = _analyze_network(site_reports)
    network_report = {**network_report, **classify_network_event(site_reports)}

    return site_reports, network_report


# Stop words / qualifiers to strip when extracting core topic from a longtail
CORE_TOPIC_STRIP = {
    "best", "top", "free", "cheap", "how", "to", "what", "is", "are",
    "why", "does", "can", "do", "the", "a", "an", "for", "in", "of",
    "vs", "versus", "and", "or", "with", "without", "near", "me",
    "online", "2024", "2025", "2026", "2027",
}

# Question prefixes for identifying question-format keywords
QUESTION_PREFIXES = ("how ", "what ", "why ", "is ", "does ", "can ", "do ", "are ", "which ", "where ", "when ", "should ")

RAW_URL_RE = re.compile(r"(https?://|www\.)", re.IGNORECASE)
HTML_ENTITY_RE = re.compile(r"&(?:[a-zA-Z]+|#\d+|#x[0-9a-fA-F]+);")
ALLOWED_KEYWORD_RE = re.compile(r"^[A-Za-z0-9\s&'\".,:;!?()/+\-_%#@$\[\]]+$")
ALLOWED_LABEL_RE = re.compile(r"^[A-Za-z0-9\s&'\".,:;!?()/+\-_%#@$\[\]]+$")
CONTROL_CHAR_RE = re.compile(r"[\u0000-\u001F\u007F-\u009F\uFFFD]")


def clean_text(value: str | None) -> str:
    """Normalize whitespace, entities, and control characters."""
    return re.sub(r"\s+", " ", CONTROL_CHAR_RE.sub(" ", html.unescape(str(value or "")))).strip()


def keyword_filter_reason(keyword: str | None) -> str | None:
    """Reject non-Latin and noisy keywords before they reach clustering."""
    text = clean_text(keyword)
    if not text:
        return "empty"

    compact = re.sub(r"\s+", "", text)
    if len(compact) < 3:
        return "too_short"
    if RAW_URL_RE.search(text):
        return "raw_url"
    if HTML_ENTITY_RE.search(text):
        return "html_entity"
    if not ALLOWED_KEYWORD_RE.fullmatch(text):
        return "non_latin_or_uncommon_chars"

    letter_count = len(re.findall(r"[A-Za-z]", text))
    if compact and (letter_count / len(compact)) < 0.5:
        return "low_letter_ratio"

    punctuation_count = len(re.findall(r"[^A-Za-z0-9\s]", text))
    if compact and (punctuation_count / len(compact)) > 0.35:
        return "excessive_special_chars"

    if re.search(r"([A-Za-z])\1{3,}", text):
        return "repeated_characters"

    return None


def is_valid_keyword(keyword: str | None) -> bool:
    return keyword_filter_reason(keyword) is None


def sanitize_keyword_record(row: dict) -> dict | None:
    """Return a clean keyword row or None if it should be excluded."""
    keyword = clean_text(row.get("keyword"))
    if not is_valid_keyword(keyword):
        return None

    cluster_label = clean_text(row.get("cluster"))
    if cluster_label and not ALLOWED_LABEL_RE.fullmatch(cluster_label):
        cluster_label = ""

    return {
        **row,
        "website": clean_text(row.get("website")),
        "keyword": keyword,
        "intent": clean_text(row.get("intent")) or "other",
        "cluster": cluster_label or None,
    }


def sanitize_cluster_label(label: str | None, primary_keyword: str, fallback: str | None = None) -> str:
    """Ensure cluster labels never contain invalid character noise."""
    text = clean_text(label)
    if text and ALLOWED_LABEL_RE.fullmatch(text):
        return text

    derived = clean_text(fallback) or extract_core_topic(primary_keyword).title() or primary_keyword.title()
    return derived if ALLOWED_LABEL_RE.fullmatch(derived) else primary_keyword.title()


def sanitize_hub_title(title: str | None, primary_keyword: str, cluster_topic: str) -> str:
    """Ensure stored hub titles are readable and Latin-only."""
    text = clean_text(title)
    if text and ALLOWED_LABEL_RE.fullmatch(text):
        return text
    fallback = f"{cluster_topic}: {primary_keyword.title()}"
    return fallback if ALLOWED_LABEL_RE.fullmatch(fallback) else primary_keyword.title()


def sanitize_narrative(text: str | None, fallback: str) -> str:
    """Keep freeform text readable while preventing non-Latin noise from being stored."""
    cleaned = clean_text(text)
    return cleaned if cleaned and ALLOWED_LABEL_RE.fullmatch(cleaned) else fallback


def extract_core_topic(longtail_keyword: str) -> str:
    """Extract the core head term from a longtail keyword by stripping qualifiers."""
    cleaned = clean_text(longtail_keyword).lower()
    words = cleaned.split()
    core = [w for w in words if w not in CORE_TOPIC_STRIP and not w.isdigit()]
    return " ".join(core) if core else cleaned


def find_question_keyword(keywords: list) -> str | None:
    """Find the best question-format keyword from a list of keyword dicts."""
    candidates = []
    for kw in keywords:
        kw_str = clean_text(kw.get("keyword")).lower()
        if not is_valid_keyword(kw_str):
            continue
        if any(kw_str.startswith(prefix) for prefix in QUESTION_PREFIXES):
            # Score by volume (prefer high volume question keywords)
            candidates.append((kw_str, kw.get("volume", 0)))
    if candidates:
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0][0]
    return None


def compute_opportunity_score(volume: int, kd: int) -> float:
    """Compute opportunity score: high volume + low difficulty = high score."""
    return min(9999.99, round(volume * (1 / (kd + 1)), 1))


CLUSTER_PROMPT = """You are an expert SEO content strategist. Analyze the keyword data below and group related keywords into thematic content clusters.

CRITICAL COVERAGE RULE:
- You MUST generate clusters for EVERY website provided in the data. Do NOT skip any website.
- Each website MUST have exactly 3 clusters.
- Each cluster must represent a genuinely DISTINCT topic theme — not variations of the same topic.

KEYWORD VOLUME RULE:
- Each cluster should have 1 primary keyword (highest volume) + ideally 10 to 20 related keywords
- IMPORTANT: If a website does not have enough qualifying keywords to fill 10-20, use AS MANY as are available from the data. Do NOT invent keywords.
- It is acceptable to have clusters with fewer than 10 related keywords if the data is limited.
- If there are not enough keywords at KD ≤ 5 / Volume ≥ 1000, include keywords with KD up to 10 or Volume down to 500.
- NEVER fabricate or invent keywords — only use keywords that appear in the provided data.

STRICT RULES:
1. ONLY use keywords that appear in the provided data — DO NOT invent or fabricate keywords under any circumstances
2. PREFER keywords with KD ≤ 5 and Volume ≥ 1000, but include KD ≤ 10 and Volume ≥ 500 when needed
3. If a website only has 15 total keywords, distribute them across 3 clusters (e.g. 5 per cluster) — do not pad with invented keywords
4. Hub article title must be optimized to target the ENTIRE cluster, not just the primary keyword
5. Never use generic titles like "Ultimate Guide to X" — be specific, compelling, and SEO-optimized
6. Estimate traffic realistically: assume 15-25% CTR for position 1-3 on total cluster volume
7. DO NOT use non-Latin characters anywhere in cluster topics, titles, or keywords
8. If a keyword contains Chinese, Cyrillic, Arabic, or other non-Latin characters, discard it instead of using it

RESPOND WITH VALID JSON ONLY. No markdown, no code fences, no explanation text.

{
  "week_of": "YYYY-MM-DD",
  "sites": [
    {
      "website": "WebsiteName",
      "summary": "One-sentence content strategy direction for this site",
      "clusters": [
        {
          "cluster_topic": "Descriptive theme name",
          "hub_article_title": "SEO-optimized article title targeting entire cluster",
          "primary_keyword": {
            "keyword": "main keyword from data",
            "volume": 20000,
            "kd": 2,
            "intent": "informational"
          },
          "related_keywords": [
            {"keyword": "related kw from data", "volume": 8000, "kd": 1, "intent": "informational"},
            {"keyword": "another related kw", "volume": 5000, "kd": 3, "intent": "commercial"}
          ],
          "total_cluster_volume": 46000,
          "estimated_traffic": 9200,
          "strategy": "Why this cluster matters — cite specific competitor gaps and traffic potential"
        }
      ]
    }
  ]
}

QUALITY REQUIREMENTS:
- Generate exactly 3 clusters for EVERY website — never skip a website
- Aim for 10-20 related keywords per cluster, but accept fewer if the data doesn't support it
- NEVER invent keywords — if you can't find enough real keywords, use fewer
- Each cluster must target a genuinely different topic area
- Strategy should mention which competitors rank for these terms"""


def generate_content_plan(context):
    """Generate topic-based keyword clusters for content briefs."""

    # ── Step 1: Pull ALL content gap keywords (wider net to cover all 9 sites) ──
    raw_kw_data = safe_query(
        "content_gap_keywords",
        "website, keyword, volume, kd, opportunity_score, intent, cluster, competitors",
        order=("volume", True),
        limit=5000,
        label="all_content_gap_for_clustering"
    )

    if not raw_kw_data:
        logger.info("  No content_gap_keywords data available, skipping clustering")
        return None

    all_kw_data = []
    invalid_reason_counts = defaultdict(int)
    seen_keyword_keys = set()
    for raw_kw in raw_kw_data:
        sanitized = sanitize_keyword_record(raw_kw)
        if not sanitized:
            invalid_reason_counts[keyword_filter_reason(raw_kw.get("keyword")) or "invalid"] += 1
            continue
        dedupe_key = f"{sanitized.get('website')}|{sanitized.get('keyword').lower()}"
        if dedupe_key in seen_keyword_keys:
            continue
        seen_keyword_keys.add(dedupe_key)
        all_kw_data.append(sanitized)

    if invalid_reason_counts:
        logger.info(
            "  Filtered invalid clustering keywords: %s",
            ", ".join(f"{reason}={count}" for reason, count in sorted(invalid_reason_counts.items()))
        )

    if not all_kw_data:
        logger.info("  No valid Latin-only content gap keywords available after filtering, skipping clustering")
        return None

    # ── Step 2: Pre-filter — two tiers for progressive threshold relaxation ──
    # Primary pool: strict KD ≤ 5, vol ≥ 1000
    qualifying_strict = [
        kw for kw in all_kw_data
        if kw.get("volume", 0) >= 1000 and kw.get("kd", 99) <= 5
    ]
    strict_keys = set(kw.get("keyword", "").strip().lower() for kw in qualifying_strict)

    # Extended pool: KD ≤ 10, vol ≥ 500 (fills clusters to 10-20 keywords)
    qualifying_extended = [
        kw for kw in all_kw_data
        if kw.get("volume", 0) >= 500 and kw.get("kd", 99) <= 10
    ]

    # Send ALL extended keywords to the model (it needs enough to form 3 × 10-20 clusters)
    qualifying = qualifying_extended

    # Count sites in each tier
    strict_sites = set(kw.get("website") for kw in qualifying_strict)
    all_sites = set(kw.get("website") for kw in qualifying)

    logger.info(
        f"  Pre-filter: {len(all_kw_data)} total -> "
        f"{len(qualifying_strict)} strict (KD≤5, vol≥1000) across {len(strict_sites)} sites, "
        f"{len(qualifying)} extended (KD≤10, vol≥500) across {len(all_sites)} sites"
    )

    if len(qualifying) < 3:
        logger.info("  Too few qualifying keywords for clustering, skipping")
        return None

    # Build lookup for post-validation (includes extended pool for lenient matching)
    kw_lookup = {}
    for kw in qualifying_extended:  # Use the wider pool for validation
        key = kw.get("keyword", "").strip().lower()
        if key:
            kw_lookup[key] = {
                "keyword": kw.get("keyword"),
                "volume": kw.get("volume"),
                "kd": kw.get("kd"),
                "intent": kw.get("intent"),
                "opportunity_score": kw.get("opportunity_score"),
            }

    # ── Step 3: Build context for GPT grouped by website ──
    sections = []
    by_site = defaultdict(list)
    for kw in qualifying:
        by_site[kw.get("website", "unknown")].append(kw)

    for site, kws in sorted(by_site.items()):
        sections.append(f"\n=== {site} ({len(kws)} qualifying keywords) ===")
        for kw in kws[:100]:  # Max 100 per site to fill 3 clusters × 10-20 keywords
            comp_str = ""
            comps = kw.get("competitors")
            if comps and isinstance(comps, list) and len(comps) > 0:
                top = comps[0] if isinstance(comps[0], dict) else {}
                comp_str = f", competitor={top.get('domain', top.get('d', ''))}"
            sections.append(
                f"  '{kw.get('keyword')}' (vol={kw.get('volume')}, KD={kw.get('kd')}, "
                f"intent={kw.get('intent')}, score={kw.get('opportunity_score')}{comp_str})"
            )

    # Add competitor context
    competitors = context.get("competitors", [])
    if competitors:
        sections.append("\n=== COMPETITOR LANDSCAPE ===")
        comp_by_site = defaultdict(list)
        for c in competitors:
            comp_by_site[c["website"]].append(f"{c['competitor_domain']} (overlap={c.get('keyword_overlap')})")
        for site, comps in comp_by_site.items():
            sections.append(f"  {site}: {', '.join(comps[:5])}")

    # Add keyword losers for defensive content
    kw_movers = context.get("keyword_movers", {})
    if kw_movers.get("losers"):
        sections.append("\n=== KEYWORDS LOSING RANKINGS (consider defensive clusters) ===")
        for m in kw_movers["losers"][:15]:
            sections.append(
                f"  {m['website']}: '{m['keyword']}' dropped #{m['position_was']:.0f} -> "
                f"#{m['position_now']:.0f}, lost {abs(m['clicks_change'])} clicks"
            )

    content_context = "\n".join(sections)
    if len(content_context) > 30000:
        content_context = content_context[:30000] + "\n[...truncated]"

    logger.info(f"  Sending {len(content_context)} chars for topic clustering")

    # ── Step 4: AI clustering call ──
    try:
        with Spinner("Building keyword clusters"):
            _ensure_gemini_priority("Topic clustering should run on Gemini 3.1 Pro Preview")
            response = _call_gemini_chat_completion(
                temperature=0.1,
                max_tokens=16000,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": CLUSTER_PROMPT},
                    {"role": "user", "content": (
                        f"Today's date: {date.today().isoformat()}\n"
                        f"Create topic-based keyword clusters from this data:\n\n{content_context}"
                    )},
                ],
            )

        raw = response.choices[0].message.content.strip()
        # Clean markdown fences if present
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3]
        raw = raw.strip()
        if raw.startswith("json"):
            raw = raw[4:].strip()
        match = re.search(r"\{.*\}", raw, flags=re.S)
        if match:
            raw = match.group(0)

        # Attempt to auto-close truncated JSON arrays/objects
        if raw and not raw.endswith("}"):
            raw = raw.rstrip(", ")
            raw += "}]}"

        # Clean trailing commas
        raw = re.sub(r",(\s*[\]}])", r"\1", raw)

        plan = json.loads(raw)

    except json.JSONDecodeError as e:
        raw_preview = (raw[:600] + "...") if "raw" in locals() and len(raw) > 600 else locals().get("raw", "")
        logger.error(f"  Failed to parse cluster plan: {e}")
        if raw_preview:
            logger.error(f"  Raw cluster response preview: {raw_preview}")
        return None
    except Exception as e:
        logger.error(f"  Clustering call failed: {e}")
        return None

    # ── Step 5: Post-validation — strip hallucinated keywords, enrich with metadata ──
    validated_sites = []
    for site_data in plan.get("sites", []):
        validated_clusters = []
        for cluster in site_data.get("clusters", []):
            # Validate primary keyword
            pk = cluster.get("primary_keyword", {})
            pk_raw = clean_text(pk.get("keyword"))
            if not is_valid_keyword(pk_raw):
                logger.warning(
                    "    Skipping cluster '%s' - primary keyword failed Latin/noise validation: '%s'",
                    clean_text(cluster.get("cluster_topic")) or "?",
                    pk_raw,
                )
                continue
            pk_key = pk_raw.lower()
            if pk_key in kw_lookup:
                real = kw_lookup[pk_key]
                pk["keyword"] = real["keyword"]
                pk["volume"] = real["volume"]
                pk["kd"] = real["kd"]
                pk["intent"] = real["intent"]
                pk["relaxed"] = pk_key not in strict_keys
                pk["opportunity_score"] = compute_opportunity_score(real["volume"], real["kd"])
            elif pk.get("volume", 0) < 500 or pk.get("kd", 99) > 10:
                logger.warning(f"    Skipping cluster '{cluster.get('cluster_topic')}' — primary keyword not in DB")
                continue
            else:
                pk["relaxed"] = True
                pk["opportunity_score"] = compute_opportunity_score(pk.get("volume", 0), pk.get("kd", 0))

            # Validate related keywords
            valid_related = []
            for rk in cluster.get("related_keywords", []):
                rk_raw = clean_text(rk.get("keyword"))
                if not is_valid_keyword(rk_raw):
                    logger.warning("    Stripped invalid related keyword: '%s'", rk_raw)
                    continue
                rk_key = rk_raw.lower()
                if rk_key in kw_lookup:
                    real = kw_lookup[rk_key]
                    rk["keyword"] = real["keyword"]
                    rk["volume"] = real["volume"]
                    rk["kd"] = real["kd"]
                    rk["intent"] = real["intent"]
                    rk["relaxed"] = rk_key not in strict_keys
                    rk["opportunity_score"] = compute_opportunity_score(real["volume"], real["kd"])
                    valid_related.append(rk)
                else:
                    logger.warning(f"    Stripped hallucinated keyword: '{rk.get('keyword')}'")

            if len(valid_related) < 1:
                logger.warning(f"    Skipping cluster '{cluster.get('cluster_topic')}' — no valid related keywords")
                continue

            # Sort related keywords by opportunity score (best first)
            valid_related.sort(key=lambda x: x.get("opportunity_score", 0), reverse=True)
            cluster["related_keywords"] = valid_related

            # Extract core topic from primary keyword
            cluster["core_topic"] = sanitize_cluster_label(
                cluster.get("core_topic"),
                pk.get("keyword", ""),
                extract_core_topic(pk.get("keyword", "")),
            )

            # Ensure cluster labels and titles stay clean even if the model returned noise.
            cluster["cluster_topic"] = sanitize_cluster_label(
                cluster.get("cluster_topic"),
                pk.get("keyword", ""),
                cluster["core_topic"],
            )
            cluster["hub_article_title"] = sanitize_hub_title(
                cluster.get("hub_article_title"),
                pk.get("keyword", ""),
                cluster["cluster_topic"],
            )
            cluster["strategy"] = sanitize_narrative(
                cluster.get("strategy"),
                f"Build content around {cluster['cluster_topic']} using validated high-opportunity keywords only.",
            )
            site_data["summary"] = sanitize_narrative(
                site_data.get("summary"),
                f"Focus {site_data.get('website', 'this site')} on validated Latin-only content clusters with real ranking opportunity.",
            )

            # Find question-format keyword in the cluster
            all_cluster_kws = [pk] + valid_related
            q_kw = find_question_keyword(all_cluster_kws)
            if q_kw:
                cluster["question_keyword"] = q_kw
            elif "question_keyword" in cluster:
                cluster.pop("question_keyword", None)

            # Recalculate totals from validated data
            all_vols = [pk.get("volume", 0)] + [r.get("volume", 0) for r in valid_related]
            cluster["total_cluster_volume"] = sum(all_vols)
            cluster["estimated_traffic"] = int(sum(all_vols) * 0.18)

            # Count relaxed keywords
            relaxed_count = sum(1 for kw in all_cluster_kws if kw.get("relaxed"))
            if relaxed_count > 0:
                cluster["relaxed_count"] = relaxed_count

            validated_clusters.append(cluster)

        if validated_clusters:
            site_data["clusters"] = validated_clusters
            validated_sites.append(site_data)

    plan["sites"] = validated_sites

    total_clusters = sum(len(s.get("clusters", [])) for s in validated_sites)
    total_kws = sum(
        1 + len(c.get("related_keywords", []))
        for s in validated_sites
        for c in s.get("clusters", [])
    )
    total_relaxed = sum(
        c.get("relaxed_count", 0)
        for s in validated_sites
        for c in s.get("clusters", [])
    )
    logger.info(f"  Validated: {len(validated_sites)} sites, {total_clusters} clusters, {total_kws} keywords ({total_relaxed} under relaxed thresholds)")
    if not validated_sites:
        logger.warning("  Clustering produced no validated site clusters after post-validation")

    return plan if validated_sites else None



# ══════════════════════════════════════════════════════════════
#  STORAGE
# ══════════════════════════════════════════════════════════════

def store_insights(insights, content_plan=None, site_reports=None, network_report=None):
    """Store legacy insights, content plan, and v2 intelligence reports."""
    today_str = date.today().isoformat()

    row = {
        "date": today_str,
        "insights": json.dumps(insights),
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "dismissed_by_user": False,
    }

    if content_plan is not None:
        row["content_plan"] = json.dumps(content_plan)
    if site_reports is not None:
        row["v2_site_reports"] = json.dumps(site_reports)
    if network_report is not None:
        row["v2_network_report"] = json.dumps(network_report)

    try:
        supabase.table("daily_insights").upsert(
            row, on_conflict="date"
        ).execute()
        logger.info(f"  Stored {len(insights)} insights + content plan + v2 reports for {today_str}")
    except Exception as e:
        logger.error(f"  Failed to store insights: {e}")


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def _pdf_escape(text):
    return str(text or "").replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


class SimplePdfReport:
    def __init__(self):
        self.pages = []
        self.current = []
        self.page_no = 0
        self._y = 720

    def _cmd(self, value):
        self.current.append(value)

    def add_page(self, title=None):
        if self.current:
            self._footer()
            self.pages.append("\n".join(self.current))
        self.page_no += 1
        self.current = []
        self.rect(0, 0, 612, 792, (0.98, 0.99, 1.00))
        self.line(42, 744, 570, 744, (0.90, 0.93, 0.97))
        self.logo(42, 758, scale=0.75)
        if title:
            self.section_header(title)
        else:
            self._y = 720

    def finish(self):
        if self.current:
            self._footer()
            self.pages.append("\n".join(self.current))
            self.current = []
        total_pages = len(self.pages)
        objects = ["<< /Type /Catalog /Pages 2 0 R >>"]
        kids = []
        for idx, content in enumerate(self.pages):
            content = content.replace("{TOTAL_PAGES}", str(total_pages))
            page_obj = 3 + idx * 2
            content_obj = page_obj + 1
            kids.append(f"{page_obj} 0 R")
            stream = content.encode("latin-1", "replace")
            objects.append(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> /F2 << /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >> >> >> /Contents {content_obj} 0 R >>")
            objects.append(f"<< /Length {len(stream)} >>\nstream\n{content}\nendstream")
        objects.insert(1, f"<< /Type /Pages /Kids [{' '.join(kids)}] /Count {len(kids)} >>")
        pdf = bytearray(b"%PDF-1.4\n")
        offsets = [0]
        for number, obj in enumerate(objects, start=1):
            offsets.append(len(pdf))
            pdf.extend(f"{number} 0 obj\n{obj}\nendobj\n".encode("latin-1", "replace"))
        xref = len(pdf)
        pdf.extend(f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode())
        for offset in offsets[1:]:
            pdf.extend(f"{offset:010d} 00000 n \n".encode())
        pdf.extend(f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF".encode())
        return bytes(pdf)

    def rect(self, x, y, w, h, color):
        r, g, b = color
        self._cmd(f"{r:.3f} {g:.3f} {b:.3f} rg {x} {y} {w} {h} re f")

    def line(self, x1, y1, x2, y2, color=(0.90, 0.93, 0.97), width=1):
        r, g, b = color
        self._cmd(f"{r:.3f} {g:.3f} {b:.3f} RG {width} w {x1} {y1} m {x2} {y2} l S")

    def text(self, text, x, y, size=10, color=(0.20, 0.27, 0.38), bold=False):
        r, g, b = color
        font = "F2" if bold else "F1"
        self._cmd(f"{r:.3f} {g:.3f} {b:.3f} rg BT /{font} {size} Tf {x} {y} Td ({_pdf_escape(text)}) Tj ET")

    def wrapped(self, text, x=42, size=10, leading=14, width=88, color=(0.20, 0.27, 0.38)):
        for line in textwrap.wrap(str(text or ""), width=width):
            if self._y < 72:
                self.add_page()
            self.text(line, x, self._y, size=size, color=color)
            self._y -= leading

    def heading(self, label):
        if self._y < 110:
            self.add_page()
        self.text(label, 42, self._y, size=14, color=(0.02, 0.08, 0.18), bold=True)
        self._y -= 22

    def section_header(self, label):
        self.rect(42, 696, 528, 30, (0.02, 0.08, 0.18))
        self.text(label, 56, 706, size=14, color=(1, 1, 1), bold=True)
        self._y = 672

    def logo(self, x, y, scale=1.0, small=False):
        blue = (0.15, 0.39, 0.92)
        dot = 2.4 * scale
        for dx, dy in [(0, 0), (12, 6), (12, -6), (24, 0)]:
            self.rect(x + dx * scale - dot / 2, y + dy * scale - dot / 2, dot, dot, blue)
        self.line(x, y, x + 12 * scale, y + 6 * scale, blue, width=max(0.8, scale))
        self.line(x, y, x + 12 * scale, y - 6 * scale, blue, width=max(0.8, scale))
        self.line(x + 12 * scale, y + 6 * scale, x + 24 * scale, y, blue, width=max(0.8, scale))
        self.line(x + 12 * scale, y - 6 * scale, x + 24 * scale, y, blue, width=max(0.8, scale))
        self.text("ztudium", x + 32 * scale, y - 5 * scale, size=8 if small else int(18 * scale), color=(0.02, 0.08, 0.18), bold=True)

    def badge(self, text, x, y, tone="slate"):
        colors = {
            "green": (0.86, 0.98, 0.91, 0.02, 0.47, 0.24),
            "amber": (1.00, 0.95, 0.82, 0.71, 0.36, 0.00),
            "red": (1.00, 0.90, 0.90, 0.72, 0.10, 0.10),
            "blue": (0.88, 0.94, 1.00, 0.15, 0.39, 0.92),
            "slate": (0.94, 0.96, 0.98, 0.29, 0.34, 0.42),
        }
        br, bg, bb, tr, tg, tb = colors.get(tone, colors["slate"])
        self._cmd(f"{br:.3f} {bg:.3f} {bb:.3f} rg {x} {y - 5} {max(44, len(str(text)) * 5.5)} 16 re f")
        self.text(text, x + 5, y, size=8, color=(tr, tg, tb), bold=True)

    def progress_bar(self, x, y, w, value, tone="green"):
        fills = {
            "green": (0.06, 0.73, 0.45),
            "amber": (0.96, 0.62, 0.04),
            "red": (0.90, 0.16, 0.16),
            "blue": (0.15, 0.39, 0.92),
            "slate": (0.58, 0.64, 0.72),
        }
        self.rect(x, y, w, 7, (0.93, 0.95, 0.97))
        self.rect(x, y, max(4, min(w, w * _num(value) / 100)), 7, fills.get(tone, fills["slate"]))

    def sparkline(self, values, x, y, w=80, h=24, color=(0.15, 0.39, 0.92)):
        values = [float(_num(v)) for v in values if v is not None]
        if len(values) < 2:
            self.line(x, y + h / 2, x + w, y + h / 2, (0.80, 0.84, 0.90), width=1)
            return
        min_v, max_v = min(values), max(values)
        span = max(1, max_v - min_v)
        points = []
        for idx, value in enumerate(values):
            px = x + (idx * w / max(1, len(values) - 1))
            py = y + ((value - min_v) / span) * h
            points.append((px, py))
        r, g, b = color
        path = f"{r:.3f} {g:.3f} {b:.3f} RG 1.6 w {points[0][0]:.1f} {points[0][1]:.1f} m "
        path += " ".join(f"{px:.1f} {py:.1f} l" for px, py in points[1:])
        self._cmd(path + " S")

    def _footer(self):
        report_date = date.today().isoformat()
        self.line(42, 44, 570, 44, (0.90, 0.93, 0.97))
        self.logo(42, 28, scale=0.35, small=True)
        self.text(f"Page {self.page_no} of {{TOTAL_PAGES}} | {report_date} | Confidential", 360, 28, size=8, color=(0.39, 0.46, 0.56))


def _score_tone(score):
    score = int(_num(score))
    if score >= 70:
        return "green"
    if score >= 50:
        return "amber"
    return "red"


def _flatten_report_alerts(site_reports):
    alerts = []
    for site, report in (site_reports or {}).items():
        for alert in report.get("critical_alerts", []) or []:
            alerts.append({**alert, "site": site})
    return sorted(alerts, key=lambda r: _num(r.get("estimated_traffic_impact")), reverse=True)


def _flatten_report_opportunities(site_reports):
    opportunities = []
    for site, report in (site_reports or {}).items():
        for opportunity in report.get("opportunities", []) or []:
            opportunities.append({**opportunity, "site": site})
    return sorted(opportunities, key=lambda r: _num(r.get("estimated_traffic_gain")), reverse=True)


def _fetch_pdf_quick_wins():
    internal = safe_query("internal_linking_suggestions", "website, source_page, target_page, target_page_keyword, score, status", filters=[("eq", ("status", "pending"))], order=("score", True), limit=3, label="pdf_internal_link_wins")
    keywords = safe_query("content_gap_keywords", "website, keyword, volume, kd", filters=[("lt", ("kd", 5)), ("gt", ("volume", 1000))], order=("volume", True), limit=3, label="pdf_easy_keyword_wins")
    broken = safe_query("ahrefs_broken_backlinks", "website, referring_page, target_url, ref_domain_dr", filters=[("gte", ("ref_domain_dr", 40))], order=("ref_domain_dr", True), limit=3, label="pdf_broken_backlink_wins")
    return internal, keywords, broken


def _fetch_pdf_click_trends(days=90):
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    rows = safe_query(
        "daily_metrics",
        "date, website, gsc_clicks",
        filters=[("gte", ("date", cutoff))],
        order=("date", True),
        limit=5000,
        label="pdf_click_trends",
    )
    by_site_date = defaultdict(lambda: defaultdict(float))
    for row in rows:
        site = row.get("website")
        row_date = str(row.get("date", ""))
        if site and row_date:
            by_site_date[site][row_date] += _num(row.get("gsc_clicks"))
    trends = {}
    for site in SITES:
        ordered = [value for _, value in sorted(by_site_date.get(site, {}).items())]
        if len(ordered) > 18:
            step = max(1, len(ordered) // 18)
            ordered = ordered[::step][-18:]
        trends[site] = ordered
    return trends


def _build_weekly_pdf(site_reports, network_report):
    pdf = SimplePdfReport()
    week_end = date.today()
    week_start = week_end - timedelta(days=6)
    generated_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    click_trends = _fetch_pdf_click_trends()
    alerts = _flatten_report_alerts(site_reports)
    opportunities = _flatten_report_opportunities(site_reports)

    pdf.add_page()
    pdf.rect(0, 670, 612, 122, (0.02, 0.08, 0.18))
    pdf.rect(0, 0, 612, 18, (0.15, 0.39, 0.92))
    pdf.logo(210, 724, scale=1.1)
    pdf.text("Weekly SEO Intelligence Report", 42, 620, size=28, color=(0.02, 0.08, 0.18), bold=True)
    pdf.text(f"{week_start.strftime('%B %-d') if os.name != 'nt' else week_start.strftime('%B %#d')} - {week_end.strftime('%B %-d, %Y') if os.name != 'nt' else week_end.strftime('%B %#d, %Y')}", 42, 588, size=14, color=(0.29, 0.34, 0.42))
    pdf.text("Confidential - Ztudium Internal", 42, 556, size=11, color=(0.15, 0.39, 0.92), bold=True)
    pdf.rect(42, 454, 528, 72, (1.00, 0.97, 0.88))
    pdf.rect(42, 454, 6, 72, (0.96, 0.62, 0.04))
    pdf.text("DRAFT VERSION", 62, 500, size=14, color=(0.71, 0.36, 0.00), bold=True)
    pdf.text("This PDF report is currently in testing. Use the live dashboard as the source of truth", 62, 482, size=10, color=(0.29, 0.34, 0.42))
    pdf.text("while the report layout and executive formatting are being refined.", 62, 468, size=10, color=(0.29, 0.34, 0.42))
    pdf.text(f"Generated: {generated_at}", 42, 84, size=10, color=(0.39, 0.46, 0.56))

    pdf.add_page("1. Executive Summary")
    score = int(_num(network_report.get("network_health_score"), 50))
    pdf.badge(f"Network Health {score}", 42, pdf._y, _score_tone(score))
    pdf.progress_bar(172, pdf._y - 3, 180, score, _score_tone(score))
    pdf._y -= 30
    pdf.wrapped(network_report.get("network_summary"))
    pdf._y -= 8
    pdf.wrapped(f"Algorithm update status: {'Detected' if network_report.get('algorithm_update_detected') else 'Not detected'} ({network_report.get('algorithm_update_confidence', 'none')}).")
    pdf.wrapped(f"Top priority: {network_report.get('top_priority_site')} - {network_report.get('top_priority_reason')}")
    pdf.wrapped(f"Network winner: {network_report.get('network_winner')} - {network_report.get('network_winner_reason')}")

    pdf.heading("2. Network Health Overview")
    for site in SITES:
        report = site_reports.get(site, {})
        y = pdf._y
        pdf.text(site, 42, y, size=10, bold=True)
        health = report.get("health_score")
        tone = "slate" if report.get("limited_ai_insight") else _score_tone(health)
        pdf.badge("Limited" if report.get("limited_ai_insight") else str(health), 144, y, tone)
        pdf.progress_bar(214, y - 3, 110, 0 if report.get("limited_ai_insight") else health, tone)
        pdf.text(str(report.get("health_direction", "stable")), 338, y, size=9)
        pdf.text(f"{len(report.get('critical_alerts', []) or [])} alerts", 430, y, size=9)
        pdf.text(f"{len(report.get('opportunities', []) or [])} opps", 500, y, size=9)
        pdf._y -= 24

    pdf.heading("3. Cross-Site Patterns")
    patterns = network_report.get("cross_site_patterns") or []
    if not patterns:
        pdf.wrapped("No cross-site pattern affecting three or more properties was detected.")
    for pattern in patterns:
        pdf.wrapped(f"{pattern.get('pattern') or pattern.get('title')}: {pattern.get('interpretation') or pattern.get('detail')}", size=10)
        pdf.wrapped(f"Affected: {', '.join(pattern.get('sites_affected') or pattern.get('sites') or [])}. Action: {pattern.get('recommended_response') or pattern.get('recommendation')}", size=9)
        pdf._y -= 6

    pdf.add_page("4. Priority Alert Feed")
    for alert in alerts[:10]:
        tone = "amber" if "tracking" in str(alert.get("diagnosis_type", "")).lower() else "red"
        pdf.rect(42, pdf._y - 96, 4, 100, (0.90, 0.16, 0.16) if tone == "red" else (0.96, 0.62, 0.04))
        pdf.badge(alert.get("diagnosis_type", "Site-Specific Issue"), 52, pdf._y, tone)
        pdf.text(f"{alert.get('site')}: {alert.get('title')}", 190, pdf._y, size=10, bold=True)
        pdf._y -= 18
        pdf.wrapped(f"What happened: {alert.get('what_happened')}", size=9)
        pdf.wrapped(f"Why it matters: {alert.get('why_it_matters')}", size=9)
        pdf.wrapped(f"Root cause: {alert.get('root_cause_hypothesis')}", size=9)
        pdf.wrapped(f"Action: {alert.get('action')} (~{int(_num(alert.get('estimated_traffic_impact'))):,} clicks/mo at risk)", size=9)
        pdf._y -= 8

    pdf.add_page("5. Top Opportunities")
    max_gain = max([_num(item.get("estimated_traffic_gain")) for item in opportunities[:6]] or [1])
    for opportunity in opportunities[:6]:
        pdf.badge(opportunity.get("type", "opportunity").replace("_", " ").title(), 42, pdf._y, "blue")
        pdf.text(f"{opportunity.get('site')}: {opportunity.get('title')}", 190, pdf._y, size=10, bold=True)
        pdf.progress_bar(410, pdf._y - 3, 120, (_num(opportunity.get("estimated_traffic_gain")) / max_gain) * 100, "blue")
        pdf._y -= 18
        pdf.wrapped(opportunity.get("insight"), size=9)
        pdf.wrapped(f"Action: {opportunity.get('action')} (~{int(_num(opportunity.get('estimated_traffic_gain'))):,} clicks potential)", size=9)
        pdf._y -= 8

    for site in SITES:
        report = site_reports.get(site, {})
        pdf.add_page(f"6. Site Deep Dive - {site}")
        tone = "slate" if report.get("limited_ai_insight") else _score_tone(report.get("health_score", 50))
        pdf.badge("Limited data" if report.get("limited_ai_insight") else f"Health {report.get('health_score', '--')}", 42, pdf._y, tone)
        pdf.text(str(report.get("health_direction", "stable")).title(), 148, pdf._y, size=9, color=(0.29, 0.34, 0.42), bold=True)
        pdf.sparkline(click_trends.get(site, []), 458, pdf._y - 8, w=90, h=26)
        pdf._y -= 26
        pdf.text(report.get("headline", "Insufficient signal this week"), 42, pdf._y, size=12, color=(0.02, 0.08, 0.18), bold=True)
        pdf._y -= 22
        summary = "Insufficient signal this week - check back after next data refresh." if report.get("limited_ai_insight") else report.get("week_summary")
        pdf.wrapped(summary, size=9)
        for alert in report.get("critical_alerts", []) or []:
            pdf.wrapped(f"Alert: {alert.get('diagnosis_type', 'Issue')} - {alert.get('title')} - {alert.get('action')}", size=8)
        for opportunity in report.get("opportunities", []) or []:
            pdf.wrapped(f"Opportunity: {opportunity.get('title')} - {opportunity.get('action')}", size=8)
        for signal in report.get("momentum_signals", []) or []:
            pdf.wrapped(f"Momentum: {signal.get('title')}: {signal.get('detail')}", size=8)
        pdf._y -= 8

    pdf.add_page("7. Quick Wins This Week")
    internal, keywords, broken = _fetch_pdf_quick_wins()
    for title, rows in [("Internal links", internal), ("Easy keywords", keywords), ("Broken backlinks", broken)]:
        pdf.heading(title)
        if not rows:
            pdf.wrapped("No quick wins available.", size=9)
        for row in rows:
            if title == "Internal links":
                pdf.wrapped(f"{row.get('website')}: Link {row.get('source_page')} -> {row.get('target_page_keyword') or row.get('target_page')} (score {row.get('score')})", size=8)
            elif title == "Easy keywords":
                pdf.wrapped(f"{row.get('website')}: \"{row.get('keyword')}\" | KD {row.get('kd')} | volume {row.get('volume')}", size=8)
            else:
                pdf.wrapped(f"{row.get('website')}: Recover backlink from {row.get('referring_page')} | DR {row.get('ref_domain_dr')}", size=8)

    pdf.add_page("8. Appendix")
    pdf.heading("Silent Decay")
    for site, report in site_reports.items():
        alerts = report.get("silent_decay_alerts", []) or []
        if alerts:
            pdf.wrapped(f"{site}: {len(alerts)} slow-decline finding(s)", size=9)
    pdf.heading("Dead Pages Summary")
    for site, report in site_reports.items():
        summary = report.get("dead_pages_summary") or {}
        pdf.wrapped(f"{site}: {summary.get('total_dead_pages', 0)} dead pages ({summary.get('visible_unclicked_count', 0)} visible unclicked, {summary.get('invisible_count', 0)} invisible)", size=9)

    pdf.add_page("9. Recommended Actions This Week")
    actions = []
    for alert in alerts[:6]:
        actions.append(("High", alert.get("action"), alert.get("site"), "SEO Lead", alert.get("estimated_traffic_impact")))
    for opportunity in opportunities[:6]:
        actions.append(("Medium", opportunity.get("action"), opportunity.get("site"), "Content / SEO", opportunity.get("estimated_traffic_gain")))
    if not actions:
        actions.append(("Low", "Continue weekly monitoring and refresh the insight run next Sunday.", "Network", "SEO Lead", 0))
    pdf.text("Priority", 42, pdf._y, size=9, bold=True)
    pdf.text("Action", 104, pdf._y, size=9, bold=True)
    pdf.text("Site", 370, pdf._y, size=9, bold=True)
    pdf.text("Owner", 448, pdf._y, size=9, bold=True)
    pdf.text("Impact", 520, pdf._y, size=9, bold=True)
    pdf._y -= 18
    for priority, action, site, owner, impact in actions[:10]:
        tone = "red" if priority == "High" else "amber" if priority == "Medium" else "slate"
        pdf.badge(priority, 42, pdf._y, tone)
        pdf.text(textwrap.shorten(str(action or "Review item"), width=72, placeholder="..."), 104, pdf._y, size=8)
        pdf.text(str(site or "Network")[:13], 370, pdf._y, size=8)
        pdf.text(str(owner)[:13], 448, pdf._y, size=8)
        pdf.text(f"~{int(_num(impact)):,}", 520, pdf._y, size=8, bold=True)
        pdf._y -= 24
    return pdf.finish()


def generate_weekly_pdf_report(site_reports, network_report):
    """Generate and upload the branded weekly SEO intelligence PDF."""
    try:
        import requests

        pdf_bytes = _build_weekly_pdf(site_reports, network_report)
        bucket = os.getenv("WEEKLY_REPORTS_BUCKET", "weekly-reports")
        path = f"{date.today().isoformat()}.pdf"
        auth_headers = {"Authorization": f"Bearer {SUPABASE_KEY}", "apikey": SUPABASE_KEY}
        bucket_resp = requests.post(
            f"{SUPABASE_URL}/storage/v1/bucket",
            headers=auth_headers,
            json={"id": bucket, "name": bucket, "public": False},
            timeout=30,
        )
        if bucket_resp.status_code not in (200, 201, 409) and "Duplicate" not in bucket_resp.text:
            logger.warning("  Weekly PDF bucket check failed: %s %s", bucket_resp.status_code, bucket_resp.text[:160])
        upload_resp = requests.post(
            f"{SUPABASE_URL}/storage/v1/object/{bucket}/{path}",
            headers={**auth_headers, "Content-Type": "application/pdf", "x-upsert": "true"},
            data=pdf_bytes,
            timeout=60,
        )
        if not upload_resp.ok:
            logger.error("  Weekly PDF upload failed: %s %s", upload_resp.status_code, upload_resp.text[:200])
            return None
        logger.info("  Weekly PDF report uploaded to Supabase Storage: %s/%s", bucket, path)
        return {"bucket": bucket, "path": path, "bytes": len(pdf_bytes)}
    except Exception as e:
        logger.error("  Weekly PDF generation failed: %s", e)
        return None


def main():
    start_time = time.time()

    print_box(
        "ZTUDIUM INSIGHTS ENGINE v4",
        f"Date: {date.today().isoformat()}"
    )

    # ── Phase 1: Data Collection ──
    print_header("PHASE 1  Collecting Dashboard Data")
    context = gather_context()

    # Pretty data summary table
    summary_rows = [
        ("Daily Metrics (7d)",       len(context.get("daily_metrics_7d", []))),
        ("Daily Metrics (prev week)",len(context.get("daily_metrics_prev_week", []))),
        ("Trend Metrics (today)",    len(context.get("trends_today", []))),
        ("Anomalies Detected",       len(context.get("anomalies_7d", []))),
        ("Ahrefs Site Profiles",     len(context.get("ahrefs_overview", []))),
        ("Keyword Gainers",          len(context.get("keyword_movers", {}).get("gainers", []))),
        ("Keyword Losers",           len(context.get("keyword_movers", {}).get("losers", []))),
        ("Page Rising",              len(context.get("page_movers", {}).get("rising", []))),
        ("Page Falling",             len(context.get("page_movers", {}).get("falling", []))),
        ("Competitors",              len(context.get("competitors", []))),
        ("New Backlinks (DR 40+)",   len(context.get("new_backlinks", []))),
        ("Broken Backlinks",         len(context.get("broken_backlinks", []))),
        ("Easy Win Keywords",        len(context.get("easy_wins", []))),
        ("Site Baselines",           len(context.get("baselines", {}))),
    ]
    total_rows = sum(r[1] for r in summary_rows)
    print(f"\n  {'Data Source':<30} {'Rows':>6}")
    print(f"  {'─' * 30} {'─' * 6}")
    for label, count in summary_rows:
        marker = " *" if count == 0 else ""
        print(f"  {label:<30} {count:>6}{marker}")
    print(f"  {'─' * 30} {'─' * 6}")
    print(f"  {'Total data points':<30} {total_rows:>6}")

    # ── Phase 2: Strategic Insights ──
    print_header("PHASE 2  Generating Strategic Insights")
    insights = generate_insights(context)

    severity_icons = {"high": "!!!", "medium": " ! ", "low": "   "}
    print(f"\n  {'#':<4} {'Sev':<5} {'Category':<14} Title")
    print(f"  {'─' * 4} {'─' * 5} {'─' * 14} {'─' * 34}")
    for i, insight in enumerate(insights):
        sev = insight.get("severity", "low")
        cat = insight.get("category", "?")
        title = insight.get("title", "No title")
        icon = severity_icons.get(sev, "   ")
        print(f"  {i+1:<4} {icon:<5} {cat:<14} {title[:50]}")

    high_count = sum(1 for i in insights if i.get("severity") == "high")
    med_count = sum(1 for i in insights if i.get("severity") == "medium")
    print(f"\n  Summary: {len(insights)} insights ({high_count} high, {med_count} medium)")

    print_header("PHASE 2.5  V2 Multi-Pass Intelligence")
    site_reports, network_report = generate_v2_insights(context)

    print(f"\n  {'Site':<18} {'Health':>6} {'Trend':<10} {'Alerts':>6} {'Opps':>5}")
    print(f"  {'-' * 18} {'-' * 6} {'-' * 10} {'-' * 6} {'-' * 5}")
    for site in SITES:
        report = site_reports.get(site, {})
        print(
            f"  {site:<18} {int(_num(report.get('health_score'), 0)):>6} "
            f"{str(report.get('health_direction', 'stable')):<10} "
            f"{len(report.get('critical_alerts', [])):>6} "
            f"{len(report.get('opportunities', [])):>5}"
        )

    print()
    print(f"  Network health score: {network_report.get('network_health_score')}")
    print(f"  Network trend: {network_report.get('network_trend')}")
    print(
        "  Algorithm update detected: "
        f"{'yes' if network_report.get('algorithm_update_detected') else 'no'} "
        f"({network_report.get('algorithm_update_confidence', 'none')})"
    )
    print(f"  Top priority site: {network_report.get('top_priority_site')}")

    # ── Phase 3: Topic-Based Clustering ──
    print_header("PHASE 3  Building Topic-Based Keyword Clusters")
    content_plan = generate_content_plan(context)

    if content_plan:
        total_clusters = 0
        total_kws = 0
        for site_plan in content_plan.get("sites", []):
            site_name = site_plan.get("website", "?")
            clusters = site_plan.get("clusters", [])
            total_clusters += len(clusters)
            print(f"\n  {site_name} ({len(clusters)} clusters)")
            print(f"  {'─' * 55}")
            for j, c in enumerate(clusters):
                pk = c.get("primary_keyword", {})
                related = c.get("related_keywords", [])
                total_kws += 1 + len(related)
                vol_str = f"{c.get('total_cluster_volume', 0):,}"
                print(f"  {j+1}. {c.get('cluster_topic', '?')}")
                print(f"     Title: {c.get('hub_article_title', '?')}")
                core = c.get('core_topic', '')
                q_kw = c.get('question_keyword', '')
                relaxed_n = c.get('relaxed_count', 0)
                print(f"     Primary: '{pk.get('keyword', '?')}' (vol={pk.get('volume', 0):,}, KD={pk.get('kd', '?')})")
                if core:
                    print(f"     Core topic: '{core}'")
                if q_kw:
                    print(f"     Question KW: '{q_kw}'")
                print(f"     Related: {len(related)} keywords | Cluster vol: {vol_str} | Est. traffic: ~{c.get('estimated_traffic', 0):,}")
                if relaxed_n:
                    print(f"     Relaxed threshold: {relaxed_n} keywords included under soft criteria")
        print(f"\n  Totals: {len(content_plan.get('sites', []))} sites, {total_clusters} clusters, {total_kws} keywords")
    else:
        print("\n  No qualifying keywords found (KD <= 5, Vol >= 1000)")
        print("  Clustering skipped.")

    # ── Phase 4: Store Results ──
    print_header("PHASE 4  Storing Results")
    store_insights(insights, content_plan, site_reports, network_report)

    if content_plan:
        try:
            semantic_result = materialize_semantic_clusters("weekly_insights")
            print(
                "\n  Semantic clusters refreshed: "
                f"{semantic_result.get('clusters', 0)} clusters, "
                f"{semantic_result.get('keywords', 0)} keywords"
            )
        except Exception as exc:
            logger.warning("  Semantic cluster refresh skipped after weekly insights: %s", str(exc)[:200])

    print_header("PHASE 5  Generating Weekly PDF Report")
    pdf_result = generate_weekly_pdf_report(site_reports, network_report)
    if pdf_result:
        print(f"\n  Weekly PDF stored: {pdf_result['bucket']}/{pdf_result['path']} ({pdf_result['bytes']:,} bytes)")
    else:
        print("\n  Weekly PDF generation skipped or failed; dashboard data is still stored.")

    # ── Done ──
    elapsed = time.time() - start_time
    print_box(
        f"COMPLETE  {len(insights)} insights + {'clusters' if content_plan else 'no clusters'} stored",
        f"Total time: {elapsed:.1f}s | Date: {date.today().isoformat()}"
    )


if __name__ == "__main__":
    main()
