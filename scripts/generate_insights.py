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
import logging
import time
import threading
from datetime import date, timedelta, datetime
from collections import defaultdict

from dotenv import load_dotenv
load_dotenv()

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

from ai_client import get_ai_client, ai_chat_completion

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
    print(f"  {'─' * w}")
    print(f"  {title}")
    print(f"  {'─' * w}")


def print_box(title, subtitle=""):
    """Print a prominent title box."""
    w = 60
    print(f"\n  {'═' * w}")
    print(f"   {title}")
    if subtitle:
        print(f"   {subtitle}")
    print(f"  {'═' * w}")


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

    # Use higher token limit to accommodate richer analysis
    max_context = 20000
    if len(context_str) > max_context:
        context_str = context_str[:max_context] + "\n\n[...data truncated for token limits]"

    logger.info(f"  Sending {len(context_str)} chars of context for analysis")

    try:
        with Spinner("Analyzing strategic insights"):
            response = ai_chat_completion(
                model="gpt-4o",
                temperature=0.3,
                max_tokens=3000,
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

        raw = response.choices[0].message.content.strip()

        # Clean potential markdown fencing
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3]
        raw = raw.strip()
        if raw.startswith("json"):
            raw = raw[4:].strip()

        # Attempt to auto-close truncated JSON arrays/objects
        if raw and not raw.endswith("}") and not raw.endswith("]"):
            raw = raw.rstrip(", ")
            raw += "}]}"

        # Clean trailing commas
        raw = re.sub(r",(\s*[\]}])", r"\1", raw)

        try:
            parsed = json.loads(raw)
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
            logger.error(f"  Failed to parse analysis response: {e}")
            logger.error(f"  Raw response preview: {raw[:300]}...")
            return []

        # Sort by severity
        severity_order = {"high": 0, "medium": 1, "low": 2}
        insights.sort(key=lambda x: severity_order.get(x.get("severity", "low"), 2))

        logger.info(f"  Generated {len(insights)} insights")
        return insights

    except json.JSONDecodeError as e:
        logger.error(f"  Failed to parse analysis response: {e}")
        logger.error(f"  Raw response: {raw[:500]}")
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
    return round(volume * (1 / (kd + 1)), 1)


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
            response = ai_chat_completion(
                model="gpt-4o",
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

def store_insights(insights, content_plan=None):
    """Store insights and content plan in daily_insights table."""
    today_str = date.today().isoformat()

    row = {
        "date": today_str,
        "insights": json.dumps(insights),
        "generated_at": datetime.now(tz=__import__('datetime').timezone.utc).isoformat(),
        "dismissed_by_user": False,
    }

    if content_plan is not None:
        row["content_plan"] = json.dumps(content_plan)

    try:
        supabase.table("daily_insights").upsert(
            row, on_conflict="date"
        ).execute()
        logger.info(f"  Stored {len(insights)} insights + content plan for {today_str}")
    except Exception as e:
        logger.error(f"  Failed to store insights: {e}")


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

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
    store_insights(insights, content_plan)

    # ── Done ──
    elapsed = time.time() - start_time
    print_box(
        f"COMPLETE  {len(insights)} insights + {'clusters' if content_plan else 'no clusters'} stored",
        f"Total time: {elapsed:.1f}s | Date: {date.today().isoformat()}"
    )


if __name__ == "__main__":
    main()
