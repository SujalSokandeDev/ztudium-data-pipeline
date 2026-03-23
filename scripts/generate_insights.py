"""
generate_insights.py — Enhanced AI Strategic Insights Engine v2
Uses GPT-4o-mini to analyze comprehensive dashboard data and generate
executive-level strategic recommendations with root cause analysis,
keyword/page-level drill-downs, competitor context, and quantified impacts.

Run after compute_trends.py: python scripts/generate_insights.py
"""
import os
import sys
import json
import logging
from datetime import date, timedelta, datetime
from collections import defaultdict

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── Dependencies ────────────────────────────────────────────
try:
    from supabase import create_client
    from openai import OpenAI
except ImportError as e:
    logger.error(f"Missing package: {e}. Run: pip install supabase openai")
    sys.exit(1)

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
    sys.exit(1)

if not OPENAI_API_KEY:
    logger.error("OPENAI_API_KEY must be set")
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY)


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
[
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

    logger.info(f"  Sending {len(context_str)} chars of context to GPT-4o-mini")

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.3,
            max_tokens=3000,
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
        # Also handle ```json prefix
        if raw.startswith("json"):
            raw = raw[4:].strip()

        insights = json.loads(raw)
        if not isinstance(insights, list):
            insights = [insights]

        # Sort by severity
        severity_order = {"high": 0, "medium": 1, "low": 2}
        insights.sort(key=lambda x: severity_order.get(x.get("severity", "low"), 2))

        logger.info(f"  Generated {len(insights)} insights")
        return insights

    except json.JSONDecodeError as e:
        logger.error(f"  Failed to parse GPT response: {e}")
        logger.error(f"  Raw response: {raw[:500]}")
        return [{
            "category": "urgent",
            "severity": "low",
            "title": "Insight generation encountered a parsing issue",
            "analysis": "AI analysis ran but the response format was unexpected. This is typically temporary.",
            "action": "Re-run the insight generation script. If this persists, check the OPENAI_API_KEY.",
            "impact": "No impact on dashboard functionality.",
            "related_website": "all"
        }]
    except Exception as e:
        logger.error(f"  GPT API call failed: {e}")
        return [{
            "category": "urgent",
            "severity": "low",
            "title": "AI insights API call failed",
            "analysis": f"The OpenAI API call returned an error: {str(e)[:200]}",
            "action": "Check OPENAI_API_KEY is valid and has credits. Re-run the script.",
            "impact": "No insights generated for today. Previous insights remain visible.",
            "related_website": "all"
        }]


# ══════════════════════════════════════════════════════════════
#  WEEKLY CONTENT PLAN GENERATION
# ══════════════════════════════════════════════════════════════

CONTENT_PLAN_PROMPT = """You are a senior content strategist for Ztudium, a media company operating 9 websites:
CitiesABC, BusinessABC, HedgeThink, FashionABC, TradersDNA, FreedomX, Wisdomia, SportsDNA, IntelligentHQ.

Based on the keyword gap data, traffic trends, competitor data, and AI insights provided, create a 
WEEKLY CONTENT PLAN with 6-7 specific content pieces per website.

For each website that has keyword gap data, recommend the most impactful content to create THIS WEEK.

CRITICAL REQUIREMENTS:
1. Each suggestion must target a SPECIFIC keyword from the data
2. Include a concrete ARTICLE TITLE (ready to publish)
3. Specify the CONTENT FORMAT (guide, listicle, comparison, case study, FAQ, how-to, news analysis)
4. Explain WHY this content should be prioritized (volume, low KD, competitor gap, trending topic)
5. Provide ESTIMATED TRAFFIC POTENTIAL based on keyword volume and realistic CTR
6. If a website is losing traffic on certain topics, recommend defensive content for those topics
7. Cross-reference with competitor data — mention what competitors are doing

RESPOND WITH VALID JSON ONLY. No markdown, no code fences. Format:
{
  "week_of": "YYYY-MM-DD",
  "sites": [
    {
      "website": "WebsiteName",
      "summary": "Brief 1-sentence content strategy direction for this site this week",
      "briefs": [
        {
          "priority": 1,
          "keyword": "target keyword from the data",
          "title": "Ready-to-publish article title",
          "format": "guide|listicle|comparison|case-study|faq|how-to|news-analysis",
          "rationale": "Why this content now — cite specific numbers",
          "estimated_traffic": "Realistic monthly traffic estimate with reasoning",
          "search_volume": 1000,
          "kd": 2
        }
      ]
    }
  ]
}

RULES:
- Only include websites that have keyword gap data available
- Maximum 7 briefs per website, minimum 3
- Order briefs by priority (1 = most important)
- Article titles should be compelling and SEO-optimized
- Never use generic titles like 'Ultimate Guide to X' — be specific and unique
- Estimated traffic should be realistic (not just keyword volume)
- If data is limited for a site, provide fewer but higher-quality suggestions"""


def generate_content_plan(context):
    """Generate a weekly content plan using GPT-4o-mini."""
    # Build content-specific context
    sections = []

    # Easy wins grouped by website
    easy_wins = context.get("easy_wins", [])
    if easy_wins:
        sections.append("=== EASY WIN KEYWORDS BY WEBSITE ===")
        by_site = defaultdict(list)
        for ew in easy_wins:
            by_site[ew.get("website", "unknown")].append(ew)
        for site, kws in sorted(by_site.items()):
            sections.append(f"\n--- {site} ---")
            for kw in kws:
                sections.append(
                    f"  '{kw.get('keyword')}' (vol={kw.get('volume')}, KD={kw.get('kd')}, "
                    f"score={kw.get('opportunity_score')}, intent={kw.get('intent')})"
                )

    # All content gap keywords (not just easy wins) for more options
    all_kw_data = safe_query(
        "content_gap_keywords",
        "website, keyword, volume, kd, opportunity_score, intent, cluster",
        order=("volume", True),
        limit=200,
        label="all_content_gap"
    )
    if all_kw_data:
        sections.append("\n=== ALL CONTENT GAP KEYWORDS (by volume) ===")
        by_site = defaultdict(list)
        for kw in all_kw_data:
            if kw.get("volume", 0) > 0:
                by_site[kw.get("website", "unknown")].append(kw)
        for site, kws in sorted(by_site.items()):
            sections.append(f"\n--- {site} (top keywords) ---")
            for kw in kws[:20]:
                sections.append(
                    f"  '{kw.get('keyword')}' (vol={kw.get('volume')}, KD={kw.get('kd')}, "
                    f"intent={kw.get('intent')}, cluster={kw.get('cluster')})"
                )

    # Keyword movers (what we're losing/gaining)
    kw_movers = context.get("keyword_movers", {})
    if kw_movers.get("losers"):
        sections.append("\n=== KEYWORDS LOSING RANKINGS (defend these topics) ===")
        for m in kw_movers["losers"]:
            sections.append(
                f"  {m['website']}: '{m['keyword']}' dropped #{m['position_was']:.0f} -> "
                f"#{m['position_now']:.0f}, lost {abs(m['clicks_change'])} clicks"
            )

    # Competitor context
    competitors = context.get("competitors", [])
    if competitors:
        sections.append("\n=== COMPETITORS ===")
        by_site = defaultdict(list)
        for c in competitors:
            by_site[c["website"]].append(f"{c['competitor_domain']} (overlap={c.get('keyword_overlap')})")
        for site, comps in by_site.items():
            sections.append(f"  {site}: {', '.join(comps[:5])}")

    # Ahrefs overview for context
    ahrefs = context.get("ahrefs_overview", [])
    if ahrefs:
        sections.append("\n=== SITE AUTHORITY OVERVIEW ===")
        for a in ahrefs:
            sections.append(
                f"  {a.get('website')}: DR={a.get('dr')}, "
                f"organic_traffic={a.get('organic_traffic')}, "
                f"keywords={a.get('organic_keywords')}"
            )

    content_context = "\n".join(sections)

    if len(content_context) > 18000:
        content_context = content_context[:18000] + "\n[...truncated]"

    if not content_context.strip() or content_context == "":
        logger.info("  No content gap data available, skipping content plan")
        return None

    logger.info(f"  Sending {len(content_context)} chars to GPT for content plan")

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.4,
            max_tokens=4000,
            messages=[
                {"role": "system", "content": CONTENT_PLAN_PROMPT},
                {"role": "user", "content": (
                    f"Today's date: {date.today().isoformat()}\n"
                    f"Generate a weekly content plan based on this data:\n\n{content_context}"
                )},
            ],
        )

        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3]
        raw = raw.strip()
        if raw.startswith("json"):
            raw = raw[4:].strip()

        plan = json.loads(raw)
        total_briefs = sum(len(s.get("briefs", [])) for s in plan.get("sites", []))
        logger.info(f"  Generated content plan: {len(plan.get('sites', []))} sites, {total_briefs} briefs")
        return plan

    except json.JSONDecodeError as e:
        logger.error(f"  Failed to parse content plan: {e}")
        return None
    except Exception as e:
        logger.error(f"  Content plan GPT call failed: {e}")
        return None


# ══════════════════════════════════════════════════════════════
#  STORAGE
# ══════════════════════════════════════════════════════════════

def store_insights(insights, content_plan=None):
    """Store insights and content plan in daily_insights table."""
    today_str = date.today().isoformat()

    row = {
        "date": today_str,
        "insights": json.dumps(insights),
        "generated_at": datetime.utcnow().isoformat(),
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
    logger.info("=" * 60)
    logger.info("  GENERATING ENHANCED AI STRATEGIC INSIGHTS (v2)")
    logger.info("=" * 60)

    logger.info("  Gathering comprehensive dashboard context...")
    context = gather_context()

    data_summary = {
        "daily_7d": len(context.get("daily_metrics_7d", [])),
        "daily_prev_week": len(context.get("daily_metrics_prev_week", [])),
        "trends_today": len(context.get("trends_today", [])),
        "anomalies": len(context.get("anomalies_7d", [])),
        "ahrefs_sites": len(context.get("ahrefs_overview", [])),
        "keyword_losers": len(context.get("keyword_movers", {}).get("losers", [])),
        "keyword_gainers": len(context.get("keyword_movers", {}).get("gainers", [])),
        "page_falling": len(context.get("page_movers", {}).get("falling", [])),
        "page_rising": len(context.get("page_movers", {}).get("rising", [])),
        "competitors": len(context.get("competitors", [])),
        "new_backlinks": len(context.get("new_backlinks", [])),
        "broken_backlinks": len(context.get("broken_backlinks", [])),
        "easy_wins": len(context.get("easy_wins", [])),
        "baselines": len(context.get("baselines", {})),
    }
    logger.info(f"  Context collected: {json.dumps(data_summary)}")

    logger.info("  Calling GPT-4o-mini for enhanced analysis...")
    insights = generate_insights(context)

    for i, insight in enumerate(insights):
        severity = insight.get("severity", "?").upper()
        category = insight.get("category", "?").upper()
        title = insight.get("title", "No title")
        logger.info(f"  [{i+1}] [{severity}] {category}: {title}")

    logger.info("  Generating weekly content plan...")
    content_plan = generate_content_plan(context)

    if content_plan:
        for site_plan in content_plan.get("sites", []):
            site_name = site_plan.get("website", "?")
            briefs = site_plan.get("briefs", [])
            logger.info(f"  CONTENT PLAN [{site_name}]: {len(briefs)} briefs")
            for b in briefs:
                logger.info(f"    #{b.get('priority', '?')}: {b.get('title', 'No title')}")

    logger.info("  Storing insights + content plan...")
    store_insights(insights, content_plan)

    logger.info("=" * 60)
    logger.info(f"  DONE — {len(insights)} insights + content plan generated and stored")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
