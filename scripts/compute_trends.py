"""
compute_trends.py — Enhanced Trend Analysis Engine v2
Computes DoD, WoW, MoM percentage changes + smart anomaly detection
with dynamic thresholds, pattern detection, and historical severity scoring.

Run after daily data fetch: python scripts/compute_trends.py
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

# ── Supabase setup ──────────────────────────────────────────
try:
    from supabase import create_client
except ImportError:
    logger.error("supabase package not installed. Run: pip install supabase")
    sys.exit(1)

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Dynamic Anomaly Thresholds ──────────────────────────────
# Each metric has its own rules: threshold %, minimum absolute change,
# and a high-severity level. This prevents noise from low-volume metrics.

ANOMALY_RULES = {
    # GSC metrics
    "gsc_clicks": {
        "threshold_pct": 0.30,
        "min_absolute_change": 50,
        "severity_high": 0.50,
    },
    "gsc_impressions": {
        "threshold_pct": 0.30,
        "min_absolute_change": 200,
        "severity_high": 0.50,
    },
    "gsc_ctr": {
        "threshold_pct": 0.20,
        "min_absolute_change": 0.5,  # 0.5% CTR change
        "severity_high": 0.40,
    },
    "gsc_position": {
        "threshold_pct": 0.15,
        "min_absolute_change": 1.0,  # 1 position change
        "severity_high": 0.30,
    },
    # GA4 metrics
    "ga_sessions": {
        "threshold_pct": 0.30,
        "min_absolute_change": 50,
        "severity_high": 0.50,
    },
    "ga_organic_sessions": {
        "threshold_pct": 0.30,
        "min_absolute_change": 30,
        "severity_high": 0.50,
    },
    "ga_bounce_rate": {
        "threshold_pct": 0.15,
        "min_absolute_change": 3.0,  # 3% bounce rate change
        "severity_high": 0.25,
    },
    # Ahrefs/Authority metrics
    "domain_rating": {
        "threshold_pct": 0.05,
        "min_absolute_change": 2,
        "severity_high": 0.10,
    },
    "ahrefs_keywords": {
        "threshold_pct": 0.15,
        "min_absolute_change": 50,
        "severity_high": 0.30,
    },
    "backlinks_total": {
        "threshold_pct": 0.20,
        "min_absolute_change": 20,
        "severity_high": 0.40,
    },
    "ref_domains_total": {
        "threshold_pct": 0.15,
        "min_absolute_change": 10,
        "severity_high": 0.30,
    },
    "ahrefs_top3": {
        "threshold_pct": 0.15,
        "min_absolute_change": 3,
        "severity_high": 0.30,
    },
}

# Metrics to track from daily_metrics table
DAILY_METRICS = [
    "gsc_clicks", "gsc_impressions", "gsc_ctr", "gsc_position",
    "ga_sessions", "ga_organic_sessions", "ga_bounce_rate",
    "domain_rating", "ahrefs_keywords", "ahrefs_top3",
    "backlinks_total", "ref_domains_total",
]

LOOKBACK_DAYS = 95  # enough for 90-day percentile calculation


def pct_change(current, previous):
    """Calculate percentage change, returns None if either value is missing."""
    if current is None or previous is None:
        return None
    if previous == 0:
        return 100.0 if current > 0 else 0.0
    return round(((current - previous) / abs(previous)) * 100, 2)


def compute_7day_avg(values):
    """Compute 7-day moving average from a list of values."""
    valid = [v for v in values[-7:] if v is not None]
    if not valid:
        return None
    return round(sum(valid) / len(valid), 2)


def compute_percentile(current, all_values):
    """
    Compute where current value ranks in historical values (0-100).
    0 = lowest ever, 100 = highest ever.
    """
    valid = [v for v in all_values if v is not None]
    if not valid or current is None:
        return None
    below = sum(1 for v in valid if v < current)
    return round((below / len(valid)) * 100, 1)


def determine_severity(metric_name, dod_pct, absolute_change):
    """Determine anomaly severity using dynamic thresholds."""
    rules = ANOMALY_RULES.get(metric_name, {
        "threshold_pct": 0.30,
        "min_absolute_change": 10,
        "severity_high": 0.50,
    })

    if dod_pct is None or absolute_change is None:
        return None, False

    abs_pct = abs(dod_pct) / 100.0

    # Must exceed BOTH percentage threshold AND minimum absolute change
    if abs_pct < rules["threshold_pct"] or abs(absolute_change) < rules["min_absolute_change"]:
        return None, False

    if abs_pct >= rules["severity_high"]:
        return "high", True
    else:
        return "medium", True


# ── Data Fetching ───────────────────────────────────────────

def fetch_daily_data(website, days=LOOKBACK_DAYS):
    """Fetch daily_metrics for a website, ordered by date ascending."""
    since = (date.today() - timedelta(days=days)).isoformat()
    result = supabase.table("daily_metrics") \
        .select("date, website, " + ", ".join(DAILY_METRICS)) \
        .eq("website", website) \
        .gte("date", since) \
        .order("date", desc=False) \
        .limit(1000) \
        .execute()
    return result.data or []


def get_all_websites():
    """Get distinct website names from daily_metrics."""
    result = supabase.table("daily_metrics") \
        .select("website") \
        .limit(2000) \
        .execute()
    return list(set(row["website"] for row in (result.data or [])))


# ── Trend Computation ───────────────────────────────────────

def compute_trends_for_website(website, rows):
    """Compute trend metrics for all DAILY_METRICS for a given website."""
    if len(rows) < 2:
        logger.info(f"  {website}: Not enough data ({len(rows)} rows), skipping")
        return []

    today_str = date.today().isoformat()
    results = []
    anomaly_directions = {}  # metric -> "drop"/"spike" for site-wide detection

    for metric in DAILY_METRICS:
        values = [row.get(metric) for row in rows]

        # Find latest non-None value
        latest_val = None
        latest_idx = None
        for i in range(len(values) - 1, -1, -1):
            if values[i] is not None:
                latest_val = values[i]
                latest_idx = i
                break

        if latest_val is None or latest_idx is None or latest_idx < 1:
            continue

        # Day-over-day
        prev_day_val = None
        for i in range(latest_idx - 1, -1, -1):
            if values[i] is not None:
                prev_day_val = values[i]
                break

        dod = pct_change(latest_val, prev_day_val)
        absolute_change = (latest_val - prev_day_val) if prev_day_val is not None else None

        # Week-over-week — find value closest to 7 days ago
        wow_val = None
        target_idx = latest_idx - 7
        if target_idx >= 0:
            for offset in [0, -1, 1, -2, 2]:
                idx = target_idx + offset
                if 0 <= idx < len(values) and values[idx] is not None:
                    wow_val = values[idx]
                    break
        wow = pct_change(latest_val, wow_val)

        # Month-over-month — find value closest to 30 days ago
        mom_val = None
        target_idx = latest_idx - 30
        if target_idx >= 0:
            for offset in [0, -1, 1, -2, 2, -3, 3]:
                idx = target_idx + offset
                if 0 <= idx < len(values) and values[idx] is not None:
                    mom_val = values[idx]
                    break
        mom = pct_change(latest_val, mom_val)

        # 7-day moving average
        avg7 = compute_7day_avg(values[:latest_idx + 1])

        # Historical percentile (where does current value rank in 90 days)
        hist_percentile = compute_percentile(latest_val, values)

        # Smart anomaly detection
        severity, is_anomaly = determine_severity(metric, dod, absolute_change)

        anomaly_desc = None
        if is_anomaly:
            direction = "spike" if (dod or 0) > 0 else "drop"
            anomaly_directions[metric] = direction
            abs_change_str = f", absolute change: {absolute_change:+.0f}" if absolute_change else ""
            percentile_str = f", historical rank: {hist_percentile}th percentile" if hist_percentile is not None else ""
            anomaly_desc = (
                f"{metric} {direction}: {dod:+.1f}% DoD change "
                f"(severity: {severity}{abs_change_str}{percentile_str})"
            )

        results.append({
            "date": today_str,
            "website": website,
            "metric_name": metric,
            "day_over_day_pct": dod,
            "week_over_week_pct": wow,
            "month_over_month_pct": mom,
            "seven_day_avg": avg7,
            "is_anomaly": is_anomaly,
            "anomaly_description": anomaly_desc,
            "severity": severity,
            "historical_percentile": hist_percentile,
            "site_wide_issue": False,  # Updated below
            "cross_site_pattern": None,  # Updated in main()
        })

    # ── Site-wide issue detection ───────────────────────────
    # If 4+ metrics are anomalous in the same direction, flag all as site-wide
    drop_count = sum(1 for d in anomaly_directions.values() if d == "drop")
    spike_count = sum(1 for d in anomaly_directions.values() if d == "spike")

    if drop_count >= 4 or spike_count >= 4:
        direction = "drop" if drop_count >= 4 else "spike"
        for result in results:
            if result["is_anomaly"]:
                result["site_wide_issue"] = True
                result["anomaly_description"] = (
                    f"SITE-WIDE {direction.upper()}: "
                    f"{result['anomaly_description']} "
                    f"({drop_count + spike_count} metrics affected simultaneously)"
                )
        logger.warning(
            f"  !! SITE-WIDE ISSUE on {website}: "
            f"{drop_count} drops + {spike_count} spikes detected"
        )

    return results, anomaly_directions


# ── Cross-site correlation ──────────────────────────────────

def detect_cross_site_patterns(all_anomalies):
    """
    Detect when 3+ websites show the same metric trending in the same direction.
    This signals a potential algorithm update or industry-wide event.
    """
    # Group by metric+direction
    patterns = defaultdict(list)
    for website, anomaly_dirs in all_anomalies.items():
        for metric, direction in anomaly_dirs.items():
            key = f"{metric}_{direction}"
            patterns[key].append(website)

    cross_site_alerts = {}
    for key, websites in patterns.items():
        if len(websites) >= 3:
            cross_site_alerts[key] = websites
            metric, direction = key.rsplit("_", 1)
            logger.warning(
                f"  !! CROSS-SITE PATTERN: {metric} {direction} "
                f"detected on {len(websites)} sites: {', '.join(websites)}"
            )

    return cross_site_alerts


# ── Main ────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("  COMPUTING ENHANCED TREND METRICS (v2)")
    logger.info("=" * 60)

    websites = get_all_websites()
    logger.info(f"Found {len(websites)} websites")
    logger.info(f"Tracking {len(DAILY_METRICS)} metrics per website")

    total_upserted = 0
    total_anomalies = 0
    all_results = {}  # website -> list of trend dicts
    all_anomalies = {}  # website -> {metric: direction}

    # Phase 1: Compute trends per website
    for website in sorted(websites):
        rows = fetch_daily_data(website)
        trends, anomaly_dirs = compute_trends_for_website(website, rows)
        all_results[website] = trends
        if anomaly_dirs:
            all_anomalies[website] = anomaly_dirs

    # Phase 2: Detect cross-site patterns
    cross_site_alerts = detect_cross_site_patterns(all_anomalies)

    # Phase 3: Annotate cross-site patterns on affected results
    for key, affected_websites in cross_site_alerts.items():
        metric, direction = key.rsplit("_", 1)
        pattern_label = f"{direction}_on_{len(affected_websites)}_sites"
        for website in affected_websites:
            for result in all_results.get(website, []):
                if result["metric_name"] == metric and result["is_anomaly"]:
                    result["cross_site_pattern"] = pattern_label
                    result["severity"] = "high"  # Escalate to high
                    result["anomaly_description"] = (
                        f"CROSS-SITE ALERT: {result['anomaly_description']} "
                        f"(same pattern on {len(affected_websites)} sites — possible algorithm update)"
                    )

    # Phase 4: Upsert all results
    for website in sorted(all_results.keys()):
        trends = all_results[website]
        for trend in trends:
            try:
                supabase.table("calculated_metrics").upsert(
                    trend,
                    on_conflict="date,website,metric_name"
                ).execute()
                total_upserted += 1
                if trend["is_anomaly"]:
                    total_anomalies += 1
                    severity_tag = f"[{(trend.get('severity') or 'medium').upper()}]"
                    logger.warning(f"  {severity_tag} {trend['anomaly_description']}")
            except Exception as e:
                logger.error(f"  Error upserting {website}/{trend['metric_name']}: {e}")

    # Summary
    site_wide_count = sum(
        1 for website_trends in all_results.values()
        for t in website_trends if t.get("site_wide_issue")
    )
    cross_site_count = sum(
        1 for website_trends in all_results.values()
        for t in website_trends if t.get("cross_site_pattern")
    )

    logger.info("=" * 60)
    logger.info(f"  DONE: {total_upserted} trend rows upserted")
    logger.info(f"  Anomalies: {total_anomalies} | Site-wide: {site_wide_count} | Cross-site: {cross_site_count}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
