#!/usr/bin/env python3
"""
Check Ahrefs organic traffic thresholds and send once-per-day email alerts.

Source of truth:
- ahrefs_overview.organic_traffic from the latest Ahrefs overview snapshot per website.

The rule format is intentionally generic so future metric threshold alerts can use
the same evaluator and delivery path.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("ahrefs_traffic_alerts")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
RESEND_FROM_EMAIL = os.getenv("RESEND_FROM_EMAIL", "AI.DNA Platform Intelligence <noreply@citiesabc.com>")

DEFAULT_RECIPIENTS = [
    "seo@ztudium.com",
    "peyman.farahani@ztudium.com",
    "neeraj.rajpal@ztudium.com",
]
TEST_RECIPIENTS = ["sujal.sokande@ztudium.com"]


@dataclass(frozen=True)
class Threshold:
    severity: str
    below: int
    label: str


@dataclass(frozen=True)
class AlertRule:
    rule_id: str
    source_table: str
    metric_field: str
    display_name: str
    thresholds: tuple[Threshold, ...]


AHREFS_TRAFFIC_RULE = AlertRule(
    rule_id="ahrefs_organic_traffic_floor",
    source_table="ahrefs_overview",
    metric_field="organic_traffic",
    display_name="Ahrefs organic traffic",
    thresholds=(
        Threshold(severity="critical", below=2000, label="Critical traffic floor breached"),
        Threshold(severity="warning", below=2500, label="Warning traffic floor breached"),
    ),
)


def _num(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip().replace(",", "")
    multiplier = 1
    if text.lower().endswith("k"):
        multiplier = 1_000
        text = text[:-1]
    elif text.lower().endswith("m"):
        multiplier = 1_000_000
        text = text[:-1]
    try:
        return int(float(text) * multiplier)
    except ValueError:
        return 0


def _today_iso() -> str:
    return date.today().isoformat()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fingerprint(rule: AlertRule, website: str, severity: str, run_date: str) -> str:
    raw = f"{rule.rule_id}|{website}|{severity}|{run_date}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _dedupe_key(rule: AlertRule, website: str, severity: str, run_date: str) -> str:
    return f"email:{rule.rule_id}:{website.lower()}:{severity}:{run_date}"


def get_supabase_headers() -> dict[str, str]:
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_KEY are required")
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }


def supabase_rest_url(table: str) -> str:
    return f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}"


def supabase_select(table: str, params: dict[str, str]) -> list[dict[str, Any]]:
    response = requests.get(
        supabase_rest_url(table),
        headers=get_supabase_headers(),
        params=params,
        timeout=60,
    )
    if not response.ok:
        raise RuntimeError(f"Supabase select {table} failed: {response.status_code} {response.text[:300]}")
    data = response.json()
    return data if isinstance(data, list) else []


def supabase_upsert(table: str, payload: dict[str, Any], conflict_key: str) -> None:
    headers = {
        **get_supabase_headers(),
        "Prefer": "resolution=merge-duplicates",
    }
    response = requests.post(
        supabase_rest_url(table),
        headers=headers,
        params={"on_conflict": conflict_key},
        json=payload,
        timeout=60,
    )
    if not response.ok:
        raise RuntimeError(f"Supabase upsert {table} failed: {response.status_code} {response.text[:300]}")


def supabase_update(table: str, payload: dict[str, Any], filters: dict[str, str]) -> None:
    response = requests.patch(
        supabase_rest_url(table),
        headers=get_supabase_headers(),
        params=filters,
        json=payload,
        timeout=60,
    )
    if not response.ok:
        raise RuntimeError(f"Supabase update {table} failed: {response.status_code} {response.text[:300]}")


def fetch_latest_ahrefs_overviews() -> list[dict[str, Any]]:
    rows = supabase_select(
        "ahrefs_overview",
        {
            "select": "website,date,domain,organic_traffic,organic_traffic_delta,source_file",
            "order": "date.desc",
            "limit": "5000",
        },
    )
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        website = str(row.get("website") or "").strip()
        if not website:
            continue
        current = latest.get(website)
        if not current or str(row.get("date") or "") > str(current.get("date") or ""):
            latest[website] = row
    return sorted(latest.values(), key=lambda item: str(item.get("website") or "").lower())


def evaluate_rule(rule: AlertRule, row: dict[str, Any]) -> dict[str, Any] | None:
    value = _num(row.get(rule.metric_field))
    for threshold in rule.thresholds:
        if value < threshold.below:
            website = str(row.get("website") or "Unknown")
            date_value = str(row.get("date") or _today_iso())
            return {
                "rule_id": rule.rule_id,
                "type": "ahrefs_traffic_drop",
                "diagnosis_type": "Ahrefs Traffic Floor",
                "severity": "high" if threshold.severity == "critical" else "medium",
                "email_severity": threshold.severity,
                "website": website,
                "domain": row.get("domain") or "",
                "metric_name": rule.display_name,
                "metric_value": value,
                "threshold": threshold.below,
                "snapshot_date": date_value,
                "traffic_delta": row.get("organic_traffic_delta"),
                "title": f"{website} Ahrefs organic traffic is below {threshold.below:,}",
                "what_happened": (
                    f"Latest Ahrefs organic traffic for {website} is {value:,}, "
                    f"which is below the {threshold.severity} threshold of {threshold.below:,}."
                ),
                "why_it_matters": (
                    "This indicates reduced search visibility or lower organic traffic potential in Ahrefs. "
                    "It should be reviewed before the decline affects content planning, prioritisation, and reporting confidence."
                ),
                "root_cause_hypothesis": (
                    "Likely causes include ranking losses, competitor gains, reduced keyword coverage, backlink loss, "
                    "or an Ahrefs data refresh showing a material visibility change."
                ),
                "action": (
                    "Review the affected website in AI.DNA Platform Intelligence, compare recent keyword and page movement, "
                    "then prioritise recovery actions for content, internal linking, and backlink protection."
                ),
                "estimated_traffic_impact": max(0, threshold.below - value),
                "recovery_status": "active",
            }
    return None


def _email_subject(alert: dict[str, Any]) -> str:
    severity = str(alert["email_severity"]).upper()
    site = alert["website"]
    value = int(alert["metric_value"])
    threshold = int(alert["threshold"])
    return f"[{severity}] AI.DNA Platform Intelligence: {site} Ahrefs traffic below {threshold:,} ({value:,})"


def _email_html(alert: dict[str, Any]) -> str:
    severity = str(alert["email_severity"]).capitalize()
    tone = "#b91c1c" if alert["email_severity"] == "critical" else "#b45309"
    site = html.escape(str(alert["website"]))
    domain = html.escape(str(alert.get("domain") or ""))
    snapshot_date = html.escape(str(alert.get("snapshot_date") or ""))
    value = int(alert["metric_value"])
    threshold = int(alert["threshold"])
    delta = alert.get("traffic_delta")
    delta_line = f"<li><strong>Ahrefs delta:</strong> {html.escape(str(delta))}</li>" if delta not in (None, "") else ""
    return f"""
    <div style="font-family:Arial,sans-serif;line-height:1.55;color:#172033;max-width:680px">
      <p style="margin:0 0 16px;font-size:13px;color:#61708a">AI.DNA Platform Intelligence</p>
      <h1 style="margin:0 0 12px;font-size:22px;color:{tone}">{severity} Ahrefs traffic alert</h1>
      <p style="font-size:15px;margin:0 0 18px">
        {site} has dropped below the defined Ahrefs organic traffic threshold and should be reviewed by the SEO team.
      </p>
      <table style="width:100%;border-collapse:collapse;margin:0 0 18px">
        <tr><td style="padding:8px;border:1px solid #e5e7eb"><strong>Website</strong></td><td style="padding:8px;border:1px solid #e5e7eb">{site}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e5e7eb"><strong>Domain</strong></td><td style="padding:8px;border:1px solid #e5e7eb">{domain or "-"}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e5e7eb"><strong>Latest Ahrefs traffic</strong></td><td style="padding:8px;border:1px solid #e5e7eb">{value:,}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e5e7eb"><strong>Threshold</strong></td><td style="padding:8px;border:1px solid #e5e7eb">{threshold:,}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e5e7eb"><strong>Snapshot date</strong></td><td style="padding:8px;border:1px solid #e5e7eb">{snapshot_date}</td></tr>
      </table>
      <ul style="padding-left:18px;margin:0 0 18px">
        {delta_line}
        <li><strong>Why it matters:</strong> Search visibility may be weakening and should be checked before it affects planning and reporting confidence.</li>
        <li><strong>Recommended action:</strong> Review keyword movement, affected pages, internal linking gaps, and backlink changes for this website.</li>
      </ul>
      <p style="font-size:13px;color:#61708a;margin:20px 0 0">
        This alert is sent once per website and severity per day to avoid duplicate notifications.
      </p>
    </div>
    """


def _email_text(alert: dict[str, Any]) -> str:
    severity = str(alert["email_severity"]).upper()
    return (
        f"{severity} Ahrefs traffic alert\n\n"
        f"Website: {alert['website']}\n"
        f"Domain: {alert.get('domain') or '-'}\n"
        f"Latest Ahrefs traffic: {int(alert['metric_value']):,}\n"
        f"Threshold: {int(alert['threshold']):,}\n"
        f"Snapshot date: {alert.get('snapshot_date')}\n\n"
        "Why it matters: Search visibility may be weakening and should be checked before it affects planning and reporting confidence.\n\n"
        "Recommended action: Review keyword movement, affected pages, internal linking gaps, and backlink changes for this website.\n"
    )


def has_email_been_sent(dedupe_key: str) -> bool:
    try:
        result = supabase_select(
            "ai_alert_tracking",
            {
                "select": "alert_fingerprint",
                "alert_fingerprint": f"eq.{dedupe_key}",
                "limit": "1",
            },
        )
        return bool(result)
    except Exception as exc:
        logger.warning("Could not read ai_alert_tracking for dedupe: %s", str(exc)[:200])
        return False


def record_email_sent(dedupe_key: str, alert: dict[str, Any], recipients: list[str]) -> None:
    payload = {
        "alert_fingerprint": dedupe_key,
        "site": alert["website"],
        "alert_type": "email_ahrefs_traffic_drop",
        "diagnosis_type": alert["diagnosis_type"],
        "title": alert["title"],
        "first_seen": _today_iso(),
        "last_seen": _today_iso(),
        "last_impact": int(alert["estimated_traffic_impact"]),
        "recovery_status": "active",
        "occurrences": 1,
        "last_payload": {
            **alert,
            "email_sent_at": _utc_now_iso(),
            "email_recipients": recipients,
        },
    }
    try:
        supabase_upsert("ai_alert_tracking", payload, "alert_fingerprint")
    except Exception as exc:
        logger.warning("Could not record email dedupe row: %s", str(exc)[:200])


def send_email(alert: dict[str, Any], recipients: list[str], dry_run: bool) -> bool:
    if dry_run:
        logger.info("[dry-run] Would send %s to %s", _email_subject(alert), ", ".join(recipients))
        return True
    if not RESEND_API_KEY:
        raise RuntimeError("RESEND_API_KEY is required to send Ahrefs traffic alerts")
    response = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "from": RESEND_FROM_EMAIL,
            "to": recipients,
            "subject": _email_subject(alert),
            "html": _email_html(alert),
            "text": _email_text(alert),
        },
        timeout=30,
    )
    if not response.ok:
        raise RuntimeError(f"Resend returned {response.status_code}: {response.text[:300]}")
    return True


def upsert_dashboard_alerts(alerts: list[dict[str, Any]]) -> None:
    if not alerts:
        return

    latest = supabase_select(
        "daily_insights",
        {
            "select": "date,v2_site_reports",
            "order": "date.desc",
            "limit": "1",
        },
    )
    if not latest:
        logger.info("No daily_insights row found; skipping dashboard alert merge")
        return

    row = latest[0]
    date_value = row["date"]
    site_reports = row.get("v2_site_reports") or {}
    if isinstance(site_reports, str):
        site_reports = json.loads(site_reports)
    if not isinstance(site_reports, dict):
        site_reports = {}

    changed = False
    for alert in alerts:
        site = alert["website"]
        report = site_reports.get(site) or {
            "site": site,
            "health_score": 0,
            "health_direction": "declining",
            "headline": f"{site} requires SEO review",
            "critical_alerts": [],
            "opportunities": [],
            "momentum_signals": [],
            "week_summary": f"{site} requires review based on latest Ahrefs threshold monitoring.",
        }
        existing_alerts = report.get("critical_alerts") if isinstance(report.get("critical_alerts"), list) else []
        dashboard_alert = {
            key: value
            for key, value in alert.items()
            if key not in {"email_severity", "website", "domain", "metric_name", "metric_value", "threshold", "snapshot_date", "traffic_delta", "rule_id"}
        }
        dashboard_alert["alert_fingerprint"] = _fingerprint(AHREFS_TRAFFIC_RULE, site, alert["email_severity"], str(alert.get("snapshot_date") or _today_iso()))
        existing_alerts = [
            item for item in existing_alerts
            if item.get("type") != "ahrefs_traffic_drop" or item.get("alert_fingerprint") != dashboard_alert["alert_fingerprint"]
        ]
        report["critical_alerts"] = [dashboard_alert, *existing_alerts][:4]
        site_reports[site] = report
        changed = True

    if changed:
        supabase_update("daily_insights", {"v2_site_reports": site_reports}, {"date": f"eq.{date_value}"})
        logger.info("Merged %d Ahrefs threshold alert(s) into daily_insights.%s", len(alerts), date_value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Check Ahrefs organic traffic threshold alerts")
    parser.add_argument("--dry-run", action="store_true", help="Evaluate alerts without sending emails or writing dashboard alerts")
    parser.add_argument("--test-recipient", action="store_true", help="Send to Sujal only for testing")
    parser.add_argument("--send", action="store_true", help="Actually send email alerts")
    parser.add_argument("--skip-dashboard", action="store_true", help="Do not merge active alerts into daily_insights")
    args = parser.parse_args()

    rows = fetch_latest_ahrefs_overviews()
    alerts = [alert for row in rows if (alert := evaluate_rule(AHREFS_TRAFFIC_RULE, row))]
    logger.info("Evaluated %d website(s), found %d active Ahrefs threshold alert(s)", len(rows), len(alerts))

    if not args.skip_dashboard and not args.dry_run:
        upsert_dashboard_alerts(alerts)

    recipients = TEST_RECIPIENTS if args.test_recipient else DEFAULT_RECIPIENTS
    run_date = _today_iso()
    sent = 0
    skipped = 0
    for alert in alerts:
        dedupe_key = _dedupe_key(AHREFS_TRAFFIC_RULE, alert["website"], alert["email_severity"], run_date)
        if has_email_been_sent(dedupe_key):
            skipped += 1
            logger.info("Skipping duplicate daily email for %s %s", alert["website"], alert["email_severity"])
            continue
        if args.send or args.dry_run:
            send_email(alert, recipients, args.dry_run)
            if not args.dry_run:
                record_email_sent(dedupe_key, alert, recipients)
            sent += 1
        else:
            logger.info("Email send disabled. Use --send to send: %s", _email_subject(alert))

    logger.info("Ahrefs traffic alert check complete. sent=%d skipped=%d active=%d", sent, skipped, len(alerts))


if __name__ == "__main__":
    main()
