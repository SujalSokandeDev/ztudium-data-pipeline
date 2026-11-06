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
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests
from dotenv import load_dotenv

import seodash_db as pg

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("ahrefs_traffic_alerts")

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
RESEND_FROM_EMAIL = os.getenv("RESEND_FROM_EMAIL", "").strip() or "AI.DNA Platform Intelligence <noreply@citiesabc.com>"

DEFAULT_RECIPIENTS = [
    "sujal.sokande@ztudium.com",
    "neeraj.rajpal@ztudium.com",
    "jonathan.vdb@ztudium.com",
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


def _num(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip().replace(",", "")
    multiplier = 1
    if text.lower().endswith("k"):
        multiplier = 1_000
        text = text[:-1]
    elif text.lower().endswith("m"):
        multiplier = 1_000_000
        text = text[:-1]
    try:
        number = float(text) * multiplier
        return int(number) if math.isfinite(number) and number >= 0 else None
    except (ValueError, OverflowError):
        return None


def _today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fingerprint(rule: AlertRule, website: str, severity: str, run_date: str) -> str:
    raw = f"{rule.rule_id}|{website}|{severity}|{run_date}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _recipient_group_key(recipients: list[str]) -> str:
    normalized = ",".join(sorted(email.strip().lower() for email in recipients if email.strip()))
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
    return f"recipients:{digest}"


def _dedupe_key(rule: AlertRule, website: str, severity: str, run_date: str, recipients: list[str]) -> str:
    return f"email:{rule.rule_id}:{website.lower()}:{severity}:{run_date}:{_recipient_group_key(recipients)}"


def fetch_latest_ahrefs_overviews() -> list[dict[str, Any]]:
    rows = pg.select_rows(
        "ahrefs_overview",
        columns="website,date,domain,organic_traffic,organic_traffic_delta,source_file",
        order_by="date",
        desc=True,
        limit=5000,
    )
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        website = str(row.get("website") or "").strip()
        if not website:
            continue
        key = website.lower()
        current = latest.get(key)
        if not current or str(row.get("date") or "") > str(current.get("date") or ""):
            latest[key] = {**row, "website": website}
    return sorted(latest.values(), key=lambda item: str(item.get("website") or "").lower())


def evaluate_rule(rule: AlertRule, row: dict[str, Any]) -> dict[str, Any] | None:
    value = _num(row.get(rule.metric_field))
    if value is None:
        logger.warning("Skipping invalid %s for %s", rule.metric_field, row.get("website"))
        return None
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
    prefix = "[TEST] " if alert.get("test_mode") else ""
    return f"{prefix}[{severity}] AI.DNA Platform Intelligence: {site} Ahrefs traffic below {threshold:,} ({value:,})"


def _email_html(alert: dict[str, Any]) -> str:
    severity = str(alert["email_severity"]).capitalize()
    tone = "#b91c1c" if alert["email_severity"] == "critical" else "#b45309"
    site = html.escape(str(alert["website"]))
    domain = html.escape(str(alert.get("domain") or ""))
    snapshot_date = html.escape(str(alert.get("snapshot_date") or ""))
    value = int(alert["metric_value"])
    threshold = int(alert["threshold"])
    test_notice = "<p><strong>Delivery test only. No source data has been changed.</strong></p>" if alert.get("test_mode") else ""
    action = html.escape(_recommended_action(alert))
    return f"""
    <div style="font-family:Arial,sans-serif;line-height:1.55;color:#172033;max-width:680px">
      <p style="margin:0 0 16px;font-size:13px;color:#61708a">AI.DNA Platform Intelligence</p>
      {test_notice}
      <h1 style="margin:0 0 12px;font-size:22px;color:{tone}">{severity} Ahrefs traffic alert</h1>
      <p style="font-size:15px;margin:0 0 18px">
        {site} is below the defined threshold in the latest imported Ahrefs snapshot. This is not a live traffic measurement.
      </p>
      <table style="width:100%;border-collapse:collapse;margin:0 0 18px">
        <tr><td style="padding:8px;border:1px solid #e5e7eb"><strong>Website</strong></td><td style="padding:8px;border:1px solid #e5e7eb">{site}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e5e7eb"><strong>Domain</strong></td><td style="padding:8px;border:1px solid #e5e7eb">{domain or "-"}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e5e7eb"><strong>Latest Ahrefs traffic</strong></td><td style="padding:8px;border:1px solid #e5e7eb">{value:,}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e5e7eb"><strong>Threshold</strong></td><td style="padding:8px;border:1px solid #e5e7eb">{threshold:,}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e5e7eb"><strong>Snapshot date</strong></td><td style="padding:8px;border:1px solid #e5e7eb">{snapshot_date}</td></tr>
      </table>
      <p><strong>Recommended action:</strong> {action}</p>
      <p style="font-size:13px;color:#61708a;margin:20px 0 0">
        This alert is sent once per website and severity per day to avoid duplicate notifications.
      </p>
    </div>
    """


def _email_text(alert: dict[str, Any]) -> str:
    severity = str(alert["email_severity"]).upper()
    return (
        ("DELIVERY TEST ONLY. No source data has been changed.\n\n" if alert.get("test_mode") else "") +
        f"{severity} Ahrefs traffic alert\n\n"
        f"Website: {alert['website']}\n"
        f"Domain: {alert.get('domain') or '-'}\n"
        f"Latest Ahrefs traffic: {int(alert['metric_value']):,}\n"
        f"Threshold: {int(alert['threshold']):,}\n"
        f"Snapshot date: {alert.get('snapshot_date')}\n\n"
        "Based on the latest imported Ahrefs snapshot, not a live traffic measurement.\n\n"
        f"Recommended action: {_recommended_action(alert)}\n"
    )


def _recommended_action(alert: dict[str, Any]) -> str:
    try:
        snapshot = datetime.fromisoformat(str(alert.get("snapshot_date") or "")).date()
        age = (datetime.now(timezone.utc).date() - snapshot).days
    except ValueError:
        age = None
    if age is not None and age > 7:
        return f"This imported snapshot is {age} days old. Refresh the Ahrefs import before assessing current traffic, then review keyword and page movement."
    return "Review keyword movement, affected pages, internal linking gaps and backlink changes."


def has_email_been_sent(dedupe_key: str) -> bool:
    try:
        result = pg.select_rows(
            "ai_alert_tracking",
            columns="alert_fingerprint",
            filters={"alert_fingerprint": dedupe_key},
            limit=1,
        )
        return bool(result)
    except Exception as exc:
        raise RuntimeError("Cannot check email history; refusing to send unchecked duplicates") from exc


def record_email_sent(dedupe_key: str, alert: dict[str, Any], recipients: list[str], email_id: str | None = None) -> None:
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
            "resend_email_id": email_id,
        },
    }
    try:
        pg.upsert_rows("ai_alert_tracking", [payload], "alert_fingerprint")
    except Exception as exc:
        raise RuntimeError("Email accepted, but recording its delivery failed; retry uses the same provider idempotency key") from exc


def send_email(alert: dict[str, Any], recipients: list[str], dry_run: bool, dedupe_key: str | None = None) -> str | None:
    requested = {email.strip().lower() for email in recipients}
    if requested == set(TEST_RECIPIENTS):
        recipients = TEST_RECIPIENTS.copy()
    elif requested == set(DEFAULT_RECIPIENTS) and not alert.get("test_mode"):
        recipients = DEFAULT_RECIPIENTS.copy()
    else:
        raise ValueError("Alerts are restricted to Sujal, Neeraj and Jon; test emails must go only to Sujal")
    if dry_run:
        logger.info("[dry-run] Would send %s to %s", _email_subject(alert), ", ".join(recipients))
        return None
    if not RESEND_API_KEY:
        raise RuntimeError("RESEND_API_KEY is required to send Ahrefs traffic alerts")
    headers = {
        "Authorization": f"Bearer {RESEND_API_KEY}",
        "Content-Type": "application/json",
        "Idempotency-Key": hashlib.sha256((dedupe_key or _dedupe_key(
            AHREFS_TRAFFIC_RULE, alert["website"], alert["email_severity"], _today_iso(), recipients
        )).encode("utf-8")).hexdigest(),
    }
    payload = {
        "from": RESEND_FROM_EMAIL,
        "to": recipients,
        "subject": _email_subject(alert),
        "html": _email_html(alert),
        "text": _email_text(alert),
    }
    for attempt in range(3):
        try:
            response = requests.post("https://api.resend.com/emails", headers=headers, json=payload, timeout=30)
        except (requests.Timeout, requests.ConnectionError):
            if attempt == 2:
                raise RuntimeError("Resend could not be reached after three attempts") from None
            time.sleep(2 ** attempt)
            continue
        if response.status_code != 429 and response.status_code < 500:
            break
        if attempt < 2:
            try:
                delay = float(response.headers.get("Retry-After", 2 ** attempt))
                if not math.isfinite(delay):
                    delay = 2 ** attempt
            except (ValueError, TypeError):
                delay = 2 ** attempt
            time.sleep(max(1, min(delay, 60)))
    if not response.ok:
        raise RuntimeError(f"Resend returned {response.status_code}: {response.text[:300]}")
    email_id = response.json().get("id")
    if not email_id:
        raise RuntimeError("Resend did not return an email ID")
    logger.info("Resend accepted %s alert for %s (email ID: %s)", alert["email_severity"], alert["website"], email_id)
    return email_id


def upsert_dashboard_alerts(alerts: list[dict[str, Any]], evaluated_sites: list[str] | None = None) -> None:
    latest = pg.select_rows(
        "daily_insights",
        columns="date,v2_site_reports",
        order_by="date",
        desc=True,
        limit=1,
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
    # Remove previous threshold snapshots/severities, including recovered sites.
    # Other priority alerts must remain untouched.
    report_keys = {site.lower(): site for site in site_reports}
    for source_site in evaluated_sites or [alert["website"] for alert in alerts]:
        site = report_keys.get(source_site.lower(), source_site)
        report = site_reports.get(site)
        if not isinstance(report, dict):
            continue
        existing = report.get("critical_alerts") or []
        retained = [item for item in existing if item.get("type") != "ahrefs_traffic_drop"]
        if retained != existing:
            report["critical_alerts"] = retained
            changed = True
    for alert in alerts:
        site = report_keys.get(alert["website"].lower(), alert["website"])
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
            if item.get("type") != "ahrefs_traffic_drop"
        ]
        report["critical_alerts"] = [dashboard_alert, *existing_alerts][:4]
        site_reports[site] = report
        changed = True

    if changed:
        pg.update_where("daily_insights", {"v2_site_reports": site_reports}, {"date": date_value})
        logger.info("Merged %d Ahrefs threshold alert(s) into daily_insights.%s", len(alerts), date_value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Check Ahrefs organic traffic threshold alerts")
    parser.add_argument("--dry-run", action="store_true", help="Evaluate alerts without sending emails or writing dashboard alerts")
    parser.add_argument("--test-recipient", action="store_true", help="Send to Sujal only for testing")
    parser.add_argument("--send", action="store_true", help="Actually send email alerts")
    parser.add_argument("--skip-email", action="store_true", help="Only evaluate and write dashboard alerts; do not send emails")
    parser.add_argument("--skip-dashboard", action="store_true", help="Do not merge active alerts into daily_insights")
    args = parser.parse_args()

    if not pg.enabled():
        raise RuntimeError("SEODASH_DATABASE_URL is required")

    rows = fetch_latest_ahrefs_overviews()
    alerts = [alert for row in rows if (alert := evaluate_rule(AHREFS_TRAFFIC_RULE, row))]
    logger.info("Evaluated %d website(s), found %d active Ahrefs threshold alert(s)", len(rows), len(alerts))

    if not args.skip_dashboard and not args.dry_run:
        upsert_dashboard_alerts(alerts, [str(row["website"]) for row in rows if _num(row.get(AHREFS_TRAFFIC_RULE.metric_field)) is not None])

    if args.skip_email:
        logger.info("Email send skipped. active=%d", len(alerts))
        return

    recipients = TEST_RECIPIENTS if args.test_recipient else DEFAULT_RECIPIENTS
    run_date = _today_iso()
    sent = 0
    skipped = 0
    for alert in alerts:
        dedupe_key = _dedupe_key(AHREFS_TRAFFIC_RULE, alert["website"], alert["email_severity"], run_date, recipients)
        if has_email_been_sent(dedupe_key):
            skipped += 1
            logger.info("Skipping duplicate daily email for %s %s", alert["website"], alert["email_severity"])
            continue
        if args.send or args.dry_run:
            email_id = send_email(alert, recipients, args.dry_run, dedupe_key)
            if not args.dry_run:
                record_email_sent(dedupe_key, alert, recipients, email_id)
            sent += 1
        else:
            logger.info("Email send disabled. Use --send to send: %s", _email_subject(alert))

    logger.info("Ahrefs traffic alert check complete. sent=%d skipped=%d active=%d", sent, skipped, len(alerts))


if __name__ == "__main__":
    main()
