"""Send at most two labelled validation emails to Sujal; never write production data."""

import argparse
import json
import time

import requests

import check_ahrefs_traffic_alerts as alerts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--send-live", action="store_true", help="Send one real warning and one critical delivery test")
    args = parser.parse_args()
    selected = {}
    for row in alerts.fetch_latest_ahrefs_overviews():
        alert = alerts.evaluate_rule(alerts.AHREFS_TRAFFIC_RULE, row)
        if alert:
            selected.setdefault(alert["email_severity"], {**alert, "test_mode": True})
    if set(selected) != {"warning", "critical"}:
        raise RuntimeError("Both severities must be present in real source data to run this check")

    for severity in ("warning", "critical"):
        alert = selected[severity]
        key = f"validation:seodash-27:{alerts._today_iso()}:{severity}"
        email_id = alerts.send_email(alert, alerts.TEST_RECIPIENTS, not args.send_live, key)
        if not email_id:
            continue
        # Repeating the exact request must return the same ID without another send.
        time.sleep(1)
        repeated_id = alerts.send_email(alert, alerts.TEST_RECIPIENTS, False, key)
        if repeated_id != email_id:
            raise RuntimeError("Provider idempotency check failed")
        event = "unknown"
        for _ in range(12):
            time.sleep(5)
            response = requests.get(f"https://api.resend.com/emails/{email_id}",
                                    headers={"Authorization": f"Bearer {alerts.RESEND_API_KEY}"}, timeout=15)
            # Some accounts expose sent-email status in the list before GET by ID.
            if response.status_code == 404:
                response = requests.get("https://api.resend.com/emails?limit=100",
                                        headers={"Authorization": f"Bearer {alerts.RESEND_API_KEY}"}, timeout=15)
                response.raise_for_status()
                delivery = next((item for item in response.json().get("data", []) if item.get("id") == email_id), None)
                if not delivery:
                    continue
            else:
                response.raise_for_status()
                delivery = response.json()
            if delivery.get("to") != alerts.TEST_RECIPIENTS or delivery.get("cc") or delivery.get("bcc"):
                raise RuntimeError("Unexpected delivery recipients")
            event = delivery.get("last_event", "unknown")
            if event in {"delivered", "opened", "clicked", "bounced", "failed", "suppressed"}:
                break
        print(json.dumps({"severity": severity, "website": alert["website"],
                          "snapshot_date": alert["snapshot_date"], "traffic": alert["metric_value"],
                          "email_id": email_id, "recipient": alerts.TEST_RECIPIENTS,
                          "last_event": event, "duplicate_request_same_id": repeated_id == email_id}))
        if event not in {"delivered", "opened", "clicked"}:
            raise RuntimeError(f"Delivery not confirmed: {event}")


if __name__ == "__main__":
    main()
