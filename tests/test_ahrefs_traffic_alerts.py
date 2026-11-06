import copy
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import check_ahrefs_traffic_alerts as alerts


def snapshot(value=2200, site="CitiesABC"):
    return {"website": site, "date": "2026-11-06", "domain": "citiesabc.com", "organic_traffic": value}


def evaluate(value=2200, site="CitiesABC"):
    return alerts.evaluate_rule(alerts.AHREFS_TRAFFIC_RULE, snapshot(value, site))


class ThresholdTests(unittest.TestCase):
    def test_boundaries_and_formatted_numbers(self):
        for value, severity in [(0, "critical"), (1999, "critical"), (2000, "warning"),
                                (2499, "warning"), (2500, None), (9000, None),
                                ("1,999", "critical"), ("2.2k", "warning"), ("1m", None)]:
            with self.subTest(value=value):
                result = evaluate(value)
                self.assertEqual(result["email_severity"] if result else None, severity)

    def test_missing_invalid_and_non_finite_data_never_send_critical(self):
        for value in [None, "", "N/A", "bad", -1, True, False, float("nan"), float("inf"), "-3k"]:
            with self.subTest(value=value):
                self.assertIsNone(evaluate(value))

    def test_severity_is_consistent_in_subject_body_and_dashboard(self):
        for value, severity, dashboard in [(1900, "critical", "high"), (2200, "warning", "medium")]:
            alert = evaluate(value)
            self.assertEqual(alert["severity"], dashboard)
            self.assertTrue(alerts._email_subject(alert).startswith(f"[{severity.upper()}]"))
            self.assertIn(f"{severity.capitalize()} Ahrefs traffic alert", alerts._email_html(alert))
            self.assertIn(f"{severity.upper()} Ahrefs traffic alert", alerts._email_text(alert))

    def test_email_escapes_external_values(self):
        alert = evaluate(site='<script>alert("x")</script>')
        self.assertNotIn("<script>", alerts._email_html(alert))
        self.assertIn("&lt;script&gt;", alerts._email_html(alert))

    def test_generic_rule_supports_another_metric(self):
        rule = alerts.AlertRule("keywords_floor", "ahrefs_overview", "keywords", "Keywords",
                                (alerts.Threshold("warning", 10, "Low keywords"),))
        self.assertEqual(alerts.evaluate_rule(rule, {**snapshot(), "keywords": 5})["metric_value"], 5)

    @patch.object(alerts.pg, "select_rows")
    def test_latest_snapshot_is_case_insensitive_and_not_peak_traffic(self, select):
        select.return_value = [snapshot(1900), {**snapshot(8000, "citiesabc"), "date": "2026-11-01"},
                               snapshot(2400, "BusinessABC"), snapshot(12, "")]
        rows = alerts.fetch_latest_ahrefs_overviews()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1]["organic_traffic"], 1900)


class DeliveryTests(unittest.TestCase):
    def test_final_recipients_are_approved_team_and_tests_remain_sujal_only(self):
        self.assertEqual(alerts.DEFAULT_RECIPIENTS, ["sujal.sokande@ztudium.com", "neeraj.rajpal@ztudium.com", "jonathan.vdb@ztudium.com"])
        self.assertEqual(alerts.TEST_RECIPIENTS, ["sujal.sokande@ztudium.com"])
        with patch.object(alerts.requests, "post") as post:
            for recipients in [[], ["neeraj.rajpal@ztudium.com"], alerts.DEFAULT_RECIPIENTS + ["other@example.com"]]:
                with self.assertRaises(ValueError):
                    alerts.send_email(evaluate(), recipients, False)
            post.assert_not_called()
        with self.assertRaises(ValueError):
            alerts.send_email({**evaluate(), "test_mode": True}, alerts.DEFAULT_RECIPIENTS, False)

    def test_dedupe_is_scoped_to_day_site_severity_and_recipients(self):
        def key(site="CitiesABC", severity="warning", day="2026-11-06", recipients=None):
            return alerts._dedupe_key(alerts.AHREFS_TRAFFIC_RULE, site, severity, day,
                                      recipients or alerts.DEFAULT_RECIPIENTS)
        self.assertEqual(key(), key(site="citiesabc"))
        self.assertNotEqual(key(), key(severity="critical"))
        self.assertNotEqual(key(), key(day="2026-11-07"))
        self.assertNotEqual(key(), key(site="BusinessABC"))
        self.assertNotEqual(key(), key(recipients=["other@example.com"]))

    @patch.object(alerts.requests, "post")
    @patch.object(alerts, "RESEND_API_KEY", "test-key")
    def test_send_payload_provider_idempotency_and_no_cc(self, post):
        post.return_value = Mock(ok=True, status_code=200, json=lambda: {"id": "email-123"})
        for _ in range(2):
            self.assertTrue(alerts.send_email(evaluate(), alerts.DEFAULT_RECIPIENTS, False, "same-key"))
        first, second = post.call_args_list
        self.assertEqual(first.kwargs["headers"]["Idempotency-Key"], second.kwargs["headers"]["Idempotency-Key"])
        payload = first.kwargs["json"]
        self.assertEqual(payload["to"], alerts.DEFAULT_RECIPIENTS)
        self.assertNotIn("cc", payload)
        self.assertNotIn("bcc", payload)
        self.assertEqual(first.kwargs["timeout"], 30)

    @patch.object(alerts.requests, "post")
    def test_dry_run_never_calls_provider(self, post):
        alerts.send_email(evaluate(), alerts.DEFAULT_RECIPIENTS, True)
        post.assert_not_called()

    def test_normal_alert_is_not_marked_test_and_stale_snapshot_is_explained(self):
        alert = evaluate()
        self.assertNotIn("[TEST]", alerts._email_subject(alert))
        self.assertNotIn("Delivery test only", alerts._email_html(alert))
        alert["snapshot_date"] = "2020-01-01"
        self.assertIn("Refresh the Ahrefs import", alerts._email_text(alert))
        alert["snapshot_date"] = alerts._today_iso()
        self.assertNotIn("days old", alerts._email_text(alert))

    @patch.object(alerts, "RESEND_API_KEY", "")
    def test_missing_key_fails_clearly(self):
        with self.assertRaisesRegex(RuntimeError, "RESEND_API_KEY"):
            alerts.send_email(evaluate(), alerts.DEFAULT_RECIPIENTS, False)

    @patch.object(alerts.requests, "post")
    @patch.object(alerts, "RESEND_API_KEY", "test-key")
    def test_rejection_or_missing_email_id_is_not_success(self, post):
        for response in [Mock(ok=False, status_code=403, text="Unverified sender"), Mock(ok=True, status_code=200, json=lambda: {})]:
            post.return_value = response
            with self.assertRaises(RuntimeError):
                alerts.send_email(evaluate(), alerts.DEFAULT_RECIPIENTS, False)

    @patch.object(alerts.time, "sleep")
    @patch.object(alerts.requests, "post")
    @patch.object(alerts, "RESEND_API_KEY", "test-key")
    def test_transient_failure_retries_same_payload_and_key(self, post, sleep):
        success = Mock(ok=True, status_code=200, json=lambda: {"id": "email-123"})
        for failure in [Mock(ok=False, status_code=429, headers={"Retry-After": "2"}),
                        Mock(ok=False, status_code=503, headers={}), alerts.requests.Timeout()]:
            post.reset_mock()
            post.side_effect = [failure, success]
            self.assertEqual(alerts.send_email(evaluate(), alerts.DEFAULT_RECIPIENTS, False), "email-123")
            self.assertEqual(post.call_args_list[0], post.call_args_list[1])
        self.assertTrue(sleep.called)

    @patch.object(alerts.time, "sleep")
    @patch.object(alerts.requests, "post")
    @patch.object(alerts, "RESEND_API_KEY", "test-key")
    def test_retries_are_bounded(self, post, sleep):
        post.return_value = Mock(ok=False, status_code=429, headers={"Retry-After": "999"}, text="Rate limit")
        with self.assertRaisesRegex(RuntimeError, "429"):
            alerts.send_email(evaluate(), alerts.DEFAULT_RECIPIENTS, False)
        self.assertEqual(post.call_count, 3)
        sleep.assert_called_with(60)

    @patch.object(alerts.pg, "select_rows", side_effect=RuntimeError("unavailable"))
    def test_history_failure_stops_unchecked_send(self, select):
        with self.assertRaisesRegex(RuntimeError, "refusing"):
            alerts.has_email_been_sent("key")

    @patch.object(alerts.pg, "upsert_rows", side_effect=RuntimeError("unavailable"))
    def test_record_failure_is_visible(self, upsert):
        with self.assertRaisesRegex(RuntimeError, "recording"):
            alerts.record_email_sent("key", evaluate(), alerts.DEFAULT_RECIPIENTS)


class DashboardTests(unittest.TestCase):
    def merge(self, incoming, sites, reports):
        with patch.object(alerts.pg, "select_rows", return_value=[{"date": "2026-11-06", "v2_site_reports": copy.deepcopy(reports)}]), \
             patch.object(alerts.pg, "update_where") as update:
            alerts.upsert_dashboard_alerts(incoming, sites)
            return update.call_args.args[1]["v2_site_reports"] if update.called else reports

    def test_warning_escalates_without_duplicate_or_losing_other_alerts(self):
        existing = {"CitiesABC": {"critical_alerts": [{"type": "ahrefs_traffic_drop", "severity": "medium"}, {"type": "ranking_loss"}]}}
        result = self.merge([evaluate(1900)], ["CitiesABC"], existing)
        rows = result["CitiesABC"]["critical_alerts"]
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["severity"], "high")
        self.assertEqual(rows[1]["type"], "ranking_loss")
        self.assertEqual(self.merge([evaluate(1900)], ["CitiesABC"], result), result)

    def test_recovered_site_clears_threshold_alert_only(self):
        existing = {"CitiesABC": {"critical_alerts": [{"type": "ahrefs_traffic_drop"}, {"type": "ranking_loss"}]},
                    "BusinessABC": {"critical_alerts": [{"type": "ahrefs_traffic_drop"}]}}
        result = self.merge([], ["CitiesABC"], existing)
        self.assertEqual(result["CitiesABC"]["critical_alerts"], [{"type": "ranking_loss"}])
        self.assertEqual(result["BusinessABC"], existing["BusinessABC"])

    def test_new_site_is_added_and_priority_feed_has_required_text(self):
        result = self.merge([evaluate()], ["CitiesABC"], {})
        alert = result["CitiesABC"]["critical_alerts"][0]
        for field in ["title", "what_happened", "why_it_matters", "action", "severity", "alert_fingerprint"]:
            self.assertTrue(alert[field])

    def test_site_casing_does_not_duplicate_reports(self):
        existing = {"CitiesABC": {"critical_alerts": [{"type": "ahrefs_traffic_drop", "severity": "medium"}]}}
        result = self.merge([evaluate(1900, "citiesabc")], ["citiesabc"], existing)
        self.assertEqual(list(result), ["CitiesABC"])
        self.assertEqual(len(result["CitiesABC"]["critical_alerts"]), 1)


class WorkflowTests(unittest.TestCase):
    def test_delivery_failure_is_not_hidden_and_runs_are_serialized(self):
        workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/ahrefs-traffic-alerts.yml").read_text()
        self.assertNotIn("continue-on-error: true", workflow)
        self.assertIn("cancel-in-progress: false", workflow)
        self.assertNotIn("secrets.RESEND_FROM_EMAIL", workflow)
        self.assertIn("noreply@citiesabc.com", workflow)


if __name__ == "__main__":
    unittest.main()
