"""Opt-in integration test using session-local temp tables, never production writes."""

import contextlib
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import check_ahrefs_traffic_alerts as alerts


@unittest.skipUnless(os.getenv("RUN_AHREFS_DB_TESTS") == "1", "Set RUN_AHREFS_DB_TESTS=1 for temp-table integration")
class DatabaseIntegrationTests(unittest.TestCase):
    def test_repeat_run_escalation_and_recovery_against_real_schema(self):
        conn = alerts.pg.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("CREATE TEMP TABLE daily_insights AS SELECT * FROM public.daily_insights ORDER BY date DESC LIMIT 1")
                cur.execute("CREATE TEMP TABLE ai_alert_tracking AS SELECT * FROM public.ai_alert_tracking WHERE false")
                cur.execute("CREATE UNIQUE INDEX ON ai_alert_tracking (alert_fingerprint)")
                cur.execute("CREATE TEMP TABLE ahrefs_overview AS SELECT * FROM public.ahrefs_overview")
                for name in ("daily_insights", "ai_alert_tracking", "ahrefs_overview"):
                    cur.execute("SELECT pg_my_temp_schema() = relnamespace FROM pg_class WHERE oid = %s::regclass", (name,))
                    self.assertTrue(cur.fetchone()[0], f"Refusing to use a non-temporary table: {name}")
                cur.execute("UPDATE ahrefs_overview SET organic_traffic = 2300 WHERE website = 'CitiesABC'")
            conn.commit()

            @contextlib.contextmanager
            def borrowed_connection():
                yield conn

            response = Mock(ok=True, status_code=200, json=lambda: {"id": "integration-mocked-provider-id"})
            with patch.object(alerts.pg, "connect", borrowed_connection), \
                 patch.object(alerts, "RESEND_API_KEY", "integration-test-not-real"), \
                 patch.object(alerts.requests, "post", return_value=response) as post, \
                 patch.object(sys, "argv", ["check", "--send", "--test-recipient"]):
                alerts.main()
                first_count = post.call_count
                self.assertGreater(first_count, 0)
                for call in post.call_args_list:
                    self.assertEqual(call.kwargs["json"]["to"], alerts.TEST_RECIPIENTS)
                alerts.main()
                self.assertEqual(post.call_count, first_count, "A repeated daily run must not resend")
                with conn.cursor() as cur:
                    cur.execute("UPDATE ahrefs_overview SET organic_traffic = 1900 WHERE website = 'CitiesABC'")
                alerts.main()
                self.assertEqual(post.call_count, first_count + 1, "Critical escalation needs one new email")
                with conn.cursor() as cur:
                    cur.execute("UPDATE ahrefs_overview SET organic_traffic = 3000 WHERE website = 'CitiesABC'")
                alerts.main()
                self.assertEqual(post.call_count, first_count + 1)
                reports = alerts.pg.select_rows("daily_insights", columns="v2_site_reports", order_by="date", desc=True, limit=1)[0]["v2_site_reports"]
                self.assertFalse(any(item["type"] == "ahrefs_traffic_drop" for item in reports["CitiesABC"]["critical_alerts"]))
                self.assertEqual(len(alerts.pg.select_rows("ai_alert_tracking")), first_count + 1)
                print(f"Temp-table integration: {first_count} initial alerts; repeat deduped; escalation +1; recovery cleared; no live emails or production writes")
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
