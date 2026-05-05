"""
Quick audit: test GSC + GA4 access for every configured website.
Run: python scripts/audit_access.py
"""
import os, sys, json
sys.path.insert(0, os.path.dirname(__file__))
from config import WEBSITES, setup_google_credentials

def main():
    creds_path = setup_google_credentials()

    from google.oauth2 import service_account

    # ── GSC ──
    print("\n" + "=" * 70)
    print("  GSC ACCESS AUDIT")
    print("=" * 70)
    try:
        from googleapiclient.discovery import build
        gsc_creds = service_account.Credentials.from_service_account_file(
            creds_path, scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
        )
        gsc = build("searchconsole", "v1", credentials=gsc_creds, cache_discovery=False)

        # Get all verified sites
        site_list_resp = gsc.sites().list().execute()
        verified_sites = [s["siteUrl"] for s in site_list_resp.get("siteEntry", [])]
        print(f"\n  Service account has access to {len(verified_sites)} GSC properties:")
        for s in sorted(verified_sites):
            print(f"    - {s}")
    except Exception as e:
        print(f"  GSC init FAILED: {e}")
        verified_sites = []

    print("\n  Per-website GSC check:")
    for ws in WEBSITES:
        name = ws["name"]
        gsc_prop = (ws.get("gsc_property", "") or "").strip()
        if not gsc_prop:
            print(f"  {name:20s} | GSC: NOT CONFIGURED")
            continue

        # Check if in verified list
        match = gsc_prop in verified_sites or gsc_prop.rstrip("/") in verified_sites or (gsc_prop + "/") in verified_sites
        if match:
            # Try a quick query
            try:
                from datetime import date, timedelta
                end = date.today() - timedelta(days=3)
                start = end - timedelta(days=3)
                resp = gsc.searchanalytics().query(
                    siteUrl=gsc_prop,
                    body={"startDate": start.isoformat(), "endDate": end.isoformat(), "dimensions": ["date"], "rowLimit": 1}
                ).execute()
                rows = len(resp.get("rows", []))
                print(f"  {name:20s} | GSC: OK ({gsc_prop}) — {rows} rows returned")
            except Exception as e:
                print(f"  {name:20s} | GSC: QUERY FAILED ({gsc_prop}) — {str(e)[:100]}")
        else:
            print(f"  {name:20s} | GSC: NO ACCESS ({gsc_prop}) — not in verified sites list")

    # ── GA4 ──
    print("\n" + "=" * 70)
    print("  GA4 ACCESS AUDIT")
    print("=" * 70)
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import (
            RunReportRequest, DateRange, Dimension, Metric,
        )
        ga4_creds = service_account.Credentials.from_service_account_file(
            creds_path, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
        )
        ga4 = BetaAnalyticsDataClient(credentials=ga4_creds)
    except Exception as e:
        print(f"  GA4 init FAILED: {e}")
        ga4 = None

    print("\n  Per-website GA4 check:")
    from datetime import date, timedelta
    for ws in WEBSITES:
        name = ws["name"]
        ga4_id = (str(ws.get("ga4_property_id", "")) or "").strip()
        if not ga4_id:
            print(f"  {name:20s} | GA4: NOT CONFIGURED (empty property ID)")
            continue
        if not ga4_id.isdigit():
            print(f"  {name:20s} | GA4: INVALID ID '{ga4_id}' (must be numeric)")
            continue
        if not ga4:
            print(f"  {name:20s} | GA4: SKIPPED (client unavailable)")
            continue

        try:
            end = date.today() - timedelta(days=1)
            start = end - timedelta(days=3)
            req = RunReportRequest(
                property=f"properties/{ga4_id}",
                date_ranges=[DateRange(start_date=start.isoformat(), end_date=end.isoformat())],
                dimensions=[Dimension(name="date")],
                metrics=[Metric(name="sessions")],
            )
            resp = ga4.run_report(req)
            total = sum(int(r.metric_values[0].value) for r in resp.rows) if resp.rows else 0
            print(f"  {name:20s} | GA4: OK (property {ga4_id}) — {total} sessions in last 3 days")
        except Exception as e:
            err = str(e)[:120]
            if "PERMISSION_DENIED" in err or "403" in err:
                print(f"  {name:20s} | GA4: NO ACCESS (property {ga4_id}) — permission denied")
            else:
                print(f"  {name:20s} | GA4: ERROR (property {ga4_id}) — {err}")

    print("\n" + "=" * 70)
    print("  AUDIT COMPLETE")
    print("=" * 70 + "\n")

if __name__ == "__main__":
    main()
