"""
Upload keyword gap CSV files to Supabase Storage.

Run locally after exporting keyword-gap files from Ahrefs:
    python scripts/upload_keyword_gap.py
"""

import glob
import argparse
import logging
import os
import sys
import time
from datetime import date

import requests

sys.path.insert(0, os.path.dirname(__file__))
from config import KEYWORD_GAP_BUCKET, SUPABASE_SERVICE_KEY, SUPABASE_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("upload_keyword_gap")

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
DEFAULT_EXPORT_DIRS = [
    os.path.abspath(os.path.join(PROJECT_ROOT, "..", "Keyword Gap")),
    os.path.abspath(os.path.join(PROJECT_ROOT, "..", "keyword gap")),
    os.path.abspath(os.path.join(PROJECT_ROOT, "..", "Content Gap")),
    os.path.abspath(os.path.join(PROJECT_ROOT, "..", "content gap")),
    os.path.abspath(
        os.path.join(
            PROJECT_ROOT,
            "..",
            "data-consolidation-dashboard",
            "Content Gap",
        )
    ),
]


def _storage_headers():
    return {
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "apikey": SUPABASE_SERVICE_KEY,
    }


def get_export_dir():
    env_dir = os.getenv("KEYWORD_GAP_EXPORT_DIR", "").strip()
    if env_dir and os.path.isdir(env_dir):
        return os.path.abspath(env_dir)

    for path in DEFAULT_EXPORT_DIRS:
        if os.path.isdir(path):
            return path
    return None


def clear_bucket():
    logger.info("Clearing old files from bucket '%s'...", KEYWORD_GAP_BUCKET)
    list_url = f"{SUPABASE_URL}/storage/v1/object/list/{KEYWORD_GAP_BUCKET}"
    headers = _storage_headers()

    try:
        resp = requests.post(
            list_url,
            headers=headers,
            json={"prefix": "", "limit": 1000},
            timeout=30,
        )
        if resp.status_code != 200:
            logger.warning("Could not list bucket (HTTP %d)", resp.status_code)
            return
        files = [item.get("name", "") for item in resp.json()]
        files = [f for f in files if f]
        if not files:
            logger.info("Bucket already empty")
            return
        base_url = f"{SUPABASE_URL}/storage/v1/object/{KEYWORD_GAP_BUCKET}"
        deleted = 0
        for name in files:
            r = requests.delete(f"{base_url}/{name}", headers=headers, timeout=30)
            if r.status_code in (200, 204):
                deleted += 1
        logger.info("Deleted %d old files", deleted)
    except Exception as exc:
        logger.warning("Bucket cleanup warning: %s", str(exc)[:160])


def upload_files(export_dir: str):
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.error("SUPABASE_URL or SUPABASE_SERVICE_KEY is missing")
        return False

    files = sorted(glob.glob(os.path.join(export_dir, "*.csv")))
    if not files:
        logger.error("No CSV files found in %s", export_dir)
        return False

    logger.info("Uploading %d files from %s", len(files), export_dir)
    upload_base = f"{SUPABASE_URL}/storage/v1/object/{KEYWORD_GAP_BUCKET}"
    headers = _storage_headers()

    uploaded = 0
    failed = 0
    for path in files:
        name = os.path.basename(path)
        try:
            with open(path, "rb") as f:
                resp = requests.post(
                    f"{upload_base}/{name}",
                    headers={
                        **headers,
                        "Content-Type": "text/csv",
                        "x-upsert": "true",
                    },
                    data=f,
                    timeout=120,
                )
            if resp.status_code in (200, 201):
                uploaded += 1
                logger.info("  OK  %s", name)
            else:
                failed += 1
                logger.error("  ERR %s (HTTP %d)", name, resp.status_code)
        except Exception as exc:
            failed += 1
            logger.error("  ERR %s (%s)", name, str(exc)[:120])
        time.sleep(0.25)

    logger.info("Upload complete: %d uploaded, %d failed", uploaded, failed)
    return failed == 0


def trigger_workflow():
    token = os.getenv("GITHUB_TOKEN", "").strip()
    repo = os.getenv("GITHUB_REPO", "SujalSokandeDev/ztudium-data-pipeline")
    if not token:
        logger.info("No GITHUB_TOKEN set. Trigger workflow manually: process-keyword-gap.yml")
        return

    dispatch_url = (
        f"https://api.github.com/repos/{repo}/actions/workflows/"
        "process-keyword-gap.yml/dispatches"
    )
    resp = requests.post(
        dispatch_url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
        },
        json={"ref": "main", "inputs": {"date_folder": date.today().isoformat()}},
        timeout=30,
    )
    if resp.status_code == 204:
        logger.info("Triggered GitHub workflow: process-keyword-gap.yml")
    else:
        logger.error(
            "Failed to trigger workflow (HTTP %d): %s",
            resp.status_code,
            resp.text[:200],
        )


def main():
    parser = argparse.ArgumentParser(description="Upload keyword gap CSVs to Supabase storage")
    parser.add_argument(
        "--dir",
        dest="export_dir",
        default="",
        help="Optional explicit folder path for keyword gap CSV files",
    )
    parser.add_argument(
        "--skip-clear",
        action="store_true",
        help="Do not clear bucket before upload",
    )
    parser.add_argument(
        "--skip-trigger",
        action="store_true",
        help="Do not trigger GitHub Actions workflow after upload",
    )
    args = parser.parse_args()

    print()
    print("=" * 58)
    print("  KEYWORD GAP CSV -> STORAGE -> GITHUB ACTION")
    print("=" * 58)

    export_dir = os.path.abspath(args.export_dir) if args.export_dir else get_export_dir()
    if not export_dir:
        logger.error("No export folder found. Set KEYWORD_GAP_EXPORT_DIR.")
        sys.exit(1)

    if not args.skip_clear:
        clear_bucket()
    ok = upload_files(export_dir)
    if ok and not args.skip_trigger:
        trigger_workflow()
    print("\nDone." if ok else "\nCompleted with errors.")


if __name__ == "__main__":
    main()
