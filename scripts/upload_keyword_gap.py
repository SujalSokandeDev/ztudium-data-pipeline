"""
Upload keyword gap CSV files to SEODash storage.

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
from config import KEYWORD_GAP_BUCKET
from seodash_storage import clear_bucket as clear_storage_bucket, upload_file

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


def get_export_dir():
    env_dir = os.getenv("KEYWORD_GAP_EXPORT_DIR", "").strip()
    if env_dir and os.path.isdir(env_dir):
        return os.path.abspath(env_dir)

    for path in DEFAULT_EXPORT_DIRS:
        if os.path.isdir(path):
            return path
    return None


def clear_bucket():
    try:
        deleted = clear_storage_bucket(KEYWORD_GAP_BUCKET)
        logger.info("Deleted %d old files from bucket '%s'", deleted, KEYWORD_GAP_BUCKET)
    except Exception as exc:
        logger.warning("Bucket cleanup warning: %s", str(exc)[:160])


def upload_files(export_dir: str):
    files = sorted(glob.glob(os.path.join(export_dir, "*.csv")))
    if not files:
        logger.error("No CSV files found in %s", export_dir)
        return False

    logger.info("Uploading %d files from %s", len(files), export_dir)
    uploaded = 0
    failed = 0
    for path in files:
        name = os.path.basename(path)
        try:
            upload_file(
                KEYWORD_GAP_BUCKET,
                path,
                name,
                metadata={"source": "upload_keyword_gap", "upload_date": date.today().isoformat()},
            )
            uploaded += 1
            logger.info("  OK  %s", name)
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
    parser = argparse.ArgumentParser(description="Upload keyword gap CSVs to SEODash storage")
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
