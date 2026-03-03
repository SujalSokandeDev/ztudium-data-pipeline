"""
Upload Ahrefs CSV exports to Supabase Storage.

Run this locally AFTER run_export.py finishes:
    python scripts/upload_csvs.py

Flow:
  1. DELETES all old files from the storage bucket (no stale data)
  2. Uploads new CSV/TXT files from the export folder
  3. Triggers the GitHub Actions workflow to process them
  4. GitHub Action downloads → parses → uploads to Supabase DB

Requires:
  - SUPABASE_URL and SUPABASE_SERVICE_KEY in .env
  - The 'ahrefs-exports' bucket to exist in Supabase Storage
"""

import os
import sys
import time
import logging
import glob
import requests
from datetime import date

sys.path.insert(0, os.path.dirname(__file__))
from config import SUPABASE_URL, SUPABASE_SERVICE_KEY, AHREFS_BUCKET

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("upload_csvs")

# Default export folders (check both)
DEFAULT_EXPORT_DIRS = [
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "Ahref's exported Data"),
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "data-consolidation-dashboard", "Ahrefs Imports"),
]


def _storage_headers():
    """Common headers for Supabase Storage REST API."""
    return {
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "apikey": SUPABASE_SERVICE_KEY,
    }


def get_export_dir():
    """Find the export directory."""
    # Check env var first
    env_dir = os.getenv("AHREFS_EXPORT_DIR")
    if env_dir and os.path.isdir(env_dir):
        return os.path.abspath(env_dir)

    # Check defaults
    for d in DEFAULT_EXPORT_DIRS:
        resolved = os.path.abspath(d)
        if os.path.isdir(resolved):
            return resolved

    return None


# ══════════════════════════════════════════════════════════════
#  Step 1: Clear old files from bucket
# ══════════════════════════════════════════════════════════════

def clear_bucket():
    """Delete ALL existing files from the storage bucket.
    
    This ensures we don't accumulate old exports and waste storage.
    Called before every upload so only the latest files exist.
    """
    logger.info("🗑️  Clearing old files from bucket '%s'...", AHREFS_BUCKET)
    headers = _storage_headers()

    # List everything in the bucket (root level)
    list_url = f"{SUPABASE_URL}/storage/v1/object/list/{AHREFS_BUCKET}"
    try:
        resp = requests.post(
            list_url,
            headers=headers,
            json={"prefix": "", "limit": 1000},
            timeout=30,
        )
        if resp.status_code != 200:
            logger.warning("  Could not list bucket: HTTP %d", resp.status_code)
            return

        items = resp.json()
        if not items:
            logger.info("  Bucket is already empty")
            return

        # Collect all file paths to delete
        file_paths = []
        for item in items:
            name = item.get("name", "")
            if name:
                file_paths.append(name)
            # Check if it's a folder (has id but might contain sub-items)
            item_id = item.get("id")
            if item_id and not name:
                # It's likely a folder metadata entry, skip
                continue

        # Also check for date-prefixed folders (from previous uploads)
        # List items inside each folder
        folder_names = [item.get("name", "") for item in items 
                        if item.get("metadata") is None and item.get("name")]
        
        for folder in folder_names:
            try:
                sub_resp = requests.post(
                    list_url,
                    headers=headers,
                    json={"prefix": f"{folder}/", "limit": 500},
                    timeout=30,
                )
                if sub_resp.status_code == 200:
                    sub_items = sub_resp.json()
                    for si in sub_items:
                        sub_name = si.get("name", "")
                        if sub_name:
                            file_paths.append(f"{folder}/{sub_name}")
            except Exception:
                pass

        if not file_paths:
            logger.info("  No files to delete")
            return

        # Delete files in batch via the remove endpoint
        delete_url = f"{SUPABASE_URL}/storage/v1/object/{AHREFS_BUCKET}"
        
        # Supabase delete endpoint accepts a list of prefixes
        for path in file_paths:
            try:
                resp = requests.delete(
                    f"{delete_url}/{path}",
                    headers=headers,
                    timeout=30,
                )
                if resp.status_code in (200, 204):
                    logger.info("  🗑️  Deleted: %s", path)
                else:
                    logger.debug("  Could not delete %s: HTTP %d", path, resp.status_code)
            except Exception as e:
                logger.debug("  Error deleting %s: %s", path, str(e)[:50])

        logger.info("  ✅ Cleared %d old files", len(file_paths))

    except Exception as e:
        logger.warning("  Error clearing bucket: %s", str(e)[:80])
        logger.info("  Continuing with upload anyway (old files will be overwritten)")


# ══════════════════════════════════════════════════════════════
#  Step 2: Upload new files
# ══════════════════════════════════════════════════════════════

def upload_to_supabase_storage(export_dir: str):
    """Upload all CSV and TXT files from export_dir to Supabase Storage.
    
    Files are uploaded flat (no date subfolder) since we clear the bucket first.
    """
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.error("SUPABASE_URL or SUPABASE_SERVICE_KEY not set")
        return False

    # Collect files
    files = []
    for ext in ["*.csv", "*.txt"]:
        files.extend(glob.glob(os.path.join(export_dir, ext)))

    if not files:
        logger.warning("No CSV/TXT files found in %s", export_dir)
        return False

    logger.info("📤 Uploading %d files from: %s", len(files), export_dir)

    upload_url = f"{SUPABASE_URL}/storage/v1/object/{AHREFS_BUCKET}"
    headers = _storage_headers()

    uploaded = 0
    failed = 0
    total_size = 0

    for filepath in sorted(files):
        filename = os.path.basename(filepath)
        file_size = os.path.getsize(filepath)

        try:
            with open(filepath, "rb") as f:
                content_type = "text/csv" if filename.endswith(".csv") else "text/plain"
                resp = requests.post(
                    f"{upload_url}/{filename}",
                    headers={
                        **headers,
                        "Content-Type": content_type,
                        "x-upsert": "true",
                    },
                    data=f,
                    timeout=120,
                )

            if resp.status_code in (200, 201):
                uploaded += 1
                total_size += file_size
                mb = file_size / 1024 / 1024
                logger.info("  ✅ %s (%.2f MB)", filename, mb)
            else:
                failed += 1
                logger.error("  ❌ %s — HTTP %d: %s", filename, resp.status_code, resp.text[:100])

        except Exception as e:
            failed += 1
            logger.error("  ❌ %s — %s", filename, str(e)[:100])

        time.sleep(0.3)  # Rate limiting

    total_mb = total_size / 1024 / 1024
    logger.info("")
    logger.info("Upload complete: %d/%d files (%.1f MB total), %d failed",
                uploaded, len(files), total_mb, failed)
    return failed == 0


# ══════════════════════════════════════════════════════════════
#  Step 3: Trigger GitHub Action
# ══════════════════════════════════════════════════════════════

def trigger_github_action():
    """Trigger the process-ahrefs workflow via GitHub API."""
    token = os.getenv("GITHUB_TOKEN")
    repo = os.getenv("GITHUB_REPO", "ztudium/ztudium-data-pipeline")

    if not token:
        logger.info("")
        logger.info("ℹ️  No GITHUB_TOKEN set — skipping auto-trigger.")
        logger.info("   To process the uploaded files:")
        logger.info("   → Go to GitHub Actions → 'Process Ahrefs CSVs' → Run workflow")
        logger.info("   Or set GITHUB_TOKEN in .env to auto-trigger next time")
        return

    today = date.today().isoformat()
    url = f"https://api.github.com/repos/{repo}/actions/workflows/process-ahrefs.yml/dispatches"
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.v3+json",
        },
        json={
            "ref": "main",
            "inputs": {"date_folder": today},
        },
        timeout=30,
    )

    if resp.status_code == 204:
        logger.info("🚀 GitHub Action triggered — processing will start automatically!")
        logger.info("   Check progress at: https://github.com/%s/actions", repo)
    else:
        logger.error("❌ Failed to trigger: HTTP %d — %s", resp.status_code, resp.text[:100])


# ══════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════

def main():
    print()
    print("=" * 60)
    print("  AHREFS CSV → SUPABASE STORAGE → GITHUB ACTION")
    print(f"  Date: {date.today().isoformat()}")
    print("=" * 60)

    export_dir = get_export_dir()
    if not export_dir:
        logger.error("Export directory not found!")
        logger.error("Set AHREFS_EXPORT_DIR in .env or check default paths.")
        sys.exit(1)

    # Count files
    files = []
    for ext in ["*.csv", "*.txt"]:
        files.extend(glob.glob(os.path.join(export_dir, ext)))
    
    print(f"  Export folder: {export_dir}")
    print(f"  Files found:   {len(files)}")
    print(f"  Bucket:        {AHREFS_BUCKET}")
    print()

    # Step 1: Clear old files
    clear_bucket()
    print()

    # Step 2: Upload new files
    success = upload_to_supabase_storage(export_dir)

    # Step 3: Trigger processing
    if success:
        print()
        trigger_github_action()

    print()
    print("✅ Done!" if success else "⚠️ Done with errors — check logs above")


if __name__ == "__main__":
    main()
