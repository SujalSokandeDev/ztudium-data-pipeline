"""
Configuration — loads from environment variables (set by GitHub Secrets or .env).
"""

import os
import json
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── Supabase ──────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL", "")

# ── Google Credentials ────────────────────────────────────────
# In GitHub Actions, the JSON content is stored as a secret and
# written to a temp file before the script runs.
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "./google-credentials.json")

# ── Storage bucket ────────────────────────────────────────────
AHREFS_BUCKET = os.getenv("AHREFS_STORAGE_BUCKET", "ahrefs-exports")
KEYWORD_GAP_BUCKET = os.getenv("KEYWORD_GAP_STORAGE_BUCKET", "keyword_gap")

# ── Websites ──────────────────────────────────────────────────
WEBSITES = [
    {
        "name": "CitiesABC",
        "slug": "citiesabc",
        "gsc_property": os.getenv("GSC_PROPERTY_CITIESABC", ""),
        "ga4_property_id": os.getenv("GA4_PROPERTY_CITIESABC", ""),
    },
    {
        "name": "BusinessABC",
        "slug": "businessabc",
        "gsc_property": os.getenv("GSC_PROPERTY_BUSINESSABC", ""),
        "ga4_property_id": os.getenv("GA4_PROPERTY_BUSINESSABC", ""),
    },
    {
        "name": "HedgeThink",
        "slug": "hedgethink",
        "gsc_property": os.getenv("GSC_PROPERTY_HEDGETHINK", ""),
        "ga4_property_id": os.getenv("GA4_PROPERTY_HEDGETHINK", ""),
    },
    {
        "name": "FashionABC",
        "slug": "fashionabc",
        "gsc_property": os.getenv("GSC_PROPERTY_FASHIONABC", ""),
        "ga4_property_id": os.getenv("GA4_PROPERTY_FASHIONABC", ""),
    },
    {
        "name": "TradersDNA",
        "slug": "tradersdna",
        "gsc_property": os.getenv("GSC_PROPERTY_TRADERSDNA", ""),
        "ga4_property_id": os.getenv("GA4_PROPERTY_TRADERSDNA", ""),
    },
    {
        "name": "FreedomX",
        "slug": "freedomx",
        "gsc_property": os.getenv("GSC_PROPERTY_FREEDOMX", ""),
        "ga4_property_id": os.getenv("GA4_PROPERTY_FREEDOMX", ""),
    },
    {
        "name": "Wisdomia",
        "slug": "wisdomia",
        "gsc_property": os.getenv("GSC_PROPERTY_WISDOMIA", ""),
        "ga4_property_id": os.getenv("GA4_PROPERTY_WISDOMIA", ""),
    },
    {
        "name": "SportsDNA",
        "slug": "sportsdna",
        "gsc_property": os.getenv("GSC_PROPERTY_SPORTSDNA", ""),
        "ga4_property_id": os.getenv("GA4_PROPERTY_SPORTSDNA", ""),
    },
    {
        "name": "IntelligentHQ",
        "slug": "intelligenthq",
        "gsc_property": os.getenv("GSC_PROPERTY_INTELLIGENTHQ", ""),
        "ga4_property_id": os.getenv("GA4_PROPERTY_INTELLIGENTHQ", ""),
    },
]


def setup_google_credentials():
    """
    If GOOGLE_CREDENTIALS_JSON env var is set (GitHub Actions),
    write it to a temp file and set GOOGLE_APPLICATION_CREDENTIALS.
    """
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if creds_json:
        creds_path = "/tmp/google-credentials.json"
        with open(creds_path, "w") as f:
            f.write(creds_json)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
        logger.info("Google credentials written to %s", creds_path)
        return creds_path
    return GOOGLE_CREDENTIALS_PATH
