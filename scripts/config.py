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
        "category": "smart cities, urban innovation, sustainability, and city ecosystems",
        "audience": "city leaders, policymakers, urban planners, researchers, and innovation-focused readers",
    },
    {
        "name": "BusinessABC",
        "slug": "businessabc",
        "gsc_property": os.getenv("GSC_PROPERTY_BUSINESSABC", ""),
        "ga4_property_id": os.getenv("GA4_PROPERTY_BUSINESSABC", ""),
        "category": "global business atlas, companies, leadership, investment, and market intelligence",
        "audience": "business professionals, investors, founders, executives, and corporate researchers",
    },
    {
        "name": "HedgeThink",
        "slug": "hedgethink",
        "gsc_property": os.getenv("GSC_PROPERTY_HEDGETHINK", ""),
        "ga4_property_id": os.getenv("GA4_PROPERTY_HEDGETHINK", ""),
        "category": "finance, hedge funds, fintech, leadership, and capital markets",
        "audience": "finance professionals, investors, asset managers, and fintech readers",
    },
    {
        "name": "FashionABC",
        "slug": "fashionabc",
        "gsc_property": os.getenv("GSC_PROPERTY_FASHIONABC", ""),
        "ga4_property_id": os.getenv("GA4_PROPERTY_FASHIONABC", ""),
        "category": "fashion, luxury, beauty, designers, and brand culture",
        "audience": "fashion industry readers, luxury brand followers, designers, and style-focused professionals",
    },
    {
        "name": "TradersDNA",
        "slug": "tradersdna",
        "gsc_property": os.getenv("GSC_PROPERTY_TRADERSDNA", ""),
        "ga4_property_id": os.getenv("GA4_PROPERTY_TRADERSDNA", ""),
        "category": "trading, forex, investing, brokers, and financial markets",
        "audience": "retail traders, investors, and market participants",
    },
    {
        "name": "FreedomX",
        "slug": "freedomx",
        "gsc_property": os.getenv("GSC_PROPERTY_FREEDOMX", ""),
        "ga4_property_id": os.getenv("GA4_PROPERTY_FREEDOMX", ""),
        "category": "crypto, blockchain, digital assets, web3, and token ecosystems",
        "audience": "crypto investors, traders, builders, and digital-asset readers",
    },
    {
        "name": "Wisdomia",
        "slug": "wisdomia",
        "gsc_property": os.getenv("GSC_PROPERTY_WISDOMIA", ""),
        "ga4_property_id": os.getenv("GA4_PROPERTY_WISDOMIA", ""),
        "category": "knowledge, explainers, future trends, education, and thought leadership",
        "audience": "curious professionals, learners, and readers looking for explanatory content",
    },
    {
        "name": "SportsDNA",
        "slug": "sportsdna",
        "gsc_property": os.getenv("GSC_PROPERTY_SPORTSDNA", ""),
        "ga4_property_id": os.getenv("GA4_PROPERTY_SPORTSDNA", ""),
        "category": "sports business, sports technology, athlete performance, and sports industry analysis",
        "audience": "sports professionals, analysts, fans, and performance-focused readers",
    },
    {
        "name": "IntelligentHQ",
        "slug": "intelligenthq",
        "gsc_property": os.getenv("GSC_PROPERTY_INTELLIGENTHQ", ""),
        "ga4_property_id": os.getenv("GA4_PROPERTY_INTELLIGENTHQ", ""),
        "category": "business innovation, technology, digital transformation, leadership, and global trends",
        "audience": "executives, founders, professionals, and innovation-focused readers",
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
