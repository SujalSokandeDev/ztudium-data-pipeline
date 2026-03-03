# Ztudium Data Pipeline

Automated data pipeline for 9 Ztudium websites. Fetches GSC + GA4 data daily via GitHub Actions and processes Ahrefs CSV exports via Supabase Storage.

## Architecture

```
Daily (automatic):   GitHub Actions → GSC/GA4 API → Supabase DB
Weekly (semi-manual): Local Ahrefs export → Supabase Storage → GitHub Actions → Supabase DB
```

## What's Automated

| Task | Frequency | How |
|------|-----------|-----|
| GSC + GA4 fetch | Daily 6 AM IST | GitHub Actions cron |
| Ahrefs CSV processing | After local export | GitHub Actions (triggered) |
| Supabase DB updates | Automatic | Both workflows |

## What's Manual

| Task | Frequency | Time |
|------|-----------|------|
| Ahrefs browser export | Weekly | ~50 min |

---

## Setup (One-Time)

### Step 1: Create GitHub Repository

1. Go to [github.com/new](https://github.com/new)
2. **Name**: `ztudium-data-pipeline`
3. **Visibility**: Public ✅ (unlimited Actions minutes)
4. **Don't** initialize with README (we'll push our code)
5. Click **Create Repository**

### Step 2: Create Supabase Storage Bucket

1. Go to your Supabase Dashboard → **Storage**
2. Click **New Bucket**
3. **Name**: `ahrefs-exports`
4. **Public bucket**: No (keep private)
5. Click **Create Bucket**

### Step 3: Add GitHub Secrets

Go to your repo → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

Add these secrets one by one:

| Secret Name | Where to Find It |
|-------------|-------------------|
| `SUPABASE_URL` | Supabase → Settings → API → Project URL |
| `SUPABASE_SERVICE_KEY` | Supabase → Settings → API → service_role key |
| `GOOGLE_CREDENTIALS_JSON` | Copy the ENTIRE contents of `google-credentials.json` |
| `GSC_PROPERTY_CITIESABC` | Your `.env` file (e.g. `sc-domain:citiesabc.com`) |
| `GSC_PROPERTY_BUSINESSABC` | Your `.env` file |
| `GSC_PROPERTY_HEDGETHINK` | Your `.env` file |
| `GSC_PROPERTY_FASHIONABC` | Your `.env` file |
| `GSC_PROPERTY_TRADERSDNA` | Your `.env` file |
| `GSC_PROPERTY_FREEDOMX` | Your `.env` file |
| `GSC_PROPERTY_WISDOMIA` | Your `.env` file |
| `GSC_PROPERTY_SPORTSDNA` | Your `.env` file |
| `GSC_PROPERTY_INTELLIGENTHQ` | Your `.env` file |
| `GA4_PROPERTY_CITIESABC` | Your `.env` file (numeric ID) |
| `GA4_PROPERTY_BUSINESSABC` | Your `.env` file |
| `GA4_PROPERTY_HEDGETHINK` | Your `.env` file |
| `GA4_PROPERTY_FASHIONABC` | Your `.env` file |
| `GA4_PROPERTY_TRADERSDNA` | Your `.env` file |
| `GA4_PROPERTY_FREEDOMX` | Your `.env` file |
| `GA4_PROPERTY_WISDOMIA` | Your `.env` file |
| `GA4_PROPERTY_SPORTSDNA` | Your `.env` file |
| `GA4_PROPERTY_INTELLIGENTHQ` | Your `.env` file |

### Step 4: Push Code to GitHub

```bash
cd "d:\Ztudium\Data Consolidation\ztudium-data-pipeline"
git init
git add .
git commit -m "Initial pipeline setup"
git branch -M main
git remote add origin https://github.com/ztudium/ztudium-data-pipeline.git
git push -u origin main
```

### Step 5: Create Local .env

```bash
copy .env.example .env
# Edit .env with your actual values (copy from dashboard backend .env)
```

### Step 6: Test the Daily Workflow

1. Go to your repo → **Actions** → **Daily Google Fetch**
2. Click **Run workflow** → **Run workflow**
3. Watch the logs — should show data fetched for all 9 sites

---

## Weekly Usage (After Setup)

### 1. Run Ahrefs export (local, ~50 min)
```bash
cd "d:\Ztudium\Data Consolidation\ahrefs-automation"
python run_export.py
```

### 2. Upload CSVs to cloud + trigger processing
```bash
cd "d:\Ztudium\Data Consolidation\ztudium-data-pipeline"
python scripts/upload_csvs.py
```

That's it! The GitHub Action will download the CSVs, process them, and upload to Supabase.

---

## File Structure

```
ztudium-data-pipeline/
├── .github/
│   └── workflows/
│       ├── daily-google-fetch.yml   # Cron: GSC+GA4 → Supabase
│       └── process-ahrefs.yml       # Triggered: Ahrefs CSVs → Supabase
├── scripts/
│   ├── config.py                    # Environment-based config
│   ├── fetch_google.py              # GSC + GA4 fetcher
│   ├── upload_csvs.py               # Local → Supabase Storage
│   └── process_ahrefs.py            # Storage → Parse → Supabase DB
├── .env.example                     # Template for local .env
├── .gitignore
├── requirements.txt
└── README.md
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| "Credentials not found" | Check `GOOGLE_CREDENTIALS_JSON` secret contains the full JSON |
| GSC returns 0 rows | Verify service account email is added to each GSC property |
| GA4 returns 0 rows | Verify service account has Viewer role in each GA4 property |
| Storage upload fails | Check `ahrefs-exports` bucket exists in Supabase Storage |
| Workflow not running | Check Actions is enabled in repo Settings → Actions → General |
