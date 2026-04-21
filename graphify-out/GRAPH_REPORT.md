# Graph Report - D:\Ztudium\Data Consolidation\ztudium-data-pipeline  (2026-04-21)

## Corpus Check
- 14 files · ~60,757 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 373 nodes · 734 edges · 29 communities detected
- Extraction: 94% EXTRACTED · 6% INFERRED · 0% AMBIGUOUS · INFERRED: 42 edges (avg confidence: 0.8)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]
- [[_COMMUNITY_Community 8|Community 8]]
- [[_COMMUNITY_Community 9|Community 9]]
- [[_COMMUNITY_Community 10|Community 10]]
- [[_COMMUNITY_Community 11|Community 11]]
- [[_COMMUNITY_Community 12|Community 12]]
- [[_COMMUNITY_Community 13|Community 13]]
- [[_COMMUNITY_Community 14|Community 14]]
- [[_COMMUNITY_Community 15|Community 15]]
- [[_COMMUNITY_Community 16|Community 16]]
- [[_COMMUNITY_Community 17|Community 17]]
- [[_COMMUNITY_Community 18|Community 18]]
- [[_COMMUNITY_Community 19|Community 19]]
- [[_COMMUNITY_Community 20|Community 20]]
- [[_COMMUNITY_Community 21|Community 21]]
- [[_COMMUNITY_Community 22|Community 22]]
- [[_COMMUNITY_Community 23|Community 23]]
- [[_COMMUNITY_Community 24|Community 24]]
- [[_COMMUNITY_Community 25|Community 25]]
- [[_COMMUNITY_Community 26|Community 26]]
- [[_COMMUNITY_Community 27|Community 27]]
- [[_COMMUNITY_Community 28|Community 28]]

## God Nodes (most connected - your core abstractions)
1. `clean_text()` - 22 edges
2. `verify_one()` - 19 edges
3. `main()` - 17 edges
4. `process_site()` - 16 edges
5. `normalize_key()` - 15 edges
6. `generate_content_plan()` - 15 edges
7. `clean_text()` - 14 edges
8. `process_site()` - 13 edges
9. `_generate_ai_suggestions_for_scope()` - 13 edges
10. `parse_keyword_gap_file()` - 12 edges

## Surprising Connections (you probably didn't know these)
- `response_error_text()` --calls--> `clean_text()`  [INFERRED]
  D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\auto_publish_arvow.py → D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\generate_insights.py
- `extract_arvow_payload()` --calls--> `build_site_arvow_config()`  [INFERRED]
  D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\auto_publish_arvow.py → D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\generate_daily_content_opportunities.py
- `extract_arvow_payload()` --calls--> `clean_text()`  [INFERRED]
  D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\auto_publish_arvow.py → D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\generate_insights.py
- `fetch_site_candidates()` --calls--> `clean_text()`  [INFERRED]
  D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\auto_publish_arvow.py → D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\generate_insights.py
- `has_cannibalization_conflict()` --calls--> `clean_text()`  [INFERRED]
  D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\auto_publish_arvow.py → D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\generate_insights.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.08
Nodes (55): batch_upsert(), _build_donor_payload(), _build_existing_link_pairs(), _build_keyword_map(), _build_page_enrichment(), _build_rule_based_suggestions(), _build_target_payload(), _coerce_confidence() (+47 more)

### Community 1 - "Community 1"
Cohesion: 0.07
Nodes (45): clean_text(), compute_baselines(), compute_keyword_movers(), compute_opportunity_score(), compute_page_movers(), extract_core_topic(), find_question_keyword(), gather_context() (+37 more)

### Community 2 - "Community 2"
Cohesion: 0.12
Nodes (43): has_cannibalization_conflict(), url_signature(), analyze_site(), build_prioritized_candidates(), build_site_arvow_config(), build_site_datasets(), build_site_payload(), build_stored_arvow_payload() (+35 more)

### Community 3 - "Community 3"
Cohesion: 0.09
Nodes (40): Configuration — loads from environment variables (set by GitHub Secrets or .env), If GOOGLE_CREDENTIALS_JSON env var is set (GitHub Actions),     write it to a t, setup_google_credentials(), batch_upsert(), _classify_api_error(), _execute_ga4_report(), _execute_gsc_query(), _extract_status_code() (+32 more)

### Community 4 - "Community 4"
Cohesion: 0.09
Nodes (35): parse_args(), categorize_file(), detect_website(), extract_snapshot_date(), _finish_ingestion_run(), main(), parse_backlinks(), parse_broken_backlinks() (+27 more)

### Community 5 - "Community 5"
Cohesion: 0.15
Nodes (26): _as_float(), _as_int(), batch_upsert(), _calc_opportunity_score(), dedupe_rows(), _detect_clusters(), _detect_competitor_domains(), _detect_intent() (+18 more)

### Community 6 - "Community 6"
Cohesion: 0.2
Nodes (19): auto_publish_sites(), candidate_url_from_response(), configure_logging(), fetch_arvow_status(), insert_publish_history(), latest_publish_history(), main(), now_iso() (+11 more)

### Community 7 - "Community 7"
Cohesion: 0.22
Nodes (18): auto_publish_sites(), build_history_payload(), configure_logging(), count_attempted_today(), dispatch_payload(), extract_arvow_payload(), fetch_site_candidates(), insert_publish_history() (+10 more)

### Community 8 - "Community 8"
Cohesion: 0.15
Nodes (18): compute_7day_avg(), compute_percentile(), compute_trends_for_website(), detect_cross_site_patterns(), determine_severity(), fetch_daily_data(), get_all_websites(), main() (+10 more)

### Community 9 - "Community 9"
Cohesion: 0.22
Nodes (17): apply_validation_update(), backlink_present(), chunked(), classify_url(), detect_soft_404(), extract_title(), extract_visible_text(), fetch_rows_by_status() (+9 more)

### Community 10 - "Community 10"
Cohesion: 0.23
Nodes (12): clear_bucket(), get_export_dir(), main(), Upload Ahrefs CSV exports to Supabase Storage.  Run this locally AFTER run_exp, Upload all CSV and TXT files from export_dir to Supabase Storage.          Fil, Trigger the process-ahrefs workflow via GitHub API., Common headers for Supabase Storage REST API., Find the export directory. (+4 more)

### Community 11 - "Community 11"
Cohesion: 0.46
Nodes (7): clear_bucket(), get_export_dir(), main(), Upload keyword gap CSV files to Supabase Storage.  Run locally after exporting k, _storage_headers(), trigger_workflow(), upload_files()

### Community 12 - "Community 12"
Cohesion: 0.67
Nodes (1): Run SQL migrations in database/migrations against SUPABASE_DB_URL.

### Community 13 - "Community 13"
Cohesion: 1.0
Nodes (1): Prompt constants for the daily content opportunity generator.

### Community 14 - "Community 14"
Cohesion: 1.0
Nodes (1): If GOOGLE_CREDENTIALS_JSON env var is set (GitHub Actions),     write it to a t

### Community 15 - "Community 15"
Cohesion: 1.0
Nodes (1): Threaded spinner that shows progress during long-running API calls.

### Community 16 - "Community 16"
Cohesion: 1.0
Nodes (1): Print a clean section header.

### Community 17 - "Community 17"
Cohesion: 1.0
Nodes (1): Print a prominent title box.

### Community 18 - "Community 18"
Cohesion: 1.0
Nodes (1): Execute a Supabase query with error handling. Returns [] on failure.

### Community 19 - "Community 19"
Cohesion: 1.0
Nodes (1): Gather comprehensive dashboard data for AI analysis.

### Community 20 - "Community 20"
Cohesion: 1.0
Nodes (1): Compare keyword positions week-over-week.     Returns top gainers and losers wit

### Community 21 - "Community 21"
Cohesion: 1.0
Nodes (1): Compare page traffic week-over-week.     Returns top gaining and losing pages.

### Community 22 - "Community 22"
Cohesion: 1.0
Nodes (1): Compute 30-day and 90-day baselines for key metrics per website.

### Community 23 - "Community 23"
Cohesion: 1.0
Nodes (1): Build a prioritized context string for GPT.     Order: Anomalies > Keyword mover

### Community 24 - "Community 24"
Cohesion: 1.0
Nodes (1): Send prioritized context to GPT-4o-mini and get structured insights.

### Community 25 - "Community 25"
Cohesion: 1.0
Nodes (1): Extract the core head term from a longtail keyword by stripping qualifiers.

### Community 26 - "Community 26"
Cohesion: 1.0
Nodes (1): Compute opportunity score: high volume + low difficulty = high score.

### Community 27 - "Community 27"
Cohesion: 1.0
Nodes (1): Generate topic-based keyword clusters for content briefs.

### Community 28 - "Community 28"
Cohesion: 1.0
Nodes (1): Store insights and content plan in daily_insights table.

## Knowledge Gaps
- **106 isolated node(s):** `Run SQL migrations in database/migrations against SUPABASE_DB_URL.`, `Automatically dispatch top daily content opportunities to Arvow for selected sit`, `compute_trends.py — Enhanced Trend Analysis Engine v2 Computes DoD, WoW, MoM per`, `Calculate percentage change, returns None if either value is missing.`, `Compute 7-day moving average from a list of values.` (+101 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 13`** (2 nodes): `daily_content_prompt_config.py`, `Prompt constants for the daily content opportunity generator.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 14`** (1 nodes): `If GOOGLE_CREDENTIALS_JSON env var is set (GitHub Actions),     write it to a t`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 15`** (1 nodes): `Threaded spinner that shows progress during long-running API calls.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 16`** (1 nodes): `Print a clean section header.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 17`** (1 nodes): `Print a prominent title box.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 18`** (1 nodes): `Execute a Supabase query with error handling. Returns [] on failure.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 19`** (1 nodes): `Gather comprehensive dashboard data for AI analysis.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 20`** (1 nodes): `Compare keyword positions week-over-week.     Returns top gainers and losers wit`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 21`** (1 nodes): `Compare page traffic week-over-week.     Returns top gaining and losing pages.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 22`** (1 nodes): `Compute 30-day and 90-day baselines for key metrics per website.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 23`** (1 nodes): `Build a prioritized context string for GPT.     Order: Anomalies > Keyword mover`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 24`** (1 nodes): `Send prioritized context to GPT-4o-mini and get structured insights.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 25`** (1 nodes): `Extract the core head term from a longtail keyword by stripping qualifiers.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 26`** (1 nodes): `Compute opportunity score: high volume + low difficulty = high score.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 27`** (1 nodes): `Generate topic-based keyword clusters for content briefs.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 28`** (1 nodes): `Store insights and content plan in daily_insights table.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `main()` connect `Community 4` to `Community 0`?**
  _High betweenness centrality (0.244) - this node is a cross-community bridge._
- **Why does `parse_args()` connect `Community 4` to `Community 11`, `Community 5`, `Community 6`?**
  _High betweenness centrality (0.205) - this node is a cross-community bridge._
- **Why does `clean_text()` connect `Community 1` to `Community 2`, `Community 6`, `Community 7`?**
  _High betweenness centrality (0.173) - this node is a cross-community bridge._
- **Are the 12 inferred relationships involving `clean_text()` (e.g. with `response_error_text()` and `extract_arvow_payload()`) actually correct?**
  _`clean_text()` has 12 INFERRED edges - model-reasoned connections that need verification._
- **Are the 2 inferred relationships involving `verify_one()` (e.g. with `clean_text()` and `build_site_arvow_config()`) actually correct?**
  _`verify_one()` has 2 INFERRED edges - model-reasoned connections that need verification._
- **Are the 2 inferred relationships involving `main()` (e.g. with `parse_args()` and `parse_args()`) actually correct?**
  _`main()` has 2 INFERRED edges - model-reasoned connections that need verification._
- **Are the 2 inferred relationships involving `process_site()` (e.g. with `build_site_arvow_config()` and `clean_text()`) actually correct?**
  _`process_site()` has 2 INFERRED edges - model-reasoned connections that need verification._