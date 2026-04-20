# Graph Report - D:\Ztudium\Data Consolidation\ztudium-data-pipeline  (2026-04-20)

## Corpus Check
- 12 files · ~46,739 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 298 nodes · 569 edges · 12 communities detected
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 5 edges (avg confidence: 0.8)
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

## God Nodes (most connected - your core abstractions)
1. `main()` - 16 edges
2. `process_site()` - 13 edges
3. `_generate_ai_suggestions_for_scope()` - 13 edges
4. `clean_text()` - 12 edges
5. `parse_keyword_gap_file()` - 12 edges
6. `main()` - 10 edges
7. `extract_snapshot_date()` - 10 edges
8. `_parse_number()` - 10 edges
9. `upload_parsed_data()` - 10 edges
10. `build_site_datasets()` - 9 edges

## Surprising Connections (you probably didn't know these)
- `main()` --calls--> `parse_args()`  [INFERRED]
  D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\process_ahrefs.py → D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\generate_daily_content_opportunities.py
- `main()` --calls--> `parse_args()`  [INFERRED]
  D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\process_keyword_gap.py → D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\generate_daily_content_opportunities.py
- `main()` --calls--> `parse_args()`  [INFERRED]
  D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\upload_keyword_gap.py → D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\generate_daily_content_opportunities.py
- `main()` --calls--> `parse_args()`  [INFERRED]
  D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\validate_backlink_urls.py → D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\generate_daily_content_opportunities.py
- `setup_google_credentials()` --calls--> `init_google_apis()`  [INFERRED]
  D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\config.py → D:\Ztudium\Data Consolidation\ztudium-data-pipeline\scripts\fetch_google.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.06
Nodes (72): _build_donor_payload(), _build_existing_link_pairs(), _build_keyword_map(), _build_page_enrichment(), _build_rule_based_suggestions(), _build_target_payload(), categorize_file(), _coerce_confidence() (+64 more)

### Community 1 - "Community 1"
Cohesion: 0.09
Nodes (40): Configuration — loads from environment variables (set by GitHub Secrets or .env), If GOOGLE_CREDENTIALS_JSON env var is set (GitHub Actions),     write it to a t, setup_google_credentials(), batch_upsert(), _classify_api_error(), _execute_ga4_report(), _execute_gsc_query(), _extract_status_code() (+32 more)

### Community 2 - "Community 2"
Cohesion: 0.14
Nodes (37): analyze_site(), build_prioritized_candidates(), build_site_arvow_config(), build_site_datasets(), build_site_payload(), build_stored_arvow_payload(), clean_text(), clean_url() (+29 more)

### Community 3 - "Community 3"
Cohesion: 0.08
Nodes (32): compute_baselines(), compute_keyword_movers(), compute_opportunity_score(), compute_page_movers(), extract_core_topic(), find_question_keyword(), gather_context(), generate_content_plan() (+24 more)

### Community 4 - "Community 4"
Cohesion: 0.15
Nodes (26): _as_float(), _as_int(), batch_upsert(), _calc_opportunity_score(), dedupe_rows(), _detect_clusters(), _detect_competitor_domains(), _detect_intent() (+18 more)

### Community 5 - "Community 5"
Cohesion: 0.21
Nodes (18): apply_validation_update(), backlink_present(), chunked(), classify_url(), detect_soft_404(), extract_title(), extract_visible_text(), fetch_rows_by_status() (+10 more)

### Community 6 - "Community 6"
Cohesion: 0.15
Nodes (18): compute_7day_avg(), compute_percentile(), compute_trends_for_website(), detect_cross_site_patterns(), determine_severity(), fetch_daily_data(), get_all_websites(), main() (+10 more)

### Community 7 - "Community 7"
Cohesion: 0.14
Nodes (15): batch_upsert(), _dedupe_rows_by_keys(), delete_where(), _is_retryable_upsert_error(), Batch upsert with key normalization., Keep only the last row for each unique key combination within a batch., Detect transient transport/rate-limit errors that benefit from retry., Upsert one chunk using supabase-py compatible parameters. (+7 more)

### Community 8 - "Community 8"
Cohesion: 0.23
Nodes (12): clear_bucket(), get_export_dir(), main(), Upload Ahrefs CSV exports to Supabase Storage.  Run this locally AFTER run_exp, Upload all CSV and TXT files from export_dir to Supabase Storage.          Fil, Trigger the process-ahrefs workflow via GitHub API., Common headers for Supabase Storage REST API., Find the export directory. (+4 more)

### Community 9 - "Community 9"
Cohesion: 0.46
Nodes (7): clear_bucket(), get_export_dir(), main(), Upload keyword gap CSV files to Supabase Storage.  Run locally after exporting k, _storage_headers(), trigger_workflow(), upload_files()

### Community 10 - "Community 10"
Cohesion: 0.67
Nodes (1): Run SQL migrations in database/migrations against SUPABASE_DB_URL.

### Community 11 - "Community 11"
Cohesion: 1.0
Nodes (1): Prompt constants for the daily content opportunity generator.

## Knowledge Gaps
- **83 isolated node(s):** `Run SQL migrations in database/migrations against SUPABASE_DB_URL.`, `compute_trends.py — Enhanced Trend Analysis Engine v2 Computes DoD, WoW, MoM per`, `Calculate percentage change, returns None if either value is missing.`, `Compute 7-day moving average from a list of values.`, `Compute where current value ranks in historical values (0-100).     0 = lowest e` (+78 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 11`** (2 nodes): `daily_content_prompt_config.py`, `Prompt constants for the daily content opportunity generator.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `parse_args()` connect `Community 2` to `Community 0`, `Community 9`, `Community 4`, `Community 5`?**
  _High betweenness centrality (0.253) - this node is a cross-community bridge._
- **Why does `main()` connect `Community 0` to `Community 2`, `Community 7`?**
  _High betweenness centrality (0.189) - this node is a cross-community bridge._
- **Why does `main()` connect `Community 4` to `Community 2`?**
  _High betweenness centrality (0.092) - this node is a cross-community bridge._
- **What connects `Run SQL migrations in database/migrations against SUPABASE_DB_URL.`, `compute_trends.py — Enhanced Trend Analysis Engine v2 Computes DoD, WoW, MoM per`, `Calculate percentage change, returns None if either value is missing.` to the rest of the system?**
  _83 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 0` be split into smaller, more focused modules?**
  _Cohesion score 0.06 - nodes in this community are weakly interconnected._
- **Should `Community 1` be split into smaller, more focused modules?**
  _Cohesion score 0.09 - nodes in this community are weakly interconnected._
- **Should `Community 2` be split into smaller, more focused modules?**
  _Cohesion score 0.14 - nodes in this community are weakly interconnected._