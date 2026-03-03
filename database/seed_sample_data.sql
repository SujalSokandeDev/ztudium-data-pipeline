-- Optional seed data for smoke testing.

INSERT INTO daily_metrics (date, website, gsc_clicks, gsc_impressions, ga_sessions, ga_organic_sessions, ga_bounce_rate, data_source)
VALUES
    (CURRENT_DATE - INTERVAL '1 day', 'BusinessABC', 7421, 63120, 9340, 7125, 41.2, 'seed'),
    (CURRENT_DATE - INTERVAL '1 day', 'CitiesABC', 3920, 28440, 5012, 3899, 46.7, 'seed'),
    (CURRENT_DATE - INTERVAL '1 day', 'Wisdomia', 5112, 41230, 6201, 4870, 38.9, 'seed')
ON CONFLICT (date, website) DO UPDATE
SET gsc_clicks = EXCLUDED.gsc_clicks,
    gsc_impressions = EXCLUDED.gsc_impressions,
    ga_sessions = EXCLUDED.ga_sessions,
    ga_organic_sessions = EXCLUDED.ga_organic_sessions,
    ga_bounce_rate = EXCLUDED.ga_bounce_rate,
    data_source = EXCLUDED.data_source;

INSERT INTO ahrefs_overview (date, website, domain, dr, organic_traffic, organic_keywords, source_file)
VALUES
    (CURRENT_DATE - INTERVAL '1 day', 'BusinessABC', 'businessabc.net', 71, 128000, 24500, 'seed_overview_businessabc.txt'),
    (CURRENT_DATE - INTERVAL '1 day', 'CitiesABC', 'citiesabc.com', 64, 87000, 16300, 'seed_overview_citiesabc.txt')
ON CONFLICT (date, website) DO UPDATE
SET dr = EXCLUDED.dr,
    organic_traffic = EXCLUDED.organic_traffic,
    organic_keywords = EXCLUDED.organic_keywords,
    source_file = EXCLUDED.source_file;
