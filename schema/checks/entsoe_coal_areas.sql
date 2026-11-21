-- ENTSO-E coal area check: every ENTSO-E area that had coal rows in the last
-- 35 days must also have coal rows (hard coal B05, lignite B02, coal-derived
-- gas B03) within the last 8 days.
--
-- Why this exists: source_freshness.sql asks whether ENTSO-E as a whole was
-- extracted. In November 2026 Poland, the largest coal area, returned no rows
-- after 10 Nov while the other areas kept loading: every run succeeded, the
-- freshness check passed, and 11 days of Polish coal were missing before a
-- manual coverage query noticed. This check asks the same question per area.
--
-- 8 days: ENTSO-E publishes unit-level output up to about five days late, and
-- the check runs on Sunday mornings after the weekly extraction. 35 days: an
-- area that stopped long ago (e.g. Italy at source in July 2026) is not
-- reported forever; a newly stopped area is reported on four weekly runs
-- before it drops out.
--
-- Run weekly by check-source-freshness in weekly-extraction.yml. By hand:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f schema/checks/entsoe_coal_areas.sql

\set ON_ERROR_STOP on

DO $$
DECLARE
    recent_ms bigint := (EXTRACT(EPOCH FROM now() - interval '8 days') * 1000)::bigint;
    window_ms bigint := (EXTRACT(EPOCH FROM now() - interval '35 days') * 1000)::bigint;
    silent text := '';
    n_areas int := 0;
    r record;
BEGIN
    FOR r IN
        SELECT country_code, MAX(timestamp_ms) AS last_ms
          FROM ingestion.entsoe_generation_data
         WHERE psr_type IN ('B02', 'B03', 'B05')
           AND timestamp_ms >= window_ms
         GROUP BY country_code
         ORDER BY country_code
    LOOP
        n_areas := n_areas + 1;
        IF r.last_ms < recent_ms THEN
            silent := silent || format(E'\n  %s — last coal row: %s UTC',
                                       r.country_code,
                                       to_char(to_timestamp(r.last_ms / 1000.0) AT TIME ZONE 'UTC',
                                               'YYYY-MM-DD HH24:MI'));
        END IF;
    END LOOP;

    IF n_areas = 0 THEN
        RAISE EXCEPTION 'ENTSO-E coal area check: no ENTSO-E coal rows at all in the last 35 days';
    END IF;

    IF silent <> '' THEN
        RAISE EXCEPTION E'ENTSO-E COAL AREAS WITH NO COAL ROWS IN THE LAST 8 DAYS (of % areas with coal in the last 35 days):%\nCheck whether ENTSO-E publishes the area, then backfill with a manual run (start_override).',
                        n_areas, silent;
    END IF;

    RAISE NOTICE 'ENTSO-E coal area check OK: all % areas with coal in the last 35 days have coal rows within 8 days',
                 n_areas;
END $$;
