-- Source freshness check: every source must have a SUCCESSFUL extraction
-- recorded in ingestion.extraction_metadata within the last 9 days (weekly
-- cadence + slack for a late Sunday run).
--
-- Why this exists: job-status alerting missed the 2026-08 ENTSO-E outage for
-- four weeks — the extractor exits 0 when the API is down, so nothing failed
-- and no issue was filed while the biggest source silently went stale. This
-- check asserts on the DATA, not on exit codes. It is expected to keep
-- failing (and keep the weekly issue open) until a stale source actually
-- recovers — that is the point.
--
-- Run weekly by check-source-freshness in weekly-extraction.yml, after the
-- extract jobs. Also runnable by hand:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f schema/checks/source_freshness.sql

\set ON_ERROR_STOP on

DO $$
DECLARE
    expected text[] := ARRAY[
        'eia', 'ons', 'entsoe', 'npp', 'oe_facility', 'occto', 'chile', 'climatetrace'
    ];
    stale text := '';
    r record;
BEGIN
    FOR r IN
        SELECT s.source,
               (SELECT MAX(extraction_timestamp)
                  FROM ingestion.extraction_metadata m
                 WHERE m.source = s.source AND m.success) AS last_success
        FROM unnest(expected) AS s(source)
    LOOP
        IF r.last_success IS NULL OR r.last_success < now() - interval '9 days' THEN
            stale := stale || format(E'\n  %s — last successful run: %s',
                                     r.source, COALESCE(r.last_success::text, 'never'));
        END IF;
    END LOOP;

    IF stale <> '' THEN
        RAISE EXCEPTION E'STALE SOURCES (no successful extraction in 9 days):%\nThe extractor may be exiting 0 on API errors — check its logs, not just job status.', stale;
    END IF;

    RAISE NOTICE 'source freshness OK: all % sources extracted successfully within 9 days',
                 array_length(expected, 1);
END $$;
