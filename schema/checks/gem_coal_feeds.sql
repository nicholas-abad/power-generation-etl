-- GEM-anchored coal feed check (Australia): every GEM site with at least one
-- OPERATING coal unit (GCPT) that the crosswalk links to an OpenElectricity
-- facility must have OE coal rows in the last 10 days.
--
-- Why this exists: extraction-level checks ask whether the API returned what
-- it promised; this one asks the question the tracker cares about, anchored
-- on GEM as the source of truth — is every operating coal plant we claim to
-- meter actually metered? It catches a station going silent in the feed, a
-- broken crosswalk link, and a failed extraction (in which case the freshness
-- check fails too). 10 days = weekly cadence + the complete-days cap + slack.
--
-- Run weekly by check-source-freshness in weekly-extraction.yml. By hand:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f schema/checks/gem_coal_feeds.sql

\set ON_ERROR_STOP on

DO $$
DECLARE
    silent text := '';
    n_sites int := 0;
    r record;
BEGIN
    FOR r IN
        SELECT l.gem_location_id, l.name,
               string_agg(DISTINCT x.plant_name, ', ') AS oe_facilities,
               (SELECT MAX(to_timestamp(g.timestamp_ms / 1000))
                  FROM ingestion.oe_facility_generation_data g
                 WHERE g.facility_name IN (SELECT y.plant_name FROM plant_crosswalk y
                                            WHERE y.source_system = 'OE'
                                              AND y.gem_location_id = l.gem_location_id)
                   AND g.fueltech IN ('coal_black', 'coal_brown')) AS last_coal_row
        FROM gem_locations l
        JOIN plant_crosswalk x
          ON x.gem_location_id = l.gem_location_id AND x.source_system = 'OE'
        WHERE EXISTS (SELECT 1 FROM gem_units u
                       WHERE u.gem_location_id = l.gem_location_id
                         AND u.tracker = 'GCPT' AND u.status = 'operating')
        GROUP BY l.gem_location_id, l.name
    LOOP
        n_sites := n_sites + 1;
        IF r.last_coal_row IS NULL OR r.last_coal_row < now() - interval '10 days' THEN
            silent := silent || format(E'\n  %s (%s; OE: %s) — last OE coal row: %s',
                                       r.name, r.gem_location_id, r.oe_facilities,
                                       COALESCE(r.last_coal_row::date::text, 'never'));
        END IF;
    END LOOP;

    IF n_sites = 0 THEN
        RAISE EXCEPTION 'GEM coal feed check: no operating GEM coal site is linked to OE — the crosswalk lost Australia';
    END IF;

    IF silent <> '' THEN
        RAISE EXCEPTION E'OPERATING GEM COAL SITES WITH NO RECENT OE COAL DATA:%', silent;
    END IF;

    RAISE NOTICE 'GEM coal feed check OK: all % operating GEM coal sites linked to OE have coal data within 10 days',
                 n_sites;
END $$;
