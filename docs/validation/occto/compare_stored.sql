-- Step 5: stored OCCTO rows vs the certified source, before anything changes.
--
-- Runs on a LOCAL scratch database (never Neon) holding two tables:
--   stored  copy of ingestion.occto_generation_data from staging
--           (id, run, created_at_ms, ts, plant, unit, plant_code, fuel_type, area_name, mwh)
--   src     parser B's canonical.tsv.gz
--           (ts, plant, unit, plant_code, fuel_code, fuel_type, area_code, area_name, res, kwh)
-- See compare_stored.sh for loading. Output: one result set per numbered query.
--
-- Two key conventions exist in the stored table:
--   utc     JST wall-clock written as UTC (every run except the bulk load)
--   berlin  the 2026-03-09 bulk load: JST wall-clock converted as Europe/Berlin
--           local time (-1 h winter, -2 h summer; spring-forward collisions)
-- The source is re-keyed to both, and each stored row is matched under the
-- convention of the run that wrote it.

\set ON_ERROR_STOP on
SET TIME ZONE 'UTC';
SET work_mem = '512MB';

-- Bulk-load runs: created on 2026-03-09 (UTC).
DROP TABLE IF EXISTS runs;
CREATE TABLE runs AS
SELECT run,
       min(created_at_ms) AS created_at_ms,
       to_char(to_timestamp(min(created_at_ms) / 1000), 'YYYY-MM-DD') = '2026-03-09' AS bulk,
       count(*) AS n
FROM stored GROUP BY run;

-- Python's naive datetime.timestamp() on a Europe/Berlin host (fold=0):
-- nonexistent spring times use the pre-gap offset (PostgreSQL agrees);
-- ambiguous autumn times use the FIRST occurrence, where PostgreSQL picks the
-- second, so step back an hour when the earlier instant shows the same wall
-- time. Checked against Python at both 2025 transitions.
CREATE OR REPLACE FUNCTION berlin_naive_ms(wall_utc_ms bigint) RETURNS bigint
LANGUAGE sql IMMUTABLE AS $$
  WITH w AS (SELECT to_timestamp(wall_utc_ms / 1000.0) AT TIME ZONE 'UTC' AS wall),
       t AS (SELECT wall, wall AT TIME ZONE 'Europe/Berlin' AS inst FROM w)
  SELECT (extract(epoch FROM CASE
            WHEN (inst - interval '1 hour') AT TIME ZONE 'Europe/Berlin' = wall
            THEN inst - interval '1 hour' ELSE inst END) * 1000)::bigint
  FROM t
$$;

-- Source keyed both ways.
DROP TABLE IF EXISTS srck;
CREATE TABLE srck AS
SELECT s.*, berlin_naive_ms(ts) AS ts_berlin,
       to_char(to_timestamp(ts / 1000), 'YYYY-MM') AS month
FROM src s WHERE fuel_type IN ('coal', 'gas', 'oil');
CREATE INDEX ON srck (plant, unit, ts);
CREATE INDEX ON srck (plant, unit, ts_berlin);

DROP TABLE IF EXISTS st;
CREATE TABLE st AS
SELECT t.*, r.bulk, to_char(to_timestamp(t.ts / 1000), 'YYYY-MM') AS stored_month
FROM stored t JOIN runs r USING (run);
CREATE INDEX ON st (plant, unit, ts);
CREATE INDEX ON st (id);

-- Each stored row's source counterpart under its own run's convention.
-- (A Berlin spring collision maps two source slots onto one key; the stored
-- row matches at most one of them by value.)
DROP TABLE IF EXISTS matched;
CREATE TABLE matched AS
WITH candidates AS (
    SELECT t.id, s.ts AS src_ts, s.kwh, s.fuel_type AS src_fuel, s.month AS src_month
    FROM st t JOIN srck s ON s.plant = t.plant AND s.unit = t.unit AND s.ts = t.ts
    WHERE NOT t.bulk
    UNION ALL
    SELECT t.id, s.ts, s.kwh, s.fuel_type, s.month
    FROM st t JOIN srck s ON s.plant = t.plant AND s.unit = t.unit AND s.ts_berlin = t.ts
    WHERE t.bulk
),
best AS (
    SELECT DISTINCT ON (c.id) c.*
    FROM candidates c JOIN st t ON t.id = c.id
    ORDER BY c.id, abs(c.kwh / 1000.0 - t.mwh), c.src_ts
)
SELECT t.id, t.bulk, t.fuel_type AS stored_fuel, t.mwh, t.stored_month,
       b.src_ts, b.kwh, b.src_fuel, b.src_month,
       CASE WHEN b.src_ts IS NULL THEN 'no_source_slot'
            WHEN abs(b.kwh / 1000.0 - t.mwh) < 1e-9 THEN 'value_equal'
            ELSE 'value_differs' END AS class
FROM st t LEFT JOIN best b ON b.id = t.id;
CREATE INDEX ON matched (src_ts);

\echo '== 1. stored rows by run convention and match class'
SELECT CASE WHEN bulk THEN 'berlin (2026-03-09 bulk)' ELSE 'utc (later runs)' END AS convention,
       class, count(*) AS rows, round(sum(mwh)::numeric / 1e6, 4) AS twh
FROM matched GROUP BY 1, 2 ORDER BY 1, 2;

-- Source slots (coal/gas/oil) that no stored row carries correctly.
-- Each value-equal stored row carries exactly one source slot (src_ts), so the
-- covered slots come straight from matched; no re-join with the source.
DROP TABLE IF EXISTS covered;
CREATE TABLE covered AS
SELECT DISTINCT t.plant, t.unit, m.src_ts AS ts
FROM matched m JOIN st t ON t.id = m.id
WHERE m.class = 'value_equal';
CREATE INDEX ON covered (plant, unit, ts);
ANALYZE covered;

DROP TABLE IF EXISTS missing;
CREATE TABLE missing AS
SELECT s.* FROM srck s
WHERE NOT EXISTS (SELECT 1 FROM covered c WHERE c.plant = s.plant AND c.unit = s.unit AND c.ts = s.ts);

\echo '== 2. source slots not stored correctly, by fuel'
SELECT fuel_type, count(*) AS slots, round(sum(kwh)::numeric / 1e9, 4) AS twh
FROM missing GROUP BY 1 ORDER BY 1;

-- Classify the missing slots.
DROP TABLE IF EXISTS missing_class;
-- Set-based (hash joins): per-row correlated subqueries over 13M stored rows
-- never finish on the full history. Each lookup set is unique on its join
-- key, so the joins cannot multiply rows.
CREATE TABLE missing_class AS
WITH unit_months AS (SELECT DISTINCT plant, unit, stored_month FROM st),
differs AS (SELECT DISTINCT t.plant, t.unit, x.src_ts FROM matched x JOIN st t ON t.id = x.id
            WHERE x.class = 'value_differs'),
shared_berlin AS (SELECT plant, unit, ts_berlin FROM srck GROUP BY 1, 2, 3 HAVING count(*) > 1),
bulk_keys AS (SELECT DISTINCT plant, unit, ts FROM st WHERE bulk)
SELECT m.*,
       CASE
         -- the unit has no stored row at all in that source month
         WHEN um.plant IS NULL THEN 'unit_month_absent'
         -- a stored row holds this key under its run's convention but another value
         WHEN d.plant IS NOT NULL THEN 'value_differs'
         -- two source slots share one Berlin key (spring-forward), a bulk-load
         -- row holds that key, and it carries the other slot's value
         WHEN sb.plant IS NOT NULL AND bk.plant IS NOT NULL THEN 'berlin_spring_collision'
         -- the first week (2024-03-25..31) predates the bulk load's 2024-04-01 start
         WHEN m.ts <= 1711929600000 THEN 'first_week_not_loaded'
         ELSE 'slot_absent'
       END AS reason
FROM missing m
LEFT JOIN unit_months um ON um.plant = m.plant AND um.unit = m.unit AND um.stored_month = m.month
LEFT JOIN differs d ON d.plant = m.plant AND d.unit = m.unit AND d.src_ts = m.ts
LEFT JOIN shared_berlin sb ON sb.plant = m.plant AND sb.unit = m.unit AND sb.ts_berlin = m.ts_berlin
LEFT JOIN bulk_keys bk ON bk.plant = m.plant AND bk.unit = m.unit AND bk.ts = m.ts_berlin;

\echo '== 3. missing source slots by reason and fuel'
SELECT reason, fuel_type, count(*) AS slots, count(DISTINCT (plant, unit)) AS units,
       round(sum(kwh)::numeric / 1e9, 4) AS twh
FROM missing_class GROUP BY 1, 2 ORDER BY 1, 2;

\echo '== 4. coal units with absent unit-months (missing units / months)'
SELECT plant_code, plant, unit, min(month) AS first_month, max(month) AS last_month,
       count(DISTINCT month) AS months, round(sum(kwh)::numeric / 1e6, 1) AS gwh
FROM missing_class WHERE reason = 'unit_month_absent' AND fuel_type = 'coal'
GROUP BY 1, 2, 3 ORDER BY gwh DESC;

\echo '== 5. slot_absent coal by unit (truncation tails and other partial gaps), top 30'
SELECT plant, unit, count(*) AS slots, count(DISTINCT to_char(to_timestamp(ts / 1000), 'YYYY-MM-DD')) AS days,
       round(sum(kwh)::numeric / 1e6, 2) AS gwh
FROM missing_class WHERE reason = 'slot_absent' AND fuel_type = 'coal'
GROUP BY 1, 2 ORDER BY gwh DESC LIMIT 30;

\echo '== 6. fuel label changed: stored fuel differs from the source fuel for the same slot'
SELECT stored_fuel, src_fuel, count(*) AS rows, round(sum(mwh)::numeric / 1e6, 4) AS twh
FROM matched WHERE src_fuel IS NOT NULL AND src_fuel <> stored_fuel GROUP BY 1, 2;

\echo '== 7. stored rows whose plant/unit never appears in the source (renamed/truncated names)'
SELECT t.plant, t.unit, t.fuel_type, count(*) AS rows, min(t.stored_month), max(t.stored_month),
       round(sum(t.mwh)::numeric / 1e3, 1) AS gwh
FROM st t WHERE NOT EXISTS (SELECT 1 FROM src s WHERE s.plant = t.plant AND s.unit = t.unit)
GROUP BY 1, 2, 3 ORDER BY gwh DESC;

\echo '== 8. duplicate stored keys (should be impossible under the unique index)'
SELECT count(*) FROM (SELECT ts, plant, unit FROM stored GROUP BY 1, 2, 3 HAVING count(*) > 1) d;

\echo '== 9. label gate: per (plant_code, unit, month) stored vs source plant/area labels'
WITH sl AS (SELECT DISTINCT plant_code, unit, stored_month AS month, plant, area_name FROM st),
     rl AS (SELECT DISTINCT plant_code, unit, month, plant, area_name FROM srck)
SELECT 'stored label not in source' AS kind, count(*) AS cases
FROM sl WHERE EXISTS (SELECT 1 FROM rl WHERE rl.plant_code = sl.plant_code AND rl.unit = sl.unit AND rl.month = sl.month)
  AND NOT EXISTS (SELECT 1 FROM rl WHERE rl.plant_code = sl.plant_code AND rl.unit = sl.unit AND rl.month = sl.month
                  AND rl.plant = sl.plant AND rl.area_name IS NOT DISTINCT FROM sl.area_name)
UNION ALL
SELECT 'stored coal row with blank plant_code', count(*) FROM st WHERE fuel_type = 'coal' AND coalesce(plant_code, '') = ''
UNION ALL
SELECT '(plant_code, unit) with several stored labels', count(*) FROM (
  SELECT plant_code, unit FROM st GROUP BY 1, 2 HAVING count(DISTINCT (plant, area_name)) > 1) x
UNION ALL
SELECT '(plant_code, unit) with several source labels', count(*) FROM (
  SELECT plant_code, unit FROM srck GROUP BY 1, 2 HAVING count(DISTINCT (plant, area_name)) > 1) y;

\echo '== 9b. the label cases themselves (first 40)'
WITH sl AS (SELECT DISTINCT plant_code, unit, stored_month AS month, plant, area_name FROM st),
     rl AS (SELECT DISTINCT plant_code, unit, month, plant, area_name FROM srck)
SELECT sl.plant_code, sl.unit, sl.month, sl.plant AS stored_plant, sl.area_name AS stored_area,
       (SELECT string_agg(DISTINCT rl.plant || ' / ' || rl.area_name, ' | ') FROM rl
        WHERE rl.plant_code = sl.plant_code AND rl.unit = sl.unit AND rl.month = sl.month) AS source_labels
FROM sl WHERE EXISTS (SELECT 1 FROM rl WHERE rl.plant_code = sl.plant_code AND rl.unit = sl.unit AND rl.month = sl.month)
  AND NOT EXISTS (SELECT 1 FROM rl WHERE rl.plant_code = sl.plant_code AND rl.unit = sl.unit AND rl.month = sl.month
                  AND rl.plant = sl.plant AND rl.area_name IS NOT DISTINCT FROM sl.area_name)
ORDER BY 1, 2, 3 LIMIT 40;

\echo '== 10. Japan coal per month: stored (as the views compute it) vs source (same key convention)'
WITH s AS (SELECT stored_month AS month, sum(mwh) AS stored_mwh FROM st WHERE fuel_type = 'coal' GROUP BY 1),
     r AS (SELECT month, sum(kwh) / 1000.0 AS source_mwh FROM srck WHERE fuel_type = 'coal' GROUP BY 1)
SELECT coalesce(s.month, r.month) AS month,
       round(coalesce(stored_mwh, 0)::numeric / 1e6, 4) AS stored_twh,
       round(coalesce(source_mwh, 0)::numeric / 1e6, 4) AS source_twh,
       round((coalesce(stored_mwh, 0) - coalesce(source_mwh, 0))::numeric / 1e6, 4) AS diff_twh,
       round((coalesce(stored_mwh, 0) / nullif(source_mwh, 0))::numeric, 4) AS ratio
FROM s FULL JOIN r ON s.month = r.month ORDER BY 1;

\echo '== 11. coal difference decomposed (TWh, whole period)'
SELECT 'source coal' AS item, round(sum(kwh)::numeric / 1e9, 4) AS twh FROM srck WHERE fuel_type = 'coal'
UNION ALL SELECT 'stored coal', round(sum(mwh)::numeric / 1e6, 4) FROM st WHERE fuel_type = 'coal'
UNION ALL SELECT 'missing: ' || reason, round(sum(kwh)::numeric / 1e9, 4) FROM missing_class WHERE fuel_type = 'coal' GROUP BY reason
UNION ALL SELECT 'stored coal with no source slot', round(sum(mwh)::numeric / 1e6, 4) FROM matched WHERE stored_fuel = 'coal' AND class = 'no_source_slot'
UNION ALL SELECT 'stored coal value differs (stored minus source)', round(sum(mwh - kwh / 1000.0)::numeric / 1e6, 4) FROM matched WHERE stored_fuel = 'coal' AND class = 'value_differs'
UNION ALL SELECT 'stored coal booked in another month than its source slot', round(sum(mwh)::numeric / 1e6, 4)
  FROM matched WHERE stored_fuel = 'coal' AND class = 'value_equal' AND stored_month <> src_month;
