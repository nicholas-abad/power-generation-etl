-- ONS source gaps: hours in which an individual plant that passes the
-- qualification policy has no published generation (null in the source).
--
-- Gaps are recorded, never imputed: ingestion.ons_generation_data holds only
-- measured values, and consumers use these rows to mark plant-months that are
-- incomplete. Each row is one contiguous hourly run per ONS ID; end_ms is
-- exclusive and both use the loader's timestamp_ms encoding. Rows are written
-- by src/ons_source_gaps.py from an audited extraction, replacing every gap
-- that starts inside the extraction's period, so a value ONS fills in later
-- removes its gap on the next audited load. kind and note come from the
-- approval in config/ons-benchmarks.json (fuel_policy.source_gaps).

CREATE TABLE IF NOT EXISTS ingestion.ons_source_gaps (
    ons_plant_id TEXT NOT NULL,
    start_ms BIGINT NOT NULL,
    end_ms BIGINT NOT NULL,
    plant TEXT NOT NULL,
    fuel_type TEXT NOT NULL,
    hours INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('publication_change', 'source_gap')),
    note TEXT NOT NULL,
    extraction_run_id TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ons_plant_id, start_ms),
    CHECK (hours > 0 AND end_ms - start_ms = hours::BIGINT * 3600000)
);

-- Gap hours per plant and UTC month, matching the month buckets of
-- public.mv_ons_individual_plant_monthly (017). Plain view: always current.
CREATE OR REPLACE VIEW public.ons_individual_plant_gap_monthly AS
WITH runs AS (
    SELECT ons_plant_id, fuel_type, kind,
        TO_TIMESTAMP(start_ms / 1000.0) AT TIME ZONE 'UTC' AS gap_start,
        TO_TIMESTAMP(end_ms / 1000.0) AT TIME ZONE 'UTC' AS gap_end
    FROM ingestion.ons_source_gaps
)
SELECT
    m.month AT TIME ZONE 'UTC' AS month,
    r.ons_plant_id,
    r.fuel_type,
    r.kind,
    SUM(EXTRACT(EPOCH FROM LEAST(r.gap_end, m.month + INTERVAL '1 month')
        - GREATEST(r.gap_start, m.month)) / 3600)::INTEGER AS gap_hours
FROM runs r
CROSS JOIN LATERAL generate_series(
    DATE_TRUNC('month', r.gap_start),
    r.gap_end - INTERVAL '1 microsecond',
    INTERVAL '1 month'
) AS m(month)
GROUP BY 1, 2, 3, 4;
