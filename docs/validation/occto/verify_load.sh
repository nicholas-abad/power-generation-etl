#!/usr/bin/env bash
# Verification after a rebuild (read-only; every query runs with
# default_transaction_read_only).
#
#   verify_load.sh ENV PARSER_B_DIR PAYLOAD_DIR OUT_DIR [--allow-production]
#
# ENV is staging or production; production also needs --allow-production.
# 0. No rows at or after the payload's end (a weekly OCCTO run since the
#    archive cut writes there; keep the workflow disabled until this passes).
# 1. Export the rebuilt rows from the certified start to the payload's end in
#    parser B's canonical format and sort order; the sha256 must equal parser
#    B's digest (database == independent parser, row for row): its coal-plant
#    digest for a coal-plant payload, its full digest for an older one.
# 1b. Every source coal row is stored: coal rows and kWh in the database equal
#    parser B's count over the whole source, not only over the cut.
# 2. The three OCCTO views equal the raw table (coal per plant-month, all fuels
#    per unit-month, rows per month).
# 3. The tracker's coal reads, run as dashboard_ro, equal source coal per month.
# Run from the ETL repository root.
set -euo pipefail
[ "$#" -ge 4 ] || { echo "usage: verify_load.sh ENV PARSER_B_DIR PAYLOAD_DIR OUT_DIR [--allow-production]"; exit 2; }
env="$1"; b_dir="$2"; payload="$3"; out="$4"
case "$env" in
  staging) ;;
  production) [ "${5:-}" = "--allow-production" ] || { echo "production needs --allow-production"; exit 2; } ;;
  *) echo "ENV must be staging or production"; exit 2 ;;
esac
mkdir -p "$out"
end_ms="$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["end_ms"])' "$payload/manifest.json")"
plants="$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1])).get("plants", "all"))' "$payload/manifest.json")"
db() { uv run python src/run_in_environment.py --environment "$env" --role "$1" -- \
         psql -X -q -v ON_ERROR_STOP=1 -c "SET default_transaction_read_only = on" "${@:2}" | sed '1{/^Target: /d;}'; }
echo "target: $env; payload plants: $plants"

echo "== 0. no rows at or after the certified end (the comparisons below cover the certified range only)"
after_end="$(db writer -At -c "SELECT count(*) FROM ingestion.occto_generation_data WHERE timestamp_ms >= $end_ms")"
echo "rows after end: $after_end"
[ "$after_end" = "0" ] || { echo "Rows exist after the certified end: a weekly OCCTO run since the archive cut? Extend the archive, or verify them separately"; exit 1; }

echo "== 1. database rows vs parser B"
not_whole_kwh="$(db writer -At -c "SELECT count(*) FROM ingestion.occto_generation_data
  WHERE generation_mwh::numeric * 1000 <> round(generation_mwh::numeric * 1000)")"
echo "stored values that are not whole kWh: $not_whole_kwh"
[ "$not_whole_kwh" = "0" ] || { echo "Rounding to kWh would hide differences"; exit 1; }
db writer -c "\copy (SELECT timestamp_ms, plant, unit, plant_code, fuel_code, fuel_type, area_code,
    area_name, resolution_minutes, round(generation_mwh::numeric * 1000)::bigint
  FROM ingestion.occto_generation_data WHERE timestamp_ms < $end_ms
  ORDER BY timestamp_ms, plant COLLATE \"C\", unit COLLATE \"C\") TO STDOUT" | gzip > "$out/db_canonical.tsv.gz"
db_digest="$(gzip -dc "$out/db_canonical.tsv.gz" | shasum -a 256 | cut -d' ' -f1)"
if [ "$plants" = "coal" ]; then b_key=coal_plants_digest; b_canonical=canonical_coal_plants.tsv.gz
else b_key=digest; b_canonical=canonical.tsv.gz; fi
b_digest="$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$b_dir/report.json" "$b_key")"
echo "db  $db_digest"; echo "B   $b_digest"
if [ "$db_digest" = "$b_digest" ]; then echo "ROWS MATCH"; else
  echo "ROWS DIFFER"; cmp <(gzip -dc "$out/db_canonical.tsv.gz") <(gzip -dc "$b_dir/$b_canonical") | head -3 || true; exit 1; fi

echo "== 1b. every source coal row is stored (parser B's count over the whole source)"
db_coal="$(db writer -At -F ' ' -c "SELECT count(*), coalesce(sum(round(generation_mwh::numeric * 1000)), 0)::bigint
  FROM ingestion.occto_generation_data WHERE fuel_type = 'coal' AND timestamp_ms < $end_ms")"
src_coal="$(python3 -I -c 'import json,sys; r=json.load(open(sys.argv[1])); print(r["coal_rows"], r["coal_kwh"]) if "coal_rows" in r else print("")' "$b_dir/report.json")"
if [ -z "$src_coal" ]; then
  [ "$plants" != "coal" ] || { echo "Parser B's report has no coal counts; rerun it"; exit 1; }
  echo "skipped: parser B's report predates the coal counts (an all-plants payload)"
else
  echo "db coal rows kWh:     $db_coal"; echo "source coal rows kWh: $src_coal"
  [ "$db_coal" = "$src_coal" ] || { echo "COAL ROWS DIFFER FROM THE SOURCE"; exit 1; }
fi

echo "== 2. views vs raw table (each count must be 0)"
db writer -At -F ' ' -f - > "$out/views_check.txt" <<SQL
SET TIME ZONE 'UTC';
SELECT 'plant_view_vs_raw', count(*) FROM (
  (SELECT month, plant, area_name, fuel_type, round(generation_mwh::numeric, 6) FROM public.mv_occto_plant_monthly
   EXCEPT SELECT date_trunc('month', to_timestamp(timestamp_ms / 1000)), plant, area_name, fuel_type,
     round(sum(generation_mwh)::numeric, 6) FROM ingestion.occto_generation_data GROUP BY 1, 2, 3, 4)
  UNION ALL
  (SELECT date_trunc('month', to_timestamp(timestamp_ms / 1000)), plant, area_name, fuel_type,
     round(sum(generation_mwh)::numeric, 6) FROM ingestion.occto_generation_data GROUP BY 1, 2, 3, 4
   EXCEPT SELECT month, plant, area_name, fuel_type, round(generation_mwh::numeric, 6) FROM public.mv_occto_plant_monthly)) d;
SELECT 'unit_view_vs_raw', count(*) FROM (
  (SELECT month, plant, unit, fuel_type, round(generation_mwh::numeric, 6) FROM public.mv_occto_unit_monthly
   EXCEPT SELECT date_trunc('month', to_timestamp(timestamp_ms / 1000)), plant, coalesce(unit, ''), max(fuel_type),
     round(sum(generation_mwh)::numeric, 6) FROM ingestion.occto_generation_data GROUP BY 1, 2, 3)
  UNION ALL
  (SELECT date_trunc('month', to_timestamp(timestamp_ms / 1000)), plant, coalesce(unit, ''), max(fuel_type),
     round(sum(generation_mwh)::numeric, 6) FROM ingestion.occto_generation_data GROUP BY 1, 2, 3
   EXCEPT SELECT month, plant, unit, fuel_type, round(generation_mwh::numeric, 6) FROM public.mv_occto_unit_monthly)) d;
SELECT 'unit_months_with_several_fuels', count(*) FROM (
  SELECT 1 FROM ingestion.occto_generation_data
  GROUP BY date_trunc('month', to_timestamp(timestamp_ms / 1000)), plant, coalesce(unit, '')
  HAVING count(DISTINCT fuel_type) > 1) d;
SELECT 'row_count_view_vs_raw', count(*) FROM (
  (SELECT month, row_count FROM public.mv_occto_row_counts
   EXCEPT SELECT date_trunc('month', to_timestamp(timestamp_ms / 1000))::date, count(*) FROM ingestion.occto_generation_data GROUP BY 1)
  UNION ALL
  (SELECT date_trunc('month', to_timestamp(timestamp_ms / 1000))::date, count(*) FROM ingestion.occto_generation_data GROUP BY 1
   EXCEPT SELECT month, row_count FROM public.mv_occto_row_counts)) d;
SQL
cat "$out/views_check.txt"
# exactly the four named checks, each 0
awk 'NF == 2 { n++; if ($2 != 0) bad = 1 } END { exit (bad || n != 4) }' "$out/views_check.txt" \
  || { echo "VIEWS DIFFER FROM THE RAW TABLE"; exit 1; }

echo "== 3. tracker coal reads as dashboard_ro vs source coal per month"
db reader -At -F ',' -c "SET TIME ZONE 'UTC'" -c "
  SELECT to_char(p.month, 'YYYY-MM'), round(sum(p.generation_mwh)::numeric, 3),
         round((SELECT sum(u.generation_mwh) FROM mv_occto_unit_monthly u
                WHERE u.month = p.month AND u.fuel_type = 'coal')::numeric, 3)
  FROM mv_occto_plant_monthly p WHERE p.fuel_type = 'coal' GROUP BY p.month ORDER BY 1" > "$out/tracker_coal.csv"
python3 -I - "$b_dir/unit_month.csv" "$out/tracker_coal.csv" <<'PY'
import csv, sys
from decimal import Decimal
src = {}
for r in csv.DictReader(open(sys.argv[1], encoding="utf-8")):
    if r["fuel_type"] == "coal":
        src[r["month"]] = src.get(r["month"], Decimal(0)) + Decimal(r["kwh"]) / 1000
bad = 0
for month, plant_view, unit_view in csv.reader(open(sys.argv[2], encoding="utf-8")):
    want = src.get(month)
    ok = want is not None and abs(Decimal(plant_view) - want) < Decimal("0.01") and abs(Decimal(unit_view) - want) < Decimal("0.01")
    bad += not ok
    print(month, plant_view, unit_view, want, "OK" if ok else "DIFF")
missing = sorted(set(src) - {row[0] for row in csv.reader(open(sys.argv[2], encoding="utf-8"))})
print("months in source but not in the views:", missing)
sys.exit(1 if bad or missing else 0)
PY
