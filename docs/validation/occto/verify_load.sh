#!/usr/bin/env bash
# Step 6 verification after the staging rebuild (read-only on staging).
#
#   verify_load.sh PARSER_B_DIR PAYLOAD_DIR OUT_DIR
#
# 1. Export the rebuilt rows from the certified start to the payload's end in
#    parser B's canonical format and sort order; the sha256 must equal parser
#    B's digest (database == independent parser, row for row).
# 2. The three OCCTO views equal the raw table (coal per plant-month, all fuels
#    per unit-month, rows per month).
# 3. The tracker's coal reads, run as dashboard_ro, equal source coal per month.
# Run from the ETL repository root.
set -euo pipefail
b_dir="$1"; payload="$2"; out="$3"
mkdir -p "$out"
end_ms="$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["end_ms"])' "$payload/manifest.json")"
staging() { uv run python src/run_in_environment.py --environment staging --role "$1" -- \
              psql -X -q -v ON_ERROR_STOP=1 -c "SET default_transaction_read_only = on" "${@:2}" | sed '1{/^Target: /d;}'; }

echo "== 1. database rows vs parser B"
staging writer -c "\copy (SELECT timestamp_ms, plant, unit, plant_code, fuel_code, fuel_type, area_code,
    area_name, resolution_minutes, round(generation_mwh::numeric * 1000)::bigint
  FROM ingestion.occto_generation_data WHERE timestamp_ms < $end_ms
  ORDER BY timestamp_ms, plant COLLATE \"C\", unit COLLATE \"C\") TO STDOUT" | gzip > "$out/db_canonical.tsv.gz"
db_digest="$(gzip -dc "$out/db_canonical.tsv.gz" | shasum -a 256 | cut -d' ' -f1)"
b_digest="$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["digest"])' "$b_dir/report.json")"
echo "db  $db_digest"; echo "B   $b_digest"
if [ "$db_digest" = "$b_digest" ]; then echo "ROWS MATCH"; else
  echo "ROWS DIFFER"; cmp <(gzip -dc "$out/db_canonical.tsv.gz") <(gzip -dc "$b_dir/canonical.tsv.gz") | head -3 || true; exit 1; fi

echo "== 2. views vs raw table (each count must be 0)"
staging writer -At -F ' ' <<SQL
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

echo "== 3. tracker coal reads as dashboard_ro vs source coal per month"
staging reader -At -F ',' -c "SET TIME ZONE 'UTC'" -c "
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
