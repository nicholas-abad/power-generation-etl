-- Migration 003: merge mojibake ENTSO-E plant identities into correct spellings
--
-- The extractor decoded the API's UTF-8 XML as ISO-8859-1 for years
-- (energy-extractors PR #13 fixed the decode), so every non-ASCII plant name
-- loaded corrupted ('Bełchatów' as 'BeÅ‚chatÃ³w'). One batch (Dec 2024 -
-- Mar 2025) decoded correctly, splitting 30 plants into DUAL identities with
-- fragmented histories and ~0.12 TWh of double-counted overlap (overlap rows
-- verified value-identical, so ON CONFLICT DO NOTHING loses nothing).
--
-- The mapping below is mechanical (a name whose latin-1 bytes decode as
-- different valid UTF-8 was mis-decoded; genuine Latin text cannot round-trip)
-- with ONE curated exception: ABOÂ¿O round-trips to 'ABO¿O', but the ¿ is the
-- source's own lossy substitution for Ñ and the correctly-spelled twin
-- 'ABOÑO' also exists — mapping to the round-trip result would leave the
-- plant split in two, so it maps to the real name. 110 names, harvested from
-- the live table 2026-08-01.
--
-- Deploy order: energy-extractors PR #13 (UTF-8 decode) MUST be merged first,
-- or the next weekly load re-creates the mojibake identities.
--
-- After running: refresh mv_entsoe_monthly / mv_entsoe_plant_monthly and
-- rebuild the plant crosswalk for ENTSOE (plant_name is its join key).
--
-- Usage:
--   psql "$DATABASE_URL" -f schema/migrations/003_entsoe_mojibake_merge.sql

BEGIN;

CREATE TEMP TABLE _mojibake_map (bad TEXT PRIMARY KEY, good TEXT NOT NULL)
ON COMMIT DROP;
INSERT INTO _mojibake_map (bad, good) VALUES
('ABOÂ¿O 1', 'ABOÑO 1'),
('ABOÂ¿O 2', 'ABOÑO 2'),
('ABOÃO 1', 'ABOÑO 1'),
('ABOÃO 2', 'ABOÑO 2'),
('BGP WÅocÅawek', 'BGP Włocławek'),
('BeÅchatÃ³w B01', 'Bełchatów B01'),
('BeÅchatÃ³w B02', 'Bełchatów B02'),
('BeÅchatÃ³w B03', 'Bełchatów B03'),
('BeÅchatÃ³w B04', 'Bełchatów B04'),
('BeÅchatÃ³w B05', 'Bełchatów B05'),
('BeÅchatÃ³w B06', 'Bełchatów B06'),
('BeÅchatÃ³w B07', 'Bełchatów B07'),
('BeÅchatÃ³w B08', 'Bełchatów B08'),
('BeÅchatÃ³w B09', 'Bełchatów B09'),
('BeÅchatÃ³w B10', 'Bełchatów B10'),
('BeÅchatÃ³w B11', 'Bełchatów B11'),
('BeÅchatÃ³w B12', 'Bełchatów B12'),
('BeÅchatÃ³w B14', 'Bełchatów B14'),
('CPCU-CogeVitryâGP', 'CPCU-CogeVitry–GP'),
('ChorzÃ³w B1', 'Chorzów B1'),
('ChorzÃ³w B2', 'Chorzów B2'),
('DG2_gÃ©p14', 'DG2_gép14'),
('DG2_gÃ©p15', 'DG2_gép15'),
('DG3_gÃ©p7', 'DG3_gép7'),
('DG3_gÃ©p8', 'DG3_gép8'),
('EC RzeszÃ³w B1', 'EC Rzeszów B1'),
('EC WrotkÃ³w B1', 'EC Wrotków B1'),
('EC WÅocÅawek B1', 'EC Włocławek B1'),
('EC Å»eraÅ 2 B20', 'EC Żerań 2 B20'),
('EC Å»eraÅ BGP', 'EC Żerań BGP'),
('GÃNYÃ_gÃ©p1', 'GÖNYÜ_gép1'),
('IbbenbÃ¼ren B', 'Ibbenbüren B'),
('KVV1 VÃ¤rtaverket', 'KVV1 Värtaverket'),
('KVV6 VÃ¤rtaverket ', 'KVV6 Värtaverket '),
('KW DÃ¼rnrohr Block 2', 'KW Dürnrohr Block 2'),
('KW JÃ¤nschwalde Block A', 'KW Jänschwalde Block A'),
('KW JÃ¤nschwalde Block B', 'KW Jänschwalde Block B'),
('KW JÃ¤nschwalde Block C', 'KW Jänschwalde Block C'),
('KW JÃ¤nschwalde Block D', 'KW Jänschwalde Block D'),
('KW JÃ¤nschwalde Block E', 'KW Jänschwalde Block E'),
('KW JÃ¤nschwalde Block F', 'KW Jänschwalde Block F'),
('KW LÃ¼nen Block 1', 'KW Lünen Block 1'),
('KW TheiÃ M3', 'KW Theiß M3'),
('KW TheiÃ M5', 'KW Theiß M5'),
('KrakÃ³w ÅÄg B1', 'Kraków Łęg B1'),
('KrakÃ³w ÅÄg B2', 'Kraków Łęg B2'),
('KrakÃ³w ÅÄg B3', 'Kraków Łęg B3'),
('KrakÃ³w ÅÄg B4', 'Kraków Łęg B4'),
('KymijÃ¤rvi B1', 'Kymijärvi B1'),
('KÃ¼stenkraftwerk', 'Küstenkraftwerk'),
('LitÃ©r_GT', 'Litér_GT'),
('LÅrinci_GT', 'Lőrinci_GT'),
('MalÅ¾enice TG1', 'Malženice TG1'),
('MÃ2', 'MÁ2'),
('MÃ2_gÃ©p3', 'MÁ2_gép3'),
('MÃ2_gÃ©p4', 'MÁ2_gép4'),
('MÃ2_gÃ©p5', 'MÁ2_gép5'),
('NiederauÃem C', 'Niederaußem C'),
('NiederauÃem D', 'Niederaußem D'),
('NiederauÃem E', 'Niederaußem E'),
('NiederauÃem F', 'Niederaußem F'),
('NiederauÃem G', 'Niederaußem G'),
('NiederauÃem H', 'Niederaußem H'),
('NiederauÃem K (BoA 1)', 'Niederaußem K (BoA 1)'),
('NovÃ¡ky TG1', 'Nováky TG1'),
('NovÃ¡ky TG2', 'Nováky TG2'),
('NovÃ¡ky TG3', 'Nováky TG3'),
('OstroÅÄka B B01', 'Ostrołęka B B01'),
('OstroÅÄka B B02', 'Ostrołęka B B02'),
('OstroÅÄka B B03', 'Ostrołęka B B03'),
('PoÅaniec B1', 'Połaniec B1'),
('PoÅaniec B2', 'Połaniec B2'),
('PoÅaniec B3', 'Połaniec B3'),
('PoÅaniec B4', 'Połaniec B4'),
('PoÅaniec B5', 'Połaniec B5'),
('PoÅaniec B6', 'Połaniec B6'),
('PoÅaniec B7', 'Połaniec B7'),
('PÄtnÃ³w 1 B1', 'Pątnów 1 B1'),
('PÄtnÃ³w 1 B2', 'Pątnów 1 B2'),
('PÄtnÃ³w 1 B3', 'Pątnów 1 B3'),
('PÄtnÃ³w 1 B4', 'Pątnów 1 B4'),
('PÄtnÃ³w 1 B5', 'Pątnów 1 B5'),
('PÄtnÃ³w 1 B6', 'Pątnów 1 B6'),
('PÄtnÃ³w 2 B9', 'Pątnów 2 B9'),
('PÅock B01', 'Płock B01'),
('SajÃ³_GT', 'Sajó_GT'),
('SeinÃ¤joki B1', 'Seinäjoki B1'),
('TE Å oÅ¡tanj 5', 'TE Šoštanj 5'),
('TE Å oÅ¡tanj 6', 'TE Šoštanj 6'),
('TheiÃ 2', 'Theiß 2'),
('TheiÃ 3', 'Theiß 3'),
('TheiÃ 5', 'Theiß 5'),
('TurÃ³w B01', 'Turów B01'),
('TurÃ³w B02', 'Turów B02'),
('TurÃ³w B03', 'Turów B03'),
('TurÃ³w B04', 'Turów B04'),
('TurÃ³w B05', 'Turów B05'),
('TurÃ³w B06', 'Turów B06'),
('TurÃ³w B11', 'Turów B11'),
('WrocÅaw Bl2', 'Wrocław Bl2'),
('WrocÅaw Bl3', 'Wrocław Bl3'),
('Zielona GÃ³ra BGP', 'Zielona Góra BGP'),
('Ãresundsverket CHP G1', 'Öresundsverket CHP G1'),
('Ãresundsverket CHP G2', 'Öresundsverket CHP G2'),
('Åagisza B10', 'Łagisza B10'),
('Åaziska 3 B09', 'Łaziska 3 B09'),
('Åaziska 3 B10', 'Łaziska 3 B10'),
('Åaziska 3 B11', 'Łaziska 3 B11'),
('Åaziska 3 B12', 'Łaziska 3 B12'),
('ÅÃ³dÅº-4 B03', 'Łódź-4 B03');

-- A good value that is ALSO a bad key would be re-homed and then DELETED in
-- the same pass — silent destruction. Assert the map is chain-free BEFORE
-- touching any rows.
DO $$
DECLARE chained BIGINT;
BEGIN
    SELECT COUNT(*) INTO chained
    FROM _mojibake_map g JOIN _mojibake_map b ON g.good = b.bad;
    IF chained > 0 THEN
        RAISE EXCEPTION 'mojibake map is chained: % good value(s) are also bad keys', chained;
    END IF;
END $$;

-- Re-home the corrupted rows under the correct spelling. Overlapping
-- timestamps (verified value-identical) collapse via DO NOTHING.
INSERT INTO entsoe_generation_data
    (extraction_run_id, created_at_ms, country_code, psr_type, plant_name,
     fuel_type, data_type, timestamp_ms, generation_mw, resolution_minutes)
SELECT e.extraction_run_id, e.created_at_ms, e.country_code, e.psr_type,
       m.good, e.fuel_type, e.data_type, e.timestamp_ms, e.generation_mw,
       e.resolution_minutes
FROM entsoe_generation_data e
JOIN _mojibake_map m ON e.plant_name = m.bad
ON CONFLICT (timestamp_ms, country_code, psr_type, plant_name) DO NOTHING;

DELETE FROM entsoe_generation_data e
USING _mojibake_map m
WHERE e.plant_name = m.bad;

COMMIT;

SELECT 'Migration 003 (entsoe mojibake merge) complete' AS status;
