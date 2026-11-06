-- ============================================================================
-- Shared definitions for the DPR plans export (PRADAN-style wide CSV).
-- Used by dpr/utils/plans.sql (psql CLI, via the include command) and by
-- dpr/plans_export.py (API). Everything is temporary and re-runnable.
--
-- Requires the session setting dpr.org_id (organization UUID):
--   psql:   SET dpr.org_id = '<uuid>';
--   Django: SELECT set_config('dpr.org_id', '<uuid>', true);
-- ============================================================================

-- ---------------------------------------------------------------- helpers ---

-- hectares string -> acres (NULL if blank / "NA" / not numeric)
CREATE OR REPLACE FUNCTION pg_temp.to_acres(v text) RETURNS numeric
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN v ~ '^[[:space:]]*-?[0-9]+([.][0-9]+)?[[:space:]]*$'
                THEN round(v::numeric * 2.47105, 4) END
$$;

-- "other" -> use the free-text companion value
CREATE OR REPLACE FUNCTION pg_temp.pick_other(v text, other text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN lower(btrim(v)) = 'other' THEN coalesce(nullif(other, ''), 'other')
                ELSE nullif(v, '') END
$$;

-- repair activity of the *_maintenance forms (structure specific key, else generic)
CREATE OR REPLACE FUNCTION pg_temp.maint_repair(d jsonb) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT coalesce(
        (SELECT pg_temp.pick_other(d ->> k, d ->> (k || '_other'))
           FROM unnest(ARRAY[
                'select_one_farm_pond', 'select_one_community_pond', 'select_one_well',
                'select_one_canal', 'select_one_farm_bund', 'select_one_repair_large_water_body',
                'select_one_repair_canal', 'select_one_check_dam', 'select_one_percolation_tank',
                'select_one_rock_fill_dam', 'select_one_loose_boulder_structure',
                'select_one_model5_structure', 'select_one_Model30_40_structure',
                'select_one_model30_40_structure', 'select_one_earthen_gully_plug',
                'select_one_drainage_soakage_channels', 'select_one_recharge_pits',
                'select_one_sokage_pits', 'select_one_trench_cum_bund_network',
                'select_one_continuous_contour_trenches', 'select_one_staggered_contour_trenches',
                'select_one_water_absorption_trenches', 'select_one_stone_bunding',
                'select_one_diversion_drains', 'select_one_bunding'
           ]) WITH ORDINALITY AS t(k, ord)
          WHERE nullif(d ->> k, '') IS NOT NULL
          ORDER BY ord LIMIT 1),
        nullif(d ->> 'select_one_activities', ''))
$$;

-- repair activity stored on a waterbody (first non-empty Repair_of_* key)
CREATE OR REPLACE FUNCTION pg_temp.wb_repair(d jsonb) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT pg_temp.pick_other(e.value, d ->> (e.key || '_other'))
      FROM jsonb_each_text(CASE WHEN jsonb_typeof(d) = 'object' THEN d ELSE '{}'::jsonb END) e
     WHERE left(e.key, 10) = 'Repair_of_' AND right(e.key, 6) <> '_other'
       AND nullif(e.value, '') IS NOT NULL
     LIMIT 1
$$;

-- ------------------------------------------------------------- plan scope ---

CREATE OR REPLACE TEMP VIEW pl AS
SELECT p.id                       AS plan_id,
       p.plan                     AS plan_name,
       o.id::text                 AS org_id,
       o.name                     AS org_name,
       pr.id                      AS project_id,
       pr.name                    AS project_name,
       p.facilitator_name,
       p.village_name,
       p.gram_panchayat,
       st.id                      AS state_id,
       st.state_name,
       di.id                      AS district_id,
       di.district_name,
       te.id                      AS tehsil_id,
       te.tehsil_name,
       p.latitude                 AS plan_lat,
       p.longitude                AS plan_lon
FROM plans_planapp p
JOIN organization_organization o ON o.id = p.organization_id
LEFT JOIN projects_project pr     ON pr.id = p.project_id
LEFT JOIN geoadmin_statesoi st    ON st.id = p.state_soi_id
LEFT JOIN geoadmin_districtsoi di ON di.id = p.district_soi_id
LEFT JOIN geoadmin_tehsilsoi te   ON te.id = p.tehsil_soi_id
WHERE p.organization_id = current_setting('dpr.org_id')::uuid
  AND p.plan NOT ILIKE '%test%'
  AND p.plan NOT ILIKE '%demo%';

-- ---------------------------------------------------------- section rows ----
-- every branch: (plan columns..., section, dpr_table, record_type, record_id, d)
-- where d is a jsonb {"<CSV header>": value}

CREATE OR REPLACE TEMP VIEW dpr_rows AS

-- ===== Section B : village details =====
SELECT pl.*, 'B - Village Details' AS section, 'village' AS dpr_table, 'village' AS record_type,
       pl.plan_id::text AS record_id,
       jsonb_build_object(
           'Number of Settlements in the Village',
               (SELECT count(*) FROM odk_settlement s
                 WHERE s.plan_id = pl.plan_id::text
                   AND s.status_re IS DISTINCT FROM 'rejected' AND NOT coalesce(s.is_deleted, false)),
           'Latitude and Longitude of the Village',
               CASE WHEN pl.plan_lat IS NOT NULL AND pl.plan_lon IS NOT NULL
                    THEN pl.plan_lat::text || ', ' || pl.plan_lon::text END
       ) AS d
FROM pl

UNION ALL
-- ===== Section C : socio-economic =====
SELECT pl.*, 'C - Social, Economic and Ecological', 'socio_economic', 'settlement', s.settlement_id,
       jsonb_build_object(
           'Name of the Settlement', s.settlement_name,
           'Total Number of Households', s.number_of_households,
           'Settlement Type', s.largest_caste,
           'Caste Group', CASE lower(btrim(s.largest_caste))
                              WHEN 'single caste group' THEN s.smallest_caste
                              WHEN 'mixed caste group'  THEN s.settlement_status END,
           'Total Households - SC',      s.data_settlement::jsonb ->> 'count_sc',
           'Total Households - ST',      s.data_settlement::jsonb ->> 'count_st',
           'Total Households - OBC',     s.data_settlement::jsonb ->> 'count_obc',
           'Total Households - General', s.data_settlement::jsonb ->> 'count_general',
           'Total marginal farmers (<2 acres)', s.farmer_family::jsonb ->> 'marginal_farmers',
           'Latitude', s.latitude,
           'Longitude', s.longitude
       )
FROM pl JOIN odk_settlement s ON s.plan_id = pl.plan_id::text
WHERE s.status_re IS DISTINCT FROM 'rejected' AND NOT coalesce(s.is_deleted, false)

UNION ALL
-- ===== Section C : MGNREGA =====
SELECT pl.*, 'C - Social, Economic and Ecological', 'mgnrega', 'settlement', s.settlement_id,
       jsonb_build_object(
           'Settlement''s Name', s.settlement_name,
           'Total Households - applied - have NREGA job cards in previous y',
               CASE WHEN s.nrega_job_card = 0 THEN NULL ELSE s.nrega_job_card END,
           'Total work days in previous year',
               CASE WHEN s.nrega_work_days = 0 THEN NULL ELSE s.nrega_work_days END,
           'Work demands made in previous year', nullif(s.nrega_past_work, ''),
           'Were you involved in the village level planning?', nullif(s.nrega_demand, ''),
           'Issues', nullif(s.nrega_issues, '')
       )
FROM pl JOIN odk_settlement s ON s.plan_id = pl.plan_id::text
WHERE s.status_re IS DISTINCT FROM 'rejected' AND NOT coalesce(s.is_deleted, false)

UNION ALL
-- ===== Section C : crops =====
SELECT pl.*, 'C - Social, Economic and Ecological', 'crop_info', 'crop_grid', c.crop_grid_id,
       jsonb_build_object(
           'Name of the Settlement', c.beneficiary_settlement,
           'Irrigation Source', nullif(c.irrigation_source, ''),
           'Crops grown in Kharif', nullif(c.cropping_patterns_kharif, ''),
           'Kharif acreage (acres)', pg_temp.to_acres(c.data_crop::jsonb ->> 'total_area_cultivation_kharif'),
           'Crops grown in Rabi', nullif(c.cropping_patterns_rabi, ''),
           'Rabi acreage (acres)', pg_temp.to_acres(c.data_crop::jsonb ->> 'total_area_cultivation_Rabi'),
           'Crops grown in Zaid', nullif(c.cropping_patterns_zaid, ''),
           'Zaid acreage (acres)', pg_temp.to_acres(c.data_crop::jsonb ->> 'total_area_cultivation_Zaid'),
           'Cropping Intensity', nullif(c.agri_productivity, ''),
           'Land Classification', nullif(c.land_classification, '')
       )
FROM pl JOIN odk_crop c ON c.plan_id = pl.plan_id::text
WHERE c.status_re IS DISTINCT FROM 'rejected' AND NOT coalesce(c.is_deleted, false)

UNION ALL
-- ===== Section C : livestock census =====
SELECT pl.*, 'C - Social, Economic and Ecological', 'livestock_info', 'settlement', s.settlement_id,
       jsonb_build_object(
           'Name of the Settlement', s.settlement_name,
           'Goats',   nullif(nullif(s.livestock_census::jsonb ->> 'Goats',   '0'), ''),
           'Sheep',   nullif(nullif(s.livestock_census::jsonb ->> 'Sheep',   '0'), ''),
           'Cattle',  nullif(nullif(s.livestock_census::jsonb ->> 'Cattle',  '0'), ''),
           'Piggery', nullif(nullif(s.livestock_census::jsonb ->> 'Piggery', '0'), ''),
           'Poultry', nullif(nullif(s.livestock_census::jsonb ->> 'Poultry', '0'), '')
       )
FROM pl JOIN odk_settlement s ON s.plan_id = pl.plan_id::text
WHERE s.status_re IS DISTINCT FROM 'rejected' AND NOT coalesce(s.is_deleted, false)

UNION ALL
-- ===== Section D : well summary (per settlement) =====
SELECT pl.*, 'D - Water Resources', 'well_summary', 'settlement', NULL::text,
       jsonb_build_object(
           'Name of Beneficiary''s Settlement', w.beneficiary_settlement,
           'Number of Wells', count(*),
           'Total Number of Household Benefitted', sum(coalesce(w.households_benefitted, 0))
       )
FROM pl JOIN odk_well w ON w.plan_id = pl.plan_id::text
WHERE w.status_re IS DISTINCT FROM 'rejected' AND NOT coalesce(w.is_deleted, false)
GROUP BY pl.plan_id, pl.plan_name, pl.org_id, pl.org_name, pl.project_id, pl.project_name,
         pl.facilitator_name, pl.village_name, pl.gram_panchayat, pl.state_id, pl.state_name,
         pl.district_id, pl.district_name, pl.tehsil_id, pl.tehsil_name, pl.plan_lat, pl.plan_lon,
         w.beneficiary_settlement

UNION ALL
-- ===== Section D : wells =====
SELECT pl.*, 'D - Water Resources', 'wells', 'well', w.well_id,
       jsonb_build_object(
           'Name of Beneficiary Settlement', w.beneficiary_settlement,
           'Type of Well', w.data_well::jsonb ->> 'select_one_well_type',
           'Who owns the Well', nullif(w.owner, ''),
           'Beneficiary Name', nullif(w.data_well::jsonb ->> 'Beneficiary_name', ''),
           'Beneficiary''s Father''s Name', nullif(w.data_well::jsonb ->> 'ben_father', ''),
           'Water Availability', w.data_well::jsonb ->> 'select_one_year',
           'Households Benefitted', w.households_benefitted,
           'Which Caste uses the well?', nullif(w.caste_uses, ''),
           'Well Usage', pg_temp.pick_other(w.data_well::jsonb -> 'Well_usage' ->> 'select_one_well_used',
                                            w.data_well::jsonb -> 'Well_usage' ->> 'select_one_well_used_other'),
           'Need Maintenance?', nullif(w.need_maintenance, ''),
           'Repair Activities', coalesce(
                pg_temp.pick_other(w.data_well::jsonb -> 'Well_usage' ->> 'repairs_type',
                                   w.data_well::jsonb -> 'Well_usage' ->> 'repairs_type_other'),
                pg_temp.pick_other(w.data_well::jsonb -> 'Well_condition' ->> 'select_one_repairs_well',
                                   w.data_well::jsonb -> 'Well_condition' ->> 'select_one_repairs_well_other')),
           'Latitude', w.latitude,
           'Longitude', w.longitude
       )
FROM pl JOIN odk_well w ON w.plan_id = pl.plan_id::text
WHERE w.status_re IS DISTINCT FROM 'rejected' AND NOT coalesce(w.is_deleted, false)

UNION ALL
-- ===== Section D : water structure summary (per settlement + structure type) =====
SELECT pl.*, 'D - Water Resources', 'water_summary', 'settlement', NULL::text,
       jsonb_build_object(
           'Name of the Beneficiary''s Settlement', b.beneficiary_settlement,
           'Type of Water Structure', b.water_structure_type,
           'Number of Water Structures', count(*),
           'Total Number of Household Benefitted', sum(coalesce(b.household_benefitted, 0))
       )
FROM pl JOIN odk_waterbody b ON b.plan_id = pl.plan_id::text
WHERE b.status_re IS DISTINCT FROM 'rejected' AND NOT coalesce(b.is_deleted, false)
GROUP BY pl.plan_id, pl.plan_name, pl.org_id, pl.org_name, pl.project_id, pl.project_name,
         pl.facilitator_name, pl.village_name, pl.gram_panchayat, pl.state_id, pl.state_name,
         pl.district_id, pl.district_name, pl.tehsil_id, pl.tehsil_name, pl.plan_lat, pl.plan_lon,
         b.beneficiary_settlement, b.water_structure_type

UNION ALL
-- ===== Section D : water structures =====
SELECT pl.*, 'D - Water Resources', 'water_structures', 'waterbody', b.waterbody_id,
       jsonb_build_object(
           'Name of the Beneficiary''s Settlement', b.beneficiary_settlement,
           'Who owns the water structure?', nullif(b.owner, ''),
           'Beneficiary Name', nullif(b.data_waterbody::jsonb ->> 'Beneficiary_name', ''),
           'Beneficiary''s Father''s Name', nullif(b.data_waterbody::jsonb ->> 'ben_father', ''),
           'Who manages?', pg_temp.pick_other(b.who_manages, b.specify_other_manager),
           'Which Caste uses the water structure?', nullif(b.caste_who_uses, ''),
           'Households Benefitted', b.household_benefitted,
           'Type of Water Structure',
               CASE WHEN lower(btrim(b.water_structure_type)) = 'other'
                    THEN 'Other: ' || coalesce(b.water_structure_other, '')
                    ELSE nullif(b.water_structure_type, '') END,
           'Usage of Water Structure', b.data_waterbody::jsonb ->> 'select_multiple_uses_structure',
           'Need Maintenance?', nullif(b.need_maintenance, ''),
           'Repair Activities', pg_temp.wb_repair(b.data_waterbody::jsonb),
           'Latitude', b.latitude,
           'Longitude', b.longitude
       )
FROM pl JOIN odk_waterbody b ON b.plan_id = pl.plan_id::text
WHERE b.status_re IS DISTINCT FROM 'rejected' AND NOT coalesce(b.is_deleted, false)

UNION ALL
-- ===== Section E : maintenance of recharge structures (GW) =====
SELECT pl.*, 'E - Maintenance of Existing Assets', 'gw_maintenance', 'maintenance', m.gw_maintenance_id::text,
       jsonb_build_object(
           'Asset Type', 'Recharge Structure',
           'Type of demand', d ->> 'demand_type',
           'Name of Beneficiary Settlement', d ->> 'beneficiary_settlement',
           'Name of Beneficiary', d ->> 'Beneficiary_Name',
           'Gender', d ->> 'select_gender',
           'Beneficiary Father''s Name', d ->> 'ben_father',
           'Type of Recharge Structure', coalesce(d ->> 'select_one_recharge_structure', d ->> 'select_one_water_structure'),
           'Repair Activity', pg_temp.maint_repair(d),
           'Latitude', m.latitude, 'Longitude', m.longitude
       )
FROM pl JOIN odk_gw_maintenance m ON m.plan_id = pl.plan_id::text
CROSS JOIN LATERAL (SELECT coalesce(m.data_gw_maintenance::jsonb, '{}'::jsonb) AS d) x
WHERE NOT coalesce(m.is_deleted, false)

UNION ALL
-- ===== Section E : maintenance of irrigation structures (agri) =====
SELECT pl.*, 'E - Maintenance of Existing Assets', 'agri_maintenance', 'maintenance', m.agri_maintenance_id::text,
       jsonb_build_object(
           'Asset Type', 'Irrigation Structure',
           'Type of demand', d ->> 'demand_type',
           'Name of Beneficiary Settlement', d ->> 'beneficiary_settlement',
           'Name of Beneficiary', d ->> 'Beneficiary_Name',
           'Beneficiary Father''s Name', d ->> 'ben_father',
           'Type of Irrigation Structure', coalesce(d ->> 'select_one_water_structure', d ->> 'select_one_irrigation_structure'),
           'Repair Activity', pg_temp.maint_repair(d),
           'Latitude', m.latitude, 'Longitude', m.longitude
       )
FROM pl JOIN odk_agri_maintenance m ON m.plan_id = pl.plan_id::text
CROSS JOIN LATERAL (SELECT coalesce(m.data_agri_maintenance::jsonb, '{}'::jsonb) AS d) x
WHERE NOT coalesce(m.is_deleted, false)

UNION ALL
-- ===== Section E : maintenance of surface water bodies =====
SELECT pl.*, 'E - Maintenance of Existing Assets', 'swb_maintenance', 'maintenance', m.swb_maintenance_id::text,
       jsonb_build_object(
           'Asset Type', 'Surface Water Body',
           'Type of demand', d ->> 'demand_type',
           'Name of Beneficiary Settlement', d ->> 'beneficiary_settlement',
           'Name of Beneficiary', d ->> 'Beneficiary_Name',
           'Gender', d ->> 'select_gender',
           'Beneficiary Father''s Name', d ->> 'ben_father',
           'Type of Work', coalesce(d ->> 'TYPE_OF_WORK', d ->> 'select_one_water_structure'),
           'Repair Activity', pg_temp.maint_repair(d),
           'Latitude', m.latitude, 'Longitude', m.longitude
       )
FROM pl JOIN odk_swb_maintenance m ON m.plan_id = pl.plan_id::text
CROSS JOIN LATERAL (SELECT coalesce(m.data_swb_maintenance::jsonb, '{}'::jsonb) AS d) x
WHERE NOT coalesce(m.is_deleted, false)

UNION ALL
-- ===== Section E : maintenance of remotely-sensed surface water bodies =====
SELECT pl.*, 'E - Maintenance of Existing Assets', 'swb_rs_maintenance', 'maintenance', m.swb_rs_maintenance_id::text,
       jsonb_build_object(
           'Asset Type', 'Remote Sensed Surface Water Body',
           'Type of demand', d ->> 'demand_type',
           'Name of Beneficiary Settlement', d ->> 'beneficiary_settlement',
           'Name of Beneficiary', d ->> 'Beneficiary_Name',
           'Gender', d ->> 'select_gender',
           'Beneficiary Father''s Name', d ->> 'ben_father',
           'Type of Work', d ->> 'TYPE_OF_WORK',
           'Repair Activity', pg_temp.maint_repair(d),
           'Latitude', m.latitude, 'Longitude', m.longitude
       )
FROM pl JOIN odk_swb_rs_maintenance m ON m.plan_id = pl.plan_id::text
CROSS JOIN LATERAL (SELECT coalesce(m.data_swb_rs_maintenance::jsonb, '{}'::jsonb) AS d) x
WHERE NOT coalesce(m.is_deleted, false)

UNION ALL
-- ===== Section F : new works - recharge structures =====
SELECT pl.*, 'F - Proposed New Works', 'nrm_works', 'work', g.recharge_structure_id,
       jsonb_build_object(
           'Work Category : Irrigation work or Recharge Structure', 'Recharge Structure',
           'Type of demand', d ->> 'demand_type',
           'Work demand', g.work_type,
           'Name of Beneficiary Settlement', g.beneficiary_settlement,
           'Beneficiary''s Name', d ->> 'Beneficiary_Name',
           'Gender', d ->> 'select_gender',
           'Beneficiary Father''s Name', d ->> 'ben_father',
           'Latitude', g.latitude, 'Longitude', g.longitude
       )
FROM pl JOIN odk_groundwater g ON g.plan_id = pl.plan_id::text
CROSS JOIN LATERAL (SELECT coalesce(g.data_groundwater::jsonb, '{}'::jsonb) AS d) x
WHERE g.status_re IS DISTINCT FROM 'rejected' AND NOT coalesce(g.is_deleted, false)

UNION ALL
-- ===== Section F : new works - irrigation works =====
SELECT pl.*, 'F - Proposed New Works', 'nrm_works', 'work', a.irrigation_work_id,
       jsonb_build_object(
           'Work Category : Irrigation work or Recharge Structure', 'Irrigation Work',
           'Type of demand', d ->> 'demand_type_irrigation',
           'Work demand', CASE WHEN lower(a.work_type) = 'other'
                               THEN coalesce(d ->> 'TYPE_OF_WORK_ID_other', 'Other (unspecified)')
                               ELSE a.work_type END,
           'Name of Beneficiary Settlement', a.beneficiary_settlement,
           'Beneficiary''s Name', d ->> 'Beneficiary_Name',
           'Gender', d ->> 'gender',
           'Beneficiary Father''s Name', d ->> 'ben_father',
           'Latitude', a.latitude, 'Longitude', a.longitude
       )
FROM pl JOIN odk_irrigation a ON a.plan_id = pl.plan_id::text
CROSS JOIN LATERAL (SELECT coalesce(a.data_agri::jsonb, '{}'::jsonb) AS d) x
WHERE a.status_re IS DISTINCT FROM 'rejected' AND NOT coalesce(a.is_deleted, false)

UNION ALL
-- ===== Section G : livelihood - livestock =====
SELECT pl.*, 'G - Livelihood Works', 'livelihood', 'livelihood', l.livelihood_id::text,
       jsonb_build_object(
           'Livelihood Works', 'Livestock',
           'Type of Demand', g ->> 'livestock_demand',
           'Work Demand', coalesce(
               pg_temp.pick_other(g ->> 'demands_promoting_livestock', g ->> 'demands_promoting_livestock_other'),
               pg_temp.pick_other(dl ->> 'select_one_promoting_livestock', dl ->> 'select_one_promoting_livestock_other')),
           'Name of Beneficiary Settlement', l.beneficiary_settlement,
           'Name of Beneficiary', coalesce(dl ->> 'beneficiary_name', g ->> 'ben_livestock'),
           'Gender', g ->> 'gender_livestock',
           'Beneficiary Father''s Name', g ->> 'ben_father_livestock',
           'Latitude', l.latitude, 'Longitude', l.longitude
       )
FROM pl JOIN odk_livelihood l ON l.plan_id = pl.plan_id::text
CROSS JOIN LATERAL (SELECT coalesce(l.data_livelihood::jsonb, '{}'::jsonb) AS dl) x
CROSS JOIN LATERAL (SELECT CASE WHEN jsonb_typeof(dl -> 'Livestock') = 'object' THEN dl -> 'Livestock' ELSE '{}'::jsonb END AS g) y
WHERE l.status_re IS DISTINCT FROM 'rejected' AND NOT coalesce(l.is_deleted, false)
  AND (lower(coalesce(g ->> 'is_demand_livestock', '')) = 'yes'
       OR lower(coalesce(dl ->> 'select_one_demand_promoting_livestock', '')) = 'yes')

UNION ALL
-- ===== Section G : livelihood - fisheries =====
SELECT pl.*, 'G - Livelihood Works', 'livelihood', 'livelihood', l.livelihood_id::text,
       jsonb_build_object(
           'Livelihood Works', 'Fisheries',
           'Type of Demand', g ->> 'demand_type_fisheries',
           'Work Demand', coalesce(
               pg_temp.pick_other(g ->> 'select_one_promoting_fisheries', g ->> 'select_one_promoting_fisheries_other'),
               pg_temp.pick_other(dl ->> 'select_one_promoting_fisheries', dl ->> 'select_one_promoting_fisheries_other')),
           'Name of Beneficiary Settlement', l.beneficiary_settlement,
           'Name of Beneficiary', coalesce(dl ->> 'beneficiary_name', g ->> 'ben_fisheries'),
           'Gender', g ->> 'gender_fisheries',
           'Beneficiary Father''s Name', g ->> 'ben_father_fisheries',
           'Latitude', l.latitude, 'Longitude', l.longitude
       )
FROM pl JOIN odk_livelihood l ON l.plan_id = pl.plan_id::text
CROSS JOIN LATERAL (SELECT coalesce(l.data_livelihood::jsonb, '{}'::jsonb) AS dl) x
CROSS JOIN LATERAL (SELECT CASE WHEN jsonb_typeof(dl -> 'fisheries') = 'object' THEN dl -> 'fisheries' ELSE '{}'::jsonb END AS g) y
WHERE l.status_re IS DISTINCT FROM 'rejected' AND NOT coalesce(l.is_deleted, false)
  AND (lower(coalesce(g ->> 'is_demand_fisheris', '')) = 'yes'
       OR lower(coalesce(dl ->> 'select_one_demand_promoting_fisheries', '')) = 'yes')

UNION ALL
-- ===== Section G : livelihood - plantations (livelihood form) =====
SELECT pl.*, 'G - Livelihood Works', 'livelihood', 'livelihood', l.livelihood_id::text,
       jsonb_build_object(
           'Livelihood Works', 'Plantations',
           'Type of Demand', g ->> 'demand_type_plantations',
           'Name of Plantation Crop', coalesce(dl ->> 'Plantation', g ->> 'crop_name'),
           'Name of Beneficiary Settlement', l.beneficiary_settlement,
           'Name of Beneficiary', coalesce(dl ->> 'beneficiary_name', g ->> 'ben_plantation'),
           'Gender', g ->> 'gender',
           'Beneficiary Father''s Name', g ->> 'ben_father',
           'Total Acres', coalesce(dl ->> 'Plantation_crop', g ->> 'crop_area'),
           'Latitude', l.latitude, 'Longitude', l.longitude
       )
FROM pl JOIN odk_livelihood l ON l.plan_id = pl.plan_id::text
CROSS JOIN LATERAL (SELECT coalesce(l.data_livelihood::jsonb, '{}'::jsonb) AS dl) x
CROSS JOIN LATERAL (SELECT CASE WHEN jsonb_typeof(dl -> 'plantations') = 'object' THEN dl -> 'plantations' ELSE '{}'::jsonb END AS g) y
WHERE l.status_re IS DISTINCT FROM 'rejected' AND NOT coalesce(l.is_deleted, false)
  AND (lower(coalesce(dl ->> 'select_one_demand_plantation', '')) = 'yes'
       OR lower(coalesce(g ->> 'select_plantation_demands', '')) = 'yes')

UNION ALL
-- ===== Section G : livelihood - kitchen garden =====
SELECT pl.*, 'G - Livelihood Works', 'livelihood', 'livelihood', l.livelihood_id::text,
       jsonb_build_object(
           'Livelihood Works', 'Kitchen Garden',
           'Type of Demand', g ->> 'demand_type_kitchen_garden',
           'Work Demand', dl ->> 'Plantation',
           'Name of Beneficiary Settlement', l.beneficiary_settlement,
           'Name of Beneficiary', coalesce(dl ->> 'beneficiary_name', g ->> 'ben_kitchen_gardens'),
           'Gender', g ->> 'gender_kitchen_gardens',
           'Beneficiary Father''s Name', g ->> 'ben_father_kitchen_gardens',
           'Total Acres', coalesce(dl ->> 'area_didi_badi', g ->> 'area_kg'),
           'Latitude', l.latitude, 'Longitude', l.longitude
       )
FROM pl JOIN odk_livelihood l ON l.plan_id = pl.plan_id::text
CROSS JOIN LATERAL (SELECT coalesce(l.data_livelihood::jsonb, '{}'::jsonb) AS dl) x
CROSS JOIN LATERAL (SELECT CASE WHEN jsonb_typeof(dl -> 'kitchen_gardens') = 'object' THEN dl -> 'kitchen_gardens' ELSE '{}'::jsonb END AS g) y
WHERE l.status_re IS DISTINCT FROM 'rejected' AND NOT coalesce(l.is_deleted, false)
  AND (lower(coalesce(dl ->> 'indi_assets', '')) = 'yes'
       OR lower(coalesce(g ->> 'assets_kg', '')) = 'yes')

UNION ALL
-- ===== Section G : livelihood - plantations (agrohorticulture form) =====
SELECT pl.*, 'G - Livelihood Works', 'agrohorticulture', 'livelihood', h.agrohorticulture_id::text,
       jsonb_build_object(
           'Livelihood Works', 'Plantations',
           'Type of Demand', d ->> 'demand_type_plantations',
           'Name of Plantation Crop',
               nullif(btrim(concat_ws(' ', d ->> 'select_multiple_species', d ->> 'select_multiple_species_other')), ''),
           'Name of Beneficiary Settlement', d ->> 'beneficiary_settlement',
           'Name of Beneficiary', d ->> 'beneficiary_name',
           'Gender', d ->> 'gender',
           'Beneficiary Father''s Name', d ->> 'ben_father',
           'Total Acres', d ->> 'crop_area',
           'Latitude', h.latitude, 'Longitude', h.longitude
       )
FROM pl JOIN odk_agrohorticulture h ON h.plan_id = pl.plan_id::text
CROSS JOIN LATERAL (SELECT coalesce(h.data_agohorticulture::jsonb, '{}'::jsonb) AS d) x
WHERE h.status_re IS DISTINCT FROM 'rejected' AND NOT coalesce(h.is_deleted, false);

-- ------------------------------------------------------------- final shape ---

CREATE OR REPLACE TEMP VIEW pradan_dpr_dump AS
SELECT
    org_name                         AS "Organization",
    org_id                           AS "Organization ID",
    project_name                     AS "Project",
    project_id                       AS "Project ID",
    plan_name                        AS "Plan",
    plan_id                          AS "Plan ID",
    facilitator_name                 AS "Facilitator Name",
    village_name                     AS "Name of the Village",
    gram_panchayat                   AS "Name of the Gram Panchayat",
    state_name                       AS "State",
    state_id                         AS "State ID",
    district_name                    AS "District",
    district_id                      AS "District ID",
    tehsil_name                      AS "Tehsil",
    tehsil_id                        AS "Tehsil ID",
    section                          AS "Section",
    dpr_table                        AS "DPR Table",
    record_type                      AS "Record Type",
    record_id                        AS "Record ID",
    d ->> 'Number of Settlements in the Village'                            AS "Number of Settlements in the Village",
    d ->> 'Intersecting Micro Watershed IDs'                               AS "Intersecting Micro Watershed IDs",
    d ->> 'Latitude and Longitude of the Village'                          AS "Latitude and Longitude of the Village",
    d ->> 'Name of the Settlement'                                         AS "Name of the Settlement",
    d ->> 'Total Number of Households'                                     AS "Total Number of Households",
    d ->> 'Settlement Type'                                                AS "Settlement Type",
    d ->> 'Caste Group'                                                    AS "Caste Group",
    d ->> 'Total Households - SC'                                          AS "Total Households - SC",
    d ->> 'Total Households - ST'                                          AS "Total Households - ST",
    d ->> 'Total Households - OBC'                                         AS "Total Households - OBC",
    d ->> 'Total Households - General'                                     AS "Total Households - General",
    d ->> 'Total marginal farmers (<2 acres)'                              AS "Total marginal farmers (<2 acres)",
    d ->> 'Settlement''s Name'                                             AS "Settlement's Name",
    d ->> 'Total Households - applied - have NREGA job cards in previous y' AS "Total Households - applied - have NREGA job cards in previous y",
    d ->> 'Total work days in previous year'                               AS "Total work days in previous year",
    d ->> 'Work demands made in previous year'                             AS "Work demands made in previous year",
    d ->> 'Were you involved in the village level planning?'               AS "Were you involved in the village level planning?",
    d ->> 'Issues'                                                         AS "Issues",
    d ->> 'Irrigation Source'                                              AS "Irrigation Source",
    d ->> 'Crops grown in Kharif'                                          AS "Crops grown in Kharif",
    d ->> 'Kharif acreage (acres)'                                         AS "Kharif acreage (acres)",
    d ->> 'Crops grown in Rabi'                                            AS "Crops grown in Rabi",
    d ->> 'Rabi acreage (acres)'                                           AS "Rabi acreage (acres)",
    d ->> 'Crops grown in Zaid'                                            AS "Crops grown in Zaid",
    d ->> 'Zaid acreage (acres)'                                           AS "Zaid acreage (acres)",
    d ->> 'Cropping Intensity'                                             AS "Cropping Intensity",
    d ->> 'Land Classification'                                            AS "Land Classification",
    d ->> 'Goats'                                                          AS "Goats",
    d ->> 'Sheep'                                                          AS "Sheep",
    d ->> 'Cattle'                                                         AS "Cattle",
    d ->> 'Piggery'                                                        AS "Piggery",
    d ->> 'Poultry'                                                        AS "Poultry",
    d ->> 'Microwatershed ID'                                              AS "Microwatershed ID",
    d ->> 'Latitude and Longitude (Centroid)'                              AS "Latitude and Longitude (Centroid)",
    d ->> 'Name of Beneficiary''s Settlement'                              AS "Name of Beneficiary's Settlement",
    d ->> 'Number of Wells'                                                AS "Number of Wells",
    d ->> 'Number of Water Structures'                                     AS "Number of Water Structures",
    d ->> 'Total Number of Household Benefitted'                           AS "Total Number of Household Benefitted",
    d ->> 'MWS ID'                                                         AS "MWS ID",
    d ->> 'Name of Beneficiary Settlement'                                 AS "Name of Beneficiary Settlement",
    d ->> 'Type of Well'                                                   AS "Type of Well",
    d ->> 'Who owns the Well'                                              AS "Who owns the Well",
    d ->> 'Beneficiary Name'                                               AS "Beneficiary Name",
    d ->> 'Beneficiary''s Father''s Name'                                  AS "Beneficiary's Father's Name",
    d ->> 'Water Availability'                                             AS "Water Availability",
    d ->> 'Households Benefitted'                                          AS "Households Benefitted",
    d ->> 'Which Caste uses the well?'                                     AS "Which Caste uses the well?",
    d ->> 'Well Usage'                                                     AS "Well Usage",
    d ->> 'Need Maintenance?'                                              AS "Need Maintenance?",
    d ->> 'Repair Activities'                                              AS "Repair Activities",
    d ->> 'Latitude'                                                       AS "Latitude",
    d ->> 'Longitude'                                                      AS "Longitude",
    d ->> 'Name of the Beneficiary''s Settlement'                          AS "Name of the Beneficiary's Settlement",
    d ->> 'Who owns the water structure?'                                  AS "Who owns the water structure?",
    d ->> 'Who manages?'                                                   AS "Who manages?",
    d ->> 'Which Caste uses the water structure?'                          AS "Which Caste uses the water structure?",
    d ->> 'Type of Water Structure'                                        AS "Type of Water Structure",
    d ->> 'Usage of Water Structure'                                       AS "Usage of Water Structure",
    d ->> 'Asset Type'                                                     AS "Asset Type",
    d ->> 'Type of demand'                                                 AS "Type of demand",
    d ->> 'Type of Recharge Structure'                                     AS "Type of Recharge Structure",
    d ->> 'Type of Irrigation Structure'                                   AS "Type of Irrigation Structure",
    d ->> 'Type of Work'                                                   AS "Type of Work",
    d ->> 'Repair Activity'                                                AS "Repair Activity",
    d ->> 'Work Category : Irrigation work or Recharge Structure'          AS "Work Category : Irrigation work or Recharge Structure",
    d ->> 'Work demand'                                                    AS "Work demand",
    d ->> 'Beneficiary''s Name'                                            AS "Beneficiary's Name",
    d ->> 'Gender'                                                         AS "Gender",
    d ->> 'Livelihood Works'                                               AS "Livelihood Works",
    d ->> 'Type of Demand'                                                 AS "Type of Demand",
    d ->> 'Work Demand'                                                    AS "Work Demand",
    d ->> 'Beneficiary Father''s Name'                                     AS "Beneficiary Father's Name",
    d ->> 'Name of Beneficiary'                                            AS "Name of Beneficiary",
    d ->> 'Name of Plantation Crop'                                        AS "Name of Plantation Crop",
    d ->> 'Total Acres'                                                    AS "Total Acres"
FROM dpr_rows;
