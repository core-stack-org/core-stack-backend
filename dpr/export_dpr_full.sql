COPY (

WITH plan_ctx AS (
    SELECT
        p.id              AS plan_id,
        p.plan            AS plan_name,
        ss.state_name     AS state_name_soi,
        ds.district_name  AS district_name_soi,
        ts.tehsil_name    AS tehsil_soi,
        o.id::text        AS org_id,
        o.name            AS org_name,
        pr.name           AS project,
        p.village_name,
        p.facilitator_name,
        p.gram_panchayat,
        p.latitude        AS plan_lat,
        p.longitude       AS plan_lon,
        (SELECT COUNT(*)
         FROM odk_settlement s
         WHERE s.plan_id = p.id::text
           AND s.status_re != 'rejected'
           AND s.is_deleted = FALSE)::text AS total_settlements
    FROM plans_planapp p
    LEFT JOIN geoadmin_statesoi         ss ON ss.id = p.state_soi_id
    LEFT JOIN geoadmin_districtsoi      ds ON ds.id = p.district_soi_id
    LEFT JOIN geoadmin_tehsilsoi        ts ON ts.id = p.tehsil_soi_id
    LEFT JOIN organization_organization o  ON o.id  = p.organization_id
    LEFT JOIN projects_project          pr ON pr.id = p.project_id
    WHERE p.enabled = TRUE
      AND p.plan NOT ILIKE '%test%'
      AND p.organization_id::text IN (
          'e6f3a128-837f-41c5-a23a-210d657a2c59',
          '92adc8e3-6353-44dc-a16b-aed78cebe277'
      )
)

-- ── A  Team Details ──────────────────────────────────────────────────────────
-- One row per plan; org/project/plan/facilitator/village are in the context cols.
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Section A (1)
    'PRA, Gram Sabha, Transect Walk, GIS Mapping'   AS "A: Process",
    -- Section B (NULL x4)
    NULL::text AS "B: Gram Panchayat",
    NULL::text AS "B: Number of Settlements",
    NULL::text AS "B: Village Latitude",
    NULL::text AS "B: Village Longitude",
    -- Section C: Settlement (NULL x15)
    NULL::text AS "C: Settlement Name",
    NULL::text AS "C: Total Households",
    NULL::text AS "C: Settlement Type",
    NULL::text AS "C: Caste Group",
    NULL::text AS "C: SC Count",
    NULL::text AS "C: ST Count",
    NULL::text AS "C: OBC Count",
    NULL::text AS "C: General Count",
    NULL::text AS "C: Marginal Farmers (<2 acres)",
    NULL::text AS "C: NREGA Applied",
    NULL::text AS "C: NREGA Card",
    NULL::text AS "C: NREGA Work Days",
    NULL::text AS "C: NREGA Past Work Demands",
    NULL::text AS "C: NREGA Planning Involved",
    NULL::text AS "C: NREGA Issues",
    -- Section C: Crop (NULL x10)
    NULL::text AS "C: Crop Settlement",
    NULL::text AS "C: Irrigation Source",
    NULL::text AS "C: Kharif Crops",
    NULL::text AS "C: Kharif Acreage (acres)",
    NULL::text AS "C: Rabi Crops",
    NULL::text AS "C: Rabi Acreage (acres)",
    NULL::text AS "C: Zaid Crops",
    NULL::text AS "C: Zaid Acreage (acres)",
    NULL::text AS "C: Cropping Intensity",
    NULL::text AS "C: Land Classification",
    -- Section C: Livestock (NULL x6)
    NULL::text AS "C: Livestock Settlement",
    NULL::text AS "C: Goats",
    NULL::text AS "C: Sheep",
    NULL::text AS "C: Cattle",
    NULL::text AS "C: Piggery",
    NULL::text AS "C: Poultry",
    -- Section D: Well (NULL x13)
    NULL::text AS "D: Well Settlement",
    NULL::text AS "D: Well Type",
    NULL::text AS "D: Well Owner",
    NULL::text AS "D: Well Beneficiary Name",
    NULL::text AS "D: Well Beneficiary Father",
    NULL::text AS "D: Water Availability",
    NULL::text AS "D: Well Households Benefitted",
    NULL::text AS "D: Well Caste Uses",
    NULL::text AS "D: Well Usage",
    NULL::text AS "D: Well Need Maintenance",
    NULL::text AS "D: Well Repair Activities",
    NULL::text AS "D: Well Latitude",
    NULL::text AS "D: Well Longitude",
    -- Section D: Waterbody (NULL x13)
    NULL::text AS "D: WB Settlement",
    NULL::text AS "D: WB Owner",
    NULL::text AS "D: WB Beneficiary Name",
    NULL::text AS "D: WB Beneficiary Father",
    NULL::text AS "D: WB Who Manages",
    NULL::text AS "D: WB Caste Uses",
    NULL::text AS "D: WB Households Benefitted",
    NULL::text AS "D: Water Structure Type",
    NULL::text AS "D: Water Structure Usage",
    NULL::text AS "D: WB Need Maintenance",
    NULL::text AS "D: WB Repair Activities",
    NULL::text AS "D: WB Latitude",
    NULL::text AS "D: WB Longitude",
    -- Section E (NULL x8)
    NULL::text AS "E: Type of demand",
    NULL::text AS "E: Name of the Beneficiary Settlement",
    NULL::text AS "E: Beneficiary Name",
    NULL::text AS "E: Beneficiary's Father's Name",
    NULL::text AS "E: Type of Recharge Structure",
    NULL::text AS "E: Repair Activities",
    NULL::text AS "E: Latitude",
    NULL::text AS "E: Longitude",
    -- Section F (NULL x9)
    NULL::text AS "F: Work Category",
    NULL::text AS "F: Type of demand",
    NULL::text AS "F: Work demand",
    NULL::text AS "F: Name of Beneficiary's Settlement",
    NULL::text AS "F: Beneficiary's Name",
    NULL::text AS "F: Gender",
    NULL::text AS "F: Beneficiary's Father's Name",
    NULL::text AS "F: Latitude",
    NULL::text AS "F: Longitude",
    -- Section G.1 (NULL x9)
    NULL::text AS "G1: Livelihood Works",
    NULL::text AS "G1: Type of Demand",
    NULL::text AS "G1: Work Demand",
    NULL::text AS "G1: Name of Beneficiary Settlement",
    NULL::text AS "G1: Beneficiary's Name",
    NULL::text AS "G1: Gender",
    NULL::text AS "G1: Beneficiary Father's Name",
    NULL::text AS "G1: Latitude",
    NULL::text AS "G1: Longitude",
    -- Section G.2 (NULL x10)
    NULL::text AS "G2: Livelihood Works",
    NULL::text AS "G2: Type of demand",
    NULL::text AS "G2: Name of Beneficiary Settlement",
    NULL::text AS "G2: Name of Beneficiary",
    NULL::text AS "G2: Gender",
    NULL::text AS "G2: Beneficiary's Father's Name",
    NULL::text AS "G2: Name of Plantation Crop",
    NULL::text AS "G2: Total Acres",
    NULL::text AS "G2: Latitude",
    NULL::text AS "G2: Longitude"
FROM plan_ctx pc

UNION ALL

-- ── B  Village Brief ─────────────────────────────────────────────────────────
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Section A (NULL x1)
    NULL::text,
    -- Section B
    pc.gram_panchayat,
    pc.total_settlements,
    pc.plan_lat::text,
    pc.plan_lon::text,
    -- Section C: Settlement (NULL x15)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section C: Crop (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section C: Livestock (NULL x6)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section D: Well (NULL x13)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text,
    -- Section D: Waterbody (NULL x13)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text,
    -- Section E (NULL x8)
    NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section F (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.1 (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.2 (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text
FROM plan_ctx pc

UNION ALL

-- ── C.1  Settlements ─────────────────────────────────────────────────────────
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Section A (NULL x1)
    NULL::text,
    -- Section B (NULL x4)
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section C: Settlement
    s.settlement_name,
    s.number_of_households::text,
    s.largest_caste,
    CASE
        WHEN lower(COALESCE(s.largest_caste, '')) = 'single caste group' THEN s.smallest_caste
        WHEN lower(COALESCE(s.largest_caste, '')) = 'mixed caste group'  THEN s.settlement_status
        ELSE NULL
    END,
    (CASE WHEN left(trim(COALESCE(s.data_settlement, '')), 1) = '{' THEN s.data_settlement::jsonb ELSE '{}'::jsonb END)->>'count_sc',
    (CASE WHEN left(trim(COALESCE(s.data_settlement, '')), 1) = '{' THEN s.data_settlement::jsonb ELSE '{}'::jsonb END)->>'count_st',
    (CASE WHEN left(trim(COALESCE(s.data_settlement, '')), 1) = '{' THEN s.data_settlement::jsonb ELSE '{}'::jsonb END)->>'count_obc',
    (CASE WHEN left(trim(COALESCE(s.data_settlement, '')), 1) = '{' THEN s.data_settlement::jsonb ELSE '{}'::jsonb END)->>'count_general',
    (CASE WHEN left(trim(COALESCE(s.farmer_family,    '')), 1) = '{' THEN s.farmer_family::jsonb    ELSE '{}'::jsonb END)->>'marginal_farmers',
    NULLIF(s.nrega_job_applied::text, '0'),
    NULLIF(s.nrega_job_card::text, '0'),
    NULLIF(s.nrega_work_days::text, '0'),
    s.nrega_past_work,
    s.nrega_demand,
    s.nrega_issues,
    -- Section C: Crop (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section C: Livestock (NULL x6)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section D: Well (NULL x13)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text,
    -- Section D: Waterbody (NULL x13)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text,
    -- Section E (NULL x8)
    NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section F (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.1 (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.2 (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text
FROM odk_settlement s
JOIN plan_ctx pc ON pc.plan_id::text = s.plan_id
WHERE s.is_deleted = FALSE AND s.status_re != 'rejected'

UNION ALL

-- ── C.2  Crop Patterns ────────────────────────────────────────────────────────
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Section A (NULL x1)
    NULL::text,
    -- Section B (NULL x4)
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section C: Settlement (NULL x15)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section C: Crop
    cr.beneficiary_settlement,
    cr.irrigation_source,
    cr.cropping_patterns_kharif,
    CASE
        WHEN NULLIF(TRIM(COALESCE(dc->>'total_area_cultivation_kharif', '')), '') IS NOT NULL
         AND dc->>'total_area_cultivation_kharif' != 'NA'
        THEN round((dc->>'total_area_cultivation_kharif')::numeric * 2.47105, 4)::text
        ELSE NULL
    END,
    cr.cropping_patterns_rabi,
    CASE
        WHEN NULLIF(TRIM(COALESCE(dc->>'total_area_cultivation_Rabi', '')), '') IS NOT NULL
         AND dc->>'total_area_cultivation_Rabi' != 'NA'
        THEN round((dc->>'total_area_cultivation_Rabi')::numeric * 2.47105, 4)::text
        ELSE NULL
    END,
    cr.cropping_patterns_zaid,
    CASE
        WHEN NULLIF(TRIM(COALESCE(dc->>'total_area_cultivation_Zaid', '')), '') IS NOT NULL
         AND dc->>'total_area_cultivation_Zaid' != 'NA'
        THEN round((dc->>'total_area_cultivation_Zaid')::numeric * 2.47105, 4)::text
        ELSE NULL
    END,
    cr.agri_productivity,
    cr.land_classification,
    -- Section C: Livestock (NULL x6)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section D: Well (NULL x13)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text,
    -- Section D: Waterbody (NULL x13)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text,
    -- Section E (NULL x8)
    NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section F (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.1 (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.2 (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text
FROM odk_crop cr
CROSS JOIN LATERAL (SELECT CASE WHEN left(trim(COALESCE(cr.data_crop, '')), 1) = '{' THEN cr.data_crop::jsonb ELSE '{}'::jsonb END) AS _dc(dc)
JOIN plan_ctx pc ON pc.plan_id::text = cr.plan_id
WHERE cr.is_deleted = FALSE AND cr.status_re != 'rejected'

UNION ALL

-- ── C.3  Livestock ───────────────────────────────────────────────────────────
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Section A (NULL x1)
    NULL::text,
    -- Section B (NULL x4)
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section C: Settlement (NULL x15)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section C: Crop (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section C: Livestock
    s.settlement_name,
    NULLIF(NULLIF(NULLIF(lc->>'Goats',   ''), '0'), 'None'),
    NULLIF(NULLIF(NULLIF(lc->>'Sheep',   ''), '0'), 'None'),
    NULLIF(NULLIF(NULLIF(lc->>'Cattle',  ''), '0'), 'None'),
    NULLIF(NULLIF(NULLIF(lc->>'Piggery', ''), '0'), 'None'),
    NULLIF(NULLIF(NULLIF(lc->>'Poultry', ''), '0'), 'None'),
    -- Section D: Well (NULL x13)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text,
    -- Section D: Waterbody (NULL x13)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text,
    -- Section E (NULL x8)
    NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section F (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.1 (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.2 (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text
FROM odk_settlement s
CROSS JOIN LATERAL (SELECT CASE WHEN left(trim(COALESCE(s.livestock_census, '')), 1) = '{' THEN s.livestock_census::jsonb ELSE '{}'::jsonb END) AS _lc(lc)
JOIN plan_ctx pc ON pc.plan_id::text = s.plan_id
WHERE s.is_deleted = FALSE AND s.status_re != 'rejected'

UNION ALL

-- ── D.1  Wells ────────────────────────────────────────────────────────────────
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Section A (NULL x1)
    NULL::text,
    -- Section B (NULL x4)
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section C: Settlement (NULL x15)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section C: Crop (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section C: Livestock (NULL x6)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section D: Well
    w.beneficiary_settlement,
    dw->>'select_one_well_type',
    w.owner,
    dw->>'Beneficiary_name',
    dw->>'ben_father',
    dw->>'select_one_year',
    w.households_benefitted::text,
    w.caste_uses,
    CASE
        WHEN lower(COALESCE(dw->'Well_usage'->>'select_one_well_used', '')) = 'other'
             AND (dw->'Well_usage'->>'select_one_well_used_other') IS NOT NULL
        THEN 'Other: ' || (dw->'Well_usage'->>'select_one_well_used_other')
        ELSE NULLIF(dw->'Well_usage'->>'select_one_well_used', '')
    END,
    w.need_maintenance,
    COALESCE(
        CASE
            WHEN lower(COALESCE(dw->'Well_usage'->>'repairs_type', '')) = 'other'
                 AND (dw->'Well_usage'->>'repairs_type_other') IS NOT NULL
            THEN 'Other: ' || (dw->'Well_usage'->>'repairs_type_other')
            ELSE NULLIF(replace(COALESCE(dw->'Well_usage'->>'repairs_type', ''), '_', ' '), '')
        END,
        CASE
            WHEN lower(COALESCE(dw->'Well_condition'->>'select_one_repairs_well', '')) = 'other'
                 AND (dw->'Well_condition'->>'select_one_repairs_well_other') IS NOT NULL
            THEN 'Other: ' || (dw->'Well_condition'->>'select_one_repairs_well_other')
            ELSE NULLIF(replace(COALESCE(dw->'Well_condition'->>'select_one_repairs_well', ''), '_', ' '), '')
        END
    ),
    w.latitude::text,
    w.longitude::text,
    -- Section D: Waterbody (NULL x13)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text,
    -- Section E (NULL x8)
    NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section F (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.1 (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.2 (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text
FROM odk_well w
CROSS JOIN LATERAL (SELECT CASE WHEN left(trim(COALESCE(w.data_well, '')), 1) = '{' THEN w.data_well::jsonb ELSE '{}'::jsonb END) AS _dw(dw)
JOIN plan_ctx pc ON pc.plan_id::text = w.plan_id
WHERE w.is_deleted = FALSE AND w.status_re != 'rejected'

UNION ALL

-- ── D.2  Waterbodies ──────────────────────────────────────────────────────────
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Section A (NULL x1)
    NULL::text,
    -- Section B (NULL x4)
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section C: Settlement (NULL x15)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section C: Crop (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section C: Livestock (NULL x6)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section D: Well (NULL x13)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text,
    -- Section D: Waterbody
    wb.beneficiary_settlement,
    wb.owner,
    dwb->>'Beneficiary_name',
    dwb->>'ben_father',
    CASE
        WHEN lower(COALESCE(wb.who_manages, '')) = 'other' AND wb.specify_other_manager IS NOT NULL
        THEN 'Other: ' || wb.specify_other_manager
        ELSE NULLIF(wb.who_manages, '')
    END,
    wb.caste_who_uses,
    wb.household_benefitted::text,
    CASE
        WHEN lower(COALESCE(wb.water_structure_type, '')) = 'other' AND wb.water_structure_other IS NOT NULL
        THEN 'Other: ' || wb.water_structure_other
        ELSE wb.water_structure_type
    END,
    NULLIF(replace(COALESCE(dwb->>'select_multiple_uses_structure', ''), '_', ' '), ''),
    wb.need_maintenance,
    COALESCE(
        CASE lower(COALESCE(wb.water_structure_type, ''))
            WHEN 'farm pond'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_farm_ponds', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_farm_ponds_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_farm_ponds', ''), '_', ' '), '')
                     END
            WHEN 'community pond'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_community_pond', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_community_pond_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_community_pond', ''), '_', ' '), '')
                     END
            WHEN 'large water body'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_large_water_body', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_large_water_body_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_large_water_body', ''), '_', ' '), '')
                     END
            WHEN 'canal'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_canal', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_canal_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_canal', ''), '_', ' '), '')
                     END
            WHEN 'check dam'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_check_dam', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_check_dam_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_check_dam', ''), '_', ' '), '')
                     END
            WHEN 'percolation tank'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_percolation_tank', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_percolation_tank_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_percolation_tank', ''), '_', ' '), '')
                     END
            WHEN 'earthen gully plug'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_earthen_gully_plug', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_earthen_gully_plug_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_earthen_gully_plug', ''), '_', ' '), '')
                     END
            WHEN 'drainage/soakage channels'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_drainage_soakage_channels', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_drainage_soakage_channels_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_drainage_soakage_channels', ''), '_', ' '), '')
                     END
            WHEN 'recharge pits'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_recharge_pits', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_recharge_pits_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_recharge_pits', ''), '_', ' '), '')
                     END
            WHEN 'soakage pits'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_soakage_pits', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_soakage_pits_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_soakage_pits', ''), '_', ' '), '')
                     END
            WHEN 'sokage pits'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_soakage_pits', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_soakage_pits_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_soakage_pits', ''), '_', ' '), '')
                     END
            WHEN 'trench cum bund network'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_trench_cum_bund_network', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_trench_cum_bund_network_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_trench_cum_bund_network', ''), '_', ' '), '')
                     END
            WHEN 'continuous contour trenches (cct)'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_Continuous_contour_trenches', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_Continuous_contour_trenches_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_Continuous_contour_trenches', ''), '_', ' '), '')
                     END
            WHEN 'staggered contour trenches(sct)'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_Staggered_contour_trenches', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_Staggered_contour_trenches_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_Staggered_contour_trenches', ''), '_', ' '), '')
                     END
            WHEN 'water absorption trenches(wat)'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_Water_absorption_trenches', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_Water_absorption_trenches_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_Water_absorption_trenches', ''), '_', ' '), '')
                     END
            WHEN 'loose boulder structure'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_loose_boulder_structure', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_loose_boulder_structure_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_loose_boulder_structure', ''), '_', ' '), '')
                     END
            WHEN 'rock fill dam'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_rock_fill_dam', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_rock_fill_dam_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_rock_fill_dam', ''), '_', ' '), '')
                     END
            WHEN 'stone bunding'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_stone_bunding', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_stone_bunding_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_stone_bunding', ''), '_', ' '), '')
                     END
            WHEN 'diversion drains'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_diversion_drains', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_diversion_drains_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_diversion_drains', ''), '_', ' '), '')
                     END
            WHEN 'bunding:contour bunds/ graded bunds'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_bunding', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_bunding_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_bunding', ''), '_', ' '), '')
                     END
            WHEN 'contour bunds/graded bunds'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_bunding', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_bunding_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_bunding', ''), '_', ' '), '')
                     END
            WHEN 'farm bund'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_farm_bund', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_farm_bund_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_farm_bund', ''), '_', ' '), '')
                     END
            WHEN '5% model structure'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_model5_structure', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_model5_structure_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_model5_structure', ''), '_', ' '), '')
                     END
            WHEN '30-40 model structure'
                THEN CASE WHEN lower(COALESCE(dwb->>'Repair_of_30_40_model_structure', '')) = 'other'
                          THEN 'Other: ' || dwb->>'Repair_of_30_40_model_structure_other'
                          ELSE NULLIF(replace(COALESCE(dwb->>'Repair_of_30_40_model_structure', ''), '_', ' '), '')
                     END
            ELSE NULL
        END,
        NULLIF(dwb->>'select_one_activities', '')
    ),
    wb.latitude::text,
    wb.longitude::text,
    -- Section E (NULL x8)
    NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section F (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.1 (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.2 (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text
FROM odk_waterbody wb
CROSS JOIN LATERAL (SELECT CASE WHEN left(trim(COALESCE(wb.data_waterbody, '')), 1) = '{' THEN wb.data_waterbody::jsonb ELSE '{}'::jsonb END) AS _dwb(dwb)
JOIN plan_ctx pc ON pc.plan_id::text = wb.plan_id
WHERE wb.is_deleted = FALSE AND wb.status_re != 'rejected'

UNION ALL

-- ── E.1  GW Maintenance ──────────────────────────────────────────────────────
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Sections A–D (NULL x62)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text,
    -- Section E
    CASE
        WHEN lower(replace(COALESCE(d->>'demand_type', ''), '_', ' ')) IN ('community','community well','community demand','public','public well','shared among families') THEN 'Community Demand'
        WHEN lower(replace(COALESCE(d->>'demand_type', ''), '_', ' ')) IN ('private','privately owned','individual demand') THEN 'Individual Demand'
        ELSE d->>'demand_type'
    END,
    d->>'beneficiary_settlement',
    d->>'Beneficiary_Name',
    d->>'ben_father',
    COALESCE(d->>'select_one_recharge_structure', d->>'select_one_water_structure', 'NA'),
    COALESCE(
        CASE lower(COALESCE(d->>'select_one_recharge_structure', d->>'select_one_water_structure', ''))
            WHEN 'check dam'                           THEN CASE WHEN lower(d->>'select_one_check_dam')='other'                        THEN d->>'select_one_check_dam_other'                       ELSE NULLIF(d->>'select_one_check_dam','')                       END
            WHEN 'percolation tank'                    THEN CASE WHEN lower(d->>'select_one_percolation_tank')='other'                 THEN d->>'select_one_percolation_tank_other'                ELSE NULLIF(d->>'select_one_percolation_tank','')                END
            WHEN 'earthen gully plug'                  THEN CASE WHEN lower(d->>'select_one_earthen_gully_plug')='other'              THEN d->>'select_one_earthen_gully_plug_other'              ELSE NULLIF(d->>'select_one_earthen_gully_plug','')              END
            WHEN 'drainage/soakage channels'           THEN CASE WHEN lower(d->>'select_one_drainage_soakage_channels')='other'       THEN d->>'select_one_drainage_soakage_channels_other'       ELSE NULLIF(d->>'select_one_drainage_soakage_channels','')       END
            WHEN 'recharge pits'                       THEN CASE WHEN lower(d->>'select_one_recharge_pits')='other'                   THEN d->>'select_one_recharge_pits_other'                   ELSE NULLIF(d->>'select_one_recharge_pits','')                   END
            WHEN 'sokage pits'                         THEN CASE WHEN lower(d->>'select_one_sokage_pits')='other'                     THEN d->>'select_one_sokage_pits_other'                     ELSE NULLIF(d->>'select_one_sokage_pits','')                     END
            WHEN 'trench cum bund network'             THEN CASE WHEN lower(d->>'select_one_trench_cum_bund_network')='other'         THEN d->>'select_one_trench_cum_bund_network_other'         ELSE NULLIF(d->>'select_one_trench_cum_bund_network','')         END
            WHEN 'continuous contour trenches (cct)'   THEN CASE WHEN lower(d->>'select_one_continuous_contour_trenches')='other'    THEN d->>'select_one_continuous_contour_trenches_other'    ELSE NULLIF(d->>'select_one_continuous_contour_trenches','')    END
            WHEN 'staggered contour trenches(sct)'     THEN CASE WHEN lower(d->>'select_one_staggered_contour_trenches')='other'     THEN d->>'select_one_staggered_contour_trenches_other'     ELSE NULLIF(d->>'select_one_staggered_contour_trenches','')     END
            WHEN 'water absorption trenches(wat)'      THEN CASE WHEN lower(d->>'select_one_water_absorption_trenches')='other'      THEN d->>'select_one_water_absorption_trenches_other'      ELSE NULLIF(d->>'select_one_water_absorption_trenches','')      END
            WHEN 'loose boulder structure'             THEN CASE WHEN lower(d->>'select_one_loose_boulder_structure')='other'        THEN d->>'select_one_loose_boulder_structure_other'        ELSE NULLIF(d->>'select_one_loose_boulder_structure','')        END
            WHEN 'rock fill dam'                       THEN CASE WHEN lower(d->>'select_one_rock_fill_dam')='other'                  THEN d->>'select_one_rock_fill_dam_other'                  ELSE NULLIF(d->>'select_one_rock_fill_dam','')                  END
            WHEN 'stone bunding'                       THEN CASE WHEN lower(d->>'select_one_stone_bunding')='other'                  THEN d->>'select_one_stone_bunding_other'                  ELSE NULLIF(d->>'select_one_stone_bunding','')                  END
            WHEN 'diversion drains'                    THEN CASE WHEN lower(d->>'select_one_diversion_drains')='other'               THEN d->>'select_one_diversion_drains_other'               ELSE NULLIF(d->>'select_one_diversion_drains','')               END
            WHEN 'bunding:contour bunds/ graded bunds' THEN CASE WHEN lower(d->>'select_one_bunding')='other'                        THEN d->>'select_one_bunding_other'                        ELSE NULLIF(d->>'select_one_bunding','')                        END
            WHEN '5% model structure'                  THEN CASE WHEN lower(d->>'select_one_model5_structure')='other'               THEN d->>'select_one_model5_structure_other'               ELSE NULLIF(d->>'select_one_model5_structure','')               END
            WHEN '30-40 model structure'               THEN CASE WHEN lower(d->>'select_one_model30_40_structure')='other'           THEN d->>'select_one_model30_40_structure_other'           ELSE NULLIF(d->>'select_one_model30_40_structure','')           END
            ELSE NULL
        END,
        NULLIF(d->>'select_one_activities', '')
    ),
    m.latitude::text,
    m.longitude::text,
    -- Section F (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.1 (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.2 (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text
FROM odk_gw_maintenance m
CROSS JOIN LATERAL (SELECT m.data_gw_maintenance::jsonb) AS _d(d)
JOIN plan_ctx pc ON pc.plan_id::text = m.plan_id
WHERE m.is_deleted = FALSE

UNION ALL

-- ── E.2  Agri Maintenance ────────────────────────────────────────────────────
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Sections A–D (NULL x62)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text,
    -- Section E
    CASE
        WHEN lower(replace(COALESCE(d->>'demand_type', ''), '_', ' ')) IN ('community','community well','community demand','public','public well','shared among families') THEN 'Community Demand'
        WHEN lower(replace(COALESCE(d->>'demand_type', ''), '_', ' ')) IN ('private','privately owned','individual demand') THEN 'Individual Demand'
        ELSE d->>'demand_type'
    END,
    d->>'beneficiary_settlement',
    d->>'Beneficiary_Name',
    d->>'ben_father',
    COALESCE(d->>'select_one_water_structure', d->>'select_one_irrigation_structure', 'NA'),
    COALESCE(
        CASE lower(COALESCE(d->>'select_one_water_structure', d->>'select_one_irrigation_structure', ''))
            WHEN 'farm pond'      THEN CASE WHEN lower(d->>'select_one_farm_pond')='other'      THEN d->>'select_one_farm_pond_other'      ELSE NULLIF(d->>'select_one_farm_pond','')      END
            WHEN 'community pond' THEN CASE WHEN lower(d->>'select_one_community_pond')='other' THEN d->>'select_one_community_pond_other' ELSE NULLIF(d->>'select_one_community_pond','') END
            WHEN 'well'           THEN CASE WHEN lower(d->>'select_one_well')='other'           THEN d->>'select_one_well_other'           ELSE NULLIF(d->>'select_one_well','')           END
            WHEN 'canal'          THEN CASE WHEN lower(d->>'select_one_canal')='other'          THEN d->>'select_one_canal_other'          ELSE NULLIF(d->>'select_one_canal','')          END
            WHEN 'farm bund'      THEN CASE WHEN lower(d->>'select_one_farm_bund')='other'      THEN d->>'select_one_farm_bund_other'      ELSE NULLIF(d->>'select_one_farm_bund','')      END
            ELSE NULL
        END,
        NULLIF(d->>'select_one_activities', '')
    ),
    m.latitude::text,
    m.longitude::text,
    -- Section F (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.1 (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.2 (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text
FROM odk_agri_maintenance m
CROSS JOIN LATERAL (SELECT m.data_agri_maintenance::jsonb) AS _d(d)
JOIN plan_ctx pc ON pc.plan_id::text = m.plan_id
WHERE m.is_deleted = FALSE

UNION ALL

-- ── E.3  SWB Maintenance ─────────────────────────────────────────────────────
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Sections A–D (NULL x62)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text,
    -- Section E
    CASE
        WHEN lower(replace(COALESCE(d->>'demand_type', ''), '_', ' ')) IN ('community','community well','community demand','public','public well','shared among families') THEN 'Community Demand'
        WHEN lower(replace(COALESCE(d->>'demand_type', ''), '_', ' ')) IN ('private','privately owned','individual demand') THEN 'Individual Demand'
        ELSE d->>'demand_type'
    END,
    d->>'beneficiary_settlement',
    d->>'Beneficiary_Name',
    d->>'ben_father',
    COALESCE(d->>'TYPE_OF_WORK', d->>'select_one_water_structure', 'NA'),
    COALESCE(
        CASE lower(COALESCE(d->>'TYPE_OF_WORK', d->>'select_one_water_structure', ''))
            WHEN 'farm pond'           THEN CASE WHEN lower(d->>'select_one_farm_pond')='other'               THEN d->>'select_one_farm_pond_other'               ELSE NULLIF(d->>'select_one_farm_pond','')               END
            WHEN 'community pond'      THEN CASE WHEN lower(d->>'select_one_community_pond')='other'          THEN d->>'select_one_community_pond_other'          ELSE NULLIF(d->>'select_one_community_pond','')          END
            WHEN 'large water body'    THEN CASE WHEN lower(d->>'select_one_repair_large_water_body')='other' THEN d->>'select_one_repair_large_water_body_other' ELSE NULLIF(d->>'select_one_repair_large_water_body','') END
            WHEN 'canal'               THEN CASE WHEN lower(d->>'select_one_repair_canal')='other'            THEN d->>'select_one_repair_canal_other'            ELSE NULLIF(d->>'select_one_repair_canal','')            END
            WHEN 'check dam'           THEN CASE WHEN lower(d->>'select_one_check_dam')='other'               THEN d->>'select_one_check_dam_other'               ELSE NULLIF(d->>'select_one_check_dam','')               END
            WHEN 'percolation tank'    THEN CASE WHEN lower(d->>'select_one_percolation_tank')='other'        THEN d->>'select_one_percolation_tank_other'        ELSE NULLIF(d->>'select_one_percolation_tank','')        END
            WHEN 'rock fill dam'       THEN CASE WHEN lower(d->>'select_one_rock_fill_dam')='other'           THEN d->>'select_one_rock_fill_dam_other'           ELSE NULLIF(d->>'select_one_rock_fill_dam','')           END
            WHEN 'loose boulder structure' THEN CASE WHEN lower(d->>'select_one_loose_boulder_structure')='other' THEN d->>'select_one_loose_boulder_structure_other' ELSE NULLIF(d->>'select_one_loose_boulder_structure','') END
            WHEN '5% model structure'  THEN CASE WHEN lower(d->>'select_one_model5_structure')='other'        THEN d->>'select_one_model5_structure_other'        ELSE NULLIF(d->>'select_one_model5_structure','')        END
            WHEN '30-40 model structure' THEN CASE WHEN lower(d->>'select_one_Model30_40_structure')='other'  THEN d->>'select_one_Model30_40_structure_other'  ELSE NULLIF(d->>'select_one_Model30_40_structure','')  END
            ELSE NULL
        END,
        NULLIF(d->>'select_one_activities', '')
    ),
    m.latitude::text,
    m.longitude::text,
    -- Section F (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.1 (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.2 (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text
FROM odk_swb_maintenance m
CROSS JOIN LATERAL (SELECT m.data_swb_maintenance::jsonb) AS _d(d)
JOIN plan_ctx pc ON pc.plan_id::text = m.plan_id
WHERE m.is_deleted = FALSE

UNION ALL

-- ── E.4  SWB-RS Maintenance ──────────────────────────────────────────────────
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Sections A–D (NULL x62)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text,
    -- Section E
    CASE
        WHEN lower(replace(COALESCE(d->>'demand_type', ''), '_', ' ')) IN ('community','community well','community demand','public','public well','shared among families') THEN 'Community Demand'
        WHEN lower(replace(COALESCE(d->>'demand_type', ''), '_', ' ')) IN ('private','privately owned','individual demand') THEN 'Individual Demand'
        ELSE d->>'demand_type'
    END,
    d->>'beneficiary_settlement',
    d->>'Beneficiary_Name',
    d->>'ben_father',
    COALESCE(d->>'TYPE_OF_WORK', 'NA'),
    COALESCE(
        CASE lower(COALESCE(d->>'TYPE_OF_WORK', ''))
            WHEN 'farm pond'           THEN CASE WHEN lower(d->>'select_one_farm_pond')='other'               THEN d->>'select_one_farm_pond_other'               ELSE NULLIF(d->>'select_one_farm_pond','')               END
            WHEN 'community pond'      THEN CASE WHEN lower(d->>'select_one_community_pond')='other'          THEN d->>'select_one_community_pond_other'          ELSE NULLIF(d->>'select_one_community_pond','')          END
            WHEN 'large water body'    THEN CASE WHEN lower(d->>'select_one_repair_large_water_body')='other' THEN d->>'select_one_repair_large_water_body_other' ELSE NULLIF(d->>'select_one_repair_large_water_body','') END
            WHEN 'canal'               THEN CASE WHEN lower(d->>'select_one_repair_canal')='other'            THEN d->>'select_one_repair_canal_other'            ELSE NULLIF(d->>'select_one_repair_canal','')            END
            WHEN 'check dam'           THEN CASE WHEN lower(d->>'select_one_check_dam')='other'               THEN d->>'select_one_check_dam_other'               ELSE NULLIF(d->>'select_one_check_dam','')               END
            WHEN 'percolation tank'    THEN CASE WHEN lower(d->>'select_one_percolation_tank')='other'        THEN d->>'select_one_percolation_tank_other'        ELSE NULLIF(d->>'select_one_percolation_tank','')        END
            WHEN 'rock fill dam'       THEN CASE WHEN lower(d->>'select_one_rock_fill_dam')='other'           THEN d->>'select_one_rock_fill_dam_other'           ELSE NULLIF(d->>'select_one_rock_fill_dam','')           END
            WHEN 'loose boulder structure' THEN CASE WHEN lower(d->>'select_one_loose_boulder_structure')='other' THEN d->>'select_one_loose_boulder_structure_other' ELSE NULLIF(d->>'select_one_loose_boulder_structure','') END
            WHEN '5% model structure'  THEN CASE WHEN lower(d->>'select_one_model5_structure')='other'        THEN d->>'select_one_model5_structure_other'        ELSE NULLIF(d->>'select_one_model5_structure','')        END
            WHEN '30-40 model structure' THEN CASE WHEN lower(d->>'select_one_Model30_40_structure')='other'  THEN d->>'select_one_Model30_40_structure_other'  ELSE NULLIF(d->>'select_one_Model30_40_structure','')  END
            ELSE NULL
        END,
        NULLIF(d->>'select_one_activities', '')
    ),
    m.latitude::text,
    m.longitude::text,
    -- Section F (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.1 (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.2 (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text
FROM odk_swb_rs_maintenance m
CROSS JOIN LATERAL (SELECT m.data_swb_rs_maintenance::jsonb) AS _d(d)
JOIN plan_ctx pc ON pc.plan_id::text = m.plan_id
WHERE m.is_deleted = FALSE

UNION ALL

-- ── F.1  Recharge Structure (odk_groundwater) ────────────────────────────────
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Sections A–D (NULL x62)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text,
    -- Section E (NULL x8)
    NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section F
    'Recharge Structure',
    CASE
        WHEN lower(replace(COALESCE(d->>'demand_type', ''), '_', ' ')) IN ('community','community well','community demand','public','public well','shared among families') THEN 'Community Demand'
        WHEN lower(replace(COALESCE(d->>'demand_type', ''), '_', ' ')) IN ('private','privately owned','individual demand') THEN 'Individual Demand'
        ELSE d->>'demand_type'
    END,
    g.work_type,
    g.beneficiary_settlement,
    d->>'Beneficiary_Name',
    d->>'select_gender',
    d->>'ben_father',
    g.latitude::text,
    g.longitude::text,
    -- Section G.1 (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.2 (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text
FROM odk_groundwater g
CROSS JOIN LATERAL (SELECT g.data_groundwater::jsonb) AS _d(d)
JOIN plan_ctx pc ON pc.plan_id::text = g.plan_id
WHERE g.is_deleted = FALSE AND g.status_re != 'rejected'

UNION ALL

-- ── F.2  Irrigation Work (odk_irrigation) ────────────────────────────────────
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Sections A–D (NULL x62)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text,
    -- Section E (NULL x8)
    NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section F
    'Irrigation Work',
    CASE
        WHEN lower(replace(COALESCE(d->>'demand_type_irrigation', ''), '_', ' ')) IN ('community','community well','community demand','public','public well','shared among families') THEN 'Community Demand'
        WHEN lower(replace(COALESCE(d->>'demand_type_irrigation', ''), '_', ' ')) IN ('private','privately owned','individual demand') THEN 'Individual Demand'
        ELSE d->>'demand_type_irrigation'
    END,
    CASE WHEN lower(i.work_type) = 'other' THEN COALESCE(d->>'TYPE_OF_WORK_ID_other', 'Other (unspecified)') ELSE i.work_type END,
    i.beneficiary_settlement,
    d->>'Beneficiary_Name',
    d->>'gender',
    d->>'ben_father',
    i.latitude::text,
    i.longitude::text,
    -- Section G.1 (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.2 (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text
FROM odk_irrigation i
CROSS JOIN LATERAL (SELECT i.data_agri::jsonb) AS _d(d)
JOIN plan_ctx pc ON pc.plan_id::text = i.plan_id
WHERE i.is_deleted = FALSE AND i.status_re != 'rejected'

UNION ALL

-- ── G.1  Livestock ───────────────────────────────────────────────────────────
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Sections A–D (NULL x62)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text,
    -- Section E (NULL x8)
    NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section F (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.1
    'Livestock',
    CASE
        WHEN lower(replace(COALESCE(dl->'Livestock'->>'livestock_demand', ''), '_', ' ')) IN ('community','community well','community demand','public','public well','shared among families') THEN 'Community Demand'
        WHEN lower(replace(COALESCE(dl->'Livestock'->>'livestock_demand', ''), '_', ' ')) IN ('private','privately owned','individual demand') THEN 'Individual Demand'
        ELSE dl->'Livestock'->>'livestock_demand'
    END,
    CASE WHEN lower(COALESCE(dl->'Livestock'->>'demands_promoting_livestock', '')) = 'other'
         THEN COALESCE(dl->'Livestock'->>'demands_promoting_livestock_other', dl->>'select_one_promoting_livestock')
         ELSE COALESCE(NULLIF(dl->'Livestock'->>'demands_promoting_livestock', ''), NULLIF(dl->>'select_one_promoting_livestock', ''))
    END,
    l.beneficiary_settlement,
    COALESCE(NULLIF(dl->>'beneficiary_name', ''), dl->'Livestock'->>'ben_livestock'),
    dl->'Livestock'->>'gender_livestock',
    dl->'Livestock'->>'ben_father_livestock',
    l.latitude::text,
    l.longitude::text,
    -- Section G.2 (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text
FROM odk_livelihood l
CROSS JOIN LATERAL (SELECT l.data_livelihood::jsonb) AS _dl(dl)
JOIN plan_ctx pc ON pc.plan_id::text = l.plan_id
WHERE l.is_deleted = FALSE AND l.status_re != 'rejected'
  AND (
      lower(COALESCE(dl->'Livestock'->>'is_demand_livestock', '')) = 'yes'
      OR lower(COALESCE(dl->>'select_one_demand_promoting_livestock', '')) = 'yes'
  )

UNION ALL

-- ── G.1  Fisheries ───────────────────────────────────────────────────────────
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Sections A–D (NULL x62)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text,
    -- Section E (NULL x8)
    NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section F (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.1
    'Fisheries',
    CASE
        WHEN lower(replace(COALESCE(dl->'fisheries'->>'demand_type_fisheries', ''), '_', ' ')) IN ('community','community well','community demand','public','public well','shared among families') THEN 'Community Demand'
        WHEN lower(replace(COALESCE(dl->'fisheries'->>'demand_type_fisheries', ''), '_', ' ')) IN ('private','privately owned','individual demand') THEN 'Individual Demand'
        ELSE dl->'fisheries'->>'demand_type_fisheries'
    END,
    CASE WHEN lower(COALESCE(dl->'fisheries'->>'select_one_promoting_fisheries', '')) = 'other'
         THEN COALESCE(dl->'fisheries'->>'select_one_promoting_fisheries_other', dl->>'select_one_promoting_fisheries')
         ELSE COALESCE(NULLIF(dl->'fisheries'->>'select_one_promoting_fisheries', ''), NULLIF(dl->>'select_one_promoting_fisheries', ''))
    END,
    l.beneficiary_settlement,
    COALESCE(NULLIF(dl->>'beneficiary_name', ''), dl->'fisheries'->>'ben_fisheries'),
    dl->'fisheries'->>'gender_fisheries',
    dl->'fisheries'->>'ben_father_fisheries',
    l.latitude::text,
    l.longitude::text,
    -- Section G.2 (NULL x10)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text
FROM odk_livelihood l
CROSS JOIN LATERAL (SELECT l.data_livelihood::jsonb) AS _dl(dl)
JOIN plan_ctx pc ON pc.plan_id::text = l.plan_id
WHERE l.is_deleted = FALSE AND l.status_re != 'rejected'
  AND (
      lower(COALESCE(dl->'fisheries'->>'is_demand_fisheris', '')) = 'yes'
      OR lower(COALESCE(dl->>'select_one_demand_promoting_fisheries', '')) = 'yes'
  )

UNION ALL

-- ── G.2  Plantations (from odk_livelihood) ───────────────────────────────────
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Sections A–D (NULL x62)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text,
    -- Section E (NULL x8)
    NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section F (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.1 (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.2
    'Plantations',
    CASE
        WHEN lower(replace(COALESCE(dl->'plantations'->>'demand_type_plantations', ''), '_', ' ')) IN ('community','community well','community demand','public','public well','shared among families') THEN 'Community Demand'
        WHEN lower(replace(COALESCE(dl->'plantations'->>'demand_type_plantations', ''), '_', ' ')) IN ('private','privately owned','individual demand') THEN 'Individual Demand'
        ELSE dl->'plantations'->>'demand_type_plantations'
    END,
    l.beneficiary_settlement,
    COALESCE(NULLIF(dl->>'beneficiary_name', ''), dl->'plantations'->>'ben_plantation'),
    dl->'plantations'->>'gender',
    dl->'plantations'->>'ben_father',
    COALESCE(NULLIF(dl->>'Plantation', ''), dl->'plantations'->>'crop_name'),
    COALESCE(NULLIF(dl->>'Plantation_crop', ''), dl->'plantations'->>'crop_area'),
    l.latitude::text,
    l.longitude::text
FROM odk_livelihood l
CROSS JOIN LATERAL (SELECT l.data_livelihood::jsonb) AS _dl(dl)
JOIN plan_ctx pc ON pc.plan_id::text = l.plan_id
WHERE l.is_deleted = FALSE AND l.status_re != 'rejected'
  AND (
      lower(COALESCE(dl->>'select_one_demand_plantation', '')) = 'yes'
      OR lower(COALESCE(dl->'plantations'->>'select_plantation_demands', '')) = 'yes'
  )

UNION ALL

-- ── G.2  Kitchen Garden (from odk_livelihood) ────────────────────────────────
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Sections A–D (NULL x62)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text,
    -- Section E (NULL x8)
    NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section F (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.1 (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.2
    'Kitchen Garden',
    CASE
        WHEN lower(replace(COALESCE(dl->'kitchen_gardens'->>'demand_type_kitchen_garden', ''), '_', ' ')) IN ('community','community well','community demand','public','public well','shared among families') THEN 'Community Demand'
        WHEN lower(replace(COALESCE(dl->'kitchen_gardens'->>'demand_type_kitchen_garden', ''), '_', ' ')) IN ('private','privately owned','individual demand') THEN 'Individual Demand'
        ELSE dl->'kitchen_gardens'->>'demand_type_kitchen_garden'
    END,
    l.beneficiary_settlement,
    COALESCE(NULLIF(dl->>'beneficiary_name', ''), dl->'kitchen_gardens'->>'ben_kitchen_gardens'),
    dl->'kitchen_gardens'->>'gender_kitchen_gardens',
    dl->'kitchen_gardens'->>'ben_father_kitchen_gardens',
    dl->>'Plantation',
    COALESCE(NULLIF(dl->>'area_didi_badi', ''), dl->'kitchen_gardens'->>'area_kg'),
    l.latitude::text,
    l.longitude::text
FROM odk_livelihood l
CROSS JOIN LATERAL (SELECT l.data_livelihood::jsonb) AS _dl(dl)
JOIN plan_ctx pc ON pc.plan_id::text = l.plan_id
WHERE l.is_deleted = FALSE AND l.status_re != 'rejected'
  AND (
      lower(COALESCE(dl->>'indi_assets', '')) = 'yes'
      OR lower(COALESCE(dl->'kitchen_gardens'->>'assets_kg', '')) = 'yes'
  )

UNION ALL

-- ── G.2  Plantations (from odk_agrohorticulture) ─────────────────────────────
SELECT
    pc.plan_id, pc.plan_name, pc.state_name_soi, pc.district_name_soi,
    pc.tehsil_soi, pc.org_id, pc.org_name, pc.project, pc.village_name, pc.facilitator_name,
    -- Sections A–D (NULL x62)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text,
    -- Section E (NULL x8)
    NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section F (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.1 (NULL x9)
    NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
    NULL::text, NULL::text, NULL::text, NULL::text,
    -- Section G.2
    'Plantations',
    CASE
        WHEN lower(replace(COALESCE(d->>'demand_type_plantations', ''), '_', ' ')) IN ('community','community well','community demand','public','public well','shared among families') THEN 'Community Demand'
        WHEN lower(replace(COALESCE(d->>'demand_type_plantations', ''), '_', ' ')) IN ('private','privately owned','individual demand') THEN 'Individual Demand'
        ELSE d->>'demand_type_plantations'
    END,
    d->>'beneficiary_settlement',
    d->>'beneficiary_name',
    d->>'gender',
    d->>'ben_father',
    NULLIF(TRIM(CONCAT_WS(' ', NULLIF(d->>'select_multiple_species', ''), NULLIF(d->>'select_multiple_species_other', ''))), ''),
    d->>'crop_area',
    a.latitude::text,
    a.longitude::text
FROM odk_agrohorticulture a
CROSS JOIN LATERAL (SELECT a.data_agohorticulture::jsonb) AS _d(d)
JOIN plan_ctx pc ON pc.plan_id::text = a.plan_id
WHERE a.is_deleted = FALSE AND a.status_re != 'rejected'

ORDER BY plan_id

) TO STDOUT WITH (FORMAT CSV, HEADER);
