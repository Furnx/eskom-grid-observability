{{ config(
    materialized='table'
) }}

/*
  dim_area
  ────────
  Kimball dimension table: unique roster of monitored geographic areas.

  Isolates static descriptive attributes (area identity, geography) from
  transactional outage facts in fct_grid_events, enabling clean star-schema
  joins for BI tools and cross-domain queries (e.g. joining to a corporate
  ERP facility dimension by municipality or province).

  Municipality and province are sourced from the _meta block injected by
  the Python extraction worker, which in turn reads from areas_config.yml.
  This makes the geographic hierarchy fully version-controlled.

  Phase 2 note: In Phase 2 this model will be joined to the areas_config
  dbt seed for additional enrichment (region_type, municipality code etc.).
*/

WITH staged_data AS (
    SELECT * FROM {{ ref('stg_eskom_grid_schedule') }}
),

seed_data AS (
    SELECT * FROM {{ ref('areas_config') }}
),

unique_areas AS (
    -- One row per area — DISTINCT collapses the many event rows per area
    SELECT DISTINCT
        area_id,
        area_name,
        municipality,
        province
    FROM staged_data
    WHERE area_id IS NOT NULL
)

SELECT
    u.area_id,
    u.area_name,
    u.municipality,
    u.province,
    s.region_type,
    s.latitude,
    s.longitude,
    s.climate_zone,
    CURRENT_TIMESTAMP AS dbt_updated_at

FROM unique_areas u
LEFT JOIN seed_data s ON u.area_id = s.area_id