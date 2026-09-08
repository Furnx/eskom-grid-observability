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

  Source: areas_config seed (CSV).
  This model is derived entirely from the seed — NOT from the staging model.
  This is deliberate: staging depends on UNNEST(events), which yields zero
  rows on zero-event days (grid is up). If dim_area derived from staging,
  it would be empty on those days, breaking any query that joins to it.
  The seed always has rows, so the dimension is always populated.
*/

WITH seed_data AS (
    SELECT * FROM {{ ref('areas_config') }}
)

SELECT
    area_id,
    area_name,
    municipality,
    province,
    region_type,
    latitude,
    longitude,
    climate_zone,
    CURRENT_TIMESTAMP AS dbt_updated_at

FROM seed_data
WHERE area_id IS NOT NULL