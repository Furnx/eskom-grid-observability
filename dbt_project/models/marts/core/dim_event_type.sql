{{ config(
    materialized='table'
) }}

/*
  dim_event_type
  ──────────────
  Kimball dimension table: taxonomy of grid events mapped to operational impact.

  Isolates static descriptive attributes (severity weight, generator dependency)
  from the transactional outage facts in fct_grid_events.
*/

WITH seed_data AS (
    SELECT * FROM {{ ref('event_classifications') }}
)

SELECT
    event_classification,
    severity_weight,
    requires_generator,
    impacts_water_pressure,
    insurance_exclusion_eligible,
    CURRENT_TIMESTAMP AS dbt_updated_at

FROM seed_data
