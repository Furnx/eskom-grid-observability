{{ config(
    materialized='table'
) }}

WITH staged_data AS (
    SELECT * FROM {{ ref('stg_eskom_tshwane_schedule') }}
),

unique_areas AS (
    -- DISTINCT ensures we only get one row per area
    SELECT DISTINCT 
        area_id,
        area_name
    FROM staged_data
    WHERE area_id IS NOT NULL
)

SELECT 
    area_id,
    area_name,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM unique_areas