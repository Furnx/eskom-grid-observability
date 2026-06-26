{{ config(
    materialized='table'
) }}

WITH staged_data AS (
    SELECT * FROM {{ ref('stg_eskom_tshwane_schedule') }}
),

calculated_facts AS (
    SELECT 
        -- generate a unique hash for each event to act as a primary key
        MD5(area_id || start_time::VARCHAR || loadshedding_stage::VARCHAR) AS event_id,
        
        area_id,
        start_time,
        end_time,
        loadshedding_stage,
        
        -- Business Metric: Calculate the duration of the outage in hours
        -- DuckDB's DATE_DIFF function handles the timestamp math natively
        DATE_DIFF('minute', start_time, end_time) / 60.0 AS duration_hours
        
    FROM staged_data
)

SELECT * FROM calculated_facts