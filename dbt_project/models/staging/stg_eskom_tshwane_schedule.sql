{{ config(
    materialized='view'
) }}

WITH raw_payload AS (
    -- Wrap the strict schema enforcement AROUND the dbt source macro
    SELECT * FROM read_json(
        {{ source('eskom_data', 'raw_eskom_tshwane_schedule') }},
        columns = {
            '_meta': 'STRUCT(area_id VARCHAR, area_name VARCHAR)',
            'events': 'STRUCT("start" VARCHAR, "end" VARCHAR, note VARCHAR)[]'
        }
    )
)

flattened_events AS (
    SELECT 
        -- Pulling explicitly from the renamed CTE
        raw_payload._meta.area_id AS area_id,
        raw_payload._meta.area_name AS area_name,
        
        CAST(event."start" AS TIMESTAMP) AS start_time,
        CAST(event."end" AS TIMESTAMP) AS end_time,
        CAST(REGEXP_EXTRACT(event.note, '\d+') AS INTEGER) AS loadshedding_stage,
        event.note AS raw_note
        
    FROM raw_payload,
    UNNEST(events) AS t(event)
)

SELECT 
    area_id,
    area_name,
    start_time,
    end_time,
    loadshedding_stage,
    raw_note,
    CURRENT_TIMESTAMP AS dbt_extracted_at
FROM flattened_events
WHERE start_time IS NOT NULL 
  AND end_time IS NOT NULL