{{ config(
    materialized='table'
) }}

WITH raw_json AS (
    -- Directly reading the local JSON file.
    -- The path is relative to where DuckDB is executed.
    SELECT * FROM read_json_auto('../data/raw_tshwane_schedule.json')
),

flattened_events AS (
    -- UNNEST flattens the array of events into individual rows.
    -- We use a CROSS JOIN implicitly by listing the raw table and the unnested array.
    SELECT
        -- Extracting top-level metadata if needed (adjust based on actual JSON keys)
        info.area_name::VARCHAR AS area_name,
        
        -- Flattening the nested events array
        event.start::TIMESTAMP AS start_time,
        event.end::TIMESTAMP AS end_time,
        
        -- Extracting the stage integer from the note string (e.g., "Stage 2" -> 2)
        -- Defensive string manipulation using REGEXP_EXTRACT
        CAST(REGEXP_EXTRACT(event.note::VARCHAR, '\d+') AS INTEGER) AS loadshedding_stage,
        
        event.note::VARCHAR AS raw_note
        
    FROM raw_json,
    UNNEST(events) AS t(event)
)

SELECT
    area_name,
    start_time,
    end_time,
    loadshedding_stage,
    raw_note,
    -- Adding an extraction timestamp for auditability
    CURRENT_TIMESTAMP AS dbt_extracted_at
FROM flattened_events
WHERE start_time IS NOT NULL
  AND end_time IS NOT NULL