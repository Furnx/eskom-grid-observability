{{ config(
    materialized='table'
) }}

WITH raw_json AS (
    -- read_json with explicit columns bypasses automatic inference.
    -- This forces the 'events' column to exist as an array of structs,
    -- even when the python script injects a completely empty array [].
    SELECT * FROM read_json(
        '../data/raw_tshwane_schedule.json',
        columns = {
            'events': 'STRUCT("start" VARCHAR, "end" VARCHAR, note VARCHAR)[]'
        }
    )
),

flattened_events AS (
    SELECT 
        -- Defensive casting of strings into native Timestamps
        CAST(event."start" AS TIMESTAMP) AS start_time,
        CAST(event."end" AS TIMESTAMP) AS end_time,
        
        -- Extracting the integer stage from the note
        CAST(REGEXP_EXTRACT(event.note, '\d+') AS INTEGER) AS loadshedding_stage,
        
        event.note AS raw_note
        
    FROM raw_json,
    UNNEST(events) AS t(event)
)

SELECT
    start_time,
    end_time,
    loadshedding_stage,
    raw_note,
    CURRENT_TIMESTAMP AS dbt_extracted_at
FROM flattened_events
WHERE start_time IS NOT NULL
  AND end_time IS NOT NULL