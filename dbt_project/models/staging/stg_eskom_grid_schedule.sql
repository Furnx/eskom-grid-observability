{{ config(
    materialized='view'
) }}

/*
  stg_eskom_grid_schedule
  ───────────────────────
  Staging model for the multi-area EskomSePush API v3.0 payload.

  Responsibilities:
    1. Reads all area JSON files from data/raw/ via DuckDB's glob union.
    2. Enforces a strict STRUCT schema so DuckDB never auto-infers from
       a potentially empty or malformed payload.
    3. UNNESTs the events array safely — when events=[] the UNNEST yields
       zero rows rather than crashing, preserving pipeline continuity.
    4. Extracts municipality and province from the _meta block injected
       by the Python extraction worker.
    5. Classifies each event by type (Loadshedding, Load Reduction, etc.)
       and parses the stage number with TRY_CAST so NULL is valid for
       non-staged events (load reduction, faults).

  Downstream: fct_grid_events (incremental), dim_area
*/

WITH raw_payload AS (

    SELECT * FROM read_json(
        {{ source('eskom_data', 'raw_eskom_grid_schedules') }},
        columns = {
            '_meta': 'STRUCT(
                area_id      VARCHAR,
                area_name    VARCHAR,
                municipality VARCHAR,
                province     VARCHAR
            )',
            'events': 'STRUCT(
                "start" VARCHAR,
                "end"   VARCHAR,
                note    VARCHAR
            )[]'
        },
        -- union_by_name ensures DuckDB aligns columns when reading multiple files
        union_by_name = true
    )

),

flattened_events AS (

    SELECT
        -- ── Geography (from injected _meta block) ──────────────────────────
        raw_payload._meta.area_id      AS area_id,
        raw_payload._meta.area_name    AS area_name,
        raw_payload._meta.municipality AS municipality,
        raw_payload._meta.province     AS province,

        -- ── Event Timestamps ──────────────────────────────────────────────
        CAST(event."start" AS TIMESTAMP) AS start_time,
        CAST(event."end"   AS TIMESTAMP) AS end_time,

        -- ── Event Classification ──────────────────────────────────────────
        -- Pattern-matched from the raw note string. NULL-safe; new patterns
        -- fall through to 'Unclassified' rather than erroring.
        CASE
            WHEN event.note ILIKE '%stage%'            THEN 'Loadshedding'
            WHEN event.note ILIKE '%load reduction%'   THEN 'Load Reduction'
            WHEN event.note ILIKE '% LR %'             THEN 'Load Reduction'
            WHEN event.note ILIKE '%load limit%'       THEN 'Load Limiting'
            WHEN event.note ILIKE '%water%'            THEN 'Water Shedding'
            ELSE                                            'Unclassified'
        END AS event_classification,

        -- ── Stage Number ──────────────────────────────────────────────────
        -- TRY_CAST returns NULL instead of throwing on non-integer notes
        -- (e.g. "Load Reduction" notes contain no stage digit).
        TRY_CAST(
            REGEXP_EXTRACT(event.note, '\d+') AS INTEGER
        ) AS event_stage,

        -- ── Raw Fields ────────────────────────────────────────────────────
        event.note AS raw_note,

        CURRENT_TIMESTAMP AS dbt_extracted_at

    FROM raw_payload,
    UNNEST(events) AS t(event)

)

SELECT
    area_id,
    area_name,
    municipality,
    province,
    start_time,
    end_time,
    event_classification,
    event_stage,
    raw_note,
    dbt_extracted_at

FROM flattened_events

-- Filter out any rows where timestamps failed to cast (genuine data corruption)
WHERE start_time IS NOT NULL
  AND end_time   IS NOT NULL
