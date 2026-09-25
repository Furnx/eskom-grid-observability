{{ config(
    materialized='view'
) }}

/*
  stg_eskom_grid_schedule
  ───────────────────────
  Staging model for the multi-area EskomSePush API v3.0 payload.

  Reads the landed copy in stg_eskom__raw_payloads, never the raw files
  themselves. That indirection is deliberate: this model is a view, and dbt
  tests query views, so reading the source here would re-scan the whole
  landing zone on every test as well as every build. See ADR 0006
  (eskom-grid-cloud).

  Responsibilities:
    1. UNNESTs the events array safely - when events=[] the UNNEST yields
       zero rows rather than crashing, preserving pipeline continuity.
    2. Casts timestamps and extracts geography from the _meta block injected
       by the Python extraction worker.
    3. Classifies each event by type (Loadshedding, Load Reduction, etc.)
       and parses the stage number with TRY_CAST so NULL is valid for
       non-staged events (load reduction, faults).

  Timezone note: the API sends offsets (e.g. '...T16:00:00+02:00') and CAST to
  TIMESTAMP drops the offset, so start_time and end_time are SAST wall-clock
  times. run_ts, by contrast, is UTC - it is stamped by the extraction worker.
  Do not compare the two without converting.

  Downstream: fct_grid_events (incremental), fct_pipeline_runs
*/

WITH landed AS (

    SELECT * FROM {{ ref('stg_eskom__raw_payloads') }}

),

flattened_events AS (

    SELECT
        -- ── Provenance ────────────────────────────────────────────────────
        landed.run_ts                 AS run_ts,

        -- ── Geography (from injected _meta block) ──────────────────────────
        landed._meta.area_id          AS area_id,
        landed._meta.area_name        AS area_name,
        landed._meta.municipality     AS municipality,
        landed._meta.province         AS province,

        -- ── Event Timestamps (SAST wall-clock; see note above) ────────────
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

    FROM landed,
    UNNEST(landed.events) AS t(event)

)

SELECT
    run_ts,
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
