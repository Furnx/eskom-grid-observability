{{ config(
    materialized='table'
) }}

/*
  fct_pipeline_runs
  ─────────────────
  Observability fact table: one row per extraction run.

  Derived entirely from the landed raw files. Every column here is recoverable
  from what actually arrived in the landing zone - the run timestamp from the
  file name, the areas and event counts from the payloads - so the same numbers
  come out whether the pipeline ran on a laptop or in Lambda. The previous
  version read a DuckDB table written by a Dagster asset that scanned the local
  disk, which no cloud run could produce.

  Because it counts files rather than intentions, a run that landed only some of
  its areas is reported honestly: areas_processed reflects what arrived, not what
  was attempted.

  Materialised as a table and rebuilt each run: the landing table it aggregates
  is complete, and the result is one small row per hour.

  Primary use: proving grid uptime. "No events" and "the pipeline did not run"
  look identical in fct_grid_events and are distinguished here.
*/

WITH landed AS (

    SELECT * FROM {{ ref('stg_eskom__raw_payloads') }}

),

runs AS (

    SELECT
        run_ts,

        -- run_ts is UTC, stamped by the extraction worker. Note this differs
        -- from start_time/end_time in the event models, which are SAST
        -- wall-clock as sent by the API.
        STRPTIME(run_ts, '%Y%m%d_%H%M%S')       AS run_timestamp,

        COUNT(*)                                AS areas_processed,
        SUM(LEN(events))                        AS total_events_found,

        -- Areas that reported no events at all: the grid was up for them.
        STRING_AGG(
            CASE WHEN LEN(events) = 0 THEN _meta.area_name END,
            ', ' ORDER BY _meta.area_name
        )                                       AS zero_event_areas

    FROM landed
    GROUP BY run_ts

)

SELECT
    {{ dbt_utils.generate_surrogate_key(['run_ts']) }} AS run_id,
    run_timestamp,
    areas_processed,
    total_events_found,
    COALESCE(zero_event_areas, '')                     AS zero_event_areas,
    CURRENT_TIMESTAMP                                  AS dbt_updated_at

FROM runs
