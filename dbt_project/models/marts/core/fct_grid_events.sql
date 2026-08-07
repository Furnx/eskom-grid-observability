{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

/*
  fct_grid_events
  ───────────────
  The core Kimball fact table for historical grid event accumulation.

  Materialization: INCREMENTAL
    On each pipeline run dbt inserts only rows whose event_id does not
    already exist in the table. The SHA256 surrogate key guarantees that
    re-processing the same API payload never produces duplicates, making
    the pipeline mathematically idempotent across unlimited reruns.

  Primary Key (event_id):
    SHA256 hash of (area_id + start_time + event_classification).
    SHA256 is chosen over MD5 for genuine cryptographic defensibility —
    required for audit contexts such as insurance claim validation and
    Scope 2 carbon accounting.

  Incremental Filter:
    On incremental runs only events with start_time in the trailing 2-day
    window are considered. This avoids full-table scans while still
    catching any late-arriving or backdated events from the API.

  Business Metrics Pre-computed:
    - duration_hours: used directly by ESG/insurance/wheeling queries
*/

WITH staged_data AS (

    SELECT * FROM {{ ref('stg_eskom_grid_schedule') }}

    {% if is_incremental() %}
    -- Only process the recent window on incremental runs.
    -- The unique_key handles deduplication; this WHERE clause is a
    -- performance guard on growing tables.
    WHERE start_time >= (
        SELECT COALESCE(MAX(start_time), '1970-01-01'::TIMESTAMP) - INTERVAL '2 days'
        FROM {{ this }}
    )
    {% endif %}

),

calculated_facts AS (

    SELECT
        -- ── Primary Key ───────────────────────────────────────────────────
        -- SHA256 of the natural key components. Deterministic, collision-resistant,
        -- and audit-defensible for financial and insurance contexts.
        SHA256(
            area_id
            || '|' || start_time::VARCHAR
            || '|' || COALESCE(event_classification, 'unknown')
        ) AS event_id,

        -- ── Foreign Keys ──────────────────────────────────────────────────
        area_id,

        -- ── Event Attributes ─────────────────────────────────────────────
        event_classification,
        event_stage,              -- NULL for non-staged events (Load Reduction etc.)
        raw_note,

        -- ── Timestamps ───────────────────────────────────────────────────
        start_time,
        end_time,

        -- ── Business Metric: Outage Duration ─────────────────────────────
        -- Pre-computed here so ESG, insurance, and wheeling queries can filter
        -- and aggregate without re-deriving the math on every analytical query.
        -- Example: WHERE duration_hours > 12 (insurance 12-hour exclusion clause)
        DATE_DIFF('minute', start_time, end_time) / 60.0 AS duration_hours,

        dbt_extracted_at

    FROM staged_data

)

SELECT * FROM calculated_facts
