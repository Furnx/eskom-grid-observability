{{ config(
    materialized='incremental',
    unique_key='source_file'
) }}

/*
  stg_eskom__raw_payloads
  ───────────────────────
  The landing layer: one row per raw JSON file, stored verbatim.

  This is the ONLY model that reads the raw landing zone. Everything downstream
  — stg_eskom_grid_schedule, the fact tables, the dimensions and every dbt test —
  reads this table from inside DuckDB instead. That layering exists for cost:
  in the cloud the landing zone is S3, and `dbt build` runs models *and* tests,
  so a staging view that read S3 directly would be rescanned by each test.

  Incremental, keyed on the file path, so a file already landed is never read
  again. The cutoff below is what makes that true at the storage layer rather
  than merely in the results.

  Downstream: stg_eskom_grid_schedule, fct_pipeline_runs
*/

{%- set since = latest_loaded_run_ts() %}

SELECT
    -- Natural key: the object path. Unique per area per run.
    filename                                             AS source_file,

    -- The run timestamp, parsed out of the file name (UTC, set by the
    -- extraction worker; every area in one run shares it).
    regexp_extract(filename, '(\d{8}_\d{6})\.json$', 1)  AS run_ts,

    -- Kept as the API + extraction worker produced them. Unpacking happens
    -- downstream, so this table stays a faithful copy of what landed.
    _meta,
    events,

    CURRENT_TIMESTAMP                                    AS loaded_at

FROM read_json(
    {{ source('eskom_data', 'raw_eskom_grid_schedules') }},

    -- Adds the source path as a `filename` column; the filter below depends on it.
    filename = true,

    -- Explicit schema: DuckDB cannot infer the shape of `events` when every
    -- file in the scan has an empty array (no loadshedding scheduled), and an
    -- inferred empty column breaks UNNEST downstream.
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

    union_by_name = true
)

-- Ignore files whose name carries no run timestamp: the two flat files left in
-- the landing zone by the pre-2026-09 layout, which had no per-run history.
WHERE regexp_matches(filename, '\d{8}_\d{6}\.json$')

{% if since %}
  -- ── DO NOT MOVE OR REWRITE THIS FILTER ──────────────────────────────────
  -- It must stay a LITERAL, and it must stay HERE, directly on the file read.
  -- Expressed as a subquery, or applied above the UNNEST in a downstream view,
  -- it still returns exactly the right rows — while making DuckDB open every
  -- file in the bucket again. Correct results, hundreds of thousands of
  -- needless S3 GETs per month. See ADR 0006 (eskom-grid-cloud) and the
  -- incrementality test in tests/test_transform.py.
  --
  -- `>=` rather than `>`: the areas of one run share a run_ts, and a run that
  -- landed only some of them (an API failure partway through) would otherwise
  -- strand the rest permanently. Re-reading the newest run each time costs a
  -- couple of GETs; missing an area costs the record of it.
  AND regexp_extract(filename, '(\d{8}_\d{6})\.json$', 1) >= '{{ since }}'
{% endif %}
