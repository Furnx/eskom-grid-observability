{{ config(
    materialized='incremental',
    unique_key='run_id'
) }}

WITH source_data AS (
    SELECT * FROM pipeline_run_log
    {% if is_incremental() %}
        WHERE run_timestamp > (SELECT MAX(run_timestamp) FROM {{ this }})
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['run_timestamp']) }} AS run_id,
    run_timestamp,
    areas_processed,
    total_events_found,
    zero_event_areas,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM source_data
