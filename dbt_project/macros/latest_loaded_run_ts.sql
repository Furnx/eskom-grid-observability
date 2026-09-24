{% macro latest_loaded_run_ts() %}
    {#-
        Returns the newest run_ts already present in the landing table, as a
        plain string, or none on a first (non-incremental) run.

        Why a macro and not a subquery: DuckDB only skips reading files when the
        filter on the file name is a LITERAL in the compiled SQL. Looking the
        cutoff up here, at compile time, lets the model embed it as
        '20260924_150016' rather than as a correlated subquery — which returns
        identical rows but re-reads every file in the bucket. On S3 that is the
        difference between a handful of GET requests per run and hundreds of
        thousands per month. See ADR 0006 in the eskom-grid-cloud repository.

        `execute` is false during dbt's parse phase, when `this` may not exist
        yet; guarding on it keeps `dbt parse` working on a clean project.
    -#}
    {%- if execute and is_incremental() -%}
        {%- set result = run_query('select max(run_ts) from ' ~ this) -%}
        {%- if result and result.rows | length > 0 -%}
            {{- return(result.columns[0].values()[0]) -}}
        {%- endif -%}
    {%- endif -%}
    {{- return(none) -}}
{% endmacro %}
