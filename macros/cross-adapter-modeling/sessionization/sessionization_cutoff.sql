{% macro sessionization_cutoff() %}

    select {{
        dbt_utils.safe_cast(
            dbt_utils.dateadd(
                'hour',
                -var('segment_sessionization_trailing_window'),
                'max(session_start_tstamp)'),
            'timestamp') }}
    from {{this}}

{% endmacro %}
