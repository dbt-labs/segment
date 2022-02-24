{% macro generate_sessionization_incremental_filter(merge_target, filter_tstamp, max_tstamp) %}

    {% if target.type == 'bigquery' %}
        where {{ filter_tstamp }} >= (
        select 
            timestamp_sub(
                max({{ max_tstamp }}), 
                interval {{ var('segment_sessionization_trailing_window') }} hour
                )
        from {{ merge_target }} 
        )

    {% elif target.type == 'postgres' %}
        where cast({{ filter_tstamp }} as timestamp) >= (
        select
            {{ dbt_utils.dateadd(
                'hour',
                -var('segment_sessionization_trailing_window'),
                'max(' ~ max_tstamp ~ ')'
            ) }}
        from {{ merge_target }} 
        )

    {% else %}
        where {{ filter_tstamp }} >= (
        select
            {{ dbt_utils.dateadd(
                'hour',
                -var('segment_sessionization_trailing_window'),
                'max(' ~ max_tstamp ~ ')'
            ) }}
        from {{ merge_target }} 
        )

    {% endif %}

{% endmacro %}