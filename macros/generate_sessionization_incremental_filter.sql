{% macro generate_sessionization_incremental_filter(merge_target, filter_tstamp, max_tstamp, operator) %}
    {{ return(adapter.dispatch('generate_sessionization_incremental_filter', 'segment') (merge_target, filter_tstamp, max_tstamp, operator)) }}
{% endmacro %}


{% macro default__generate_sessionization_incremental_filter(merge_target, filter_tstamp, max_tstamp, operator) %}
    where {{ filter_tstamp }} {{ operator }} (
        select
            {{ dbt_utils.dateadd(
                'hour',
                -var('segment_sessionization_trailing_window'),
                'max(' ~ max_tstamp ~ ')'
            ) }}
        from {{ merge_target }} 
    )
{%- endmacro -%}

{% macro bigquery__generate_sessionization_incremental_filter(merge_target, filter_tstamp, max_tstamp, operator) %}
    where {{ filter_tstamp }} {{ operator }} (
        select 
            timestamp_sub(
                max({{ max_tstamp }}), 
                interval {{ var('segment_sessionization_trailing_window') }} hour
                )
        from {{ merge_target }} 
    )
{%- endmacro -%}

{% macro postgres__generate_sessionization_incremental_filter(merge_target, filter_tstamp, max_tstamp, operator) %}
    where cast({{ filter_tstamp }} as timestamp) {{ operator }} (
        select
            {{ dbt_utils.dateadd(
                'hour',
                -var('segment_sessionization_trailing_window'),
                'max(' ~ max_tstamp ~ ')'
            ) }}
        from {{ merge_target }} 
    )
{%- endmacro -%}