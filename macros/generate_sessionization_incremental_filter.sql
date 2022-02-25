{% macro generate_sessionization_incremental_filter(merge_target, filter_tstamp, max_tstamp) %}
    {{ return(adapter.dispatch('generate_sessionization_incremental_filter', 'segment') (expression)) }}
{% endmacro %}


{% macro default__generate_sessionization_incremental_filter(merge_target, filter_tstamp, max_tstamp) %}
    where {{ filter_tstamp }} >= (
        select
            {{ dbt_utils.dateadd(
                'hour',
                -var('segment_sessionization_trailing_window'),
                'max(' ~ max_tstamp ~ ')'
            ) }}
        from {{ merge_target }} 
    )
{%- endmacro -%}

{% macro bigquery__generate_sessionization_incremental_filter(merge_target, filter_tstamp, max_tstamp) %}
    where {{ filter_tstamp }} >= (
        select 
            timestamp_sub(
                max({{ max_tstamp }}), 
                interval {{ var('segment_sessionization_trailing_window') }} hour
                )
        from {{ merge_target }} 
    )
{%- endmacro -%}

{% macro postgres__generate_sessionization_incremental_filter(merge_target, filter_tstamp, max_tstamp) %}
    where cast({{ filter_tstamp }} as timestamp) >= (
        select
            {{ dbt_utils.dateadd(
                'hour',
                -var('segment_sessionization_trailing_window'),
                'max(' ~ max_tstamp ~ ')'
            ) }}
        from {{ merge_target }} 
    )
{%- endmacro -%}