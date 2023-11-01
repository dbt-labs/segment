{{ config(
    materialized = 'incremental',
    unique_key = 'session_id',
    sort = 'session_start_tstamp',
    partition_by = {'field': 'session_start_tstamp', 'data_type': 'timestamp', 'granularity': var('segment_bigquery_partition_granularity')},
    dist = 'session_id',
    cluster_by = 'session_id'
    )}}

{% set partition_by = "partition by source_name, session_id" %}

{% set window_clause = "
    partition by source_name, session_id
    order by page_view_number
    rows between unbounded preceding and unbounded following
    " %}

{% set first_values = {
    'utm_source' : 'utm_source',
    'utm_content' : 'utm_content',
    'utm_medium' : 'utm_medium',
    'utm_campaign' : 'utm_campaign',
    'utm_term' : 'utm_term',
    'gclid' : 'gclid',
    'page_url' : 'first_page_url',
    'page_url_host' : 'first_page_url_host',
    'page_url_path' : 'first_page_url_path',
    'page_url_query' : 'first_page_url_query',
    'referrer' : 'referrer',
    'referrer_host' : 'referrer_host',
    'device' : 'device',
    'device_category' : 'device_category'
    } %}

{% set last_values = {
    'page_url' : 'last_page_url',
    'page_url_host' : 'last_page_url_host',
    'page_url_path' : 'last_page_url_path',
    'page_url_query' : 'last_page_url_query'
    } %}

{% for col in var('segment_pass_through_columns') %}
    {% do first_values.update({col: 'first_' ~ col}) %}
    {% do last_values.update({col: 'last_' ~ col}) %}
{% endfor %}

with pageviews_sessionized as (

    select * from {{ref('segment_web_page_views__sessionized')}}

    {% if is_incremental() %}
    {{
        generate_sessionization_incremental_filter( this, 'tstamp', 'session_start_tstamp', '>' )
    }}
    {% endif %}

),

referrer_mapping as (

    select * from {{ ref('referrer_mapping') }}

),

agg as (

    select distinct

        source_name,
        session_id,
        anonymous_id,
        min(tstamp) over ( {{partition_by}} ) as session_start_tstamp,
        max(tstamp) over ( {{partition_by}} ) as session_end_tstamp,
        count(*) over ( {{partition_by}} ) as page_views,

        {% for (key, value) in first_values.items() %}
        first_value({{key}}) over ({{window_clause}}) as {{value}},
        {% endfor %}

        {% for (key, value) in last_values.items() %}
        last_value({{key}}) over ({{window_clause}}) as {{value}}{% if not loop.last %},{% endif %}
        {% endfor %}

    from pageviews_sessionized

),

diffs as (

    select

        *,

        {{ dbt.datediff('session_start_tstamp', 'session_end_tstamp', 'second') }} as duration_in_s

    from agg

),

tiers as (

    select

        *,

        case
            when duration_in_s between 0 and 9 then '0s to 9s'
            when duration_in_s between 10 and 29 then '10s to 29s'
            when duration_in_s between 30 and 59 then '30s to 59s'
            when duration_in_s > 59 then '60s or more'
            else null
        end as duration_in_s_tier

    from diffs

),

mapped as (

    select
        tiers.*,
        referrer_mapping.medium as referrer_medium,
        referrer_mapping.source as referrer_source

    from tiers

    left join referrer_mapping on tiers.referrer_host = referrer_mapping.host

)

select * from mapped
