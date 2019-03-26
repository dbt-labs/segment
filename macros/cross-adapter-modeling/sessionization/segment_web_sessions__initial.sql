{% macro segment_web_sessions__initial() %}

    {{ adapter_macro('segment.segment_web_sessions__initial') }}

{% endmacro %}


{% macro default__segment_web_sessions__initial() %}

{{ config(
    materialized = 'incremental',
    unique_key = 'session_id',
    sort = 'session_start_tstamp',
    dist = 'session_id'
    )}}

{% set partition_by = "partition by session_id" %}

{% set window_clause = "
    partition by session_id
    order by page_view_number
    rows between unbounded preceding and unbounded following
    " %}

{% set first_values = {
    'referrer_source': 'referrer_source',
    'referrer_content': 'referrer_content',
    'referrer_medium': 'referrer_medium',
    'referrer_campaign': 'referrer_campaign',
    'referrer_term': 'referrer_term',
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

with pageviews as (

    select * from {{ref('segment_web_page_views__sessionized')}}

    {% if is_incremental() %}
        where tstamp > (
          select {{
            dbt_utils.safe_cast(
              dbt_utils.dateadd(
                'hour',
                -var('segment_sessionization_trailing_window'),
                'max(session_start_tstamp)'),
              'timestamp') }}
          from {{ this }})
    {% endif %}

),

agg as (

    select distinct

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

    from pageviews

),

diffs as (

    select

        *,

        {{ dbt_utils.datediff('session_start_tstamp', 'session_end_tstamp', 'second') }} as duration_in_s

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

)

select * from tiers

{% endmacro %}
