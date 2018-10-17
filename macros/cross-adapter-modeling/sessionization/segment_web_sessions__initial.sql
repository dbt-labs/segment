{% macro segment_web_sessions__initial() %}

    {{ adapter_macro('segment.segment_web_sessions__initial') }}

{% endmacro %}


{% macro default__segment_web_sessions__initial() %}

{{ config(
    materialized = 'incremental',
    sql_where = 'TRUE',
    unique_key = 'session_id'
    )}}

{% set partition_by = "partition by session_id" %}

{% set window_clause = "
    partition by session_id 
    order by user_page_view_number 
    rows between unbounded preceding and unbounded following
    " %}
    
{% set first_values = {
    'utm_source' : 'utm_source',
    'utm_content' : 'utm_content',
    'utm_medium' : 'utm_medium',
    'utm_campaign' : 'utm_campaign',
    'utm_term' : 'utm_term',
    'mkw_id' : 'mkw_id',
    'gclid' : 'gclid',
    'page_url' : 'first_page_url',
    'page_url_host' : 'first_page_url_host',
    'page_url_path' : 'first_page_url_path',
    'page_url_query' : 'first_page_url_query',
    'referrer' : 'referrer',
    'referrer_host' : 'referrer_host'
    } %}
    
{% set last_values = {
    'page_url' : 'last_page_url',
    'page_url_host' : 'last_page_url_host',
    'page_url_path' : 'last_page_url_path',
    'page_url_query' : 'last_page_url_query'
    } %}

with pageviews as (

    select * from {{ref('segment_web_page_views__sessionized')}}

    {% if adapter.already_exists(this.schema, this.table) and not flags.FULL_REFRESH %}
        where tstamp > (select dateadd(hour, -3, max(session_start_tstamp)) from {{ this }})
    {% endif %}

),

agg as (

    select distinct

        session_id,
        anonymous_id,
        blended_user_id,
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
        
        datediff(s, session_start_tstamp, session_end_tstamp) as duration_in_s
    
    from agg

)

select * from diffs

{% endmacro %}