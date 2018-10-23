{% macro segment_web_user_stitching() %}

    {{ adapter_macro('segment.segment_web_user_stitching') }}

{% endmacro %}


{% macro default__segment_web_user_stitching() %}

{{config(materialized = 'table')}}

with events as (

    select * from {{ref('segment_web_page_views')}}

),

mapping as (

    select distinct
    
        anonymous_id, 
        
        last_value(user_id ignore nulls) over (
            partition by anonymous_id 
            order by tstamp 
            rows between unbounded preceding and unbounded following
            ) as user_id
        
    from events

)

select * from mapping 

{% endmacro %}