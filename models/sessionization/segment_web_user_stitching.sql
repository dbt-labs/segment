{{ config(
    materialized = 'table'
) }}

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
        ) as user_id,

        min(tstamp) over (
            partition by anonymous_id
        ) as first_seen_at,

        max(tstamp) over (
            partition by anonymous_id
        ) as last_seen_at

    from events

)

select * from mapping 
