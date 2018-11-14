{% macro segment_web_page_views__sessionized() %}

    {{ adapter_macro('segment.segment_web_page_views__sessionized') }}

{% endmacro %}


{% macro default__segment_web_page_views__sessionized() %}

{{ config(
    materialized = 'incremental',
    sql_where = 'TRUE',
    unique_key = 'page_view_id',
    sort = 'tstamp',
    dist = 'page_view_id'
    )}}

{# 
the initial CTE in this model is unusually complicated; its function is to 
select all pageviews (for all time) for users who have pageviews since the 
model was most recently run. there are many window functions in this model so
in order to appropriately calculate all of them we need each user's entire 
page view history, but we only want to grab that for users who have page view
events we need to calculate.
#}

with pageviews as (

    select * from {{ref('segment_web_page_views')}}
    
    {% if adapter.already_exists(this.schema, this.table) and not flags.FULL_REFRESH %}
    where anonymous_id in (
        select distinct anonymous_id 
        from {{ref('segment_web_page_views')}} 
        where tstamp >= (select dateadd(hour, -{{var('segment_sessionization_trailing_window')}}, max(tstamp)) from {{this}})
        )
    {% endif %}

),

numbered as (

    --This CTE is responsible for assigning an all-time page view number for a 
    --given anonymous_id. We don't need to do this across devices because the 
    --whole point of this field is for sessionization, and sessions can't span
    --multiple devices.

    select 
    
        *,
        
        row_number() over (
            partition by anonymous_id 
            order by tstamp
            ) as page_view_number
    
    from pageviews

),

lagged as (

    --This CTE is responsible for simply grabbing the last value of `tstamp`.
    --We'll use this downstream to do timestamp math--it's how we determine the
    --period of inactivity.

    select 
    
        *,
        
        lag(tstamp) over (
            partition by anonymous_id 
            order by page_view_number
            ) as previous_tstamp
    
    from numbered

),

diffed as (

    --This CTE simply calculates `period_of_inactivity`.

    select
        *,
        datediff(s, previous_tstamp, tstamp) as period_of_inactivity
    from lagged

),

new_sessions as (

    --This CTE calculates a single 1/0 field--if the period of inactivity prior
    --to this page view was greater than 30 minutes, the value is 1, otherwise 
    --it's 0. We'll use this to calculate the user's session #.

    select 
        *,
        case 
            when period_of_inactivity <= 30 * 60 then 0
            else 1
        end as new_session
    from diffed

),

session_numbers as (

    --This CTE calculates a user's session (1, 2, 3) number from `new_session`.
    --This single field is the entire point of the entire prior series of
    --calculations.

    select 
    
        *,
    
        sum(new_session) over (
            partition by anonymous_id 
            order by page_view_number 
            rows between unbounded preceding and current row
            ) as session_number
    
    from new_sessions

),

session_ids as (

    --This CTE assigns a globally unique session id based on the combination of 
    --`anonymous_id` and `session_number`.

    select 
        
        {{segment.star(ref('segment_web_page_views'))}},
        page_view_number,
        {{segment.surrogate_key('anonymous_id', 'session_number')}} as session_id,
        case
            when device_raw = 'iPhone' then 'iPhone'
            when device_raw = 'Android' then 'Android'
            when device_raw in ('iPad', 'iPod') then 'Tablet'
            when device_raw in ('Windows', 'Macintosh', 'X11') then 'Desktop'
            else 'uncategorized'
        end as device

    from session_numbers

)

select * from session_ids

{% endmacro %}