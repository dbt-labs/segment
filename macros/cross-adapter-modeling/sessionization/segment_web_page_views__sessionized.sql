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
    where user_id in (
        select distinct user_id 
        from {{ref('segment_web_page_views')}} 
        where tstamp >= (select dateadd(hour, -24, max(tstamp)) from {{this}})
        )
        
    or anonymous_id in (
        select distinct anonymous_id 
        from {{ref('segment_web_page_views')}} 
        where tstamp >= (select dateadd(hour, -24, max(tstamp)) from {{this}})
        )
    {% endif %}

),

id_stitching as (

    select * from {{ref('segment_web_user_stitching')}}

),

joined as (

    select 
        
        pageviews.*,
        
        coalesce(id_stitching.user_id, pageviews.anonymous_id) 
            as blended_user_id
    
    from pageviews
    left join id_stitching using (anonymous_id)

),

numbered as (

    --This CTE is responsible for assigning an all-time page view number for a 
    --given user. We need to do this after we have the blended user id that came
    --out of the user stitching process because that way we can number page views
    --across multiple user devices.

    select 
    
        *,
        
        row_number() over (
            partition by anonymous_id 
            order by tstamp
            ) as page_view_number,
        
        row_number() over (
            partition by blended_user_id
            order by tstamp
            ) as user_page_view_number
    
    from joined

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
            rows between unbounded preceding and unbounded following
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
        blended_user_id,
        user_page_view_number,
        {{segment.surrogate_key('anonymous_id', 'session_number')}} as session_id

    from session_numbers

)

select * from session_ids

{% endmacro %}