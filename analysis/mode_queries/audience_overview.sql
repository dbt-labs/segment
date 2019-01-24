{#-
-- When compiled, the following query can be used in Mode to calculate the
-- metrics required for an audience overview similar to the one found in GA.
-- Since the Liquid `form` tag looks similar to a Jinja tag, dbt is erroring
-- when compiling as `form` is an unknown tag in Jinja (even when it is wrapped
-- in a `raw` tag).
-- As a result, when adding to Mode, replace the comments with the correct tags.
-#}

with source as (
    
    select * from {{ref('segment_web_sessions')}}
    
)

, final as (
    
    select
        date_trunc({% raw %}'{{date_part}}'{% endraw %}, session_start_tstamp)::date as period,
        
        count(*) as sessions,
        count(distinct blended_user_id) as distinct_users,
        sum(page_views) as page_views,
        1.0 * sum(page_views) / nullif(count(*), 0) as pages_per_session,
        avg(duration_in_s) as avg_session_duration,
        1.0 * sum(case when page_views = 1 then 1 else 0 end) /
            nullif(count(*), 0) as bounce_rate,
        sum(case when session_number = 1 then 1 else 0 end) as new_sessions,
        sum(case when session_number > 1 then 1 else 0 end) as repeat_sessions

    from source
        
    where session_start_tstamp >= '{% raw %}{{start_date}}{% endraw %}'
      and session_start_tstamp <  '{% raw %}{{end_date}}{% endraw %}'
     
    group by 1
    
)

select * from final

-- A form tag needs to go here

date_part:
    type: select
    default: day
    options: [hour, day, week, month]

start_date:
    type: date
    default: 2018-11-01

end_date:
    type: date
    default: 2018-12-01

-- An endform tag needs to go here
