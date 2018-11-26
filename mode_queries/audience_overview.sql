select
date_trunc('{{date_part}}', session_start_tstamp)::date as period,
count(*) as sessions,
count(distinct blended_user_id) as distinct_users,
sum(page_views) as page_views,
1.0 * sum(page_views) / nullif(count(*), 0) as pages_per_session,
avg(duration_in_s) as avg_session_duration,
1.0 * sum(case when page_views = 1 then 1 else 0 end) / nullif(count(*), 0) as bounce_rate,
sum(case when session_number = 1 then 1 else 0 end) as new_sessions,
sum(case when session_number > 1 then 1 else 0 end) as repeat_sessions

from dbt_claire.segment_web_sessions
where session_start_tstamp >= '{{start_date}}'
  and session_start_tstamp <  '{{end_date}}'
group by 1
