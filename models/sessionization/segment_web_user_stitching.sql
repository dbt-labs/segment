{{config(materialized = 'table')}}

with events as (

    select * from {{ref('segment_web_page_views')}}

),

last_user_id as (

    select distinct 
        anonymous_id,
        last_value(user_id) over (
            partition by anonymous_id
            order by tstamp
            rows between unbounded preceding and unbounded following
        ) as not_null_user_id
    from events
    where user_id is not null
),

seen_times as (

    select distinct

        anonymous_id,

        min(tstamp) over (
            partition by anonymous_id
        ) as first_seen_at,

        max(tstamp) over (
            partition by anonymous_id
        ) as last_seen_at

    from events

),

mapping as (

    select 
        s.anonymous_id,
        u.not_null_user_id as user_id,
        s.first_seen_at,
        s.last_seen_at
    from seen_times s
    left outer join last_user_id u 
        on s.anonymous_id = u.anonymous_id

)

select * from mapping
