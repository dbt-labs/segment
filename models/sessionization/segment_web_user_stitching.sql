{{config(materialized = 'table')}}

with events as (

    select * from {{ref('segment_web_page_views')}}

),

mapping as (

    select distinct

        anonymous_id,
        
        /* 
            Postgres doesn't have "ignore nulls", so instead we partition over
            anonymous_id and order the user_ids within it such that non-null
            values come up last, sorted secondarily by increasing tstamp.
        */

        last_value(user_id {% if target.type != "postgres" -%} ignore nulls {%- endif -%}) over (
            partition by anonymous_id
            order by
                {% if target.type == "postgres" -%}
                case when user_id is not null then 1 else 0 end asc, 
                {%- endif %}
                tstamp
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
