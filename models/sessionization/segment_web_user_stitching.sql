{{config(materialized = 'table')}}

with source as (

    select * from {{ source('lyka_interface_prod', 'identifies') }}
    where CHAR_LENGTH(user_id) = 5 --AL: last observed error rate of 8 (includes 'Checkout Completed') on 19 Jun 2023
),

renamed as (

    select
        distinct
        anonymous_id,
        user_id,
        timestamp,
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number, --AL: sequence_number = 1 will be the most recent (timestamp) identify call on the user
    from source

), dataset as (

    select
        anonymous_id,
        user_id,
        row_number() over (partition by user_id order by timestamp desc) as device_sequence_number --AL: device_sequence_number = 1 will be the most recent (timestamp) device that had an identify call
    --AL: ast at 29/06/23 still very few instances where multiple annon_id mapped to single user_id.
    from renamed
    where 
        sequence_number = 1
)

select *
from
    dataset
where
    device_sequence_number = 1 
/*
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
*/