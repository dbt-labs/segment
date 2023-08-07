
{{config(materialized = 'table')}}

with source as (

    select * from {{ source('lyka_interface_prod', 'identifies') }}
    where email is not null
    or (
        user_id is not null
        and CHAR_LENGTH(user_id) = 5 --AL: last observed error rate of 8 (includes 'Checkout Completed') on 19 Jun 2023
    )
)

, identify as (
    select
        distinct
        email,
        user_id,
    from source
    where user_id is not null

)

, device as (

    select
        distinct
        anonymous_id,
        email,
        user_id,
        timestamp,
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number, --AL: sequence_number = 1 will be the most recent (timestamp) identify call on the user
    from source
)

select
device.anonymous_id, device.email, identify.user_id
from device
left join identify on device.email = identify.email
where device.sequence_number = 1