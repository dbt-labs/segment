with source as (
    select
        anonymous_id, email, user_id, timestamp
    from {{ source('lyka_interface_prod', 'identifies') }}
    where (email is not null and anonymous_id is not null) or (user_id is not null and anonymous_id is not null)
    order by timestamp
)

, identify as (
    select
        distinct
        email,
        user_id,
        row_number() over (partition by email order by timestamp desc) as sequence_number, --AL: sequence_number = 1 will be where the last time a unique email has been associated with a Lyka User ID
    from source
    where user_id is not null
)
, device as (
    select
        distinct
        anonymous_id,
        email,
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number, --AL: sequence_number = 1 will be where the last time a unique anonymous ID has been associated with an email
    from source
)
select
device.anonymous_id, device.email, identify.user_id --AL: if an email is associated with a Lyka User ID then it will be stitched onto the anonymous_id
from device
left join identify on device.email = identify.email and identify.sequence_number = 1
where device.sequence_number = 1