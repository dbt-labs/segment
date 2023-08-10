with source as (

    select * from {{ source('lyka_interface_prod', 'identifies') }}
    where (email is not null or user_id is not null)
)

, identify as (
    select
        distinct
        email,
        user_id,
        row_number() over (partition by email order by timestamp desc) as sequence_number, --AL: sequence_number = 1 will be the most recent (timestamp) identify call against the user
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
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number, --AL: sequence_number = 1 will be the most recent (timestamp) identify call against the email
    from source
)

select
device.anonymous_id, device.email, identify.user_id,
case
    when device.user_id is not null
    then row_number() over (partition by device.user_id, anonymous_id order by device.timestamp asc)
    else null
end as user_id_sequence_asc
    --AL: this is to ensure we are using the first identify call against a Lyka UID to tie to an anonymous ID given checkout complete event does not capture anonymouse ID
from device
left join identify on device.email = identify.email and identify.sequence_number = 1
where device.sequence_number = 1