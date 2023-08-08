with source as (

    select * from lyka_interface_prod.identifies
    where (email is not null or user_id is not null)
        and user_id != 'Checkout Completed' --AL: last observed error rate of 8 (includes 'Checkout Completed') on 19 Jun 2023
)

renamed as (

    select
        distinct
        anonymous_id,
        email,
        user_id,
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number, --AL: sequence_number = 1 will be the most recent (timestamp) identify call against the email
    from source
)

select
device.anonymous_id, device.email, identify.user_id,
from device
left join identify on device.email = identify.email and identify.sequence_number = 1
where device.sequence_number = 1