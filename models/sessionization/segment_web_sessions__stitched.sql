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

)

select
*,
row_number() over (partition by user_id order by timestamp desc) as device_sequence_number --AL: device_sequence_number = 1 will be the most recent (timestamp) device that had an identify call
--AL: ast at 29/06/23 still very few instances where multiple annon_id mapped to single user_id.
from renamed
where sequence_number = 1
