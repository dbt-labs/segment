with source as (

    select
        anonymous_id, email, user_id, timestamp
    from {{ source('lyka_interface_prod', 'identifies') }}

)

, identified as (

--Each email can be associated with only 1 user, however a user can have many emails (via BaB email capture)

    select
        distinct
        email,
        user_id,
        row_number() over (partition by email order by timestamp desc) as sequence_number
    from source
    where email is not null and user_id is not null

)

, device as (

--Each id can be associated with only 1 email, however an email can have many ids

    select
        distinct
        anonymous_id,
        email,
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number
    from source
    where anonymous_id is not null and email is not null 

)

, other as (

--Each id can be associated with only 1 user, however a user can have many ids

    select
        distinct
        anonymous_id,
        user_id,
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number
    from source
    where anonymous_id is not null and user_id is not null
    
)

--For the final results, take the user ID if known, and/or email otherwise

select
device.anonymous_id,
coalesce(other.user_id, identified.user_id) as user_id,
coalesce(identified.email, device.email) as email
from device
left join other on device.anonymous_id = other.anonymous_id and other.sequence_number = 1
left join identified on identified.email = device.email and identified.sequence_number = 1
where device.sequence_number = 1

--For validation, ensure each id is only represented once for each email and user (i.e. unique) and that email is only associated with one user (but can have many ids)