with source as (

    select
        anonymous_id, email, user_id, timestamp
    from {{ source('lyka_interface_prod', 'identifies') }}
    where (email is not null and user_id is not null) or (email is not null or anonymous_id is not null)

)

, identify as (

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

--Each email can be associated with only 1 email, however an email can have many ids

    select
        distinct
        anonymous_id,
        email,
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number
    from source
    where email is not null and anonymous_id is not null
)

--For the final results, take the user ID if known, and/or email otherwise

select
identify.user_id, identify.email, device.anonymous_id --AL: if an email is associated with a Lyka User ID then it will be stitched onto the anonymous_id
from identify
left join device on identify.email = device.email and device.sequence_number = 1
where identify.sequence_number = 1

--For validation, ensure each id is only represented once for each email and user (i.e. unique) and that email is only associated with one user (but can have many ids)
