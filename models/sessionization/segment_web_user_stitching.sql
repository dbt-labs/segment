with source as (

    select
        anonymous_id, email, user_id, timestamp
    from {{ source('lyka_interface_prod', 'identifies') }}

    UNION ALL

    select
        anonymous_id, email, user_id, timestamp
    from {{ source('lyka_service_prod', 'identifies') }}

)

, known_email as (

--A user can have many emails (via BaB email capture)
--In this case the email has been matched to a user

    select
        distinct
        email,
        user_id,
        row_number() over (partition by email order by timestamp desc) as sequence_number
    from source
    where email is not null

)

, known_user as (

--A user can have many ids
--In this case the id has been matched to a user

    select
        distinct
        anonymous_id,
        user_id,
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number
    from source
    where anonymous_id is not null
    
)

, unknown_email as (

--An email can have many ids
--In this case the id will be associated with an email until it is matched to a user

    select
        distinct
        anonymous_id,
        email,
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number
    from source
    where anonymous_id is not null

)

--For the final results, take the user ID if known, and/or email otherwise

select
known_user.anonymous_id,
coalesce(known_email.email, unknown_email.email) as email,
coalesce(known_user.user_id, known_email.user_id) as user_id
from known_user
left join unknown_email on known_user.anonymous_id = unknown_email.anonymous_id and unknown_email.sequence_number = 1
left join known_email on unknown_email.email = known_email.email and known_email.sequence_number = 1
where known_user.sequence_number = 1