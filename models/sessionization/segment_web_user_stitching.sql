with source as (

    select
        anonymous_id, email, user_id, timestamp
    from {{ source('lyka_interface_prod', 'identifies') }}

    UNION ALL

    select
        anonymous_id, email, user_id, timestamp
    from {{ source('lyka_service_prod', 'identifies') }}

)

, anonymous_id as (
    select
        distinct
        anonymous_id,

        min(timestamp) over (
            partition by anonymous_id
        ) as first_seen_at,

        max(timestamp) over (
            partition by anonymous_id
        ) as last_seen_at

    from source
    where anonymous_id is not null

)

, known_email as (

--In this case the email has been matched to a user account

    select
        distinct
        anonymous_id,
        email,
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number
    from source
    where anonymous_id is not null and email is not null and user_id is not null

)

, known_user as (

--In this case the id has been matched to a user account

    select
        distinct
        anonymous_id,
        user_id,
        cast(timestamp as datetime) as identified_datetime,
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number
    from source
    where anonymous_id is not null and user_id is not null
    
)

, unknown_email as (

--In this case the id will be associated with an email until it is matched to a user account

    select
        distinct
        anonymous_id,
        email,
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number
    from source
    where anonymous_id is not null and email is not null and user_id is null

)

--For the final output, take the user ID if known, and/or email if known else unknown

select
anonymous_id.anonymous_id,
coalesce(known_email.email, unknown_email.email) as email,
known_user.user_id,
cast(anonymous_id.first_seen_at as datetime) as first_seen_at,
cast(anonymous_id.last_seen_at as datetime) as last_seen_at,
known_user.identified_datetime,
case
    when known_user.identified_datetime is not null
    then row_number() over (partition by known_user.user_id order by identified_datetime asc)
    else null
end as user_identified_rank
from anonymous_id
left join unknown_email on anonymous_id.anonymous_id = unknown_email.anonymous_id and unknown_email.sequence_number = 1
left join known_user on anonymous_id.anonymous_id = known_user.anonymous_id and known_user.sequence_number = 1
left join known_email on anonymous_id.anonymous_id = known_email.anonymous_id and known_email.sequence_number = 1