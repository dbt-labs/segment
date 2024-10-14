--The first CTE combines data from lyka_service_prod (front-end)
--and lyka_interface_prod (back-end) for an exhaustive dataset
--Only required fields are included

with source as (

    select
        anonymous_id, email, user_id, timestamp, received_at
    from {{ source('lyka_interface_prod', 'identifies') }}

    UNION ALL

    select
        anonymous_id, email, user_id, timestamp, received_at
    from {{ source('lyka_service_prod', 'identifies') }}

)

--The next CTE performs some simple aggregations

, anonymous_id as (
    select
        distinct
        anonymous_id,

        min(timestamp) over (
            partition by anonymous_id
        ) as first_seen_at,

        max(timestamp) over (
            partition by anonymous_id
        ) as last_seen_at,

        max(received_at) over (
            partition by anonymous_id
        ) as received_at

    from source
    where anonymous_id is not null

)

--The next CTE seeks to find the last known identify call between an email and a Lyka user ID.
--In this case the email has been matched to a user account

, known_email as (

    select
        distinct
        anonymous_id,
        email,
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number
    from source
    where anonymous_id is not null and email is not null and user_id is not null

)

--The next CTE seeks to find the last known identify call between an anonymous ID and a Lyka user ID
--In this case the id has been matched to a user account
--It also includes the datetime the anonymous ID was first identified against the the anonymous ID.

, known_user as (

    select
        distinct
        anonymous_id,
        user_id,
        cast(min(timestamp) over (partition by anonymous_id) as datetime) as first_identified_datetime,
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number
    from source
    where anonymous_id is not null and user_id is not null

)

--The next CTE seeks to find the last known identify call between an anonymous ID and an email (where Lyka user ID is unknown)
--In this case the id will can be associated with an email until it is matched to a user account

, unknown_email as (

    select
        distinct
        anonymous_id,
        email,
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number
    from source
    where anonymous_id is not null and email is not null and user_id is null

)

--The final select statement includes a unique anonymous ID as the grain
--It will also include the Lyka user ID and/or email if known, otherwise these fields will be null
--The stitched or blended ID, will return the Lyka user ID if known, else email, else anonymous ID
--The user identified rank is helpful where user has one or more anonymous ID associated in order to ascertain the device that was used to complete build-a-box

select
anonymous_id.anonymous_id,
coalesce(known_email.email, unknown_email.email) as email,
known_user.user_id,
cast(anonymous_id.first_seen_at as datetime) as first_seen_at,
cast(anonymous_id.last_seen_at as datetime) as last_seen_at,
known_user.first_identified_datetime,
case
    when known_user.first_identified_datetime is not null
    then row_number() over (partition by known_user.user_id order by first_identified_datetime asc)
    else null
end as user_identified_rank,
received_at
from anonymous_id
left join unknown_email on anonymous_id.anonymous_id = unknown_email.anonymous_id and unknown_email.sequence_number = 1
left join known_user on anonymous_id.anonymous_id = known_user.anonymous_id and known_user.sequence_number = 1
left join known_email on anonymous_id.anonymous_id = known_email.anonymous_id and known_email.sequence_number = 1
