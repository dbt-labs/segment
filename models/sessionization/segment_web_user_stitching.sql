with source as (

--First limit data set to identify calls against email or user or both

    select
        anonymous_id, email, user_id, timestamp
    from {{ source('lyka_interface_prod', 'identifies') }}
    where (email is not null and anonymous_id is not null) or (user_id is not null and anonymous_id is not null)
    order by timestamp
)

, lead as (

--Next, find the last time an identify call was made against an email

    select
        distinct
        anonymous_id,
        email,
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number
    from source
)

, user as (

--Next, find the last time an identify call was made against a user

    select
        distinct
        anonymous_id,
        user_id,
        row_number() over (partition by anonymous_id order by timestamp desc) as sequence_number
    from source
)

--For the final results, take the user ID if known, and/or email otherwise

select
user.anonymous_id, user.user_id, lead.email --AL: if user_id is null id will be stitched to email
from user
left join lead on user.anonymous_id = lead.anonymous_id and lead.sequence_number = 1
where user.sequence_number = 1