with users as (
    select * from {{ var('segment_users_table') }}
)

select * from users
