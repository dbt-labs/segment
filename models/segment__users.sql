with users as (
    select * from {{ var('segment__schema') }}.users
)

select * from users
