with identifies as (
    select * from {{ var('segment__schema') }}.identifies
)

select * from identifies
