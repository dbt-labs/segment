with pages as (
    select * from {{ var('segment__schema') }}.pages
)

select * from pages
