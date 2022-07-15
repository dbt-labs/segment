with tracks as (
    select * from {{ var('segment__schema') }}.tracks
)

select * from tracks
