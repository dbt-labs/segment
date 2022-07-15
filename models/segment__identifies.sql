with identifies as (
    select * from {{ var('segment_identifies_table') }}
)

select * from identifies
