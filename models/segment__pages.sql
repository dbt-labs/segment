with pages as (
    select * from {{ var('segment_page_views_table') }}
)

select * from pages
