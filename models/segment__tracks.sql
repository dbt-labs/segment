with tracks as (
    select * from {{ var('segment_tracks_table') }}
)

select * from tracks
