{{ config(
    materialized = 'incremental',
    unique_key = 'session_id',
    sort = 'session_start_tstamp',
    partition_by = {'field': 'session_start_tstamp', 'data_type': 'timestamp'},
    dist = 'session_id',
    cluster_by = 'session_id'
    )}}

with sessions as (

    select * from {{ref('segment_web_sessions__initial')}}

    {% if is_incremental() %}

        {% if target.type == 'bigquery'%}
            where session_start_tstamp > (
            select 
                timestamp_sub(
                    max(session_start_tstamp), 
                    interval {{var('segment_sessionization_trailing_window')}} hour
                    )
            from {{ this }} )

        {% else %}
            where session_start_tstamp > (
            select
                {{ dbt_utils.dateadd(
                    'hour',
                    -var('segment_sessionization_trailing_window'),
                    'max(session_start_tstamp)'
                ) }}
            from {{ this }} )

        {% endif %}

    {% endif %}

),

id_stitching as (

    select * from {{ref('segment_web_user_stitching')}}

),

joined as (

    select

        sessions.*,

        coalesce(id_stitching.user_id, sessions.anonymous_id)
            as blended_user_id

    from sessions
    left join id_stitching using (anonymous_id)

)

select * from joined
