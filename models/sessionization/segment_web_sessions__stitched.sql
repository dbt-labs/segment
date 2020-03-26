{% macro segment_web_sessions__stitched() %}

    {{ adapter_macro('segment.segment_web_sessions__stitched') }}

{% endmacro %}


{% macro default__segment_web_sessions__stitched() %}

{{ config(
    materialized = 'incremental',
    unique_key = 'session_id',
    sort = 'session_start_tstamp',
    dist = 'session_id'
    )}}

with sessions as (

    select * from {{ref('segment_web_sessions__initial')}}

    {% if is_incremental() %}
        where cast(session_start_tstamp as datetime) > (
          select
            {{ dbt_utils.dateadd(
                'hour',
                -var('segment_sessionization_trailing_window'),
                'max(session_start_tstamp)'
            ) }}
          from {{ this }})
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

{% endmacro %}
