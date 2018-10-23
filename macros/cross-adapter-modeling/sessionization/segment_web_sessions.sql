{% macro segment_web_sessions() %}

    {{ adapter_macro('segment.segment_web_sessions') }}

{% endmacro %}


{% macro default__segment_web_sessions() %}

{{ config(
    materialized = 'incremental',
    sql_where = 'TRUE',
    unique_key = 'session_id',
    sort = 'session_start_tstamp',
    dist = 'session_id'
    )}}
    
{% set incremental = adapter.already_exists(this.schema, this.table) and not flags.FULL_REFRESH %}

{# 
Window functions are challenging to make incremental. This approach grabs 
existing values from the existing table and then adds the value of session_number
on top of that seed. During development, this decreased the model runtime 
by 25x on 2 years of data (from 600 to 25 seconds), so even though the code is 
more complicated, the performance tradeoff is worth it.
#}

with sessions as (

    select * from {{ref('segment_web_sessions__stitched')}}
    
    {% if incremental %}
    where session_start_tstamp > (
        select 
            dateadd(
                hour, 
                -{{var('segment_sessionization_trailing_window')}}, 
                max(session_start_tstamp)
                ) 
        from {{this}})
    {% endif %}

),

{% if incremental %}

agg as (

    select 
        blended_user_id, 
        count(*) as starting_session_number
    from {{this}}
    group by 1

),

{% endif %}

windowed as (

    select 

        *,

        row_number() over (
            partition by blended_user_id 
            order by sessions.session_start_tstamp
            ) 
            {% if incremental %}+ agg.starting_session_number {% endif %}
            as session_number

    from sessions

    {% if incremental %} 
    left join agg using (blended_user_id) 
    {% endif %}
    

)

select * from windowed

{% endmacro %}