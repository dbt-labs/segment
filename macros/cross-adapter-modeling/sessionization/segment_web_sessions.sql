{% macro segment_web_sessions() %}

    {{ adapter_macro('segment.segment_web_sessions') }}

{% endmacro %}


{% macro default__segment_web_sessions() %}

{{ config(
    materialized = 'incremental',
    unique_key = 'session_id',
    sort = 'session_start_tstamp',
    dist = 'session_id'
    )}}
    
{% set sessionization_cutoff = "
(
    select 
        dateadd(
            hour, 
            -{{var('segment_sessionization_trailing_window')}}, 
            max(session_start_tstamp)
            ) 
    from {{this}}
)    
" %}

{# 
Window functions are challenging to make incremental. This approach grabs 
existing values from the existing table and then adds the value of session_number
on top of that seed. During development, this decreased the model runtime 
by 25x on 2 years of data (from 600 to 25 seconds), so even though the code is 
more complicated, the performance tradeoff is worth it.
#}

with sessions as (

    select * from {{ref('segment_web_sessions__stitched')}}
    
    {% if is_incremental() %}
    where session_start_tstamp > {{sessionization_cutoff}}
    {% endif %}

),

{% if is_incremental() %}

agg as (

    select 
        blended_user_id, 
        count(*) as starting_session_number
    from {{this}}
    
    -- only include sessions that are not going to be resessionized in this run
    where session_start_tstamp <= {{sessionization_cutoff}}

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
            {% if is_incremental() %}+ agg.starting_session_number {% endif %}
            as session_number

    from sessions
    
    {% if is_incremental() %}
    left join agg using (blended_user_id) 
    {% endif %}
    

)

select * from windowed

{% endmacro %}