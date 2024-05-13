{{ config(
    materialized = 'incremental',
    unique_key = 'page_view_id',
    sort = 'tstamp',
    partition_by = {'field': 'tstamp', 'data_type': 'timestamp', 'granularity': var('segment_bigquery_partition_granularity')},
    dist = 'page_view_id',
    cluster_by = 'page_view_id'
    )}}

{#
the initial CTE in this model is unusually complicated; its function is to
select all pageviews (for all time) for users who have pageviews since the
model was most recently run. there are many window functions in this model so
in order to appropriately calculate all of them we need each users entire
page view history, but we only want to grab that for users who have page view
events we need to calculate.
#}

with pageviews as (

    select * from {{ref('segment_web_page_views')}}

    {% if is_incremental() %}
    where anonymous_id in (
        select distinct anonymous_id
        from {{ref('segment_web_page_views')}}
        {{
            generate_sessionization_incremental_filter( this, 'tstamp', 'tstamp', '>' )
        }}
    )
    {% endif %}

),

numbered as (

    --This CTE is responsible for assigning an all-time page view number for a
    --given anonymous_id. We don't need to do this across devices because the
    --whole point of this field is for sessionization, and sessions can't span
    --multiple devices.

    select

        *,

        row_number() over (
            partition by anonymous_id
            order by tstamp
            ) as page_view_number

    from pageviews

),

lagged as (

    --This CTE is responsible for simply grabbing the last value of `tstamp`.
    --We'll use this downstream to do timestamp math--it's how we determine the
    --period of inactivity.

    select

        *,

        lag(tstamp) over (
            partition by anonymous_id
            order by page_view_number
            ) as previous_tstamp

    from numbered

),

diffed as (

    --This CTE simply calculates `period_of_inactivity`.

    select
        *,
        {{ dbt_utils.datediff('previous_tstamp', 'tstamp', 'second') }} as period_of_inactivity
    from lagged

),

new_sessions as (

    --This CTE calculates a single 1/0 field--if the period of inactivity prior
    --to this page view was greater than 30 minutes, the value is 1, otherwise
    --it's 0. We'll use this to calculate the user's session #.

    select
        *,
        case
            when period_of_inactivity <= {{var('segment_inactivity_cutoff')}} then 0
            else 1
        end as new_session
    from diffed

),

session_numbers as (

    --This CTE calculates a user's session (1, 2, 3) number from `new_session`.
    --This single field is the entire point of the entire prior series of
    --calculations.

    select

        *,

        sum(new_session) over (
            partition by anonymous_id
            order by page_view_number
            rows between unbounded preceding and current row
            ) as session_number

    from new_sessions

),

session_starts as (

    --This CTE calculates the first event timestamp within any session number.
    --Because our version of the Segment package only evaluates a sliding window
    --of events in this model, we need to guarantee uniqueness when we calculate
    --session_id. `session_number` is no longer globally unique so we uniquify
    --it with the addition of the start timestamp.

    select

        *,

        min(tstamp) over (
            partition by anonymous_id, session_number
            ) as session_start_tstamp

    from session_numbers

),

session_ids as (

    --This CTE assigns a unique session id based on the combination of
    --`anonymous_id`, `session_start_tstamp`, and `session_number`.
    --including `session_start_tstamp` helps us uniquify since we no longer
    --include the full lifetime of events in `segment_web_page_views`.

    select

        {{dbt_utils.star(from=ref('segment_web_page_views'), relation_alias='session_starts')}},
        session_starts.page_view_number,
        --if an event has previously been sessionized, keep the existing session ID
        {% if is_incremental() %}sessionized.session_id{% else %}null::string{% endif %} as existing_session_id,
        {{dbt_utils.surrogate_key(['session_starts.anonymous_id', 'session_starts.session_start_tstamp', 'session_starts.session_number'])}} as session_id

    from session_starts
    {% if is_incremental() %}
    left join {{ this }} as sessionized
        on session_starts.page_view_id = sessionized.page_view_id
    {% endif %}

),

consolidated_session as (

    select
        * exclude (existing_session_id, session_id),
        --this line handles new events that are part of an existing session - instead of assigning a brand new
        --session ID, see if there's an existing session ID from other events in the same session, and use that
        coalesce(
            min_by(existing_session_id, tstamp) over (partition by anonymous_id, session_id), --existing ID across the session
            session_id --fall back to new session_id
        ) as session_id
    from session_ids

)

select * from consolidated_session
