-- depends_on: {{ ref('segment_web_sessions') }}




{{ config(materialized = 'table') }}

select *
from {{ metrics.metric(
    metric_name='segment__monthly_average_session_time_seconds_by_referrer_source',
    grain='month',
    dimensions=['referrer_source'],
    secondary_calculations=[]
) }}
