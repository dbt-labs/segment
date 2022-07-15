-- depends_on: {{ ref('segment_web_sessions') }}




{{ config(materialized = 'table') }}

select *
from {{ metrics.metric(
    metric_name='segment__daily_number_of_sessions_by_referrer_source',
    grain='day',
    dimensions=['referrer_source'],
    secondary_calculations=[]
) }}
