-- depends_on: {{ ref('segment_web_sessions') }}




{{ config(materialized = 'table') }}

select *
from {{ metrics.metric(
    metric_name='segment__daily_number_of_sessions',
    grain='day',
    dimensions=[],
    secondary_calculations=[]
) }}
