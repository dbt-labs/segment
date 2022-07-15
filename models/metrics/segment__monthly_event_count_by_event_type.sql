-- depends_on: {{ ref('segment__tracks') }}




{{ config(materialized = 'table') }}

select *
from {{ metrics.metric(
    metric_name='segment__monthly_event_count_by_event_type',
    grain='month',
    dimensions=['event'],
    secondary_calculations=[]
) }}

