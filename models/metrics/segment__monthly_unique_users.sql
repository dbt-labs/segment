-- depends_on: {{ ref('segment__users') }}




{{ config(materialized = 'table') }}

select *
from {{ metrics.metric(
    metric_name='segment__monthly_unique_users',
    grain='month',
    dimensions=[],
    secondary_calculations=[]
) }}


