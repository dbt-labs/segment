-- depends_on: {{ ref('segment__users') }}




{{ config(materialized = 'table') }}

select *
from {{ metrics.metric(
    metric_name='segment__daily_unique_users_by_country',
    grain='day',
    dimensions=['context_location_country'],
    secondary_calculations=[]
) }}

