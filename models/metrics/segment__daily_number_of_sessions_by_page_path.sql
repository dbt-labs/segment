-- depends_on: {{ ref('segment_web_sessions') }}




{{ config(materialized = 'table') }}

select *
from {{ metrics.metric(
    metric_name='segment__daily_number_of_sessions_by_page_path',
    grain='day',
    dimensions=['last_page_url_path'],
    secondary_calculations=[]
) }}
