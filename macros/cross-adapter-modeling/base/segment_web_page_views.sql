{% macro segment_web_page_views() %}

    {{ adapter_macro('segment.segment_web_page_views') }}

{% endmacro %}


{% macro default__segment_web_page_views() %}

with source as (

    select * from {{var('segment_page_views_table')}}
    
),

renamed as (

    select
    
        id as page_view_id,
        anonymous_id,
        user_id,
        
        received_at as received_at_tstamp,
        sent_at as sent_at_tstamp,
        timestamp as tstamp,

        url as page_url,
        {{ dbt_utils.get_url_host('url') }} as page_url_host,
        path as page_url_path,
        title as page_title,
        search as page_url_query,
        
        referrer,
        ltrim({{ dbt_utils.safe_cast(dbt_utils.get_url_host('referrer'), 'string') }}, 'www.') as referrer_host,

        context_campaign_source as utm_source,
        context_campaign_medium as utm_medium,
        context_campaign_name as utm_campaign,
        context_campaign_term as utm_term,
        context_campaign_content as utm_content,
        {{ dbt_utils.get_url_parameter('url', 'gclid') }} as gclid,
        context_ip as ip,
        context_user_agent as user_agent,
        case
            when lower(context_user_agent) like '%android%' then 'Android'
            else replace(
                {{ dbt_utils.split_part(dbt_utils.split_part('context_user_agent', "'('", 2), "' '", 1) }},
                ';', '')
        end as device
                        
    from source

),

final as (
    
    select
        *,
        case
            when device = 'iPhone' then 'iPhone'
            when device = 'Android' then 'Android'
            when device in ('iPad', 'iPod') then 'Tablet'
            when device in ('Windows', 'Macintosh', 'X11') then 'Desktop'
            else 'uncategorized'
        end as device_category
    from renamed
    
)

select * from final

{% endmacro %}