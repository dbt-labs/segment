with

{#
The if statement below checks to see if segment_page_views_table is a string or a list, and then builds the model accordingly
#}

{% if var('segment_page_views_table') is string %}
    
    unioned_sources AS (
        select 'segment_page_views_table' as source_name, * from {{var('segment_page_views_table')}}
    ),


{% elif var('segment_page_views_table') is iterable %}

    {#
    The section below takes each of the items listed for the segment_page_views_table variable, creates CTEs for them,
    and then adds a field to note the name of the source table that the records are related to.
    #}

    unioned_sources as (
        {% for table_ref in var('segment_page_views_table', default=[]) %}
            SELECT
                '{{ table_ref }}' as source_name
                , *
            FROM
                {{ ref(table_ref) }}
            {%- if not loop.last %}
                UNION ALL
            {%- endif %}  
        {% endfor %}
        ),

{% endif %}


row_numbering as (

    select
        *,
        row_number() over (partition by source_name, id order by received_at asc) as row_num
    from unioned_sources

),

deduped as (

    select
        *
    from row_numbering
    where row_num = 1

),

renamed as (

    select

        source_name,
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
        replace(
            {{ dbt_utils.get_url_host('referrer') }},
            'www.',
            ''
        ) as referrer_host,

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
                {{ dbt.split_part(dbt.split_part('context_user_agent', "'('", 2), "' '", 1) }},
                ';', '')
        end as device

        {% if var('segment_pass_through_columns') != [] %}
        ,
        {{ var('segment_pass_through_columns') | join (", ")}}

        {% endif %}

    from deduped

),

final as (

    select
        *,
        case
            when device = 'iPhone' then 'iPhone'
            when device = 'Android' then 'Android'
            when device in ('iPad', 'iPod') then 'Tablet'
            when device in ('Windows', 'Macintosh', 'X11') then 'Desktop'
            else 'Uncategorized'
        end as device_category
    from renamed

)

select * from final
