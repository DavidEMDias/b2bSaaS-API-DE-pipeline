{{
    config(
        materialized='table',
        schema='gold',
    )
}}

select
    s.session_id as session_id,
    c.customer_key,
    co.country_key,
    dev.device_key,
    cmp.campaign_key,
    d.date_key as session_date_key,
    s.pageviews,
    s.session_duration_s,
    s.bounced,
    s.converted,
    s.updated_at
from {{ ref('stg_sessions') }} s
left join {{ ref('dim_customer') }} c
    on s.customer_id = c.customer_id
left join {{ ref('dim_country') }} co
    on s.country = co.country_name
left join {{ ref('dim_device') }} dev
    on lower(s.device) = dev.device_name
left join {{ ref('dim_campaign') }} cmp
    on lower(s.source) = cmp.source
   and lower(s.medium) = cmp.medium
   and s.campaign = cmp.campaign
left join {{ ref('dim_date') }} d
    on s.session_start::date = d.full_date
