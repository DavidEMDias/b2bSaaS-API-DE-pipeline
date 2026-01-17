{{
    config(
        materialized='table',
        schema='gold',
    )
}}

with campaigns as (
    select distinct lower(source) as source, lower(medium) as medium, campaign
    from {{ ref('stg_sessions') }}
)

select
    dense_rank() over (order by source, medium, campaign) as campaign_key,
    source,
    medium,
    campaign
from campaigns
