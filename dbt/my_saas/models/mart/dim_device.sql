{{
    config(
        materialized='table',
        schema='gold',
    )
}}

with devices as (
    select distinct lower(device) as device
    from {{ ref('stg_sessions') }}
    where device is not null
)

select
    dense_rank() over (order by device) as device_key,
    device as device_name
from devices
