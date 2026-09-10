{{ config(materialized='table') }}

with latest_snapshot as (
    select
        coin_id,
        symbol,
        name,
        current_price,
        market_cap,
        market_cap_rank,
        total_volume,
        price_change_percentage_24h,
        ingested_at,
        row_number() over (
            partition by coin_id
            order by ingested_at desc
        ) as rn
    from {{ ref('stg_coins_market') }}
)

select
    coin_id,
    symbol,
    name,
    current_price,
    market_cap,
    market_cap_rank,
    total_volume,
    price_change_percentage_24h,
    ingested_at as last_captured_at
from latest_snapshot
where rn = 1
order by market_cap_rank