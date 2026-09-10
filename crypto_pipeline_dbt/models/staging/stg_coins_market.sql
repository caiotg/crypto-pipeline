with source as (
    select * from {{ source('raw', 'raw_coins_market') }}
),

deduplicated as (
    select
        id as coin_id,
        symbol,
        name,
        current_price,
        market_cap,
        market_cap_rank,
        total_volume,
        price_change_percentage_24h,
        ingested_at,
        row_number() over (
            partition by id, date_trunc('hour', ingested_at)
            order by ingested_at desc
        ) as rn
    from source
)

select
    coin_id, symbol, name, current_price, market_cap,
    market_cap_rank, total_volume, price_change_percentage_24h, ingested_at
from deduplicated
where rn = 1