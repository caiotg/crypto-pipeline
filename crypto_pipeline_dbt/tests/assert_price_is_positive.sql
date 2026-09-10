select
    coin_id,
    current_price
from {{ ref('mart_market_overview') }}
where current_price < 0