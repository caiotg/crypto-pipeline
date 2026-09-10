select
    coin_id,
    last_captured_at
from {{ ref('mart_market_overview') }}
where last_captured_at < now() - interval '12 hours'