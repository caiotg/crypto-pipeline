from plugins.coingecko_client import CoinGeckoClient

client = CoinGeckoClient()
dados = client.get_markets(per_page=5, page=1)

for moeda in dados:
    print(moeda["id"], moeda["current_price"])