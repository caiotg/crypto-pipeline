import os
import time
import requests
from typing import Any
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.coingecko.com/api/v3"


class CoinGeckoClient:
    def __init__(self, session: requests.Session | None = None, max_retries: int = 3):
        self.session = session or requests.Session()
        self.max_retries = max_retries
        self.api_key = os.getenv("COINGECKO_API_KEY")

    def get_markets(self, vs_currency: str = "usd", per_page: int = 100, page: int = 1) -> list[dict[str, Any]]:
        params = {
            "vs_currency": vs_currency,
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": page,
            "sparkline": "false",
        }
        return self._get("/coins/markets", params)

    def _get(self, path: str, params: dict) -> list[dict[str, Any]]:
        url = f"{BASE_URL}{path}"
        headers = {"x-cg-demo-api-key": self.api_key} if self.api_key else {}

        for attempt in range(1, self.max_retries + 1):
            resp = self.session.get(url, params=params, headers=headers, timeout=15)
            if resp.status_code == 429:
                wait = 2 ** attempt
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(f"Falha ao consultar {url} após {self.max_retries} tentativas")