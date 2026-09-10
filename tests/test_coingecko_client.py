from unittest.mock import MagicMock

import pytest

from plugins.coingecko_client import CoinGeckoClient


def test_get_markets_retorna_lista_de_moedas():
    session_mock = MagicMock()
    response_mock = MagicMock()
    response_mock.status_code = 200
    response_mock.json.return_value = [
        {"id": "bitcoin", "current_price": 77000},
        {"id": "ethereum", "current_price": 2400},
    ]
    session_mock.get.return_value = response_mock

    client = CoinGeckoClient(session=session_mock)
    resultado = client.get_markets(per_page=2)

    assert len(resultado) == 2
    assert resultado[0]["id"] == "bitcoin"


def test_retry_em_rate_limit_429():
    session_mock = MagicMock()

    response_429 = MagicMock()
    response_429.status_code = 429

    response_200 = MagicMock()
    response_200.status_code = 200
    response_200.json.return_value = [{"id": "bitcoin", "current_price": 77000}]

    # primeira chamada retorna 429, segunda retorna sucesso
    session_mock.get.side_effect = [response_429, response_200]

    client = CoinGeckoClient(session=session_mock, max_retries=3)
    resultado = client.get_markets(per_page=1)

    assert session_mock.get.call_count == 2
    assert resultado[0]["id"] == "bitcoin"


def test_falha_apos_esgotar_tentativas():
    session_mock = MagicMock()
    response_429 = MagicMock()
    response_429.status_code = 429
    session_mock.get.return_value = response_429

    client = CoinGeckoClient(session=session_mock, max_retries=2)

    with pytest.raises(RuntimeError):
        client.get_markets()

    assert session_mock.get.call_count == 2