import socket
from unittest.mock import Mock

import pytest
import requests

from wb_client import WBAPIConfig, WBAPIError, WBClient, load_config


def test_config_builds_urls_from_env(monkeypatch):
    load_config.cache_clear()
    monkeypatch.setenv("WB_API_BASE", " https://example.org/api/ ")
    monkeypatch.setenv("WB_API_ENDPOINT_CARDS_V2", " content/v2/cards/list ")
    monkeypatch.setenv("WB_API_ENDPOINT_CARDS_CURSOR_V1", "content/v1/cards/cursor/list")

    config = load_config()

    assert config.base_url == "https://example.org/api"
    assert config.cards_v2_endpoint == "/content/v2/cards/list"
    assert config.cards_cursor_v1_endpoint == "/content/v1/cards/cursor/list"
    assert config.build_url(config.cards_v2_endpoint) == "https://example.org/api/content/v2/cards/list"
    assert config.build_url(config.cards_cursor_v1_endpoint) == "https://example.org/api/content/v1/cards/cursor/list"

    load_config.cache_clear()


def test_dns_error_has_friendly_message():
    load_config.cache_clear()
    session = Mock(spec=requests.Session)

    root_exc = socket.gaierror(-2, "Name or service not known")

    def raise_connection_error(*args, **kwargs):
        raise requests.exceptions.ConnectionError("Name or service not known") from root_exc

    session.request.side_effect = raise_connection_error

    config = WBAPIConfig(
        base_url="https://bad-host.example",
        cards_cursor_v1_endpoint="/content/v1/cards/cursor/list",
        cards_v2_endpoint="/content/v2/get/cards/list",
    )

    client = WBClient(token="token", session=session, config=config, max_retries=0)

    with pytest.raises(WBAPIError) as excinfo:
        client.fetch_cards_v2(limit=1)

    message = str(excinfo.value)
    assert "Не удалось разрешить имя хоста" in message
    assert "bad-host.example" in message
    session.request.assert_called_once()

    load_config.cache_clear()
