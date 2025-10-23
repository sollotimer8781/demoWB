import json
import socket
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests
from requests import Response
from requests.structures import CaseInsensitiveDict

import wb_client


def _make_response(status_code: int, payload: dict, url: str) -> Response:
    response = Response()
    response.status_code = status_code
    response._content = json.dumps(payload).encode("utf-8")
    response.headers = CaseInsensitiveDict()
    response.url = url
    return response


def test_config_builds_urls_from_env(monkeypatch):
    wb_client.load_config.cache_clear()
    monkeypatch.setenv("WB_API_BASE", " https://example.org/api/ ")
    monkeypatch.setenv("WB_CONTENT_API_BASE", " https://content.example.org ")
    monkeypatch.setenv("WB_API_ENDPOINT_CARDS_V2", " content/v2/cards/list ")
    monkeypatch.setenv("WB_API_ENDPOINT_CARDS_CURSOR_V1", "content/v1/cards/cursor/list")
    monkeypatch.setenv("WB_API_ENDPOINT_PRICES", " api/v2/prices ")
    monkeypatch.setenv("WB_API_ENDPOINT_PRICES_UPDATE", "/api/v2/prices/update")

    config = wb_client.load_config()

    assert config.base_url == "https://example.org/api"
    assert config.content_base_url == "https://content.example.org"
    assert config.cards_v2_endpoint == "/content/v2/cards/list"
    assert config.cards_cursor_v1_endpoint == "/content/v1/cards/cursor/list"
    assert config.prices_list_endpoint == "/api/v2/prices"
    assert config.prices_update_endpoint == "/api/v2/prices/update"
    assert (
        config.build_url(config.cards_v2_endpoint, use_content_api=True)
        == "https://content.example.org/content/v2/cards/list"
    )
    assert (
        config.build_url(config.prices_list_endpoint)
        == "https://example.org/api/api/v2/prices"
    )

    wb_client.load_config.cache_clear()


def test_config_defaults_to_new_domains(monkeypatch):
    wb_client.load_config.cache_clear()
    monkeypatch.delenv("WB_API_BASE", raising=False)
    monkeypatch.delenv("WB_CONTENT_API_BASE", raising=False)
    config = wb_client.load_config()
    assert config.base_url == wb_client.DEFAULT_BASE_URL
    assert config.content_base_url in {wb_client.DEFAULT_CONTENT_BASE_URL, config.base_url}
    wb_client.load_config.cache_clear()


def test_dns_error_has_friendly_message():
    wb_client.load_config.cache_clear()
    session = Mock(spec=requests.Session)

    root_exc = socket.gaierror(-2, "Name or service not known")

    def raise_connection_error(*args, **kwargs):
        raise requests.exceptions.ConnectionError("Name or service not known") from root_exc

    session.request.side_effect = raise_connection_error

    config = wb_client.WBAPIConfig(
        base_url="https://bad-host.example",
        content_base_url="https://bad-host.example",
        cards_cursor_v1_endpoint="/content/v1/cards/cursor/list",
        cards_v2_endpoint="/content/v2/get/cards/list",
        prices_list_endpoint="/api/v2/prices",
        prices_update_endpoint="/api/v2/prices",
    )

    client = wb_client.WBClient(token="token", session=session, config=config, max_retries=0)

    with pytest.raises(wb_client.WBAPIError) as excinfo:
        client.fetch_cards_v2(limit=1)

    message = str(excinfo.value)
    assert "Не удалось разрешить имя хоста" in message
    assert "bad-host.example" in message
    session.request.assert_called_once()

    wb_client.load_config.cache_clear()


def test_resolve_setting_priority_env_section_flat(monkeypatch):
    class Secrets(dict):
        def get(self, key, default=None):  # type: ignore[override]
            return super().get(key, default)

    section_secrets = Secrets(
        {
            "wb": {"WB_API_BASE": " https://section.example "},
            "WB_API_BASE": "https://flat.example",
        }
    )
    monkeypatch.setattr(wb_client, "st", SimpleNamespace(secrets=section_secrets))
    monkeypatch.delenv("WB_API_BASE", raising=False)

    assert wb_client._resolve_setting("WB_API_BASE") == "https://section.example"

    monkeypatch.setenv("WB_API_BASE", "https://env.example")
    assert wb_client._resolve_setting("WB_API_BASE") == "https://env.example"

    monkeypatch.delenv("WB_API_BASE", raising=False)
    flat_secrets = Secrets({"WB_API_BASE": "https://flat.example"})
    monkeypatch.setattr(wb_client, "st", SimpleNamespace(secrets=flat_secrets))
    assert wb_client._resolve_setting("WB_API_BASE") == "https://flat.example"

    monkeypatch.setattr(wb_client, "st", None)


def test_client_switches_authorization_header_on_401():
    session = Mock(spec=requests.Session)
    responses = [
        _make_response(
            401,
            {"message": "invalid"},
            "https://content-api.wildberries.ru/content/v2/get/cards/list",
        ),
        _make_response(
            200,
            {"data": {"cards": []}},
            "https://content-api.wildberries.ru/content/v2/get/cards/list",
        ),
    ]
    session.request.side_effect = responses

    config = wb_client.WBAPIConfig(
        base_url="https://marketplace-api.wildberries.ru",
        content_base_url="https://content-api.wildberries.ru",
        cards_cursor_v1_endpoint="/content/v1/cards/cursor/list",
        cards_v2_endpoint="/content/v2/get/cards/list",
        prices_list_endpoint="/api/v2/prices",
        prices_update_endpoint="/api/v2/prices",
    )

    client = wb_client.WBClient(token="token", session=session, config=config, max_retries=0)

    cards, _, _ = client.fetch_cards_v2(limit=1)
    assert cards == []
    assert session.request.call_count == 2

    first_headers = session.request.call_args_list[0][1]["headers"]
    second_headers = session.request.call_args_list[1][1]["headers"]

    assert first_headers["Authorization"] == "Bearer token"
    assert second_headers["Authorization"] == "token"
    assert client.get_active_auth_label() == "Authorization"


def test_update_prices_dry_run_normalizes_payload():
    session = Mock(spec=requests.Session)
    config = wb_client.WBAPIConfig(
        base_url="https://marketplace-api.wildberries.ru",
        content_base_url="https://content-api.wildberries.ru",
        cards_cursor_v1_endpoint="/content/v1/cards/cursor/list",
        cards_v2_endpoint="/content/v2/get/cards/list",
        prices_list_endpoint="/api/v2/prices",
        prices_update_endpoint="/api/v2/prices",
    )

    client = wb_client.WBClient(token="secret", session=session, config=config, max_retries=0)

    result = client.update_prices(
        [
            {"nm_id": "123", "price": 150.5, "discount": "10", "note": "test"},
        ],
        dry_run=True,
    )

    session.request.assert_not_called()
    assert result["dry_run"] is True
    payload = result["payload"]
    assert len(payload) == 1
    item = payload[0]
    assert item["nmId"] == 123
    assert item["price"] == 15050
    assert item["discount"] == 10
    assert item["note"] == "test"


def test_update_prices_sends_request():
    response = _make_response(
        200,
        {"result": "ok"},
        "https://marketplace-api.wildberries.ru/api/v2/prices",
    )
    session = Mock(spec=requests.Session)
    session.request.return_value = response

    config = wb_client.WBAPIConfig(
        base_url="https://marketplace-api.wildberries.ru",
        content_base_url="https://content-api.wildberries.ru",
        cards_cursor_v1_endpoint="/content/v1/cards/cursor/list",
        cards_v2_endpoint="/content/v2/get/cards/list",
        prices_list_endpoint="/api/v2/prices",
        prices_update_endpoint="/api/v2/prices",
    )

    client = wb_client.WBClient(token="secret", session=session, config=config, max_retries=0)

    result = client.update_prices([
        {"nmId": 1, "price": 1500},
    ])

    assert result == {"result": "ok"}
    session.request.assert_called_once()
    args, kwargs = session.request.call_args
    assert kwargs["method"] == "POST"
    assert kwargs["json"] == [{"nmId": 1, "price": 1500}]


def test_fetch_prices_builds_params():
    response = _make_response(
        200,
        {"prices": []},
        "https://marketplace-api.wildberries.ru/api/v2/prices",
    )
    session = Mock(spec=requests.Session)
    session.request.return_value = response

    config = wb_client.WBAPIConfig(
        base_url="https://marketplace-api.wildberries.ru",
        content_base_url="https://content-api.wildberries.ru",
        cards_cursor_v1_endpoint="/content/v1/cards/cursor/list",
        cards_v2_endpoint="/content/v2/get/cards/list",
        prices_list_endpoint="/api/v2/prices",
        prices_update_endpoint="/api/v2/prices",
    )

    client = wb_client.WBClient(token="secret", session=session, config=config, max_retries=0)

    client.fetch_prices(limit=10, offset=5, nm_ids=[123, "456"])

    args, kwargs = session.request.call_args
    assert kwargs["method"] == "GET"
    assert kwargs["params"] == {"limit": 10, "offset": 5, "nmID": "123,456"}


def test_check_connection_returns_diagnostics():
    response = _make_response(
        200,
        {"data": {"cards": []}},
        "https://content-api.wildberries.ru/content/v2/get/cards/list",
    )
    session = Mock(spec=requests.Session)
    session.request.return_value = response

    config = wb_client.WBAPIConfig(
        base_url="https://suppliers-api.wildberries.ru",
        content_base_url="https://content-api.wildberries.ru",
        cards_cursor_v1_endpoint="/content/v1/cards/cursor/list",
        cards_v2_endpoint="/content/v2/get/cards/list",
        prices_list_endpoint="/api/v2/prices",
        prices_update_endpoint="/api/v2/prices",
    )

    client = wb_client.WBClient(token="secret", session=session, config=config, max_retries=0)

    info = client.check_connection()

    assert info["status"] == "ok"
    assert "warning" in info
    assert "suppliers-api" in info["warning"]
    assert info["auth_header"]
    session.request.assert_called_once()
    request_kwargs = session.request.call_args[1]
    assert request_kwargs["method"] == "POST"
    assert request_kwargs["url"].startswith("https://content-api.wildberries.ru")


