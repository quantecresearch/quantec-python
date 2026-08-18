import json
import logging

import requests

from quantec import __version__
from quantec.easydata.client import Client


class FakeResponse:
    def __init__(self, status_code=200, text="", content=b"", json_data=None, headers=None):
        self.status_code = status_code
        self.text = text
        self.content = content or text.encode()
        self._json_data = json_data
        self.headers = headers or {}

    def json(self):
        if self._json_data is not None:
            return self._json_data
        return json.loads(self.text)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def merged_headers(client, call):
    headers = dict(client.session.headers)
    headers.update(call.get("headers") or {})
    return headers


def assert_header_auth_only(client, call, token="test-key"):
    headers = merged_headers(client, call)
    assert headers["Authorization"] == f"Token {token}"
    assert headers["User-Agent"] == client.user_agent
    assert "auth_token" not in call.get("params", {})
    assert "auth_token" not in call.get("json", {})


def test_default_user_agent():
    client = Client(api_key="test-key", api_url="http://fake", use_cache=False)

    assert client.user_agent == f"quantec-python/{__version__}"
    assert client.session.headers["User-Agent"] == f"quantec-python/{__version__}"


def test_custom_user_agent_is_prefixed_and_preserves_package_identifier():
    client = Client(
        api_key="test-key",
        api_url="http://fake",
        use_cache=False,
        user_agent="easydata-cli/0.3.0",
    )

    assert client.user_agent == f"easydata-cli/0.3.0 quantec-python/{__version__}"
    assert client.session.headers["User-Agent"].startswith("easydata-cli/0.3.0")


def test_headers_cannot_override_first_class_user_agent_or_auth():
    client = Client(
        api_key="test-key",
        api_url="http://fake",
        use_cache=False,
        user_agent="easydata-cli/0.3.0",
        headers={
            "Authorization": "Bearer wrong-token",
            "User-Agent": "other-agent",
            "X-Test": "yes",
        },
    )

    assert client.session.headers["User-Agent"] == client.user_agent
    assert client.session.headers["X-Test"] == "yes"
    assert "Authorization" not in client.session.headers


def test_get_data_uses_header_auth_only(monkeypatch):
    calls = []
    client = Client(api_key="test-key", api_url="http://fake", use_cache=False)

    def fake_get(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return FakeResponse(text="Date,Value\n2020,1\n")

    monkeypatch.setattr(client.session, "get", fake_get)

    client.get_data(time_series_codes="ABC")

    assert_header_auth_only(client, calls[0])
    assert calls[0]["params"]["timeSeriesCodes"] == "ABC"


def test_get_recipes_uses_header_auth_only(monkeypatch):
    calls = []
    client = Client(api_key="test-key", api_url="http://fake", use_cache=False)

    def fake_get(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return FakeResponse(json_data=[{"id": 1, "dataset_code": "TEST"}])

    monkeypatch.setattr(client.session, "get", fake_get)

    client.get_recipes(dataset="TEST", private=True)

    assert_header_auth_only(client, calls[0])
    assert calls[0]["params"] == {"dataset": "TEST", "private": "y"}


def test_get_selections_uses_header_auth_only(monkeypatch):
    calls = []
    client = Client(api_key="test-key", api_url="http://fake", use_cache=False)

    def fake_get(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return FakeResponse(json_data=[])

    monkeypatch.setattr(client.session, "get", fake_get)

    client.get_selections(status="PSO")

    assert_header_auth_only(client, calls[0])
    assert calls[0]["params"] == {"format": "json", "status": "PSO"}


def test_get_grid_data_get_uses_header_auth_only(monkeypatch):
    calls = []
    client = Client(api_key="test-key", api_url="http://fake", use_cache=False)

    def fake_get(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return FakeResponse(text="A,B\n1,2\n")

    monkeypatch.setattr(client.session, "get", fake_get)

    client.get_grid_data(recipe_pk=1, resp_format="csv", use_async=False)

    assert_header_auth_only(client, calls[0])
    assert calls[0]["params"]["respFormat"] == "csv"


def test_async_polling_uses_session_user_agent_and_auth(monkeypatch):
    calls = []
    client = Client(api_key="test-key", api_url="http://fake", use_cache=False)

    def fake_get(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return FakeResponse(
            headers={"Content-Type": "application/json"},
            text=json.dumps({"status": "r", "download_url": "http://fake/file.csv"}),
        )

    monkeypatch.setattr(client.session, "get", fake_get)

    client._poll_download_status(
        status_url="http://fake/griddownloads/1/",
        download_id=1,
        poll_interval=0,
        max_poll_attempts=1,
    )

    assert_header_auth_only(client, calls[0])


def test_ready_file_download_to_external_url_uses_user_agent_without_auth(monkeypatch):
    calls = []
    client = Client(api_key="test-key", api_url="http://fake", use_cache=False)

    def fake_get(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return FakeResponse(text="A,B\n1,2\n")

    monkeypatch.setattr(client.session, "get", fake_get)

    client._download_ready_file(
        "https://bucket.s3.amazonaws.com/file.csv?X-Amz-Signature=abc",
        download_id=1,
    )

    headers = merged_headers(client, calls[0])
    assert headers["User-Agent"] == client.user_agent
    assert "Authorization" not in headers


def test_api_token_is_not_logged(monkeypatch, caplog):
    client = Client(api_key="secret-token", api_url="http://fake", use_cache=False)

    def fake_get(url, **kwargs):
        return FakeResponse(text="Date,Value\n2020,1\n")

    monkeypatch.setattr(client.session, "get", fake_get)

    with caplog.at_level(logging.DEBUG, logger="quantec.easydata.client"):
        client.get_data(time_series_codes="ABC")

    assert "secret-token" not in caplog.text


def test_get_grid_data_post_uses_header_auth_only(monkeypatch):
    calls = []
    client = Client(api_key="test-key", api_url="http://fake", use_cache=False)

    def fake_post(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return FakeResponse(text="A,B\n1,2\n")

    monkeypatch.setattr(client.session, "post", fake_post)

    client.get_grid_data(
        recipe_pk=1,
        selectdimensionnodes={"dimension": "d1", "codes": ["X"]},
        resp_format="csv",
        use_async=False,
    )

    assert_header_auth_only(client, calls[0])
    assert merged_headers(client, calls[0])["Content-Type"] == "application/json"
    # A single dict filter is wrapped in a list for the API
    assert calls[0]["json"]["selectdimensionnodes"] == [{"dimension": "d1", "codes": ["X"]}]
