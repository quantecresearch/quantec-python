import json

import requests

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


def assert_header_auth_only(call, token="test-key"):
    assert call["headers"]["Authorization"] == f"Token {token}"
    assert "auth_token" not in call.get("params", {})
    assert "auth_token" not in call.get("json", {})


def test_get_data_uses_header_auth_only(monkeypatch):
    calls = []

    def fake_get(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return FakeResponse(text="Date,Value\n2020,1\n")

    monkeypatch.setattr(requests, "get", fake_get)

    client = Client(api_key="test-key", api_url="http://fake", use_cache=False)
    client.get_data(time_series_codes="ABC")

    assert_header_auth_only(calls[0])
    assert calls[0]["params"]["timeSeriesCodes"] == "ABC"


def test_get_recipes_uses_header_auth_only(monkeypatch):
    calls = []

    def fake_get(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return FakeResponse(json_data=[{"id": 1, "dataset_code": "TEST"}])

    monkeypatch.setattr(requests, "get", fake_get)

    client = Client(api_key="test-key", api_url="http://fake", use_cache=False)
    client.get_recipes(dataset="TEST", private=True)

    assert_header_auth_only(calls[0])
    assert calls[0]["params"] == {"dataset": "TEST", "private": "y"}


def test_get_selections_uses_header_auth_only(monkeypatch):
    calls = []

    def fake_get(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return FakeResponse(json_data=[])

    monkeypatch.setattr(requests, "get", fake_get)

    client = Client(api_key="test-key", api_url="http://fake", use_cache=False)
    client.get_selections(status="PSO")

    assert_header_auth_only(calls[0])
    assert calls[0]["params"] == {"format": "json", "status": "PSO"}


def test_get_grid_data_get_uses_header_auth_only(monkeypatch):
    calls = []

    def fake_get(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return FakeResponse(text="A,B\n1,2\n")

    monkeypatch.setattr(requests, "get", fake_get)

    client = Client(api_key="test-key", api_url="http://fake", use_cache=False)
    client.get_grid_data(recipe_pk=1, resp_format="csv", use_async=False)

    assert_header_auth_only(calls[0])
    assert calls[0]["params"]["respFormat"] == "csv"


def test_get_grid_data_post_uses_header_auth_only(monkeypatch):
    calls = []

    def fake_post(url, **kwargs):
        calls.append({"url": url, **kwargs})
        return FakeResponse(text="A,B\n1,2\n")

    monkeypatch.setattr(requests, "post", fake_post)

    client = Client(api_key="test-key", api_url="http://fake", use_cache=False)
    client.get_grid_data(
        recipe_pk=1,
        selectdimensionnodes={"dimension": "d1", "codes": ["X"]},
        resp_format="csv",
        use_async=False,
    )

    assert_header_auth_only(calls[0])
    assert calls[0]["headers"]["Content-Type"] == "application/json"
    assert calls[0]["json"]["selectdimensionnodes"] == {"dimension": "d1", "codes": ["X"]}
