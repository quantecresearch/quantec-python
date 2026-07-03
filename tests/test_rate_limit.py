"""Tests for 429 backoff/retry behavior in the client."""
import pytest
import requests

import quantec.easydata.client as client_module
from quantec.easydata.client import Client, MAX_RETRIES

from tests.test_header_auth import FakeResponse


@pytest.fixture
def sleeps(monkeypatch):
    """Capture time.sleep calls instead of sleeping."""
    calls = []
    monkeypatch.setattr(client_module.time, "sleep", calls.append)
    return calls


def make_client():
    return Client(api_key="test-key", api_url="http://fake", use_cache=False)


def test_retries_on_429_then_succeeds(monkeypatch, sleeps):
    client = make_client()
    responses = [
        FakeResponse(status_code=429, headers={"Retry-After": "2"}),
        FakeResponse(text="Date,Value\n2020,1\n"),
    ]
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return responses[len(calls) - 1]

    monkeypatch.setattr(client.session, "get", fake_get)

    result = client.get_data(time_series_codes="ABC", resp_format="csv")
    assert result == "Date,Value\n2020,1\n"
    assert len(calls) == 2
    assert sleeps == [2.0]


def test_gives_up_after_max_retries(monkeypatch, sleeps):
    client = make_client()
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return FakeResponse(status_code=429, headers={"Retry-After": "1"})

    monkeypatch.setattr(client.session, "get", fake_get)

    with pytest.raises(RuntimeError, match="Rate limited"):
        client.get_data(time_series_codes="ABC")
    assert len(calls) == MAX_RETRIES + 1
    assert len(sleeps) == MAX_RETRIES


def test_fails_fast_on_long_retry_after(monkeypatch, sleeps):
    client = make_client()
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return FakeResponse(status_code=429, headers={"Retry-After": "3600"})

    monkeypatch.setattr(client.session, "get", fake_get)

    with pytest.raises(RuntimeError, match="Try again in 3600s"):
        client.get_data(time_series_codes="ABC")
    assert len(calls) == 1
    assert sleeps == []


def test_exponential_backoff_without_retry_after(monkeypatch, sleeps):
    client = make_client()
    responses = [
        FakeResponse(status_code=429),
        FakeResponse(status_code=429),
        FakeResponse(status_code=429),
        FakeResponse(text="Date,Value\n2020,1\n"),
    ]
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return responses[len(calls) - 1]

    monkeypatch.setattr(client.session, "get", fake_get)

    client.get_data(time_series_codes="ABC", resp_format="csv")
    assert sleeps == [1.0, 2.0, 4.0]


def test_non_429_errors_raise_immediately(monkeypatch, sleeps):
    client = make_client()
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return FakeResponse(status_code=500, text="server error")

    monkeypatch.setattr(client.session, "get", fake_get)

    with pytest.raises(requests.HTTPError):
        client.get_data(time_series_codes="ABC")
    assert len(calls) == 1
    assert sleeps == []


def test_retry_applies_to_grid_data_post(monkeypatch, sleeps):
    client = make_client()
    responses = [
        FakeResponse(status_code=429, headers={"Retry-After": "1"}),
        FakeResponse(text="A,B\n1,2\n"),
    ]
    calls = []

    def fake_post(url, **kwargs):
        calls.append(url)
        return responses[len(calls) - 1]

    monkeypatch.setattr(client.session, "post", fake_post)

    result = client.get_grid_data(
        recipe_pk=1,
        selectdimensionnodes={"dimension": "d1", "codes": ["X"]},
        resp_format="csv",
        use_async=False,
    )
    assert result == "A,B\n1,2\n"
    assert len(calls) == 2
    assert sleeps == [1.0]
