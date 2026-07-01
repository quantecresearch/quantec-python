"""Tests for _poll_download_status handling direct file responses (non-JSON).

When an async download is already complete, the API may return the file content
directly (e.g. Content-Type: text/csv) instead of a JSON status object.
Previously this caused a JSONDecodeError.
"""

import pytest
import pandas as pd
import requests
from quantec.easydata.client import (
    AsyncDownloadFailedError,
    AsyncDownloadTimeoutError,
    Client,
)


class FakeResponse:
    """Minimal fake requests.Response for testing."""

    def __init__(self, status_code=200, headers=None, text="", content=b""):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text
        self.content = content

    def json(self):
        import json
        return json.loads(self.text)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


CSV_BODY = "Flow,Unit,Date,Value\nExport,KG,2020,100\nImport,KG,2021,200\n"
PARQUET_BODY = pd.DataFrame({"col": [1, 2]}).to_parquet()


class TestPollDownloadDirectResponse:
    """Test _poll_download_status when API returns file content directly."""

    @pytest.fixture
    def client(self):
        return Client(api_key="test-key", api_url="http://fake", use_cache=False)

    def test_csv_returned_directly(self, client, monkeypatch):
        """When status endpoint returns CSV directly, polling should succeed."""
        resp = FakeResponse(
            status_code=200,
            headers={"Content-Type": "text/csv"},
            text=CSV_BODY,
            content=CSV_BODY.encode(),
        )
        monkeypatch.setattr(requests, "get", lambda *a, **kw: resp)

        result = client._poll_download_status(
            status_url="http://fake/griddownloads/1/",
            download_id=1,
            poll_interval=0,
            max_poll_attempts=1,
        )

        assert result["status"] == "r"
        assert "_response" in result
        assert result["_response"] is resp

    def test_octet_stream_returned_directly(self, client, monkeypatch):
        """When status endpoint returns binary directly, polling should succeed."""
        resp = FakeResponse(
            status_code=200,
            headers={"Content-Type": "application/octet-stream"},
            content=PARQUET_BODY,
        )
        monkeypatch.setattr(requests, "get", lambda *a, **kw: resp)

        result = client._poll_download_status(
            status_url="http://fake/griddownloads/1/",
            download_id=1,
            poll_interval=0,
            max_poll_attempts=1,
        )

        assert result["status"] == "r"
        assert result["_response"] is resp

    def test_json_status_still_works(self, client, monkeypatch):
        """Normal JSON status responses should still be handled correctly."""
        import json

        body = json.dumps({"status": "r", "download_url": "http://fake/file.csv"})
        resp = FakeResponse(
            status_code=200,
            headers={"Content-Type": "application/json"},
            text=body,
        )
        monkeypatch.setattr(requests, "get", lambda *a, **kw: resp)

        result = client._poll_download_status(
            status_url="http://fake/griddownloads/1/",
            download_id=1,
            poll_interval=0,
            max_poll_attempts=1,
        )

        assert result["status"] == "r"
        assert result["download_url"] == "http://fake/file.csv"
        assert "_response" not in result


class TestPollDownloadErrors:
    """Test async polling error states."""

    @pytest.fixture
    def client(self):
        return Client(api_key="test-key", api_url="http://fake", use_cache=False)

    def test_timeout_when_max_poll_attempts_exceeded(self, client, monkeypatch):
        """Busy status should eventually raise AsyncDownloadTimeoutError."""
        import json

        resp = FakeResponse(
            status_code=200,
            headers={"Content-Type": "application/json"},
            text=json.dumps({"status": "b"}),
        )
        monkeypatch.setattr(requests, "get", lambda *a, **kw: resp)

        with pytest.raises(AsyncDownloadTimeoutError, match="exceeded maximum polling attempts"):
            client._poll_download_status(
                status_url="http://fake/griddownloads/1/",
                download_id=1,
                poll_interval=0,
                max_poll_attempts=2,
            )

    @pytest.mark.parametrize(
        ("status", "message"),
        [
            ("c", "was cancelled"),
            ("x", "has expired"),
            ("t", "timed out on server"),
        ],
    )
    def test_failed_statuses_raise_async_download_failed_error(
        self, client, monkeypatch, status, message
    ):
        """Terminal failure statuses should raise AsyncDownloadFailedError."""
        import json

        resp = FakeResponse(
            status_code=200,
            headers={"Content-Type": "application/json"},
            text=json.dumps({"status": status}),
        )
        monkeypatch.setattr(requests, "get", lambda *a, **kw: resp)

        with pytest.raises(AsyncDownloadFailedError, match=message):
            client._poll_download_status(
                status_url="http://fake/griddownloads/1/",
                download_id=1,
                poll_interval=0,
                max_poll_attempts=1,
            )


class TestGetGridDataDirectResponse:
    """Test that get_grid_data handles the _response from polling correctly."""

    @pytest.fixture
    def client(self):
        return Client(api_key="test-key", api_url="http://fake", use_cache=False)

    def _make_202_response(self, download_id=1):
        """Create a fake 202 async-initiated response."""
        import json

        body = json.dumps({
            "download": {"id": download_id},
            "status_url": f"http://fake/griddownloads/{download_id}/",
        })
        return FakeResponse(
            status_code=202,
            headers={"Content-Type": "application/json"},
            text=body,
        )

    def test_get_grid_data_csv_direct_response(self, client, monkeypatch):
        """get_grid_data should return CSV when polling gets file directly (GET path)."""
        call_count = {"n": 0}

        def fake_get(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                # First call: initial GET to start async download
                return self._make_202_response()
            else:
                # Second call: polling returns CSV directly
                return FakeResponse(
                    status_code=200,
                    headers={"Content-Type": "text/csv"},
                    text=CSV_BODY,
                    content=CSV_BODY.encode(),
                )

        monkeypatch.setattr(requests, "get", fake_get)

        result = client.get_grid_data(
            recipe_pk=1,
            resp_format="csv",
            use_async=True,
            poll_interval=0,
            max_poll_attempts=1,
        )

        assert isinstance(result, str)
        assert "Flow" in result

    def test_get_grid_data_dataframe_direct_response(self, client, monkeypatch):
        """get_grid_data should return DataFrame when polling gets parquet directly (GET path)."""
        call_count = {"n": 0}

        def fake_get(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return self._make_202_response()
            else:
                return FakeResponse(
                    status_code=200,
                    headers={"Content-Type": "application/octet-stream"},
                    content=PARQUET_BODY,
                )

        monkeypatch.setattr(requests, "get", fake_get)

        result = client.get_grid_data(
            recipe_pk=1,
            resp_format="dataframe",
            use_async=True,
            poll_interval=0,
            max_poll_attempts=1,
        )

        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_get_grid_data_post_path_direct_response(self, client, monkeypatch):
        """get_grid_data should handle direct response on the POST path (with filters)."""
        monkeypatch.setattr(
            requests,
            "post",
            lambda *a, **kw: self._make_202_response(),
        )

        csv_resp = FakeResponse(
            status_code=200,
            headers={"Content-Type": "text/csv"},
            text=CSV_BODY,
            content=CSV_BODY.encode(),
        )
        monkeypatch.setattr(requests, "get", lambda *a, **kw: csv_resp)

        result = client.get_grid_data(
            recipe_pk=1,
            selectdimensionnodes={"dimension": "d1", "codes": ["X"]},
            resp_format="csv",
            use_async=True,
            poll_interval=0,
            max_poll_attempts=1,
        )

        assert isinstance(result, str)
        assert "Flow" in result
