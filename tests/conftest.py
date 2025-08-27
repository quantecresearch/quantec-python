import os
import pytest
from quantec.easydata.client import Client


@pytest.fixture
def test_client():
    """Create a test client using environment variables."""
    return Client()


@pytest.fixture
def test_client_csv():
    """Create a test client with CSV format."""
    return Client(respformat="csv")


@pytest.fixture
def test_client_with_cache():
    """Create a test client with caching enabled."""
    return Client(respformat="parquet", use_cache=True, cache_dir="test_cache")


@pytest.fixture
def test_recipe_pk():
    """Recipe PK for TRD01 dataset."""
    return 53


@pytest.fixture
def test_dimension_filter():
    """Dimension filter payload for testing."""
    return {"dimension": "d3", "levels": [2], "codes": []}


@pytest.fixture
def test_dimension_filter_w_codes():
    """Dimension filter payload for testing."""
    return {"dimension": "d3", "levels": [1], "codes": ["TRD01-R_FI"]}


@pytest.fixture
def sample_time_series_codes():
    """Sample time series codes for testing."""
    return "NMS-EC_BUS,NMS-GA_BUS"


@pytest.fixture(scope="session", autouse=True)
def cleanup_test_cache():
    """Clean up test cache after tests."""
    yield
    import shutil
    from pathlib import Path

    test_cache_dir = Path("test_cache")
    if test_cache_dir.exists():
        shutil.rmtree(test_cache_dir)
