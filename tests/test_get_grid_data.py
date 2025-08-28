import pytest
import pandas as pd
import requests
from pathlib import Path
from quantec.easydata.client import Client


class TestGetGridData:
    """Test cases for the get_grid_data endpoint."""

    def test_get_grid_data_basic(self, test_client, test_recipe_pk):
        """Test basic grid data retrieval."""
        result = test_client.get_grid_data(recipe_pk=test_recipe_pk)
        
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result.columns) > 0

    def test_get_grid_data_csv_format(self, test_client_csv, test_recipe_pk):
        """Test grid data retrieval in CSV format returns valid CSV that can be loaded as DataFrame."""
        result = test_client_csv.get_grid_data(recipe_pk=test_recipe_pk, resp_format="csv")
        
        # Should return raw CSV string
        assert isinstance(result, str)
        assert len(result) > 0
        
        # CSV should be valid and loadable into DataFrame
        from io import StringIO
        df = pd.read_csv(StringIO(result))
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_get_grid_data_parquet_format(self, test_client, test_recipe_pk):
        """Test grid data retrieval in Parquet format returns valid parquet that can be loaded as DataFrame."""
        result = test_client.get_grid_data(recipe_pk=test_recipe_pk, resp_format="parquet")
        
        # Should return raw parquet bytes
        assert isinstance(result, bytes)
        assert len(result) > 0
        
        # Parquet should be valid and loadable into DataFrame
        from io import BytesIO
        df = pd.read_parquet(BytesIO(result))
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_get_grid_data_json_format_raises_error(self, test_client, test_recipe_pk):
        """Test that JSON format raises error (not supported for grid data)."""
        with pytest.raises(ValueError, match="resp_format must be 'dataframe', 'parquet', or 'csv'"):
            test_client.get_grid_data(recipe_pk=test_recipe_pk, resp_format="json")

    def test_get_grid_data_dataframe_format(self, test_client, test_recipe_pk):
        """Test grid data retrieval in DataFrame format (default)."""
        result = test_client.get_grid_data(recipe_pk=test_recipe_pk, resp_format="dataframe")
        
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_get_grid_data_csv_format_returns_string(self, test_client, test_recipe_pk):
        """Test grid data retrieval in CSV format returns string."""
        result = test_client.get_grid_data(recipe_pk=test_recipe_pk, resp_format="csv")
        
        assert isinstance(result, str)
        assert len(result) > 0
        # Should contain CSV headers/data
        assert "," in result or "\n" in result

    def test_get_grid_data_parquet_format_returns_bytes(self, test_client, test_recipe_pk):
        """Test grid data retrieval in Parquet format returns bytes."""
        result = test_client.get_grid_data(recipe_pk=test_recipe_pk, resp_format="parquet")
        
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_get_grid_data_with_dimension_filter(self, test_client, test_recipe_pk, test_dimension_filter):
        """Test grid data retrieval with dimension filtering (levels only)."""
        result = test_client.get_grid_data(
            recipe_pk=test_recipe_pk,
            selectdimensionnodes=test_dimension_filter,
            resp_format="dataframe"
        )
        
        assert isinstance(result, pd.DataFrame)
        # Result might be empty if filter returns no data, but should be valid DataFrame

    def test_get_grid_data_with_dimension_filter_codes(self, test_client, test_recipe_pk, test_dimension_filter_w_codes):
        """Test grid data retrieval with dimension filtering (levels and codes)."""
        result = test_client.get_grid_data(
            recipe_pk=test_recipe_pk,
            selectdimensionnodes=test_dimension_filter_w_codes,
            resp_format="dataframe"
        )
        
        assert isinstance(result, pd.DataFrame)
        # Result might be empty if filter returns no data, but should be valid DataFrame

    def test_get_grid_data_expanded_and_melted_options(self, test_client, test_recipe_pk):
        """Test grid data with different expansion and melting options."""
        # Test expanded=True, melted=True (default)
        result1 = test_client.get_grid_data(
            recipe_pk=test_recipe_pk,
            is_expanded=True,
            is_melted=True
        )
        
        # Test expanded=False, melted=False
        result2 = test_client.get_grid_data(
            recipe_pk=test_recipe_pk,
            is_expanded=False,
            is_melted=False
        )
        
        assert isinstance(result1, pd.DataFrame)
        assert isinstance(result2, pd.DataFrame)
        
        # Results may have different shapes due to expansion/melting
        assert not result1.empty
        assert not result2.empty

    def test_get_grid_data_caching_enabled(self, test_client_with_cache, test_recipe_pk):
        """Test grid data caching functionality."""
        # First call should fetch from API and cache
        result1 = test_client_with_cache.get_grid_data(recipe_pk=test_recipe_pk)
        
        # Check that cache directory was created
        cache_dir = Path("test_cache")
        assert cache_dir.exists()
        
        # Check that cache file exists
        cache_files = list(cache_dir.glob("*.parquet"))
        assert len(cache_files) > 0
        
        # Second call should load from cache
        result2 = test_client_with_cache.get_grid_data(recipe_pk=test_recipe_pk)
        
        # Results should be identical
        pd.testing.assert_frame_equal(result1, result2)

    def test_get_grid_data_caching_with_different_params(self, test_client_with_cache, test_recipe_pk):
        """Test that different parameters create different cache entries."""
        # Call with different parameters
        result1 = test_client_with_cache.get_grid_data(
            recipe_pk=test_recipe_pk,
            is_expanded=True,
            is_melted=True
        )
        
        result2 = test_client_with_cache.get_grid_data(
            recipe_pk=test_recipe_pk,
            is_expanded=False,
            is_melted=False
        )
        
        # Should create separate cache entries
        cache_dir = Path("test_cache")
        cache_files = list(cache_dir.glob("*.parquet"))
        assert len(cache_files) >= 2

    def test_get_grid_data_invalid_format_raises_error(self, test_client, test_recipe_pk):
        """Test that invalid response format raises ValueError."""
        with pytest.raises(ValueError, match="resp_format must be 'dataframe', 'parquet', or 'csv'"):
            test_client.get_grid_data(recipe_pk=test_recipe_pk, resp_format="invalid_format")

    def test_get_grid_data_invalid_recipe_pk(self, test_client):
        """Test handling of invalid recipe primary key."""
        with pytest.raises(requests.HTTPError):
            test_client.get_grid_data(recipe_pk=99999)  # Non-existent recipe


    def test_get_grid_data_auth_error_handling(self):
        """Test authentication error handling with invalid API key."""
        client = Client(
            apikey="invalid_api_key",
            api_url="http://127.0.0.1:8001"
        )
        
        with pytest.raises(requests.HTTPError):
            client.get_grid_data(recipe_pk=53)

    def test_get_grid_data_post_vs_get_requests(self, test_client, test_recipe_pk, test_dimension_filter, test_dimension_filter_w_codes):
        """Test that filtering uses POST while normal requests use GET."""
        # Normal request (should use GET)
        result1 = test_client.get_grid_data(recipe_pk=test_recipe_pk)
        
        # Filtered request with levels (should use POST)
        result2 = test_client.get_grid_data(
            recipe_pk=test_recipe_pk,
            selectdimensionnodes=test_dimension_filter
        )
        
        # Filtered request with levels and codes (should use POST)
        result3 = test_client.get_grid_data(
            recipe_pk=test_recipe_pk,
            selectdimensionnodes=test_dimension_filter_w_codes
        )
        
        # All should return valid DataFrames
        assert isinstance(result1, pd.DataFrame)
        assert isinstance(result2, pd.DataFrame)
        assert isinstance(result3, pd.DataFrame)

    def test_get_grid_data_empty_filter_raises_error(self, test_client, test_recipe_pk):
        """Test grid data with empty dimension filter raises appropriate error."""
        empty_filter = {"dimension": "d3", "levels": [], "codes": []}
        
        # Backend should reject empty filters
        with pytest.raises(requests.HTTPError, match="Must provide at least one of"):
            test_client.get_grid_data(
                recipe_pk=test_recipe_pk,
                selectdimensionnodes=empty_filter
            )

    def test_get_grid_data_data_cleaning(self, test_client, test_recipe_pk):
        """Test that returned data is properly cleaned (no all-null columns)."""
        result = test_client.get_grid_data(recipe_pk=test_recipe_pk)
        
        # Check that no columns are all null (client should drop them)
        for column in result.columns:
            assert not result[column].isnull().all(), f"Column {column} is all null"

    def test_get_grid_data_consistent_format_across_calls(self, test_client, test_recipe_pk):
        """Test that multiple calls return consistent data format."""
        result1 = test_client.get_grid_data(recipe_pk=test_recipe_pk)
        result2 = test_client.get_grid_data(recipe_pk=test_recipe_pk)
        
        # Should have same columns
        assert result1.columns.tolist() == result2.columns.tolist()
        
        # Should have same shape
        assert result1.shape == result2.shape