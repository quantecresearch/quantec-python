import pytest
import pandas as pd
import requests
from pathlib import Path
from quantec.easydata.client import Client


class TestGetGridData:
    """Test cases for the get_grid_data endpoint."""

    def test_get_grid_data_basic(self, test_client, test_recipe_pk):
        """Test basic grid data retrieval (async mode default)."""
        result = test_client.get_grid_data(recipe_pk=test_recipe_pk)

        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result.columns) > 0

    def test_get_grid_data_csv_format(self, test_client_csv, test_recipe_pk):
        """Test grid data retrieval in CSV format returns valid CSV that can be loaded as DataFrame."""
        result = test_client_csv.get_grid_data(
            recipe_pk=test_recipe_pk, resp_format="csv"
        )

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
        result = test_client.get_grid_data(
            recipe_pk=test_recipe_pk, resp_format="parquet"
        )

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
        with pytest.raises(
            ValueError, match="resp_format must be 'dataframe', 'parquet', or 'csv'"
        ):
            test_client.get_grid_data(recipe_pk=test_recipe_pk, resp_format="json")

    def test_get_grid_data_dataframe_format(self, test_client, test_recipe_pk):
        """Test grid data retrieval in DataFrame format (default)."""
        result = test_client.get_grid_data(
            recipe_pk=test_recipe_pk, resp_format="dataframe"
        )

        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_get_grid_data_csv_format_returns_string(self, test_client, test_recipe_pk):
        """Test grid data retrieval in CSV format returns string."""
        result = test_client.get_grid_data(recipe_pk=test_recipe_pk, resp_format="csv")

        assert isinstance(result, str)
        assert len(result) > 0
        # Should contain CSV headers/data
        assert "," in result or "\n" in result

    def test_get_grid_data_parquet_format_returns_bytes(
        self, test_client, test_recipe_pk
    ):
        """Test grid data retrieval in Parquet format returns bytes."""
        result = test_client.get_grid_data(
            recipe_pk=test_recipe_pk, resp_format="parquet"
        )

        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_get_grid_data_with_dimension_filter(
        self, test_client, test_recipe_pk, test_dimension_filter
    ):
        """Test grid data retrieval with dimension filtering (levels only)."""
        result = test_client.get_grid_data(
            recipe_pk=test_recipe_pk,
            selectdimensionnodes=[test_dimension_filter],  # Wrap in list for API
            resp_format="dataframe",
        )

        assert isinstance(result, pd.DataFrame)
        # Result might be empty if filter returns no data, but should be valid DataFrame

    def test_get_grid_data_with_dimension_filter_codes(
        self, test_client, test_recipe_pk, test_dimension_filter_w_codes
    ):
        """Test grid data retrieval with dimension filtering (levels and codes)."""
        result = test_client.get_grid_data(
            recipe_pk=test_recipe_pk,
            selectdimensionnodes=[test_dimension_filter_w_codes],  # Wrap in list for API
            resp_format="dataframe",
        )

        assert isinstance(result, pd.DataFrame)
        # Result might be empty if filter returns no data, but should be valid DataFrame

    def test_get_grid_data_expanded_and_melted_options(
        self, test_client, test_recipe_pk
    ):
        """Test grid data with different expansion and melting options."""
        # Test expanded=True, melted=True (default)
        result1 = test_client.get_grid_data(
            recipe_pk=test_recipe_pk, is_expanded=True, is_melted=True
        )

        # Test expanded=False, melted=False
        result2 = test_client.get_grid_data(
            recipe_pk=test_recipe_pk, is_expanded=False, is_melted=False
        )

        assert isinstance(result1, pd.DataFrame)
        assert isinstance(result2, pd.DataFrame)

        # Results may have different shapes due to expansion/melting
        assert not result1.empty
        assert not result2.empty

    def test_get_grid_data_caching_enabled(
        self, test_client_with_cache, test_recipe_pk
    ):
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

    def test_get_grid_data_caching_with_different_params(
        self, test_client_with_cache, test_recipe_pk
    ):
        """Test that different parameters create different cache entries."""
        # Call with different parameters
        result1 = test_client_with_cache.get_grid_data(
            recipe_pk=test_recipe_pk, is_expanded=True, is_melted=True
        )

        result2 = test_client_with_cache.get_grid_data(
            recipe_pk=test_recipe_pk, is_expanded=False, is_melted=False
        )

        # Should create separate cache entries
        cache_dir = Path("test_cache")
        cache_files = list(cache_dir.glob("*.parquet"))
        assert len(cache_files) >= 2

    def test_get_grid_data_invalid_format_raises_error(
        self, test_client, test_recipe_pk
    ):
        """Test that invalid response format raises ValueError."""
        with pytest.raises(
            ValueError, match="resp_format must be 'dataframe', 'parquet', or 'csv'"
        ):
            test_client.get_grid_data(
                recipe_pk=test_recipe_pk, resp_format="invalid_format"
            )

    def test_get_grid_data_invalid_recipe_pk(self, test_client):
        """Test handling of invalid recipe primary key."""
        with pytest.raises(requests.HTTPError):
            test_client.get_grid_data(recipe_pk=99999)  # Non-existent recipe

    def test_get_grid_data_auth_error_handling(self):
        """Test authentication error handling with invalid API key."""
        client = Client(
            api_key="invalid_api_key",
            api_url="http://127.0.0.1:8001/api/v3",
            use_cache=False,
        )
        with pytest.raises(requests.HTTPError):
            client.get_grid_data(recipe_pk=1066)

    def test_get_grid_data_post_vs_get_requests(
        self,
        test_client,
        test_recipe_pk,
        test_dimension_filter,
        test_dimension_filter_w_codes,
    ):
        """Test that filtering uses POST while normal requests use GET."""
        # Normal request (should use GET)
        result1 = test_client.get_grid_data(recipe_pk=test_recipe_pk)

        # Filtered request with levels (should use POST)
        result2 = test_client.get_grid_data(
            recipe_pk=test_recipe_pk, selectdimensionnodes=[test_dimension_filter]  # Wrap in list for API
        )

        # Filtered request with levels and codes (should use POST)
        result3 = test_client.get_grid_data(
            recipe_pk=test_recipe_pk, selectdimensionnodes=[test_dimension_filter_w_codes]  # Wrap in list for API
        )

        # All should return valid DataFrames
        assert isinstance(result1, pd.DataFrame)
        assert isinstance(result2, pd.DataFrame)
        assert isinstance(result3, pd.DataFrame)

    def test_get_grid_data_empty_filter_raises_error(self, test_client, test_recipe_pk):
        """Test grid data with empty dimension filter raises appropriate error."""
        empty_filter = {"dimension": "d3", "levels": [], "codes": []}

        # Client-side validation should reject empty filters
        with pytest.raises(ValueError, match="At least one of.*must be provided"):
            test_client.get_grid_data(
                recipe_pk=test_recipe_pk, selectdimensionnodes=empty_filter
            )

    def test_get_grid_data_data_cleaning(self, test_client, test_recipe_pk):
        """Test that returned data is properly cleaned (no all-null columns)."""
        result = test_client.get_grid_data(recipe_pk=test_recipe_pk)

        # Check that no columns are all null (client should drop them)
        for column in result.columns:
            assert not result[column].isnull().all(), f"Column {column} is all null"

    def test_get_grid_data_consistent_format_across_calls(
        self, test_client, test_recipe_pk
    ):
        """Test that multiple calls return consistent data format."""
        result1 = test_client.get_grid_data(recipe_pk=test_recipe_pk)
        result2 = test_client.get_grid_data(recipe_pk=test_recipe_pk)

        # Should have same columns
        assert result1.columns.tolist() == result2.columns.tolist()

        # Should have same shape
        assert result1.shape == result2.shape

    def test_get_grid_data_multiple_dimension_filters(
        self, test_client, test_recipe_pk, test_multiple_dimension_filters
    ):
        """Test grid data retrieval with multiple dimension filters."""
        result = test_client.get_grid_data(
            recipe_pk=test_recipe_pk,
            selectdimensionnodes=test_multiple_dimension_filters,
            resp_format="dataframe",
        )

        assert isinstance(result, pd.DataFrame)
        # Result might be empty if filters return no data, but should be valid DataFrame

    def test_dimension_filter_validation_invalid_dimension(
        self, test_client, test_recipe_pk
    ):
        """Test that invalid dimension names are rejected."""
        invalid_filter = {"dimension": "invalid_dim", "levels": [1]}

        with pytest.raises(ValueError, match="Dimension must be one of"):
            test_client.get_grid_data(
                recipe_pk=test_recipe_pk, selectdimensionnodes=invalid_filter
            )

    def test_dimension_filter_validation_missing_dimension(
        self, test_client, test_recipe_pk
    ):
        """Test that filters without dimension field are rejected."""
        invalid_filter = {"levels": [1], "codes": []}

        with pytest.raises(
            ValueError, match="Dimension filter must include 'dimension' field"
        ):
            test_client.get_grid_data(
                recipe_pk=test_recipe_pk, selectdimensionnodes=invalid_filter
            )

    def test_dimension_filter_validation_no_criteria(self, test_client, test_recipe_pk):
        """Test that filters without any criteria are rejected."""
        invalid_filter = {"dimension": "d1"}

        with pytest.raises(ValueError, match="At least one of.*must be provided"):
            test_client.get_grid_data(
                recipe_pk=test_recipe_pk, selectdimensionnodes=invalid_filter
            )

    def test_dimension_filter_validation_invalid_types(
        self, test_client, test_recipe_pk
    ):
        """Test that filters with invalid field types are rejected."""
        # Test invalid codes type
        with pytest.raises(ValueError, match="'codes' must be a list"):
            test_client.get_grid_data(
                recipe_pk=test_recipe_pk,
                selectdimensionnodes={"dimension": "d1", "codes": "not_a_list"},
            )

        # Test invalid levels type
        with pytest.raises(ValueError, match="'levels' must be a list"):
            test_client.get_grid_data(
                recipe_pk=test_recipe_pk,
                selectdimensionnodes={"dimension": "d1", "levels": "not_a_list"},
            )

        # Test invalid levels content
        with pytest.raises(ValueError, match="All items in 'levels' must be integers"):
            test_client.get_grid_data(
                recipe_pk=test_recipe_pk,
                selectdimensionnodes={"dimension": "d1", "levels": ["not_int"]},
            )

        # Test invalid children type
        with pytest.raises(ValueError, match="'children' must be a boolean"):
            test_client.get_grid_data(
                recipe_pk=test_recipe_pk,
                selectdimensionnodes={"dimension": "d1", "children": "not_bool"},
            )

    def test_dimension_filter_validation_invalid_selectdimensionnodes_type(
        self, test_client, test_recipe_pk
    ):
        """Test that non-dict/list selectdimensionnodes are rejected."""
        with pytest.raises(
            ValueError, match="selectdimensionnodes must be a dict or list of dicts"
        ):
            test_client.get_grid_data(
                recipe_pk=test_recipe_pk, selectdimensionnodes="invalid_type"
            )

    def test_dimension_filter_validation_empty_list(self, test_client, test_recipe_pk):
        """Test that empty list of filters is rejected."""
        with pytest.raises(
            ValueError, match="selectdimensionnodes list cannot be empty"
        ):
            test_client.get_grid_data(recipe_pk=test_recipe_pk, selectdimensionnodes=[])

    def test_dimension_filter_validation_list_with_invalid_dict(
        self, test_client, test_recipe_pk
    ):
        """Test that list containing invalid dicts is rejected."""
        invalid_filters = [
            {"dimension": "d1", "levels": [1]},
            {"dimension": "invalid_dim", "levels": [2]},
        ]

        with pytest.raises(ValueError, match="Dimension must be one of"):
            test_client.get_grid_data(
                recipe_pk=test_recipe_pk, selectdimensionnodes=invalid_filters
            )

    def test_caching_with_multiple_filters_normalization(
        self, test_client_with_cache, test_recipe_pk, test_multiple_dimension_filters
    ):
        """Test that caching works correctly with normalized multiple filters."""
        # Use the test fixture for valid filters
        filters1 = test_multiple_dimension_filters
        # Create a reordered version for normalization testing
        filters2 = [filters1[1], filters1[0]]  # Swap order

        # First call
        result1 = test_client_with_cache.get_grid_data(
            recipe_pk=test_recipe_pk, selectdimensionnodes=filters1
        )

        # Check cache was created
        from pathlib import Path

        cache_files = list(Path("test_cache").glob("*.parquet"))
        initial_cache_count = len(cache_files)

        # Second call with normalized equivalent filters
        result2 = test_client_with_cache.get_grid_data(
            recipe_pk=test_recipe_pk, selectdimensionnodes=filters2
        )

        # Should not create new cache file (same normalized key)
        final_cache_count = len(list(Path("test_cache").glob("*.parquet")))
        assert final_cache_count == initial_cache_count

        # Results should be identical
        pd.testing.assert_frame_equal(result1, result2)

    def test_caching_different_filter_orders(
        self, test_client_with_cache, test_recipe_pk, test_multiple_dimension_filters
    ):
        """Test that different filter orders are normalized for caching."""
        # Use the test fixture for valid filters
        filters1 = test_multiple_dimension_filters
        # Create a reordered version
        filters2 = [filters1[1], filters1[0]]  # Swap order

        # First call
        result1 = test_client_with_cache.get_grid_data(
            recipe_pk=test_recipe_pk, selectdimensionnodes=filters1
        )

        # Check initial cache state
        from pathlib import Path

        cache_files = list(Path("test_cache").glob("*.parquet"))
        initial_cache_count = len(cache_files)

        # Second call with reordered filters
        result2 = test_client_with_cache.get_grid_data(
            recipe_pk=test_recipe_pk, selectdimensionnodes=filters2
        )

        # Should not create new cache file (same normalized key)
        final_cache_count = len(list(Path("test_cache").glob("*.parquet")))
        assert final_cache_count == initial_cache_count

    def test_cache_clear(self, test_client_with_cache, test_recipe_pk):
        """Test that cache.clear() removes all cached files."""
        from pathlib import Path

        cache_dir = Path("test_cache")

        # Make some cached requests
        test_client_with_cache.get_grid_data(recipe_pk=test_recipe_pk)
        test_client_with_cache.get_grid_data(
            recipe_pk=test_recipe_pk, is_expanded=True
        )

        # Verify cache files exist
        cache_files = list(cache_dir.glob("*.parquet"))
        assert len(cache_files) >= 2, "Cache files should exist"

        # Clear cache
        test_client_with_cache.cache.clear()

        # Verify all cache files are removed
        cache_files_after = list(cache_dir.glob("*.parquet"))
        assert len(cache_files_after) == 0, "All cache files should be removed"

    def test_get_grid_data_sync_mode_explicit(self, test_client, test_recipe_pk):
        """Test that synchronous mode still works when explicitly requested."""
        result = test_client.get_grid_data(
            recipe_pk=test_recipe_pk,
            use_async=False  # Explicitly use sync mode
        )

        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result.columns) > 0


class TestGetGridDataAsync:
    """Test cases for async download functionality (integration tests with dev server)."""

    def test_get_grid_data_async_basic(self, test_client, test_recipe_pk):
        """Test basic async grid data retrieval."""
        result = test_client.get_grid_data(
            recipe_pk=test_recipe_pk,
            use_async=True,
            resp_format="dataframe",
            poll_interval=0.5,  # Poll every 0.5 seconds
        )

        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result.columns) > 0

    def test_get_grid_data_async_with_filters(
        self, test_client, test_recipe_pk, test_multiple_dimension_filters
    ):
        """Test async download with dimension filters (POST request)."""
        result = test_client.get_grid_data(
            recipe_pk=test_recipe_pk,
            selectdimensionnodes=test_multiple_dimension_filters,
            use_async=True,
            resp_format="dataframe",
            poll_interval=0.5,
        )

        assert isinstance(result, pd.DataFrame)

    def test_get_grid_data_async_csv_format(self, test_client, test_recipe_pk):
        """Test async download returns CSV format correctly."""
        result = test_client.get_grid_data(
            recipe_pk=test_recipe_pk,
            use_async=True,
            resp_format="csv",
            poll_interval=0.5,
        )

        assert isinstance(result, str)
        assert len(result) > 0
        assert "," in result or "\n" in result

    def test_get_grid_data_async_parquet_format(self, test_client, test_recipe_pk):
        """Test async download returns Parquet format correctly."""
        result = test_client.get_grid_data(
            recipe_pk=test_recipe_pk,
            use_async=True,
            resp_format="parquet",
            poll_interval=0.5,
        )

        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_get_grid_data_async_vs_sync_same_result(self, test_client, test_recipe_pk):
        """Test that async and sync modes return the same data."""
        # Get data synchronously
        sync_result = test_client.get_grid_data(
            recipe_pk=test_recipe_pk,
            use_async=False,
            resp_format="dataframe",
        )

        # Get data asynchronously
        async_result = test_client.get_grid_data(
            recipe_pk=test_recipe_pk,
            use_async=True,
            resp_format="dataframe",
            poll_interval=0.5,
        )

        # Results should be identical
        pd.testing.assert_frame_equal(sync_result, async_result)

    def test_get_grid_data_async_with_caching(
        self, test_client_with_cache, test_recipe_pk
    ):
        """Test that async downloads are properly cached."""
        # First call with async
        result1 = test_client_with_cache.get_grid_data(
            recipe_pk=test_recipe_pk,
            use_async=True,
            poll_interval=0.5,
        )

        # Check cache was created
        from pathlib import Path

        cache_dir = Path("test_cache")
        cache_files = list(cache_dir.glob("*.parquet"))
        assert len(cache_files) > 0

        # Second call should load from cache (much faster, no async download)
        result2 = test_client_with_cache.get_grid_data(
            recipe_pk=test_recipe_pk,
            use_async=True,
            poll_interval=0.5,
        )

        # Results should be identical
        pd.testing.assert_frame_equal(result1, result2)
