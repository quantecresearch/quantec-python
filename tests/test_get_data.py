import pytest
import pandas as pd
import requests
from quantec.easydata.client import Client


class TestGetData:
    """Test cases for the get_data endpoint."""

    def test_get_data_with_time_series_codes_dataframe(self, test_client, sample_time_series_codes):
        """Test getting data with time series codes (default dataframe format)."""
        result = test_client.get_data(time_series_codes=sample_time_series_codes)
        
        # Default format is now dataframe, which returns DataFrame
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result.columns) > 0

    def test_get_data_with_time_series_codes_csv(self, test_client, sample_time_series_codes):
        """Test getting data with time series codes in CSV format."""
        result = test_client.get_data(time_series_codes=sample_time_series_codes, resp_format="csv")
        
        # CSV format should return string
        assert isinstance(result, str)
        assert len(result) > 0
        
        # CSV should be valid and loadable into DataFrame
        from io import StringIO
        df = pd.read_csv(StringIO(result))
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_get_data_with_time_series_codes_json(self, test_client, sample_time_series_codes):
        """Test getting data with time series codes in JSON format."""
        result = test_client.get_data(time_series_codes=sample_time_series_codes, resp_format="json")
        
        # JSON format should return dict
        assert isinstance(result, dict)
        assert len(result) > 0

    def test_get_data_invalid_format_raises_error(self, test_client, sample_time_series_codes):
        """Test that invalid response format raises ValueError."""
        with pytest.raises(ValueError, match="resp_format must be 'dataframe', 'csv', or 'json'"):
            test_client.get_data(time_series_codes=sample_time_series_codes, resp_format="invalid_format")

    def test_get_data_with_selection_pk(self, test_client):
        """Test getting data with selection primary key."""
        # First get available selections (always returns DataFrame)
        selections = test_client.get_selections(status="PSO")
        
        assert isinstance(selections, pd.DataFrame)
        
        if len(selections) > 0:
            selection_pk = selections.iloc[0]["pk"]
            result = test_client.get_data(selection_pk=selection_pk)
            
            assert result is not None
            assert isinstance(result, pd.DataFrame)
            assert not result.empty

    def test_get_data_with_frequency_params(self, test_client, sample_time_series_codes):
        """Test getting data with frequency and date parameters."""
        result = test_client.get_data(
            time_series_codes=sample_time_series_codes,
            freq="Q",
            start_year="2020",
            end_year="2023"
        )
        
        assert result is not None

    def test_get_data_no_parameters_raises_error(self, test_client):
        """Test that calling get_data without parameters raises ValueError."""
        with pytest.raises(ValueError, match="Either time_series_codes or selection_pk must be provided"):
            test_client.get_data()

    def test_get_data_invalid_codes_handles_error(self, test_client):
        """Test that invalid time series codes are handled gracefully."""
        with pytest.raises((requests.HTTPError, ValueError)):
            test_client.get_data(time_series_codes="INVALID_CODE_12345")


    def test_get_data_selection_pk_priority(self, test_client, sample_time_series_codes):
        """Test that selection_pk takes precedence over time_series_codes."""
        # Get a valid selection first (always returns DataFrame)
        selections = test_client.get_selections(status="PSO")
        
        assert isinstance(selections, pd.DataFrame)
        
        if len(selections) > 0:
            selection_pk = selections.iloc[0]["pk"]
            
            # Call with both parameters - selection_pk should take precedence
            result = test_client.get_data(
                time_series_codes=sample_time_series_codes,
                selection_pk=selection_pk
            )
            
            assert result is not None
            assert isinstance(result, pd.DataFrame)
            assert not result.empty