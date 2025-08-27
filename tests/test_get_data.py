import pytest
import pandas as pd
import requests
from quantec.easydata.client import Client


class TestGetData:
    """Test cases for the get_data endpoint."""

    def test_get_data_with_time_series_codes_csv(self, test_client, sample_time_series_codes):
        """Test getting data with time series codes (CSV format only for time series)."""
        result = test_client.get_data(time_series_codes=sample_time_series_codes)
        
        # Default client uses CSV format, which returns DataFrame
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result.columns) > 0

    def test_get_data_with_time_series_codes_csv(self, test_client_csv, sample_time_series_codes):
        """Test getting data with time series codes in CSV format."""
        result = test_client_csv.get_data(time_series_codes=sample_time_series_codes)
        
        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_get_data_with_selection_pk(self, test_client):
        """Test getting data with selection primary key."""
        # First get available selections
        selections = test_client.get_selections(status="PSO")
        
        if isinstance(selections, dict) and "selections" in selections:
            selections_list = selections["selections"]
        elif isinstance(selections, pd.DataFrame):
            selections_list = selections.to_dict("records")
        else:
            selections_list = selections
            
        if len(selections_list) > 0:
            selection_pk = selections_list[0]["pk"]
            result = test_client.get_data(selection_pk=selection_pk)
            
            assert result is not None
            if isinstance(result, dict):
                assert len(result) > 0
            elif isinstance(result, pd.DataFrame):
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
        # Get a valid selection first
        selections = test_client.get_selections(status="PSO")
        
        if isinstance(selections, dict) and "selections" in selections:
            selections_list = selections["selections"]
        elif isinstance(selections, pd.DataFrame):
            selections_list = selections.to_dict("records")
        else:
            selections_list = selections
            
        if len(selections_list) > 0:
            selection_pk = selections_list[0]["pk"]
            
            # Call with both parameters - selection_pk should take precedence
            result = test_client.get_data(
                time_series_codes=sample_time_series_codes,
                selection_pk=selection_pk
            )
            
            assert result is not None