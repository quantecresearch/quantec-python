import pytest
import pandas as pd
import requests
from quantec.easydata.client import Client


class TestGetSelections:
    """Test cases for the get_selections endpoint."""

    def test_get_selections_no_filters(self, test_client):
        """Test getting selections without any filters."""
        result = test_client.get_selections()
        
        # Always returns DataFrame now
        assert isinstance(result, pd.DataFrame)

    def test_get_selections_with_status_filter(self, test_client):
        """Test getting selections with status filter."""
        result = test_client.get_selections(status="PSO")
        
        # Always returns DataFrame now
        assert isinstance(result, pd.DataFrame)
        
        # Check for required columns
        required_columns = ["pk", "title", "code_count", "is_owner", "owner", "status"]
        for column in required_columns:
            assert column in result.columns


    def test_get_selections_shared_only(self, test_client):
        """Test getting shared selections only."""
        result = test_client.get_selections(status="S")
        
        # Always returns DataFrame now
        assert isinstance(result, pd.DataFrame)

    def test_get_selections_open_only(self, test_client):
        """Test getting open selections only."""
        result = test_client.get_selections(status="O")
        
        assert result is not None

    def test_get_selections_with_show_parameter(self, test_client):
        """Test getting selections with show parameter."""
        result = test_client.get_selections(show="shared")
        
        assert result is not None

    def test_get_selections_with_filter_parameter(self, test_client):
        """Test getting selections with filter parameter."""
        result = test_client.get_selections(filter="active")
        
        assert result is not None

    def test_get_selections_combined_parameters(self, test_client):
        """Test getting selections with multiple parameters."""
        result = test_client.get_selections(
            status="PSO",
            show="shared",
            filter="active"
        )
        
        assert result is not None

    def test_get_selections_response_structure(self, test_client):
        """Test the structure of selections response."""
        result = test_client.get_selections(status="PSO")
        
        # Always returns DataFrame now
        assert isinstance(result, pd.DataFrame)
        
        # Check for required transformed fields from the client
        required_fields = ["item", "pk", "title", "code_count", "is_owner", "owner", "status"]
        for field in required_fields:
            assert field in result.columns, f"Missing field: {field}"
        
        # Validate field types (if we have data)
        if len(result) > 0:
            assert result["pk"].dtype in ["int64", "Int64"]
            assert result["title"].dtype == "object"
            assert result["code_count"].dtype in ["int64", "Int64"]
            assert result["is_owner"].dtype == "bool"

    def test_get_selections_consistent_format(self, test_client, test_client_csv):
        """Test that both clients return consistent DataFrame format."""
        result1 = test_client.get_selections(status="PSO")
        result2 = test_client_csv.get_selections(status="PSO")
        
        # Both always return DataFrame now
        assert isinstance(result1, pd.DataFrame)
        assert isinstance(result2, pd.DataFrame)
        
        # Should have same number of selections and columns
        assert len(result1) == len(result2)
        assert result1.columns.tolist() == result2.columns.tolist()


    def test_get_selections_auth_error_handling(self):
        """Test authentication error handling with invalid API key."""
        client = Client(
            api_key="invalid_api_key",
            api_url="http://127.0.0.1:8001/api/v3"
        )
        
        with pytest.raises(requests.HTTPError):
            client.get_selections()