import pytest
import pandas as pd
import requests
from quantec.easydata.client import Client


class TestGetSelections:
    """Test cases for the get_selections endpoint."""

    def test_get_selections_no_filters(self, test_client):
        """Test getting selections without any filters."""
        result = test_client.get_selections()
        
        assert result is not None
        
        if isinstance(result, dict):
            assert "selections" in result
            selections_list = result["selections"]
        else:
            selections_list = result.to_dict("records") if isinstance(result, pd.DataFrame) else result
        
        # Should return a list (might be empty)
        assert isinstance(selections_list, list)

    def test_get_selections_with_status_filter(self, test_client):
        """Test getting selections with status filter."""
        result = test_client.get_selections(status="PSO")
        
        assert result is not None
        
        if isinstance(result, dict):
            selections_list = result["selections"]
        elif isinstance(result, pd.DataFrame):
            selections_list = result.to_dict("records")
        else:
            selections_list = result
        
        # Each selection should have required fields
        for selection in selections_list:
            assert "pk" in selection
            assert "title" in selection
            assert "code_count" in selection
            assert "is_owner" in selection
            assert "owner" in selection
            assert "status" in selection

    def test_get_selections_private_only(self, test_client):
        """Test getting private selections only."""
        result = test_client.get_selections(status="P")
        
        assert result is not None
        
        if isinstance(result, dict):
            selections_list = result["selections"]
        elif isinstance(result, pd.DataFrame):
            selections_list = result.to_dict("records")
        else:
            selections_list = result
        
        # All returned selections should be private (status contains 'P')
        for selection in selections_list:
            assert selection["status"] in ["P", "Private"]  # API may return different formats

    def test_get_selections_shared_only(self, test_client):
        """Test getting shared selections only."""
        result = test_client.get_selections(status="S")
        
        assert result is not None
        
        if isinstance(result, dict):
            selections_list = result["selections"]
        elif isinstance(result, pd.DataFrame):
            selections_list = result.to_dict("records")
        else:
            selections_list = result

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
        
        if isinstance(result, dict):
            selections_list = result["selections"]
        elif isinstance(result, pd.DataFrame):
            selections_list = result.to_dict("records")
        else:
            selections_list = result
        
        # Check structure of first selection (if any exist)
        if len(selections_list) > 0:
            first_selection = selections_list[0]
            
            # Check for required transformed fields from the client
            required_fields = ["item", "pk", "title", "code_count", "is_owner", "owner", "status"]
            for field in required_fields:
                assert field in first_selection, f"Missing field: {field}"
            
            # Validate field types
            assert isinstance(first_selection["pk"], int)
            assert isinstance(first_selection["title"], str)
            assert isinstance(first_selection["code_count"], int)
            assert isinstance(first_selection["is_owner"], bool)

    def test_get_selections_csv_vs_json_format(self, test_client, test_client_csv):
        """Test that CSV and JSON formats return equivalent data."""
        json_result = test_client.get_selections(status="PSO")
        csv_result = test_client_csv.get_selections(status="PSO")
        
        # JSON returns dict with 'selections' key, CSV returns DataFrame
        if isinstance(json_result, dict):
            json_selections = json_result["selections"]
        else:
            json_selections = json_result
        
        csv_selections = csv_result.to_dict("records") if isinstance(csv_result, pd.DataFrame) else csv_result
        
        # Should have same number of selections
        assert len(json_selections) == len(csv_selections)


    def test_get_selections_auth_error_handling(self):
        """Test authentication error handling with invalid API key."""
        client = Client(
            apikey="invalid_api_key",
            api_url="http://127.0.0.1:8001"
        )
        
        with pytest.raises(requests.HTTPError):
            client.get_selections()