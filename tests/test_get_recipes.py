import pytest
import pandas as pd
import requests
from quantec.easydata.client import Client


class TestGetRecipes:
    """Test cases for the get_recipes endpoint."""

    def test_get_recipes_json_format(self, test_client):
        """Test getting recipes (default CSV format returns DataFrame)."""
        result = test_client.get_recipes()
        
        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert len(result.columns) > 0

    def test_get_recipes_csv_format(self, test_client_csv):
        """Test getting recipes in CSV format (returns DataFrame)."""
        result = test_client_csv.get_recipes()
        
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        
        # Check for expected columns that recipes should have
        expected_columns = ["id"]  # At minimum, recipes should have an ID
        for col in expected_columns:
            assert col in result.columns

    def test_get_recipes_contains_test_recipe(self, test_client, test_recipe_pk):
        """Test that the TRD01 test recipe (pk=1066) is in the results."""
        result = test_client.get_recipes()
        
        # Always returns DataFrame now
        assert isinstance(result, pd.DataFrame)
        
        # Look for our test recipe
        recipe_ids = result["id"].tolist()
        assert test_recipe_pk in recipe_ids, f"Test recipe {test_recipe_pk} not found in recipes"

    def test_get_recipes_structure(self, test_client):
        """Test the structure of recipe objects."""
        result = test_client.get_recipes()
        
        # Always returns DataFrame now
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        
        # Check for common recipe fields
        assert "id" in result.columns
        # Other fields may vary based on API implementation


    def test_get_recipes_auth_error_handling(self):
        """Test authentication error handling with invalid API key."""
        client = Client(
            api_key="invalid_api_key",
            api_url="http://127.0.0.1:8001/api/v3"
        )
        
        with pytest.raises(requests.HTTPError):
            client.get_recipes()

    def test_get_recipes_consistent_response_format(self, test_client):
        """Test that recipes response format is consistent across multiple calls."""
        result1 = test_client.get_recipes()
        result2 = test_client.get_recipes()
        
        # Always returns DataFrame now
        assert isinstance(result1, pd.DataFrame)
        assert isinstance(result2, pd.DataFrame)
        assert len(result1) == len(result2)
        assert result1.columns.tolist() == result2.columns.tolist()