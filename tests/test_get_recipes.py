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
        """Test that the TRD01 test recipe (pk=53) is in the results."""
        result = test_client.get_recipes()
        
        # Handle both dict and DataFrame responses
        if isinstance(result, dict):
            # Convert to list if it's a dict response
            recipes_list = result if isinstance(result, list) else [result]
        else:
            recipes_list = result.to_dict("records")
        
        # Look for our test recipe
        recipe_ids = [recipe.get("id") for recipe in recipes_list]
        assert test_recipe_pk in recipe_ids, f"Test recipe {test_recipe_pk} not found in recipes"

    def test_get_recipes_structure(self, test_client):
        """Test the structure of recipe objects."""
        result = test_client.get_recipes()
        
        if isinstance(result, dict) and len(result) > 0:
            # Get first recipe to check structure
            if isinstance(result, list):
                first_recipe = result[0]
            else:
                # If it's a dict with recipes as values
                first_recipe = next(iter(result.values())) if result else {}
            
            # Check for common recipe fields
            assert "id" in first_recipe
            # Other fields may vary based on API implementation


    def test_get_recipes_auth_error_handling(self):
        """Test authentication error handling with invalid API key."""
        client = Client(
            apikey="invalid_api_key",
            api_url="http://127.0.0.1:8001"
        )
        
        with pytest.raises(requests.HTTPError):
            client.get_recipes()

    def test_get_recipes_consistent_response_format(self, test_client):
        """Test that recipes response format is consistent across multiple calls."""
        result1 = test_client.get_recipes()
        result2 = test_client.get_recipes()
        
        assert type(result1) == type(result2)
        
        if isinstance(result1, dict):
            assert len(result1) == len(result2)
        elif isinstance(result1, pd.DataFrame):
            assert len(result1) == len(result2)
            assert result1.columns.tolist() == result2.columns.tolist()