# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Quantec is a Python package for working with Quantec EasyData API. It provides a client interface for fetching financial/economic time series data with specific format support per endpoint type.

## Architecture

The project follows a simple modular structure:

- `quantec/__init__.py` - Package version definition
- `quantec/easydata/client.py` - Main API client implementation
- `quantec/core/` - Core functionality (charts, LLM processing) - referenced in README but not yet implemented

## Key Components

### EasyData Client (`quantec/easydata/client.py`)
- Main `Client` class for API interactions with four endpoints:
  - `get_data()` - Time series data via time series codes or selection primary keys
  - `get_recipes()` - List available recipes for grid data
  - `get_selections()` - List user's available selections for time series data
  - `get_grid_data()` - Grid/pivot table data using recipe primary keys
- **Format support varies by endpoint**: 
  - Time series data (`get_data`): CSV only
  - Grid data (`get_grid_data`): CSV and Parquet only  
  - Other endpoints: JSON responses converted to DataFrame
- Optional caching system for grid data with hash-based filenames and automatic cache management
- Advanced filtering via `selectdimensionnodes` parameter for dimension-based grid data filtering
- Uses environment variables for configuration (EASYDATA_API_KEY, EASYDATA_API_URL)
- Comprehensive error handling for network, HTTP, and parsing errors
- POST/GET request handling - POST for filtered grid data, GET for standard requests

### Typical Workflow Patterns:

1. **Direct time series access**:
   ```python
   client = Client()
   data = client.get_data(time_series_codes="code1,code2")
   ```

2. **Discovery-based time series access**:
   ```python
   selections = client.get_selections(status="PSO")  # Private, Shared, Open
   data = client.get_data(selection_pk=selections[0]['pk'])
   ```

3. **Grid/pivot data access**:
   ```python
   recipes = client.get_recipes()
   grid = client.get_grid_data(recipe_pk=recipes[0]['id'])
   ```

4. **Advanced grid data with filtering**:
   ```python
   # Filter by levels only
   filters = {"dimension": "d3", "levels": [2], "codes": []}
   grid = client.get_grid_data(recipe_pk=53, selectdimensionnodes=filters)
   
   # Filter by levels and specific codes
   filters = {"dimension": "d3", "levels": [1], "codes": ["TRD01-R_FI"]}
   grid = client.get_grid_data(recipe_pk=53, selectdimensionnodes=filters)
   ```

## Development Commands

This project uses Python's modern packaging with `pyproject.toml` and hatchling build backend. **Use `uv` for development**:

- **Setup development environment**: `uv sync`
- **Install in development mode**: `uv pip install -e .`
- **Run scripts**: `uv run <command>`
- **Add dependencies**: `uv add <package>`
- **Add dev dependencies**: `uv add --dev <package>`
- **Build package**: `uv build`
- **Run tests**: `uv run pytest`
- **Run tests with coverage**: `uv run pytest --cov=quantec --cov-report=html`
- **Run specific test file**: `uv run pytest tests/test_get_data.py`
- **Run tests verbosely**: `uv run pytest -v`

## Environment Configuration

Required environment variables:
- `EASYDATA_API_KEY` - API authentication key
- `EASYDATA_API_URL` - Base URL for the Quantec API (defaults to https://www.easydata.co.za/api/v3/ if not set)

## Testing

The project includes comprehensive tests (39 total) for all API endpoints using pytest. Test configuration is in `pytest.ini` with environment variables for the local dev backend:

- **Test environment**: Uses local dev backend at `http://127.0.0.1:8001/api/v3`
- **Test dataset**: TRD01 with `recipe_pk=53`  
- **Test data**: Time series codes `NMS-EC_BUS,NMS-GA_BUS`
- **Coverage reporting**: HTML reports generated in `htmlcov/`
- **Test structure**:
  - `test_get_data.py` - Time series data endpoint tests (CSV only)
  - `test_get_recipes.py` - Recipe listing endpoint tests
  - `test_get_selections.py` - Selection listing endpoint tests
  - `test_get_grid_data.py` - Grid/pivot data endpoint tests (includes caching, filtering, format restrictions)
- **Key test coverage**: Dimension filtering with both levels-only and levels+codes variants, caching system, POST/GET request patterns, format restrictions, error validation

## Dependencies

Core dependencies (from pyproject.toml):
- numpy>=2.2.6
- pandas>=2.2.3
- pyarrow>=18.0.0 (for parquet support)
- python-dotenv>=1.0.0
- requests>=2.28.0

Requires Python >=3.10

## Important API Constraints

When working with this client, note these key restrictions:

- **Time series data**: Only supports CSV format, returns pandas DataFrame
- **Grid data**: Only supports CSV and Parquet formats (no JSON), returns pandas DataFrame  
- **Dimension filtering**: Must provide at least one of: codes, levels, children, or children_include_self
- **Analysis parameter**: Cannot be used with time series codes (only with selection_pk)
- **Date parameters**: start_year/end_year should be year only (e.g., "2020", not "2020-01-01")
- **Caching**: Only available for grid data, uses hash-based filenames
- Default production API URL is `https://www.easydata.co.za/api/v3/`
- We target to use the latest available Python version using uv, currently 3.13
- When using uv sync, remember to do: `uv sync --extra dev`