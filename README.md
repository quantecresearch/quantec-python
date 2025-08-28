# Quantec

A Python package for working with Quantec EasyData API. Fetch economic time series data with support for caching and advanced filtering.

> **⚠️ Early Development Notice**: This package is in early development and may undergo breaking changes without backwards compatibility until version 1.0 is reached.

## Features

- 🎯 **Multiple Data Access**: Time series codes, selections, and grid/pivot data
- 📊 **Format Support**: CSV for time series, CSV/Parquet for grid data
- 📈 **Multiple Frequencies**: Monthly (M), Quarterly (Q), and Annual (A) frequencies
- 🔍 **Advanced Filtering**: Dimension-based filtering for grid data
- ⚡ **Performance Caching**: Optional caching for grid data
- 🛡️ **Error Handling**: Comprehensive network and API error handling
- 🔧 **Flexible Configuration**: Environment variables and parameter setup

## Installation

```bash
pip install quantec
```

## Quick Start

```python
from quantec.easydata.client import Client

# Initialize client
client = Client()

# Get time series data (CSV format only)
data = client.get_data(time_series_codes="NMS-EC_BUS,NMS-GA_BUS")
print(data.head())
```

## Configuration

### Environment Variables

```bash
export EASYDATA_API_KEY="your-api-key-here"
export EASYDATA_API_URL="https://www.easydata.co.za/api/v3/"
```

### Client Options

```python
from quantec.easydata.client import Client

# Basic client (uses environment variables)
client = Client()

# With caching enabled
client = Client(
    use_cache=True,          # Enable caching for grid data
    cache_dir="./cache"      # Cache directory path
)
```

## Time Series Data

### Direct Access with Codes

```python
# Single time series
data = client.get_data(time_series_codes="NMS-EC_BUS")

# Multiple time series
data = client.get_data(time_series_codes="NMS-EC_BUS,NMS-GA_BUS")

# With date filtering and frequency
data = client.get_data(
    time_series_codes="NMS-EC_BUS,NMS-GA_BUS",
    freq="Q",           # Quarterly data
    start_year="2020",  # Year format only
    end_year="2023"
)
```

### Discovery-Based Access

```python
# Find available selections
selections = client.get_selections(status="PSO")  # Private, Shared, Open

# Extract selections list (format depends on response)
if isinstance(selections, dict) and "selections" in selections:
    selections_list = selections["selections"]
else:
    selections_list = selections

# Use selection for data retrieval
if len(selections_list) > 0:
    selection = selections_list[0]
    data = client.get_data(selection_pk=selection['pk'])
```

## Grid/Pivot Data

### Basic Grid Data Access

```python
# Get available recipes
recipes = client.get_recipes()

# Basic grid data retrieval
if len(recipes) > 0:
    recipe_id = recipes[0]['id']
    grid_data = client.get_grid_data(recipe_pk=recipe_id)
```

### Grid Data with Filtering

```python
# Filter by dimension levels only
filters = {"dimension": "d3", "levels": [2], "codes": []}
grid_data = client.get_grid_data(recipe_pk=53, selectdimensionnodes=filters)

# Filter by levels and specific codes
filters = {"dimension": "d3", "levels": [1], "codes": ["TRD01-R_FI"]}
grid_data = client.get_grid_data(recipe_pk=53, selectdimensionnodes=filters)
```

### Format Options (CSV/Parquet only)

```python
# CSV format (default)
csv_data = client.get_grid_data(recipe_pk=53, resp_format="csv")

# Parquet format (recommended for large datasets)
parquet_data = client.get_grid_data(recipe_pk=53, resp_format="parquet")
```

## Caching (Grid Data Only)

```python
# Initialize client with caching
cached_client = Client(use_cache=True, cache_dir="./cache")

# First call - fetches from API and caches
grid_data = cached_client.get_grid_data(recipe_pk=53)

# Subsequent calls - loads from cache (faster)
grid_data = cached_client.get_grid_data(recipe_pk=53)
```

## Error Handling

```python
import requests
from quantec.easydata.client import Client

client = Client()

try:
    data = client.get_data(time_series_codes="INVALID_CODE")
except requests.HTTPError as e:
    print(f"API Error: {e}")
except ValueError as e:
    print(f"Parameter Error: {e}")
```

## Complete Example

```python
from quantec.easydata.client import Client

# Initialize client with caching
client = Client(use_cache=True, cache_dir="./cache")

# 1. Get time series data
ts_data = client.get_data(
    time_series_codes="NMS-EC_BUS,NMS-GA_BUS",
    freq="Q",
    start_year="2020"
)

# 2. Get available recipes and grid data
recipes = client.get_recipes()
if len(recipes) > 0:
    grid_data = client.get_grid_data(
        recipe_pk=recipes[0]['id'],
        resp_format="parquet"
    )

# 3. Get selections for discovery
selections = client.get_selections(status="PSO")
```

## Important Notes

- **Time series data**: Only supports CSV format
- **Grid data**: Only supports CSV and Parquet formats (no JSON)
- **Date parameters**: Use year format only (e.g., "2020", not "2020-01-01")
- **Caching**: Only available for grid data
- **Dimension filtering**: Must provide at least one of: codes, levels, children, or children_include_self

## License

MIT License - see the LICENSE file for details.

## Support

For support, contact [Quantec](https://www.quantec.co.za/contact/)
