# Quantec

A Python package for working with Quantec EasyData API. Fetch economic time series data with support for caching and advanced filtering.

> **⚠️ Early Development Notice**: This package is in early development and may undergo breaking changes without backwards compatibility until version 1.0 is reached.

The Quantec EasyData API is available to EasyData subscribers. To subscribe and get API access, visit [quantec.co.za/easydata](https://www.quantec.co.za/easydata/) for more information.

## Features

- 🎯 **Multiple Data Access**: Time series codes, selections, and grid/pivot data
- 📊 **Format Support**: CSV for time series, CSV/Parquet for grid data
- 📈 **Multiple Frequencies**: Monthly (M), Quarterly (Q), and Annual (A) frequencies
- 🔍 **Advanced Filtering**: Dimension-based filtering for grid data
- ⚡ **Performance Caching**: Optional caching for time series and grid data
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

Mac/Linux (bash/zsh):

```bash
export EASYDATA_API_KEY="your-api-key-here"
export EASYDATA_API_URL="https://www.easydata.co.za/api/v3/"
```

On Windows (PowerShell):

```powershell
$env:EASYDATA_API_KEY = "your-api-key-here"
$env:EASYDATA_API_URL = "https://www.easydata.co.za/api/v3/"
```

### Client Options

```python
from quantec.easydata.client import Client

# Basic client (uses environment variables)
client = Client()

```

```python
# All parameters and defaults
client = Client(
    api_key=None,  # default: uses EASYDATA_API_KEY env var
    api_url=None,  # default: EASYDATA_API_URL or "https://www.easydata.co.za/api/v3"
    use_cache=True,
    cache_dir="cache",
)
```

## Time Series Data

### Main Methods

- get_data: Fetch time series or selection data.
  - Parameters:
    - `time_series_codes: Optional[str] = None` - Comma-separated time series codes
    - `selection_pk: Optional[int] = None` - Selection primary key (takes precedence over codes)
    - `freq: str = "A"` - Data frequency ("M", "Q", "A")
    - `start_year: str = ""` - Start year (yyyy)
    - `end_year: str = ""` - End year (yyyy)
    - `analysis: bool = False` - Include analysis (selection_pk only)
    - `resp_format: str = "dataframe"` - Response format ("dataframe", "csv", "json")
    - `is_tidy: bool = True` - Return tidy data
  - Returns: DataFrame when `resp_format="dataframe"`, CSV string when `"csv"`, dict when `"json"`.

- get_selections: Fetch user selections.
  - Parameters:
    - `status: Optional[str] = None` - Status flags: U=Unsaved, P=Private, S=Shared, O=Open, W=Owner (e.g., "PSW" for Private and Shared selections owned by user)
  - Returns: DataFrame of selections.

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

# Different return formats
df_data = client.get_data(time_series_codes="NMS-EC_BUS", resp_format="dataframe")  # Default
csv_data = client.get_data(time_series_codes="NMS-EC_BUS", resp_format="csv")       # Raw CSV string
json_data = client.get_data(time_series_codes="NMS-EC_BUS", resp_format="json")     # JSON dict
```


### Discovery-Based Access

```python
# Find available selections
selections = client.get_selections(status="PSO")  # Private, Shared, Open
# Or get only private and shared selections owned by user
selections = client.get_selections(status="PSW")  # Private, Shared, Owner

# Use selection for data retrieval (returns DataFrame)
if len(selections) > 0:
    selection_pk = selections.iloc[0]['pk']
    data = client.get_data(selection_pk=selection_pk)
```

## Grid/Pivot Data

### Main Methods

- get_recipes: Fetch available recipes.
  - Parameters: none.
  - Returns: DataFrame of recipes.

- get_grid_data: Fetch grid/pivot data by recipe.
  - Parameters:
    - `recipe_pk: int` - Recipe primary key identifier
    - `is_expanded: bool = False` - Return expanded data format
    - `is_melted: bool = True` - Return melted data format
    - `resp_format: str = "dataframe"` - Response format ("dataframe", "csv", "parquet")
    - `selectdimensionnodes: dict | list[dict] | None = None` - Dimension filtering
    - `has_tscodes: bool = False` - Include time series codes
    - `has_dncodes: bool = False` - Include dimension node codes
    - `is_sparse: bool = True` - Return sparse data format
    - `freq: Optional[str] = None` - Data frequency ("M", "Q", "A")
  - Returns: DataFrame when `resp_format="dataframe"`, CSV string when `"csv"`, bytes when `"parquet"`.

### Basic Grid Data Access

```python
# Get available recipes
recipes = client.get_recipes()

# Basic grid data retrieval
if len(recipes) > 0:
    recipe_id = recipes.iloc[0]['id']
    grid_data = client.get_grid_data(recipe_pk=recipe_id)
    
# Grid data with frequency
if len(recipes) > 0:
    recipe_id = recipes.iloc[0]['id']
    grid_data = client.get_grid_data(
        recipe_pk=recipe_id,
        freq="Q"  # Quarterly data
    )
```

### Grid Data with Filtering

```python
# Single dimension filter by levels only
filters = {"dimension": "d3", "levels": [2]}
grid_data = client.get_grid_data(recipe_pk=1066, selectdimensionnodes=filters)

# Single dimension filter by codes only
filters = {"dimension": "d3", "codes": ["TRD01-R_FI"]}
grid_data = client.get_grid_data(recipe_pk=1066, selectdimensionnodes=filters)

# Single dimension filter combining codes and levels
filters = {"dimension": "d3", "levels": [1], "codes": ["TRD01-R_FI"]}
grid_data = client.get_grid_data(recipe_pk=1066, selectdimensionnodes=filters)

# Multiple dimension filters
filters = [
    {"dimension": "d1", "codes": ["CODE1", "CODE2"]},
    {"dimension": "d3", "levels": [2]},
    {"dimension": "d2", "codes": ["PARENT_CODE"], "children": True}
]
grid_data = client.get_grid_data(recipe_pk=1066, selectdimensionnodes=filters)

# Single code with children
filters = {"dimension": "d3", "codes": ["TRD01-R_FI"], "children": True}
grid_data = client.get_grid_data(recipe_pk=1066, selectdimensionnodes=filters)

# Single code with children including self
filters = {"dimension": "d3", "codes": ["TRD01-R_FI"], "children_include_self": True}
grid_data = client.get_grid_data(recipe_pk=1066, selectdimensionnodes=filters)
```

#### Supported Filter Combinations

These are the valid filter combinations for each dimension:

1. **Codes only**: `{"dimension": "d1", "codes": ["CODE1", "CODE2", ...]}`
2. **Levels only**: `{"dimension": "d1", "levels": [1, 2, ...]}`
3. **Codes and levels**: `{"dimension": "d1", "codes": ["CODE1"], "levels": [1, 2]}`
4. **Single code with children**: `{"dimension": "d1", "codes": ["PARENT_CODE"], "children": True}`
5. **Single code with children including self**: `{"dimension": "d1", "codes": ["PARENT_CODE"], "children_include_self": True}`

**Important constraints:**
- `children` and `children_include_self` require exactly one code
- `children` and `children_include_self` cannot be used together
- `children` and `children_include_self` cannot be combined with `levels`
- Valid dimensions: "d1", "d2", "d3", "d4", "d5", "d6", "d7"

### Format Options (CSV/Parquet only)

```python
# DataFrame format (default)
df_data = client.get_grid_data(recipe_pk=1066)

# CSV format 
csv_data = client.get_grid_data(recipe_pk=1066, resp_format="csv")

# Parquet format (recommended for large datasets)
parquet_data = client.get_grid_data(recipe_pk=1066, resp_format="parquet")
```

## Caching

Caching is **enabled by default** to improve performance. The client automatically caches time series and grid data responses.

```python
# Caching is enabled by default
client = Client()  # use_cache=True by default

# Time series caching
ts1 = client.get_data(time_series_codes="NMS-EC_BUS")     # fetch + cache
ts2 = client.get_data(time_series_codes="NMS-EC_BUS")     # loaded from cache

# Grid data caching
grid1 = client.get_grid_data(recipe_pk=1066)               # fetch + cache
grid2 = client.get_grid_data(recipe_pk=1066)               # loaded from cache

# Clear cache
client.cache.clear()  # Remove all cached files

# Disable caching if needed
no_cache_client = Client(use_cache=False)
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

# 2. Get selections for discovery
selections = client.get_selections(status="PSO")  # Private, Shared, Open
# selections.head()

selection_pk = int(selections.loc[selections.title == "CPI overview", "pk"].iloc[0])

selection_data = client.get_data(
    selection_pk=selection_pk,
    freq="M"
)

# 3. Get available recipes and grid data for `TRD01` data set
recipes = client.get_recipes()
# recipes.head()

recipe_pk = int(recipes.loc[recipes.dataset_code == "TRD01", "id"].iloc[0])

grid_data = client.get_grid_data(
    recipe_pk=recipes.iloc[0]['id'],
)

```

## Important Notes

- **Time series data**: Supports DataFrame (default), CSV and JSON formats
- **Grid data**: Supports DataFrame (default), CSV and Parquet formats
- **Date parameters**: Use year format only (e.g., "2020", not "2020-01-01")
- **Caching**: Available for time series and grid data by default
- **Dimension filtering**: Must provide at least one of: codes, levels, children, or children_include_self. May provide a list of filters.

## License

MIT License - see the LICENSE file for details.

## Support

For support, contact [Quantec](https://www.quantec.co.za/contact/)
