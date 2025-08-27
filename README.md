# Quantec

A Python package for working with Quantec EasyData API. Fetch financial and economic time series data with support for multiple response formats, caching, and advanced filtering capabilities.

## Features

- 🎯 **Multiple Data Access Patterns**: Time series codes, selections, and grid/pivot data
- 📊 **Format Support**: CSV, JSON, and Parquet response formats
- 🔍 **Advanced Filtering**: Dimension-based filtering for grid data
- ⚡ **Performance Caching**: Optional caching system for grid data
- 🛡️ **Error Handling**: Comprehensive error handling for network and API issues
- 🔧 **Flexible Configuration**: Environment variables and parameter-based setup

## Installation

```bash
pip install quantec
```

## Quick Start

```python
from quantec.easydata.client import Client

# Initialize client
client = Client()

# Get time series data
data = client.get_data(time_series_codes="GDP,CPI,UNEMP")
print(data.head())
```

## Configuration

### Environment Variables

Set up your credentials using environment variables:

```bash
export QUANTEC_API_KEY="your-api-key-here"
export QUANTEC_API_URL="https://api.quantec.co.za"
```

### Client Initialization Options

```python
from quantec.easydata.client import Client

# Basic initialization (uses environment variables)
client = Client()

# Custom configuration
client = Client(
    apikey="your-api-key",
    api_url="https://api.quantec.co.za",
    respformat="json",        # 'csv', 'json', or 'parquet'
    is_tidy=True,            # Return tidy data format
    use_cache=True,          # Enable caching for grid data
    cache_dir="./cache"      # Cache directory path
)
```

## Time Series Data

### Direct Access with Codes

```python
# Single time series
data = client.get_data(time_series_codes="GDP_SA")

# Multiple time series
data = client.get_data(time_series_codes="GDP_SA,CPI_TOTAL,UNEMP_RATE")

# With date filtering and frequency
data = client.get_data(
    time_series_codes="GDP_SA,CPI_TOTAL",
    freq="Q",                    # Quarterly data
    start_year="2020-01-01",
    end_year="2023-12-31",
    analysis=True               # Include analysis parameters
)
```

### Discovery-Based Access

```python
# Find available selections
selections = client.get_selections(status="PSO")  # Private, Shared, Open
print(f"Found {len(selections)} selections")

# Use selection for data retrieval
if len(selections) > 0:
    selection = selections[0]
    print(f"Using selection: {selection['title']} ({selection['code_count']} codes)")
    data = client.get_data(selection_pk=selection['pk'])
```

### Advanced Selection Filtering

```python
# Filter by status flags
shared_selections = client.get_selections(status="S")      # Shared only
private_selections = client.get_selections(status="P")     # Private only
open_selections = client.get_selections(status="O")       # Open only

# Combined status flags
all_selections = client.get_selections(status="PSO")      # All types

# Additional filters
active_selections = client.get_selections(
    status="PSO", 
    filter="active",
    show="shared"
)
```

## Grid/Pivot Data

### Basic Grid Data Access

```python
# Get available recipes
recipes = client.get_recipes()
print(f"Available recipes: {len(recipes)}")

# Basic grid data retrieval
if len(recipes) > 0:
    recipe_id = recipes[0]['id']  # Use first available recipe
    grid_data = client.get_grid_data(recipe_pk=recipe_id)
    print(f"Grid data shape: {grid_data.shape}")
```

### Advanced Grid Data with Filtering

```python
# Grid data with dimension filtering (NEW FEATURE)
filters = {
    "dimension": "d1", 
    "codes": ["CODE1", "CODE2", "CODE3"]
}

filtered_grid = client.get_grid_data(
    recipe_pk=12345,
    selectdimensionnodes=filters,
    resp_format="parquet",       # Optimal for large datasets
    is_expanded=True,
    is_melted=True
)

# Multiple dimension filtering
complex_filters = {
    "dimension": "geography",
    "codes": ["ZAF", "USA", "GBR", "DEU"]
}

regional_data = client.get_grid_data(
    recipe_pk=12345,
    selectdimensionnodes=complex_filters
)
```

### Response Format Options

```python
# CSV format (default, good for small datasets)
csv_data = client.get_grid_data(recipe_pk=12345, resp_format="csv")

# JSON format (good for nested data structures)
json_data = client.get_grid_data(recipe_pk=12345, resp_format="json")

# Parquet format (recommended for large datasets)
parquet_data = client.get_grid_data(recipe_pk=12345, resp_format="parquet")
```

## Caching for Performance

### Enable Caching

```python
# Initialize client with caching
cached_client = Client(
    use_cache=True,
    cache_dir="./quantec_cache"
)

# First call - fetches from API and caches
grid_data = cached_client.get_grid_data(recipe_pk=12345)

# Subsequent calls - loads from cache (much faster!)
grid_data = cached_client.get_grid_data(recipe_pk=12345)
```

### Cache Management

```python
import os
from pathlib import Path

# Check cache directory
cache_dir = Path("./quantec_cache")
if cache_dir.exists():
    cache_files = list(cache_dir.glob("*.parquet"))
    print(f"Cached files: {len(cache_files)}")
    
    # Calculate cache size
    total_size = sum(f.stat().st_size for f in cache_files)
    print(f"Cache size: {total_size / (1024*1024):.2f} MB")
```

## Error Handling

### Robust Error Handling

```python
import requests
from quantec.easydata.client import Client

client = Client()

try:
    data = client.get_data(time_series_codes="INVALID_CODE")
except requests.HTTPError as e:
    print(f"API Error: {e}")
except requests.ConnectionError as e:
    print(f"Network Error: {e}")
except ValueError as e:
    print(f"Data Parsing Error: {e}")
except Exception as e:
    print(f"Unexpected Error: {e}")
```

### Validation and Fallbacks

```python
def safe_data_fetch(client, codes, fallback_selection_pk=None):
    """Safely fetch data with fallback options."""
    try:
        # Try primary method
        return client.get_data(time_series_codes=codes)
    except Exception as e:
        print(f"Primary fetch failed: {e}")
        
        if fallback_selection_pk:
            try:
                # Fallback to selection
                print("Trying fallback selection...")
                return client.get_data(selection_pk=fallback_selection_pk)
            except Exception as e2:
                print(f"Fallback also failed: {e2}")
                return None
    return None

# Usage
data = safe_data_fetch(client, "GDP,CPI", fallback_selection_pk=123)
```

## Complete Workflow Example

```python
from quantec.easydata.client import Client
import pandas as pd

def comprehensive_data_analysis():
    # Initialize client with optimal settings
    client = Client(
        use_cache=True,
        cache_dir="./analysis_cache",
        respformat="parquet"  # Best performance for large data
    )
    
    # 1. Explore available data
    print("🔍 Discovering available data...")
    selections = client.get_selections(status="PSO")
    recipes = client.get_recipes()
    
    print(f"Found {len(selections)} selections and {len(recipes)} recipes")
    
    # 2. Get time series data
    print("📈 Fetching time series data...")
    ts_data = client.get_data(
        time_series_codes="GDP_SA,CPI_TOTAL,UNEMP_RATE",
        freq="Q",
        start_year="2020-01-01"
    )
    
    # 3. Get filtered grid data
    print("📊 Fetching filtered grid data...")
    if len(recipes) > 0:
        filters = {"dimension": "d1", "codes": ["ZAF", "USA"]}
        grid_data = client.get_grid_data(
            recipe_pk=recipes[0]['id'],
            selectdimensionnodes=filters,
            resp_format="parquet"
        )
        
        print(f"Grid data shape: {grid_data.shape}")
    
    # 4. Combine and analyze
    print("🔬 Analysis complete!")
    return {
        'time_series': ts_data,
        'grid_data': grid_data if 'grid_data' in locals() else None,
        'metadata': {
            'selections_count': len(selections),
            'recipes_count': len(recipes)
        }
    }

# Run the analysis
results = comprehensive_data_analysis()
```

## Best Practices

### 1. **Use Appropriate Response Formats**
- **CSV**: Small datasets, simple analysis
- **JSON**: Complex nested structures
- **Parquet**: Large datasets, best performance

### 2. **Enable Caching for Grid Data**
```python
# Recommended for repeated grid data access
client = Client(use_cache=True, cache_dir="./cache")
```

### 3. **Handle API Limits Gracefully**
```python
import time

def rate_limited_requests(client, requests_list, delay=1.0):
    """Make requests with rate limiting."""
    results = []
    for i, request_params in enumerate(requests_list):
        if i > 0:
            time.sleep(delay)  # Respectful delay between requests
        
        try:
            result = client.get_data(**request_params)
            results.append(result)
        except Exception as e:
            print(f"Request {i} failed: {e}")
            results.append(None)
    
    return results
```

### 4. **Optimize Memory Usage**
```python
# For large datasets, process in chunks
def process_large_grid(client, recipe_pk, dimension_codes, chunk_size=100):
    """Process large grid data in chunks."""
    all_data = []
    
    for i in range(0, len(dimension_codes), chunk_size):
        chunk_codes = dimension_codes[i:i+chunk_size]
        filters = {"dimension": "d1", "codes": chunk_codes}
        
        chunk_data = client.get_grid_data(
            recipe_pk=recipe_pk,
            selectdimensionnodes=filters,
            resp_format="parquet"
        )
        all_data.append(chunk_data)
    
    return pd.concat(all_data, ignore_index=True)
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Check the [documentation](https://docs.quantec.co.za)
- Review API endpoints and parameters
- Ensure environment variables are set correctly