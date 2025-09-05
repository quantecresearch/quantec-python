import logging
import os
from io import StringIO, BytesIO
from typing import Optional, Union

import pandas as pd
import requests
from dotenv import load_dotenv

from .. import __version__
from .cache import CacheManager
from . import validators

load_dotenv()

log = logging.getLogger(__name__)


class Client:
    """Client for Quantec API.

    Parameters
    ----------
    api_key : Optional[str], optional
        API key. Defaults to EASYDATA_API_KEY env variable.
    api_url : Optional[str], optional
        API base URL. Defaults to EASYDATA_API_URL env variable or https://www.easydata.co.za/api/v3/.
    use_cache : bool, optional
        Enable caching for time series and grid data. Defaults to True.
    cache_dir : str, optional
        Directory for cached files. Defaults to 'cache'.

    Raises
    ------
    ValueError
        If api_key is empty.

    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_url: Optional[str] = None,
        use_cache: bool = True,
        cache_dir: str = "cache",
    ) -> None:
        api_key = api_key or os.getenv("EASYDATA_API_KEY")
        api_url = (
            api_url
            or os.getenv("EASYDATA_API_URL")
            or "https://www.easydata.co.za/api/v3"
        )
        if not api_key:
            raise ValueError(
                "API key must be provided via api_key parameter or EASYDATA_API_KEY environment variable"
            )

        self.__version__: str = __version__
        self.api_key: str = api_key
        self.api_url: str = api_url.rstrip("/")

        # Initialize cache manager if caching is enabled
        self.cache = CacheManager(cache_dir) if use_cache else None

    def get_data(
        self,
        time_series_codes: Optional[str] = None,
        selection_pk: Optional[int] = None,
        freq: str = "A",
        start_year: str = "",
        end_year: str = "",
        analysis: bool = False,
        resp_format: str = "dataframe",
        is_tidy: bool = True,
    ) -> Union[pd.DataFrame, str, dict]:
        """
        Fetch data from Quantec API.

        Parameters
        ----------
        time_series_codes : Optional[str], optional
            Comma-separated string of time series codes (e.g., "code1,code2").
        selection_pk : Optional[int], optional
            Selection primary key. Takes precedence over time_series_codes.
        freq : str, optional
            Data frequency ('M', 'Q', etc.). Defaults to 'M'.
        start_year : str, optional
            Start year (yyyy). Defaults to ''.
        end_year : str, optional
            End year (yyyy). Defaults to ''.
        analysis : bool, optional
            Include analysis parameter. Defaults to False.
        resp_format : str, optional
            Response format ('dataframe', 'csv', or 'json'). Defaults to 'dataframe'.
        is_tidy : bool, optional
            Return tidy data. Defaults to True.

        Returns
        -------
        Union[pd.DataFrame, str, dict]
            DataFrame for dataframe format, CSV string for csv format, dict for JSON.

        Raises
        ------
        ValueError
            If neither time_series_codes nor selection_pk is provided.
        requests.HTTPError
            If API request fails.
        requests.ConnectionError
            If network issue occurs.
        ValueError
            If response parsing fails.

        """
        if not time_series_codes and selection_pk is None:
            raise ValueError(
                "Either time_series_codes or selection_pk must be provided"
            )

        if resp_format not in ["dataframe", "csv", "json"]:
            raise ValueError("resp_format must be 'dataframe', 'csv', or 'json'")

        # Determine API format (use csv for dataframe requests)
        api_format = "csv" if resp_format == "dataframe" else resp_format

        url: str = f"{self.api_url}/download/"

        query_params: dict[str, Union[str, bool, int]] = {
            "respFormat": api_format,
            "freqs": freq,
            "startYear": start_year,
            "endYear": end_year,
            "isTidy": is_tidy,
            "analysis": analysis,
        }

        if selection_pk is not None:
            query_params["selectionPk"] = selection_pk
            log_key = str(selection_pk)
        else:
            query_params["timeSeriesCodes"] = time_series_codes
            log_key = time_series_codes

        log.debug(f"[{log_key}] -- Querying with parameters: {query_params}")

        # Caching: attempt to load from cache first
        cache_key = None
        if self.cache:
            cache_key = self.cache.generate_key(
                "get_data",
                log_key,
                freq,
                start_year,
                end_year,
                analysis,
                resp_format,
                is_tidy,
            )
            cached = self.cache.read(cache_key, resp_format, api_format)
            if cached is not None:
                log.debug(f"[{log_key}] -- Loaded get_data from cache")
                return cached  # type: ignore[return-value]

        try:
            response = requests.get(
                url, params={**query_params, "auth_token": self.api_key}
            )
            response.raise_for_status()
        except requests.ConnectionError as e:
            raise requests.ConnectionError(
                "Network error: Unable to connect to API"
            ) from e
        except requests.HTTPError as e:
            raise requests.HTTPError(f"API request failed: {response.text}") from e

        # Save raw response to cache after successful request
        if self.cache and cache_key:
            self.cache.write(cache_key, api_format, response)

        # Handle return format based on user's request
        if resp_format == "csv":
            # Return raw CSV string
            log.debug(f"[{log_key}] -- Returning raw CSV data")
            return response.text
        elif resp_format == "json":
            # Return JSON dict
            try:
                out: dict = response.json()
                log.debug(f"[{log_key}] -- Found {len(out)} items")
                return out
            except ValueError as e:
                raise ValueError("Failed to parse JSON response") from e
        else:  # resp_format == "dataframe"
            # Parse CSV into DataFrame
            try:
                out: pd.DataFrame = (
                    pd.read_csv(StringIO(response.text))
                    .dropna(how="all")
                    .reset_index(drop=True)
                )
                log.debug(f"[{log_key}] -- Found {len(out)} rows")
                return out
            except pd.errors.ParserError as e:
                raise ValueError("Failed to parse CSV response") from e

    def get_recipes(self) -> Union[pd.DataFrame, dict]:
        """
        Fetch available recipes from Quantec API.

        Returns
        -------
        pd.DataFrame
            DataFrame containing recipe information.

        Raises
        ------
        requests.HTTPError
            If API request fails.
        requests.ConnectionError
            If network issue occurs.
        ValueError
            If response parsing fails.

        """
        url: str = f"{self.api_url}/recipes/"

        try:
            response = requests.get(url, params={"auth_token": self.api_key})
            response.raise_for_status()
        except requests.ConnectionError as e:
            raise requests.ConnectionError(
                "Network error: Unable to connect to API"
            ) from e
        except requests.HTTPError as e:
            raise requests.HTTPError(f"API request failed: {response.text}") from e

        # Always return as DataFrame for recipes
        try:
            recipes_data = response.json()
            out: pd.DataFrame = pd.DataFrame(recipes_data).dropna(axis=1, how="all")
        except (ValueError, pd.errors.ParserError) as e:
            raise ValueError("Failed to parse recipes response") from e

        log.debug(
            f"Found {len(out) if isinstance(out, pd.DataFrame) else len(out)} recipes"
        )
        return out

    def get_selections(
        self,
        status: Optional[str] = None,
        show: Optional[str] = None,
        filter: Optional[str] = None,
    ) -> Union[pd.DataFrame, dict]:
        """
        Fetch user's available selections from Quantec API.

        Parameters
        ----------
        status : Optional[str], optional
            Filter by selection status using combined flags:
            U=Unsaved, P=Private, S=Shared, O=Open (e.g., "PSO").
        show : Optional[str], optional
            Show specific selection types ("shared" or "open").
        filter : Optional[str], optional
            Apply additional filters (e.g., "active").

        Returns
        -------
        pd.DataFrame
            Selection data with transformed fields: item, pk, title,
            code_count, is_owner, owner, status, description, modified.

        Raises
        ------
        requests.HTTPError
            If API request fails.
        requests.ConnectionError
            If network issue occurs.
        ValueError
            If response parsing fails.

        """
        url: str = f"{self.api_url}/selections/"

        query_params: dict[str, str] = {"auth_token": self.api_key, "format": "json"}

        if status:
            query_params["status"] = status
        if show:
            query_params["show"] = show
        if filter:
            query_params["filter"] = filter

        log.debug(f"Querying selections with parameters: {query_params}")

        try:
            response = requests.get(url, params=query_params)
            response.raise_for_status()
        except requests.ConnectionError as e:
            raise requests.ConnectionError(
                "Network error: Unable to connect to API"
            ) from e
        except requests.HTTPError as e:
            raise requests.HTTPError(f"API request failed: {response.text}") from e

        try:
            resp = response.json()
            if not resp:
                selections_data = []
            else:
                # Transform data following the original logic
                selections_data = [
                    {
                        "item": i,
                        "pk": item["id"],
                        "title": item["title"],
                        "code_count": len(item.get("timeseriescodes", [])),
                        "is_owner": item["is_owner"],
                        "owner": item["owner"]["username"],
                        "status": item["status"],
                        "description": item.get("description", ""),
                        "modified": item["modified"],
                    }
                    for i, item in enumerate(resp, 1)
                ]
        except (ValueError, KeyError, TypeError) as e:
            raise ValueError("Failed to parse selections response") from e

        # Always return as DataFrame for selections
        out: pd.DataFrame = pd.DataFrame(selections_data).dropna(axis=1, how="all")

        log.debug(f"Found {len(selections_data)} selections")
        return out

    def get_grid_data(
        self,
        recipe_pk: int,
        is_expanded: bool = False,
        is_melted: bool = True,
        resp_format: str = "dataframe",
        selectdimensionnodes: Union[dict, list[dict]] = None,
        has_tscodes: bool = False,
        has_dncodes: bool = False,
    ) -> Union[pd.DataFrame, str, bytes]:
        """
        Fetch grid/pivot table data using recipe primary key.

        Parameters
        ----------
        recipe_pk : int
            Recipe primary key identifier.
        is_expanded : bool, optional
            Return expanded data format. Defaults to False.
        is_melted : bool, optional
            Return melted data format. Defaults to True.
        resp_format : str, optional
            Response format ('dataframe', 'parquet', or 'csv'). Defaults to 'dataframe'.
        selectdimensionnodes : Union[dict, list[dict]], optional
            Dimension filtering. Single dict or list of dicts for multiple dimensions.
            Example single: {"dimension": "d1", "codes": ["CODE1"]}.
            Example multiple: [{"dimension": "d1", "codes": ["CODE1"]}, {"dimension": "d2", "levels": [2]}].
            Each dict can contain: dimension (required), levels, codes, children, children_include_self.
            Defaults to None.
        has_tscodes : bool, optional
            Include time series codes in response. Defaults to False.
        has_dncodes : bool, optional
            Include dimension node codes in response. Defaults to False.

        Returns
        -------
        Union[pd.DataFrame, str, bytes]
            DataFrame if resp_format='dataframe', CSV string if resp_format='csv',
            or bytes if resp_format='parquet'.

        Raises
        ------
        ValueError
            If resp_format is invalid.
        requests.HTTPError
            If API request fails.
        requests.ConnectionError
            If network issue occurs.
        ValueError
            If response parsing fails.

        """
        if resp_format not in ["dataframe", "parquet", "csv"]:
            raise ValueError("resp_format must be 'dataframe', 'parquet', or 'csv'")

        # Validate and normalize dimension filters
        normalized_filters_str = None
        if selectdimensionnodes is not None:
            validators.validate_dimension_filters(selectdimensionnodes)

            # Normalize for caching (only if not empty)
            if self.cache and selectdimensionnodes:
                normalized_filters_str = self.cache.normalize_dimension_filters(
                    selectdimensionnodes
                )

        # Determine API format (use parquet for dataframe requests for efficiency)
        api_format = "parquet" if resp_format == "dataframe" else resp_format

        # Check cache first - use normalized filters for consistent caching
        cache_key = None
        if self.cache:
            cache_key = self.cache.generate_key(
                recipe_pk,
                is_expanded,
                is_melted,
                api_format,
                normalized_filters_str,
                has_tscodes,
                has_dncodes,
            )
            cached_grid = self.cache.read(cache_key, resp_format, api_format)
            if cached_grid is not None:
                log.debug(f"[{recipe_pk}] -- Loaded from cache")
                return cached_grid  # type: ignore[return-value]

        url: str = f"{self.api_url}/download/recipes/{recipe_pk}/"

        # Use POST if filtering (non-empty), GET otherwise
        if selectdimensionnodes:
            # POST request for filtering
            request_data = {
                "respFormat": api_format,
                "isExpanded": is_expanded,
                "isMelted": is_melted,
                "selectdimensionnodes": selectdimensionnodes,
                "hasTimeSeriesCodes": has_tscodes,
                "hasDimensionNodeCodes": has_dncodes,
            }

            headers = {
                "Authorization": f"Token {self.api_key}",
                "Content-Type": "application/json",
            }

            filter_count = (
                1
                if isinstance(selectdimensionnodes, dict)
                else len(selectdimensionnodes)
            )
            log.debug(
                f"[{recipe_pk}] -- POST with {filter_count} dimension filter(s): {selectdimensionnodes}"
            )

            try:
                response = requests.post(url, json=request_data, headers=headers)
                response.raise_for_status()
            except requests.ConnectionError as e:
                raise requests.ConnectionError(
                    "Network error: Unable to connect to API"
                ) from e
            except requests.HTTPError as e:
                raise requests.HTTPError(f"API request failed: {response.text}") from e
        else:
            # GET request (existing code)
            query_params: dict[str, Union[str, bool, int]] = {
                "respFormat": api_format,
                "isExpanded": is_expanded,
                "isMelted": is_melted,
                "auth_token": self.api_key,
                "hasTimeSeriesCodes": has_tscodes,
                "hasDimensionNodeCodes": has_dncodes,
            }

            log.debug(f"[{recipe_pk}] -- Querying with parameters: {query_params}")

            try:
                response = requests.get(url, params=query_params)
                response.raise_for_status()
            except requests.ConnectionError as e:
                raise requests.ConnectionError(
                    "Network error: Unable to connect to API"
                ) from e
            except requests.HTTPError as e:
                raise requests.HTTPError(f"API request failed: {response.text}") from e

        # Save raw response to cache
        if self.cache and cache_key:
            self.cache.write(cache_key, api_format, response)

        # Handle return format based on user's request
        if resp_format == "csv":
            # Return raw CSV string
            log.debug(f"[{recipe_pk}] -- Returning raw CSV data")
            return response.text
        elif resp_format == "parquet":
            # Return raw parquet bytes
            log.debug(f"[{recipe_pk}] -- Returning raw parquet data")
            return response.content
        else:  # resp_format == "dataframe"
            # Parse into DataFrame and apply cleaning
            if api_format == "parquet":
                try:
                    out: pd.DataFrame = pd.read_parquet(BytesIO(response.content))
                except Exception as e:
                    raise ValueError("Failed to parse parquet response") from e
            else:  # api_format == "csv"
                try:
                    out: pd.DataFrame = pd.read_csv(StringIO(response.text))
                except pd.errors.ParserError as e:
                    raise ValueError("Failed to parse CSV response") from e

            # Clean up data (only for DataFrame output)
            out = out.dropna(axis=1, how="all")

            log.debug(f"[{recipe_pk}] -- Found {len(out)} rows")
            return out
