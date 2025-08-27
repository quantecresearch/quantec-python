import hashlib
import logging
import os
from io import StringIO
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import requests
from dotenv import load_dotenv

from .. import __version__

load_dotenv()

log = logging.getLogger(__name__)


class Client:
    """Client for Quantec API.

    Parameters
    ----------
    apikey : Optional[str], optional
        API key. Defaults to QUANTEC_API_KEY env variable.
    respformat : str, optional
        Response format ('csv' for time series, 'csv'/'parquet' for grid data). Defaults to 'csv'.
    is_tidy : bool, optional
        Return tidy data. Defaults to True.
    api_url : Optional[str], optional
        API base URL. Defaults to QUANTEC_API_URL env variable.
    use_cache : bool, optional
        Enable caching for grid data. Defaults to False.
    cache_dir : str, optional
        Directory for cached files. Defaults to 'cache'.

    Raises
    ------
    ValueError
        If apikey or api_url is empty or respformat is invalid.

    Examples
    --------
    >>> from quantec.easydata import Client
    >>> client = Client()
    >>> data = client.get_data(time_series_codes="code1,code2")
    >>>
    >>> # With caching enabled
    >>> client = Client(use_cache=True)
    >>> recipes = client.get_recipes()
    >>> grid = client.get_grid_data(recipe_pk=12345)
    >>>
    >>> # Discover and use selections
    >>> selections = client.get_selections(status="PSO")
    >>> data = client.get_data(selection_pk=selections[0]['pk'])
    """

    def __init__(
        self,
        apikey: Optional[str] = None,
        respformat: str = "csv",
        is_tidy: bool = True,
        api_url: Optional[str] = None,
        use_cache: bool = False,
        cache_dir: str = "cache",
    ) -> None:
        apikey = apikey or os.getenv("QUANTEC_API_KEY")
        api_url = api_url or os.getenv("QUANTEC_API_URL")
        if not apikey:
            raise ValueError(
                "API key must be provided via apikey parameter or QUANTEC_API_KEY environment variable"
            )
        if not api_url:
            raise ValueError(
                "API URL must be provided via api_url parameter or QUANTEC_API_URL environment variable"
            )
        if respformat not in ["csv", "json", "parquet"]:
            raise ValueError("respformat must be 'csv', 'json', or 'parquet'")

        self.__version__: str = __version__
        self.apikey: str = apikey
        self.respformat: str = respformat
        self.is_tidy: bool = is_tidy
        self.api_url: str = api_url.rstrip("/")
        self.use_cache: bool = use_cache
        self.cache_dir: str = cache_dir

        if use_cache:
            self._setup_cache()

    def get_data(
        self,
        time_series_codes: Optional[str] = None,
        selection_pk: Optional[int] = None,
        freq: str = "M",
        start_year: str = "",
        end_year: str = "",
        analysis: bool = False,
    ) -> Union[pd.DataFrame, dict]:
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
            Start date ('YYYY-MM-DD'). Defaults to ''.
        end_year : str, optional
            End date ('YYYY-MM-DD'). Defaults to ''.
        analysis : bool, optional
            Include analysis parameter. Defaults to False.

        Returns
        -------
        Union[pd.DataFrame, dict]
            DataFrame for CSV, dict for JSON.

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

        Examples
        --------
        >>> client = Client()
        >>> df = client.get_data(time_series_codes="code1,code2", freq="Q")
        >>> df = client.get_data(selection_pk=123, freq="M")
        """
        if not time_series_codes and selection_pk is None:
            raise ValueError(
                "Either time_series_codes or selection_pk must be provided"
            )

        url: str = f"{self.api_url}/download/"

        query_params: dict[str, Union[str, bool, int]] = {
            "respFormat": self.respformat,
            "freqs": freq,
            "startYear": start_year,
            "endYear": end_year,
            "isTidy": self.is_tidy,
            "analysis": analysis,
        }

        if selection_pk is not None:
            query_params["selectionPk"] = selection_pk
            log_key = str(selection_pk)
        else:
            query_params["timeSeriesCodes"] = time_series_codes
            log_key = time_series_codes

        log.debug(f"[{log_key}] -- Querying with parameters: {query_params}")

        try:
            response = requests.get(
                url, params={**query_params, "auth_token": self.apikey}
            )
            response.raise_for_status()
        except requests.ConnectionError as e:
            raise requests.ConnectionError(
                "Network error: Unable to connect to API"
            ) from e
        except requests.HTTPError as e:
            raise requests.HTTPError(f"API request failed: {response.text}") from e

        if self.respformat == "csv":
            try:
                out: pd.DataFrame = (
                    pd.read_csv(StringIO(response.text)).dropna().reset_index()
                )
            except pd.errors.ParserError as e:
                raise ValueError("Failed to parse CSV response") from e
        else:
            try:
                out: dict = response.json()
            except ValueError as e:
                raise ValueError("Failed to parse JSON response") from e

        log.debug(f"[{log_key}] -- Found {len(out)} rows")
        return out

    def _setup_cache(self) -> None:
        """Create cache directory if it doesn't exist."""
        Path(self.cache_dir).mkdir(parents=True, exist_ok=True)

    def _generate_cache_key(self, *args, debug: bool = False) -> str:
        """Generate hash-based cache key from arguments."""
        hash_input = "".join(str(arg) for arg in args)
        if debug:
            return f"debug_{hashlib.md5(hash_input.encode()).hexdigest()[:8]}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    def _load_from_cache(
        self, cache_key: str, resp_format: str
    ) -> Optional[pd.DataFrame]:
        """Load data from cache if it exists."""
        if not self.use_cache:
            return None

        cache_path = Path(self.cache_dir) / f"{cache_key}.{resp_format}"
        if not cache_path.exists():
            return None

        log.debug(f"Loading from cache: {cache_path}")
        if resp_format == "parquet":
            return pd.read_parquet(cache_path)
        elif resp_format == "csv":
            return pd.read_csv(cache_path)
        return None

    def _save_to_cache(
        self, data: pd.DataFrame, cache_key: str, resp_format: str
    ) -> None:
        """Save data to cache."""
        if not self.use_cache:
            return

        cache_path = Path(self.cache_dir) / f"{cache_key}.{resp_format}"
        log.debug(f"Saving to cache: {cache_path}")
        if resp_format == "parquet":
            data.to_parquet(cache_path, index=False)
        elif resp_format == "csv":
            data.to_csv(cache_path, index=False)

    def get_recipes(self) -> Union[pd.DataFrame, dict]:
        """
        Fetch available recipes from Quantec API.

        Returns
        -------
        Union[pd.DataFrame, dict]
            DataFrame for CSV, dict for JSON.

        Raises
        ------
        requests.HTTPError
            If API request fails.
        requests.ConnectionError
            If network issue occurs.
        ValueError
            If response parsing fails.

        Examples
        --------
        >>> client = Client()
        >>> recipes = client.get_recipes()
        """
        url: str = f"{self.api_url}/recipes/"

        try:
            response = requests.get(url, params={"auth_token": self.apikey})
            response.raise_for_status()
        except requests.ConnectionError as e:
            raise requests.ConnectionError(
                "Network error: Unable to connect to API"
            ) from e
        except requests.HTTPError as e:
            raise requests.HTTPError(f"API request failed: {response.text}") from e

        if self.respformat == "csv":
            try:
                recipes_data = response.json()
                out: pd.DataFrame = pd.DataFrame(recipes_data).dropna(axis=1, how="all")
            except (ValueError, pd.errors.ParserError) as e:
                raise ValueError("Failed to parse recipes response") from e
        else:
            try:
                out: dict = response.json()
            except ValueError as e:
                raise ValueError("Failed to parse JSON response") from e

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
        Union[pd.DataFrame, dict]
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

        Examples
        --------
        >>> client = Client()
        >>> selections = client.get_selections(status="PSO")
        >>> data = client.get_data(selection_pk=selections[0]['pk'])
        """
        url: str = f"{self.api_url}/selections/"

        query_params: dict[str, str] = {"auth_token": self.apikey, "format": "json"}

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

        if self.respformat == "csv":
            out: pd.DataFrame = pd.DataFrame(selections_data).dropna(axis=1, how="all")
        else:
            out: dict = {"selections": selections_data}

        log.debug(f"Found {len(selections_data)} selections")
        return out

    def get_grid_data(
        self,
        recipe_pk: int,
        is_expanded: bool = True,
        is_melted: bool = True,
        resp_format: str = "parquet",
        selectdimensionnodes: dict = None,
    ) -> pd.DataFrame:
        """
        Fetch grid/pivot table data using recipe primary key.

        Parameters
        ----------
        recipe_pk : int
            Recipe primary key identifier.
        is_expanded : bool, optional
            Return expanded data format. Defaults to True.
        is_melted : bool, optional
            Return melted data format. Defaults to True.
        resp_format : str, optional
            Response format ('parquet' or 'csv'). Defaults to 'parquet'.
        selectdimensionnodes : dict, optional
            Dimension filtering. Example: {"dimension": "d1", "codes": ["CODE1"]}.
            Defaults to None.

        Returns
        -------
        pd.DataFrame
            Grid data as DataFrame.

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

        Examples
        --------
        >>> client = Client()
        >>> grid = client.get_grid_data(recipe_pk=12345)
        >>> grid = client.get_grid_data(recipe_pk=12345, resp_format="csv")
        >>>
        >>> # With filtering
        >>> filters = {"dimension": "d1", "codes": ["CODE1", "CODE2"]}
        >>> grid = client.get_grid_data(recipe_pk=12345, selectdimensionnodes=filters)
        """
        if resp_format not in ["parquet", "csv"]:
            raise ValueError("resp_format must be 'parquet' or 'csv'")

        # Check cache first
        cache_key = self._generate_cache_key(
            recipe_pk, is_expanded, is_melted, resp_format, selectdimensionnodes
        )
        cached_data = self._load_from_cache(cache_key, resp_format)
        if cached_data is not None:
            log.debug(f"[{recipe_pk}] -- Loaded from cache")
            return cached_data

        url: str = f"{self.api_url}/download/recipes/{recipe_pk}/"

        # Use POST if filtering, GET otherwise
        if selectdimensionnodes:
            # POST request for filtering
            request_data = {
                "respFormat": resp_format,
                "isExpanded": is_expanded,
                "isMelted": is_melted,
                "selectdimensionnodes": selectdimensionnodes,
            }

            headers = {
                "Authorization": f"Token {self.apikey}",
                "Content-Type": "application/json",
            }

            log.debug(f"[{recipe_pk}] -- POST with filters: {selectdimensionnodes}")

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
                "respFormat": resp_format,
                "isExpanded": is_expanded,
                "isMelted": is_melted,
                "auth_token": self.apikey,
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

        # Parse response based on format
        if resp_format == "parquet":
            try:
                from io import BytesIO

                out: pd.DataFrame = pd.read_parquet(BytesIO(response.content))
            except Exception as e:
                raise ValueError("Failed to parse parquet response") from e
        else:  # csv
            try:
                out: pd.DataFrame = pd.read_csv(StringIO(response.text))
            except pd.errors.ParserError as e:
                raise ValueError("Failed to parse CSV response") from e

        # Clean up data
        out = out.dropna(axis=1, how="all")

        # Save to cache
        self._save_to_cache(out, cache_key, resp_format)

        log.debug(f"[{recipe_pk}] -- Found {len(out)} rows")
        return out
