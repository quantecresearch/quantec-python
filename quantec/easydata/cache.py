import hashlib
import json
import logging
from io import StringIO
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import requests

log = logging.getLogger(__name__)


class CacheManager:
    """Manages caching for API responses with hash-based filenames."""

    def __init__(self, cache_dir: str = "cache") -> None:
        """Initialize cache manager.
        
        Parameters
        ----------
        cache_dir : str, optional
            Directory for cached files. Defaults to "cache".
        """
        self.cache_dir = cache_dir
        self._setup()

    def _setup(self) -> None:
        """Create cache directory if it doesn't exist."""
        Path(self.cache_dir).mkdir(parents=True, exist_ok=True)

    def _cache_path(self, cache_key: str, ext: str) -> Path:
        """Get path for cache file.
        
        Parameters
        ----------
        cache_key : str
            Cache key identifier.
        ext : str
            File extension.
            
        Returns
        -------
        Path
            Path to cache file.
        """
        return Path(self.cache_dir) / f"{cache_key}.{ext}"

    def generate_key(self, *args, debug: bool = False) -> str:
        """Generate hash-based cache key from arguments.
        
        Parameters
        ----------
        *args
            Arguments to include in cache key.
        debug : bool, optional
            Generate debug key with prefix. Defaults to False.
            
        Returns
        -------
        str
            Cache key string.
        """
        hash_input = "".join(str(arg) for arg in args)
        if debug:
            return f"debug_{hashlib.md5(hash_input.encode()).hexdigest()[:8]}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    def normalize_dimension_filters(self, selectdimensionnodes: Union[dict, list[dict]]) -> str:
        """Normalize dimension filters for consistent caching.
        
        Parameters
        ----------
        selectdimensionnodes : Union[dict, list[dict]]
            Single dimension filter dict or list of dimension filter dicts.
            
        Returns
        -------
        str
            JSON string representation of normalized filters for cache key.
        """
        # Convert to list format for consistent processing
        if isinstance(selectdimensionnodes, dict):
            filters_list = [selectdimensionnodes]
        else:
            filters_list = selectdimensionnodes.copy()
        
        # Normalize each filter
        normalized_filters = []
        for filter_dict in filters_list:
            # Create a copy to avoid modifying original
            normalized = filter_dict.copy()
            
            # Sort codes if present for consistency
            if "codes" in normalized and normalized["codes"]:
                normalized["codes"] = sorted(normalized["codes"])
            
            # Sort levels if present for consistency  
            if "levels" in normalized and normalized["levels"]:
                normalized["levels"] = sorted(normalized["levels"])
            
            # Ensure consistent key ordering
            ordered_filter = {}
            for key in ["dimension", "levels", "codes", "children", "children_include_self"]:
                if key in normalized:
                    ordered_filter[key] = normalized[key]
            
            normalized_filters.append(ordered_filter)
        
        # Sort filters by dimension for consistent ordering
        normalized_filters.sort(key=lambda x: x["dimension"])
        
        # Convert to JSON string for cache key
        return json.dumps(normalized_filters, separators=(',', ':'), sort_keys=True)

    def read(
        self, cache_key: str, return_format: str, api_format: Optional[str] = None
    ) -> Optional[Union[pd.DataFrame, str, bytes, dict]]:
        """Read data from cache.
        
        Parameters
        ----------
        cache_key : str
            Cache key identifier.
        return_format : str
            Desired return format.
        api_format : Optional[str], optional
            API format used. Defaults to None.
            
        Returns
        -------
        Optional[Union[pd.DataFrame, str, bytes, dict]]
            Cached data if exists, None otherwise.
        """
        ext = api_format or return_format
        path = self._cache_path(cache_key, ext)
        if not path.exists():
            return None

        if return_format == "csv":
            try:
                return path.read_text()
            except Exception:
                return None
        if return_format == "json":
            try:
                return json.loads(path.read_text())
            except Exception:
                return None
        if return_format == "parquet":
            try:
                return path.read_bytes()
            except Exception:
                return None
        if return_format == "dataframe":
            try:
                if (api_format or "").lower() == "parquet":
                    return pd.read_parquet(path).dropna(axis=1, how="all")
                else:
                    return pd.read_csv(path).dropna(axis=1, how="all")
            except Exception:
                return None

        return None

    def write(
        self,
        cache_key: str,
        api_format: str,
        response: requests.Response,
    ) -> None:
        """Write response to cache.
        
        Parameters
        ----------
        cache_key : str
            Cache key identifier.
        api_format : str
            API format of the response.
        response : requests.Response
            HTTP response to cache.
        """
        path = self._cache_path(cache_key, api_format)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            if api_format == "parquet":
                path.write_bytes(response.content)
            else:  # csv or json
                path.write_text(response.text)
        except Exception as e:
            log.debug(f"Failed to write cache: {e}")

    def clear(self) -> int:
        """Clear all cached files under cache directory.

        Returns
        -------
        int
            Number of files deleted.
        """
        cache_root = Path(self.cache_dir)
        if not cache_root.exists():
            return 0

        deleted = 0
        # Remove files
        for p in cache_root.glob("**/*"):
            if p.is_file():
                try:
                    p.unlink()
                    deleted += 1
                except Exception as e:
                    log.debug(f"Failed to delete cache file {p}: {e}")

        # Attempt to remove empty directories (best-effort)
        for p in sorted(cache_root.glob("**/*"), reverse=True):
            if p.is_dir():
                try:
                    p.rmdir()
                except OSError:
                    # Directory not empty or cannot be removed; ignore
                    pass

        return deleted

    # Legacy methods for backward compatibility during transition
    def _load_from_cache(
        self, cache_key: str, resp_format: str
    ) -> Optional[pd.DataFrame]:
        """Load data from cache if it exists.
        
        Note: This is a legacy method. Use read() instead.
        """
        if not Path(self.cache_dir).exists():
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
        """Save data to cache.
        
        Note: This is a legacy method. Use write() instead.
        """
        cache_path = Path(self.cache_dir) / f"{cache_key}.{resp_format}"
        log.debug(f"Saving to cache: {cache_path}")
        if resp_format == "parquet":
            data.to_parquet(cache_path, index=False)
        elif resp_format == "csv":
            data.to_csv(cache_path, index=False)