"""
File caching module to avoid redundant I/O operations.

This module provides a cache manager that stores DataFrames from file reads
to eliminate redundant reads of the same files within a single day's processing.
"""

import pandas as pd
from typing import Callable, Dict, Tuple, Optional
from src.readers import i90_reader


class FileCacheManager:
    """Manages file-level caching to avoid redundant I/O operations within a single day."""
    
    def __init__(self):
        self._i90_cache: Dict[str, Dict[str, pd.DataFrame]] = {}  # zip_path -> {sheet_name -> DataFrame}
        self._esios_cache: Dict[str, pd.DataFrame] = {}  # file_path -> DataFrame
        self._omie_cache: Dict[Tuple[str, str], pd.DataFrame] = {}  # (zip_path, prefix) -> DataFrame
    
    def clear(self):
        """Clear all caches. Call this at the start of each new day."""
        self._i90_cache.clear()
        self._esios_cache.clear()
        self._omie_cache.clear()
        i90_reader.clear_workbook_cache()
    
    def get_i90_sheet(self, zip_path: str, sheet_name: str, reader_func: Callable) -> pd.DataFrame:
        """
        Get I90 sheet from cache or read it.
        
        Args:
            zip_path: Path to the I90 zip file
            sheet_name: Name of the Excel sheet to read
            reader_func: Function to call if cache miss (signature: func(zip_path, sheet_name))
            
        Returns:
            DataFrame with the sheet data (a copy to prevent cache corruption)
        """
        if zip_path not in self._i90_cache:
            self._i90_cache[zip_path] = {}
        
        if sheet_name not in self._i90_cache[zip_path]:
            self._i90_cache[zip_path][sheet_name] = reader_func(zip_path, sheet_name)
        
        return self._i90_cache[zip_path][sheet_name].copy()
    
    def get_esios_indicator(self, file_path: str, reader_func: Callable) -> pd.DataFrame:
        """
        Get ESIOS indicator from cache or read it.
        
        Args:
            file_path: Path to the ESIOS CSV file
            reader_func: Function to call if cache miss (signature: func(file_path))
            
        Returns:
            DataFrame with the indicator data (a copy to prevent cache corruption)
        """
        if file_path not in self._esios_cache:
            self._esios_cache[file_path] = reader_func(file_path)
        
        return self._esios_cache[file_path].copy()
    
    def get_omie_file(self, zip_path: str, prefix: str, reader_func: Callable) -> pd.DataFrame:
        """
        Get OMIE file from cache or read it.
        
        Args:
            zip_path: Path to the OMIE zip file
            prefix: Filename prefix to search for within the zip
            reader_func: Function to call if cache miss (signature: func(zip_path, prefix))
            
        Returns:
            DataFrame with the OMIE data (a copy to prevent cache corruption)
        """
        key = (zip_path, prefix)
        if key not in self._omie_cache:
            self._omie_cache[key] = reader_func(zip_path, prefix)
        
        return self._omie_cache[key].copy()
