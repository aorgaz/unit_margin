"""Reader module for I90 ESIOS data files.

Handles reading Excel sheets from zipped I90 daily files.
"""

import pandas as pd
import zipfile
import os
import logging
from io import BytesIO

# Module-level cache for Excel workbook data
_workbook_cache = {}

def read_i90_zip(zip_path, sheet_name):
    """Read a specific sheet from an Excel file inside an I90 zip archive.
    
    This function caches the Excel file content in memory to avoid repeatedly
    opening the same file when reading multiple sheets.
    
    Args:
        zip_path: Path to the I90 zip file
        sheet_name: Name of the Excel sheet to read
        
    Returns:
        DataFrame containing the sheet data, or empty DataFrame if not found/error
    """
    logger = logging.getLogger()
    try:
        if not os.path.exists(zip_path):
            logger.warning(f"File not found: {zip_path}")
            return pd.DataFrame()

        # Check if workbook content is already cached
        if zip_path not in _workbook_cache:
            with zipfile.ZipFile(zip_path, 'r') as z:
                xls_files = [f for f in z.namelist() if f.endswith('.xls') or f.endswith('.xlsx')]
                if not xls_files:
                    logger.warning(f"No Excel file found in {zip_path}")
                    return pd.DataFrame()
                
                target_file = xls_files[0]
                with z.open(target_file) as f:
                    # Cache the entire Excel file content in memory
                    _workbook_cache[zip_path] = BytesIO(f.read())
                    logger.debug(f"Cached Excel workbook from {zip_path}")
        
        # Read the specific sheet from the cached workbook
        # Reset position to beginning for each read
        _workbook_cache[zip_path].seek(0)
        
        try:
            df = pd.read_excel(_workbook_cache[zip_path], sheet_name=sheet_name, engine=None)
            return df
        except ValueError as ve:
            # Sheet might not exist
            logger.debug(f"Sheet {sheet_name} not found in {zip_path}")
            return pd.DataFrame()
                    
    except Exception as e:
        logger.error(f"Error reading {zip_path}: {e}")
        return pd.DataFrame()

def clear_workbook_cache():
    """Clear the workbook cache to free memory."""
    global _workbook_cache
    _workbook_cache = {}
