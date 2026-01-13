"""Reader module for ESIOS indicator data files.

Handles reading CSV files containing ESIOS price indicators.
"""

import pandas as pd
import logging
import os

def read_esios_indicator(file_path):
    """Read ESIOS indicator CSV file.
    
    Args:
        file_path: Path to the ESIOS CSV file
        
    Returns:
        DataFrame containing indicator data, or empty DataFrame if not found/error
    """
    logger = logging.getLogger()
    try:
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}")
            return pd.DataFrame()
            
        try:
            return pd.read_csv(file_path, sep=',', encoding='utf-8')
        except UnicodeDecodeError:
            return pd.read_csv(file_path, sep=',', encoding='latin-1')
            
    except Exception as e:
        logger.error(f"Error reading indicator {file_path}: {e}")
        return pd.DataFrame()

