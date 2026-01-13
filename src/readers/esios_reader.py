import pandas as pd
import logging
import os

def read_esios_indicator(file_path):
    """
    Reads ESIOS indicator CSV.
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

