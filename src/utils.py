"""Utility functions for logging, time handling, and data processing helpers."""

import logging
import os
import datetime
import pandas as pd
import pytz

def setup_logging(log_dir="logs"):
    """Set up logging configuration for the application.
    
    Args:
        log_dir: Directory where log files will be stored. Default is "logs".
        
    Returns:
        tuple: (logger instance, timestamp string for the log file)
    """
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"process_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(), timestamp

def get_madrid_time_range(date_str):
    """Calculate start and end datetime for a date in Europe/Madrid timezone.
    
    Correctly handles DST transitions where days can be 23, 24, or 25 hours long.
    
    Args:
        date_str: Date string in format 'YYYYMMDD'
        
    Returns:
        tuple: (start_datetime, end_datetime) in Europe/Madrid timezone
    """
    madrid = pytz.timezone('Europe/Madrid')
    # Naive date
    dt_date = datetime.datetime.strptime(date_str, "%Y%m%d").date()
    
    # Localize 00:00 of the current day
    start = madrid.localize(datetime.datetime.combine(dt_date, datetime.time.min))
    
    # Localize 00:00 of the NEXT day (not start + 24h, to handle DST properly)
    next_date = dt_date + datetime.timedelta(days=1)
    end = madrid.localize(datetime.datetime.combine(next_date, datetime.time.min))
    
    return start, end


def find_unit_column(df, candidates=None):
    """
    Find unit column in DataFrame using common candidate names.
    
    Args:
        df: DataFrame to search
        candidates: List of candidate column names (uppercase). If None, uses default list.
        
    Returns:
        Column name if found, None otherwise.
    """
    if candidates is None:
        candidates = [
            'UNIDAD DE PROGRAMACIÓN', 'UNIDAD DE PROGRAMACION', 
            'PARTICIPANTE DEL MERCADO', 'CODIGO', 'CODUOG', 
            'CÓDIGO', 'UP', 'UNIDAD', 'UNIT'
        ]
    
    return next((c for c in df.columns if str(c).upper() in candidates), None)
