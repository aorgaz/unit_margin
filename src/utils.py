import logging
import os
import datetime
import pandas as pd
import pytz

def setup_logging(log_dir="logs"):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
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
    """
    Returns start and end datetime for a given date string 'YYYYMMDD' in Europe/Madrid timezone.
    Important: Handles 23, 24, or 25 hours depending on DST.
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


