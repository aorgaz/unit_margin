"""Main entry point for unit margin processing.

This script processes energy market data to calculate unit margins across
multiple markets and date ranges.
"""

import argparse
import time
from src import utils, engine, config

def main():
    """Execute the unit margin processing workflow.
    
    Configures processing parameters, runs the engine, and logs results.
    Processing parameters can be set manually in this function for debugging.
    """
    logger, timestamp = utils.setup_logging()
    
    # --- MANUAL CONFIGURATION (For Debugging) ---
    # Change these values directly instead of passing command line arguments
    # Set to None to use defaults or if not applicable
    manual_years = None #config.TARGET_YEARS  # Example: [2024, 2025]
    manual_start_date = "20230325"            # Example: "20240101" (YYYYMMDD)
    manual_end_date = "20230328"              # Example: "20240131" (YYYYMMDD)
    manual_markets = None # Example: ["PDBF", "PDBC"] - Set to None to run all
    manual_target_units = config.TARGET_UNITS # Example: ['GUIG', 'GUIB'] or None to process all
    #manual_markets = ["RR Subir", "RR Bajar", "Restricciones tecnicas Subir", "Restricciones tecnicas Bajar", "Restricciones TR Subir", "Restricciones TR Bajar"] # Example: ["PDBF", "PDBC"] - Set to None to run all
    #manual_markets = ["Bilaterales", "PDBC", "PDBF", "Restricciones tecnicas", "PDVP", "PDVD", "MIC"]
    # -------------------------------------------

    # Argparse section commented out
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--years', nargs='+', type=int, default=None)
    # parser.add_argument('--start-date', type=str, help='YYYYMMDD')
    # parser.add_argument('--end-date', type=str, help='YYYYMMDD')
    # args = parser.parse_args()
    
    start_time = time.time()
    
    import datetime
    s_date = None
    e_date = None
    
    if manual_start_date:
        s_date = datetime.datetime.strptime(manual_start_date, "%Y%m%d").date()
    if manual_end_date:
        e_date = datetime.datetime.strptime(manual_end_date, "%Y%m%d").date()
        
    # Default to config years if no args provided
    if not manual_years and not s_date:
        manual_years = config.TARGET_YEARS
    
    try:
        engine.run_process(start_date=s_date, end_date=e_date, years=manual_years, target_markets=manual_markets, target_units=manual_target_units)
    except Exception as e:
        logger.exception("Fatal error in main process")
        
    duration = time.time() - start_time
    logger.info(f"Total Duration: {duration:.2f}s")
    print(f"Process finished. Check logs/process_{timestamp}.log")

if __name__ == "__main__":
    main()
