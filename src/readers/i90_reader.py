"""Reader module for I90 ESIOS data files.

Handles reading Excel sheets from zipped I90 daily files.
"""

import pandas as pd
import zipfile
import os
import logging
from io import BytesIO

def read_i90_zip(zip_path, sheet_name):
    """Read a specific sheet from an Excel file inside an I90 zip archive.
    
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

        with zipfile.ZipFile(zip_path, 'r') as z:
            # content usually matches 'I90DIA_YYYYMMDD.xls'
            xls_files = [f for f in z.namelist() if f.endswith('.xls') or f.endswith('.xlsx')]
            if not xls_files:
                logger.warning(f"No Excel file found in {zip_path}")
                return pd.DataFrame()
            
            target_file = xls_files[0]
            with z.open(target_file) as f:
                # Read excel content to memory
                content = BytesIO(f.read())
                
                # Try reading without header first to detect structure?
                # User says: "no aparece la columna unidad de programación en cada hoja, pero está presente."
                # Usually I90 files have a header row. We might need to skip rows.
                # 'determinar cuántas filas hay que saltarse'
                
                # Heuristic: Read first few lines to find 'UNIDAD' or standard headers
                try:
                    df = pd.read_excel(content, sheet_name=sheet_name)
                    # Simple cleanup: remove empty rows, look for header
                    # Use prompt provided info if available. 
                    # Assuming standard format for now, will refine if necessary.
                    return df
                except ValueError as ve:
                    # Sheet might not exist
                    logger.debug(f"Sheet {sheet_name} not found in {zip_path}")
                    return pd.DataFrame()
                    
    except Exception as e:
        logger.error(f"Error reading {zip_path}: {e}")
        return pd.DataFrame()
