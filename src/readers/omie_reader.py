import pandas as pd
import zipfile
import os
import logging
from io import StringIO
import csv

def parse_omie_standard(content, skip_lines, names):
    """
    Parses standard OMIE semicolon files (PDBC, PDVD, etc).
    """
    # OMIE files often end with * or have weird footers.
    # We'll use csv reader manually or pandas with careful checking.
    
    # Filter out empty lines or footer lines (starting with *)
    lines = [l for l in content.splitlines() if l.strip() and not l.strip().startswith('*')]
    
    # Skip Header Lines
    if len(lines) > skip_lines:
        lines = lines[skip_lines:]
    else:
        return pd.DataFrame()
        
    df = pd.read_csv(StringIO('\n'.join(lines)), sep=';', names=names, engine='python', index_col=False)
    # Remove last column if it's NaNs (due to trailing ;)
    if not df.empty and df.iloc[:, -1].isna().all():
        df = df.iloc[:, :-1]
        
    return df

def parse_trades(content):
    """
    Parses MIC Trades file.
    Looking for header line "Fecha;Contrato..."
    """
    lines = content.splitlines()
    header_idx = -1
    for i, line in enumerate(lines):
        if line.startswith("Fecha;Contrato") or line.startswith("Date;Contract"):
            header_idx = i
            break
            
    if header_idx == -1:
        return pd.DataFrame()
        
    # Read from header
    valid_lines = [l for l in lines[header_idx:] if l.strip() and not l.strip().startswith('*')]
    df = pd.read_csv(StringIO('\n'.join(valid_lines)), sep=';', engine='python')
    return df

def read_omie_file(zip_path, inner_filename_prefix):
    """
    Reads OMIE flat files from zip.
    """
    logger = logging.getLogger()
    try:
        if not os.path.exists(zip_path):
            logger.warning(f"File not found: {zip_path}")
            return pd.DataFrame()

        with zipfile.ZipFile(zip_path, 'r') as z:
            candidates = [f for f in z.namelist() if inner_filename_prefix in f]
            
            if not candidates:
                return pd.DataFrame()
            
            def get_version(fname):
                try:
                    ext = fname.split('.')[-1]
                    if ext.lower() == 'v': # .v is not numeric, maybe version 1? Usually files are .1, .2
                        return 0 # Prefer explicit numbers? Or .v is valid? Prompt said ".v"
                        # Prompt: "pdbc_[...].v"
                        # The extension might be literal 'v'? Or '1'?
                        # Let's assume numeric sort works, 'v' might need handling.
                        # If extension is 'v', treat as final?
                        return 999 
                    return int(ext)
                except:
                    return 0
            
            best_file = max(candidates, key=get_version)
            
            with z.open(best_file) as f:
                content = f.read().decode('latin-1', errors='replace') 
                
                # Determine type by filename (or mapped type)
                fname_lower = best_file.lower()
                
                # 1. PDBC (Program Daily Base Casación)
                # Structure: PDBC; (Line 1). Data starts line 2?
                # Cols: Year;Month;Day;Period;Unit;Power;...
                if 'pdbc' in fname_lower and 'marginal' not in fname_lower:
                    # Skip 1 line ("PDBC;")
                    names = ['Year', 'Month', 'Day', 'Period', 'Unit', 'Quantity', 'Unused', 'Type', 'NumOf']
                    return parse_omie_standard(content, 1, names)
                    
                # 2. MARGINAL PDBC
                # Structure: MARGINALPDBC; (Line 1). Data line 2.
                # Cols: Year;Month;Day;Period;MarginalPT;MarginalES
                elif 'marginalpdbc' in fname_lower:
                    names = ['Year', 'Month', 'Day', 'Period', 'MarginalPT', 'MarginalES']
                    return parse_omie_standard(content, 1, names)
                    
                # 3. PDVD (Viable Daily)
                # Structure: Line 1 PDVD;. Line 2 Info. Line 3 Data using specs.
                # Cols: Year;Month;Day;Period;Unit;Power;Type
                elif 'pdvd' in fname_lower:
                    names = ['Year', 'Month', 'Day', 'Period', 'Unit', 'Quantity', 'Type']
                    return parse_omie_standard(content, 2, names)
                    
                # 4. PIBCI (Intraday)
                # Structure: Line 1 PIBCI; Data Line 2? 
                # Specs do not show Line 2 info, just "PIBCI;".
                # Cols: Year;Month;Day;Period;Session;Unit;Quantity;Flag;Type
                elif 'pibci' in fname_lower:
                    names = ['Year', 'Month', 'Day', 'Period', 'Session', 'Unit', 'Quantity', 'Flag', 'Type']
                    return parse_omie_standard(content, 1, names)
                    
                # 5. TRADES (MIC)
                # Header lookup
                elif 'trades' in fname_lower:
                    return parse_trades(content)

                # Fallback
                return pd.read_csv(StringIO(content), sep=';', skipfooter=1, engine='python')

    except Exception as e:
        logger.error(f"Error reading {zip_path}: {e}")
        return pd.DataFrame()
