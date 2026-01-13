"""Core processing engine for unit margin calculation.

This module contains the main logic for:
- Reading quantity and price data from various sources (I90, OMIE, ESIOS)
- Processing and transforming market data
- Calculating unit margins across multiple markets
"""

import pandas as pd
import numpy as np
import os
import logging
import datetime
import pytz
from src import config, processing, utils, file_cache
from src.readers import i90_reader, omie_reader, esios_reader

def get_price_data(mkt, date_obj, cache_manager):
    """Retrieve price data based on market configuration.
    
    Args:
        mkt: Market configuration dictionary
        date_obj: Date object for which to retrieve prices
        cache_manager: FileCacheManager instance for caching file reads
        
    Returns:
        DataFrame with price data including Datetime_Madrid and Price columns
    """
    logger = logging.getLogger()
    year = date_obj.year
    ymd = date_obj.strftime("%Y%m%d")
    
    price_df = pd.DataFrame()
    
    source = mkt.get('price_source')
    
    if source == 'indicator':
        price_id = mkt['price_id']
        
        # Dynamic Rules
        if price_id == 'BANDA_SUBIR_RULE':
            # Switch date 20-11-2024
            # 634 until (inclusive), 2130 from
            switch_date = datetime.date(2024, 11, 20)
            if date_obj <= switch_date:
                price_id = 634
            else:
                price_id = 2130
        elif price_id == 'MFRR_SUBIR_RULE':
            # Switch date 10-12-2024
            # 677 until (inclusive), 2197 from
            switch_date = datetime.date(2024, 12, 10)
            if date_obj <= switch_date:
                price_id = 677
            else:
                price_id = 2197
        elif price_id == 'MFRR_BAJAR_RULE':
            # Switch date 10-12-2024
            # 676 until (inclusive), 2197 from
            switch_date = datetime.date(2024, 12, 10)
            if date_obj <= switch_date:
                price_id = 676
            else:
                price_id = 2197
            
        if isinstance(price_id, int) or isinstance(price_id, str):
            fname = f"{price_id}_{year}_{date_obj.month}.csv"
            fpath = os.path.join(config.PATH_ESIOS_IND, str(price_id), fname)
            price_df = cache_manager.get_esios_indicator(fpath, esios_reader.read_esios_indicator)
            
            # Standardize Price DF Timestamp
            if not price_df.empty:
                # ESIOS indicators structure: 'datetime', 'value', 'geo_id', etc.
                if 'datetime' in price_df.columns:
                    # Parse to datetime (UTC) to handle potential mixed offsets, then convert to Madrid
                    price_df['Datetime_Madrid'] = pd.to_datetime(price_df['datetime'], utc=True).dt.tz_convert('Europe/Madrid')
                    
                    # Filter by geo_id
                    # 612..618 -> geo_id 3 (Spain)
                    # Other -> geo_id 8741 (Peninsula)
                    # Note: price_id might be a string if rules applied
                    pid_int = int(price_id) if str(price_id).isdigit() else 0
                    
                    if 612 <= pid_int <= 618:
                         target_geo = 3
                    else:
                         target_geo = 8741
                    
                    if 'geo_id' in price_df.columns:
                        price_df = price_df[price_df['geo_id'] == target_geo]
                    
                    # Map 'value' to 'Price'
                    if 'value' in price_df.columns:
                        price_df['Price'] = price_df['value']
                        # Keep only necessary columns
                        price_df = price_df[['Datetime_Madrid', 'Price']]
                    else:
                        logger.warning(f"Value column missing in indicator {price_id}")

                else:
                    logger.warning(f"Datetime column missing in indicator {price_id}")
            
    elif source == 'omie':
        data_id_price = mkt.get('price_id')
        if data_id_price:
            year = date_obj.year
            zip_name = f"{data_id_price}_{year}.zip"
            # Assuming folder matches data_id_price logic (e.g. marginalpdbc folder)
            zip_path = os.path.join(config.PATH_OMIE, data_id_price, zip_name)
            
            inner_prefix = f"{data_id_price}_{ymd}"
            price_df = cache_manager.get_omie_file(zip_path, inner_prefix, omie_reader.read_omie_file)
            
            if not price_df.empty:
                # Normalize OMIE Price
                # Marginal PDBC Cols: Year;Month;Day;Period;MarginalPT;MarginalES
                # We need "Datetime_Madrid" and "Price" columns.
                
                # 1. Construct Datetime
                start_madrid, end_madrid = utils.get_madrid_time_range(ymd)
                expected_timestamps = pd.date_range(start=start_madrid, end=end_madrid, freq='h', inclusive='left') # Marginal is hourly
                
                # Map Period 1..N -> Timestamps
                if 'Period' in price_df.columns:
                    periods = sorted(price_df['Period'].unique())
                    # Safe mapping
                    ts_map = {}
                    valid_range = range(1, len(expected_timestamps) + 1)
                    for i, p in enumerate(valid_range):
                        ts_map[p] = expected_timestamps[i]
                        
                    price_df['Datetime_Madrid'] = price_df['Period'].map(ts_map)
                    price_df = price_df.dropna(subset=['Datetime_Madrid'])
                    
                    # 2. Extract Price
                    # "MarginalES" is usually the price for Spain.
                    if 'MarginalES' in price_df.columns:
                        price_df['Price'] = price_df['MarginalES']
                    elif 'Price' not in price_df.columns:
                        # Fallback
                        price_df['Price'] = np.nan
                else:
                    logger.warning(f"Period column missing in OMIE Price {data_id_price}")
    
    elif source == 'i90':
        # Read I90 Price Data (same zip as quantity, different sheet)
        price_id = mkt.get('price_id')
        if price_id:
            zip_name = f"I90DIA_{ymd}.zip"
            zip_path = os.path.join(config.PATH_ESIOS_I90, f"i90_{year}", zip_name)
            
            sheet_ids = price_id if isinstance(price_id, list) else [price_id]
            
            dfs = []
            for sheet in sheet_ids:
                d = cache_manager.get_i90_sheet(zip_path, sheet, i90_reader.read_i90_zip)
                if not d.empty:
                    dfs.append(d)
            
            if dfs:
                price_df = pd.concat(dfs)
                
                if not price_df.empty:
                    # Process similar to quantity data
                    price_df = price_df.dropna(subset=[price_df.columns[0]])
                    
                    if not price_df.empty:
                        price_df.columns = price_df.iloc[0]
                        price_df = price_df[1:]
                        
                    # Sanitize columns
                    price_df.columns = [str(c).strip() for c in price_df.columns]
                    price_df.columns = [str(int(float(c))) if c.replace('.','',1).isdigit() and c.endswith('.0') else c for c in price_df.columns]
                    
                    # Apply filters (supports multiple formats)
                    # New format with separate filters: "price_filters": {"Sentido": "Subir", "Tipo": "RR"}
                    # Unified format: "filters": {"Sentido": "Subir"}
                    # Old format: "filter_col": "Sentido", "filter_val": "Subir"
                    filters = mkt.get('price_filters') or mkt.get('filters', {})
                    if not filters:
                        filter_col = mkt.get('filter_col')
                        filter_val = mkt.get('filter_val')
                        if filter_col and filter_val:
                            filters = {filter_col: filter_val}
                    
                    for col, val in filters.items():
                        if col in price_df.columns:
                            price_df = price_df[price_df[col] == val]
                    
                    if not price_df.empty:
                        # Process timestamps
                        start_madrid, end_madrid = utils.get_madrid_time_range(ymd)
                        duration_hours = int((end_madrid - start_madrid).total_seconds() / 3600)
                        
                        col_to_ts = {}
                        has_hourly_sig = '00-01' in price_df.columns
                        has_quarterly_sig = ('1' in price_df.columns and '4' in price_df.columns) or (1 in price_df.columns and 4 in price_df.columns)
                        
                        cols = []
                        timestamp_freq = 'h'
                        
                        if has_hourly_sig:
                            timestamp_freq = 'h'
                            if duration_hours == 24:
                                cols = [f"{h:02d}-{h+1:02d}" for h in range(24)]
                            elif duration_hours == 25:
                                cols = [f"{h:02d}-{h+1:02d}" for h in range(25)]
                            elif duration_hours == 23:
                                cols = [f"{h:02d}-{h+1:02d}" for h in range(23)]
                        elif has_quarterly_sig:
                            timestamp_freq = '15min'
                            num_quarters = duration_hours * 4
                            cols = [str(i) for i in range(1, num_quarters + 1)]
                        
                        if cols:
                            expected_timestamps = pd.date_range(start=start_madrid, end=end_madrid, freq=timestamp_freq, inclusive='left')
                            for i, col_name in enumerate(cols):
                                if i < len(expected_timestamps):
                                    col_to_ts[col_name] = expected_timestamps[i]
                            
                            val_cols = [c for c in col_to_ts.keys() if c in price_df.columns]
                            
                            if val_cols:
                                base_cols = [c for c in price_df.columns if c not in val_cols]
                                price_df = price_df.melt(id_vars=base_cols, value_vars=val_cols, var_name='PeriodLabel', value_name='Price')
                                price_df = price_df.dropna(subset=['Price'])
                                price_df['Datetime_Madrid'] = price_df['PeriodLabel'].map(col_to_ts)
                                price_df = price_df.dropna(subset=['Datetime_Madrid'])
                                
                                # Keep only necessary columns
                                # Include Unit for sheets with unit-specific pricing (I90DIA09, I90DIA10)
                                # Include Sentido if it exists for merging
                                keep_cols = ['Datetime_Madrid', 'Price']
                                
                                unit_col = utils.find_unit_column(price_df)
                                if unit_col:
                                    price_df['Unit'] = price_df[unit_col]
                                    keep_cols.insert(0, 'Unit')
                                
                                if 'Sentido' in price_df.columns:
                                    keep_cols.insert(0, 'Sentido')
                                
                                price_df = price_df[keep_cols]
         
    return price_df

def process_market(mkt, date_obj, target_units, cache_manager):
    """Process a single market for a specific date.
    
    This function orchestrates the complete processing workflow:
    1. Read quantity data (similar structure to get_price_data)
    2. Read price data (via get_price_data)
    3. Merge quantity and price
    4. Calculate margins
    
    NOTE: Quantity reading logic (section 1) mirrors the structure of get_price_data()
    with separate handling for I90, OMIE, and special markets (MIC, PIBC).
    
    Args:
        mkt: Market configuration dictionary
        date_obj: Date object to process
        target_units: List of unit codes to include
        cache_manager: FileCacheManager instance for caching file reads
        
    Returns:
       DataFrame with processed market data including margins
    """
    logger = logging.getLogger()
    ymd = date_obj.strftime("%Y%m%d")
    ym = date_obj.strftime("%Y%m")
    year = date_obj.year
    
    market_name = mkt['market']
    data_source = mkt['source']
    
    # ============================================================================
    # SECTION 1: READ QUANTITY DATA
    # ============================================================================
    # This section reads quantity data from various sources (I90, OMIE)
    # Structure mirrors get_price_data() for consistency
    # ============================================================================
    
    df_qty = pd.DataFrame()
    
    try:
        # --- I90 Source: Read from zipped Excel files ---
        if data_source == 'i90':
            zip_name = f"I90DIA_{ymd}.zip"
            zip_path = os.path.join(config.PATH_ESIOS_I90, f"i90_{year}", zip_name)
            
            sheet_ids = mkt['data_id']
            if not isinstance(sheet_ids, list):
                sheet_ids = [sheet_ids]
            
            dfs = []
            for sheet in sheet_ids:
                d = cache_manager.get_i90_sheet(zip_path, sheet, i90_reader.read_i90_zip)
                if not d.empty:
                    dfs.append(d)
            
            if dfs:
                df_qty = pd.concat(dfs)
                
                if not df_qty.empty:
                    # Drop rows with NaN in the first column
                    df_qty = df_qty.dropna(subset=[df_qty.columns[0]])

                    unit_col = None
                    if not df_qty.empty:
                        # Set headers as the first line after dropping NaNs
                        df_qty.columns = df_qty.iloc[0]
                        df_qty = df_qty[1:]
                        
                    # Sanitize columns
                    df_qty.columns = [str(c).strip() for c in df_qty.columns]
                    # Handle float-like headers (e.g. '1.0' -> '1')
                    df_qty.columns = [str(int(float(c))) if c.replace('.','',1).isdigit() and c.endswith('.0') else c for c in df_qty.columns]
                    
                    unit_col = utils.find_unit_column(df_qty)
                    
                    if unit_col:
                        df_qty = df_qty[df_qty[unit_col].isin(target_units)]
                    
                    # Apply filters (supports multiple formats)
                    # New format with separate filters: "quantity_filters": {"Sentido": "Subir", "Redespacho": "RR"}
                    # Unified format: "filters": {"Sentido": "Subir", "Redespacho": "RR"}
                    # Old format: "filter_col": "Sentido", "filter_val": "Subir"
                    filters = mkt.get('quantity_filters') or mkt.get('filters', {})
                    if not filters:
                        # Backward compatibility with old format
                        filter_col = mkt.get('filter_col')
                        filter_val = mkt.get('filter_val')
                        if filter_col and filter_val:
                            filters = {filter_col: filter_val}
                    
                    for col, val in filters.items():
                        if col in df_qty.columns:
                            df_qty = df_qty[df_qty[col] == val]
                    
                    if not df_qty.empty:
                        # New Logic for I90 Formats (Hourly vs Quarterly)
                        
                        start_madrid, end_madrid = utils.get_madrid_time_range(ymd)
                        duration_hours = int((end_madrid - start_madrid).total_seconds() / 3600)
                        
                        # Generate Expected Column Names mapping to Timestamps
                        col_to_ts = {}
                        
                        # Check for existence of specific signatures
                        # Hourly Signature: "00-01"
                        # Quarterly Signature: "1", "2"
                        
                        has_hourly_sig = '00-01' in df_qty.columns
                        # Check for '1' but careful it might be somewhere else. 
                        # Quarterly logic relies on columns 1..N being present.
                        has_quarterly_sig = ('1' in df_qty.columns and '4' in df_qty.columns) or (1 in df_qty.columns and 4 in df_qty.columns)
                        
                        cols = []
                        timestamp_freq = 'h'
                        
                        if has_hourly_sig:
                            timestamp_freq = 'h'
                            # Generate Expected Hourly Cols
                            # 24h
                            if duration_hours == 24:
                                cols = [f"{h:02d}-{h+1:02d}" for h in range(24)]
                            # 25h (DST End / Oct)
                            elif duration_hours == 25:
                                cols = [f"{h:02d}-{h+1:02d}" for h in range(2)] # 00-01, 01-02
                                cols.append("02-03a")
                                cols.append("02-03b")
                                cols.extend([f"{h:02d}-{h+1:02d}" for h in range(3, 24)])
                            # 23h (DST Start / Mar)
                            elif duration_hours == 23:
                                cols = [f"{h:02d}-{h+1:02d}" for h in range(2)] # 00-01, 01-02
                                # Skip 02-03
                                cols.extend([f"{h:02d}-{h+1:02d}" for h in range(3, 24)])
                                
                        elif has_quarterly_sig:
                            timestamp_freq = '15min'
                            # Generate Expected Quarterly Cols
                            if duration_hours == 24:
                                # 1..96
                                cols = [str(i) for i in range(1, 97)]
                            elif duration_hours == 25:
                                # 1..100
                                cols = [str(i) for i in range(1, 101)]
                            elif duration_hours == 23:
                                # 1..8, 13..96
                                cols = [str(i) for i in range(1, 9)]
                                cols.extend([str(i) for i in range(13, 97)])
                        
                        if cols:
                            # Generate Timestamps
                            # Note: pd.date_range logic must match duration_hours exactly
                            ts_list = pd.date_range(start=start_madrid, end=end_madrid, freq=timestamp_freq, inclusive='left')
                            
                            if len(cols) == len(ts_list):
                                for c, ts in zip(cols, ts_list):
                                    col_to_ts[c] = ts
                            else:
                                logger.warning(f"I90 Format mismatch (Hours: {duration_hours}): Generated {len(cols)} cols vs {len(ts_list)} timestamps")
                        
                        if col_to_ts:
                            # Unpivot
                            # Identify value columns that exist in the dataframe
                            val_cols = [c for c in col_to_ts.keys() if c in df_qty.columns]
                            
                            if val_cols:
                                base_cols = [c for c in df_qty.columns if c not in val_cols]
                                df_qty = df_qty.melt(id_vars=base_cols, value_vars=val_cols, var_name='PeriodLabel', value_name='Quantity')
                                df_qty = df_qty.dropna(subset=['Quantity'])
                                df_qty['Datetime_Madrid'] = df_qty['PeriodLabel'].map(col_to_ts)
                                df_qty = df_qty.dropna(subset=['Datetime_Madrid'])
                                df_qty = df_qty[df_qty['Quantity'] != 0]
                                
                                fixed_offset = datetime.timezone(datetime.timedelta(hours=1))
                                df_qty['Datetime_UTC1'] = df_qty['Datetime_Madrid'].apply(lambda x: x.astimezone(fixed_offset))
                            else:
                                logger.warning(f"No valid time columns found for I90 in {market_name} (Expected cols like {cols[:3]}...)")
                        else:
                            # Fallback or Log
                            pass # If we couldn't map, we return empty or partial?
                            # logger.warning(f"Could not determine I90 format for {market_name}")
        
        # --- OMIE Source: Read from zipped semicolon files ---
        elif data_source == 'omie':
            # Read OMIE Data
            data_id = mkt.get('data_id')
            zip_name = f"{data_id}_{ym}.zip"
            zip_path = os.path.join(config.PATH_OMIE, data_id, zip_name)
            
            inner_prefix = f"{data_id}_{ymd}"
            df_qty = cache_manager.get_omie_file(zip_path, inner_prefix, omie_reader.read_omie_file)

            # The reader now returns standard columns for known types:
            # Year, Month, Day, Period, [Unit], [Quantity]...
            
            cols = df_qty.columns
            # Normalize cols just in case
            cols_upper = [str(c).upper() for c in cols]
            df_qty.columns = cols_upper
            
            # Identify Unit Col
            unit_col = utils.find_unit_column(df_qty, candidates=['UNIT', 'CODIGO', 'CODUOG', 'CÓDIGO'])
            
            # Identify Date Cols
            has_ymd = all(c in df_qty.columns for c in ['YEAR', 'MONTH', 'DAY', 'PERIOD'])
            
            if market_name == 'MIC':
                # Parse Dates from CONTRATO Column
                # Format: YYYYMMDD HH:MM-YYYYMMDD HH:MM (Start-End)
                # Handle 25h days: Endings with 'A' (DST) or 'B' (STD)
                
                def parse_contract_date(val):
                    try:
                        # Take start time part
                        start_part = val.split('-')[0].strip()
                        
                        is_dst = None
                        if start_part.endswith('A'):
                            is_dst = True
                            start_part = start_part[:-1]
                        elif start_part.endswith('B'):
                            is_dst = False
                            start_part = start_part[:-1]
                            
                        dt = datetime.datetime.strptime(start_part, "%Y%m%d %H:%M")
                        madrid = pytz.timezone('Europe/Madrid')
                        
                        if is_dst is not None:
                            return madrid.localize(dt, is_dst=is_dst)
                        else:
                            # For normal days or non-ambiguous times
                            # localize raises AmbiguousTimeError if strictly ambiguous and is_dst=None
                            # But OMIE usually disambiguates via A/B in the file text during shift.
                            # If we hit ambiguity without suffix, it's a file anomaly, but we try standard.
                            return madrid.localize(dt)
                    except Exception:
                        return pd.NaT
 
                if 'CONTRATO' in df_qty.columns:
                    # Optimize: Parse unique values
                    unique_dates = df_qty['CONTRATO'].unique()
                    date_map = {d: parse_contract_date(d) for d in unique_dates}
                    df_qty['Datetime_Madrid'] = df_qty['CONTRATO'].map(date_map)
                    df_qty = df_qty.dropna(subset=['Datetime_Madrid'])
                    
                    # UTC+1
                    fixed_offset = datetime.timezone(datetime.timedelta(hours=1))
                    df_qty['Datetime_UTC1'] = df_qty['Datetime_Madrid'].apply(lambda x: x.astimezone(fixed_offset))
                    
                    target_set = set(target_units)
                    
                    # 1. Sell Side (Income -> Positive Qty)
                    # Filter UNIDAD VENTA (Unidad venta)
                    # Normalized to uppercase -> UNIDAD VENTA
                    mask_sell = df_qty['UNIDAD VENTA'].isin(target_set)
                    df_sell = df_qty[mask_sell].copy()
                    df_sell['Unit'] = df_sell['UNIDAD VENTA']
                    df_sell['Quantity'] = pd.to_numeric(df_sell['CANTIDAD'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
                    df_sell['Price'] = pd.to_numeric(df_sell['PRECIO'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
                    
                    # 2. Buy Side (Expense -> Negative Qty)
                    # Filter UNIDAD COMPRA (Unidad compra)
                    # Normalized to uppercase -> UNIDAD COMPRA
                    mask_buy = df_qty['UNIDAD COMPRA'].isin(target_set)
                    df_buy = df_qty[mask_buy].copy()
                    df_buy['Unit'] = df_buy['UNIDAD COMPRA']
                    df_buy['Quantity'] = -pd.to_numeric(df_buy['CANTIDAD'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
                    df_buy['Price'] = pd.to_numeric(df_buy['PRECIO'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
                    
                    # Combine
                    combined = pd.concat([df_sell[['Unit', 'Datetime_Madrid', 'Datetime_UTC1', 'Quantity', 'Price']], 
                                        df_buy[['Unit', 'Datetime_Madrid', 'Datetime_UTC1', 'Quantity', 'Price']]])
                                        
                    if not combined.empty:
                        # Calculate Margin components for aggregation
                        combined['Amount'] = combined['Quantity'] * combined['Price']
                        
                        # Group By Unit, Date
                        grp_cols = ['Unit', 'Datetime_Madrid']
                        if 'Datetime_UTC1' in combined.columns:
                            grp_cols.append('Datetime_UTC1')
                            
                        agg_df = combined.groupby(grp_cols, as_index=False).agg({
                            'Quantity': 'sum',
                            'Amount': 'sum'
                        })
                        
                        # Weighted Average Price = Sum(Amount) / Sum(Quantity)
                        # Handle div by 0
                        agg_df['Price'] = agg_df.apply(lambda row: row['Amount'] / row['Quantity'] if row['Quantity'] != 0 else 0, axis=1)
                        agg_df['Margin'] = agg_df['Amount']
                        
                        agg_df['Market'] = market_name
                        df_qty = agg_df[['Unit', 'Datetime_Madrid', 'Datetime_UTC1', 'Quantity', 'Price', 'Margin', 'Market']]
                        return df_qty
                    else:
                        return pd.DataFrame()
                        
                return pd.DataFrame()
 
            elif unit_col:
                # Filter Units
                df_qty = df_qty[df_qty[unit_col].isin(target_units)].copy()
                 
            if not df_qty.empty:
                # Construct Datetime from Year/Month/Day/Period
                if has_ymd:
                    # Period 1 = 00:00. Period N = (N-1) hours offset?
                    # "Periodo de la oferta: 1-24/25/23"
                    # Yes, Period 1 is the first hour.
                    
                    # Vectorized Datetime construction
                    # CAUTION with DST:
                    # If we simply do pd.to_datetime(YMD) + Period offset, we might skip/dup 2am/3am.
                    # Correct way for OMIE:
                    # They usually output strictly 1..24, 1..23, or 1..25.
                    # We can create a naive datetime and localize?
                    # BUT 02:00A and 02:00B in October? 
                    # OMIE typically handles this by Period number.
                    #   Short day: Periods 1..23.
                    #   Long day: Periods 1..25.
                    #   Standard: 1..24.
                    
                    # We need to map (Y,M,D) to base, then add (Period-1) * Freq?
                    # If we construct strings "Y-M-D H:00", ambiguous times will fail or pick one.
                    # Since we have explicit Period, we can try to map to the Madrid timeline.
                    
                    # Strategy:
                    # 1. Group by Date.
                    # 2. For each Date, get the Madrid Time Range (Start..End).
                    # 3. Generate expected timestamps (Hourly/Quarterly).
                    # 4. Match Period 1 -> First timestamp, etc.
                    
                    # Since we iterate day by day in 'run_process', 'df_qty' SHOULD contain only one day?
                    # The OMIE zip might contain the WHOLE MONTH file?
                    # "pdbc_yyyymm.zip" -> Contains "pdbc_yyyymmdd.v".
                    # We extract specific "inner_prefix = f"{folder}_{ymd}"". 
                    # So df_qty is SINGLE DAY.
                    
                    # Fast Path for Single Day
                    start_madrid, end_madrid = utils.get_madrid_time_range(ymd)
                    
                    # Determine Freq. PDBC/marginal is Hourly. PIBCI can be Hourly.
                    # PDVD Quarter? "Programa diario viable ... desglose cuarto horario" (See Image PDVD description)
                    # Image says: "desglose cuarto horario".
                    # But columns show "Periodo 1..100"? Max 100 implies quarters (25*4=100).
                    # So we need to detect.
                    
                    max_period = df_qty['PERIOD'].max()
                    freq = '15min' if max_period > 25 else 'h'
                    
                    expected_timestamps = pd.date_range(start=start_madrid, end=end_madrid, freq=freq, inclusive='left')
                    
                    # Map Period -> Timestamp
                    # Period is 1-based index.
                    # Create dictionary
                    # Check matching lengths
                    periods = sorted(df_qty['PERIOD'].unique())
                    
                    # Safe mapping
                    # If expected has 24 items and we have Period 1..24 -> Direct map.
                    # If expected has 25 (DST end) and we have Period 1..25 -> Direct map.
                    
                    # We create a map for specific period integers to timestamps
                    # Assuming strict ordering 1..N matches 0..N-1 timestamps
                    # This relies on OMIE following sequential periods aligned with clock.
                    
                    ts_map = {}
                    # Build list of periods 1..len(expected)
                    valid_range = range(1, len(expected_timestamps) + 1)
                    for i, p in enumerate(valid_range):
                        ts_map[p] = expected_timestamps[i]
                        
                    df_qty['Datetime_Madrid'] = df_qty['PERIOD'].map(ts_map)
                    
                    # Drop rows where Period didn't map (e.g. out of bounds)
                    df_qty = df_qty.dropna(subset=['Datetime_Madrid'])
                    df_qty = df_qty[df_qty['QUANTITY'] != 0]
                    
                    # UTC+1
                    fixed_offset = datetime.timezone(datetime.timedelta(hours=1))
                    df_qty['Datetime_UTC1'] = df_qty['Datetime_Madrid'].apply(lambda x: x.astimezone(fixed_offset))
                    
                    # Rename Quantity
                    # Rename Quantity
                    if 'QUANTITY' in df_qty.columns:
                        df_qty['Quantity'] = df_qty['QUANTITY']
                    elif 'POTENCIA' in df_qty.columns:
                        df_qty['Quantity'] = df_qty['POTENCIA']
                    elif 'POTENCIA ASIGNADA' in df_qty.columns:
                        df_qty['Quantity'] = df_qty['POTENCIA ASIGNADA']

                    # Specific: PIBCI Session filtering?
                    # User: "PIBC ss1...ss7". If we have rows for multiple sessions for the same hour?
                    # Usually we take the LAST session? Or sum?
                    # "PIBC | omie | Energía | pibci...v"
                    # "PIBC ss1... precio"
                    # If we have energy from PIBCI, does it contain all sessions?
                    # File has 'NUMSES'.
                    # User table implies: "PIBC ss (Energie)" -> pibci_...v
                    # This file likely contains definitive schedule? Or increments?
                    # Image title: "Resultado incremental del intradiario". "Potencia casada... de forma incremental".
                    # If it's incremental, we might need to sum all sessions for a Target Unit?
                    # Or is it cumulative? "incorporando las modificaciones".
                    # AND "Este fichero se generará una vez por cada convocatoria... publicará una vez cumplido...".
                    # We read ONE file `pibci_...ss.v`.
                    # WAIT. User path: `pibci_[aaaammddss].[v]`.
                    # NOTE: The User path has `ss` in filename!
                    # `pibci_2024010101.v` (Session 1), `pibci_2024010102.v` (Session 2)...
                    # In config.py we mapped Data_ID='pibci'.
                    # And `omie_reader` reads `f"{folder}_{ymd}"`.
                    # It will match `pibci_20250101`... but files are `pibci_2025010101`, `02`, etc.
                    # We might be reading just ONE or getting mismatch.
                    # If we need ALL sessions, we need to iterate sessions 1..7?
                    # Config defines PIBC generally.
                    # We should probably iterate valid sessions and concat?
                    # For now, let's allow the reader to pick matching files.
                    # `read_omie_file` picks BEST version.
                    # If zip has `pibci_...01.1`, `pibci_...02.1`.
                    # Inner prefix was `pibci_yyyymmdd`.
                    # This prefix matches ALL sessions.
                    # Reader `candidates = ... if prefix in f`.
                    # Reader `best_file = max(candidates, key=version)`.
                    # This logic fails if we have multiple files (sessions) with same version logic or different logic.
                    # We probably need to read ALL sessions for PIBCI?
                    # Or is there a `pibci_...` aggregated?
                    # "Fichero con la potencia casada... generará una vez por cada convocatoria".
                    # So we have up to 7 files per day.
                    # And prices are per session (612..618).
                    # This implies we calculate Margin PER SESSION and SUM?
                    # Or we have a final schedule?
                    # Prompt: "PIBC ss | omie | Energía | pibci_aaaammddss.v"
                    # Prompt: "PIBC ss1 | indicador | Precio".
                    # Explicit mapping implies we calculate (Qty_SS1 * Price_SS1) + (Qty_SS2 * Price_SS2)...
                    if market_name == 'PIBC':
                        # Handle Sessions
                        # PIBCI file has 'SESSION' column.
                        if 'SESSION' in df_qty.columns:
                            price_map = mkt.get('price_id')
                            if isinstance(price_map, dict):
                                unique_sessions = df_qty['SESSION'].unique()
                                dfs_sess = []
                                for sess in unique_sessions:
                                    pid = price_map.get(sess)
                                    if not pid: continue
                                    
                                    fname = f"{pid}_{year}_{date_obj.month}.csv"
                                    fpath = os.path.join(config.PATH_ESIOS_IND, str(pid), fname)
                                    p_df = cache_manager.get_esios_indicator(fpath, esios_reader.read_esios_indicator)
                                    
                                    if not p_df.empty and 'datetime' in p_df.columns:
                                        p_df['Datetime_Madrid'] = pd.to_datetime(p_df['datetime'], utc=True).dt.tz_convert('Europe/Madrid')
                                        pid_int = int(pid) if str(pid).isdigit() else 0
                                        target_geo = 3 if 612 <= pid_int <= 618 else 8741
                                        if 'geo_id' in p_df.columns:
                                            p_df = p_df[p_df['geo_id'] == target_geo]
                                        if 'value' in p_df.columns:
                                            p_df['Price'] = p_df['value']
                                    
                                    d_s = df_qty[df_qty['SESSION'] == sess].copy()
                                    if not p_df.empty and 'Datetime_Madrid' in p_df.columns:
                                        merged = pd.merge(d_s, p_df[['Datetime_Madrid', 'Price']], on='Datetime_Madrid', how='left')
                                        merged['Market'] = f"{market_name} s{int(sess):02d}"
                                        dfs_sess.append(merged)
                                    else:
                                        d_s['Price'] = np.nan
                                        d_s['Market'] = f"{market_name} s{int(sess):02d}"
                                        dfs_sess.append(d_s)
                                
                                if dfs_sess:
                                    df_qty = pd.concat(dfs_sess)
                                else:
                                    return pd.DataFrame()
                        
                    # For MIC (Trades): Columns might be explicitly Sell/Buy
                    if market_name == 'MIC':
                        # If 'trades' file, distinct format?
                        # Usually columns: ... Buy/Sell ...
                        # User says: "filtrar por unidad de compra por un lado y de venta por otro"
                        
                        # Check for Buy/Sell columns
                        # Normalized cols likely: FECHA, CONTRATO, AGENTEC, UNIDADC, ... PRECIO, CANTIDAD
                        
                        seller_col = 'UNIDADV'
                        buyer_col = 'UNIDADC'
                        price_col_mic = 'PRECIO'
                        qty_col_mic = 'CANTIDAD'
                        
                        # Ensure cols exist
                        if all(c in df_qty.columns for c in [seller_col, buyer_col, price_col_mic, qty_col_mic]):
                            # Trades are usually specific instants.
                            # We need to aggregate to Hourly/Quarterly?
                            # Prompt: "aunque salga a nivel segundo habrá que quedarse a nivel hora o minuto"
                            # "Datettime Madrid" needed.
                            # We have 'FECHA' (dd/mm/yyyy) + 'HORACAS'?? No, 'HORACAS' is execution time?
                            # Or 'CONTRATO' has delivery time?
                            # Image: "Contrato: 20180814 19:00-20180814 20:00" -> This is the Delivery Period!
                            # We need to parse 'CONTRATO'.
                            
                            # Parser logic for Contract:
                            # "YYYYMMDD HH:MM-YYYYMMDD HH:MM"
                            # Take Start Time.
                            
                            # Vectorized parse?
                            # Split by '-' then take first part.
                            # "20180814 19:00"
                            # Format "%Y%m%d %H:%M"
                            
                            try:
                                # Split Contrato
                                # Assuming 'CONTRATO' col exists
                                df_qty['DeliveryStart'] = df_qty['CONTRATO'].str.split('-').str[0].str.strip()
                                # Parse
                                # Madrid timezone? Usually contracts are local time.
                                df_qty['Datetime_Madrid'] = pd.to_datetime(df_qty['DeliveryStart'], format='%Y%m%d %H:%M')
                                # Localize?
                                madrid = pytz.timezone('Europe/Madrid')
                                # If naive, assume Madrid (since files usually local).
                                # But pd.to_datetime is naive.
                                df_qty['Datetime_Madrid'] = df_qty['Datetime_Madrid'].apply(lambda x: madrid.localize(x) if x.tzinfo is None else x)
                                
                                fixed_offset = datetime.timezone(datetime.timedelta(hours=1))
                                df_qty['Datetime_UTC1'] = df_qty['Datetime_Madrid'].apply(lambda x: x.astimezone(fixed_offset))
                                
                            except Exception as e:
                                logger.warning(f"Failed to parse MIC Contract dates: {e}")
                                return pd.DataFrame()

                            # Sell Side
                            df_sell = df_qty[df_qty[seller_col].isin(target_units)].copy()
                            df_sell['Quantity'] = df_sell[qty_col_mic]
                            df_sell['Price'] = df_sell[price_col_mic]
                            df_sell['Unit'] = df_sell[seller_col]
                            
                            # Buy Side
                            df_buy = df_qty[df_qty[buyer_col].isin(target_units)].copy()
                            df_buy['Quantity'] = -df_buy[qty_col_mic]
                            df_buy['Price'] = df_buy[price_col_mic]
                            df_buy['Unit'] = df_buy[buyer_col]
                            
                            # Combine
                            final_df = pd.concat([df_sell, df_buy])
                            final_df['Margin'] = final_df['Quantity'] * final_df['Price']
                            df_qty = final_df

    except Exception as e:
        logger.error(f"Error processing {market_name} on {ymd}: {e}")
        return pd.DataFrame()
        
    if df_qty.empty:
        return pd.DataFrame()

    # ============================================================================
    # SECTION 2: READ PRICE DATA
    # ============================================================================
    # Price data is retrieved via get_price_data() function
    # Special markets MIC and PIBC may have embedded prices from section 1
    # ============================================================================
    # Need price for the same range
    # If MIC, we already have price.
    if market_name == 'MIC' or market_name == 'PIBC': # PIBC handled internally above
        df_price = pd.DataFrame()
    else:
        df_price = get_price_data(mkt, date_obj, cache_manager)
    
    # ============================================================================
    # SECTION 3: MERGE & CALCULATE MARGINS
    # ============================================================================
    # Merge quantity and price data, then calculate margins
    # ============================================================================
    final_df = pd.DataFrame()
    
    if market_name == 'MIC' or market_name == 'PIBC':
        final_df = df_qty
        # Skip standard merge logic
        
    elif not df_price.empty:


        if 'Datetime_Madrid' in df_price.columns:
            # Determine merge keys and columns to keep from price
            merge_keys = ['Datetime_Madrid']
            price_keep_cols = ['Datetime_Madrid', 'Price']
            
            # If Unit exists in both, include it in merge (for I90DIA09, I90DIA10)
            if 'Unit' in df_qty.columns and 'Unit' in df_price.columns:
                merge_keys.insert(0, 'Unit')
                price_keep_cols.insert(0, 'Unit')
            
            # If Sentido exists in both, include it in merge
            if 'Sentido' in df_qty.columns and 'Sentido' in df_price.columns:
                merge_keys.insert(0, 'Sentido')
                price_keep_cols.insert(0, 'Sentido')
            
            final_df = pd.merge(df_qty, df_price[price_keep_cols], on=merge_keys, how='left')
        else:
             # Fallback: simple join if lengths match? Dangerous.
             # Log warning
             # logger.warning("Price DF missing Datetime_Madrid")
             final_df = df_qty
    
    else:
        # If no price (e.g. some markets missing price), just return qty?
        # User: "Hay mercados en los que no hay precio. En esos casos dejar el precio en blanco"
        final_df = df_qty
        if 'Price' not in final_df.columns:
            final_df['Price'] = np.nan
        final_df['Margin'] = np.nan

    # Calculation
    if 'Price' in final_df.columns and 'Quantity' in final_df.columns:
         final_df['Margin'] = final_df['Quantity'] * final_df['Price']

    # Aggregation
    if not final_df.empty:
        # Standardize Unit Column Name (includes MIC-specific columns UNIDADV, UNIDADC)
        unit_candidates = ['UNIDAD DE PROGRAMACIÓN', 'UNIDAD DE PROGRAMACION', 'CODIGO', 'CODUOG', 'CÓDIGO', 'UP', 'UNIDAD', 'UNIT', 'UNIDADV', 'UNIDADC']
        found_unit = utils.find_unit_column(final_df, candidates=unit_candidates)
        if found_unit:
            final_df = final_df.rename(columns={found_unit: 'Unit'})

        # Determine Group Keys: Dates + Sentido (if exists)
        group_keys = ['Datetime_Madrid']
        
        if 'Unit' in final_df.columns:
            group_keys.insert(0, 'Unit')
        
        if 'Market' in final_df.columns:
            group_keys.append('Market')
        
        # Helper: Ensure expected cols exist or handle gracefully
        if 'Datetime_UTC1' in final_df.columns:
             group_keys.append('Datetime_UTC1')
        
        # Check for Sentido (case-insensitive check?)
        sentido_col = next((c for c in final_df.columns if c.upper() == 'SENTIDO'), None)
        if sentido_col:
            group_keys.append(sentido_col)
            
        # Define Aggregations
        agg_map = {}
        if 'Quantity' in final_df.columns:
            agg_map['Quantity'] = 'sum'
        if 'Price' in final_df.columns:
            agg_map['Price'] = 'mean'
        if 'Margin' in final_df.columns:
            # Use min_count=1 to prefer NaN over 0 if all values are NaN
            agg_map['Margin'] = lambda x: x.sum(min_count=1)
            
        if agg_map:
            final_df = final_df.groupby(group_keys, as_index=False).agg(agg_map)

        # FINAL COLUMN NORMALIZATION
        # Expected: Unit, Datetime_Madrid, Datetime_UTC1, Sentido, Quantity, Price, Margin, Market
        expected_cols = ['Unit', 'Datetime_Madrid', 'Datetime_UTC1', 'Sentido', 'Quantity', 'Price', 'Margin', 'Market']
        
        # Ensure Market exists (default to market_name if not set per row)
        if 'Market' not in final_df.columns:
            final_df['Market'] = market_name
        else:
            final_df['Market'] = final_df['Market'].fillna(market_name)
        
        # Ensure Sentido exists (rename if case mismatch)
        if 'Sentido' not in final_df.columns:
            sent_col = next((c for c in final_df.columns if str(c).upper() == 'SENTIDO'), None)
            if sent_col:
                final_df = final_df.rename(columns={sent_col: 'Sentido'})
            else:
                final_df['Sentido'] = np.nan
        
        # Ensure all expected columns exist
        for col in expected_cols:
            if col not in final_df.columns:
                final_df[col] = np.nan
                
        # Filter and reorder to exact specification
        final_df = final_df[expected_cols]

    return final_df

def process_single_day(date_obj, target_units, target_markets):
    """Process all markets for a single day (worker function for parallel processing).
    
    Args:
        date_obj: Date to process
        target_units: List of unit codes to include
        target_markets: List of market names to process (or None for all)
        
    Returns:
        tuple: (date_obj, list of DataFrames for this day)
    """
    logger = logging.getLogger()
    cache_manager = file_cache.FileCacheManager()
    
    day_results = []
    
    logger.info(f"Processing {date_obj}")
    
    for mkt in config.MARKET_CONFIG:
        if target_markets and mkt['market'] not in target_markets:
            continue
            
        df_mkt = process_market(mkt, date_obj, target_units, cache_manager)
        if not df_mkt.empty:
            day_results.append(df_mkt)
    
    return (date_obj, day_results)


def run_process(start_date=None, end_date=None, years=None, target_markets=None):
    """Run the margin calculation process for specified date ranges.
    
    Supports both sequential and parallel processing based on config.MAX_WORKERS.
    
    Args:
        start_date: Start date for processing
        end_date: End date for processing  
        years: List of years to process
        target_markets: List of market names to process (or None for all)
    """
    logger = logging.getLogger()
    
    # Determine date ranges
    date_ranges = []
    
    if start_date and end_date:
        date_ranges.append((start_date, end_date))
    elif years:
        for year in years:
            s = datetime.date(year, 1, 1)
            e = datetime.date(year, 12, 31)
            date_ranges.append((s, e))
            
    if not date_ranges:
        logger.error("No valid date range provided.")
        return

    logger.info(f"Running process for ranges: {date_ranges}")
    if target_markets:
        logger.info(f"Filtering for markets: {target_markets}")
    
    # Load target units
    target_units = config.TARGET_UNITS
    
    # Ensure output directory exists
    if not os.path.exists("output"):
        os.makedirs("output")
    
    # Collect all dates to process
    all_dates = []
    for s_date, e_date in date_ranges:
        current = s_date
        while current <= e_date:
            all_dates.append(current)
            current += datetime.timedelta(days=1)
    
    # Process days (parallel or sequential based on config)
    max_workers = getattr(config, 'MAX_WORKERS', 1)
    
    # Dictionary to accumulate results by month
    monthly_buffers = {}
    
    if max_workers > 1:
        # Parallel processing
        logger.info(f"Using parallel processing with {max_workers} workers")
        from concurrent.futures import ProcessPoolExecutor, as_completed
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit all days for processing
            futures = {executor.submit(process_single_day, date, target_units, target_markets): date 
                      for date in all_dates}
            
            # Collect results as they complete
            for future in as_completed(futures):
                date_obj = futures[future]
                try:
                    _, day_results = future.result()
                    
                    # Group by month
                    ym = (date_obj.year, date_obj.month)
                    if ym not in monthly_buffers:
                        monthly_buffers[ym] = []
                    monthly_buffers[ym].extend(day_results)
                    
                except Exception as exc:
                    logger.error(f"Day {date_obj} generated an exception: {exc}")
    else:
        # Sequential processing (original logic)
        logger.info("Using sequential processing")
        cache_manager = file_cache.FileCacheManager()
        
        for date_obj in all_dates:
            # Clear cache at the start of each new day
            cache_manager.clear()
            
            _, day_results = process_single_day(date_obj, target_units, target_markets)
            
            # Group by month
            ym = (date_obj.year, date_obj.month)
            if ym not in monthly_buffers:
                monthly_buffers[ym] = []
            monthly_buffers[ym].extend(day_results)
    
    # Save monthly CSVs
    for ym, dataframes in sorted(monthly_buffers.items()):
        if dataframes:
            chunk_df = pd.concat(dataframes)
            fname = f"unit_margin_{ym[0]}{ym[1]:02d}.csv"
            chunk_df.to_csv(os.path.join("output", fname), index=False)
            logger.info(f"Saved chunk {fname}")
    
    if not monthly_buffers:
        logger.info("No data found to save.")
