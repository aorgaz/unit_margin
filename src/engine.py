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

def process_mic_trades(mkt, date_obj, target_units, cache_manager):
    """Process MIC market trades file (special handling).
    
    SPECIAL PROCESSING FOR MIC MARKET:
    MIC trades require both quantity and price data from the same trades file.
    This function reads the file once and calculates both, returning a merged DataFrame.
    
    Args:
        mkt: Market configuration dictionary
        date_obj: Date object for which to retrieve trades
        target_units: List of units to filter for quantity data
        cache_manager: FileCacheManager instance for caching file reads
        
    Returns:
        DataFrame with columns: ['Unit', 'Datetime_Madrid', 'Datetime_UTC1', 'Quantity', 'Price']
    """
    logger = logging.getLogger()
    ymd = date_obj.strftime("%Y%m%d")
    ym = date_obj.strftime("%Y%m")
    
    # Read trades file
    data_id = mkt.get('data_id')
    zip_name = f"{data_id}_{ym}.zip"
    zip_path = os.path.join(config.PATH_OMIE, data_id, zip_name)
    inner_prefix = f"{data_id}_{ymd}"
    
    df_trades = cache_manager.get_omie_file(zip_path, inner_prefix, omie_reader.read_omie_file)
    
    if df_trades.empty:
        return pd.DataFrame()
    
    # Uppercase column names to match OMIE format
    df_trades.columns = [str(c).upper() for c in df_trades.columns]
    
    # Parse contract dates
    if 'CONTRATO' not in df_trades.columns:
        logger.warning(f"MIC trades file missing CONTRATO column. Available: {df_trades.columns.tolist()}")
        return pd.DataFrame()
    
    try:
        # Parse delivery start time from contract field
        df_trades['DeliveryStart'] = df_trades['CONTRATO'].str.split('-').str[0].str.strip()
        df_trades['Datetime_Madrid'] = pd.to_datetime(df_trades['DeliveryStart'], format='%Y%m%d %H:%M')
        df_trades['Datetime_Madrid'] = df_trades['Datetime_Madrid'].dt.tz_localize('Europe/Madrid', ambiguous='infer')
        df_trades = df_trades.dropna(subset=['Datetime_Madrid'])
        
        # Identify column names (support both abbreviated and full formats)
        seller_col = 'UNIDADV' if 'UNIDADV' in df_trades.columns else 'UNIDAD VENTA'
        buyer_col = 'UNIDADC' if 'UNIDADC' in df_trades.columns else 'UNIDAD COMPRA'
        price_col = 'PRECIO'
        qty_col = 'CANTIDAD'
        
        if seller_col not in df_trades.columns or buyer_col not in df_trades.columns:
            logger.warning(f"MIC trades missing required columns (need {seller_col} or {buyer_col}). Available: {df_trades.columns.tolist()}")
            return pd.DataFrame()
        
        # Normalize numeric values
        df_trades['Price'] = pd.to_numeric(df_trades[price_col].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
        df_trades['Quantity'] = pd.to_numeric(df_trades[qty_col].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
        df_trades['Amount'] = df_trades['Price'] * df_trades['Quantity']
        
        # Split into sell and buy records by unit
        df_sell = df_trades[[seller_col, 'Datetime_Madrid', 'Quantity', 'Amount']].copy()
        df_sell.columns = ['Unit', 'Datetime_Madrid', 'Quantity', 'Amount']
        
        df_buy = df_trades[[buyer_col, 'Datetime_Madrid', 'Quantity', 'Amount']].copy()
        df_buy.columns = ['Unit', 'Datetime_Madrid', 'Quantity', 'Amount']
        df_buy['Quantity'] = -df_buy['Quantity']  # Negative for purchases
        
        # Filter by target units if specified
        if target_units:
            df_sell = df_sell[df_sell['Unit'].isin(target_units)]
            df_buy = df_buy[df_buy['Unit'].isin(target_units)]
        
        # Combine and aggregate by Unit + Datetime
        combined = pd.concat([df_sell, df_buy])
        result = combined.groupby(['Unit', 'Datetime_Madrid'], as_index=False).agg({
            'Quantity': 'sum',
            'Amount': 'sum'
        })
        
        # Calculate weighted average price
        result['Price'] = result.apply(lambda r: r['Amount'] / r['Quantity'] if r['Quantity'] != 0 else 0, axis=1)
        
        # Add UTC1 timestamp
        fixed_offset = datetime.timezone(datetime.timedelta(hours=1))
        result['Datetime_UTC1'] = result['Datetime_Madrid'].dt.tz_convert(fixed_offset)
        
        # Return with quantity and price already merged
        return result[['Unit', 'Datetime_Madrid', 'Datetime_UTC1', 'Quantity', 'Price']]
        
    except Exception as e:
        logger.error(f"Error processing MIC trades: {e}")
        return pd.DataFrame()

def get_price_data(mkt, date_obj, cache_manager):
    """Retrieve price data based on market configuration.
    
    Args:
        mkt: Market configuration dictionary
        date_obj: Date object for which to retrieve prices
        cache_manager: FileCacheManager instance for caching file reads
        
    Returns:
        DataFrame with price data including Datetime_Madrid and Price columns.
        May include 'Unit' or 'Session' for specific markets.
    """
    logger = logging.getLogger()
    year = date_obj.year
    ymd = date_obj.strftime("%Y%m%d")
    
    price_df = pd.DataFrame()
    
    source = mkt.get('price_source')
    market_name = mkt.get('market')
    
    # SPECIAL CASE: MIC market is handled by process_mic_trades() function
    # to avoid reading the trades file twice (once for quantity, once for price).
    # MIC price data is NOT retrieved here.
    
    if market_name == 'PIBCI':
        # PIBCI Price Logic: Per Session Indicators
        # Iterate sessions 1..7 (or as defined in config)
        price_map = mkt.get('price_id') # Dict {1: id, 2: id...}
        if isinstance(price_map, dict):
            dfs = []
            for sess, pid in price_map.items():
                if not pid: continue
                fname = f"{pid}_{year}_{date_obj.month}.csv"
                fpath = os.path.join(config.PATH_ESIOS_IND, str(pid), fname)
                p_df = cache_manager.get_esios_indicator(fpath, esios_reader.read_esios_indicator)
                
                if not p_df.empty:
                     if 'datetime' in p_df.columns:
                        p_df['Datetime_Madrid'] = pd.to_datetime(p_df['datetime'], utc=True).dt.tz_convert('Europe/Madrid')
                        
                        # Filter Geo
                        pid_int = int(pid) if str(pid).isdigit() else 0
                        target_geo = 3 if 612 <= pid_int <= 618 else 8741
                        if 'geo_id' in p_df.columns:
                            p_df = p_df[p_df['geo_id'] == target_geo]
                        
                        if 'value' in p_df.columns:
                            p_df['Price'] = p_df['value']
                            p_df['Session'] = sess
                            dfs.append(p_df[['Datetime_Madrid', 'Price', 'Session']])
            
            if dfs:
                price_df = pd.concat(dfs)
                
    elif source == 'indicator':
        price_id = mkt['price_id']
        
        # Dynamic Rules
        if price_id == 'BANDA_SUBIR_RULE':
            switch_date = datetime.date(2024, 11, 20)
            price_id = 634 if date_obj <= switch_date else 2130
        elif price_id == 'MFRR_SUBIR_RULE':
            switch_date = datetime.date(2024, 12, 10)
            price_id = 677 if date_obj <= switch_date else 2197
        elif price_id == 'MFRR_BAJAR_RULE':
            switch_date = datetime.date(2024, 12, 10)
            price_id = 676 if date_obj <= switch_date else 2197
            
        if isinstance(price_id, int) or isinstance(price_id, str):
            fname = f"{price_id}_{year}_{date_obj.month}.csv"
            fpath = os.path.join(config.PATH_ESIOS_IND, str(price_id), fname)
            price_df = cache_manager.get_esios_indicator(fpath, esios_reader.read_esios_indicator)
            
            # Standardize Price DF Timestamp
            if not price_df.empty and 'datetime' in price_df.columns:
                price_df['Datetime_Madrid'] = pd.to_datetime(price_df['datetime'], utc=True).dt.tz_convert('Europe/Madrid')
                
                # Filter by geo_id
                pid_int = int(price_id) if str(price_id).isdigit() else 0
                target_geo = 3 if 612 <= pid_int <= 618 else 8741
                if 'geo_id' in price_df.columns:
                    price_df = price_df[price_df['geo_id'] == target_geo]
                
                if 'value' in price_df.columns:
                    price_df['Price'] = price_df['value']
                    price_df = price_df[['Datetime_Madrid', 'Price']]
            
    elif source == 'omie':
        data_id_price = mkt.get('price_id')
        if data_id_price:
            year = date_obj.year
            zip_name = f"{data_id_price}_{year}.zip"
            zip_path = os.path.join(config.PATH_OMIE, data_id_price, zip_name)
            
            inner_prefix = f"{data_id_price}_{ymd}"
            price_df = cache_manager.get_omie_file(zip_path, inner_prefix, omie_reader.read_omie_file)
            
            if not price_df.empty:
                # Normalize OMIE Price
                start_madrid, end_madrid = utils.get_madrid_time_range(ymd)
                expected_timestamps = pd.date_range(start=start_madrid, end=end_madrid, freq='h', inclusive='left') 
                
                if 'Period' in price_df.columns:
                    ts_map = {}
                    valid_range = range(1, len(expected_timestamps) + 1)
                    for i, p in enumerate(valid_range):
                        ts_map[p] = expected_timestamps[i]
                        
                    price_df['Datetime_Madrid'] = price_df['Period'].map(ts_map)
                    price_df = price_df.dropna(subset=['Datetime_Madrid'])
                    
                    if 'MarginalES' in price_df.columns:
                        price_df['Price'] = price_df['MarginalES']
                    elif 'Price' not in price_df.columns:
                        price_df['Price'] = np.nan
                        
                    price_df = price_df[['Datetime_Madrid', 'Price']]
    
    elif source == 'i90':
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
                    # Sanitize
                    price_df = price_df.dropna(subset=[price_df.columns[0]])
                    if not price_df.empty:
                        price_df.columns = price_df.iloc[0]
                        price_df = price_df[1:]
                    
                    price_df.columns = [str(c).strip() for c in price_df.columns]
                    price_df.columns = [str(int(float(c))) if c.replace('.','',1).isdigit() and c.endswith('.0') else c for c in price_df.columns]
                    
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
                        # Process timestamps (similar to qty)
                        start_madrid, end_madrid = utils.get_madrid_time_range(ymd)
                        duration_hours = int((end_madrid - start_madrid).total_seconds() / 3600)
                        
                        col_to_ts = {}
                        cols = []
                        # Detect freq
                        if '00-01' in price_df.columns:
                             cols = [f"{h:02d}-{h+1:02d}" for h in range(duration_hours)] # naive gen
                             # handle 25/23 specific logic if needed, but keeping simple for brevity as in original
                             if duration_hours == 25:
                                cols = [f"{h:02d}-{h+1:02d}" for h in range(2)] + ["02-03a", "02-03b"] + [f"{h:02d}-{h+1:02d}" for h in range(3, 24)]
                             elif duration_hours == 23:
                                cols = [f"{h:02d}-{h+1:02d}" for h in range(2)] + [f"{h:02d}-{h+1:02d}" for h in range(3, 24)]
                                
                        elif ('1' in price_df.columns and '4' in price_df.columns) or (1 in price_df.columns and 4 in price_df.columns):
                             cols = [str(i) for i in range(1, duration_hours * 4 + 1)]

                        if cols:
                             ts_list = pd.date_range(start=start_madrid, end=end_madrid, freq='h' if '00-01' in price_df.columns else '15min', inclusive='left')
                             for i, c in enumerate(cols):
                                 if i < len(ts_list):
                                     col_to_ts[c] = ts_list[i]
                                     
                             val_cols = [c for c in col_to_ts.keys() if c in price_df.columns]
                             if val_cols:
                                 base_cols = [c for c in price_df.columns if c not in val_cols]
                                 price_df = price_df.melt(id_vars=base_cols, value_vars=val_cols, var_name='PeriodLabel', value_name='Price')
                                 price_df = price_df.dropna(subset=['Price'])
                                 price_df['Datetime_Madrid'] = price_df['PeriodLabel'].map(col_to_ts)
                                 price_df = price_df.dropna(subset=['Datetime_Madrid'])
                                 
                                 keep_cols = ['Datetime_Madrid', 'Price']
                                 
                                 unit_col = utils.find_unit_column(price_df)
                                 if unit_col:
                                     price_df['Unit'] = price_df[unit_col]
                                     keep_cols.insert(0, 'Unit')
                                 
                                 if 'Sentido' in price_df.columns:
                                     keep_cols.insert(0, 'Sentido')
                                 
                                 price_df = price_df[keep_cols]
         
    return price_df

def get_quantity_data(mkt, date_obj, target_units, cache_manager):
    """Retrieve quantity data based on market configuration."""
    logger = logging.getLogger()
    ymd = date_obj.strftime("%Y%m%d")
    year = date_obj.year
    ym = date_obj.strftime("%Y%m")
    
    market_name = mkt['market']
    data_source = mkt['source']
    
    # SPECIAL CASE: MIC market is handled by process_mic_trades() function
    # to avoid reading the trades file twice (once for quantity, once for price).
    # MIC quantity data is NOT retrieved here.
    
    df_qty = pd.DataFrame()
    
    try:
        # I90 Source
        if data_source == 'i90':
            zip_name = f"I90DIA_{ymd}.zip"
            zip_path = os.path.join(config.PATH_ESIOS_I90, f"i90_{year}", zip_name)
            
            sheet_ids = mkt['data_id']
            if not isinstance(sheet_ids, list): sheet_ids = [sheet_ids]
            
            dfs = []
            for sheet in sheet_ids:
                d = cache_manager.get_i90_sheet(zip_path, sheet, i90_reader.read_i90_zip)
                if not d.empty: dfs.append(d)
            
            if dfs:
                df_qty = pd.concat(dfs)
                if not df_qty.empty:
                    df_qty = df_qty.dropna(subset=[df_qty.columns[0]])
                    if not df_qty.empty:
                        df_qty.columns = df_qty.iloc[0]
                        df_qty = df_qty[1:]
                    
                    df_qty.columns = [str(c).strip() for c in df_qty.columns]
                    df_qty.columns = [str(int(float(c))) if c.replace('.','',1).isdigit() and c.endswith('.0') else c for c in df_qty.columns]
                    
                    unit_col = utils.find_unit_column(df_qty)
                    if unit_col and target_units is not None:
                        df_qty = df_qty[df_qty[unit_col].isin(target_units)]
                    
                    filters = mkt.get('quantity_filters') or mkt.get('filters', {})
                    if not filters and mkt.get('filter_col'):
                         filters = {mkt.get('filter_col'): mkt.get('filter_val')}
                    
                    for col, val in filters.items():
                        if col in df_qty.columns:
                            df_qty = df_qty[df_qty[col] == val]
                    
                    if not df_qty.empty:
                        start_madrid, end_madrid = utils.get_madrid_time_range(ymd)
                        duration_hours = int((end_madrid - start_madrid).total_seconds() / 3600)
                        
                        col_to_ts = {}
                        cols = []
                        if '00-01' in df_qty.columns:
                            if duration_hours == 25:
                                cols = [f"{h:02d}-{h+1:02d}" for h in range(2)] + ["02-03a", "02-03b"] + [f"{h:02d}-{h+1:02d}" for h in range(3, 24)]
                            elif duration_hours == 23:
                                cols = [f"{h:02d}-{h+1:02d}" for h in range(2)] + [f"{h:02d}-{h+1:02d}" for h in range(3, 24)]
                            else:
                                cols = [f"{h:02d}-{h+1:02d}" for h in range(24)]
                        elif ('1' in df_qty.columns and '4' in df_qty.columns) or (1 in df_qty.columns and 4 in df_qty.columns):
                            cols = [str(i) for i in range(1, duration_hours*4 + 1)]
                        
                        if cols:
                             ts_list = pd.date_range(start=start_madrid, end=end_madrid, freq='h' if '00-01' in df_qty.columns else '15min', inclusive='left')
                             for i, c in enumerate(cols):
                                 if i < len(ts_list): col_to_ts[c] = ts_list[i]
                             
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

        # OMIE Source
        elif data_source == 'omie':
            data_id = mkt.get('data_id')
            zip_name = f"{data_id}_{ym}.zip"
            zip_path = os.path.join(config.PATH_OMIE, data_id, zip_name)
            
            # Helper for PIBCI sessions
            if market_name == 'PIBCI':
                 dfs_sess = []
                 for s in range(1, 8):
                     inner_prefix = f"{data_id}_{ymd}{s:02d}"
                     df_part = cache_manager.get_omie_file(zip_path, inner_prefix, omie_reader.read_omie_file)
                     if not df_part.empty:
                         # PIBCI Session Identification
                         # The file itself usually contains SESSION column or we infer
                         df_part['Session'] = s
                         dfs_sess.append(df_part)
                 if dfs_sess:
                     df_qty = pd.concat(dfs_sess, ignore_index=True)
            else:
                 inner_prefix = f"{data_id}_{ymd}"
                 df_qty = cache_manager.get_omie_file(zip_path, inner_prefix, omie_reader.read_omie_file)
            
            if not df_qty.empty:
                # Uppercase columns, but preserve 'Session' for PIBCI matching
                cols_upper = []
                for c in df_qty.columns:
                    if c == 'Session':  # Preserve Session column name for PIBCI
                        cols_upper.append(c)
                    else:
                        cols_upper.append(str(c).upper())
                df_qty.columns = cols_upper
                
                # Standard OMIE processing (MIC is handled separately in process_mic_trades)
                # PIBCI already processed above with sessions
                unit_col = utils.find_unit_column(df_qty, candidates=['UNIT', 'CODIGO', 'CODUOG', 'CÓDIGO'])
                if unit_col and target_units:
                    df_qty = df_qty[df_qty[unit_col].isin(target_units)]
                
                if not df_qty.empty:
                         start_madrid, end_madrid = utils.get_madrid_time_range(ymd)
                         max_period = df_qty['PERIOD'].max()
                         freq = '15min' if max_period > 25 else 'h'
                         expected = pd.date_range(start=start_madrid, end=end_madrid, freq=freq, inclusive='left')
                         
                         ts_map = {}
                         for i, p in enumerate(range(1, len(expected)+1)):
                             ts_map[p] = expected[i]
                             
                         df_qty['Datetime_Madrid'] = df_qty['PERIOD'].map(ts_map)
                         df_qty = df_qty.dropna(subset=['Datetime_Madrid'])
                         
                         fixed_offset = datetime.timezone(datetime.timedelta(hours=1))
                         df_qty['Datetime_UTC1'] = df_qty['Datetime_Madrid'].apply(lambda x: x.astimezone(fixed_offset))
                         
                         if 'QUANTITY' in df_qty.columns: df_qty['Quantity'] = df_qty['QUANTITY']
                         elif 'POTENCIA' in df_qty.columns: df_qty['Quantity'] = df_qty['POTENCIA']
                         elif 'POTENCIA ASIGNADA' in df_qty.columns: df_qty['Quantity'] = df_qty['POTENCIA ASIGNADA']
                         
                         # Filter out zero quantities
                         if 'Quantity' in df_qty.columns:
                             df_qty = df_qty[df_qty['Quantity'] != 0]

    except Exception as e:
        logger.error(f"Error reading quantity for {market_name}: {e}")
        return pd.DataFrame()
        
    return df_qty

def process_market(mkt, date_obj, target_units, cache_manager):
    """Process a single market for a specific date.
    
    SPECIAL HANDLING: MIC market uses process_mic_trades() which returns
    a DataFrame with Quantity and Price already merged together.
    """
    logger = logging.getLogger()
    ymd = date_obj.strftime("%Y%m%d")
    market_name = mkt['market']
    
    # ========== SPECIAL CASE: MIC MARKET ==========
    # MIC processes both quantity and price together from trades file
    if market_name == 'MIC':
        final_df = process_mic_trades(mkt, date_obj, target_units, cache_manager)
        
        if final_df.empty:
            return pd.DataFrame()
        
        # MIC already has Quantity, Price, and Unit columns - skip merge section
        # Just calculate margin and proceed to aggregation
        if 'Quantity' in final_df.columns and 'Price' in final_df.columns:
            final_df['Margin'] = final_df['Quantity'] * final_df['Price']
    else:
        # ========== STANDARD PROCESSING ==========
        # 1. READ QUANTITY
        df_qty = get_quantity_data(mkt, date_obj, target_units, cache_manager)
        
        if df_qty.empty:
            return pd.DataFrame()
            
        # 2. READ PRICE
        df_price = get_price_data(mkt, date_obj, cache_manager)
        
        # 3. MERGE & CALCULATE
        # Identify Unit Col
        unit_col = utils.find_unit_column(df_qty) or 'Unit'
        if unit_col in df_qty.columns:
            df_qty = df_qty.rename(columns={unit_col: 'Unit'})
        else:
            logger.warning(f"Could not find Unit column in {market_name}. Available: {df_qty.columns.tolist()}")
        
        if not df_price.empty:
            # Merge Keys
            keys = ['Datetime_Madrid']
            if 'Unit' in df_price.columns and 'Unit' in df_qty.columns: keys.append('Unit')
            if 'Session' in df_price.columns and 'Session' in df_qty.columns: keys.append('Session')
            if 'Sentido' in df_qty.columns and 'Sentido' in df_price.columns: keys.append('Sentido')
            
            final_df = pd.merge(df_qty, df_price, on=keys, how='left')
        else:
            final_df = df_qty.copy()
            final_df['Price'] = np.nan
            
        # Calculate Margin
        if 'Quantity' in final_df.columns and 'Price' in final_df.columns:
            final_df['Margin'] = final_df['Quantity'] * final_df['Price']

        
    # Aggregate (Group by Unit, Date) to remove Trade granularity but preserve Session for PIBCI
    # Keys for final grouping
    grp_keys = ['Unit', 'Datetime_Madrid']
    if 'Datetime_UTC1' in final_df.columns: grp_keys.append('Datetime_UTC1')
    if 'Session' in final_df.columns: grp_keys.append('Session')  # Keep Session separate (PIBCI)
    if 'Sentido' in final_df.columns: grp_keys.append('Sentido')
    
    agg_df = final_df.groupby(grp_keys, as_index=False).agg({
        'Quantity': 'sum',
        'Margin': lambda x: x.sum(min_count=1)
    })
    
    # Recalculate implied price
    agg_df['Price'] = agg_df.apply(lambda r: r['Margin'] / r['Quantity'] if r['Quantity'] != 0 else 0, axis=1)
    
    # Set Market name - special handling for PIBCI sessions
    if market_name == 'PIBCI' and 'Session' in agg_df.columns:
        agg_df['Market'] = agg_df['Session'].apply(lambda s: f'PIBCI s{s:02d}')
        # Drop Session column since it's now encoded in Market name
        agg_df = agg_df.drop(columns=['Session'])
    else:
        agg_df['Market'] = market_name
    
    # Reorder columns to match desired output format
    # Base columns that should always exist
    output_cols = ['Unit', 'Datetime_Madrid', 'Datetime_UTC1', 'Sentido', 'Quantity', 'Price', 'Margin', 'Market']
    # Only include columns that actually exist in the dataframe
    final_cols = [col for col in output_cols if col in agg_df.columns]
    
    return agg_df[final_cols]

def process_single_day(date_obj, target_units, target_markets):
    """Process all markets for a single day (worker function for parallel processing)."""
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

def run_process(start_date=None, end_date=None, years=None, target_markets=None, target_units=None):
    """Run the margin calculation process for specified date ranges."""
    logger = logging.getLogger()
    
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
    
    if not os.path.exists("output"):
        os.makedirs("output")
    
    all_dates = []
    for s_date, e_date in date_ranges:
        current = s_date
        while current <= e_date:
            all_dates.append(current)
            current += datetime.timedelta(days=1)
    
    max_workers = getattr(config, 'MAX_WORKERS', 1)
    monthly_buffers = {}
    
    if max_workers > 1:
        logger.info(f"Using parallel processing with {max_workers} workers")
        from concurrent.futures import ProcessPoolExecutor, as_completed
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_single_day, date, target_units, target_markets): date for date in all_dates}
            for future in as_completed(futures):
                date_obj = futures[future]
                try:
                    _, day_results = future.result()
                    ym = (date_obj.year, date_obj.month)
                    if ym not in monthly_buffers: monthly_buffers[ym] = []
                    monthly_buffers[ym].extend(day_results)
                except Exception as exc:
                    logger.error(f"Day {date_obj} generated an exception: {exc}")
    else:
        logger.info("Using sequential processing")
        cache_manager = file_cache.FileCacheManager()
        for date_obj in all_dates:
            cache_manager.clear()
            _, day_results = process_single_day(date_obj, target_units, target_markets)
            ym = (date_obj.year, date_obj.month)
            if ym not in monthly_buffers: monthly_buffers[ym] = []
            monthly_buffers[ym].extend(day_results)
    
    for ym, dataframes in sorted(monthly_buffers.items()):
        if dataframes:
            chunk_df = pd.concat(dataframes)
            fname = f"unit_margin_{ym[0]}{ym[1]:02d}.csv"
            chunk_df.to_csv(os.path.join("output", fname), index=False)
            logger.info(f"Saved chunk {fname}")
