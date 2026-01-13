"""Data processing utilities for market data transformation.

Contains functions for normalizing time resolution, merging datasets,
and calculating margins.
"""

import pandas as pd
import numpy as np
import logging

def normalize_resolution(df, target_freq='15min', time_col='time', method='ffill'):
    """
    Normalizes dataframe to a target frequency.
    If upsampling (Hour -> 15min), use ffill for prices, or divide for energy?
    Usually:
    - Prices: forward fill (Same price for all quarters).
    - Energy: strictly speaking, hourly energy might need splitting / 4, 
      BUT usually market data is expressed in MWh (avg power) or similar.
      If it's MWh/h, then MWh per quarter is different. 
      However, usually 'Energy' in these files matches the resolution.
      
      If we have Hourly Price and Quarter Energy:
      We expand Price to Quarter.
    """
    # This function assumes df has a datetime index or column
    if time_col in df.columns:
        df = df.set_index(time_col)
    
    # Check current resolution
    # simple heuristic check
    if len(df) <= 1:
        return df
    
    # Resample
    # if we are just ensuring we have rows:
    df_res = df.resample(target_freq).asfreq()
    
    if method == 'ffill':
        df_res = df_res.ffill()
    
    return df_res.reset_index()

def merge_market_data(df_energy, df_price, join_keys=['time', 'unit']):
    """
    Merges energy and price dataframes.
    df_energy: [time, unit, quantity, ...]
    df_price: [time, (unit optional), price]
    """
    logger = logging.getLogger()
    
    # Ensure time columns are datetime
    if 'time' in df_energy.columns:
        df_energy['time'] = pd.to_datetime(df_energy['time'])
    if 'time' in df_price.columns:
        df_price['time'] = pd.to_datetime(df_price['time'])
    
    # Check if price is by unit or system-wide
    # Some prices are by unit (e.g. maybe specific bids, though usually marginal is system-wide)
    # The prompt implies some prices are indicators (system wide).
    
    if 'unit' in df_price.columns:
        # Merge on time and unit
        merged = pd.merge(df_energy, df_price, on=join_keys, how='inner', suffixes=('_energy', '_price'))
    else:
        # Merge on time only (broadcast price to all units)
        merged = pd.merge(df_energy, df_price, on='time', how='inner', suffixes=('_energy', '_price'))
        
    return merged

def calculate_margin(df):
    """
    Calculates Margin = Quantity * Price.
    """
    # Columns expected: 'quantity', 'price' (normalized names)
    # If missing price, leave blank (NaN)
    
    if 'quantity' not in df.columns or 'price' not in df.columns:
        return df
        
    df['margin'] = df['quantity'] * df['price']
    return df

def filter_units(df, target_units, unit_col='unit'):
    if df.empty:
        return df
    return df[df[unit_col].isin(target_units)]
