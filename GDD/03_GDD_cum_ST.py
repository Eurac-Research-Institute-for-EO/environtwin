#!/usr/bin/env python3
"""
Growing Degree Days (GDD) Calculator for South Tyrol - Environtwin Project
======================================================================
Processes monthly temperature stacks (daily layers) with per-pixel dynamic 
start dates from 'firstover5_year.tif' rasters. Computes cumulative GDD 
from first day T>5°C until Oct 31 (DOY 304).

Date: Feb 2026
"""

import rioxarray as rxr
import xarray as xr
import numpy as np
import glob
import os
import re
import warnings
warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION - ADJUST PATHS FOR YOUR SETUP
# ============================================================================
TEMP_DIR = "/mnt/CEPH_PROJECTS/Environtwin/GDD/SouthTyrol/temperature"     # Monthly stacks: tmean_250m_YYYY_MM.tif
START_DIR = "/mnt/CEPH_PROJECTS/Environtwin/GDD/SouthTyrol/temperature"     # firstover5_YYYY.tif (per-pixel DOY)
OUTPUT_DIR = "/mnt/CEPH_PROJECTS/Environtwin/GDD/SouthTyrol/gdd"           # Output cumulative GDD

YEARS =  [2025]
#list(range(2017, 2025))             # Test one year first! → range(2020, 2025) for full run
TBASES = [5, 2]                # Base temperatures (°C)
END_DOY = 304               # Season end: Oct 31 ≈ DOY 304
CHUNKSIZE = {'x': 2000, 'y': 2000}  # Memory-efficient chunks

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def is_leap_year(year):
    """Check if year is leap year (affects Feb 29 DOY calculation)."""
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)

def month_to_doy_offset(year, month):
    """
    Calculate day-of-year offset for start of each month (0-indexed).
    Jan=0, Feb=31(32), Mar=60(61), Apr=91(92), etc.
    """
    days_in_month = [31, 
                     29 if is_leap_year(year) else 28, 
                     31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    return sum(days_in_month[:month-1])

def parse_monthly_filename(filename):
    """
    Parse filename: tmean_250m_2024_03.tif → (2024, 3, 60)
    Returns (year, month, doy_offset) or None if invalid format.
    """
    basename = os.path.basename(filename)
    match = re.search(r'tmean_250m_(\d{4})_(\d{1,2})\.tif$', basename)
    if match:
        year, month = int(match.group(1)), int(match.group(2))
        return year, month, month_to_doy_offset(year, month)
    return None

def print_start_doy_stats(start_doy):
    """Validate and print start DOY raster statistics."""
    stats = start_doy.compute()
    print(f"     📈 Start DOY stats: "
          f"min={float(stats.min()):3.0f}, max={float(stats.max()):3.0f}, "
          f"mean={float(start_doy.mean()):3.0f}")
    if float(stats.min()) < 1 or float(stats.max()) > 365:
        print("     ⚠️  Warning: Unusual DOY range in firstover5 raster")

# ============================================================================
# MAIN PROCESSING
# ============================================================================
def main():
    print("🌡️  Dynamic GDD Calculator (Fixed band dim)...")
    
    for year in YEARS:
        print(f"\n{'='*70}\n📅 Processing {year}")
        
        # 1. Find monthly stacks
        pattern = os.path.join(TEMP_DIR, f"tmean_250m_{year}_*.tif")
        files = sorted(glob.glob(pattern))
        if not files:
            print(f"No files for {year}")
            continue
            
        print(f"📂 Found {len(files)} monthly stacks")
        
        # 2. Build year stack
        daily_rasters = []
        
        for file_path in files:
            info = parse_monthly_filename(file_path)
            if not info or info[0] != year: 
                continue
                
            year_num, month, doy_offset = info
            
            print(f"🔄 Loading {os.path.basename(file_path)}...")
            
            stack = rxr.open_rasterio(file_path, chunks=CHUNKSIZE, masked=True, decode_times=False)
            
            # Check actual dimension name and get layer count
            time_dim = stack.dims[0]  # First dim is time/band
            n_days = stack.sizes[time_dim]
            
            month_doys = list(range(doy_offset, doy_offset + n_days))
            
            # Assign DOY coordinates to time dimension (whatever it's called)
            stack = stack.assign_coords(**{time_dim: month_doys})
            daily_rasters.append(stack)
            
            print(f"  → {n_days} days (DOY {month_doys[0]:3d}–{month_doys[-1]:3d})")
        
        if not daily_rasters:
            print(f"No valid stacks for {year}")
            continue
        
        # 3. Concatenate months
        temp_stack = xr.concat(daily_rasters, dim=daily_rasters[0].dims[0]).sortby(daily_rasters[0].dims[0])
        print(f"✅ Full stack: {temp_stack.sizes[temp_stack.dims[0]]} days")

        # remove problematic band metadata
        temp_stack.attrs.pop("long_name", None)
        
        # 4. Load start DOY
        start_file = os.path.join(START_DIR, f"first_over5_{year}.tif")
        if not os.path.exists(start_file):
            print(f"❌ Missing: {start_file}")
            temp_stack.close()
            continue
        
        start_doy = rxr.open_rasterio(start_file, chunks=CHUNKSIZE).squeeze()
        print(f"📍 Start DOY: {float(start_doy.mean()):.0f} (mean)")
        
        # 5. GDD calculation
        for tbase in TBASES:
            print(f"🌡️ Tbase {tbase}°C")
            
            time_dim = temp_stack.dims[0]
            
            # Per-pixel dynamic masking
            valid_period = (temp_stack[time_dim] >= start_doy) & (temp_stack[time_dim] <= END_DOY)
            valid_temps = temp_stack.where(valid_period)
            
            # calculate daily gdds
            daily_gdd = (valid_temps - tbase).clip(min=0).persist()

            # remove problematic metadata
            daily_gdd.attrs.pop("long_name", None)

            # calculate cummulative daily GDD
            daily_cumulative_gdd = daily_gdd.cumsum(dim=time_dim).compute()

            # calculate seasonal cummulative GDD
            #cumulative_gdd = daily_gdd.sum(dim=time_dim).compute()
            
            # Save all
            # Daily output
            daily_file = os.path.join(OUTPUT_DIR, f"GDD_daily_T{tbase}_{year}.tif")
            daily_gdd.rio.to_raster(daily_file, compress='lzw', tiled=True)
            print(f"✅ Daily:  {os.path.basename(daily_file)}")

            # Daily cumulative
            daily_cumulative_gdd = daily_gdd.cumsum(dim=time_dim)
            daily_cumulative_gdd.attrs.pop("long_name", None)

            cum_daily_file = os.path.join(OUTPUT_DIR, f"GDD_daily_cumulative_T{tbase}_{year}.tif")
            daily_cumulative_gdd.rio.to_raster(cum_daily_file, compress='lzw', tiled=True)
            print(f"✅ Daily cum:   {os.path.basename(cum_daily_file)}")
            
            # Cummmulative
            #out_file = os.path.join(OUTPUT_DIR, f"GDD_cummulative_T{tbase}_{year}.tif")
            #cumulative_gdd.rio.to_raster(out_file, compress='lzw', tiled=True, dtype='float32')
            #print(f"✅ {os.path.basename(out_file)}")
        
        temp_stack.close()
        start_doy.close()
    
    print("🎉 Done!")

if __name__ == '__main__':
    main()