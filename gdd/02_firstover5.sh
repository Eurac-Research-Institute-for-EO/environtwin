#!/bin/bash
# First warm spell DOY detection (full year), multiple window versions
set -euo pipefail

tmean_dir="/mnt/CEPH_PROJECTS/Environtwin/GDD/SouthTyrol/temperature"
out_dir="/mnt/CEPH_PROJECTS/Environtwin/GDD/SouthTyrol/temperature"
mkdir -p "$out_dir"

for year in {2015..2025}; do
    echo "Processing first warm spell for year $year"

    python3 - <<EOF
import os, glob, numpy as np
import rasterio
from datetime import date

year = "$year"
tmean_dir = "$tmean_dir"
out_dir  = "$out_dir"

months = ["01","02","03","04","05","06","07","08","09","10","11","12"]
thresh = 5       # warm spell threshold

# --- Collect all monthly stacks ---
tmean_files = []
for m in months:
    files = sorted(glob.glob(os.path.join(tmean_dir, f"tmean_250m_{year}_{m}*.tif")))
    tmean_files.extend(files)

if not tmean_files:
    print(f" No Tmean files for {year}")
    raise SystemExit(0)

# --- Open first file to get metadata ---
with rasterio.open(tmean_files[0]) as src:
    meta = src.profile
    meta.update(count=1, dtype="float32", compress="deflate")
    width, height = src.width, src.height

# --- Compute DOY array automatically ---
# --- Compute DOY array automatically ---
doy_list = []
for mo in months:
    mo_files = sorted(glob.glob(os.path.join(tmean_dir, f"tmean_250m_{year}_{mo}.tif")))
    for f in mo_files:
        with rasterio.open(f) as src_file:
            n_bands = src_file.count
        
        # Extract month from filename: tmean_250m_2025_01.tif
        basename = os.path.basename(f)
        parts = basename.replace('.tif', '').split('_')  # Remove .tif first
        file_year = int(parts[2])
        file_month = int(parts[3])  # Now '01' → 1 correctly
        
        # DOY range for this month
        import calendar
        days_in_month = calendar.monthrange(file_year, file_month)[1]
        doy_base = sum(calendar.monthrange(file_year, m)[1] for m in range(1, file_month)) + 1
        if calendar.isleap(file_year) and file_month > 2:
            doy_base += 1
        
        for day in range(1, min(n_bands+1, days_in_month+1)):
            doy_list.append(doy_base + day - 1)

doy_arr = np.array(doy_list)

def find_first_warmspell_day(block_stack, doy_arr, thresh, consecutive_days=5):
    n_layers, nrows, ncols = block_stack.shape
    out_block = np.full((nrows, ncols), np.nan, dtype="float32")
    for r in range(nrows):
        for c in range(ncols):
            px = block_stack[:, r, c]
            if np.all(np.isnan(px)):
                continue
            for i in range(n_layers - consecutive_days + 1):
                window_vals = px[i:i+consecutive_days]
                if np.all(window_vals >= thresh):
                    out_block[r, c] = doy_arr[i]
                    break
    return out_block

# --- Read all daily bands into a big stack ---
block_stack = []
for f in tmean_files:
    with rasterio.open(f) as s:
        data = s.read().astype("float32")
        block_stack.append(data)
block_stack = np.vstack(block_stack)

for N in [5]:  # Add [3,5] if you want multiple windows
    out_file = os.path.join(out_dir, f"first_over{N}_{year}.tif")
    out_block = find_first_warmspell_day(block_stack, doy_arr, thresh, consecutive_days=N)
    with rasterio.open(out_file, "w", **meta) as dst_out:
        dst_out.write(out_block, 1)
    print(f"Saved {out_file}")

EOF

done

echo "First warm spell processing complete!"
