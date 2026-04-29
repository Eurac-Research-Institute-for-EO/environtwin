#!/bin/bash
# First frost DOY detection (Sep–Dec), block-wise per year with multiple window versions
set -euo pipefail

tmin_dir="/mnt/CEPH_PROJECTS/Environtwin/GDD/SouthTyrol/temperature"
out_dir="/mnt/CEPH_PROJECTS/Environtwin/GDD/SouthTyrol/frost"
mkdir -p "$out_dir"

for year in {2017..2025}; do
    echo "Processing first frost for year $year"

python3 - <<EOF
import os, glob, numpy as np
import rasterio
from datetime import date

year = "$year"
tmin_dir = "$tmin_dir"
out_dir  = "$out_dir"

months = ["09","10","11","12"]
thresh = 0       # frost threshold

# --- Collect all monthly stacks ---
tmin_files = []
for m in months:
    files = sorted(glob.glob(os.path.join(tmin_dir, f"tmin_250m_{year}_{m}*.tif")))
    tmin_files.extend(files)

if not tmin_files:
    print(f" No Tmin files for {year}")
    raise SystemExit(0)

# --- Open first file to get metadata ---
with rasterio.open(tmin_files[0]) as src:
    meta = src.profile
    meta.update(count=1, dtype="float32", compress="deflate")
    width, height = src.width, src.height

# --- Compute DOY array automatically ---
doy_list = []
for f in tmin_files:
    with rasterio.open(f) as src_file:
        n_bands = src_file.count
    basename = os.path.basename(f)           # e.g., tmin_250m_2023_09_01.tif
    parts = basename.split("_")
    file_year = int(parts[2])
    file_month = int(parts[3].split(".")[0])
    for day in range(1, n_bands+1):
        doy = date(file_year, file_month, day).timetuple().tm_yday
        doy_list.append(doy)

doy_arr = np.array(doy_list)

def find_first_frost_day(block_stack, doy_arr, thresh, consecutive_days=1):
    n_layers, nrows, ncols = block_stack.shape
    out_block = np.full((nrows, ncols), np.nan, dtype="float32")
    for r in range(nrows):
        for c in range(ncols):
            px = block_stack[:, r, c]
            if np.all(np.isnan(px)):
                continue
            if consecutive_days == 1:
                frost_indices = np.where(px <= thresh)[0]
                if frost_indices.size > 0:
                    out_block[r, c] = doy_arr[frost_indices[0]]
            else:
                for i in range(n_layers - consecutive_days + 1):
                    window_vals = px[i:i+consecutive_days]
                    if np.all(window_vals <= thresh):
                        out_block[r, c] = doy_arr[i]
                        break
    return out_block

# --- Read all daily bands into a big stack ---
block_stack = []
for f in tmin_files:
    with rasterio.open(f) as s:
        data = s.read().astype("float32")  # shape: (bands, rows, cols)
        block_stack.append(data)
block_stack = np.vstack(block_stack)  # shape: (total_days, rows, cols)

for N in [1, 3, 5]:
    out_file = os.path.join(out_dir, f"first_frost_{year}_{N}.tif")
    out_block = find_first_frost_day(block_stack, doy_arr, thresh, consecutive_days=N)
    with rasterio.open(out_file, "w", **meta) as dst_out:
        dst_out.write(out_block, 1)
    print(f"Saved {out_file}")

EOF

done

echo "First frost processing complete!"

