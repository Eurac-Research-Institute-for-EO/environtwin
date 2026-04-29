#!/bin/bash
# Compute first stable NDVI day (>1000) using windowed processing and create agreement map

set -euo pipefail

# --- Hardcoded paths ---
in_dir="/mnt/CEPH_PROJECTS/Environtwin/FORCE/P_level3/X-001_Y-001"
out_dir="/mnt/CEPH_PROJECTS/Environtwin/GDD/SouthTyrol"
mkdir -p "$out_dir"

# --- Years to process ---
for year in 2024; do
    echo "🟢 Processing NDVI for year $year"

    outfile="$out_dir/first_posNDVI_${year}.tif"

    python3 - <<EOF
import os, glob, rasterio, numpy as np
from rasterio.enums import Resampling

in_dir  = "$in_dir"
out_dir = "$out_dir"
year    = "$year"
outfile = "$outfile"

# --- Find matching files ---
files = sorted(glob.glob(os.path.join(in_dir, f"*{year}*_PLA_NDV_TSS.tif")))
if not files:
    raise SystemExit(f"No NDVI files found for {year} in {in_dir}")

print(f"Found {len(files)} NDVI files for {year}:")
for f in files:
    print("  ", os.path.basename(f))

# --- Open raster ---
with rasterio.open(files[0]) as src:
    meta = src.profile
    meta.update(count=1, dtype="float32", compress="deflate")

    band_names = src.descriptions
    if not all(band_names):
        print("⚠️ Band names missing — using sequential DOYs instead.")
        doys = np.arange(1, src.count + 1)
    else:
        doys = np.array([int(b[9:12]) for b in band_names])  # characters 10–12

    print("Extracted DOYs:", doys[:10], "...")

    # --- Prepare output raster ---
    with rasterio.open(outfile, "w", **meta) as dst:

        # Process in windows (block by block)
        for ji, window in src.block_windows(1):
            data = src.read(window=window).astype("float32")  # shape (bands, rows, cols)
            n_layers, nrows, ncols = data.shape

            # Prepare output block
            out_block = np.full((nrows, ncols), np.nan, dtype="float32")

            # Vectorized detection of first stable NDVI > 1000
            mask = data > 1000
            valid = mask.any(axis=0)  # pixels with at least one NDVI > 1000
            first_idx = np.argmax(mask, axis=0)
            out_block[valid] = doys[first_idx[valid]]

            dst.write(out_block, 1, window=window)

print(f"✅ NDVI saved: {outfile}")

# --- Agreement map ---
import re
first5_files = glob.glob(os.path.join(out_dir, f"first_over5_{year}*.tif"))
if not first5_files:
    print(f"⚠️ No first_over5 file for {year}, skipping agreement.")
    raise SystemExit(0)
first5_file = first5_files[0]

agree_file = os.path.join(out_dir, f"agreement_difference_map_{year}.tif")

with rasterio.open(outfile) as ndvi_src, rasterio.open(first5_file) as temp_src:
    ndvi = ndvi_src.read(1).astype("float32")
    temp = temp_src.read(
        1,
        out_shape=(ndvi_src.height, ndvi_src.width),
        resampling=Resampling.nearest
    ).astype("float32")

    cond = (ndvi < temp).astype("float32")  # 1 if NDVI before TEMP, else 0
    diff = ndvi - temp

    profile = ndvi_src.profile
    profile.update(count=2, dtype="float32", compress="deflate")

    with rasterio.open(agree_file, "w", **profile) as dst:
        dst.write(cond, 1)
        dst.write(diff, 2)

print(f"✅ Agreement map saved: {agree_file}")
EOF

done

echo "🎉 All years processed successfully!"

