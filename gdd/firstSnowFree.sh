#!/bin/bash
# Snow-free DOY detection and agreement map (Feb–May), windowed pixel-wise

set -euo pipefail

snow_dir="/mnt/CEPH_PROJECTS/Environtwin/GDD/snow"
out_dir="/mnt/CEPH_PROJECTS/Environtwin/GDD/SouthTyrol"
mkdir -p "$out_dir"

for year in {2015..2020}; do
    echo "❄️  Processing snow for year $year"

    python3 - <<EOF
import os, glob, numpy as np
import rasterio
from rasterio.enums import Resampling

year = "$year"
snow_dir = "$snow_dir"
out_dir  = "$out_dir"

# --- Collect Feb–May snow stacks ---
snow_files = sorted(glob.glob(os.path.join(snow_dir, f"{year}0[2-5]_SNOW_stack*.tif")))
if not snow_files:
    print(f"⚠️ No snow files for {year}")
    raise SystemExit(0)

print(f"Found {len(snow_files)} snow stacks for {year}:")
for f in snow_files:
    print("  ", os.path.basename(f))

# --- Open first snow stack to get metadata and DOYs ---
with rasterio.open(snow_files[0]) as src:
    meta = src.profile
    meta.update(count=1, dtype="float32", compress="deflate")
    width, height = src.width, src.height
    band_names = src.descriptions
    if not all(band_names):
        raise ValueError("Band names missing!")
    doys = np.array([int(b[3:]) for b in band_names])  # 'DOY1' -> 1
    n_layers = len(doys)

snow_free_file = os.path.join(out_dir, f"snow_free_day_{year}.tif")
window_size = 10
threshold = 90  # snow-free < 90

# --- Windowed processing ---
with rasterio.open(snow_free_file, "w", **meta) as dst_out:
    for ji, window in src.block_windows(1):
        # Read all snow files for this window
        block_stack = []
        for f in snow_files:
            with rasterio.open(f) as s:
                data = s.read(window=window).astype("float32")
                data[data == 205] = np.nan
                block_stack.append(data)
        block_stack = np.stack(block_stack)  # shape: (layers, rows, cols)
        n_layers, nrows, ncols = block_stack.shape

        out_block = np.full((nrows, ncols), np.nan, dtype="float32")

        # Pixel-wise first 10 consecutive snow-free days
        for r in range(nrows):
            for c in range(ncols):
                px = block_stack[:, r, c]
                if np.all(np.isnan(px)):
                    continue
                snow_free = px < threshold
                for i in range(len(snow_free) - window_size + 1):
                    window_px = snow_free[i:i+window_size]
                    if np.all(~np.isnan(window_px)) and np.all(window_px):
                        out_block[r, c] = doys[i]
                        break

        dst_out.write(out_block, 1, window=window)

print(f"✅ Saved snow-free DOY raster: {snow_free_file}")

# --- Agreement map with first_over5 ---
first5_files = glob.glob(os.path.join(out_dir, f"first_over5_{year}*.tif"))
if not first5_files:
    print(f"⚠️ No first_over5 file for {year}, skipping agreement.")
    raise SystemExit(0)
first5_file = first5_files[0]

agree_file = os.path.join(out_dir, f"snow_vs_temperature_doy_agreement_{year}.tif")

with rasterio.open(snow_free_file) as snow_src, rasterio.open(first5_file) as temp_src:
    snow = snow_src.read(1)
    temp = temp_src.read(
        1,
        out_shape=(snow_src.height, snow_src.width),
        resampling=Resampling.nearest
    )

    cond = (snow < temp).astype("uint8")
    profile = snow_src.profile
    profile.update(count=1, dtype="uint8", compress="deflate")

    with rasterio.open(agree_file, "w", **profile) as dst:
        dst.write(cond, 1)

print(f"✅ Saved snow vs temperature agreement: {agree_file}")
EOF

done

echo "🎉 Snow processing complete!"


