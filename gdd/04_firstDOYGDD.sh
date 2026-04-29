#!/bin/bash
# Batch process all GDD_BT5_*_dynamic_cumulative.tif files to compute first DOY > multiple thresholds

set -e

# --- Hardcoded paths ---
in_dir="/mnt/CEPH_PROJECTS/Environtwin/GDD/SouthTyrol/gdd"
out_dir="/mnt/CEPH_PROJECTS/Environtwin/GDD/SouthTyrol/gdd/DOY"

# Define thresholds to process
thresholds=(100 150 200)



mkdir -p "$out_dir"

# Loop over all relevant GDD_BT5 files
for infile in "$in_dir"/GDD_daily_cumulative_T5_*.tif; do
    [ -e "$infile" ] || continue  # skip if no files match
    filename=$(basename "$infile")
    
    # Extract year from filename
    if [[ "$filename" =~ GDD_daily_cumulative_T5_([0-9]{4})\.tif ]]; then
        year="${BASH_REMATCH[1]}"
    else
        echo "Skipping $filename, cannot extract year."
        continue
    fi

    # Loop through all thresholds
    for threshold in "${thresholds[@]}"; do
        outfile="$out_dir/GDD_${year}_${threshold}.tif"
        echo "Processing $filename → $outfile (threshold=$threshold)"

        python3 - <<EOF
import rasterio
import numpy as np

infile = "$infile"
outfile = "$outfile"
threshold = float("$threshold")

with rasterio.open(infile) as src:
    profile = src.profile
    n_bands = src.count
    
    nodata = -9999

    try:
        DOY = np.array([int(band.split("_")[1]) for band in src.descriptions])
    except Exception:
        DOY = np.arange(1, n_bands + 1)

    data = src.read().astype("int16")

mask = data > threshold
first_doy = np.full(data.shape[1:], nodata, dtype="int16")
valid = mask.any(axis=0)
first_doy[valid] = DOY[np.argmax(mask, axis=0)][valid]

profile.update(count=1, dtype="int16", nodata=nodata, compress='deflate')

with rasterio.open(outfile, "w", **profile) as dst:
    dst.write(first_doy, 1)

print(f"✅ Done {outfile}")
EOF

    done
done

echo "✅ All files and thresholds processed."



