#!/bin/bash
set -o errexit
set -o pipefail

BASE_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw"
OUTPUT_FILE="$BASE_DIR/wrong_files_PLANET.txt"
PARALLEL_JOBS=8   # adjust number of parallel jobs as needed

> "$OUTPUT_FILE"

# Find all matching TIFFs
files=($(find "$BASE_DIR" -type f \( \
  -iname "*_PLANET_BOA.tif" -o -iname "*_PLANET_udm2_mask.tif" \
\)))

if [[ ${#files[@]} -eq 0 ]]; then
    echo "No matching TIFFs found."
    exit 1
fi

echo "Checking all files for 4000x4000 size and 3m resolution..."
echo "Files not matching expected size/resolution will be logged in $OUTPUT_FILE"

# ===============================
# Function to check a single file
# ===============================
check_file() {
    local f="$1"

    # Width and Height
    width=$(gdalinfo "$f" | awk '/Size is/ {gsub(",",""); print $3}')
    height=$(gdalinfo "$f" | awk '/Size is/ {gsub(",",""); print $4}')

    # Pixel Size (numeric)
    pixel_size=$(gdalinfo "$f" | awk -F'[()]' '/Pixel Size/ {gsub(" ","",$2); print $2}')
    res_x=$(echo $pixel_size | cut -d',' -f1)
    res_y=$(echo $pixel_size | cut -d',' -f2)

    mismatch=0
    if (( $(echo "$width != 4000" | bc -l) )); then mismatch=1; fi
    if (( $(echo "$height != 4000" | bc -l) )); then mismatch=1; fi
    if (( $(echo "$res_x != 3" | bc -l) )); then mismatch=1; fi
    if (( $(echo "$res_y != -3" | bc -l) )); then mismatch=1; fi

    if [[ $mismatch -eq 1 ]]; then
        # Use flock to avoid concurrent writes to the log file
        {
            echo "[MISMATCH] $f → Width: $width, Height: $height, Pixel Size: $res_x,$res_y"
        } >> "$OUTPUT_FILE"
    fi
}

export -f check_file
export OUTPUT_FILE

# ===============================
# Run in parallel
# ===============================
printf "%s\n" "${files[@]}" | parallel -P "$PARALLEL_JOBS" check_file {}

echo "Check complete."

