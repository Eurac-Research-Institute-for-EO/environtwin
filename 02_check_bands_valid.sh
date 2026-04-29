#!/bin/bash

# Root directory containing multiple subfolders with rasters
RASTER_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/test_PA"

# Raster pattern
PATTERN="*_PLANET_BOA.tif"

# Log file location
LOG_FILE="$RASTER_DIR/band_name_check.log"

# Expected band names (lowercase)
EXPECTED_BANDS=("blue" "green" "red" "nir")

# Prepare log file
echo "Raster band validation (parallel) - $(date)" > "$LOG_FILE"
echo "==========================================================" >> "$LOG_FILE"

echo "🔍 Searching recursively in: $RASTER_DIR"
echo "Looking for files matching: $PATTERN"
echo "----------------------------------------------------------"

# Find all raster files recursively
mapfile -t FILES < <(find "$RASTER_DIR" -type f -name "$PATTERN")

if [ ${#FILES[@]} -eq 0 ]; then
    echo "⚠ No rasters found in $RASTER_DIR (including subfolders)"
    exit 1
fi

echo "✅ Found ${#FILES[@]} raster files."
echo "▶ Starting parallel band check..."
echo "----------------------------------------------------------"

# Function to process each raster
check_raster() {
    f="$1"
    band_list=$(gdalinfo "$f" | grep "Description" | sed 's/.*=//' | tr '[:upper:]' '[:lower:]' | tr -d ' ')
    missing=()

    for band in "blue" "green" "red" "nir"; do
        if ! echo "$band_list" | grep -q "$band"; then
            missing+=("$band")
        fi
    done

    if [ ${#missing[@]} -eq 0 ]; then
        echo "✅ $(basename "$f")"
    else
        echo "❌ $(basename "$f") — Missing: ${missing[*]}"
        {
            echo ""
            echo "❌ File: $f"
            echo "Missing bands: ${missing[*]}"
        } >> "$LOG_FILE"
    fi
}

export -f check_raster
export LOG_FILE

# Run in parallel (adjust -j for number of CPU cores)
parallel -j 8 check_raster ::: "${FILES[@]}"

echo "----------------------------------------------------------"
echo "✅ Band validation complete."
echo "📄 Full mismatch report saved to: $LOG_FILE"

