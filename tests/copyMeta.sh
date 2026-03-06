# ============================================================
# PLANET TIFF/UDM Processing Script
#
# This script processes PLANET imagery batches stored in ZIP files.
# Features:
#  - Handles both 8-band and 4-band TIFFs.
#  - Subsets 8-band images to bands 2,4,6,8 using gdal_translate.
#  - Renames/moves 4-band images.
#  - Copies corresponding UDM files.
#  - Organizes outputs into "test" or "standard" folders based on JSON metadata.
#  - Supports parallel processing for faster batch handling.
#  - Logs processed, skipped, and missing files.
#
# Requirements:
#  - jq
#  - gdal_translate (from GDAL)
# ============================================================

# -----------------------------------
# Configuration
# -----------------------------------
BASE_ROOT="/mnt/CEPH_BASEDATA/SATELLITE/PLANET/Malser_Heide_L2"
#BASE_ROOT="/mnt/CEPH_PROJECTS/Environtwin/PLANET/MalserHeide"
OUTPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH"
YEARS=("2017" "2018" "2019" "2020" "2021" "2022" "2023" "2024" "2025")                        # Years to process
TMP_DIR="/tmp/planet_batches"         # Temporary extraction folder
PARALLEL_JOBS=2                     # Number of parallel processes

# Persistent log files
PROCESSED_LOG="$OUTPUT_DIR/processed.log"
SKIPPED_LOG="$OUTPUT_DIR/skipped.log"
MISSING_LOG="$OUTPUT_DIR/missing.log"

# Start fresh each run
: > "$PROCESSED_LOG"
: > "$SKIPPED_LOG"
: > "$MISSING_LOG"


# Ensure main output and temp directories exist
mkdir -p "$OUTPUT_DIR/test" "$OUTPUT_DIR/standard" "$TMP_DIR"

# -----------------------------------
# Function: copy_metadata
# -----------------------------------
copy_metadata() {
    local TIFF_FILE="$1"
    local BASENAME=$(basename "$TIFF_FILE" .tif)
    local DIRNAME=$(dirname "$TIFF_FILE")

    # Determine base for JSON
    if [[ "$BASENAME" == *8b_harmonized_clip ]]; then
        base="${BASENAME%_3B_AnalyticMS_SR_8b_harmonized_clip}"
    else
        base="${BASENAME%_3B_AnalyticMS_SR_harmonized_clip}"
    fi

    # Locate JSON metadata file
    JSON_FILE="$DIRNAME/${base}_metadata.json"
    if [[ ! -f "$JSON_FILE" ]]; then
        echo "JSON not found for: $BASENAME"
        echo "$BASENAME" >> "$SKIPPED_LOG"
        return
    fi

    # Extract QC category
    QC=$(jq -r '.properties.quality_category // empty' "$JSON_FILE")

    # Determine destination folder
    if [[ "$QC" == "test" ]]; then
        DEST="$OUTPUT_DIR/test"
    elif [[ "$QC" == "standard" ]]; then
        DEST="$OUTPUT_DIR/standard"
    else
        echo "$BASENAME" >> "$SKIPPED_LOG"
        echo "Unknown quality_category '$QC' for $BASENAME"
        return
    fi

    mkdir -p "$DEST"

    # Copy JSON metadata
    OUT_JSON="${DEST}/${base}_metadata.json"
    cp "$JSON_FILE" "$OUT_JSON"
    echo "Copied JSON metadata: $OUT_JSON"

    # Log the operation
    echo "$BASENAME" >> "$PROCESSED_LOG"
}

# Export function and variables for parallel usage
export -f copy_metadata
export OUTPUT_DIR PROCESSED_LOG SKIPPED_LOG

# -----------------------------------
# Main loop over years and ZIP batches
# -----------------------------------
for YEAR in "${YEARS[@]}"; do
    YEAR_DIR="$BASE_ROOT/$YEAR"
    [[ -d "$YEAR_DIR" ]] || { echo "Year directory does not exist: $YEAR_DIR"; continue; }

    for ZIP_FILE in "$YEAR_DIR"/batch_*.zip; do
        [[ -f "$ZIP_FILE" ]] || { echo "No batch ZIP found in $YEAR_DIR"; continue; }
        echo "Processing batch: $ZIP_FILE"

        # Temporary extraction folder
        EXTRACT_DIR="$TMP_DIR/$(basename "$ZIP_FILE" .zip)"
        rm -rf "$EXTRACT_DIR"
        mkdir -p "$EXTRACT_DIR"

        # Extract ZIP silently
        unzip -oq "$ZIP_FILE" -d "$EXTRACT_DIR"

        # Find all TIFFs (both 4b and 8b) and copy metadata in parallel
        find "$EXTRACT_DIR/files" -type f \( \
            -name '*3B_AnalyticMS_SR_8b_harmonized_clip.tif' -o \
            -name '*3B_AnalyticMS_SR_harmonized_clip.tif' \
        \) -print0 | xargs -0 -n 1 -P "$PARALLEL_JOBS" bash -c 'copy_metadata "$0"' 

        # Clean up extracted folder
        rm -rf "$EXTRACT_DIR"
        echo "Cleaned up extracted batch: $EXTRACT_DIR"
    done
done

# -----------------------------------
# Summary
# -----------------------------------
echo ""
echo "=== Metadata Copy Summary ==="
echo "Processed: $(wc -l < "$PROCESSED_LOG")"
echo "Skipped:   $(wc -l < "$SKIPPED_LOG")"

echo "All batches complete!"

