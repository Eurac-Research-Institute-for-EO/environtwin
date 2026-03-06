#!/usr/bin/env bash
set -o errexit
set -o pipefail

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
YEARS=("2017" "2018" "2019" "2020" "2021" "2022" "2023" "2024")                       # Years to process
TMP_DIR="$OUTPUT_DIR/planet_batches"
PARALLEL_JOBS=4

MASK_FILE="/mnt/CEPH_PROJECTS/Environtwin/gis/masks/MH_mask.tif"

# Persistent log files
PROCESSED_LOG="$OUTPUT_DIR/processed.log"
SKIPPED_LOG="$OUTPUT_DIR/skipped.log"
MISSING_LOG="$OUTPUT_DIR/missing.log"

# Start fresh
: > "$PROCESSED_LOG"
: > "$SKIPPED_LOG"
: > "$MISSING_LOG"

mkdir -p "$OUTPUT_DIR/test" "$OUTPUT_DIR/standard" "$TMP_DIR"

# -----------------------------------
# Function: process a single TIFF
# -----------------------------------
process_tiff() {
    local TIFF_FILE="$1"
    local MASK_FILE="$2"
    local BASENAME=$(basename "$TIFF_FILE" .tif)
    local DIRNAME=$(dirname "$TIFF_FILE")

    # Determine base for JSON and UDM (remove 8b/4b suffix)
    if [[ "$BASENAME" == *8b_harmonized_clip ]]; then
        base="${BASENAME%_3B_AnalyticMS_SR_8b_harmonized_clip}"
        BAND_TYPE="8b"
    else
        base="${BASENAME%_3B_AnalyticMS_SR_harmonized_clip}"
        BAND_TYPE="4b"
    fi

    # Locate JSON metadata file
    JSON_FILE="$DIRNAME/${base}_metadata.json"
    if [[ ! -f "$JSON_FILE" ]]; then
        echo "$BASENAME" >> "$MISSING_LOG"
        echo "No metadata found for $BASENAME"
        return
    fi

    # Extract relevant metadata
    INST=$(jq -r '.properties.instrument // empty' "$JSON_FILE")
QC=$(jq -r '.properties.quality_category // empty' "$JSON_FILE" | tr -d '[:space:]')
GCP=$(jq -r '.properties.ground_control // empty' "$JSON_FILE" | tr -d '[:space:]')

# Debug print
echo "DEBUG: $BASENAME QC='$QC' GCP='$GCP'"

# Determine destination
# Explicit test for true
if [[ "$GCP" == "true" ]]; then
    DEST="$OUTPUT_DIR/standard"
else
    DEST="$OUTPUT_DIR/test"
fi
    mkdir -p "$DEST"

    OUT_FILE="${DEST}/${base}_PLANET_${INST}_BOA.tif"
    
    # -----------------------
    # Copy metadata into output folder
    # -----------------------
    cp "$JSON_FILE" "${DEST}/${base}_metadata.json"

    # Skip if output already exists
    if [[ -f "$OUT_FILE" ]]; then
        echo "$BASENAME" >> "$SKIPPED_LOG"
        echo "Skipping existing TIFF: $OUT_FILE"
        return
    fi

     # Read mask extent
    read xmin ymin xmax ymax <<< $(gdalinfo -json "$MASK_FILE" \
        | jq -r '.cornerCoordinates | "\(.lowerLeft[0]) \(.lowerLeft[1]) \(.upperRight[0]) \(.upperRight[1])"')

    TMP_WARP="$DIRNAME/${base}_warp.tif"

    gdalwarp -overwrite -te $xmin $ymin $xmax $ymax "$TIFF_FILE" "$TMP_WARP" || {
        echo "GDALWARP FAILED: $BASENAME"
        echo "$BASENAME" >> "$SKIPPED_LOG"
        rm -f "$TMP_WARP"
        return
    }

    # -----------------------
    # 8-band vs 4-band handling
    # -----------------------
    if [[ "$BAND_TYPE" == "8b" ]]; then
        echo "Processing 8-band TIFF: $BASENAME"
        # Subset bands 2,4,6,8 with gdal_translate
        if gdal_translate -b 2 -b 4 -b 6 -b 8 "$TMP_WARP" "$OUT_FILE" >/dev/null 2>&1; then
            echo "Created 8-band output: $OUT_FILE"
        else
            echo "Failed to subset bands for: $BASENAME" >&2
            echo "$BASENAME" >> "$SKIPPED_LOG"
            return
        fi
    else
        echo "Processing 4-band TIFF: $BASENAME"
        mv "$TMP_WARP" "$OUT_FILE"
        echo "Moved 4-band TIFF -> $OUT_FILE"
    fi

    # Cleanup temp warp
    rm -f "$TMP_WARP"

    # Copy UDM
    UDM_FILE="$DIRNAME/${base}_3B_udm2_clip.tif"
    if [[ -f "$UDM_FILE" ]]; then
        OUT_UDM="${DEST}/${base}_PLANET_udm2.tif"
        gdalwarp -overwrite -te $xmin $ymin $xmax $ymax "$UDM_FILE" "$OUT_UDM"
    else
        echo "UDM not found for: $BASENAME"
    fi

    echo "$BASENAME" >> "$PROCESSED_LOG"
}

# Export function & variables for parallel
export -f process_tiff
export OUTPUT_DIR MASK_FILE PROCESSED_LOG SKIPPED_LOG MISSING_LOG

# -----------------------------------
# Main loop over years & ZIP batches
# -----------------------------------
for YEAR in "${YEARS[@]}"; do
    YEAR_DIR="$BASE_ROOT/$YEAR"
    [[ -d "$YEAR_DIR" ]] || { echo "Year directory does not exist: $YEAR_DIR"; continue; }

    for ZIP_FILE in "$YEAR_DIR"/batch_*.zip; do
        [[ -f "$ZIP_FILE" ]] || continue
        echo "Processing batch: $ZIP_FILE"

        EXTRACT_DIR="$TMP_DIR/$(basename "$ZIP_FILE" .zip)"
        rm -rf "$EXTRACT_DIR"
        mkdir -p "$EXTRACT_DIR"

        unzip -oq "$ZIP_FILE" -d "$EXTRACT_DIR"

        # Run processing in parallel
        parallel -P "$PARALLEL_JOBS" process_tiff {} "$MASK_FILE" ::: "$EXTRACT_DIR/files"/*_3B_AnalyticMS_SR*_harmonized_clip.tif

        # Cleanup extracted batch
        rm -rf "$EXTRACT_DIR"
        echo "Cleaned up extracted batch: $EXTRACT_DIR"
    done
done

# -----------------------------------
# Summary
# -----------------------------------
echo ""
echo "=== Processing Summary ==="
echo "Processed: $(wc -l < "$PROCESSED_LOG")"
echo "Skipped:   $(wc -l < "$SKIPPED_LOG")"
echo "Missing:   $(wc -l < "$MISSING_LOG")"


echo "All batches complete!"

