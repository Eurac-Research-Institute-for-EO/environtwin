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
BASE_ROOT="/mnt/CEPH_BASEDATA/SATELLITE/PLANET/PA"
OUTPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/AW"
TMP_DIR="$OUTPUT_DIR/planet_batches"
PARALLEL_JOBS=4

MASK_FILE="/mnt/CEPH_PROJECTS/Environtwin/gis/masks/AW_mask.tif"

# Batch range to process: set START_BATCH=1 and END_BATCH=9999 (or a suitably large number) 
# to process all batches without filtering.
#START_BATCH=0
#END_BATCH=1

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
# Read mask extent 
# -----------------------------------
read xmin ymin xmax ymax <<< $(gdalinfo -json "$MASK_FILE" \
    | jq -r '.cornerCoordinates | "\(.lowerLeft[0]) \(.lowerLeft[1]) \(.upperRight[0]) \(.upperRight[1])"')

export xmin ymin xmax ymax MASK_FILE

# -----------------------------------
# Function to process a single TIFF
# -----------------------------------
process_tiff() {
    local TIFF_FILE="$1"
    local BASENAME=$(basename "$TIFF_FILE" .tif)
    local DIRNAME=$(dirname "$TIFF_FILE")

    # Identify type
    if [[ "$BASENAME" == *8b_harmonized_clip ]]; then
        base="${BASENAME%_3B_AnalyticMS_SR_8b_harmonized_clip}"
        BAND_TYPE="8b"
    else
        base="${BASENAME%_3B_AnalyticMS_SR_harmonized_clip}"
        BAND_TYPE="4b"
    fi

    JSON_FILE="$DIRNAME/${base}_metadata.json"
    if [[ ! -f "$JSON_FILE" ]]; then
        echo "$BASENAME" >> "$MISSING_LOG"
        return
    fi
 
    # -----------------------
    # Metadata classification
    # -----------------------
    INST=$(jq -r '.properties.instrument // empty' "$JSON_FILE")
    GCP=$(jq -r '.properties.ground_control // empty' "$JSON_FILE" | tr -d '[:space:]')

    if [[ "$GCP" == "true" ]]; then
        DEST="$OUTPUT_DIR/standard"
    else
        DEST="$OUTPUT_DIR/test"
    fi
    mkdir -p "$DEST"

    OUT_FILE="${DEST}/${base}_PLANET_${INST}_BOA.tif"
    if [[ -f "$OUT_FILE" ]]; then
        echo "$BASENAME" >> "$SKIPPED_LOG"
        return
    fi

    TMP_WARP="$DIRNAME/${base}_warp.tif"

    # -----------------------
    # Warp to fixed mask extent
    # -----------------------
gdalwarp -overwrite \
    -t_srs EPSG:32632 \
    -te $xmin $ymin $xmax $ymax \
    -dstnodata 0 \
    "$TIFF_FILE" "$TMP_WARP"
    
    # -----------------------
    # Verify valid pixels to avoid NaNs
    # -----------------------
    MIN_VAL=$(gdalinfo -stats "$TMP_WARP" 2>/dev/null | grep STATISTICS_MINIMUM | head -1 | cut -d= -f2)

    if [[ -z "$MIN_VAL" || "$MIN_VAL" == "nan" ]]; then
        echo "Empty raster after warp: $BASENAME"
        echo "$BASENAME" >> "$SKIPPED_LOG"
        rm -f "$TMP_WARP"
        return
    fi

    # -----------------------
    # Copy metadata
    # -----------------------
    cp "$JSON_FILE" "${DEST}/${base}_metadata.json"

    # -----------------------
    # 8-band vs 4-band handling
    # -----------------------
    if [[ "$BAND_TYPE" == "8b" ]]; then
        echo "Processing 8-band TIFF: $BASENAME"
        if gdal_translate -b 2 -b 4 -b 6 -b 8 "$TMP_WARP" "$OUT_FILE" >/dev/null 2>&1; then
            echo "Created 8-band output: $OUT_FILE"
        else
            echo "Failed to subset bands: $BASENAME" >&2
            echo "$BASENAME" >> "$SKIPPED_LOG"
            rm -f "$TMP_WARP"
            return
        fi
    else
        echo "Processing 4-band TIFF: $BASENAME"
        mv "$TMP_WARP" "$OUT_FILE"
        echo "Moved 4-band TIFF -> $OUT_FILE"
    fi

    rm -f "$TMP_WARP"

    # -----------------------
    # Copy UDM
    # -----------------------
    UDM_FILE="$DIRNAME/${base}_3B_udm2_clip.tif"
    if [[ -f "$UDM_FILE" ]]; then
        OUT_UDM="${DEST}/${base}_PLANET_udm2.tif"
        gdalwarp -overwrite -te $xmin $ymin $xmax $ymax -dstnodata 0 "$UDM_FILE" "$OUT_UDM"
    else
        echo "UDM not found: $BASENAME"
    fi

    echo "$BASENAME" >> "$PROCESSED_LOG"
}

# Export function & variables for parallel
export -f process_tiff
export OUTPUT_DIR MASK_FILE PROCESSED_LOG SKIPPED_LOG MISSING_LOG

# ===============================
# MAIN LOOP: Batch ZIPs with range filter
# ===============================
for ZIP_FILE in "$BASE_ROOT"/batch_*.zip; do
    if [[ ! -f "$ZIP_FILE" ]]; then
        echo "No batch ZIP found: $ZIP_FILE"
        continue
    fi

    # Extract batch number from filename, e.g., batch_98.zip -> 98
    BASENAME=$(basename "$ZIP_FILE")
    #BATCH_NUM=$(echo "$BASENAME" | sed -n 's/^batch_\([0-9]\+\)\.zip$/\1/p')

    # Check if BATCH_NUM is numeric and within the configured range
    #if [[ -z "$BATCH_NUM" ]]; then
    #    echo "Warning: Cannot extract batch number from $BASENAME, skipping."
    #    continue
    #fi

    #if (( BATCH_NUM < START_BATCH || BATCH_NUM > END_BATCH )); then
    #    echo "Skipping batch outside range: $BASENAME"
    #    continue
    #fi

    echo "Processing batch archive: $ZIP_FILE"

    EXTRACT_DIR="$TMP_DIR/$(basename "$ZIP_FILE" .zip)"
        rm -rf "$EXTRACT_DIR"
        mkdir -p "$EXTRACT_DIR"

        echo "Extracting $ZIP_FILE to $EXTRACT_DIR"
        unzip -oq "$ZIP_FILE" -d "$EXTRACT_DIR"

        # Find all TIFFs
        TIFF_FILES=$(find "$EXTRACT_DIR/files" -type f \( -name '*3B_AnalyticMS_SR_8b_harmonized_clip.tif' -o -name '*3B_AnalyticMS_SR_harmonized_clip.tif' \))
        if [[ -z "$TIFF_FILES" ]]; then
            echo "No TIFF files found in $EXTRACT_DIR"
            continue
        fi

        # Run processing in parallel
        echo "$TIFF_FILES" | tr '\n' '\0' | xargs -0 -n 1 -P "$PARALLEL_JOBS" \
    bash -c 'process_tiff "$1"' _

        # Clean up extraction folder
        rm -rf "$EXTRACT_DIR"
        echo "Cleaned up extracted batch: $EXTRACT_DIR"
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

