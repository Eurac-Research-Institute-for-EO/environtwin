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
OUT_ROOT="/mnt/CEPH_PROJECTS/Environtwin/FORCE/test"
TMP_DIR="$OUT_ROOT/planet_batches"
PARALLEL_JOBS=4

# Declare associative array (Bash 4+)
declare -A MASK_FILES_PAIRS=(
    [1]="/mnt/CEPH_PROJECTS/Environtwin/gis/masks/AW_mask.tif"
    [2]="/mnt/CEPH_PROJECTS/Environtwin/gis/masks/FSP_mask.tif"
    [3]="/mnt/CEPH_PROJECTS/Environtwin/gis/masks/R_mask.tif"
    [4]="/mnt/CEPH_PROJECTS/Environtwin/gis/masks/PG1_mask.tif"
    [5]="/mnt/CEPH_PROJECTS/Environtwin/gis/masks/SA_mask.tif"
    [6]="/mnt/CEPH_PROJECTS/Environtwin/gis/masks/TH_mask.tif"
    [7]="/mnt/CEPH_PROJECTS/Environtwin/gis/masks/HS_mask.tif"
    [8]="/mnt/CEPH_PROJECTS/Environtwin/gis/masks/TG_mask.tif"
    [9]="/mnt/CEPH_PROJECTS/Environtwin/gis/masks/PG2_mask.tif"
)

# -----------------------------------
# Function to process a single TIFF
# -----------------------------------
process_tiff() {
    local TIFF_FILE="$1"
    local MASK_FILE="$2"
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
   gdalwarp -overwrite -te $xmin $ymin $xmax $ymax "$TIFF_FILE" "$TMP_WARP" || {
        echo "GDALWARP FAILED: $BASENAME"
        echo "$BASENAME" >> "$SKIPPED_LOG"
        rm -f "$TMP_WARP"
        return
    }

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
for ((i=1; i<=9; i++)); do
    AOI_NUM="$i"
    MASK_FILE="${MASK_FILES_PAIRS[$AOI_NUM]}"

    # Input dir (create if needed)
    AOI_DIR="$BASE_ROOT/$AOI_NUM"
    
    # Site-named output (map 1->AW, 2->FSP, etc.; customize array)
    declare -A AOI_NAMES=(
        [1]="AW" [2]="FSP" [3]="R" [4]="PG1" [5]="SA"
        [6]="TH" [7]="HS" [8]="TG" [9]="PG2"
    )
    SITE_NAME="${AOI_NAMES[$AOI_NUM]:-$AOI_NUM}"  # Fallback to num
    OUTPUT_DIR="$OUT_ROOT/$SITE_NAME"
    mkdir -p "$OUTPUT_DIR/test" "$OUTPUT_DIR/standard"
    
    # Site-specific logs
    PROCESSED_LOG="$OUTPUT_DIR/processed.log"
    SKIPPED_LOG="$OUTPUT_DIR/skipped.log"
    MISSING_LOG="$OUTPUT_DIR/missing.log"
    : > "$PROCESSED_LOG"  # Reset per site
    : > "$SKIPPED_LOG"
    : > "$MISSING_LOG"
    
    if [[ -z "$MASK_FILE" || ! -f "$MASK_FILE" ]]; then
        echo "Missing mask for folder $FOLDER_NUM: $MASK_FILE"
        continue
    fi
    
    AOI_DIR="$BASE_ROOT/$AOI_NUM"
    [[ -d "$AOI_DIR" ]] || { echo "Dir missing: $AOI_DIR"; continue; }
    
    echo "Processing AOI $AOI_NUM (folder $FOLDER_NUM, mask: $(basename $MASK_FILE))"

      # Read extent
    read xmin ymin xmax ymax <<< $(gdalinfo -json "$MASK_FILE" \
    | jq -r '.cornerCoordinates | "\(.lowerLeft[0]) \(.lowerLeft[1]) \(.upperRight[0]) \(.upperRight[1])"')
    
    export xmin ymin xmax ymax MASK_FILE OUTPUT_DIR PROCESSED_LOG SKIPPED_LOG MISSING_LOG AOI_DIR

    for ZIP_FILE in "$AOI_DIR"/batch_*.zip; do
        [[ -f "$ZIP_FILE" ]] || { echo "No ZIP in $AOI_DIR"; continue; }

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

