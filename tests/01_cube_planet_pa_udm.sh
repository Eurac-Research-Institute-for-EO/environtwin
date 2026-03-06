#!/usr/bin/env bash
set -o errexit
set -o pipefail

# ===============================
# CONFIGURATION
# ===============================
BASE_ROOT="/mnt/CEPH_PROJECTS/Environtwin/PLANET/PA/2025"
OUTPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw"
DOCKER_IMAGE="davidfrantz/force"
RESOLUTION=3
NODATA=-9999
JOBS=2
PARALLEL_JOBS=10  # Number of TIFFs to process in parallel
TMP_DIR="/tmp/planet_batches"

# Batch range to process: set START_BATCH=1 and END_BATCH=9999 (or a suitably large number) 
# to process all batches without filtering.
#START_BATCH=1
#END_BATCH=158

# Temporary log files
PROCESSED_LOG=$(mktemp)
SKIPPED_LOG=$(mktemp)
MISSING_LOG=$(mktemp)

mkdir -p "$TMP_DIR"

# ===============================
# FUNCTION: Process single TIFF
# ===============================
process_tiff() {
    local TIFF_FILE="$1"
    local PROCESSED_LOG="$2"
    local SKIPPED_LOG="$3"
    local MISSING_LOG="$4"

    BASENAME=$(basename "$TIFF_FILE" .tif)

    # Skip if output already exists
    if [[ -f "$OUTPUT_DIR/$BASENAME.tif" ]]; then
        echo "$BASENAME" >> "$SKIPPED_LOG"
        echo "Skipping $TIFF_FILE (already exists)"
        return
    fi

    REL_PATH=$(realpath --relative-to="$(dirname "$TIFF_FILE")" "$TIFF_FILE")

    docker run --rm --user "$(id -u):$(id -g)" \
        -v "$(dirname "$TIFF_FILE")":/data/input \
        -v "$OUTPUT_DIR":/data/output \
        -v /tmp:/tmp \
        "$DOCKER_IMAGE" bash -c "
            set -e
            INPUT_FILE=\"/data/input/$REL_PATH\"
            BN=\$(basename \"\$INPUT_FILE\" .tif)
            VRT=\"/tmp/\${BN}.vrt\"

            echo 'Building VRT with NoData=$NODATA ...'
            gdal_translate -of VRT -ot Int16 -a_nodata $NODATA \"\$INPUT_FILE\" \"\$VRT\"

            echo 'Cubing VRT ...'
            force-cube -r near -s $RESOLUTION -t Int16 -n $NODATA -j $JOBS -o /data/output \"\$VRT\"

            # Rename output from FORCE naming to PLANET naming (remove _3B)
            NEW_NAME=\$(echo \"\$BN\" | sed 's/_3B_/_/')
            if [[ -f /data/output/\$BN.tif ]]; then
                mv /data/output/\$BN.tif /data/output/\$NEW_NAME.tif
                echo \"Renamed output to: \$NEW_NAME.tif\"
            fi

            rm -f \"\$VRT\"
        "

    echo "$BASENAME" >> "$PROCESSED_LOG"
    echo "Finished processing $TIFF_FILE"
}

export -f process_tiff
export OUTPUT_DIR DOCKER_IMAGE RESOLUTION JOBS NODATA

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
    TIFF_FILES=$(find "$EXTRACT_DIR" -type f -name '*_3B_udm2_clip.tif')
    if [[ -z "$TIFF_FILES" ]]; then
        echo "No TIFF files found in $EXTRACT_DIR"
        continue
    fi

    # Run processing in parallel
    echo "$TIFF_FILES" | tr '\n' '\0' | xargs -0 -n 1 -P "$PARALLEL_JOBS" \
        bash -c 'process_tiff "$0" "'"$PROCESSED_LOG"'" "'"$SKIPPED_LOG"'" "'"$MISSING_LOG"'"'

    # Clean up extraction folder
    rm -rf "$EXTRACT_DIR"
    echo "Cleaned up extracted batch: $EXTRACT_DIR"
done

# ===============================
# SUMMARY
# ===============================
echo ""
echo "=== Processing Summary ==="
echo "Processed: $(wc -l < "$PROCESSED_LOG")"
echo "Skipped:   $(wc -l < "$SKIPPED_LOG")"
echo "Missing:   $(wc -l < "$MISSING_LOG")"

# Clean up temp logs
rm "$PROCESSED_LOG" "$SKIPPED_LOG" "$MISSING_LOG"

echo "All batches complete!"

