#!/usr/bin/env bash
set -o errexit
set -o pipefail

# ===============================
# CONFIGURATION
# ===============================
BASE_ROOT="/mnt/CEPH_BASEDATA/SATELLITE/PLANET/Malser_Heide/TOA"
OUTPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level1"
DOCKER_IMAGE="davidfrantz/force"
RESOLUTION=3
JOBS=4
SKIP_DUPLICATES=1
TMP_DIR="/tmp/planet_batches"
PARALLEL_JOBS=4  # Number of TIFFs to process in parallel
YEARS=("2024")  # Add more years if needed

# Temporary log files for counters
PROCESSED_LOG=$(mktemp)
SKIPPED_LOG=$(mktemp)
MISSING_LOG=$(mktemp)

mkdir -p "$TMP_DIR"

# ===============================
# FUNCTION: Process single TIFF
# Uses log files instead of Bash counters
# ===============================
process_tiff() {
    local TIFF_FILE="$1"
    local PROCESSED_LOG="$2"
    local SKIPPED_LOG="$3"
    local MISSING_LOG="$4"

    BASENAME=$(basename "$TIFF_FILE" .tif)

    # Skip if output exists
    if [[ -f "$OUTPUT_DIR/${BASENAME}.tif" ]]; then
        echo "$BASENAME" >> "$SKIPPED_LOG"
        echo "Skipping $TIFF_FILE (already exists)"
        return
    fi

    REL_PATH=$(realpath --relative-to="$(dirname "$TIFF_FILE")" "$TIFF_FILE")

    # Run force-cube in Docker
    docker run --rm --user "$(id -u):$(id -g)" \
        -v "$(dirname "$TIFF_FILE")":/data/input \
        -v "$OUTPUT_DIR":/data/output \
        "$DOCKER_IMAGE" bash -c "
            set -e
            INPUT_FILE=\"/data/input/$REL_PATH\"
            echo 'Cubing input file: \$INPUT_FILE'
            force-cube -r near -s $RESOLUTION -t Int16 -j $JOBS -o /data/output \"\$INPUT_FILE\"
        "

    echo "$BASENAME" >> "$PROCESSED_LOG"
    echo "Finished processing $TIFF_FILE"
}

export -f process_tiff
export OUTPUT_DIR DOCKER_IMAGE RESOLUTION JOBS

# ===============================
# MAIN LOOP: Years and Batch ZIPs
# ===============================
for YEAR in "${YEARS[@]}"; do
    YEAR_DIR="$BASE_ROOT/$YEAR"
    echo "Processing year: $YEAR"

    if [[ ! -d "$YEAR_DIR" ]]; then
        echo "Year directory does not exist: $YEAR_DIR"
        continue
    fi

    # Loop over all batch ZIPs
    for ZIP_FILE in "$YEAR_DIR"/batch_*.zip; do
        if [[ ! -f "$ZIP_FILE" ]]; then
            echo "No batch ZIP found in $YEAR_DIR"
            continue
        fi

        echo "Processing batch archive: $ZIP_FILE"

        EXTRACT_DIR="$TMP_DIR/$(basename "$ZIP_FILE" .zip)"
        rm -rf "$EXTRACT_DIR"
        mkdir -p "$EXTRACT_DIR"

        echo "Extracting $ZIP_FILE to $EXTRACT_DIR"
        unzip -oq "$ZIP_FILE" -d "$EXTRACT_DIR"

        # Find all TIFFs
        TIFF_FILES=$(find "$EXTRACT_DIR/files" -type f -name '*_AnalyticMS_8b_clip.tif')
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




