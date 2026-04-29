#!/usr/bin/env bash
set -o errexit
set -o pipefail

# ===============================
# CONFIGURATION
# ===============================
INPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/gis/masks"
OUTPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/masks/sites"
DOCKER_IMAGE="davidfrantz/force"
RESOLUTION=3
JOBS=2
PARALLEL_JOBS=4  # Number of TIFFs to process in parallel

# Temporary log files
PROCESSED_LOG=$(mktemp)
SKIPPED_LOG=$(mktemp)
MISSING_LOG=$(mktemp)

mkdir -p "$OUTPUT_DIR"

# ===============================
# FUNCTION: Process single TIFF
# ===============================
process_tif() {
    local TIF_FILE="$1"
    local PROCESSED_LOG="$2"
    local SKIPPED_LOG="$3"
    local MISSING_LOG="$4"

    BASENAME=$(basename "$TIF_FILE" .tif)
    OUT_FILE="$OUTPUT_DIR/${BASENAME}.tif"

    # Skip if output already exists
    if [[ -f "$OUT_FILE" ]]; then
        echo "$BASENAME" >> "$SKIPPED_LOG"
        echo "Skipping $TIF_FILE (already exists)"
        return
    fi

    REL_PATH=$(realpath --relative-to="$(dirname "$TIF_FILE")" "$TIF_FILE")

    echo "Processing TIFF: $TIF_FILE"

    # Run force-cube inside Docker
    docker run --rm --user "$(id -u):$(id -g)" \
        -v "$(dirname "$TIF_FILE")":/data/input \
        -v "$OUTPUT_DIR":/data/output \
        "$DOCKER_IMAGE" bash -c "
            set -e
            INPUT_FILE=\"/data/input/$REL_PATH\"
            echo 'Processing TIFF with FORCE: \$INPUT_FILE'

            force-cube -r near -s $RESOLUTION -n -9999 -t Int16 -j $JOBS -o /data/output \"\$INPUT_FILE\"
        "

    if [[ -f "$OUT_FILE" ]]; then
        echo "$BASENAME" >> "$PROCESSED_LOG"
        echo "Finished processing $TIF_FILE"
    else
        echo "$BASENAME" >> "$MISSING_LOG"
        echo "⚠️ Output not created for $TIF_FILE"
    fi
}

export -f process_tif
export OUTPUT_DIR DOCKER_IMAGE RESOLUTION JOBS

# ===============================
# MAIN: Find and process all TIFFs
# ===============================
TIF_FILES=$(find "$INPUT_DIR" -type f -name "*.tif")

if [[ -z "$TIF_FILES" ]]; then
    echo "⚠️ No TIFF files found in $INPUT_DIR"
    exit 0
fi

echo "$TIF_FILES" | tr '\n' '\0' | xargs -0 -n 1 -P "$PARALLEL_JOBS" \
    bash -c 'process_tif "$0" "'"$PROCESSED_LOG"'" "'"$SKIPPED_LOG"'" "'"$MISSING_LOG"'"'

# ===============================
# SUMMARY
# ===============================
echo ""
echo "=== Processing Summary ==="
echo "Processed: $(wc -l < "$PROCESSED_LOG")"
echo "Skipped:   $(wc -l < "$SKIPPED_LOG")"
echo "Missing:   $(wc -l < "$MISSING_LOG")"
echo ""

# Clean up
rm "$PROCESSED_LOG" "$SKIPPED_LOG" "$MISSING_LOG"

echo "✅ All TIFF files processed successfully!"

