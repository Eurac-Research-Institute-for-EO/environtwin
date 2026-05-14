#!/usr/bin/env bash
set -o errexit
set -o pipefail

# ===============================
# CONFIGURATION
# ===============================
INPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/GDD/SouthTyrol/gdd/DOY"
OUTPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/masks/gdd"
DOCKER_IMAGE="davidfrantz/force"
RESOLUTION=3
JOBS=2
PARALLEL_JOBS=4
YEARS=("2017" "2018" "2019" "2020" "2021" "2022" "2023" "2024" "2025")

mkdir -p "$OUTPUT_DIR"

# ===============================
# LOG FILES
# ===============================
PROCESSED_LOG=$(mktemp)
SKIPPED_LOG=$(mktemp)
MISSING_LOG=$(mktemp)

ERROR_LOG_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/masks/gdd/error_logs"
mkdir -p "$ERROR_LOG_DIR"
export PROCESSED_LOG SKIPPED_LOG MISSING_LOG ERROR_LOG_DIR

# ===============================
# TOTAL IMAGES COUNT
# ===============================
TOTAL_IMAGES=$(find "$INPUT_DIR" -type f -name 'GDD_*.tif' | wc -l)
export TOTAL_IMAGES

# ===============================
# FUNCTION: Process single TIFF
# ===============================
process_tiff() {
    local TIFF_FILE="$1"
    local OUTPUT_DIR="$2"
    local DOCKER_IMAGE="$3"
    local RESOLUTION="$4"
    local JOBS="$5"

    local BASENAME
    BASENAME=$(basename "$TIFF_FILE" .tif)
    local ERROR_LOG="${ERROR_LOG_DIR}/${BASENAME}.log"

    # Skip if output already exists
    #if find "$OUTPUT_DIR" -type f -name "${BASENAME}*" -print -quit | grep -q .; then
    #    echo "$BASENAME" >> "$SKIPPED_LOG"
    #    echo "Skipping $TIFF_FILE (already processed)"
    #    return
    #fi

    local MOUNT_DIR
    MOUNT_DIR=$(dirname "$TIFF_FILE")
    local REL_PATH
    REL_PATH=$(basename "$TIFF_FILE")

    if ! docker run --rm --user "$(id -u):$(id -g)" \
        -v "${MOUNT_DIR}:/data/input:ro" \
        -v "${OUTPUT_DIR}:/data/output" \
        "${DOCKER_IMAGE}" bash -c "
            set -e
            INPUT_FILE=\"/data/input/${REL_PATH}\"
            force-cube -r cubic -s ${RESOLUTION} -n -9999 -t Int16 -j ${JOBS} -o /data/output \"\$INPUT_FILE\"
        " >"$ERROR_LOG" 2>&1; then
        echo "$BASENAME" >> "$MISSING_LOG"
        echo "Failed: $TIFF_FILE (see $ERROR_LOG)"
        return
    fi

    if ! find "$OUTPUT_DIR" -type f -name "${BASENAME}*" -print -quit | grep -q .; then
        echo "$BASENAME" >> "$MISSING_LOG"
        echo "No output found for $TIFF_FILE"
        return
    fi

    rm -f "$ERROR_LOG"
    echo "$BASENAME" >> "$PROCESSED_LOG"
    echo "Done: $TIFF_FILE"
}
export -f process_tiff

# ===============================
# SINGLE-FILE MODE
# ===============================
if [[ $# -eq 1 ]]; then
    SINGLE_FILE="$1"
    FILE_PATH=$(find "$INPUT_DIR" -type f -name "$SINGLE_FILE" | head -n 1)
    [[ -z "$FILE_PATH" ]] && { echo "❌ File not found: $SINGLE_FILE"; exit 1; }

    process_tiff "$FILE_PATH" "$OUTPUT_DIR" "$DOCKER_IMAGE" "$RESOLUTION" "$JOBS"
    echo "✅ Single-file processing complete: $SINGLE_FILE"
    exit 0
fi

# ===============================
# MAIN
# ===============================
find "$INPUT_DIR" -type f -name 'GDD_*.tif' | \
    parallel -P "$PARALLEL_JOBS" process_tiff {} "$OUTPUT_DIR" "$DOCKER_IMAGE" "$RESOLUTION" "$JOBS" || true

# ===============================
# SUMMARY
# ===============================
echo ""
echo "=== Processing Summary ==="
echo "Total input images found: $TOTAL_IMAGES"
echo "Processed: $(wc -l < "$PROCESSED_LOG")"
#echo "Skipped:   $(wc -l < "$SKIPPED_LOG")"
echo "Missing:   $(wc -l < "$MISSING_LOG")"

cp "$MISSING_LOG" "missing_files.log"

if [[ -s "$MISSING_LOG" ]]; then
    echo "⚠️ Some files failed. See logs in $ERROR_LOG_DIR and missing_files.log"
else
    echo "✅ All files processed successfully."
fi

rm "$PROCESSED_LOG" "$SKIPPED_LOG" 
#"$MISSING_LOG"

