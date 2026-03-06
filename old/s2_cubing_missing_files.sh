#!/usr/bin/env bash
set -o errexit
set -o pipefail

# ===============================
# CONFIGURATION
# ===============================
#INPUT_DIR="/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level2/X0003_Y0004"
INPUT_DIRS=(
    "/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level2/X0003_Y0004"
    #"/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level2/X0000_Y0003"
    #"/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level2/X0002_Y0005"
    #"/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level2/X0004_Y0004"
    #"/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level2/X0002_Y0003"
)
LOG_FILE="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/wrong_files_SEN2.txt"
OUTPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/test_PA"
DOCKER_IMAGE="davidfrantz/force"
RESOLUTION=3
JOBS=2
PARALLEL_JOBS=4  # Number of TIFFs to process in parallel

mkdir -p "$OUTPUT_DIR"
ERROR_LOG_DIR="error_logs"
mkdir -p "$ERROR_LOG_DIR"

PROCESSED_LOG=$(mktemp)
MISSING_LOG=$(mktemp)

export PROCESSED_LOG MISSING_LOG ERROR_LOG_DIR OUTPUT_DIR DOCKER_IMAGE RESOLUTION JOBS

# ===============================
# FUNCTION: Process single TIFF
# ===============================
process_tiff() {
    local TIFF_FILE="$1"
    local BASENAME
    BASENAME=$(basename "$TIFF_FILE" .tif)
    local ERROR_LOG="${ERROR_LOG_DIR}/${BASENAME}.log"

    if [[ ! -f "$TIFF_FILE" ]]; then
        echo "⚠️  File not found: $TIFF_FILE"
        echo "$BASENAME" >> "$MISSING_LOG"
        return
    fi

    local MOUNT_DIR
    MOUNT_DIR=$(dirname "$TIFF_FILE")
    local REL_PATH
    REL_PATH=$(basename "$TIFF_FILE")

    # Run Docker and log errors
    if ! docker run --rm --user "$(id -u):$(id -g)" \
        -v "${MOUNT_DIR}":/data/input \
        -v "${OUTPUT_DIR}":/data/output \
        "${DOCKER_IMAGE}" bash -c "
            set -e
            INPUT_FILE=\"/data/input/${REL_PATH}\"
            TEMP_FILE=\"/tmp/${BASENAME}.tif\"
            echo \"Cubing input file: \$INPUT_FILE (bands 1,2,3,7)\"
            gdal_translate -b 1 -b 2 -b 3 -b 7 \"\$INPUT_FILE\" \"\$TEMP_FILE\"
            force-cube -r near -s ${RESOLUTION} -n -9999 -t Int16 -j ${JOBS} -o /data/output \"\$TEMP_FILE\"
            rm -f \"\$TEMP_FILE\"
        " >"$ERROR_LOG" 2>&1; then
        echo "$BASENAME" >> "$MISSING_LOG"
        echo "❌ Failed processing $TIFF_FILE. See $ERROR_LOG for details."
        return
    fi

    # Verify output exists
    if ! find "$OUTPUT_DIR" -type f -name "${BASENAME}*" | grep -q .; then
        echo "$BASENAME" >> "$MISSING_LOG"
        echo "❌ No output tiles found for $TIFF_FILE"
        return
    fi

    [[ -f "$ERROR_LOG" ]] && rm -f "$ERROR_LOG"
    echo "$BASENAME" >> "$PROCESSED_LOG"
    echo "✅ Finished processing $TIFF_FILE"
}

export -f process_tiff

# ===============================
# MAIN: Process files from mismatch log using INPUT_DIRS
# ===============================
if [[ ! -f "$LOG_FILE" ]]; then
    echo "❌ Log file not found: $LOG_FILE"
    exit 1
fi

echo "Processing files listed in $LOG_FILE using INPUT_DIRS..."

# Prepare a list of full paths by searching in INPUT_DIRS
FULL_PATHS=()
while read -r FILENAME; do
    FOUND_FILE=""
    for DIR in "${INPUT_DIRS[@]}"; do
        FOUND_FILE=$(find "$DIR" -type f -name "$(basename "$FILENAME")" | head -n 1)
        [[ -n "$FOUND_FILE" ]] && break
    done

    if [[ -n "$FOUND_FILE" ]]; then
        FULL_PATHS+=("$FOUND_FILE")
    else
        echo "⚠️  File not found in INPUT_DIRS: $FILENAME"
        echo "$FILENAME" >> "$MISSING_LOG"
    fi
done < "$LOG_FILE"

# Process all found files in parallel
printf "%s\n" "${FULL_PATHS[@]}" | parallel -P "$PARALLEL_JOBS" process_tiff {}

# ===============================
# SUMMARY
# ===============================
echo ""
echo "=== Processing Summary ==="
TOTAL_FILES=$(wc -l < "$LOG_FILE")
echo "Total files in log: $TOTAL_FILES"
echo "Processed successfully: $(wc -l < "$PROCESSED_LOG")"
echo "Missing / failed: $(wc -l < "$MISSING_LOG")"

cp "$MISSING_LOG" "missing_files.log"
echo "Missing files list saved to missing_files.log"

rm "$PROCESSED_LOG" "$MISSING_LOG"

