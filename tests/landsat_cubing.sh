#!/usr/bin/env bash
set -o errexit
set -o pipefail

# ===============================
# CONFIGURATION
# ===============================
INPUT_DIR="/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level2/X0000_Y0004"
OUTPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/PS_level2_3m"
DOCKER_IMAGE="davidfrantz/force"
RESOLUTION=3
JOBS=2
PARALLEL_JOBS=4  # Number of TIFFs to process in parallel
YEARS=("2017"  "2018" "2019" "2020" "2021" "2022" "2023" "2024" "2025" )  # Add more years if needed

mkdir -p "$OUTPUT_DIR"

# ===============================
# TOTAL IMAGES COUNT
# ===============================
TOTAL_IMAGES=$(find "$INPUT_DIR" -type f -name '*_LND0?_BOA.tif' | wc -l)
export TOTAL_IMAGES

# ===============================
# LOG FILES
# ===============================
PROCESSED_LOG=$(mktemp)
SKIPPED_LOG=$(mktemp)
MISSING_LOG=$(mktemp)

# Create error logs directory
ERROR_LOG_DIR="error_logs"
mkdir -p "$ERROR_LOG_DIR"

# Export logs so GNU parallel can access them
export PROCESSED_LOG SKIPPED_LOG MISSING_LOG ERROR_LOG_DIR

# ===============================
# FUNCTION: Process single TIFF with bands 1,2,3,7
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
    if find "$OUTPUT_DIR" -type f -name "${BASENAME}*" | grep -q .; then
        echo "$BASENAME" >> "$SKIPPED_LOG"
        echo "Skipping $TIFF_FILE (output already exists)"
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
            gdal_translate -b 1 -b 2 -b 3 -b 4 \"\$INPUT_FILE\" \"\$TEMP_FILE\"
            force-cube -r near -s ${RESOLUTION} -n -9999 -t Int16 -j ${JOBS} -o /data/output \"\$TEMP_FILE\"
            rm -f \"\$TEMP_FILE\"
        " >"$ERROR_LOG" 2>&1; then
        echo "$BASENAME" >> "$MISSING_LOG"
        echo "Failed processing $TIFF_FILE. See $ERROR_LOG for details."
        return
    fi

    # Verify output exists
    if ! find "$OUTPUT_DIR" -type f -name "${BASENAME}*" | grep -q .; then
        echo "$BASENAME" >> "$MISSING_LOG"
        echo "No output tiles found for $TIFF_FILE"
        return
    fi

    # Cleanup
    if [ -f "$ERROR_LOG" ]; then
        rm -f "$ERROR_LOG"
    fi

    echo "$BASENAME" >> "$PROCESSED_LOG"
    echo "Finished processing $TIFF_FILE"
}

export -f process_tiff

# ===============================
# OPTIONAL SINGLE FILE MODE
# ===============================
if [[ $# -eq 1 ]]; then
    SINGLE_FILE="$1"
    echo "Running in single-file mode for: $SINGLE_FILE"
    export SINGLE_FILE

    FILE_PATH=$(find "$INPUT_DIR" -type f -name "$SINGLE_FILE" | head -n 1)
    if [[ -z "$FILE_PATH" ]]; then
        echo "File not found in $INPUT_DIR: $SINGLE_FILE"
        exit 1
    fi

    process_tiff "$FILE_PATH" "$OUTPUT_DIR" "$DOCKER_IMAGE" "$RESOLUTION" "$JOBS"
    echo "Single-file processing complete: $SINGLE_FILE"
    exit 0
fi

# ===============================
# MAIN: Process all Sentinel TIFFs
# ===============================
YEAR_PATTERN=$(printf "%s|" "${YEARS[@]}")
YEAR_PATTERN=${YEAR_PATTERN::-1}  # remove trailing "|"

echo "Filtering input files for years: ${YEARS[*]}"
echo "Using regex pattern: $YEAR_PATTERN"

find "$INPUT_DIR" -type f -name '*_LND0?_BOA.tif' | \
    grep -E "/(${YEAR_PATTERN})" | \
    parallel -P "$PARALLEL_JOBS" process_tiff {} "$OUTPUT_DIR" "$DOCKER_IMAGE" "$RESOLUTION" "$JOBS"


# ===============================
# SUMMARY
# ===============================
echo ""
echo "=== Processing Summary ==="
echo "Total input images found: $TOTAL_IMAGES"
echo "Processed: $(wc -l < "$PROCESSED_LOG")"
echo "Skipped:   $(wc -l < "$SKIPPED_LOG")"
echo "Missing:   $(wc -l < "$MISSING_LOG")"

# Save missing files log
cp "$MISSING_LOG" "missing_files.log"
echo "Missing files list saved to missing_files.log"

# Clean up temporary logs
rm "$PROCESSED_LOG" "$SKIPPED_LOG" "$MISSING_LOG"
