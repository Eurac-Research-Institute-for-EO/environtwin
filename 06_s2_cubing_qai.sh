#!/usr/bin/env bash
set -o errexit
set -o pipefail

# ===============================
# CONFIGURATION
# ===============================
INPUT_DIRS=(
    "/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level2/X0003_Y0004"
    "/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level2/X0000_Y0003"
    "/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level2/X0002_Y0005"
    "/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level2/X0004_Y0004"
    "/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level2/X0002_Y0003"
    "/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level2/X0000_Y0004"
)
OUTPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/missing"
DOCKER_IMAGE="davidfrantz/force"
RESOLUTION=3
JOBS=2
PARALLEL_JOBS=4  # Number of TIFFs to process in parallel

mkdir -p "$OUTPUT_DIR"

# ===============================
# TOTAL IMAGES COUNT
# ===============================
TOTAL_IMAGES=0
for DIR in "${INPUT_DIRS[@]}"; do
    COUNT=$(find "$DIR" -type f -name '*_SEN2?_QAI.tif' | wc -l)
    TOTAL_IMAGES=$((TOTAL_IMAGES + COUNT))
done
export TOTAL_IMAGES

# ===============================
# LOG FILES
# ===============================
PROCESSED_LOG=$(mktemp)
#SKIPPED_LOG=$(mktemp)
MISSING_LOG=$(mktemp)

# Create error logs directory
ERROR_LOG_DIR="error_logs"
mkdir -p "$ERROR_LOG_DIR"

# Export logs so GNU parallel can access them
export PROCESSED_LOG MISSING_LOG ERROR_LOG_DIR
#SKIPPED_LOG

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

    # Skip if output exists (recursively checks output folder)
   # if find "$OUTPUT_DIR" -type f -name "${BASENAME}*" | grep -q .; then
    #    echo "$BASENAME" >> "$SKIPPED_LOG"
     #   echo "Skipping $TIFF_FILE (output already exists)"
      #  return
   # fi

    local MOUNT_DIR
    MOUNT_DIR=$(dirname "$TIFF_FILE")
    local REL_PATH
    REL_PATH=$(basename "$TIFF_FILE")

    # Run Docker and save logs to error log file (stdout+stderr)
    if ! docker run --rm --user "$(id -u):$(id -g)" \
        -v "${MOUNT_DIR}":/data/input \
        -v "${OUTPUT_DIR}":/data/output \
        "${DOCKER_IMAGE}" bash -c "
            set -e
            INPUT_FILE=\"/data/input/${REL_PATH}\"
            echo \"Cubing input file (all bands): \$INPUT_FILE\"
            force-cube -r near -s ${RESOLUTION} -n -9999 -t Int16 -j ${JOBS} -o /data/output \"\$INPUT_FILE\"
            rm -f \"\$TEMP_FILE\"
        " >"$ERROR_LOG" 2>&1; then
        echo "$BASENAME" >> "$MISSING_LOG"
        echo "Failed processing $TIFF_FILE. See $ERROR_LOG for details."
        return
    fi

    # Verify output tiles exist after processing
    if ! find "$OUTPUT_DIR" -type f -name "${BASENAME}*" | grep -q .; then
        echo "$BASENAME" >> "$MISSING_LOG"
        echo "No output tiles found for $TIFF_FILE"
        return
    fi

    # Remove error log if successful
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

    # Search the file in all input directories
    FILE_PATH=""
    for DIR in "${INPUT_DIRS[@]}"; do
        FILE_PATH=$(find "$DIR" -type f -name "$SINGLE_FILE" | head -n 1)
        [[ -n "$FILE_PATH" ]] && break
    done

    if [[ -z "$FILE_PATH" ]]; then
        echo "❌ File not found in any input directories: $SINGLE_FILE"
        exit 1
    fi

    process_tiff "$FILE_PATH" "$OUTPUT_DIR" "$DOCKER_IMAGE" "$RESOLUTION" "$JOBS"
    echo "✅ Single-file processing complete: $SINGLE_FILE"
    exit 0
fi


# ===============================
# MAIN: Process all Sentinel TIFFs from all folders
# ===============================
#for DIR in "${INPUT_DIRS[@]}"; do
 #   find "$DIR" -type f -name '*_SEN2?_QAI.tif' | \
  #      parallel -P "$PARALLEL_JOBS" process_tiff {} "$OUTPUT_DIR" "$DOCKER_IMAGE" "$RESOLUTION" "$JOBS"
#done

for DIR in "${INPUT_DIRS[@]}"; do
    find "$DIR" -type f \
        -name '*2025*' \
        -name '*_SEN2?_QAI.tif' | \
        parallel -P "$PARALLEL_JOBS" process_tiff {} "$OUTPUT_DIR" "$DOCKER_IMAGE" "$RESOLUTION" "$JOBS"
done
# ===============================
# SUMMARY
# ===============================
echo ""
echo "=== Processing Summary ==="
echo "Total input images found: $TOTAL_IMAGES"
echo "Processed: $(wc -l < "$PROCESSED_LOG")"
#echo "Skipped:   $(wc -l < "$SKIPPED_LOG")"
echo "Missing:   $(wc -l < "$MISSING_LOG")"

# Export missing files log to permanent file
cp "$MISSING_LOG" "missing_files.log"
echo "Missing files list saved to missing_files.log"

# Clean up logs
rm "$PROCESSED_LOG" "$MISSING_LOG"
#"$SKIPPED_LOG"

echo "All done!"

