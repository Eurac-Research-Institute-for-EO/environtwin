#!/usr/bin/env bash
set -o errexit
set -o pipefail

# ===============================
# CONFIGURATION
# ===============================
INPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2/X-001_Y-001"
OUTPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2"
DOCKER_IMAGE="davidfrantz/force"
RESOLUTION=3
JOBS=2
PARALLEL_JOBS=4  # Number of TIFFs to process in parallel
MISSING_LOG_FILE="missing_files.log"

mkdir -p "$OUTPUT_DIR"
ERROR_LOG_DIR="error_logs"
mkdir -p "$ERROR_LOG_DIR"

# ===============================
# FUNCTION: Process a single Planet TIFF
# ===============================
process_tiff() {
    local TIFF_FILE="$1"
    local BASENAME
    BASENAME=$(basename "$TIFF_FILE" .tif)

    # ----------------------------------------------------------
    # OLD VERSION (for 4-band inputs)
    # local OUTPUT_BASENAME=${BASENAME/_PLANET_4b/_PLANET}
    # local INPUT_SUFFIX="_PLANET_4b.tif"
    # ----------------------------------------------------------

    # NEW VERSION (for UDM2 inputs)
    # If output files KEEP the "_udm2" suffix:
    local OUTPUT_BASENAME="$BASENAME"
    # If output files DROP "_udm2" (e.g. become *_PLANET.tif):
    # local OUTPUT_BASENAME=${BASENAME/_PLANET_udm2/_PLANET}

    local OUTPUT_FILE="${OUTPUT_DIR}/${OUTPUT_BASENAME}.tif"
    local ERROR_LOG="${ERROR_LOG_DIR}/${BASENAME}.log"

    echo "Processing $TIFF_FILE..."

    local MOUNT_DIR
    MOUNT_DIR=$(dirname "$TIFF_FILE")
    local REL_PATH
    REL_PATH=$(basename "$TIFF_FILE")

    docker run --rm --user "$(id -u):$(id -g)" \
        -v "${MOUNT_DIR}":/data/input \
        -v "${OUTPUT_DIR}":/data/output \
        "${DOCKER_IMAGE}" bash -c "
            set -e
            INPUT_FILE=\"/data/input/${REL_PATH}\"
            echo \"Cubing input file: \$INPUT_FILE\"
            force-cube -r near -s ${RESOLUTION} -n -9999 -t Int16 -j ${JOBS} -o /data/output \"\$INPUT_FILE\"
        " >"$ERROR_LOG" 2>&1

    echo "✅ Finished $TIFF_FILE"
}

export -f process_tiff
export OUTPUT_DIR DOCKER_IMAGE RESOLUTION JOBS ERROR_LOG_DIR

# ===============================
# Build list of input TIFFs from log
# ===============================
temp_input_list=$(mktemp)

while IFS= read -r file_id; do
    # ----------------------------------------------------------
    # OLD VERSION (4-band input)
    # tiff_path="${INPUT_DIR}/${file_id}_PLANET_4b.tif"
    # ----------------------------------------------------------

    # NEW VERSION (UDM2 input)
    tiff_path="${INPUT_DIR}/${file_id}_PLANET_udm2.tif"

    if [[ -f "$tiff_path" ]]; then
        echo "$tiff_path"
    else
        echo "⚠️  Missing TIFF file: $tiff_path" >&2
    fi
done < "$MISSING_LOG_FILE" > "$temp_input_list"

# ===============================
# Process all files in parallel
# ===============================
cat "$temp_input_list" | parallel -P "$PARALLEL_JOBS" process_tiff {}

rm "$temp_input_list"

echo ""
echo "=== Done processing all files from $MISSING_LOG_FILE ==="




