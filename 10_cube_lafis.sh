#!/usr/bin/env bash
set -o errexit
set -o pipefail

# ===============================
# CONFIGURATION
# ===============================
INPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/gis/lafis/MH"
OUTPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/masks/lafis"
DOCKER_IMAGE="davidfrantz/force"
RESOLUTION=3
JOBS=2
PARALLEL_JOBS=4  # Number of shapefiles to process in parallel
#YEARS=("2017" "2018" "2019" "2020" "2021" "2022" "2023" "2024" "2025")

# Temporary log files
PROCESSED_LOG=$(mktemp)
SKIPPED_LOG=$(mktemp)
MISSING_LOG=$(mktemp)

mkdir -p "$OUTPUT_DIR"

# ===============================
# FUNCTION: Process single shapefile
# ===============================
process_vector() {
    local SHP_FILE="$1"
    local PROCESSED_LOG="$2"
    local SKIPPED_LOG="$3"
    local MISSING_LOG="$4"

    BASENAME=$(basename "$SHP_FILE" .shp)
    OUT_FILE="$OUTPUT_DIR/${BASENAME}.tif"

    # Skip if output already exists
    if [[ -f "$OUT_FILE" ]]; then
        echo "$BASENAME" >> "$SKIPPED_LOG"
        echo "Skipping $SHP_FILE (already exists)"
        return
    fi

    REL_PATH=$(realpath --relative-to="$(dirname "$SHP_FILE")" "$SHP_FILE")

    echo "Processing shapefile: $SHP_FILE"

    # Run force-cube inside Docker
    docker run --rm --user "$(id -u):$(id -g)" \
        -v "$(dirname "$SHP_FILE")":/data/input \
        -v "$OUTPUT_DIR":/data/output \
        "$DOCKER_IMAGE" bash -c "
            set -e
            INPUT_FILE=\"/data/input/$REL_PATH\"
            echo 'Cubing shapefile: \$INPUT_FILE'

            # Run force-cube (vector input)
            force-cube -r cubic -s $RESOLUTION -n -9999 -t Int16 -j $JOBS -o /data/output \"\$INPUT_FILE\"
        "

    if [[ -f "$OUT_FILE" ]]; then
        echo "$BASENAME" >> "$PROCESSED_LOG"
        echo "Finished processing $SHP_FILE"
    else
        echo "$BASENAME" >> "$MISSING_LOG"
        echo "⚠️ Output not created for $SHP_FILE"
    fi
}

export -f process_vector
export OUTPUT_DIR DOCKER_IMAGE RESOLUTION JOBS

# ===============================
# MAIN LOOP: Iterate through years
# ===============================
#for YEAR in "${YEARS[@]}"; do
#    echo ""
#    echo "==============================="
#    echo "Processing year: $YEAR"
#    echo "==============================="

#    YEAR_DIR="${INPUT_DIR}/${YEAR}"
#    if [[ ! -d "$YEAR_DIR" ]]; then
#        echo "⚠️ Directory not found: $YEAR_DIR"
#        continue
#    fi
#
#    SHP_FILES=$(find "$YEAR_DIR" -type f -name "lafis_grassland_${YEAR}_v4_zones.shp")
#
#    if [[ -z "$SHP_FILES" ]]; then
#        echo "⚠️ No shapefiles found in $YEAR_DIR"
#        continue
#    fi
#
#    echo "$SHP_FILES" | tr '\n' '\0' | xargs -0 -n 1 -P "$PARALLEL_JOBS" \
#        bash -c 'process_vector "$0" "'"$PROCESSED_LOG"'" "'"$SKIPPED_LOG"'" "'"$MISSING_LOG"'"'
#done


# ===============================
# MAIN LOOP: Iterate over all shapefiles (no years)
# ===============================
SHP_FILES=$(find "$INPUT_DIR" -type f -name "MH_lafis_grassland_*.shp")


if [[ -z "$SHP_FILES" ]]; then
    echo "⚠️ No shapefiles found in $INPUT_DIR"
else
    echo "$SHP_FILES" | tr '\n' '\0' | xargs -0 -n 1 -P "$PARALLEL_JOBS" \
        bash -c 'process_vector "$0" "'"$PROCESSED_LOG"'" "'"$SKIPPED_LOG"'" "'"$MISSING_LOG"'"'
fi

# ===============================
# SUMMARY
# ===============================
echo ""
echo "=== Processing Summary ==="
echo "Processed: $(wc -l < "$PROCESSED_LOG")"
echo "Skipped:   $(wc -l < "$SKIPPED_LOG")"
echo "Missing:   $(wc -l < "$MISSING_LOG")"
echo ""

# Clean up temp logs
rm "$PROCESSED_LOG" "$SKIPPED_LOG" "$MISSING_LOG"

echo "✅ All shapefiles processed successfully!"

