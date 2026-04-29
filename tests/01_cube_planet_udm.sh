#!/usr/bin/env bash
set -o errexit
set -o pipefail

# ===============================
# CONFIGURATION
# ===============================
#BASE_ROOT="/mnt/CEPH_PROJECTS/Environtwin/PLANET/MalserHeide"
BASE_ROOT="/mnt/CEPH_PROJECTS/Environtwin/PLANET/Missing"
OUTPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/missing"
DOCKER_IMAGE="davidfrantz/force"
RESOLUTION=3
NODATA=-9999
JOBS=6
TMP_DIR="/tmp/planet_batches"
PARALLEL_JOBS=8  # Number of TIFFs to process in parallel
YEARS=("2017"  "2018" "2019" "2020" "2021" "2022" "2023" "2024" "2025" ) # Add more years if needed

# Temporary log files for counters
PROCESSED_LOG=$(mktemp)
SKIPPED_LOG=$(mktemp)
MISSING_LOG=$(mktemp)

mkdir -p "$TMP_DIR"

# ===============================
# FUNCTION TO PROCESS A SINGLE TIFF
# ===============================
process_udm() {
    local TIFF_FILE="$1"
    local PROCESSED_LOG="$2"
    local SKIPPED_LOG="$3"
    local MISSING_LOG="$4"
    local BASENAME
    
    BASENAME=$(basename "$TIFF_FILE" .tif)

    # Skip if output exists
    if [[ -f "$OUTPUT_DIR/${BASENAME}.tif" ]]; then
        echo "$BASENAME" >> "$SKIPPED_LOG"
        echo "Skipping $TIFF_FILE (already exists)"
        return
    fi

    REL_PATH=$(realpath --relative-to="$(dirname "$TIFF_FILE")" "$TIFF_FILE")

    docker run --rm --user "$(id -u):$(id -g)" \
        -v "$(dirname "$TIFF_FILE")":/data/input \
        -v "$OUTPUT_DIR":/data/output \
        "$DOCKER_IMAGE" bash -c "
            set -e
            INPUT_FILE=\"/data/input/$REL_PATH\"
            BN=\$(basename \"\$INPUT_FILE\" .tif)
            VRT=\"/tmp/\${BN}.vrt\"

            echo 'Building VRT with NoData=$NODATA ...'
            gdal_translate -of VRT -ot Int16 -a_nodata $NODATA \"\$INPUT_FILE\" \"\$VRT\"

            echo 'Cubing VRT ...'
            force-cube -r near -s $RESOLUTION -t Int16 -n $NODATA -j $JOBS -o /data/output \"\$VRT\"
            
            
            # --- Rename to PLANET convention ---
            # Remove _3B if present and keep date_time_id_udm2.tif
            NEW_NAME=\$(echo \"\$BN\" | sed 's/_3B_/_/')
            echo \"Renaming output to: \$NEW_NAME\"
            mv /data/output/\$BN.tif /data/output/\$NEW_NAME

            rm -f \"\$VRT\"
        "

    echo "$BASENAME" >> "$PROCESSED_LOG"
    echo "Finished processing $TIFF_FILE"
}

export -f process_udm
export OUTPUT_DIR DOCKER_IMAGE RESOLUTION JOBS NODATA PROCESSED_LOG SKIPPED_LOG MISSING_LOG

# ===============================
# MAIN LOOP: Years and Batch Directories
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

        # Find all matching TIFFs
        TIFF_LIST=$(find "$EXTRACT_DIR/files" -type f -name '*_udm2_clip.tif')
        if [[ -z "$TIFF_LIST" ]]; then
            echo "No UDM TIFFs found in $EXTRACT_DIR"
            continue
        fi
        
        # Run processing in parallel
        echo "$TIFF_LIST" | tr '\n' '\0' | xargs -0 -n 1 -P "$PARALLEL_JOBS" \
            bash -c 'process_udm "$0" "'"$PROCESSED_LOG"'" "'"$SKIPPED_LOG"'" "'"$MISSING_LOG"'"'
        
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

echo "All processing complete!"

