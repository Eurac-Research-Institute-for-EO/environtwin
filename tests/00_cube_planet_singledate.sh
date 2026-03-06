#!/usr/bin/env bash
set -o errexit
set -o pipefail

# ===============================
# CONFIGURATION
# ===============================
BASE_ROOT="/mnt/CEPH_BASEDATA/PLANET/SATELLITE/Malser_Heide_L2"
OUTPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/P_level2"
DOCKER_IMAGE="davidfrantz/force"
RESOLUTION=3
JOBS=2
TMP_DIR="/tmp/planet_batches"
PARALLEL_JOBS=5
YEARS=("2017" "2018" "2019" "2020" "2021" "2022" "2023" "2024" "2025")
MISSING_FILE_LIST="/mnt/CEPH_PROJECTS/Environtwin/FORCE/missing_ids.txt"

# Temporary log files
PROCESSED_LOG=$(mktemp)
SKIPPED_LOG=$(mktemp)
MISSING_LOG=$(mktemp)

mkdir -p "$TMP_DIR"
mkdir -p "$OUTPUT_DIR"

# ===============================
# FUNCTION: Process single TIFF
# ===============================
process_tiff() {
    local TIFF_FILE="$1"
    local PROCESSED_LOG="$2"
    local SKIPPED_LOG="$3"
    local MISSING_LOG="$4"

    BASENAME=$(basename "$TIFF_FILE" .tif)

    if [[ -f "$OUTPUT_DIR/${BASENAME}.tif" ]]; then
        echo "$BASENAME" >> "$SKIPPED_LOG"
        echo "Skipping $TIFF_FILE (already exists)"
        return
    fi

    docker run --rm --user "$(id -u):$(id -g)" \
        -v "$(dirname "$TIFF_FILE")":/data/input \
        -v "$OUTPUT_DIR":/data/output \
        -v "/mnt/CEPH_PROJECTS/Environtwin/FORCE/scripts":/scripts \
        "$DOCKER_IMAGE" bash -c "
            set -e
            INPUT_FILE=\"/data/input/$(basename "$TIFF_FILE")\"
            echo 'Cubing input file: \$INPUT_FILE'
            force-cube -r near -s $RESOLUTION -n -9999 -t Int16 -j $JOBS -o /data/output \"\$INPUT_FILE\"

            OUTPUT_FILE=\$(basename \"\$INPUT_FILE\" .tif)
            if [[ \"\$OUTPUT_FILE\" =~ ([48]b) ]]; then
                BAND=\${BASH_REMATCH[1]}
            else
                BAND=\"\"
            fi

            PARTS=(\$(echo \"\$OUTPUT_FILE\" | tr '_' ' '))
            DATE=\${PARTS[0]}
            TIME=\${PARTS[1]}
            ID=\${PARTS[2]}

            NEW_NAME=\"\${DATE}_\${TIME}_\${ID}_PLANET_\${BAND}.tif\"
            echo \"Renaming output to: \$NEW_NAME\"
            mv /data/output/\$OUTPUT_FILE.tif /data/output/\$NEW_NAME
        "

    echo "$BASENAME" >> "$PROCESSED_LOG"
    echo "Finished processing $TIFF_FILE"
}

export -f process_tiff
export OUTPUT_DIR DOCKER_IMAGE RESOLUTION JOBS

# ===============================
# MAIN LOOP: Process missing files efficiently
# ===============================
> "$TMP_DIR/tiff_to_process.txt"

while read -r BASENAME; do
    FOUND=0
    for YEAR in "${YEARS[@]}"; do
        YEAR_DIR="$BASE_ROOT/$YEAR"
        [[ ! -d "$YEAR_DIR" ]] && continue

        for ZIP_FILE in "$YEAR_DIR"/batch_*.zip; do
            [[ ! -f "$ZIP_FILE" ]] && continue

            # Look inside ZIP for the matching file without full extraction
            FILE_IN_ZIP=$(unzip -Z1 "$ZIP_FILE" | grep -i "${BASENAME}_.*AnalyticMS_SR.*_clip.tif" | head -n1)
            if [[ -n "$FILE_IN_ZIP" ]]; then
                FOUND=1
                EXTRACT_DIR="$TMP_DIR/$(basename "$ZIP_FILE" .zip)"
                mkdir -p "$EXTRACT_DIR"

                # Extract only the needed TIFF
                unzip -oq "$ZIP_FILE" "$FILE_IN_ZIP" -d "$EXTRACT_DIR"
                TIFF_FILE="$EXTRACT_DIR/$FILE_IN_ZIP"

                # Add to the processing list
                printf '%s\0' "$TIFF_FILE" >> "$TMP_DIR/tiff_to_process.txt"
            fi
        done
    done

    if [[ $FOUND -eq 0 ]]; then
        echo "$BASENAME" >> "$MISSING_LOG"
        echo "Could not find TIFF for missing file: $BASENAME"
    fi
done < "$MISSING_FILE_LIST"

# ===============================
# Run processing in parallel safely
# ===============================
if [[ -s "$TMP_DIR/tiff_to_process.txt" ]]; then
    xargs -0 -n 1 -P "$PARALLEL_JOBS" bash -c 'process_tiff "$0" "'"$PROCESSED_LOG"'" "'"$SKIPPED_LOG"'" "'"$MISSING_LOG"'"' < "$TMP_DIR/tiff_to_process.txt"
fi

# ===============================
# SUMMARY
# ===============================
echo ""
echo "=== Processing Summary ==="
echo "Processed: $(wc -l < "$PROCESSED_LOG")"
echo "Skipped:   $(wc -l < "$SKIPPED_LOG")"
echo "Missing:   $(wc -l < "$MISSING_LOG")"

# Cleanup
rm "$PROCESSED_LOG" "$SKIPPED_LOG" "$MISSING_LOG"
rm -rf "$TMP_DIR"

echo "All missing files processed!"


