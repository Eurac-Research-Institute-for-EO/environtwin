



#!/usr/bin/env bash
set -o errexit
set -o pipefail

# === CONFIGURATION ===
BASE_ROOT_BASE="/mnt/CEPH_PROJECTS/Environtwin/PLANET/MalserHeide"
OUTPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level1"
DOCKER_IMAGE="davidfrantz/force"
RESOLUTION=3
JOBS=2
SKIP_DUPLICATES=1  # 1 to skip duplicates, 0 to process all

# Specify years to process, can be adjusted
YEARS=("2024")

for YEAR in "${YEARS[@]}"; do
    BASE_ROOT="$BASE_ROOT_BASE/$YEAR"
    echo "Processing year: $YEAR, directory: $BASE_ROOT"

    if [[ ! -d "$BASE_ROOT" ]]; then
        echo "Directory for year $YEAR does not exist: $BASE_ROOT"
        continue
    fi

    # Iterate over all batch_* folders inside the current year's base directory
    for BATCH_DIR in "$BASE_ROOT"/batch_*; do
        if [[ -d "$BATCH_DIR" ]]; then
            echo "Processing batch directory: $BATCH_DIR"
            declare -A SEEN_BASENAMES

            # Extract unique 8-digit date codes from matching files in this batch
            DATE_CODES=$(find "$BATCH_DIR/files" -type f -name '*_AnalyticMS_SR_8b_clip.tif' | \
                sed -n 's|.*/\([0-9]\{8\}\)_[0-9].*|\1|p' | sort -u)

            for DATE_CODE in $DATE_CODES; do
                echo "Processing date code: $DATE_CODE"

                # Find all files starting with the date code and matching pattern
                find "$BATCH_DIR/files" -type f -name "${DATE_CODE}_*_AnalyticMS_SR_8b_clip.tif" | while read -r TIFF_FILE; do
                    BASENAME=$(basename "$TIFF_FILE" .tif)
                    echo "Processing file: $TIFF_FILE"

                    if [[ $SKIP_DUPLICATES -eq 1 && -n "${SEEN_BASENAMES[$BASENAME]}" ]]; then
                        echo "Skipping duplicate $BASENAME"
                        continue
                    fi
                    SEEN_BASENAMES[$BASENAME]=1

                    if [[ -f "$OUTPUT_DIR/${BASENAME}.tif" ]]; then
                        echo "Skipping $TIFF_FILE (output already exists)"
                        continue
                    fi

                    REL_PATH=$(realpath --relative-to="$BATCH_DIR" "$TIFF_FILE")

                    docker run --rm --user "$(id -u):$(id -g)" \
                        -v "$BATCH_DIR":/data/input \
                        -v "$OUTPUT_DIR":/data/output \
                        "$DOCKER_IMAGE" bash -c "
                            set -e
                            INPUT_FILE=\"/data/input/$REL_PATH\"
                            echo 'Cubing input file...'
                            force-cube -r near -s $RESOLUTION -t Int16 -j $JOBS -o /data/output \"\$INPUT_FILE\"
                        "
                    echo "Finished processing $TIFF_FILE"
                    echo "--------------------------------"
                done
            done
        fi
    done
done

echo "All processing complete!"

