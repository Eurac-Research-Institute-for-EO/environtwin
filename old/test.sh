#!/usr/bin/env bash
set -o errexit
set -o pipefail

BASE_ROOT="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/test"
OUTPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/test"

# Create destination folders
mkdir -p "$OUTPUT_DIR/30/test"
mkdir -p "$OUTPUT_DIR/30/shit"

# Find all TIFFs
mapfile -t TIFF_FILES < <(
    find "$BASE_ROOT" -type f -name '*PLA_*_BOA.tif'
)

if (( ${#TIFF_FILES[@]} == 0 )); then
   echo "No TIFF files found in $BASE_ROOT"
   exit 0
fi

for tif in "${TIFF_FILES[@]}"; do

    filename=$(basename "$tif")
    base="${filename%%_PLA_*}"
    
    echo "Filename $filename"

    # Find JSON
    JSON_FILE=$(find "$BASE_ROOT" -type f -name "${base}_metadata.json")
    echo "Using JSON: $JSON_FILE"


    if [[ -z "$JSON_FILE" ]]; then
        echo "No metadata found for $filename"
        continue
    fi

    # Read quality_control value
    CP=$(jq -r '.properties.clear_percent // empty' "$JSON_FILE")


    # Decide destination by clear_percent
if (( CP > 30 )); then
    DEST="$OUTPUT_DIR/30/test"
elif (( CP < 30 )); then
    DEST="$OUTPUT_DIR/30/shit"
else
    echo "Invalid clear_percent '$CP' for $filename"
    continue
fi


# publishing_stage must be finalized
PS=$(jq -r '.properties.publishing_stage // empty' "$JSON_FILE")

if [[ "$PS" != "finalized" ]]; then
    echo "Skipping $filename (publishing_stage=$PS)"
    continue
fi

out_file="${DEST}/${base}_PLA_${PS}_BOA.tif"
cp "$tif" "$out_file"


    echo "Moved $filename -> $out_file"
done

        





