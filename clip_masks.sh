#!/bin/bash
# ==========================================
# Purpose: Clip GDD TIFFs to mask extent
# ==========================================

INPUT=(
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/masks/gdd/X-001_Y-001"
)

BASE_OUTPUT="/mnt/CEPH_PROJECTS/Environtwin/FORCE/masks/gdd/MH"
MASK_FILE="/mnt/CEPH_PROJECTS/Environtwin/FORCE/masks/sites/MH/MH_mask.tif"

mkdir -p "$BASE_OUTPUT"

# ------------------------------------------
# Compute mask extent ONCE (bulletproof)
# ------------------------------------------
read MINX MINY MAXX MAXY < <(
    gdalinfo -json "$MASK_FILE" | \
    jq -r '.cornerCoordinates | "\(.lowerLeft[0]) \(.lowerLeft[1]) \(.upperRight[0]) \(.upperRight[1])"'
)

echo "======================================"
echo "Using mask extent (meters, safe):"
echo "$MINX $MINY $MAXX $MAXY"
echo "======================================"

export MINX MINY MAXX MAXY BASE_OUTPUT

# ------------------------------------------
# FUNCTION
# ------------------------------------------
process_tiff() {
    local TIFF_FILE="$1"

    file_name=$(basename "$TIFF_FILE")
    OUTPUT_FILE="${BASE_OUTPUT}/${file_name%.tif}.tif"

    echo "Processing $file_name -> $OUTPUT_FILE"

    gdalwarp -overwrite \
        -te "$MINX" "$MINY" "$MAXX" "$MAXY" \
        "$TIFF_FILE" "$OUTPUT_FILE"

    echo "Finished $file_name"
}

export -f process_tiff

# ------------------------------------------
# MAIN
# ------------------------------------------
for DIR in "${INPUT[@]}"; do
    find "$DIR" -type f -name '*GDD*.tif' | \
        parallel -P 4 process_tiff {}
done
