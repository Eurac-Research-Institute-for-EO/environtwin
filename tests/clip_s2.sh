#!/bin/bash
# ================================
# Purpose: Clip Sentinel NDVI data to the site extent
# ================================

INPUT_SEN2=(
	"/mnt/CEPH_PROJECTS/Environtwin/GDD/SouthTyrol/gdd"
	#"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/SEN2/X-001_Y-001"
	#"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/SEN2/X-002_Y-002"
	#"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/SEN2/X0009_Y-001"
	#"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/SEN2/X0008_Y-001"
	#"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/SEN2/X0008_Y0000"
	#"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/SEN2/X0007_Y-001"
	#"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/SEN2/X0007_Y0000"
	#"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/SEN2/X0006_Y0001"
	#"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/SEN2/X0006_Y0000"
	#"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/SEN2/X0004_Y0002"
)

BASE_OUTPUT="/mnt/CEPH_PROJECTS/Environtwin/FORCE/masks/GDD/MH"

MASK_FILE=("/mnt/CEPH_PROJECTS/Environtwin/gis/masks/MH_mask.tif")

# ===============================
# FUNCTION: cube tiffs
# ===============================
process_tiff() {
    local TIFF_FILE="$1"
    local BASE_OUTPUT="$2"
    local MASK_FILE="$3"
    
    echo "=============================================="
    echo "📂 Processing Sentinel folder: $TIFF_FILE"
    echo "=============================================="

    # Extract the site name from directory 
    tile=$(basename "$(dirname "$TIFF_FILE")")
    
    echo "Processing tile: $tile"
    
    OUTPUT_DIR="${BASE_OUTPUT}"
    mkdir -p "$OUTPUT_DIR"

    file_name=$(basename "$TIFF_FILE")
    OUTPUT_FILE="${OUTPUT_DIR}/${file_name%.tif}.tif"
    
    echo "Processing $file_name -> $OUTPUT_FILE"
    
    gdalwarp -overwrite -te $(gdalinfo $MASK_FILE | grep "Lower Left" -A 1 | awk '{print $4, $5, $10, $11}') "$TIFF_FILE" "$OUTPUT_FILE"

    echo "Finished processing $file_name"
}

export -f process_tiff

# ===============================
# MAIN: Process all Sentinel TIFFs
# ===============================
for DIR in "${INPUT_SEN2[@]}"; do
    find "$DIR" -type f -name 'GDD*.tif' | \
    parallel -P 4 process_tiff {} "$BASE_OUTPUT" "$MASK_FILE"
done
