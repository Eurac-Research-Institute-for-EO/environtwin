#!/bin/bash
INPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard"
OUTPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/coregistered"
MISSING_IDS="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/coregistered/missing_ids.txt"
CPU_CORES=2

export INPUT_DIR OUTPUT_DIR MISSING_IDS

mkdir -p "$OUTPUT_DIR"

convert_bsq() {
    local file="$1"
    local basefile=$(basename "$file")
    local prefix="${basefile%%_PLANET_*}"
    local output_file="$OUTPUT_DIR/${prefix}_PLANET_BOA.bsq"
    
     if [ ! -f "$output_file" ]; then
        echo "Converting $file → $output_file"
        gdal_translate -of ENVI \
            -ot UInt16 \
            -a_nodata 0 \
            -co "INTERLEAVE=BSQ" \
            "$file" "$output_file"
       
    else
        echo "Skipping $output_file"
    fi
}

export -f convert_bsq

# Only BOA files + parallel + missing IDs filter
find "$INPUT_DIR" -type f -name "*_BOA.tif" -print0 | \
xargs -0 -I {} -P "$CPU_CORES" bash -c '
    basefile=$(basename "$1")
    prefix="${basefile%%_PLANET_*}"
    if grep -Fxq "$prefix" '"$MISSING_IDS"'; then
        convert_bsq "$1"
    fi
' _ {}

echo "✅ All missing BOA files processed!"
