#!/bin/bash

# ===============================
# Compute NDVI (supports 4-band and 8-band images)
# ===============================

INPUT_DIRS=(
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/mosaic/MH/PLANET_MOSAIC_4BANDS_PERIOD/final"
)

BASE_OUTPUT="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/mosaics"

PROCESSED_LOG="/tmp/processed.log.$$"
SKIPPED_LOG="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/skipped.log"
MISSING_LOG="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/missing.log"

> "$PROCESSED_LOG"
> "$SKIPPED_LOG"
> "$MISSING_LOG"

process_file() {
    local file="$1"
    local dir="$2"
    local outdir="$3"

    filename=$(basename "$file")
    name="${filename%.tif}"
    output="${outdir}/${name}_NDV.tif"
    
    echo "DEBUG: file=$file"
    echo "DEBUG: output=$output"

    if [[ -f "$output" ]]; then
        echo "$filename" >> "$SKIPPED_LOG"
        return
    fi
    
    gdal_calc.py \
        -A "$file" --A_band=3 \
        -B "$file" --B_band=4 \
        --outfile="$output" \
        --calc="numpy.where((A<=0) | (B<=0) | (A+B==0), -9999, ((B-A)/(B+A))*10000)" \
        --NoDataValue=-9999 \
        --type=Int16 \
        --co COMPRESS=LZW --co PREDICTOR=2 --co TILED=YES \
        --overwrite \
        && echo "$filename" >> "$PROCESSED_LOG" \
        || echo "$filename (calc failed)" >> "$MISSING_LOG"
}

export -f process_file
export PROCESSED_LOG SKIPPED_LOG MISSING_LOG

for INPUT_DIR in "${INPUT_DIRS[@]}"; do

    OUTPUT_DIR="${BASE_OUTPUT}/data"

    mkdir -p "$OUTPUT_DIR"

    find "$INPUT_DIR" -type f \
    	-name '*_DATA.tif' \
    	| parallel -j 20 process_file {} "$INPUT_DIR" "$OUTPUT_DIR"
    
done

echo ""
echo "=== NDVI Processing Summary ==="
echo "Processed: $(wc -l < "$PROCESSED_LOG")"
echo "Skipped:   $(wc -l < "$SKIPPED_LOG")"
echo "Missing:   $(wc -l < "$MISSING_LOG")"
