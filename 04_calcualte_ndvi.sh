#!/bin/bash

# ===============================
# Compute NDVI (supports 4-band and 8-band images)
# ===============================

INPUT_DIRS=(
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_daily/03/MH"
)

BASE_OUTPUT="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/03"

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
    
    mkdir -p "$outdir"

    filename=$(basename "$file")
    base="${filename%%_PLANET_BOA.tif}"

    #mask="${dir}/${base}_PLANET_udm2_mask.tif"
    output="${outdir}/${base}_PLA_masked_NDV.tif"

    if [[ -f "$output" ]]; then
        echo "$base" >> "$SKIPPED_LOG"
        return
    fi

    
    # Compute NDVI with binary mask
    gdal_calc.py \
        -A "$file" --A_band=3 \
        -B "$file" --B_band=4 \
        --outfile="$output" \
        --calc="numpy.where((A==-9999) | (B==-9999) | (A+B==0), -9999, ((B-A)/(B+A))*10000)" \
        --NoDataValue=-9999 \
        --type=Int16 \
        --co COMPRESS=LZW --co PREDICTOR=2 --co TILED=YES \
        --overwrite

    echo "$base" >> "$PROCESSED_LOG"
}

export -f process_file
export PROCESSED_LOG SKIPPED_LOG MISSING_LOG

# Loop over each input directory
for INPUT_DIR in "${INPUT_DIRS[@]}"; do
    tile=$(basename "$INPUT_DIR")           
    OUTPUT_DIR="${BASE_OUTPUT}/${tile}/data"     

    echo "Processing tile $tile"

    # when using -name to find files use pattern "*" only
    find "$INPUT_DIR" -type f \
    	-name '*_PLANET_BOA.tif' \
    	| parallel -j 20 process_file {} "$INPUT_DIR" "$OUTPUT_DIR"
    
    # when using -regex you have to use .*    
    #find "$INPUT_DIR" -type f \
    #-regex '.*/20251[01].*_PLANET_.*_BOA\.tif$' \
    #| parallel -j 20 process_file {} "$INPUT_DIR" "$OUTPUT_DIR"
done

echo ""
echo "=== NDVI Processing Summary ==="
echo "Processed: $(wc -l < "$PROCESSED_LOG")"
echo "Skipped:   $(wc -l < "$SKIPPED_LOG")"
echo "Missing:   $(wc -l < "$MISSING_LOG")"
