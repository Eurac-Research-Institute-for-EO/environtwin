#!/bin/bash

# ===============================
# Compute NDVI (supports 4-band and 8-band images)
# Mask areas where band 1 of improved mask is 0
# ===============================


INPUT_DIRS=(
    #"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0004_Y0002"
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0006_Y0000"
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0006_Y0001"
    #"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0007_Y0000"
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0008_Y0000"
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0008_Y-001"
    #"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0007_Y-001"
    #"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0009_Y-001"
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X-001_Y-001"
    #"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X-002_Y-002"
)

BASE_OUTPUT="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/02"

PROCESSED_LOG="/tmp/processed.log.$$"
SKIPPED_LOG="/tmp/skipped.log.$$"
MISSING_LOG="/tmp/missing.log.$$"

> "$PROCESSED_LOG"
> "$SKIPPED_LOG"
> "$MISSING_LOG"

process_file() {
    local file="$1"
    local dir="$2"
    local outdir="$3"

    filename=$(basename "$file")
    base="${filename%_PLANET_BOA.tif}"

    mask="${dir}/${base}_PLANET_udm2_mask.tif"
    output="${outdir}/${base}_PLA_masked_blue_NDV.tif"

    mkdir -p "$outdir"

    if [[ -f "$output" ]]; then
        echo "$base" >> "$SKIPPED_LOG"
        return
    fi

    if [[ ! -f "$mask" ]]; then
        echo "$base" >> "$MISSING_LOG"
        return
    fi

    gdal_calc.py \
        -A "$file" --A_band=3 \
        -B "$file" --B_band=4 \
        -C "$file" --C_band=1 \
        -M "$mask" --M_band=1 \
        --outfile="$output" \
        --calc="numpy.where((M==0) | (C>900) | (A==-9999) | (B==-9999) | (A+B==0), -9999, ((B-A)/(B+A))*10000)" \
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
    tile=$(basename "$INPUT_DIR")           # e.g. X0004_Y0002
    OUTPUT_DIR="${BASE_OUTPUT}/${tile}"     # e.g. 02_mowing/X0004_Y0002

    echo "Processing tile $tile"

    #find "$INPUT_DIR" -type f \
    #	-name '*_PLANET_BOA.tif' \
    #	-name '*2025*' \
     #   | parallel -j 20 process_file {} "$INPUT_DIR" "$OUTPUT_DIR"
        
    find "$INPUT_DIR" -type f \
    -regex '.*/20251[01].*_PLANET_BOA\.tif$' \
    | parallel -j 20 process_file {} "$INPUT_DIR" "$OUTPUT_DIR"
done

echo ""
echo "=== NDVI Processing Summary ==="
echo "Processed: $(wc -l < "$PROCESSED_LOG")"
echo "Skipped:   $(wc -l < "$SKIPPED_LOG")"
echo "Missing:   $(wc -l < "$MISSING_LOG")"
