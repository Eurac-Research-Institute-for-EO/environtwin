#!/bin/bash
INPUT_DIR_IMAGE="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level1/X0000_Y0004"
OUTPUT_DIR_MASK="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2/X0000_Y0004"
mkdir -p "$OUTPUT_DIR_MASK"

# Temporary log files for counters
PROCESSED_LOG=$(mktemp)
SKIPPED_LOG=$(mktemp)
MISSING_LOG=$(mktemp)

create_mask() {
    local file="$1"
    base=$(basename "$file" _AnalyticMS_SR_8b_clip.tif)
    udm="${INPUT_DIR_IMAGE}/${base}_udm2_clip.tif"
    output_mask="${OUTPUT_DIR_MASK}/${base}_mask.tif"

    if [[ -f "$udm" ]]; then
        if [[ -f "$output_mask" && "$output_mask" -nt "$udm" ]]; then
            echo "$base" >> "$SKIPPED_LOG"
            echo "Skipping mask for $base (up to date)"
            return
        fi

        echo "$base" >> "$PROCESSED_LOG"
        echo "Creating mask for $base"
        gdal_calc.py \
            -A "$udm" --A_band=2 \
            -B "$udm" --B_band=3 \
            -C "$udm" --C_band=4 \
            -D "$udm" --D_band=5 \
            -E "$udm" --E_band=6 \
            --outfile="$output_mask" \
            --calc='(((A==1) | (B==1) | (C==1) | (D==1) | (E==1)) * 1)' \
            --type=Byte --NoDataValue=255 --overwrite \
            --co COMPRESS=LZW --co PREDICTOR=2 --co TILED=YES
    else
        echo "$base" >> "$MISSING_LOG"
        echo "Missing UDM for $base"
    fi
}

export -f create_mask
export INPUT_DIR_IMAGE
export OUTPUT_DIR_MASK
export PROCESSED_LOG SKIPPED_LOG MISSING_LOG

# Run in parallel
find "$INPUT_DIR_IMAGE" -name '*_AnalyticMS_SR_8b_clip.tif' | parallel -j 4 create_mask {}

# Summarize
echo ""
echo "=== Mask Creation Summary ==="
echo "Processed: $(wc -l < "$PROCESSED_LOG")"
echo "Skipped:   $(wc -l < "$SKIPPED_LOG")"
echo "Missing:   $(wc -l < "$MISSING_LOG")"

# Clean up temp files
rm "$PROCESSED_LOG" "$SKIPPED_LOG" "$MISSING_LOG"
