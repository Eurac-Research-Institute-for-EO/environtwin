#!/bin/bash
INPUT_DIR_IMAGE="/mnt/CEPH_PROJECTS/Environtwin/FORCE/P_level2/X-001_Y-001"
OUTPUT_DIR_MASK="/mnt/CEPH_PROJECTS/Environtwin/FORCE/P_masks/X-001_Y-001"

mkdir -p "$OUTPUT_DIR_MASK"

# Temporary log files for counters
PROCESSED_LOG=$(mktemp)
SKIPPED_LOG=$(mktemp)
MISSING_LOG=$(mktemp)

create_mask() {
   local file="$1"
    filename=$(basename "$file")
    base="${filename%_AnalyticMS_SR_8b_harmonized_clip.tif}"
    if [[ "$base" == "$filename" ]]; then
        base="${filename%_AnalyticMS_SR_harmonized_clip.tif}"
    fi
    
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
            --type=Byte --NoDataValue=-9999 --overwrite \
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

# Run in parallel for both 8b and 4b files
find "$INPUT_DIR_IMAGE" -name '*_AnalyticMS_SR_8b_harmonized_clip.tif' -o -name '*_AnalyticMS_SR_harmonized_clip.tif' | parallel -j 20 create_mask {}


# Summarize
echo ""
echo "=== Mask Creation Summary ==="
echo "Processed: $(wc -l < "$PROCESSED_LOG")"
echo "Skipped:   $(wc -l < "$SKIPPED_LOG")"
echo "Missing:   $(wc -l < "$MISSING_LOG")"

# Clean up temp files
rm "$PROCESSED_LOG" "$SKIPPED_LOG" "$MISSING_LOG"
