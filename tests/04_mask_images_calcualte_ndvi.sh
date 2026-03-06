#!/bin/bash

# ===============================
# Apply binary masks to 8-band images and compute NDVI
# ===============================

INPUT_DIR_IMAGE="/mnt/CEPH_PROJECTS/Environtwin/FORCE/P_level2/X-001_Y-001"
INPUT_DIR_MASK="/mnt/CEPH_PROJECTS/Environtwin/FORCE/P_mask/X-001_Y-001"
OUTPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/P_level3/X-001_Y-001"

mkdir -p "$OUTPUT_DIR"

process_file() {
   local file="$1"

    # Extract base filename
    filename=$(basename "$file")
    base="${filename%_AnalyticMS_SR_8b_harmonized_clip.tif}"
    if [[ "$base" == "$filename" ]]; then
        base="${filename%_AnalyticMS_SR_harmonized_clip.tif}"
    fi

    mask="${INPUT_DIR_MASK}/${base}_mask.tif"
    
    # Detect number of bands
    nbands=$(gdalinfo -json "$file" | jq '.bands | length')
    if [[ "$nbands" -eq 8 ]]; then
        red_band=6
        nir_band=8
    elif [[ "$nbands" -eq 4 ]]; then
        red_band=3
        nir_band=4
    else
        echo "Skipping $base: unsupported number of bands ($nbands)"
        return
    fi

    echo "Processing $base (bands: Red=$red_band, NIR=$nir_band)"
    
    if [[ -f "$mask" ]]; then
        echo "Processing $base"

        gdal_calc.py \
          -A "$file" \
	  -B "$mask" \
	  --allBands=A \
	  --outfile="${OUTPUT_DIR}/${base}_masked.tif" \
	  --calc="A*(B==0) + (-9999)*(B!=0)" \
	  --NoDataValue=-9999 \
	  --type=Int16 \
	  --co COMPRESS=LZW --co PREDICTOR=2 --co TILED=YES \
	  --overwrite

        # Compute NDVI
        gdal_calc.py \
          -A "$file" --A_band=$nir_band \
          -B "$file" --B_band=$red_band \
          --outfile="${OUTPUT_DIR}/${base}_harm_NDV.tif" \
          --calc="(((A-B)/(A+B))*10000)" \
          --NoDataValue=-9999 \
          --type=Int16 \
          --co COMPRESS=LZW --co PREDICTOR=2 --co TILED=YES \
          --overwrite
    else
        echo "Missing Mask for $base"
    fi
}
export -f process_file
export INPUT_DIR_MASK
export OUTPUT_DIR

# Run in parallel for both 8b and 4b files
find "$INPUT_DIR_IMAGE" \( -name '*_AnalyticMS_SR_8b_harmonized_clip.tif' -o -name '*_AnalyticMS_SR_harmonized_clip.tif' \) -print \
    | parallel -j 20 process_file {}



