#!/usr/bin/env bash
set -o errexit
set -o pipefail

# ===============================
# CONFIGURATION
# ===============================
BASE_ROOT="/mnt/CEPH_BASEDATA/SATELLITE/PLANET/Malser_Heide"
OUTPUT_DIR_MASK="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2"
REF_IMAGE="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level1/X0000_Y0004/20241030_102958_52_251f_3B_AnalyticMS_SR_8b_clip.tif"
PARALLEL_JOBS=4
YEARS=("2024")  # add more years if needed
TMP_DIR="/tmp/environtwin_processing"
mkdir -p "$OUTPUT_DIR_MASK"
mkdir -p "$TMP_DIR"

# ===============================
# LOG FILES
# ===============================
# Temporary log files for counters
PROCESSED_LOG=$(mktemp)
SKIPPED_LOG=$(mktemp)
MISSING_LOG=$(mktemp)

# ===============================
# READ PROJECTION AND EXTENT FROM REFERENCE IMAGE
# ===============================
# Extract projection string (WKT)
PROJ_STRING=$(gdalinfo "$REF_IMAGE" | grep -A 20 'Coordinate System is:' | head -n 20 | tr '\n' ' ')

# Extract extent coordinates
UL=$(gdalinfo "$REF_IMAGE" | grep "Upper Left")
LR=$(gdalinfo "$REF_IMAGE" | grep "Lower Right")

# Parse coordinates
XMIN=$(echo "$UL" | sed -E 's/.*\(([0-9.\-]+), ([0-9.\-]+)\).*/\1/')
YMAX=$(echo "$UL" | sed -E 's/.*\(([0-9.\-]+), ([0-9.\-]+)\).*/\2/')
XMAX=$(echo "$LR" | sed -E 's/.*\(([0-9.\-]+), ([0-9.\-]+)\).*/\1/')
YMIN=$(echo "$LR" | sed -E 's/.*\(([0-9.\-]+), ([0-9.\-]+)\).*/\2/')

# Extract pixel size (resolution)
PIXEL_SIZE=$(gdalinfo "$REF_IMAGE" | grep "Pixel Size")
XRES=$(echo "$PIXEL_SIZE" | sed -E 's/Pixel Size = \(([0-9.\-]+), ([0-9.\-]+)\)/\1/')
YRES=$(echo "$PIXEL_SIZE" | sed -E 's/Pixel Size = \(([0-9.\-]+), ([0-9.\-]+)\)/\2/')

echo "Reference image info:"
echo "Projection string: $PROJ_STRING"
echo "Extent: XMIN=$XMIN, YMIN=$YMIN, XMAX=$XMAX, YMAX=$YMAX"
echo "Resolution: XRES=$XRES, YRES=$YRES"

# ===============================
# MASK CREATION FUNCTION
# ===============================
create_mask_from_udm() {
    local udm_file="$1"
    local output_dir_mask="$2"
    local processed_log="$3"
    local skipped_log="$4"
    local missing_log="$5"
    local basename_udm
    basename_udm=$(basename "$udm_file" _udm2_clip.tif)
    local output_mask="${output_dir_mask}/${basename_udm}_mask.tif"
    local tmp_mask="${output_mask%.tif}_tmp.tif"
    if [[ ! -f "$udm_file" ]]; then
        echo "$basename_udm" >> "$missing_log"
        echo "Missing UDM file for $basename_udm"
        return
    fi
    gdal_calc.py \
         -A "$udm_file" --A_band=2 \
         -B "$udm_file" --B_band=3 \
         -C "$udm_file" --C_band=4 \
         -D "$udm_file" --D_band=5 \
         -E "$udm_file" --E_band=6 \
         --outfile="$tmp_mask" \
         --calc='(((A==1) | (B==1) | (C==1) | (D==1) | (E==1)) * 1)' \
         --type=Byte --NoDataValue=255 --overwrite \
         --co COMPRESS=LZW --co PREDICTOR=2 --co TILED=YES
    gdalwarp -overwrite \
         -r nearest \
         -t_srs "$PROJ_STRING" \
         -te "$XMIN" "$YMIN" "$XMAX" "$YMAX" \
         -tr "$XRES" "$YRES" \
         "$tmp_mask" "$output_mask"
    rm -f "$tmp_mask"
    echo "$basename_udm" >> "$processed_log"
}

export -f create_mask_from_udm
export OUTPUT_DIR_MASK PROCESSED_LOG SKIPPED_LOG MISSING_LOG PROJ_STRING XMIN YMIN XMAX YMAX XRES YRES

# ===============================
# LOOP OVER BATCH FOLDERS
# ===============================
for YEAR in "${YEARS[@]}"; do
    YEAR_DIR="$BASE_ROOT/$YEAR"
    echo "Processing year: $YEAR"
    if [[ ! -d "$YEAR_DIR" ]]; then
        echo "Year directory does not exist: $YEAR_DIR"
        continue
    fi
    # Loop over all batch ZIPs
    shopt -s nullglob
    ZIP_FILES=("$YEAR_DIR"/batch_*.zip)
    if [[ ${#ZIP_FILES[@]} -eq 0 ]]; then
        echo "No batch ZIP files found in $YEAR_DIR"
        continue
    fi
    for ZIP_FILE in "${ZIP_FILES[@]}"; do
        echo "Processing batch archive: $ZIP_FILE"
        EXTRACT_DIR="$TMP_DIR/$(basename "$ZIP_FILE" .zip)"
        rm -rf "$EXTRACT_DIR"
        mkdir -p "$EXTRACT_DIR"
        echo "Extracting $ZIP_FILE to $EXTRACT_DIR"
        unzip -oq "$ZIP_FILE" -d "$EXTRACT_DIR"
        
        # Find UDM files, process only first occurrence per unique basename
        declare -A seen_files=()
        while IFS= read -r -d '' udm_file; do
            filename=$(basename "$udm_file")
            if [[ -z "${seen_files[$filename]:-}" ]]; then
                seen_files[$filename]=1
                create_mask_from_udm "$udm_file" "$OUTPUT_DIR_MASK" "$PROCESSED_LOG" "$SKIPPED_LOG" "$MISSING_LOG" &
            else
                echo "Skipping duplicate UDM file: $udm_file"
                echo "$filename" >> "$SKIPPED_LOG"
            fi
        done < <(find "$EXTRACT_DIR/files" -type f -name '*_udm2_clip.tif' -print0)
        wait  # Wait for all background mask creation jobs to finish
        # Clean up extraction folder
        rm -rf "$EXTRACT_DIR"
        echo "Cleaned up extracted batch: $EXTRACT_DIR"
    done
done

# ===============================
# SUMMARY
# ===============================
echo ""
echo "=== Mask Creation Summary ==="
echo "Processed: $(wc -l < "$PROCESSED_LOG")"
echo "Skipped:   $(wc -l < "$SKIPPED_LOG")"
echo "Missing:   $(wc -l < "$MISSING_LOG")"
rm "$PROCESSED_LOG" "$SKIPPED_LOG" "$MISSING_LOG"
echo "All masks created and resampled!"



