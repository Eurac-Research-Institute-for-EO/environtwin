#!/bin/bash
# ================================
# Purpose: Build annual NDVI stacks from SENTINEL tiff tss
# and export them as ENVI BSQ files
# ================================

INPUT_DIRS=(
	"/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level3/indices/environtwin/X0000_Y0003"
	"/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level3/indices/environtwin/X0000_Y0004"
	"/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level3/indices/environtwin/X0002_Y0003"
	"/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level3/indices/environtwin/X0002_Y0005"
	"/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level3/indices/environtwin/X0003_Y0004"
	"/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level3/indices/environtwin/X0004_Y0004"
)

BASE_OUTPUT="/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level3/indices/environtwin"

# ======================================================
# Loop through each input folder (tile)
# ======================================================
for INPUT_DIR in "${INPUT_DIRS[@]}"; do

    TILE=$(basename "$INPUT_DIR")
    OUTPUT_DIR="${BASE_OUTPUT}/${TILE}"
    mkdir -p "$OUTPUT_DIR"

    echo "======================================="
    echo "📂 Processing tile: $TILE"
    echo "   → folder: $INPUT_DIR"
    echo "======================================="

    # ------------------------------------------------------
    # Collect all NDV_TSS.tif files in the folder
    # ------------------------------------------------------
    FILES_ALL=($(find "$INPUT_DIR" -maxdepth 1 -type f -name "*NDV_TSS.tif" | sort -V))

    if [ ${#FILES_ALL[@]} -eq 0 ]; then
        echo "⚠️ No NDV_TSS.tif files found in $INPUT_DIR, skipping..."
        continue
    fi

    # ------------------------------------------------------
    # Extract unique years from filenames
    # ------------------------------------------------------
    YEARS=$(basename -a "${FILES_ALL[@]}" | grep -oE '^[0-9]{4}' | sort -u)

    # ------------------------------------------------------
    # Loop through each year
    # ------------------------------------------------------
    for YEAR in $YEARS; do
        echo "------------ Processing year $YEAR in $INPUT_DIR -------------"

        # Select files for this year
        FILES=($(find "$INPUT_DIR" -maxdepth 1 -type f -name "${YEAR}*_NDV_TSS.tif" | sort -V))
        if [ ${#FILES[@]} -eq 0 ]; then
            echo "⚠️ No NDV_TSS.tif files found for $YEAR, skipping..."
            continue
        fi

        TSS_FILE="${FILES[0]}"  # assume one file per year

        # ------------------------------------------------------
        # Extract band names from Description
        # ------------------------------------------------------
        mapfile -t BANDNAMES < <(
            gdalinfo "$TSS_FILE" | grep "Description =" | sed 's/.*= //'
        )

        if [ ${#BANDNAMES[@]} -eq 0 ]; then
            echo "❌ Could not read band names for $TSS_FILE, skipping..."
            continue
        fi

        # ------------------------------------------------------
        # Create metadata dates_YEAR_SEN.txt
        # ------------------------------------------------------
        DATE_FILE="${OUTPUT_DIR}/dates_${YEAR}_SEN.txt"
        > "$DATE_FILE"
        DOY_LIST=()

        for BN in "${BANDNAMES[@]}"; do
            DATE=$(echo "$BN" | cut -d'_' -f1)
            SENSOR=$(echo "$BN" | cut -d'_' -f2)
            DOY=$(date -d "$DATE" +%j)
            echo "${DATE}_${DOY}_${SENSOR}" >> "$DATE_FILE"
            DOY_LIST+=("$DOY")
        done

        FIRST_DATE=$(head -n1 "$DATE_FILE" | cut -d'_' -f1)
        LAST_DATE=$(tail -n1 "$DATE_FILE" | cut -d'_' -f1)
        FIRST_DOY=$(head -n1 "$DATE_FILE" | cut -d'_' -f2)
        LAST_DOY=$(tail -n1 "$DATE_FILE" | cut -d'_' -f2)

        # ------------------------------------------------------
        # Convert TIFF → ENVI BSQ
        # ------------------------------------------------------
        OUT_BSQ="${OUTPUT_DIR}/${FIRST_DATE}-${LAST_DATE}_${FIRST_DOY}-${LAST_DOY}_TSA_SEN_NDV_TSS.bsq"

        gdal_translate -of ENVI -ot Int16 -a_nodata -9999 "$TSS_FILE" "$OUT_BSQ"

        HDR="${OUT_BSQ%.bsq}.hdr"
        echo "📝 Updating header: $HDR"

        # ------------------------------------------------------
        # Build band names and wavelength blocks
        # ------------------------------------------------------
        BAND_BLOCK="band names = {\n"
        count=0
        for BN in "${BANDNAMES[@]}"; do
            BAND_BLOCK+=" $BN,"
            ((count++))
            ((count % 8 == 0)) && BAND_BLOCK+="\n"
        done
        BAND_BLOCK="${BAND_BLOCK%,}\n}"

        WAVE_BLOCK="wavelength = {\n"
        count=0
        for DOY in "${DOY_LIST[@]}"; do
            WAVE_BLOCK+=" $DOY,"
            ((count++))
            ((count % 8 == 0)) && WAVE_BLOCK+="\n"
        done
        WAVE_BLOCK="${WAVE_BLOCK%,}\n}"

        # ------------------------------------------------------
        # Remove old blocks and insert new ones
        # ------------------------------------------------------
        awk '
            BEGIN {in_band=0; in_wave=0}
            /^band names =/ {in_band=1; next}
            /^wavelength =/ {in_wave=1; next}
            /^\}/ {
                if (in_band) {in_band=0; next}
                if (in_wave) {in_wave=0; next}
            }
            {if(!in_band && !in_wave) print}
        ' "$HDR" > "${HDR}.tmp"

        {
            echo -e "$BAND_BLOCK"
            echo -e "$WAVE_BLOCK"
        } >> "${HDR}.tmp"

        mv "${HDR}.tmp" "$HDR"

        echo "✅ Finished year $YEAR for tile $TILE"
        echo "   → BSQ: $(basename "$OUT_BSQ")"
        echo "   → dates: $(basename "$DATE_FILE")"

    done
done
