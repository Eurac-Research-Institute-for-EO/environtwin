#!/bin/bash
# ================================
# Purpose: Build annual NDVI stacks from SENTINEL files
# and export them as ENVI BSQ files
# ================================

INPUT_ROOT="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/SEN2"
BASE_OUTPUT="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/SEN2"

# ================================
# LOOP THROUGH ALL data FOLDERS
# ================================
for SEN2_DIR in "$INPUT_ROOT"/*/data; do

    # skip if not existing
    [ -d "$SEN2_DIR" ] || continue

    echo "=============================================="
    echo "📂 Processing folder: $SEN2_DIR"
    echo "=============================================="

       # extract tile name (parent of "data")
    tile=$(basename "$(dirname "$SEN2_DIR")")
    echo "Processing tile: $tile"

    OUTPUT_DIR="${BASE_OUTPUT}/${tile}"
    mkdir -p "$OUTPUT_DIR"

    # Collect all NDVI files
    mapfile -t FILES_ALL < <(
        find "$SEN2_DIR" -maxdepth 1 -type f \
        -name "*_SEN2*_site.tif"
    )

    if [ ${#FILES_ALL[@]} -eq 0 ]; then
        echo "❌ No NDVI files found in $SEN2_DIR"
        continue
    fi


    # Extract unique years from filenames
    YEARS=$(basename -a "${FILES_ALL[@]}" | grep -oE '^[0-9]{4}' | sort -u)

 YEARS=$(printf "%s\n" "${FILES_ALL[@]}" \
        | xargs -n1 basename \
        | grep -oE '^[0-9]{4}' \
        | sort -u)

    for year in $YEARS; do
        echo "------------ Processing year $year -------------"

        mapfile -t FILES < <(
            find "$SEN2_DIR" -maxdepth 1 -type f \
            -name "${year}*_SEN2*_site.tif" \
            | awk -F'/' '{
                fname=$NF;
                if (match(fname,/^[0-9]{8}/)) {
                    date=substr(fname,RSTART,RLENGTH);
                    print date" "$0
                }
            }' \
            | sort -k1,1 \
            | cut -d' ' -f2-
        )

        # Filter files: only March (03) to November (11)
        FILTERED_FILES=()
        for f in "${FILES[@]}"; do
            fname=$(basename "$f")
            date=$(echo "$fname" | grep -oE '^[0-9]{8}')
            month=${date:4:2}
            if ((10#$month >= 3 && 10#$month <= 11)); then
                FILTERED_FILES+=("$f")
            fi
        done
        FILES=("${FILTERED_FILES[@]}")

        # Skip if no files left
        if [ ${#FILES[@]} -eq 0 ]; then
            echo "⚠️ No files for $year in months 03–11, skipping..."
            continue
        fi

        # Determine start and end dates for the stack
        FIRST_DATE=$(basename "${FILES[0]}" | grep -oE '^[0-9]{8}')
        LAST_DATE=$(basename "${FILES[@]: -1}" | grep -oE '^[0-9]{8}')
        START_DOY=$(date -d "$FIRST_DATE" +%j)
        END_DOY=$(date -d "$LAST_DATE" +%j)
        BASE_NAME="${year}${START_DOY}-${year}${END_DOY}_TSA_SEN_NDV_TSS"

        OUTPUT_TXT="$OUTPUT_DIR/dates_${year}_SEN.txt"
        > "$OUTPUT_TXT"

        # Build metadata list: date, DOY, and sensor type
        for f in "${FILES[@]}"; do
            fname=$(basename "$f")
            date=$(echo "$fname" | grep -oE '^[0-9]{8}')
            sensor=$(echo "$fname" | grep -oE 'SEN2A|SEN2B|SEN2C' || echo "UNKNOWN")
            doy=$(date -d "$date" +%j)
            echo "${date}_${doy}_${sensor}" >> "$OUTPUT_TXT"
        done

        # Build a VRT and convert to ENVI BSQ
        TEMP_VRT="$OUTPUT_DIR/temp_${year}.vrt"
        gdalbuildvrt -separate "$TEMP_VRT" "${FILES[@]}"

        OUTPUT_BSQ="$OUTPUT_DIR/${FIRST_DATE}-${LAST_DATE}_${START_DOY}-${END_DOY}_TSA_SEN_NDV_TSS.bsq"
        gdal_translate -of ENVI -ot Int16 -a_nodata -9999 "$TEMP_VRT" "$OUTPUT_BSQ"

        HDR_FILE="${OUTPUT_BSQ%.bsq}.hdr"

        # --- Rebuild "band names" and "wavelength" lists from the metadata file ---
        BAND_NAMES=()
        WAVELENGTHS=()

        while read -r line; do
            DATE=$(echo "$line" | cut -d'_' -f1)
            DOY=$(echo "$line" | cut -d'_' -f2)
            SENSOR=$(echo "$line" | cut -d'_' -f3)
            BAND_NAMES+=("${DATE}_${SENSOR}")
            WAVELENGTHS+=("${DOY}")
        done < "$OUTPUT_TXT"

        # Join arrays into comma-separated strings
        BAND_NAMES_STR=$(printf " %s," "${BAND_NAMES[@]}")
        BAND_NAMES_STR="${BAND_NAMES_STR%,}"  # remove trailing comma
        WAVELENGTHS_STR=$(printf " %s," "${WAVELENGTHS[@]}")
        WAVELENGTHS_STR="${WAVELENGTHS_STR%,}"

        # --- Overwrite or append the "band names" and "wavelength" sections in the header ---
        if [ -f "$HDR_FILE" ]; then
            echo "🧩 Updating band names and wavelengths in header: $HDR_FILE"

            BAND_BLOCK="band names = {\n"
            count=0
            for b in "${BAND_NAMES[@]}"; do
                BAND_BLOCK+=" $b,"
                ((count++))
                if (( count % 8 == 0 )); then
                    BAND_BLOCK+="\n"
                fi
            done
            BAND_BLOCK="${BAND_BLOCK%,}\n}"

            WAVE_BLOCK="wavelength = {\n"
            count=0
            for w in "${WAVELENGTHS[@]}"; do
                WAVE_BLOCK+=" $w,"
                ((count++))
                if (( count % 8 == 0 )); then
                    WAVE_BLOCK+="\n"
                fi
            done
            WAVE_BLOCK="${WAVE_BLOCK%,}\n}"

            #cp "$HDR_FILE" "${HDR_FILE}.bak"

            # Remove old band/wavelength blocks
            awk '
                BEGIN {in_band=0; in_wave=0}
                /^band names =/ {in_band=1; next}
                /^wavelength =/ {in_wave=1; next}
                /^\}/ {
                    if (in_band) {in_band=0; next}
                    if (in_wave) {in_wave=0; next}
                }
                {if(!in_band && !in_wave) print}
            ' "$HDR_FILE" > "${HDR_FILE}.tmp"

            # Append new blocks
            {
                echo -e "$BAND_BLOCK"
                echo -e "$WAVE_BLOCK"
            } >> "${HDR_FILE}.tmp"

            mv "${HDR_FILE}.tmp" "$HDR_FILE"
            echo "✅ Updated header: $HDR_FILE"
        else
            echo "⚠️ Header file not found for $OUTPUT_BSQ — skipping header update."
        fi

        echo "✅ Year $year stack created:"
        echo "  BSQ: $OUTPUT_BSQ"
        rm -f "$TEMP_VRT"
    done
done

