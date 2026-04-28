#!/bin/bash
# ===============================================
# Purpose: Build annual NDVI stacks from PLANET files
#          and export them as ENVI BSQ files with rich headers
# ===============================================


INPUT_DIRS=(
	"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/mosaics/data"
	)

BASE_OUTPUT="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/mosaics"


# ===============================================
# MAIN MULTI-FOLDER LOOP
# ===============================================
for INPUT_DIR in "${INPUT_DIRS[@]}"; do

    echo "=============================================="
    echo "📂 Processing PLANET NDVI in tile: $INPUT_DIR"
    echo "=============================================="
    
    # Extract the site name from directory 
    tile=$(basename "$(dirname "$INPUT_DIR")")
    echo "Processing tile: $tile"
    
    OUTPUT_DIR="${BASE_OUTPUT}"

    # Collect all Planet NDVI files
    mapfile -t FILES_ALL < <(find "$INPUT_DIR" -type f -name "*_DATA_NDV.tif")

    # Extract unique years
    YEARS=$(basename -a "${FILES_ALL[@]}" | grep -oE '20[0-9]{2}' | sort -u)

    if [ -z "$YEARS" ]; then
        echo "❌ No valid years found in filenames in $INPUT_DIR"
        continue
    fi

    # ===========================================
    # PER-YEAR PROCESSING
    # ===========================================
    for year in $YEARS; do
        echo "------------ Processing year $year in $INPUT_DIR -------------"

        # Find all files for this year
mapfile -t FILES < <(find "$INPUT_DIR" -type f -iname "PLANET_MOSAIC_${year}*_DATA_NDV.tif" | sort -V)


        if [ ${#FILES[@]} -eq 0 ]; then
            echo "⚠️ No PLANET files for $year, skipping..."
            continue
        fi

        first_file=$(basename "${FILES[0]}")
        last_file=$(basename "${FILES[-1]}")

        first_date=$(echo "$first_file" | grep -oE '[0-9]{8}' | head -n1)
        last_date=$(echo "$last_file" | grep -oE '[0-9]{8}' | tail -n1)

        first_doy=$(date -d "$first_date" +%j)
        last_doy=$(date -d "$last_date" +%j)

        # Metadata text file per year
        OUTPUT_TXT="$OUTPUT_DIR/dates_${year}_PLA.txt"
        > "$OUTPUT_TXT"

        for f in "${FILES[@]}"; do
            fname=$(basename "$f")
            date=$(echo "$fname" | grep -oE '[0-9]{8}' | head -n1)
            doy=$(date -d "$date" +%j)
            end_date=$(echo "$fname" | grep -oE '[0-9]{8}' | tail -n1)
            end_doy=$(date -d "$end_date" +%j)
            echo "${date}_${doy}_PLA" >> "$OUTPUT_TXT"
        done

        # Create VRT + BSQ
        TEMP_VRT="$OUTPUT_DIR/temp_${year}.vrt"
        OUTPUT_BSQ="$OUTPUT_DIR/${first_date}-${last_date}_${first_doy}-${last_doy}_TSA_PLA_NDV_TSS.bsq"

        gdalbuildvrt -separate -vrtnodata -9999 "$TEMP_VRT" "${FILES[@]}"
        gdal_translate -of ENVI -ot Int16 -a_nodata -9999 "$TEMP_VRT" "$OUTPUT_BSQ"

        HDR_FILE="${OUTPUT_BSQ%.bsq}.hdr"

        # Build ENVI header
        BAND_NAMES=()
        WAVELENGTHS=()

        while read -r line; do
            DATE=$(echo "$line" | cut -d'_' -f1)
            DOY=$(echo "$line" | cut -d'_' -f2)
            SENSOR=$(echo "$line" | cut -d'_' -f3)
            BAND_NAMES+=("${DATE}_${SENSOR}")
            WAVELENGTHS+=("${DOY}")
        done < "$OUTPUT_TXT"

        # Update or insert header fields
        if [ -f "$HDR_FILE" ]; then
            echo "🧩 Updating ENVI header: $HDR_FILE"

            BAND_BLOCK="band names = {\n"
            count=0
            for b in "${BAND_NAMES[@]}"; do
                BAND_BLOCK+=" $b,"
                ((count++))
                (( count % 8 == 0 )) && BAND_BLOCK+="\n"
            done
            BAND_BLOCK="${BAND_BLOCK%,}\n}"

            WAVE_BLOCK="wavelength = {\n"
            count=0
            for w in "${WAVELENGTHS[@]}"; do
                WAVE_BLOCK+=" $w,"
                ((count++))
                (( count % 8 == 0 )) && WAVE_BLOCK+="\n"
            done
            WAVE_BLOCK="${WAVE_BLOCK%,}\n}"

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

            {
                echo ""
                echo -e "$BAND_BLOCK"
                echo -e "$WAVE_BLOCK"
            } >> "${HDR_FILE}.tmp"

            mv "${HDR_FILE}.tmp" "$HDR_FILE"
            echo "✅ Header updated: $HDR_FILE"
        else
            echo "⚠️ No header found for $OUTPUT_BSQ — skipping header update."
        fi

        echo "✅ Completed year $year"
        echo "   → Output: $OUTPUT_BSQ"

        rm -f "$TEMP_VRT"

    done  # end per-year

done  # end tile loop

