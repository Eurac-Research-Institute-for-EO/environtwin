#!/bin/bash
# ===============================================
# Purpose: Build annual NDVI stacks from PLANET files
#          and export them as ENVI BSQ files with rich headers
# ===============================================

PLANET_ROOT="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/03"
BASE_OUTPUT="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/03"

# ===============================================
# LOOP THROUGH ALL PLANET TILES
# ===============================================
for TILE_DIR in "$PLANET_ROOT"/*; do

    [ -d "$TILE_DIR" ] || continue

    INPUT_DIR="${TILE_DIR}/data"
    [ -d "$INPUT_DIR" ] || continue

    tile=$(basename "$TILE_DIR")

    echo "=============================================="
    echo "📂 Processing PLANET NDVI in tile: $tile"
    echo "📂 Folder: $INPUT_DIR"
    echo "=============================================="

    OUTPUT_DIR="${BASE_OUTPUT}/${tile}"
    mkdir -p "$OUTPUT_DIR"

    # ===============================================
    # COLLECT FILES
    # ===============================================
    mapfile -t FILES_ALL < <(
        find "$INPUT_DIR" -maxdepth 1 -type f \
        -name "*_PLA_masked_NDV.tif"
    )

    if [ ${#FILES_ALL[@]} -eq 0 ]; then
        echo "❌ No PLANET NDVI files found"
        echo "➡️ Skipping this tile"
        continue
    fi

    # ===============================================
    # EXTRACT YEARS
    # ===============================================
    YEARS=$(printf "%s\n" "${FILES_ALL[@]}" \
        | xargs -n1 basename \
        | grep -oE '^[0-9]{4}' \
        | sort -u)

    if [ -z "$YEARS" ]; then
        echo "❌ No valid years found in filenames"
        continue
    fi

    
    # ===============================================
    # PROCESS PER YEAR
    # ===============================================
    for year in $YEARS; do
        echo "------------ Processing year $year -------------"

        mapfile -t FILES < <(
            find "$INPUT_DIR" -maxdepth 1 -type f \
            -name "${year}*_PLA_masked_NDV.tif" \
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

        echo "Found ${#FILES[@]} files for $year"

        first_file=$(basename "${FILES[0]}")
        last_file=$(basename "${FILES[-1]}")

        first_date=$(echo "$first_file" | grep -oE '^[0-9]{8}')
        last_date=$(echo "$last_file" | grep -oE '^[0-9]{8}')

        first_doy=$(date -d "$first_date" +%j)
        last_doy=$(date -d "$last_date" +%j)

        # Metadata text file per year
        OUTPUT_TXT="$OUTPUT_DIR/dates_${year}_PLA.txt"
        > "$OUTPUT_TXT"

        for f in "${FILES[@]}"; do
            fname=$(basename "$f")
            date=$(echo "$fname" | grep -oE '^[0-9]{8}')
            doy=$(date -d "$date" +%j)
            echo "${date}_${doy}_PLA" >> "$OUTPUT_TXT"
        done

        # Create VRT + BSQ
        TEMP_VRT="$OUTPUT_DIR/temp_${year}.vrt"
        OUTPUT_BSQ="$OUTPUT_DIR/${first_date}-${last_date}_${first_doy}-${last_doy}_TSA_PLA_NDV_TSS.bsq"
        
        echo "---------------------------------"
	echo "Year: $year"
	echo "Scenes detected: ${#FILES[@]}"
	echo "Metadata entries: $(wc -l < "$OUTPUT_TXT")"
	echo "---------------------------------"

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

