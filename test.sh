#!/usr/bin/env bash

# ===============================
# CONFIGURATION
# ===============================
PLANET_ROOT="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw"
BASE_OUTPUT="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw"

# ===============================
# Process only files with high haze
# ===============================
for TILE_DIR in "$PLANET_ROOT"/*; do

    [ -d "$TILE_DIR" ] || continue

    INPUT_DIR="${TILE_DIR}/coregistered"
    [ -d "$INPUT_DIR" ] || continue
    
    UDM_INPUT="${TILE_DIR}/standard"

    tile=$(basename "$TILE_DIR")
    txt_file="${TILE_DIR}/Planet_status_info.csv"

    [ -f "$txt_file" ] || { echo "❌ Missing CSV for $tile"; continue; }

    echo "=============================================="
    echo "📂 Processing tile: $tile"
    echo "📂 Folder: $INPUT_DIR"
    echo "=============================================="

    OUTPUT_DIR="${BASE_OUTPUT}/${tile}/haze"
    mkdir -p "$OUTPUT_DIR"

    # ===============================================
    # COLLECT FILES (filtered by haze)
    # ===============================================
    mapfile -t FILES_ALL < <(
    find "$INPUT_DIR" -maxdepth 1 -type f -name "*_PLANET_BOA.bsq" \
    | awk -v csv="$txt_file" '
    BEGIN {
        FS=",";
        first=1;

        while ((getline line < csv) > 0) {

            if (first) {
                split(line, header, ",");
                for (i in header) {
                    if (header[i] ~ /id/) id_col = i;
                    if (header[i] ~ /haze_light/) hl_col = i;
                    if (header[i] ~ /haze_heavy/) hh_col = i;
                }
                first=0;
                continue;
            }

            split(line, row, ",");

            for (i in row) gsub(/"/, "", row[i]);

            if (row[hl_col] > 45 || row[hh_col] > 45) {
                split(row[id_col], parts, "_");
                key = parts[1] "_" parts[2];
                ids[key] = 1;
            }
        }
        close(csv);
    }

    {
        fname = $0;
        sub(/^.*\//, "", fname);
        split(fname, parts, "_");
        key = parts[1] "_" parts[2];

        if (key in ids) {
            print $0;
        }
    }'
    )

    if [ ${#FILES_ALL[@]} -eq 0 ]; then
        echo "❌ No haze-filtered files found"
        continue
    fi

    echo "✅ Haze-filtered scenes: ${#FILES_ALL[@]}"
    
    # find corresponding udms
    mapfile -t FILES_UDM_ALL < <(
printf "%s\n" "${FILES_ALL[@]}" \
| while read -r boa; do
    fname=$(basename "$boa")

    # extract key: YYYYMMDD_HHMMSS
    key=$(echo "$fname" | cut -d'_' -f1-2)

    # find matching UDM file
    udm_file=$(find "$UDM_INPUT" -maxdepth 1 -type f -name "${key}*_PLANET_udm2_buffer.tif" | head -n1)

    if [ -n "$udm_file" ]; then
        echo "$udm_file"
    else
        echo "⚠️ Missing UDM for $key" >&2
    fi
done
)

    # ===============================================
    # EXTRACT YEARS
    # ===============================================
    YEARS=$(printf "%s\n" "${FILES_ALL[@]}" \
        | xargs -n1 basename \
        | grep -oE '^[0-9]{4}' \
        | sort -u)

    [ -z "$YEARS" ] && { echo "❌ No valid years found"; continue; }

    echo "=============================="
    echo "Extracted years:"
    echo "$YEARS"
    echo "=============================="

    # ===============================================
    # PROCESS PER YEAR
    # ===============================================
    while read -r year; do

        [ -z "$year" ] && continue

        echo "------------ Processing year $year -------------"

       # BOA files
mapfile -t FILES < <(
    printf "%s\n" "${FILES_ALL[@]}" \
    | awk -v year="$year" -F'/' '{
        fname=$NF
        if (fname ~ "^"year) {
            if (match(fname,/^[0-9]{8}/)) {
                date=substr(fname,RSTART,RLENGTH)
                print date" "$0
            }
        }
    }' | sort -k1,1 | cut -d' ' -f2-
)

# MATCHING UDM files (same order!)
mapfile -t FILES_UDM < <(
    printf "%s\n" "${FILES_UDM_ALL[@]}" \
    | awk -v year="$year" -F'/' '{
        fname=$NF
        if (fname ~ "^"year) {
            if (match(fname,/^[0-9]{8}/)) {
                date=substr(fname,RSTART,RLENGTH)
                print date" "$0
            }
        }
    }' | sort -k1,1 | cut -d' ' -f2-
)

        if [ ${#FILES[@]} -eq 0 ]; then
            echo "⚠️ No files for year $year"
            continue
        fi

        echo "Found ${#FILES[@]} files for $year"

        first_file=$(basename "${FILES[0]}")
        last_file=$(basename "${FILES[${#FILES[@]}-1]}")

        first_date=$(echo "$first_file" | grep -oE '^[0-9]{8}')
        last_date=$(echo "$last_file" | grep -oE '^[0-9]{8}')

        if ! first_doy=$(date -d "$first_date" +%j); then
            echo "❌ Failed to compute DOY for $first_date"
            continue
        fi

        if ! last_doy=$(date -d "$last_date" +%j); then
            echo "❌ Failed to compute DOY for $last_date"
            continue
        fi

        OUTPUT_TXT="$OUTPUT_DIR/dates_${year}_PLA.txt"
        > "$OUTPUT_TXT"

        for f in "${FILES[@]}"; do
            fname=$(basename "$f")
            date=$(echo "$fname" | grep -oE '^[0-9]{8}')
            doy=$(date -d "$date" +%j)
            echo "${date}_${doy}_PLA" >> "$OUTPUT_TXT"
        done

        TEMP_VRT="$OUTPUT_DIR/temp_${year}.vrt"
        OUTPUT_BSQ="$OUTPUT_DIR/${first_date}-${last_date}_${first_doy}-${last_doy}_TSA_PLA_HAZE_TSS.bsq"

        echo "---------------------------------"
        echo "Year: $year"
        echo "Scenes detected: ${#FILES[@]}"
        echo "Metadata entries: $(wc -l < "$OUTPUT_TXT")"
        echo "---------------------------------"

        if ! gdalbuildvrt -separate -b 1 -vrtnodata -9999 "$TEMP_VRT" "${FILES[@]}"; then
            echo "❌ gdalbuildvrt failed for year $year"
            continue
        fi

        if ! gdal_translate -of ENVI -ot Int16 -a_nodata -9999 "$TEMP_VRT" "$OUTPUT_BSQ"; then
            echo "❌ gdal_translate failed for year $year"
            continue
        fi

        HDR_FILE="${OUTPUT_BSQ%.bsq}.hdr"
        
        TEMP_VRT_UDM="$OUTPUT_DIR/temp_${year}_udm.vrt"
        OUTPUT_UDM="$OUTPUT_DIR/${first_date}-${last_date}_UDM_HAZE_TSS.bsq"
        
        gdalbuildvrt -separate -b 4 -b 5 "$TEMP_VRT_UDM" "${FILES_UDM[@]}"
        gdal_translate -of ENVI -ot Byte "$TEMP_VRT_UDM" "$OUTPUT_UDM"
        
        # write out clear layer
        TEMP_VRT_CLEAR="$OUTPUT_DIR/temp_${year}_udm_clear.vrt"
        OUTPUT_UDM_CLEAR="$OUTPUT_DIR/${first_date}-${last_date}_UDM_CLEAR_TSS.bsq"
        
        gdalbuildvrt -separate -b 11 "$TEMP_VRT_CLEAR" "${FILES_UDM[@]}"
        gdal_translate -of ENVI -ot Byte "$TEMP_VRT_CLEAR" "$OUTPUT_UDM_CLEAR"
        
        # write out json file as well
        cp "$JSON_FILE" "${DEST}/${base}_metadata.json"


        # ===============================================
        # UPDATE ENVI HEADER
        # ===============================================
        if [ -f "$HDR_FILE" ]; then
            echo "🧩 Updating ENVI header"

            BAND_BLOCK="band names = {\n"
            WAVE_BLOCK="wavelength = {\n"

            count=0
            while read -r line; do
                DATE=$(echo "$line" | cut -d'_' -f1)
                DOY=$(echo "$line" | cut -d'_' -f2)

                BAND_BLOCK+=" ${DATE}_PLA,"
                WAVE_BLOCK+=" ${DOY},"

                ((count++))
                (( count % 8 == 0 )) && {
                    BAND_BLOCK+="\n"
                    WAVE_BLOCK+="\n"
                }
            done < "$OUTPUT_TXT"

            BAND_BLOCK="${BAND_BLOCK%,}\n}"
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
            echo "✅ Header updated"
        else
            echo "⚠️ No header found"
        fi

        rm -f "$TEMP_VRT"
        rm -f "$TEMP_VRT_CLEAR"
        rm -f "$TEMP_VRT_UDM"

        echo "✅ Completed year $year"
        echo "→ Output: $OUTPUT_BSQ"

    done <<< "$YEARS"

done

