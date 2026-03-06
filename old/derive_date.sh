#!/bin/bash

FOLDER="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/01_mowing/X-001_Y-001"
OUTPUT_DIR="$FOLDER"

shopt -s nullglob

for file in "$FOLDER"/*PLA_masked_NDV.tif; do
  filename=$(basename "$file")
  
  # Only extract from harmonized files
  if [[ "$filename" =~ PLA_masked_NDV\.tif$ ]]; then
    # Extract YYYYMMDD pattern
    date=$(echo "$filename" | grep -oE '20[0-9]{6}')
    if [[ -n $date ]]; then
      year=${date:0:4}
      
      # Convert YYYYMMDD to DOY
      doy=$(date -d "${date:0:4}-${date:4:2}-${date:6:2}" +%j)
      
      output_file="$OUTPUT_DIR/dates_${year}_PLA.txt"
      echo "${date}_${doy}_PLA" >> "$output_file"
    fi
  fi
done

shopt -u nullglob

echo "Date info (with DOY) extracted into $OUTPUT_DIR/dates_YYYY_PLA.txt files (with _PLA suffix)"

