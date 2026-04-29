#!/bin/bash

ROOT="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw"
BAD="bad_files.txt"

# Clear bad files log
> "$BAD"

echo "Scanning for TIFFs in $ROOT ..."

# Generate list of files
FILES=$(find "$ROOT" -type f -regextype posix-egrep \
	-regex ".*/[0-9]{8}_.*_(PLANET_BOA|udm2_mask)\.tif$")
    #-regex ".*[0-9]{6}_LEVEL2_SEN2[A-C]_(BOA|QAI)\.tif$")
    
TOTAL_FILES=$(echo "$FILES" | wc -l)
echo "Found $TOTAL_FILES candidate TIFF files."

export BAD

# Function that will run in parallel
check_file() {
    file="$1"
    if ! gdalinfo "$file" >/dev/null 2>&1; then
        echo "$file" >> "$BAD"
    fi
}

export -f check_file

# Run in parallel using all CPU cores
echo "$FILES" | parallel -j "$(nproc)" check_file {}

# Count bad files
BAD_COUNT=$(wc -l < "$BAD")

# Print summary to console
echo
echo "========== TIFF CHECK SUMMARY =========="
echo "Total files scanned: $TOTAL_FILES"
echo "Unreadable files:   $BAD_COUNT"
echo "Bad files logged in: $BAD"
echo "======================================="

