#!/bin/bash

# Parent folder containing all subfolders to check
BASE_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH"

# Output file for missing files
OUTPUT_FILE="$BASE_DIR/missing_files.txt"
> "$OUTPUT_FILE"  # Clear previous results

echo "Missing file report" >> "$OUTPUT_FILE"
echo "====================" >> "$OUTPUT_FILE"

# Loop through all subfolders in test_PA
for DIR in "$BASE_DIR"/*; do
    echo ""
    echo "Checking folder: $DIR"
    echo "Folder: $DIR" >> "$OUTPUT_FILE"
    echo "-------------------------" >> "$OUTPUT_FILE"

    cd "$DIR" || continue

    # Count files
    count_sr=$(ls *_PLANET_*_BOA.tif 2>/dev/null | wc -l)
    count_udm2=$(ls *_PLANET_udm2_mask.tif 2>/dev/null | wc -l)

    echo "  SR files:   $count_sr"
    echo "  UDM2 files: $count_udm2"
    echo "" 

    echo "  SR files:   $count_sr" >> "$OUTPUT_FILE"
    echo "  UDM2 files: $count_udm2" >> "$OUTPUT_FILE"

    # Extract prefixes
    prefixes_sr=$(ls *_PLANET_*_BOA.tif 2>/dev/null | sed 's/_PLANET_.*_BOA\.tif$//')
    prefixes_udm2=$(ls *_PLANET_udm2_mask.tif 2>/dev/null | sed 's/_PLANET_udm2_mask.tif//')

    missing_udm2=0
    missing_sr=0

    # Check for missing UDM2
    for prefix in $prefixes_sr; do
        if ! echo "$prefixes_udm2" | grep -q "^$prefix$"; then
            echo "Missing udm2 for: $prefix"
            echo "Missing udm2 for: $prefix" >> "$OUTPUT_FILE"
            missing_udm2=$((missing_udm2+1))
        fi
    done

    # Check for missing SR/BOA
    for prefix in $prefixes_udm2; do
        if ! echo "$prefixes_sr" | grep -q "^$prefix$"; then
            echo "Missing PLANET_BOA for: $prefix"
            echo "Missing PLANET_BOA for: $prefix" >> "$OUTPUT_FILE"
            missing_sr=$((missing_sr+1))
        fi
    done

    echo "Summary:"
    echo "  Missing UDM2 files:          $missing_udm2"
    echo "  Missing PLANET_BOA files:    $missing_sr"

    echo "" >> "$OUTPUT_FILE"
    echo "Summary:" >> "$OUTPUT_FILE"
    echo "  Missing UDM2 files:          $missing_udm2" >> "$OUTPUT_FILE"
    echo "  Missing PLANET_BOA files:    $missing_sr" >> "$OUTPUT_FILE"
    echo "-------------------------" >> "$OUTPUT_FILE"
done

echo ""
echo "✅ Search complete! All results stored in:"
echo "$OUTPUT_FILE"

