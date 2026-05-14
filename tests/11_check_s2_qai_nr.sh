#!/bin/bash

BASE_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH"
OUTPUT_FILE="$BASE_DIR/missing_files_SEN2.txt"
> "$OUTPUT_FILE"

echo "Missing file report" >> "$OUTPUT_FILE"
echo "====================" >> "$OUTPUT_FILE"

for DIR in "$BASE_DIR"/*/; do
    echo ""
    echo "Checking folder: $DIR"
    echo "Folder: $DIR" >> "$OUTPUT_FILE"
    echo "-------------------------" >> "$OUTPUT_FILE"

    # Count files
    count_sr=$(find "$DIR" -maxdepth 1 -type f -name '*_SEN2?_BOA.tif' | wc -l)
    count_udm2=$(find "$DIR" -maxdepth 1 -type f -name '*_SEN2?_QAI.tif' | wc -l)

    echo "  SR files:   $count_sr"
    echo "  QAI files:  $count_udm2"
    echo "" >> "$OUTPUT_FILE"
    echo "  SR files:   $count_sr" >> "$OUTPUT_FILE"
    echo "  QAI files:  $count_udm2" >> "$OUTPUT_FILE"

    # Extract prefixes
    IFS=$'\n'
    prefixes_sr=$(find "$DIR" -maxdepth 1 -type f -name '*_SEN2?_BOA.tif' -printf "%f\n" | sed -E 's/_SEN2._BOA\.tif$//')
    prefixes_udm2=$(find "$DIR" -maxdepth 1 -type f -name '*_SEN2?_QAI.tif' -printf "%f\n" | sed -E 's/_SEN2._QAI\.tif$//')

    missing_udm2=0
    for prefix in $prefixes_sr; do
        if ! echo "$prefixes_udm2" | grep -Fxq "$prefix"; then
            echo "Missing UDM2 for: $prefix"
            echo "Missing UDM2 for: $prefix" >> "$OUTPUT_FILE"
            missing_udm2=$((missing_udm2+1))
        fi
    done

    missing_sr=0
    for prefix in $prefixes_udm2; do
        if ! echo "$prefixes_sr" | grep -Fxq "$prefix"; then
            echo "Missing SR/BOA for: $prefix"
            echo "Missing SR/BOA for: $prefix" >> "$OUTPUT_FILE"
            missing_sr=$((missing_sr+1))
        fi
    done
    unset IFS

    echo "Summary:"
    echo "  Missing QAI files:       $missing_udm2"
    echo "  Missing SR/BOA files:    $missing_sr"
    echo "" >> "$OUTPUT_FILE"
    echo "Summary:" >> "$OUTPUT_FILE"
    echo "  Missing QAI files:       $missing_udm2" >> "$OUTPUT_FILE"
    echo "  Missing SR/BOA files:    $missing_sr" >> "$OUTPUT_FILE"
    echo "-------------------------" >> "$OUTPUT_FILE"
done

echo ""
echo "✅ Search complete! All results stored in:"
echo "$OUTPUT_FILE"


