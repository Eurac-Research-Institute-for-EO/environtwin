#!/bin/bash

INPUT_DIRS=(
    #"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/02/X0004_Y0002/data"
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/02/X0006_Y0000/data"
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/02/X0006_Y0001/data"
    #"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/02/X0007_Y0000/data"
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/02/X0008_Y0000/data"
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/02/X0008_Y-001/data"
    #"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/02/X0007_Y-001/data"
    #"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/02/X0009_Y-001/data"
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/02/X-001_Y-001/data"
    #"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/02/X-002_Y-002/data"

)

echo "-----------------------------------------------------------"
echo "Parallel deleting files with 'PLA_masked_NDV'"
echo "-----------------------------------------------------------"
echo ""

# Export function so GNU parallel can use it
delete_file() {
    local file="$1"
    echo "🗑 Deleting: $file"
    rm -f "$file"
}
export -f delete_file

# Count files before deleting
total_before=0
for INPUT_DIR in "${INPUT_DIRS[@]}"; do
    count=$(find "$INPUT_DIR" -type f -name "*PLA_masked_blue_NDV.tif" | wc -l)
    total_before=$((total_before + count))
done

echo "Found $total_before files to delete."

# Delete files in parallel
for INPUT_DIR in "${INPUT_DIRS[@]}"; do
    find "$INPUT_DIR" -type f -name "*PLA_masked_blue_NDV.tif" \
        | parallel -j 20 delete_file {}
done


echo ""
echo "-----------------------------------------------------------"
echo "✅ Done! Deleted $(find "$BASE_DIR" -type f -name "*PLA_masked_blue_NDV.tif" | wc -l) files."
echo "-----------------------------------------------------------"

