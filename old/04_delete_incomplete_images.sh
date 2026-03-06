#!/bin/bash

BASE_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/test_PA"
LOG_FILE="$BASE_DIR/missing_files_SEN2.txt"
TARGET_DIR="$BASE_DIR/incomplete_scenes"

# Create target folder if it doesn't exist
mkdir -p "$TARGET_DIR"

if [ ! -f "$LOG_FILE" ]; then
    echo "Error: Missing log file at $LOG_FILE"
    exit 1
fi

echo "--------------------------------------------------"
echo " MOVING incomplete Planet scenes (instead of deleting)"
echo " Destination: $TARGET_DIR"
echo "--------------------------------------------------"
echo ""

current_dir=""

while IFS= read -r line; do
    # Detect folder lines
    if [[ $line == Folder:* ]]; then
        current_dir=$(echo "$line" | awk -F': ' '{print $2}')
        echo "📂 Folder: $current_dir"
        continue
    fi
    
# Detect missing file prefixes
if [[ $line == Missing*for:* ]]; then
    prefix=$(echo "$line" | awk -F': ' '{print $2}' | xargs)  # trim whitespace
    echo "  ⚠️ Incomplete prefix detected: $prefix"

    if [ -d "$current_dir" ]; then
        # Build find command safely
        find_cmd=(find "$current_dir" -maxdepth 1 -type f \( \
            -name "${prefix}_PLANET_BOA.tif" -o \
            -name "${prefix}_PLANET_udm2.tif" -o \
            -name "${prefix}_SEN2?_BOA.tif" -o \
            -name "${prefix}_SEN2?_QAI.tif" -o \
            -name "${prefix}*.tif" -o \
            -name "${prefix}*.xml" -o \
            -name "${prefix}*.json" \
        \) -print)

        # Execute find and move files
        while IFS= read -r file; do
            echo "    → Moving: $file"
            mv "$file" "$TARGET_DIR/"
        done < <("${find_cmd[@]}")
    else
        echo "  ⚠️ Warning: Folder $current_dir not found — skipping"
    fi
fi

done < "$LOG_FILE"

echo ""
echo "--------------------------------------------------"
echo "✅ DONE — All incomplete scenes have been MOVED to:"
echo "   $TARGET_DIR"
echo "--------------------------------------------------"


