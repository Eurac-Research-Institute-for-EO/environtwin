#!/bin/bash

BASE_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/missing"
TARGET_DIR="$BASE_DIR/winter_months"

# Create target folder if it doesn't exist
mkdir -p "$TARGET_DIR"

echo "-----------------------------------------------------------"
echo " Moving all files from months 01, 02, and 12"
echo " Destination: $TARGET_DIR"
echo "-----------------------------------------------------------"
echo ""

moved_count=0

# Loop through all subfolders directly under BASE_DIR
for DIR in "$BASE_DIR"/*/; do
    # Skip the target folder itself
    [[ "$DIR" == "$TARGET_DIR/" ]] && continue
    foldername=$(basename "$DIR")
    echo "📁 Checking folder: $DIR"

    # Create a corresponding subfolder inside winter_months
    DEST_SUBFOLDER="$TARGET_DIR/$foldername"
    mkdir -p "$DEST_SUBFOLDER"

    # Loop through all files directly inside this folder
    while IFS= read -r file; do
        filename=$(basename "$file")

        # Match YYYYMMDD anywhere in the filename
        if [[ "$filename" =~ ([0-9]{4})([0-9]{2})([0-9]{2}) ]]; then
            month="${BASH_REMATCH[2]}"

            if [[ "$month" =~ ^(01|02|12)$ ]]; then
                echo "    → Moving $filename → $DEST_SUBFOLDER/"
                mv "$file" "$DEST_SUBFOLDER/"
                ((moved_count++))
            fi
        fi
    done < <(find "$DIR" -maxdepth 1 -type f)
done

echo ""
echo "-----------------------------------------------------------"
echo "✅ Done! $moved_count files moved to: $TARGET_DIR"
echo "-----------------------------------------------------------"

