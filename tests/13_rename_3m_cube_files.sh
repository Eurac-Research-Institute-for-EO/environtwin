#!/bin/bash
# Rename *_PLANET_4b.tif files by replacing '_4b' with '_BOA'
# and *_PLANET.tif files by adding '_BOA' before '.tif'
# with optional dry-run mode

in_folder="/mnt/CEPH_PROJECTS/Environtwin/FORCE/PS_level2_3m/X-001_Y-001"
dry_run=false  # Set to false to actually apply renaming

echo "Processing files in: $in_folder"
echo

rename_file () {
    local file="$1"
    local old_name="$2"
    local new_name="$3"

    echo "Renaming file:"
    echo "  Old: $old_name"
    echo "  New: $new_name"
    echo

    if [ "$dry_run" = false ]; then
        mv "$file" "$in_folder/$new_name"
    fi
}

# Step 1: Replace '_4b' with '_BOA' for PLANET files
for file in "$in_folder"/*PLANET_4b.tif; do
    [ -e "$file" ] || continue  # skip if no matching files

    file_name=$(basename "$file")
    base="${file_name%_4b.tif}"
    newname="${base}_BOA.tif"

    rename_file "$file" "$file_name" "$newname"
done

# Step 2: Add '_BOA' before '.tif' for PLANET files (if not already done)
for file in "$in_folder"/*_PLANET.tif; do
    [ -e "$file" ] || continue  # skip if no files match
    [[ "$file" == *_BOA.tif ]] && continue  # skip if already has _BOA

    file_name=$(basename "$file")
    base="${file_name%.tif}"
    newname="${base}_BOA.tif"

    rename_file "$file" "$file_name" "$newname"
done

echo "Done!"

