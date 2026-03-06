#!/bin/bash
# This script:
# 1) Renames files from *_3B_PLANET_BOA.tif to *_PLANET_BOA.tif
# 2) Deletes files matching *_8b_harmonized_clip.tif
#    across all subfolders of a given base folder, in parallel.

in_folder="/mnt/CEPH_PROJECTS/Environtwin/FORCE/test_PA"

echo "🔍 Searching recursively in: $in_folder"
echo

#############################################
# 1) Remove _3B from PLANET_BOA filenames
#############################################
rename_planet_3b() {
    file="$1"
    file_name=$(basename "$file")
    dir_name=$(dirname "$file")

    if [[ "$file_name" == *"_3B_PLANET_BOA.tif" ]]; then
        # Remove only the _3B before PLANET_BOA
        newname=$(echo "$file_name" | sed -E 's/_3B_PLANET_BOA\.tif$/_PLANET_BOA.tif/')
        newpath="${dir_name}/${newname}"

        echo "Renaming:"
        echo "  $file_name → $newname"
        mv "$file" "$newpath"
    fi
}
export -f rename_planet_3b

echo "➤ Renaming PLANET_BOA files (removing _3B)..."
find "$in_folder" -type f -name "*PLANET_BOA.tif" \
  | parallel -j "$(nproc)" rename_planet_3b {}

echo "✅ Done renaming PLANET_BOA files."
echo

#############################################
# 2) Delete *_8b_harmonized_clip.tif files
#############################################
delete_harmonized() {
    file="$1"
    file_name=$(basename "$file")
    echo "Deleting: $file_name"
    rm -f "$file"
}
export -f delete_harmonized

echo "➤ Deleting *_8b_harmonized_clip.tif files..."
find "$in_folder" -type f -name "*_8b_harmonized_clip.tif" \
  | parallel -j "$(nproc)" delete_harmonized {}

echo
echo "✅ All done: cleaned PLANET_BOA filenames and removed harmonized files."

