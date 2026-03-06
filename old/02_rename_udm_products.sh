#!/bin/bash
# Renames files like:
# 20250904_104135_56_253c_3B_udm2_clip.tif → 20250904_104135_56_253c_PLANET_udm2.tif

in_folder="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw"

echo "🔍 Searching recursively in: $in_folder"
echo "Running renaming in parallel..."
echo

rename_file_func() {
    file="$1"
    file_name=$(basename "$file")
    dir_name=$(dirname "$file")

    # ✅ Remove the last parts: _3B_udm2_clip.tif or _3b_udm2_clip.tif
    base=$(echo "$file_name" | sed -E 's/_[0-9]+[bB]_udm2_clip\.tif$//')

    # ✅ Create new standardized name
    newname="${base}_PLANET_udm2.tif"
    #newname=$(echo "$file_name" | sed -E 's/_udm_count\.tif$/_udm2_count.tif/')
    newpath="${dir_name}/${newname}"

    echo "Renaming:"
    echo "  $file_name → $newname"
    mv "$file" "$newpath"
}

export -f rename_file_func

# ✅ Find and rename only matching UDM2 files
#find "$in_folder" -type f -name "*_udm_count.tif" \
find "$in_folder" -type f -name "*_udm2_clip.tif" \
    | parallel -j "$(nproc)" rename_file_func {}

echo
echo "✅ Done renaming all UDM2 PlanetScope files."

