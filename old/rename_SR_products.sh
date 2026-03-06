#!/bin/bash
# Parallel renaming of PlanetScope images in all subfolders:
# *_AnalyticMS_SR_harmonized_clip.tif → date_time_id_PLANET_BOA.tif  (without _3B in id)

in_folder="/mnt/CEPH_PROJECTS/Environtwin/FORCE/missing"

echo "🔍 Searching recursively in: $in_folder"
echo "Running renaming in parallel..."
echo

export rename_file_func
rename_file_func() {
    file="$1"
    file_name=$(basename "$file")
    dir_name=$(dirname "$file")

    # Extract date and time
    date=$(echo "$file_name" | cut -d'_' -f1)
    time=$(echo "$file_name" | cut -d'_' -f2)

    # Extract everything between date_time_ and _AnalyticMS...
    id=$(echo "$file_name" | sed -E "s/^${date}_${time}_(.*)_AnalyticMS_SR_8b_harmonized_clip\.tif$/\1/")

    # ✅ Remove trailing _3B if present in ID
    id=$(echo "$id" | sed -E 's/_3B$//')

    # Create final filename
    newname="${date}_${time}_${id}_PLANET_BOA.tif"
    newpath="${dir_name}/${newname}"

    echo "Renaming:"
    echo "  $file_name → $newname"
    mv "$file" "$newpath"
}

export -f rename_file_func

# Find files and process them in parallel
find "$in_folder" -type f -name "*_AnalyticMS_SR_8b_harmonized_clip.tif" \
    | parallel -j "$(nproc)" rename_file_func {}

echo
echo "✅ Done renaming all matching PlanetScope images (in parallel)."

