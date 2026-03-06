#!/bin/bash
# Parallel subset of 8-band PlanetScope images to 4 bands (2,4,6,8)
# Rename files to YYYYMMDD_HHMMSS_ID_PLANET_BOA.tif
# Delete original 8-band file after success

in_folder="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw"
missing_file_ids="$1"

dry_run=true   # Set to false to enable real processing

process_file() {
  local file="$1"
  local file_name
  file_name=$(basename "$file")
  local dir
  dir=$(dirname "$file")

  # Case 1: 8-band
  if [[ "$file_name" == *_3B_AnalyticMS_SR_8b_harmonized_clip.tif ]]; then
    local base_no_ext="${file_name%%_3B_AnalyticMS_SR_8b_harmonized_clip.tif}"
    local date_part="${base_no_ext%%_*}"
    local rest="${base_no_ext#*_}"
    local out_file="${dir}/${date_part}_${rest}_PLANET_BOA.tif"

    if [[ -f "$out_file" ]]; then
      echo "Skipping existing (8b): $out_file"
      return
    fi

    if [[ "$dry_run" == true ]]; then
      echo "[DRY RUN] Would extract bands 2,4,6,8 from: $file"
      echo "[DRY RUN] Would create: $out_file"
      echo "[DRY RUN] Would delete original: $file"
    else
      echo "Processing 8-band -> $file_name"
      if gdal_translate -b 2 -b 4 -b 6 -b 8 "$file" "$out_file" >/dev/null 2>&1; then
        echo "Created: $out_file"
        rm -f "$file"
      else
        echo "Failed to process (8b): $file" >&2
      fi
    fi

  # Case 2: 4-band
  elif [[ "$file_name" == *_3B_AnalyticMS_SR_harmonized_clip.tif ]]; then
    local base_no_ext="${file_name%%_3B_AnalyticMS_SR_harmonized_clip.tif}"
    local date_part="${base_no_ext%%_*}"
    local rest="${base_no_ext#*_}"
    local out_file="${dir}/${date_part}_${rest}_PLANET_BOA.tif"

    if [[ -f "$out_file" ]]; then
      echo "Skipping existing (4b): $out_file"
      return
    fi

    if [[ "$dry_run" == true ]]; then
      echo "[DRY RUN] Would rename: $file"
      echo "[DRY RUN] Would become: $out_file"
    else
      echo "Renaming 4-band -> $file_name"
      mv "$file" "$out_file"
    fi

  else
    echo "Skipping (unknown pattern): $file"
  fi
}

export -f process_file

# --- MAIN LOGIC ---

if [[ -z "$missing_file_ids" ]]; then
  echo "Parallel processing of all 8-band files in all folders..."
  find "$in_folder" -type f -name "*_3B_AnalyticMS_SR_8b_harmonized_clip.tif" \
    | parallel -j 8 process_file {}
else
  echo "Processing only missing ID list: $missing_file_ids"
  grep -o '[0-9]\{8\}_[0-9]\{6\}_[^ ]*' "$missing_file_ids" | \
  while read -r file_id; do
    find "$in_folder" -type f -name "${file_id}_3B_AnalyticMS_SR_8b_harmonized_clip.tif" \
      | parallel -j 8 process_file {}
  done
fi

echo "✅ All done."

