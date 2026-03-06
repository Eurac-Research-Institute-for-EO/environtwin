#!/bin/bash

# User settings
ndvi_folder="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/01_mowing/X-001_Y-001"
years_to_process="2017 2018 2019 2020 2021 2022 2023 2024 2025"

cd "$ndvi_folder" || { echo "Cannot change directory to $ndvi_folder"; exit 1; }

# Gather all files for specified years
all_files=()
for year in $years_to_process; do
  mapfile -t files < <(find . -maxdepth 1 -type f -name "${year}*_PLA_masked_NDV.tif" -print | sort)
  all_files+=("${files[@]}")
done

# Extract unique dates (first 8 chars of basename)
dates=()
for f in "${all_files[@]}"; do
  filename=$(basename "$f")
  dates+=("${filename:0:8}")
done
unique_dates=($(printf "%s\n" "${dates[@]}" | sort -u))

# Process files for each date
process_date() {
  local d=$1
  mapfile -t files_for_date < <(find . -maxdepth 1 -type f -name "${d}*_PLA_masked_NDV.tif" -print | sort)
  count=${#files_for_date[@]}

  if (( count > 1 )); then
    echo "Processing date $d with $count files..."

    inputs=()
    expr_terms=()
    letters=(A B C D E F G H I J K L M N O P Q R S T U V W X Y Z)
    i=0
    for f in "${files_for_date[@]}"; do
      inputs+=("-${letters[i]}=${f}")
      expr_terms+=("${letters[i]}.astype(numpy.float64)")
      ((i++))
      # GDAL supports max 26 inputs labeled A-Z
      if ((i >= 26)); then
        echo "Error: more than 26 files for date $d, cannot process with gdal_calc.py"
        return
      fi
    done

    expr="round(($(IFS=+; echo "${expr_terms[*]}"))/$count)"
    final_out="${d}_PLA_masked_NDV.tif"

    # Calculate mean with rounding and save as Int16 in one step
    gdal_calc.py "${inputs[@]}" --outfile="$final_out" --calc="$expr" --NoDataValue=-9999 --type=Int16 --quiet

    if [[ $? -eq 0 ]]; then
      rm -f "${files_for_date[@]}"
      echo "Saved mean NDVI for $d -> $final_out"
    else
      echo "Error processing date $d"
    fi

  else
    echo "Skipping date $d - only one NDVI image."
  fi
}

for date in "${unique_dates[@]}"; do
  process_date "$date"
done

echo "All NDVI files with multiple images processed and originals deleted."


