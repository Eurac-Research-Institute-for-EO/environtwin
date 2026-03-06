#!/bin/bash

# Root directory
root_dir="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw"

process_file() {
  file=$1
  temp_output="${file}.tmp.tif"
  
  gdal_calc.py \
    -A "$file" \
    --outfile="$temp_output" \
    --calc="A*(A!=-32768) + (-9999)*(A==-32768)" \
    --type=Int16 \
    --NoDataValue=-9999 \
    --allBands=A \
    --co=COMPRESS=LZW \
    --co=TILED=YES \
    --overwrite
  
  if [ $? -eq 0 ]; then
    mv "$temp_output" "$file"
    echo "Processed $file"
  else
    echo "Failed $file; keeping $temp_output"
  fi
}

export -f process_file

# FIXED: Integer jobs = cores + cores/2 (~1.5x) or just use all cores
jobs=$(( $(nproc) + $(nproc) / 2 ))
find "$root_dir" -name "*_udm2_mask.tif" -type f -print0 | \
  parallel -j "$jobs" --halt 2 --null --eta process_file {}

