#!/bin/bash

# Folder containing the files
FOLDER="/mnt/CEPH_PROJECTS/Environtwin/FORCE/P_level2/X-001_Y-001"
# Output file
OUTPUT="/mnt/CEPH_PROJECTS/Environtwin/FORCE/image_ids.txt"

# Clear or create output file
> "$OUTPUT"

# Enable nullglob so non-matching globs expand to nothing
shopt -s nullglob

# Loop through files matching either pattern
for file in "$FOLDER"/*_udm2.tif; do
  # Get just the filename
  filename=$(basename "$file")
  
  # Extract the prefix 
  prefix=${filename%%_udm2*}

  # Write prefix to output file
  echo "$prefix" >> "$OUTPUT"
done

# Disable nullglob if needed
shopt -u nullglob

echo "Prefixes extracted to $OUTPUT"

