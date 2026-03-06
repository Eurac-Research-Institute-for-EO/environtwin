#!/bin/bash

IN_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard"
TARGET_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/coregistered"
MISSING_IDS="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/coregistered/missing_ids.txt"

mkdir -p "$TARGET_DIR"

# Loop over only BOA files
for file in "$IN_DIR"/*_PLANET_*BOA.tif; do
    basefile=$(basename "$file")

    # Extract the prefix before _PLANET_
    prefix="${basefile%%_PLANET_*}"

    # Only copy if the prefix is in missing IDs
    if grep -Fxq "$prefix" "$MISSING_IDS"; then
        echo "Copying $basefile..."
        cp "$file" "$TARGET_DIR/"
    fi
done
