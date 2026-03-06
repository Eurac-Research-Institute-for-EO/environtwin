#!/bin/bash

INPUT_DIR="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/SEN2"
BLUE_BAND=1

export INPUT_DIR BLUE_BAND

find "$INPUT_DIR" -maxdepth 1 -type f -name "2017*_SEN2*_BOA.tif" -print0 | \
parallel -0 -j +0 '
    raster={}
    filename=$(basename "$raster")

    output="$INPUT_DIR/${filename%}_blue_mask.tif"

    echo "⚙️ Processing: $filename"

    gdal_calc.py -A "$raster" --A_band=$BLUE_BAND --calc="A>2000" \
        --type=Int16 --NoDataValue=-9999 --outfile="$output" --quiet
'

echo "✅ Done! All 2017 files processed."

