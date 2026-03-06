#!/bin/bash
set -euo pipefail

level3_root="/mnt/CEPH_PROJECTS/Environtwin/FORCE/masks/frost"
#level3_root="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/SEN2"
out_base="/mnt/CEPH_PROJECTS/Environtwin/FORCE/masks/frost/mosaic"
#out_base="/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/SEN2/mosaic"
DOCKER_IMAGE="davidfrantz/force"

mkdir -p "$out_base"

declare -a tile_pairs=(
  "X0006_Y0000 X0006_Y0001"
  "X0008_Y0000 X0008_Y-001"
  "X0007_Y0000 X0007_Y-001"
)

echo "Copying .tif files and running force-mosaic..."

for pair in "${tile_pairs[@]}"; do
  read -r tile1 tile2 <<< "$pair"
  echo "→ Processing pair: $tile1 + $tile2"

  # Create pair folder structure
  pair_folder="$out_base/${tile1}_${tile2}"
  mkdir -p "$pair_folder/$tile1" "$pair_folder/$tile2"

  # Copy .tif files into respective subfolders
  echo "  Copying files..."
  find "$level3_root/$tile1" -name "*.tif" -exec cp {} "$pair_folder/$tile1/" \;
  find "$level3_root/$tile2" -name "*.tif" -exec cp {} "$pair_folder/$tile2/" \;

  # Run force-mosaic on the pair folder
  echo "  Running force-mosaic..."
  docker run --rm --user "$(id -u):$(id -g)" \
    -v "$pair_folder:/data:rw" \
    "$DOCKER_IMAGE" bash -c "
      set -euo pipefail
      cd /data
      echo 'Available tiles:'
      ls -la */*.tif | head -10 || true
      echo 'Running force-mosaic...'
      force-mosaic /data -m mosaic
    "

  echo "✓ Completed: $pair_folder"
  echo ""
done

echo "All mosaics finished."

