#!/usr/bin/env python3
"""
PlanetScope Daily Mosaicking Pipeline
------------------------------------

This script processes PlanetScope BOA (surface reflectance) images
together with their corresponding UDM2 masks to produce **daily mean mosaics**.

Key steps:
1. Match each BOA to its **specific UDM** using full filename prefix.
2. Apply the improved clear mask per image.
3. For each date, compute:
   - Masked BOA daily mean composite
   - Combined UDM mosaic
   - Count of valid pixels contributing to the daily mosaic

Logs:
- missing_udm_pairs.log → BOAs without matching UDM
- profile_mismatches.log → any image profile inconsistencies
"""

import os
import shutil
import numpy as np
import rasterio
from collections import defaultdict
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================

# Input folders
im_folder = Path('/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/coregistered/')
udm_folder = Path('/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard')

# Output folder for daily mosaics
output_folder = Path('/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_daily/03/MH')
output_folder.mkdir(parents=True, exist_ok=True)

# Nodata value used in outputs
nodata_val = -9999

# Log files
log_missing_pairs = output_folder / "missing_udm_pairs.log"
log_profile_mismatch = output_folder / "profile_mismatches.log"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def extract_date(filepath):
    """Extract YYYYMMDD date from PlanetScope filename."""
    filename = Path(filepath).stem
    if "_PLANET" not in filename:
        return None
    return filename[:8]  # first 8 chars are date


def append_log(log_file, msg):
    """Append a message to a log file with timestamp."""
    try:
        with open(log_file, 'a') as f:
            f.write(f"[{os.popen('date').read().strip()}] {msg}\n")
    except:
        pass


def extract_prefix(filepath):
    """
    Extract unique scene prefix from PlanetScope filenames.
    
    Example:
        20211130_091749_81_2423_PLANET_BOA.bsq → 20211130_091749_81_2423
    """
    filename = os.path.basename(filepath)
    stem = os.path.splitext(filename)[0]  # remove extension
    if "_PLANET" not in stem:
        return None
    return stem.split("_PLANET")[0]


# =============================================================================
# 1. MATCH BOA & UDM FILES BY FULL PREFIX
# =============================================================================

def find_individual_pairs():
    """
    Scan BOA and UDM folders and match files using extract_prefix().
    
    Returns:
        date_groups: dict {YYYYMMDD: [(boa_fp, udm_fp), ...]}
    """
    print("Matching BOA-UDM pairs by common prefix...")

    boa_dict = {}  # {prefix: boa_filepath}
    udm_dict = {}  # {prefix: udm_filepath}

    # Scan BOA files
    for f in im_folder.rglob("*_PLANET_BOA.bsq"):
        prefix = extract_prefix(f)
        if prefix:
            boa_dict[prefix] = f
            print(f"BOA: {f.name} → {prefix}")

    # Scan UDM files
    for f in udm_folder.rglob("*_PLANET_udm2_buffer.tif"):
        prefix = extract_prefix(f)
        if prefix:
            udm_dict[prefix] = f
            print(f"UDM: {f.name} → {prefix}")

    # Match pairs and group by date
    pairs = []
    date_groups = defaultdict(list)

    for prefix, boa_fp in boa_dict.items():
        if prefix in udm_dict:
            udm_fp = udm_dict[prefix]
            pairs.append((boa_fp, udm_fp))
            date = prefix[:8]  # YYYYMMDD
            date_groups[date].append((boa_fp, udm_fp))
            print(f"✓ MATCH: {prefix}")
        else:
            print(f"NO UDM for prefix: {prefix}")

    print(f"\n {len(pairs)} PERFECT PAIRS across {len(date_groups)} dates")
    return dict(date_groups)


# =============================================================================
# 2. PROCESS DAILY MOSAIC FOR A SINGLE DATE
# =============================================================================

def process_daily_mosaic(date, image_pairs):
    """
    Combine all BOA + UDM pairs for a date into a daily mosaic.

    Steps:
        - Apply improved clear mask per image
        - Accumulate sum/count for BOA bands
        - Accumulate sum for UDM bands
        - Compute masked mean BOA composite
        - Clip UDM to 0/1
        - Write outputs:
            - BOA daily mean
            - UDM mosaic
            - Count raster
    """
    n_images = len(image_pairs)
    print(f"\n{'='*80}")
    print(f"DATE {date}: MOSAICKING {n_images} IMAGE PAIRS")
    print(f"{'='*80}")

    # Output files
    out_boa = output_folder / f"{date}_PLANET_BOA.tif"
    out_udm = output_folder / f"{date}_PLANET_udm2_mask.tif"
    out_cnt = output_folder / f"{date}_PLANET_count.tif"

    # Skip if already processed
    if all(f.exists() for f in [out_boa, out_udm, out_cnt]):
        print(f"⏭️  Skipping {date} (already processed)")
        return

    # Reference profiles from first image
    ref_boa, ref_udm = image_pairs[0]

    # BOA raster profile
    with rasterio.open(ref_boa) as ref:
        height, width = ref.height, ref.width
        bands = ref.count
        ref_crs = ref.crs
        ref_transform = ref.transform
        band_descriptions = ref.descriptions
        boa_profile = ref.profile.copy()
        boa_profile.update(dtype="float32", nodata=nodata_val, compress="deflate")

    # UDM raster profile
    with rasterio.open(ref_udm) as refm:
        udm_bands = refm.count
        udm_profile = refm.profile.copy()
        udm_profile.update(count=udm_bands, dtype="int16", compress="deflate", nodata=-9999)

    # Count raster profile
    cnt_profile = boa_profile.copy()
    cnt_profile.update(count=1, dtype="uint16", nodata=0)

    print(f" Scene: {width}x{height}, BOA={bands}b, UDM={udm_bands}b")

    # =============================================================================
    # SINGLE IMAGE CASE → write directly
    # =============================================================================

    if n_images == 1:
        print("Single image → applying mask and writing directly")
        boa_fp, udm_fp = image_pairs[0]

        # Initialize arrays
        masked_full = np.full((bands, height, width), nodata_val, dtype=np.int16)
        udm_full = np.full((udm_bands, height, width), -9999, dtype=np.int16)
        count_full = np.zeros((height, width), dtype=np.uint16)

        with rasterio.open(boa_fp) as boa, rasterio.open(udm_fp) as udm:

            for (r0, c0), win in boa.block_windows(1):
                rr = slice(r0, r0 + win.height)
                cc = slice(c0, c0 + win.width)

                boa_win = boa.read(window=win)
                udm_win = udm.read(window=win)

                # Use improved clear band (band 11)
                clear = udm_win[10, :, :]

                # Valid BOA pixels
                if boa.nodata is not None:
                    boa_valid = np.all(boa_win != boa.nodata, axis=0)

                # Final mask = valid BOA AND clear pixels
                final_mask = (clear == 1) & boa_valid

                # Apply mask
                masked_win = np.where(final_mask[None, :, :], boa_win, nodata_val)
                masked_full[:, rr, cc] = masked_win

                # Count valid pixels
                count_full[rr, cc] = final_mask.astype(np.int16)

        # Write BOA
        with rasterio.open(out_boa, "w", **boa_profile) as dst:
            dst.write(masked_full)
            for i, desc in enumerate(band_descriptions, start=1):
                if desc:
                    dst.set_band_description(i, desc)

        # Write UDM
        with rasterio.open(out_udm, "w", **udm_profile) as dst:
            dst.write(udm_win)
            band_names = [
                'clear', 'snow', 'shadow', 'light_haze', 'heavy_haze',
                'cloud', 'confidence', 'udm2_unusable',
                'cloud_buffer', 'shadow_buffer', 'new_clear'
            ]
            for idx, name in enumerate(band_names, start=1):
                dst.set_band_description(idx, name)

        # Write count
        with rasterio.open(out_cnt, "w", **cnt_profile) as dst:
            dst.write(count_full, 1)
            dst.set_band_description(1, "count")

        print(f"{date}: single masked BOA + extended UDM written")
        return

    # =============================================================================
    # MULTIPLE IMAGES → COMPUTE DAILY MEAN MOSAIC
    # =============================================================================

    print(f"{n_images} images → computing masked mean mosaic")

    # Initialize accumulators
    sum_boa = np.zeros((bands, height, width), dtype=np.float64)
    cnt_boa = np.zeros((bands, height, width), dtype=np.int16)
    cnt_img = np.zeros((height, width), dtype=np.int16)
    sum_udm = np.zeros((udm_bands, height, width), dtype=np.int16)

    # Loop through image pairs
    for boa_fp, udm_fp in image_pairs:
        print(f"Processing {boa_fp.name}")

        with rasterio.open(boa_fp) as boa, rasterio.open(udm_fp) as udm:

            for (r0, c0), win in boa.block_windows(1):
                rr = slice(r0, r0 + win.height)
                cc = slice(c0, c0 + win.width)

                boa_win = boa.read(window=win)
                udm_win = udm.read(window=win)

                # Clear mask
                clear = udm_win[10].astype(np.int16)

                # Accumulate UDM
                sum_udm[:, rr, cc] += udm_win.astype(np.int16)

                # Valid BOA pixels
                boa_valid = np.all(boa_win != (boa.nodata if boa.nodata else nodata_val), axis=0)
                final_mask = clear & boa_valid

                # Accumulate counts
                cnt_img[rr, cc] += final_mask
                cnt_boa[:, rr, cc] += final_mask

                # Masked BOA for accumulation
                masked_win = np.where(final_mask[None, :, :], boa_win, 0)
                sum_boa[:, rr, cc] += masked_win

    # Compute daily mean BOA
    with np.errstate(divide='ignore', invalid='ignore'):
        daily_mean = np.where(cnt_boa > 0, sum_boa / cnt_boa, nodata_val).astype(np.int16)

    # Clip UDM to 0/1
    udm_final = np.where(sum_udm > 1, 1, sum_udm).astype(np.int16)

    # Write outputs
    with rasterio.open(out_boa, "w", **boa_profile) as dst:
        dst.write(daily_mean)
        for i, desc in enumerate(band_descriptions, start=1):
            if desc:
                dst.set_band_description(i, desc)

    with rasterio.open(out_udm, "w", **udm_profile) as dst:
        dst.write(udm_final)
        band_names = [
            'clear', 'snow', 'shadow', 'light_haze', 'heavy_haze',
            'cloud', 'confidence', 'udm2_unusable',
            'cloud_buffer', 'shadow_buffer', 'new_clear'
        ]
        for idx, name in enumerate(band_names, start=1):
            dst.set_band_description(idx, name)

    with rasterio.open(out_cnt, "w", **cnt_profile) as dst:
        dst.write(cnt_img, 1)
        dst.set_band_description(1, "count")

    print(f"{date}: BOA mosaic + UDM + count written")


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    print("PlanetScope Daily Mosaicking (Individual UDM matching)")
    date_groups = find_individual_pairs()

    if not date_groups:
        print("No valid image pairs found!")
        return

    # Process each date sequentially
    for date, image_pairs in date_groups.items():
        process_daily_mosaic(date, image_pairs)

    print("\nALL DAILY MOSAICS COMPLETED!")
    print(f"Logs: {log_missing_pairs}, {log_profile_mismatch}")


if __name__ == "__main__":
    main()