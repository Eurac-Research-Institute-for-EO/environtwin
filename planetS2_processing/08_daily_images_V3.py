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
   - write out standard deviation

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
BASE_PATH = Path('/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw')
sites = ["MH"]

# Output folder for daily mosaics
output_folder = Path('/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_daily/03')
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

def find_individual_pairs(im_folder: Path, udm_folder: Path):
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
def process_daily_mosaic(date, image_pairs, output_folder):
    global nodata_val

    """
    Combine all BOA + UDM pairs for a date into a daily mosaic.

    Logic:
        - clear == 1 defines a usable observation
        - BOA pixels are included only where clear == 1
        - UDM pixels are included only where clear == 1
        - If there are no clear pixels in any image, nothing is written
        - For multiple images, BOA is averaged per pixel
        - Count = number of clear observations per pixel
        - Standard deviation is calculated where >1 observations exist
    """

    n_images = len(image_pairs)

    print(f"\n{'='*80}")
    print(f"DATE {date}: MOSAICKING {n_images} IMAGE PAIRS")
    print(f"{'='*80}")

    # -------------------------------------------------------------------------
    # Output files
    # -------------------------------------------------------------------------

    out_boa = output_folder / f"{date}_PLANET_BOA.tif"
    out_udm = output_folder / f"{date}_PLANET_udm2_mask.tif"
    out_cnt = output_folder / f"{date}_PLANET_count.tif"
    out_std = output_folder / f"{date}_PLANET_std.tif"

    # -------------------------------------------------------------------------
    # Reference profiles
    # -------------------------------------------------------------------------

    ref_boa, ref_udm = image_pairs[0]

    with rasterio.open(ref_boa) as ref:

        height = ref.height
        width = ref.width
        bands = ref.count

        boa_band_descriptions = ref.descriptions

        boa_profile = ref.profile.copy()
        boa_profile.update(
            dtype="float32",
            nodata=nodata_val,
            compress="deflate"
        )

        std_profile = boa_profile.copy()
        std_profile.update(
            dtype="float32",
            nodata=nodata_val
        )

    with rasterio.open(ref_udm) as refm:

        udm_bands = refm.count

        udm_profile = refm.profile.copy()
        udm_profile.update(
            dtype="int16",
            nodata=nodata_val,
            compress="deflate"
        )

        udm_band_descriptions = refm.descriptions

    # Count raster
    cnt_profile = boa_profile.copy()
    cnt_profile.update(
        count=1,
        dtype="uint16",
        nodata=0
    )

    print(
        f"Scene: {width}x{height}, "
        f"BOA={bands}b, UDM={udm_bands}b"
    )

    band_names = [
        "clear",
        "snow",
        "shadow",
        "light_haze",
        "heavy_haze",
        "cloud",
        "confidence",
        "udm2_unusable",
        "cloud_buffer",
        "shadow_buffer",
        "new_clear"
    ]

    # =========================================================================
    # SINGLE IMAGE
    # =========================================================================

    if n_images == 1:

        print("Single image → applying mask and writing directly")

        boa_fp, udm_fp = image_pairs[0]

        masked_full = np.full(
            (bands, height, width),
            nodata_val,
            dtype=np.int16
        )

        count_full = np.zeros(
            (height, width),
            dtype=np.int16
        )

        with rasterio.open(boa_fp) as boa, rasterio.open(udm_fp) as udm:

            for (r0, c0), win in boa.block_windows(1):

                rr = slice(r0, r0 + win.height)
                cc = slice(c0, c0 + win.width)

                boa_win = boa.read(window=win)
                udm_win = udm.read(window=win)

                # -------------------------------------------------------------
                # Clear mask
                # -------------------------------------------------------------

                clear = udm_win[10]                

                # -------------------------------------------------------------
                # BOA validity
                # -------------------------------------------------------------

                if boa.nodata is not None: 
                    boa_valid = np.all(
                        boa_win != boa.nodata, #
                        axis=0 
                    )                 
                else: 
                    boa_valid = np.ones(
                        (win.height, win.width), dtype=bool
                    )
                    
                final_mask = (clear == 1) & boa_valid

                
                # -------------------------------------------------------------
                # BOA
                # -------------------------------------------------------------

                masked_win = np.where(
                    final_mask[None, :, :],
                    boa_win,
                    nodata_val
                )

                masked_full[:, rr, cc] = masked_win

                # -------------------------------------------------------------
                # Count
                # -------------------------------------------------------------

                count_full[rr, cc] = final_mask.astype(np.uint16)

        # ---------------------------------------------------------------------
        # Check for valid pixels 
        # ---------------------------------------------------------------------

        valid_pixels = np.count_nonzero(count_full)

        print(f"Valid pixels: {valid_pixels}")

        if valid_pixels == 0:

            print(
                f"⚠️ Skipping {date} — "
                f"no clear/valid pixels: {boa_fp}"
            )

            return

        # ---------------------------------------------------------------------
        # Standard deviation
        # ---------------------------------------------------------------------

        std_full = np.where(
            count_full == 1,
            0,
            nodata_val
        ).astype(np.float32)

        # ---------------------------------------------------------------------
        # Write BOA
        # ---------------------------------------------------------------------

        with rasterio.open(out_boa, "w", **boa_profile) as dst:

            dst.write(masked_full)

            for i, desc in enumerate(
                boa_band_descriptions,
                start=1
            ):
                if desc:
                    dst.set_band_description(i, desc)

        # ---------------------------------------------------------------------
        # Write UDM
        # ---------------------------------------------------------------------

        with rasterio.open(udm_fp) as src: 
            udm_copy_profile = src.profile.copy() 
            udm_copy_profile.update( 
                compress="deflate" 
            ) 
            
            with rasterio.open(out_udm, "w", **udm_copy_profile) as dst: 
                dst.write(src.read()) # Preserve original UDM band descriptions 
                for i, desc in enumerate( 
                    src.descriptions, start=1 
                ): 
                    if desc: 
                        dst.set_band_description(i, desc)

        # ---------------------------------------------------------------------
        # Write count
        # ---------------------------------------------------------------------

        with rasterio.open(out_cnt, "w", **cnt_profile) as dst:

            dst.write(count_full, 1)
            dst.set_band_description(1, "count")

        # ---------------------------------------------------------------------
        # Write standard deviation
        # ---------------------------------------------------------------------

        with rasterio.open(out_std, "w", **std_profile) as dst:

            dst.write(std_full, 1)

            for i, desc in enumerate(
                boa_band_descriptions,
                start=1
            ):
                if desc:
                    dst.set_band_description(
                        i,
                        f"{desc}_std"
                    )

        print(
            f"{date}: single masked BOA + UDM + count + std written"
        )

        return

    # =========================================================================
    # MULTIPLE IMAGES
    # =========================================================================

    print(
        f"{n_images} images → computing masked mean mosaic"
    )

    # -------------------------------------------------------------------------
    # Initialize accumulators
    # -------------------------------------------------------------------------

    sum_boa = np.zeros(
        (bands, height, width),
        dtype=np.float64
    )

    sumsq_boa = np.zeros(
        (bands, height, width),
        dtype=np.float64
    )

    cnt_boa = np.zeros(
        (bands, height, width),
        dtype=np.uint16
    )

    cnt_img = np.zeros(
        (height, width),
        dtype=np.uint16
    )

    sum_udm = np.zeros(
        (udm_bands, height, width),
        dtype=np.int32
    )

    # -------------------------------------------------------------------------
    # Process each image
    # -------------------------------------------------------------------------

    for boa_fp, udm_fp in image_pairs:

        print(f"Processing {boa_fp.name}")

        with rasterio.open(boa_fp) as boa, rasterio.open(udm_fp) as udm:

            for (r0, c0), win in boa.block_windows(1):

                rr = slice(r0, r0 + win.height)
                cc = slice(c0, c0 + win.width)

                boa_win = boa.read(window=win)
                udm_win = udm.read(window=win)

                # -------------------------------------------------------------
                # Clear mask
                # -------------------------------------------------------------

                clear = udm_win[10]

                final_mask = clear == 1

                # -------------------------------------------------------------
                # BOA validity
                # -------------------------------------------------------------

                if boa.nodata is not None:

                    boa_valid = np.all(
                        boa_win != boa.nodata,
                        axis=0
                    )

                    final_mask &= boa_valid

                # -------------------------------------------------------------
                # Count observations
                # -------------------------------------------------------------

                mask_uint = final_mask.astype(np.uint16)

                cnt_img[rr, cc] += mask_uint

                cnt_boa[:, rr, cc] += mask_uint

                # -------------------------------------------------------------
                # BOA accumulation
                # -------------------------------------------------------------

                masked_win = np.where(
                    final_mask[None, :, :],
                    boa_win,
                    0
                )

                sum_boa[:, rr, cc] += masked_win

                sumsq_boa[:, rr, cc] += masked_win ** 2

                # -------------------------------------------------------------
                # UDM accumulation
                # -------------------------------------------------------------

                masked_udm_win = np.where(
                    final_mask[None, :, :],
                    udm_win,
                    0
                )

                sum_udm[:, rr, cc] += (
                    masked_udm_win.astype(np.int32)
                )

    # =========================================================================
    # CHECK WHETHER ANY VALID PIXELS EXIST
    # =========================================================================

    valid_pixels = np.count_nonzero(cnt_img)

    print(f"Valid clear pixels: {valid_pixels}")

    if valid_pixels == 0:

        print(
            f"⚠️ Skipping {date} — "
            f"no clear/valid pixels in any image"
        )

        return

    # =========================================================================
    # DAILY MEAN
    # =========================================================================

    with np.errstate(
        divide="ignore",
        invalid="ignore"
    ):

        daily_mean = np.where(
            cnt_boa > 0,
            sum_boa / cnt_boa,
            nodata_val
        ).astype(np.float32)

    # =========================================================================
    # STANDARD DEVIATION
    # =========================================================================

    with np.errstate(
        divide="ignore",
        invalid="ignore"
    ):

        mean = np.where(
            cnt_boa > 0,
            sum_boa / cnt_boa,
            0
        )

        mean_sq = np.where(
            cnt_boa > 0,
            sumsq_boa / cnt_boa,
            0
        )

        variance = mean_sq - mean ** 2

        variance = np.maximum(
            variance,
            0
        )

        daily_std = np.sqrt(variance)

        daily_std = np.where(
            cnt_boa > 1,
            daily_std,
            0
        )

        daily_std = np.where(
            cnt_boa > 0,
            daily_std,
            nodata_val
        ).astype(np.float32)

    # =========================================================================
    # UDM
    # =========================================================================

    udm_final = np.where(
        sum_udm > 0,
        1,
        0
    ).astype(np.int16)

    # =========================================================================
    # WRITE OUTPUTS
    # =========================================================================

    with rasterio.open(
        out_boa,
        "w",
        **boa_profile
    ) as dst:

        dst.write(daily_mean)

        for i, desc in enumerate(
            boa_band_descriptions,
            start=1
        ):
            if desc:
                dst.set_band_description(i, desc)

    with rasterio.open(
        out_udm,
        "w",
        **udm_profile
    ) as dst:

        dst.write(udm_final)

        for idx, name in enumerate(
            band_names,
            start=1
        ):
            dst.set_band_description(idx, name)

    with rasterio.open(
        out_cnt,
        "w",
        **cnt_profile
    ) as dst:

        dst.write(cnt_img, 1)
        dst.set_band_description(1, "count")

    with rasterio.open(
        out_std,
        "w",
        **std_profile
    ) as dst:

        dst.write(daily_std)

        for i, desc in enumerate(
            boa_band_descriptions,
            start=1
        ):
            if desc:
                dst.set_band_description(
                    i,
                    f"{desc}_std"
                )

    print(
        f"{date}: BOA mosaic + UDM + count + std written"
    )


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    print("PlanetScope Daily Mosaicking - ALL SITES SEQUENTIAL")
    
    # Sequential loop over sites (MH, etc.)
    for site in sites:
    
        in_path = BASE_PATH / site
    
        im_folder = in_path / "coregistered"  # BOA *.bsq
        udm_folder = in_path / "standard"     # UDM *.tif
        
        if not (im_folder.is_dir() and udm_folder.is_dir()):
            print(f"Skipping {in_path} (missing folders)")
            continue

        site_output = output_folder / site
        site_output.mkdir(exist_ok=True)
            
        print(f"\n{'='*80}")
        print(f"SITE: {in_path.name}")
        print(f"  BOA: {im_folder}")
        print(f"  UDM: {udm_folder}")
        print(f"{'='*80}")
        
        # find_individual_pairs 
        date_groups = find_individual_pairs(im_folder, udm_folder)
        
        if not date_groups:
            append_log(log_missing_pairs, f"No pairs in {in_path}")
            continue
        
        # process_daily_mosaic loop 
        for date, image_pairs in date_groups.items():
            process_daily_mosaic(date, image_pairs, site_output)
        
        print(f"✓ SITE COMPLETE: {in_path} ({len(date_groups)} dates)")
    
    print("\n ALL SITES PROCESSED!")
    print(f"Logs: {log_missing_pairs}, {log_profile_mismatch}")

if __name__ == "__main__":
    main()