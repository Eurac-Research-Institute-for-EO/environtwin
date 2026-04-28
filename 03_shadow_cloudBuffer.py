#!/usr/bin/env python3
"""
UDM2 Post-Processing Pipeline
-----------------------------

This script enhances Planet UDM2 masks by:

1. Reprojecting UDM bands to a common mask grid
2. Creating a cloud buffer
3. Detecting shadows using:
      - NIR reflectance
      - Whiteness layer
      - Year-specific reference mosaic
4. Cleaning masks using sieve + dilation
5. Producing an improved clear mask
6. Writing a final 11-band GeoTIFF output

Processing is parallelized using multiprocessing.
Failed files are logged to CSV.
"""

import os
import glob
import csv
import numpy as np
import rasterio
from rasterio.warp import reproject, Resampling
from rasterio.features import sieve
from scipy.ndimage import maximum_filter
from multiprocessing import Pool


# ============================================================
# PATH CONFIGURATION
# ============================================================

# Base directory containing Level-2 Planet scenes
#BASE_DIR = "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/coregistered"
BASE_DIR = "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/SA/coregistered"
#UDM_DIR = "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard"
UDM_DIR = "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/SA/standard"

# Directory containing whiteness rasters
WHITENESS_DIR = "/mnt/CEPH_PROJECTS/Environtwin/FORCE/sites_whiteness/SA"

# Reference mosaics (one per year) used for shadow detection
mosaic_refs = [
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/mosaic/MH/PLANET_MOSAIC_4BANDS_PERIOD/final/PLANET_MOSAIC_20170701_20170715_DATA.tif",
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/mosaic/MH/PLANET_MOSAIC_4BANDS_PERIOD/final/PLANET_MOSAIC_20180616_20180630_DATA.tif",
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/mosaic/MH/PLANET_MOSAIC_4BANDS_PERIOD/final/PLANET_MOSAIC_20190701_20190715_DATA.tif",
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/mosaic/MH/PLANET_MOSAIC_4BANDS_PERIOD/final/PLANET_MOSAIC_20200701_20200715_DATA.tif",
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/mosaic/MH/PLANET_MOSAIC_4BANDS_PERIOD/final/PLANET_MOSAIC_20210701_20210715_DATA.tif",
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/mosaic/MH/PLANET_MOSAIC_4BANDS_PERIOD/final/PLANET_MOSAIC_20220701_20220715_DATA.tif",
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/mosaic/MH/PLANET_MOSAIC_4BANDS_PERIOD/final/PLANET_MOSAIC_20230701_20230715_DATA.tif",
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/mosaic/MH/PLANET_MOSAIC_4BANDS_PERIOD/final/PLANET_MOSAIC_20240701_20240715_DATA.tif",
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/mosaic/MH/PLANET_MOSAIC_4BANDS_PERIOD/final/PLANET_MOSAIC_20250716_20250722_DATA.tif"
]

# Target mask grid (defines CRS, resolution, and extent)
MASK_PATH = "/mnt/CEPH_PROJECTS/Environtwin/gis/masks/MH_mask.tif"

# Output directory
OUT_DIR = "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard/"
#OUT_DIR = "/mnt/CEPH_PROJECTS/Environtwin/FORCE/test/buffer"
os.makedirs(OUT_DIR, exist_ok=True)

# CSV log for failures
ERROR_LOG_PATH = os.path.join(OUT_DIR, "failed_udms.csv")


# ============================================================
# BUILD YEAR → REFERENCE MOSAIC LOOKUP
# ============================================================

# Extract year from mosaic filename and create dictionary
REF_BY_YEAR = {}

for ref_path in mosaic_refs:
    fname = os.path.basename(ref_path)
    year = fname.split("_")[2][:4]   # Extract "2017", "2018", etc.
    REF_BY_YEAR[year] = ref_path


# ============================================================
# PROCESSING PARAMETERS
# ============================================================

DARK_FACTOR = 0.6       # NIR must be < 60% of reference mosaic
NIR_THRESH = 3000       # Absolute NIR reflectance threshold
WHITE_THRESH = 500      # Whiteness threshold
KERNEL_SIZE = 21        # Shadow dilation size
SIEVE_SIZE = 1000       # Minimum connected pixel size
NODATA = -9999          # Output nodata value


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def safe_reproject(source, destination, src_ds, dst_ds, resampling):
    """
    CRS-safe reprojection wrapper.
    
    Handles cases where source CRS is missing by falling back to
    destination CRS to prevent rasterio errors.
    """
    src_crs = src_ds.crs if src_ds.crs is not None else dst_ds.crs

    reproject(
        source=source,
        destination=destination,
        src_transform=src_ds.transform,
        src_crs=src_crs,
        dst_transform=dst_ds.transform,
        dst_crs=dst_ds.crs,
        resampling=resampling
    )


def log_failed_id(udm_path, error_msg):
    """
    Append failed UDM file and error message to CSV log.
    Creates header if file does not exist.
    """
    row = [os.path.basename(udm_path), str(error_msg)]
    file_exists = os.path.isfile(ERROR_LOG_PATH)

    with open(ERROR_LOG_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["udm_basename", "error_message"])
        writer.writerow(row)

def extract_scene_key(path):
    base = os.path.basename(path)
    return base.split("PLANET")[0].rstrip("_")

nir_index = {}
white_index = {}

for f in glob.glob(os.path.join(BASE_DIR, "*PLANET_BOA.bsq")):
    key = extract_scene_key(f)
    nir_index[key] = f

for f in glob.glob(os.path.join(WHITENESS_DIR, "*PLANET*white.tif")):
    key = extract_scene_key(f)
    white_index[key] = f


# ============================================================
# MAIN PROCESSING FUNCTION
# ============================================================

def process_shadow(udm_path):
    """
    Process a single UDM2 file:
        - Reproject bands
        - Create cloud buffer
        - Detect shadows
        - Generate improved clear mask
        - Write 11-band output
    """

    basename = os.path.basename(udm_path)
    print(f" → Processing: {basename}")

    # Define output filename
    out_basename = basename.replace("_udm2_mask.tif", "_udm2_buffer.tif")
    output_file = os.path.join(OUT_DIR, out_basename)

    # --------------------------------------------------------
    # Determine reference mosaic based on acquisition year
    # --------------------------------------------------------

    date_token = basename.split("_")[0]  # Example: 20251122
    year = date_token[:4]

    if year not in REF_BY_YEAR:
        print(f" ⚠️  No reference mosaic for year {year}; skipping")
        return None

    ref_path = REF_BY_YEAR[year]
    print(f"   Using reference mosaic for {year}")

    # --------------------------------------------------------
    # Locate matching NIR and whiteness files
    # --------------------------------------------------------

    key = extract_scene_key(basename)

    nir_file = nir_index.get(key)
    white_file = white_index.get(key)

    if nir_file is None or white_file is None:
        print(f" ❌ Missing match for key: {key}")
        log_failed_id(udm_path, f"Missing NIR or whiteness for key {key}")
        return None


    try:
        # ----------------------------------------------------
        # Load target mask grid
        # ----------------------------------------------------

        with rasterio.open(MASK_PATH) as mask_ds:
            mask_profile = mask_ds.profile
            mask_shape = mask_ds.shape

            # ------------------------------------------------
            # Reproject all 8 UDM bands
            # ------------------------------------------------

            with rasterio.open(udm_path) as udm_ds:
                orig_udm_bands = udm_ds.count
                udm_data = np.full(
                    (orig_udm_bands, mask_shape[0], mask_shape[1]),
                    NODATA,
                    dtype=np.int16
                )

                for i in range(8):
                    band_data = udm_ds.read(i+1, masked=True).filled(NODATA)
                    safe_reproject(
                        band_data,
                        udm_data[i],
                        udm_ds,
                        mask_ds,
                        Resampling.nearest
                    )

                # ------------------------------------------------
                # Cloud Buffer (dilate band 6)
                # ------------------------------------------------

                cloud_raw = udm_data[5].copy()
                cloud_buffer = maximum_filter(
                    cloud_raw,
                    size=31,
                    mode='constant',
                    cval=NODATA
                )
                cloud_buffer = np.round(cloud_buffer).astype(np.int16)

            # ----------------------------------------------------
            # Shadow Detection
            # ----------------------------------------------------

            with rasterio.open(nir_file) as nir_ds, \
                rasterio.open(white_file) as white_ds, \
                rasterio.open(ref_path) as ref_ds:

                    nir_res = np.full(mask_shape, NODATA, np.int16)
                    white_res = np.full(mask_shape, NODATA, np.int16)

                    # Resample to mask grid
                    safe_reproject(rasterio.band(nir_ds, 4), nir_res,
                                   nir_ds, mask_ds, Resampling.bilinear)
                    safe_reproject(rasterio.band(white_ds, 1), white_res,
                                   white_ds, mask_ds, Resampling.bilinear)

                    ref_nir = ref_ds.read(4).astype(np.int16)

                    valid = (nir_res != NODATA) & (white_res != NODATA)

                    shadow_mask = np.zeros(mask_shape, np.int16)

                    if np.any(valid):
                        shadow_mask[valid] = (
                            (nir_res[valid] < ref_nir[valid] * DARK_FACTOR) &
                            (nir_res[valid] < NIR_THRESH) &
                            (white_res[valid] < WHITE_THRESH)
                        ).astype(np.int16)

                    # add the original shadow mask to it 
                    shadow_ori = udm_data[2]
                    shadow_new = (
                        (nir_res != 0) &
                        (shadow_mask == 1) |
                        (shadow_ori == 1) 
                    ).astype(np.int16)


                    # Remove small objects
                    shadow_sieved = sieve(shadow_new, size=SIEVE_SIZE)

                    # Dilate shadow
                    shadow_dilated = maximum_filter(
                        shadow_sieved,
                        size=KERNEL_SIZE,
                        mode='constant',
                        cval=NODATA
                    )

                    shadow_buffer = np.full(mask_shape, NODATA, dtype=np.int16)
                    shadow_buffer[valid] = (shadow_dilated > 0)[valid]
                    shadow_buffer[~valid] = NODATA

            # ----------------------------------------------------
            # Improved Clear Mask
            # ----------------------------------------------------

            clear = udm_data[0]
            cloud = cloud_buffer
            shadow = shadow_buffer

            improved_clear = (
                (clear == 1) &
                (cloud == 0) &
                (shadow == 0)
            ).astype(np.int16)

            improved_clear_sieved = sieve(improved_clear, size=SIEVE_SIZE)

            # ----------------------------------------------------
            # Stack Final 11 Bands
            # ----------------------------------------------------

            output_data = np.concatenate([
                udm_data,
                cloud_buffer[None,:,:],
                shadow_buffer[None,:,:],
                improved_clear_sieved[None,:,:]
            ], axis=0)

            # ----------------------------------------------------
            # Write Output GeoTIFF
            # ----------------------------------------------------

            output_profile = mask_profile.copy()
            output_profile.update({
                'count': 11,
                'dtype': rasterio.int16,
                'nodata': NODATA,
                'compress': 'lzw'
            })

            with rasterio.open(output_file, 'w', **output_profile) as dst:
                dst.write(output_data)

                band_names = [
                    'clear', 'snow', 'shadow', 'light_haze', 'heavy_haze',
                    'cloud', 'confidence', 'udm2_unusable',
                    'cloud_buffer', 'shadow_buffer', 'new_clear'
                ]

                for idx, name in enumerate(band_names, start=1):
                    dst.set_band_description(idx, name)

            print(f" ✓ Saved: {os.path.basename(output_file)}")
            return output_file

    except Exception as e:
        print(f" ❌ Error processing {basename}: {e}")
        log_failed_id(udm_path, str(e))
        return None


# ============================================================
# PARALLEL EXECUTION
# ============================================================

if __name__ == "__main__":

    # Collect all UDM2 mask files
    udm_files = sorted(
        glob.glob(os.path.join(UDM_DIR, "*_udm2_mask.tif"))
    )

    print(f"🚀 Processing {len(udm_files)} UDM files")

    # Parallel processing
    with Pool(processes=4) as pool:
        results = pool.map(process_shadow, udm_files)

    completed = [r for r in results if r is not None]
    failed = [r for r in results if r is None]

    print(f"🎉 Completed {len(completed)}/{len(udm_files)} files!")
    print(f"❌ Failed {len(failed)} files (see {ERROR_LOG_PATH})")
nir_file