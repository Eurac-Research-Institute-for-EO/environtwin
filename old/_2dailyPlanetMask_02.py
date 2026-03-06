import os
import glob
import numpy as np
import rasterio
from collections import defaultdict
from multiprocessing import Pool, cpu_count

# Input folders - TESTING: Using test data
input_dirs = [
    "mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard"
]

# For production, uncomment these:
# input_dirs = [
#     "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X-001_Y-001",
#     "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X-002_Y-002",
#     "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0004_Y0002",
#     "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0006_Y0000",
#     "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0006_Y0001",
#     "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0007_Y-001",
#     "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0007_Y0000",
#     "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0008_Y-001",
#     "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0008_Y0000",
#     "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0009_Y-001",
# ]

output_root = "/mnt/CEPH_PROJECTS/Environtwin/FORCE/test/daily"
# For production: output_root = "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_daily/02"

nodata_val = -9999

# Blue band threshold for cloud filtering (reflectance scaled 0-10000)
# Pixels with blue band > threshold are masked as potential clouds/bright targets
BLUE_BAND_THRESHOLD = 900  # ~9% reflectance

# Log files - save in current directory
log_missing_pairs = "missing_pairs_02.log"
log_invalid_mosaics = "invalid_mosaic_02.log"
log_profile_mismatch = "profile_mismatch_02.log"
log_fully_filtered = "fully_filtered_02.log"

os.makedirs(output_root, exist_ok=True)


def append_log(path, text):
    with open(path, "a") as f:
        f.write(text + "\n")


def validate_spatial_alignment(boa_path, udm_path):
    """
    Validates that BOA and UDM2 files are spatially aligned.
    Returns (is_valid, error_message)
    """
    with rasterio.open(boa_path) as boa, rasterio.open(udm_path) as udm:
        # Check dimensions
        if (boa.width, boa.height) != (udm.width, udm.height):
            return False, f"Dimension mismatch: BOA={boa.width}x{boa.height}, UDM={udm.width}x{udm.height}"

        # Check CRS
        if boa.crs != udm.crs:
            return False, f"CRS mismatch: BOA={boa.crs}, UDM={udm.crs}"

        # Check geotransform
        if boa.transform != udm.transform:
            return False, f"Transform mismatch: BOA={boa.transform}, UDM={udm.transform}"

        # Check bounds
        boa_bounds = boa.bounds
        udm_bounds = udm.bounds
        tolerance = 0.001
        if not all(abs(b - u) < tolerance for b, u in zip(boa_bounds, udm_bounds)):
            return False, f"Bounds mismatch: BOA={boa_bounds}, UDM={udm_bounds}"

    return True, None


def extract_prefix(filepath):
    """Extract date_time_id prefix from Planet filename"""
    filename = os.path.basename(filepath)
    stem = os.path.splitext(filename)[0]
    if "_PLANET" not in stem:
        return None, None
    prefix = stem.split("_PLANET")[0]
    date = prefix[:8]
    return prefix, date


def find_pairs():
    """Find BOA and UDM2 file pairs"""
    boa_files = {}
    udm_files = {}

    for folder in input_dirs:
        for f in glob.glob(os.path.join(folder, "*_PLANET_*_BOA.tif")):
            prefix, date = extract_prefix(f)
            if prefix and date:
                boa_files[prefix] = (f, date)

        for f in glob.glob(os.path.join(folder, "*_PLANET_udm2_mask.tif")):
            prefix, date = extract_prefix(f)
            if prefix and date:
                udm_files[prefix] = f

    # Log missing pairs
    for prefix in boa_files:
        if prefix not in udm_files:
            append_log(log_missing_pairs, f"Missing UDM2 for {prefix}")

    # Group by date and subfolder
    paired = defaultdict(list)
    for prefix in boa_files:
        if prefix in udm_files:
            boa_fp, date = boa_files[prefix]
            subfolder = os.path.basename(os.path.dirname(boa_fp))
            paired[(date, subfolder)].append((boa_fp, udm_files[prefix]))

    return paired


def process_date_subfolder(args):
    date, subfolder, file_pairs = args
    n = len(file_pairs)

    print(f"\nProcessing {date}/{subfolder} with {n} images")

    if n == 0:
        append_log(log_invalid_mosaics, f"No images for {date}/{subfolder}")
        return

    out_dir = os.path.join(output_root, subfolder)
    os.makedirs(out_dir, exist_ok=True)

    out_boa = os.path.join(out_dir, f"{date}_PLANET_BOA.tif")
    out_cnt = os.path.join(out_dir, f"{date}_PLANET_count.tif")
    out_mask = os.path.join(out_dir, f"{date}_PLANET_combined_mask.tif")

    if os.path.exists(out_boa):
        print("Skipping (already exists)")
        return

    # Open reference BOA and UDM
    ref_boa_fp, ref_udm_fp = file_pairs[0]

    # Validate reference pair alignment
    is_valid, error_msg = validate_spatial_alignment(ref_boa_fp, ref_udm_fp)
    if not is_valid:
        msg = f"{date}/{subfolder} - Reference pair alignment failed: {error_msg}"
        append_log(log_profile_mismatch, msg)
        print(f"ERROR: {msg}")
        return

    with rasterio.open(ref_boa_fp) as ref:
        height, width = ref.height, ref.width
        bands = ref.count
        ref_crs = ref.crs
        ref_transform = ref.transform
        band_descriptions = ref.descriptions
        boa_profile = ref.profile.copy()
        boa_profile.update(dtype="int16", nodata=nodata_val, compress="deflate")

    # Get UDM band count for combined mask
    with rasterio.open(ref_udm_fp) as udm_ref:
        udm_bands = udm_ref.count

    cnt_profile = boa_profile.copy()
    cnt_profile.update(count=1, dtype="uint16", nodata=0)

    # Combined mask profile (8 bands like UDM2, but int16 to support -9999)
    mask_profile = boa_profile.copy()
    mask_profile.update(count=udm_bands, dtype="int16", nodata=-9999, compress="deflate")

    # Allocate arrays
    sum_boa = np.zeros((bands, height, width), dtype=np.float64)
    cnt_boa = np.zeros((bands, height, width), dtype=np.uint32)
    cnt_img = np.zeros((height, width), dtype=np.uint16)
    sum_mask = np.zeros((udm_bands, height, width), dtype=np.uint32)  # Accumulate mask values

    # ---- Process each pair ----
    for boa_fp, udm_fp in file_pairs:
        # Validate each pair's spatial alignment
        is_valid, error_msg = validate_spatial_alignment(boa_fp, udm_fp)
        if not is_valid:
            msg = f"{date}/{subfolder} - Skipping pair {os.path.basename(boa_fp)}: {error_msg}"
            append_log(log_profile_mismatch, msg)
            print(f"WARNING: {msg}")
            continue

        # Validate against reference dimensions/CRS
        with rasterio.open(boa_fp) as boa, rasterio.open(udm_fp) as udm:
            if (boa.width, boa.height) != (width, height):
                msg = f"{date}/{subfolder} - Skipping {os.path.basename(boa_fp)}: dimension mismatch"
                append_log(log_profile_mismatch, msg)
                print(f"WARNING: {msg}")
                continue

            if boa.crs != ref_crs:
                msg = f"{date}/{subfolder} - Skipping {os.path.basename(boa_fp)}: CRS mismatch"
                append_log(log_profile_mismatch, msg)
                print(f"WARNING: {msg}")
                continue

            if boa.transform != ref_transform:
                msg = f"{date}/{subfolder} - Skipping {os.path.basename(boa_fp)}: transform mismatch"
                append_log(log_profile_mismatch, msg)
                print(f"WARNING: {msg}")
                continue

            # Block-based processing
            for _, win in boa.block_windows(1):
                boa_win = boa.read(window=win)
                udm_win = udm.read(window=win)

                # Window slices
                r0, c0 = win.row_off, win.col_off
                rr = slice(r0, r0 + win.height)
                cc = slice(c0, c0 + win.width)

                # Determine pixels with UDM data
                has_udm_data = np.ones(udm_win[0].shape, dtype=bool)
                if udm.nodata is not None:
                    has_udm_data &= np.all(udm_win != udm.nodata, axis=0)

                # Determine pixels with BOA data
                has_boa_data = np.ones(boa_win[0].shape, dtype=bool)
                if boa.nodata is not None:
                    has_boa_data &= np.all(boa_win != boa.nodata, axis=0)

                # UDM clear flag (band 1 == 1)
                udm_clear = (udm_win[0] == 1)

                # BLUE BAND FILTER: Mask out bright pixels (potential clouds)
                # This is the key feature of _02.py
                blue_band_valid = (boa_win[0] <= BLUE_BAND_THRESHOLD)

                # VALID pixel rule: has BOA data AND UDM clear AND blue band not too bright
                valid_for_boa = has_boa_data & has_udm_data & udm_clear & blue_band_valid

                # Accumulate BOA mean (only for valid pixels)
                for b in range(bands):
                    sum_boa[b, rr, cc] += np.where(valid_for_boa, boa_win[b], 0)
                    cnt_boa[b, rr, cc] += valid_for_boa.astype(np.uint32)

                # Count contributing images
                cnt_img[rr, cc] += valid_for_boa.astype(np.uint16)

                # Accumulate combined mask (all UDM bands, but filtered by blue band)
                # For pixels that pass the blue filter, accumulate original UDM values
                # For pixels that fail, they remain 0 (will become nodata in output)
                for b in range(udm_bands):
                    # Only accumulate UDM where pixel is valid (passes all filters)
                    sum_mask[b, rr, cc] += np.where(valid_for_boa, udm_win[b], 0).astype(np.uint32)

    # ---- Build outputs ----

    # BOA mean mosaic
    with np.errstate(divide='ignore', invalid='ignore'):
        boa_mean = np.where(
            cnt_boa > 0,
            sum_boa / cnt_boa,
            nodata_val
        ).astype(np.int16)

    # Combined mask (average UDM values for valid pixels, nodata for invalid)
    # Note: For pixels that passed the filter, we average the UDM values
    # For pixels that failed, they remain 0 and we set to nodata
    with np.errstate(divide='ignore', invalid='ignore'):
        mask_mean = np.where(
            cnt_boa[0] > 0,  # Use count from BOA (same valid pixels)
            sum_mask / cnt_boa[0],  # Average mask values
            -9999  # nodata
        ).astype(np.int16)

    # ---- Check if all pixels were filtered ----
    total_valid_pixels = (cnt_boa[0] > 0).sum()

    if total_valid_pixels == 0:
        msg = f"{date}/{subfolder} - All pixels filtered by blue band threshold (>{BLUE_BAND_THRESHOLD}). No output created."
        append_log(log_fully_filtered, msg)
        print(f"⚠️  WARNING: {msg}")
        return

    # ---- Write output ----
    if n == 1:
        print("Single image: applying blue band filter and copying")
        # Even for single image, apply the blue band filter
        with rasterio.open(ref_boa_fp) as src_boa, rasterio.open(ref_udm_fp) as src_udm:
            boa_data = src_boa.read()
            udm_data = src_udm.read()

            # Apply filters
            has_boa = np.all(boa_data != src_boa.nodata, axis=0) if src_boa.nodata is not None else np.ones(boa_data[0].shape, dtype=bool)
            has_udm = np.all(udm_data != src_udm.nodata, axis=0) if src_udm.nodata is not None else np.ones(udm_data[0].shape, dtype=bool)
            is_clear = (udm_data[0] == 1)
            blue_ok = (boa_data[0] <= BLUE_BAND_THRESHOLD)
            valid = has_boa & has_udm & is_clear & blue_ok

            # Mask invalid pixels in BOA
            for b in range(bands):
                boa_data[b] = np.where(valid, boa_data[b], nodata_val)

            # Mask invalid pixels in UDM (combined mask)
            mask_data = udm_data.copy()
            for b in range(udm_bands):
                mask_data[b] = np.where(valid, udm_data[b], -9999)

            with rasterio.open(out_boa, "w", **boa_profile) as dst:
                dst.write(boa_data)
                for i, desc in enumerate(band_descriptions, start=1):
                    if desc:
                        dst.set_band_description(i, desc)

        # Count file for single image
        count_data = valid.astype(np.uint16)
        with rasterio.open(out_cnt, "w", **cnt_profile) as dst:
            dst.write(count_data, 1)
            dst.set_band_description(1, "count")

        # Combined mask file for single image
        with rasterio.open(out_mask, "w", **mask_profile) as dst:
            dst.write(mask_data)
            # Set band descriptions (same as original UDM2)
            with rasterio.open(ref_udm_fp) as udm_ref:
                udm_descriptions = udm_ref.descriptions
                if udm_descriptions:
                    for i, desc in enumerate(udm_descriptions, start=1):
                        if desc:
                            dst.set_band_description(i, desc)
    else:
        with rasterio.open(out_boa, "w", **boa_profile) as dst:
            dst.write(boa_mean)
            # Set band descriptions
            for i, desc in enumerate(band_descriptions, start=1):
                if desc:
                    dst.set_band_description(i, desc)

        with rasterio.open(out_cnt, "w", **cnt_profile) as dst:
            dst.write(cnt_img, 1)
            dst.set_band_description(1, "count")

        # Combined mask file for multi-image mosaic
        with rasterio.open(out_mask, "w", **mask_profile) as dst:
            dst.write(mask_mean)
            # Set band descriptions (same as original UDM2)
            with rasterio.open(ref_udm_fp) as udm_ref:
                udm_descriptions = udm_ref.descriptions
                if udm_descriptions:
                    for i, desc in enumerate(udm_descriptions, start=1):
                        if desc:
                            dst.set_band_description(i, desc)

    print(f"Finished {date}/{subfolder}")


def main():
    pairs = find_pairs()
    args_list = [(d, sf, p) for (d, sf), p in pairs.items()]

    print(f"Found {len(args_list)} date groups.")
    for d, sf, p in args_list:
        print(f"{d} / {sf}: {len(p)} pairs")

    if not args_list:
        print("No data found.")
        return

    n_workers = min(cpu_count(), len(args_list))
    with Pool(n_workers) as pool:
        pool.map(process_date_subfolder, args_list)


if __name__ == "__main__":
    main()
