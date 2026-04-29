import os
import glob
import numpy as np
import rasterio
from collections import defaultdict
from multiprocessing import Pool, cpu_count

# For production, uncomment these:
input_dirs = [
    "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/SA/coregistered/",
]
udm_folder = '/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/SA/standard'

output_root = "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_daily/02"
nodata_val = -9999

# Log files - save in current directory
log_missing_pairs = "missing_pairs.log"
log_invalid_mosaics = "invalid_mosaic.log"
log_profile_mismatch = "profile_mismatch.log"

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

        # Check geotransform (pixel origin and size)
        if boa.transform != udm.transform:
            return False, f"Transform mismatch: BOA={boa.transform}, UDM={udm.transform}"

        # Check bounds (derived from transform, but good to verify)
        boa_bounds = boa.bounds
        udm_bounds = udm.bounds
        # Allow small floating point tolerance (1mm)
        tolerance = 0.001
        if not all(abs(b - u) < tolerance for b, u in zip(boa_bounds, udm_bounds)):
            return False, f"Bounds mismatch: BOA={boa_bounds}, UDM={udm_bounds}"

    return True, None

def extract_prefix(filepath):
    filename = os.path.basename(filepath)
    stem = os.path.splitext(filename)[0]
    if "_PLANET" not in stem:
        return None, None
    prefix = stem.split("_PLANET")[0]
    date = prefix[:8]
    return prefix, date

def find_pairs():
    boa_files = {}
    udm_files = {}

    print("Scanning for 202510* and 202511* PLANET files...")
    
    for folder in input_dirs:
        # Find BOA files matching date pattern DIRECTLY with glob
        boa_pattern = os.path.join(folder, "*_PLANET_BOA.bsq")
        for f in glob.glob(boa_pattern):
            prefix, date = extract_prefix(f)
            if prefix and date:
                boa_files[prefix] = (f, date)
                print(f"Found BOA: {os.path.basename(f)}")

    # Scan UDM files
    udm_pattern = os.path.join(udm_folder, "*_PLANET_udm2_mask.tif")
    for f in glob.glob(udm_pattern):
        prefix, date = extract_prefix(f)
        if prefix and date:
            udm_files[prefix] = f
            print(f"Found UDM: {os.path.basename(f)}")

    # log missing
    for prefix in boa_files:
        if prefix not in udm_files:
            append_log(log_missing_pairs, f"Missing UDM2 for {prefix}")

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

    out_dir = os.path.join(output_root, "SA")
    os.makedirs(out_dir, exist_ok=True)

    out_boa = os.path.join(out_dir, f"{date}_PLANET_BOA.tif")
    out_udm = os.path.join(out_dir, f"{date}_PLANET_udm2_blue_mask.tif")
    out_cnt = os.path.join(out_dir, f"{date}_PLANET_count.tif")
    
    #if os.path.exists(out_boa) and os.path.exists(out_udm):
    #   print("Skipping (already exists)")
    #   return

    ref_boa_fp, ref_udm_fp = file_pairs[0]

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
        boa_profile.update(dtype="float32", nodata=nodata_val, compress="deflate")

    with rasterio.open(ref_udm_fp) as refm:
        udm_bands = refm.count
        udm_profile = refm.profile.copy()
        udm_profile.update(dtype="int16", compress="deflate", nodata=-9999)

    cnt_profile = boa_profile.copy()
    cnt_profile.update(count=1, dtype="uint16", nodata=0)

    sum_boa = np.zeros((bands, height, width), dtype=np.float64)
    cnt_boa = np.zeros((bands, height, width), dtype=np.uint32)
    cnt_img = np.zeros((height, width), dtype=np.uint16)
    sum_udm = np.zeros((udm_bands, height, width), dtype=np.uint16)
    cnt_udm = np.zeros((height, width), dtype=np.uint16)

    for boa_fp, udm_fp in file_pairs:
        is_valid, error_msg = validate_spatial_alignment(boa_fp, udm_fp)
        if not is_valid:
            msg = f"{date}/{subfolder} - Skipping pair {os.path.basename(boa_fp)}: {error_msg}"
            append_log(log_profile_mismatch, msg)
            print(f"WARNING: {msg}")
            continue

        with rasterio.open(boa_fp) as boa, rasterio.open(udm_fp) as udm:
            if (boa.width, boa.height) != (width, height):
                msg = f"{date}/{subfolder} - Skipping {os.path.basename(boa_fp)}: dimension mismatch with reference"
                append_log(log_profile_mismatch, msg)
                print(f"WARNING: {msg}")
                continue

            if boa.crs != ref_crs:
                msg = f"{date}/{subfolder} - Skipping {os.path.basename(boa_fp)}: CRS mismatch with reference"
                append_log(log_profile_mismatch, msg)
                print(f"WARNING: {msg}")
                continue

            if boa.transform != ref_transform:
                msg = f"{date}/{subfolder} - Skipping {os.path.basename(boa_fp)}: transform mismatch with reference"
                append_log(log_profile_mismatch, msg)
                print(f"WARNING: {msg}")
                continue

            for _, win in boa.block_windows(1):
                boa_win = boa.read(window=win)
                udm_win = udm.read(window=win)

                r0, c0 = win.row_off, win.col_off
                rr = slice(r0, r0 + win.height)
                cc = slice(c0, c0 + win.width)

                # Create new mask from first BOA band: values > 900 set to 0, else 1 (assuming mask is binary 0/1)
                new_mask_from_boa = np.where(boa_win[0] > 900, 0, 1).astype(np.int16)
                
                # Combine (logical AND) original UDM first band with the new mask (element-wise multiplication)
                combined_mask_first_band = udm_win[0] * new_mask_from_boa

                bands_mask = udm_win.shape[0]
		
                # Create new mask like original one
                new_mask_data = np.empty_like(udm_win)
                new_mask_data[0] = combined_mask_first_band
                if bands_mask > 1:
                    new_mask_data[1:] = udm_win[1:]
		
                # Use combined mask to determine valid pixels
                has_udm_data = np.ones_like(new_mask_data[0], dtype=bool)
                if udm.nodata is not None:
                    has_udm_data &= (new_mask_data[0] != udm.nodata)

                has_boa_data = np.ones_like(boa_win[0], dtype=bool)
                
                # Determine pixels with BOA data
                if boa.nodata is not None:
                    has_boa_data &= np.all(boa_win != boa.nodata, axis=0)
		
                # VALID pixel rule for BOA: has BOA data AND UDM band 1 == 1 (clear)
                valid_for_boa = has_boa_data & has_udm_data & (new_mask_data[0] == 1)
		
                # Accumulate BOA mean (only for clear pixels)
                for b in range(bands):
                    sum_boa[b, rr, cc] += np.where(valid_for_boa, boa_win[b], 0)
                    cnt_boa[b, rr, cc] += valid_for_boa.astype(np.uint32)
		
                # Count contributing images (only clear pixels)
                cnt_img[rr, cc] += valid_for_boa.astype(np.uint16)

                for b in range(bands_mask):
                    sum_udm[b, rr, cc] += np.where(has_udm_data, new_mask_data[b], 0).astype(np.uint16)
                    
                # Track pixels with UDM data
                cnt_udm[rr, cc] += has_udm_data.astype(np.uint16)
                
    # ---- Build outputs ----
    with np.errstate(divide='ignore', invalid='ignore'):
        boa_mean = np.where(
            cnt_boa > 0,
            sum_boa / cnt_boa,
            nodata_val
        ).astype(np.float32)
        
    # UDM mosaic - preserve actual 0/1 values, set -9999 for nodata
    # For pixels with UDM data: take the max value seen (if any image had flag=1, output is 1)
    # For nodata pixels: set to -9999
    udm_has_data = cnt_udm > 0
    udm_final = np.zeros_like(sum_udm, dtype=np.int16)
    for b in range(udm_bands):
        udm_final[b] = np.where(
            udm_has_data,
            (sum_udm[b] > 0).astype(np.int16),
            -9999
        )

    if n == 1:
        print("Single image: copying original files")
        with rasterio.open(ref_boa_fp) as src:
            data = src.read()
            with rasterio.open(out_boa, "w", **boa_profile) as dst:
                dst.write(data)
                for i, desc in enumerate(band_descriptions, start=1):
                    if desc:
                        dst.set_band_description(i, desc)

        with rasterio.open(ref_udm_fp) as src:
            data = src.read()
            udm_descriptions = src.descriptions
            with rasterio.open(out_udm, "w", **udm_profile) as dst:
                dst.write(data)
                for i, desc in enumerate(udm_descriptions, start=1):
                    if desc:
                        dst.set_band_description(i, desc)
    else:
        with rasterio.open(out_boa, "w", **boa_profile) as dst:
            dst.write(boa_mean)
            for i, desc in enumerate(band_descriptions, start=1):
                if desc:
                    dst.set_band_description(i, desc)

        with rasterio.open(out_udm, "w", **udm_profile) as dst:
            dst.write(udm_final)
            with rasterio.open(ref_udm_fp) as src:
                udm_descriptions = src.descriptions
                for i, desc in enumerate(udm_descriptions, start=1):
                    if desc:
                        dst.set_band_description(i, desc)

    with rasterio.open(out_cnt, "w", **cnt_profile) as dst:
        dst.write(cnt_img, 1)
        dst.set_band_description(1, "count")

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

