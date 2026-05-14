#!/usr/bin/env python3

import json
import subprocess
from pathlib import Path
import zipfile
import shutil


# --- Configuration ---
BASE_ROOT = Path("/mnt/CEPH_PROJECTS/Environtwin/PLANET")
OUT_ROOT = Path("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw")

AOI_TO_MASK = {
    #1: "/mnt/CEPH_PROJECTS/Environtwin/gis/masks/AW_mask.tif",
    #2: "/mnt/CEPH_PROJECTS/Environtwin/gis/masks/FSP_mask.tif",
    #3: "/mnt/CEPH_PROJECTS/Environtwin/gis/masks/R_mask.tif",
    #4: "/mnt/CEPH_PROJECTS/Environtwin/gis/masks/PG1_mask.tif",
    #5: "/mnt/CEPH_PROJECTS/Environtwin/gis/masks/SA_mask.tif",
    #6: "/mnt/CEPH_PROJECTS/Environtwin/gis/masks/TH_mask.tif",
    #7: "/mnt/CEPH_PROJECTS/Environtwin/gis/masks/HS_mask.tif",
    #8: "/mnt/CEPH_PROJECTS/Environtwin/gis/masks/TG_mask.tif",
    #9: "/mnt/CEPH_PROJECTS/Environtwin/gis/masks/PG2_mask.tif",
    10: "/mnt/CEPH_PROJECTS/Environtwin/gis/masks/MH_mask.tif"
}

AOI_TO_SITE = {
    #1: "AW", 5: "SA", 
    10: "MH"
    #2: "FSP", 3: "R",
    #4: "PG1",  6: "TH", 7: "HS", 8: "TG", 9: "PG2", 
}


def decide_output_folder(json_path, out_base):
    """Read metadata and decide if output goes to 'test' or 'standard'."""
    if not json_path.exists():
        raise FileNotFoundError(f"Metadata not found: {json_path}")

    with open(json_path) as f:
        meta = json.load(f)

    raw_gcp = meta.get("properties", {}).get("ground_control", None)
    if raw_gcp is None:
        gcp = ""
    else:
        gcp = str(raw_gcp).strip().lower()

    if gcp == "true":
        return out_base / "standard"
    else:
        return out_base / "test"


def get_mask_extent(mask_path):
    cmd = ["gdalinfo", "-json", str(mask_path)]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    data = json.loads(result.stdout)

    cc = data["cornerCoordinates"]
    xmin, ymin = cc["lowerLeft"]
    xmax, ymax = cc["upperRight"]

    gt = data["geoTransform"]
    xres = gt[1]
    yres = abs(gt[5])
    epsg = data["stac"]["proj:epsg"]

    return xmin, ymin, xmax, ymax, xres, yres, epsg


def clip_raster_to_mask(tiff_path, mask_extent, out_path):
    xmin, ymin, xmax, ymax, xres, yres, epsg = mask_extent

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "gdalwarp",
        "-overwrite",
        "-t_srs", f"EPSG:{epsg}",
        "-te", str(xmin), str(ymin), str(xmax), str(ymax),
        "-te_srs", f"EPSG:{epsg}",
        "-tr", str(xres), str(yres),
        "-tap",
        #"-dstnodata", "-9999",
        str(tiff_path),
        str(out_path),
    ]
    subprocess.run(cmd, check=True, capture_output=True)


def process_aoi_batches(in_dir, mask_path, out_base):
    """
    Extracts all ZIPs in a directory, clips TIFFs, handles 8b/4b + UDM,
    and copies metadata into test/standard based on GCP.
    """
    batch_tmp = Path("/tmp/planet_processing")
    batch_tmp.mkdir(parents=True, exist_ok=True)

    # 1. Read mask geometry once
    mask_extent = get_mask_extent(mask_path)
    xmin, ymin, xmax, ymax, xres, yres, epsg = mask_extent

    skipped_log = out_base / "skipped.log"
    processed_log = out_base / "processed.log"
    missing_log = out_base / "missing.log"

    for p in [skipped_log, processed_log, missing_log]:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch()

    for zip_path in in_dir.glob("batch_*.zip"):
        print(f"Processing ZIP: {zip_path.name}")

        extract_dir = batch_tmp / zip_path.stem
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)

        tiffs = list(extract_dir.rglob("*3B_AnalyticMS_SR*_harmonized_clip.tif"))
        for tiff in tiffs:
            base = tiff.stem
            base = base.replace("_3B_AnalyticMS_SR_8b_harmonized_clip", "").replace("_3B_AnalyticMS_SR_harmonized_clip", "")

            json_path = tiff.parent / f"{base}_metadata.json"
            if not json_path.exists():
                with open(missing_log, "a") as f:
                    f.write(f"{base}\n")
                continue

            # 1. Decide test/standard and output dir
            try:
                dest_dir = decide_output_folder(json_path, out_base)
                dest_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                print(f"Metadata read failed for {base}: {e}")
                with open(missing_log, "a") as f:
                    f.write(f"{base}\n")
                continue

            # 1a. Copy metadata JSON
            out_json = dest_dir / f"{base}_metadata.json"
            shutil.copy(json_path, out_json)

            # 1b. Read instrument from metadata
            with open(json_path) as f:
                meta = json.load(f)
            inst = meta.get("properties", {}).get("instrument", "UNKNOWN")

            # 2. Warp to mask extent (temporary warping)
            tmp_warp = tiff.parent / f"{base}_warp.tif"
            tmp_warp.unlink(missing_ok=True)

            try:
                clip_raster_to_mask(tiff, mask_extent, tmp_warp)
            except subprocess.CalledProcessError as e:
                print(f"Failed to warp {base}")
                with open(skipped_log, "a") as f:
                    f.write(f"{base}\n")
                tmp_warp.unlink(missing_ok=True)
                continue

            # 3. 8-band vs 4-band handling
            band_type = "8b" if "8b_harmonized_clip" in tiff.name else "4b"

            out_tiff = dest_dir / f"{base}_PLANET_{inst}_BOA.tif"

            if band_type == "8b":
                cmd_trans = [
                    "gdal_translate",
                    "-b", "2", "-b", "4", "-b", "6", "-b", "8",
                    str(tmp_warp),
                    str(out_tiff),
                ]
                try:
                    subprocess.run(cmd_trans, check=True, capture_output=True)
                    print(f"Created 8-band output: {out_tiff}")
                except subprocess.CalledProcessError as e:
                    print(f"Failed to subset bands for {base}")
                    with open(skipped_log, "a") as f:
                        f.write(f"{base}\n")
                    tmp_warp.unlink(missing_ok=True)
                    continue
            else:
                print(f"Processing 4-band TIFF: {base}")
                shutil.move(tmp_warp, out_tiff)
                print(f"Moved 4-band TIFF -> {out_tiff}")

            # 4. UDM warp
            udm_path = tiff.parent / f"{base}_3B_udm2_clip.tif"
            out_udm = dest_dir / f"{base}_PLANET_udm2.tif"
            if udm_path.exists():
                cmd_udm = [
                    "gdalwarp",
                    "-overwrite",
                    "-t_srs", f"EPSG:{epsg}",
                    "-te", str(xmin), str(ymin), str(xmax), str(ymax),
                    "-te_srs", f"EPSG:{epsg}",
                    "-tr", str(xres), str(yres),
                    "-tap",
                    #"-dstnodata", "-9999",
                    str(udm_path),
                    str(out_udm),
                ]
                try:
                    subprocess.run(cmd_udm, check=True, capture_output=True)
                    print(f"UDM warped -> {out_udm}")
                except subprocess.CalledProcessError as e:
                    print(f"Failed UDM warp for {base}")
            else:
                print(f"UDM not found for {base}")

            # 5. Log
            with open(processed_log, "a") as f:
                f.write(f"{base}\n")

        # 5. Clean up extraction dir
        shutil.rmtree(extract_dir)
        print(f"Completed ZIP: {zip_path.name}")


# --- Main loop ---
for aoii in range(1, 11):  # 1..10
    in_dir = BASE_ROOT / str(aoii)
    if not in_dir.is_dir():
        print(f"Skipping non‑existent AOI {aoii}: {in_dir}")
        continue

    mask_path = AOI_TO_MASK.get(aoii)
    if not mask_path or not Path(mask_path).exists():
        print(f"Missing mask for AOI {aoii}: {mask_path}")
        continue

    site_name = AOI_TO_SITE.get(aoii)
    if not site_name:
        print(f"No site name defined for AOI {aoii}")
        continue

    out_base = OUT_ROOT / site_name
    out_base.mkdir(parents=True, exist_ok=True)

    # Call the batch processor for this AOI
    print(f"Processing AOI {aoii} -> {site_name}")
    process_aoi_batches(in_dir, mask_path, out_base)

print("All AOIs processed.")