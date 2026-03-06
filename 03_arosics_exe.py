#!/usr/bin/env python3
"""
AROSICS Local Co-Registration Pipeline
---------------------------------------

This script performs local co-registration of Planet BOA images
against a fixed reference image using AROSICS (COREG_LOCAL).

Workflow per image:
    1. Clean broken GDAL statistics metadata (if present)
    2. Perform local co-registration
    3. Export:
          - Coregistered image (BSQ format)
          - Tie point / shift table (CSV)
    4. If AROSICS fails:
          - Write raw BSQ fallback copy
          - Log error

Parallel processing is implemented via ProcessPoolExecutor.
"""

import os
import subprocess
from concurrent.futures import ProcessPoolExecutor
from arosics import COREG_LOCAL


# ============================================================
# PATH CONFIGURATION
# ============================================================

# Reference image used for all co-registration
im_reference = '/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard/20250812_103214_91_251c_PLANET_PSB.SD_BOA.tif'

# Folder containing target BOA images
target_folder = '/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard/'

# Output directory for coregistered products
output_folder = "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/coregistered/"

os.makedirs(output_folder, exist_ok=True)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def extract_prefix(filepath):
    """
    Extract unique scene prefix from Planet filename.

    Example:
        20250812_103214_91_251c_PLANET_PSB.SD_BOA.tif
        → 20250812_103214_91_251c
    """
    filename = os.path.basename(filepath)
    stem = os.path.splitext(filename)[0]

    # Ensure filename contains expected Planet naming pattern
    if "_PLANET" not in stem:
        return None

    return stem.split("_PLANET")[0]


def clean_metadata_inplace(im_target):
    """
    Remove potentially broken GDAL statistics metadata in-place.

    This avoids AROSICS failures caused by corrupted
    STATISTICS_* tags.
    """
    try:
        subprocess.run(
            ['gdal_edit.py', '-unsetstats', im_target],
            capture_output=True,
            check=True
        )
        return True
    except subprocess.CalledProcessError:
        return False


def write_bsq_copy(im_target, out_boa):
    """
    Fallback mechanism:
    If AROSICS fails, export original image as BSQ using GDAL.

    Output format:
        ENVI + BSQ interleave
    """
    try:
        subprocess.run([
            'gdal_translate',
            '-of', 'ENVI',              # ENVI supports BSQ
            '-co', 'INTERLEAVE=BSQ',    # Force BSQ format
            im_target,
            out_boa
        ], check=True, capture_output=True)
        return True
    except subprocess.CalledProcessError:
        return False


# ============================================================
# CORE PROCESSING FUNCTION
# ============================================================

def process_image(img):
    """
    Process a single BOA image:

        - Validate filename
        - Clean metadata
        - Run AROSICS local co-registration
        - Export corrected image + tie point table
        - If failure → write fallback BSQ
    """

    print(f"ENTRY: {img}")

    im_target = os.path.join(target_folder, img)
    prefix = extract_prefix(img)

    # --------------------------------------------------------
    # Filter unwanted files
    # --------------------------------------------------------

    if prefix is None or not img.endswith("BOA.tif"):
        print(f"FILTERED: {img}")
        return f"SKIPPED {img}"

    # Define outputs
    out_boa = os.path.join(output_folder, f"{prefix}_PLANET_BOA.bsq")
    out_csv = os.path.join(output_folder, f"{prefix}_PLANET_BOA_table.csv")
    log_path = os.path.join(output_folder, "errors.log")
    processed_log_path = os.path.join(output_folder, "processed.txt")

    # Skip already processed scenes
    if os.path.exists(out_boa) and os.path.exists(out_csv):
        with open(processed_log_path, "a") as f:
            f.write(f"{img}\n")

        print(f"OK_PROCESSED: {img}")
        return out_boa

    try:
        # ----------------------------------------------------
        # Step 1: Clean metadata
        # ----------------------------------------------------

        print(f"Cleaning metadata: {img}")
        clean_metadata_inplace(im_target)

        # ----------------------------------------------------
        # Step 2: Run AROSICS local coregistration
        # ----------------------------------------------------

        CR = COREG_LOCAL(
            im_reference,          # Reference image
            im_target,             # Target image
            window_size=(256, 256),  # Matching window size
            grid_res=150,            # Tie point grid resolution
            path_out=out_boa         # Output path
        )

        # Estimate spatial shifts
        CR.calculate_spatial_shifts()

        # Apply correction and write output
        CR.correct_shifts()

        # Export tie point table
        CR.CoRegPoints_table.to_csv(out_csv)

        # ----------------------------------------------------
        # Verify successful writing
        # ----------------------------------------------------

        if os.path.exists(out_boa) and os.path.exists(out_csv):
            with open(processed_log_path, "a") as f:
                f.write(f"{img}\n")

            print(f" ✓ Saved: {os.path.basename(out_csv)} and {os.path.basename(out_boa)}")
            return out_boa
        else:
            print(f"WRITING_FAILED: {img}")
            return None

    except Exception as e:
        # ----------------------------------------------------
        # Fallback handling if AROSICS fails
        # ----------------------------------------------------

        print(f"AROSICS FAILED: {img} → Writing raw BSQ")

        fallback_written = write_bsq_copy(im_target, out_boa)

        if fallback_written:
            with open(log_path, "a") as f:
                f.write(f"AROSICS_FAILED_BUT_BSQ_WRITTEN {img} → {str(e)}\n")

            with open(processed_log_path, "a") as f:
                f.write(f"{img}\n")

            return f"FALLBACK_BSQ_WRITTEN {img}"

        else:
            with open(log_path, "a") as f:
                f.write(f"TOTAL_FAIL {img} → {str(e)}\n")

            return f"TOTAL_FAIL {img}"


# ============================================================
# PARALLEL EXECUTION
# ============================================================

if __name__ == "__main__":

    # Collect only BOA images
    images = [
        f for f in os.listdir(target_folder)
        if f.endswith("BOA.tif")
    ]

    # Parallel execution (adjust workers depending on RAM/CPU)
    with ProcessPoolExecutor(max_workers=2) as exe:
        results = exe.map(process_image, images)

    # Print final status
    for r in results:
        print(r)