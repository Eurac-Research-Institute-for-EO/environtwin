import os
import subprocess
from pathlib import Path

# =============================================================================
# CONFIG
# =============================================================================

coreg_folder = Path('/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/coregistered/')
source_folder = Path('/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard/')

log_file = coreg_folder / "bsq_repair.log"


# =============================================================================
# HELPERS
# =============================================================================

def extract_prefix(filepath):
    name = filepath.stem
    if "_PLANET" not in name:
        return None
    return name.split("_PLANET")[0]


def find_hdr(bsq_fp):
    """Check both possible ENVI header naming conventions."""
    candidates = [
        bsq_fp.with_suffix('.hdr'),
        Path(str(bsq_fp) + '.hdr')
    ]
    for c in candidates:
        if c.exists() and c.stat().st_size > 0:
            return c
    return None


def find_source_tif(prefix):
    """Find matching original BOA tif."""
    matches = list(source_folder.rglob(f"{prefix}_PLANET*_BOA.tif"))
    return matches[0] if matches else None


def log(msg):
    with open(log_file, "a") as f:
        f.write(msg + "\n")
    print(msg)


# =============================================================================
# REPAIR FUNCTION
# =============================================================================

def repair_bsq(bsq_fp):
    prefix = extract_prefix(bsq_fp)
    if prefix is None:
        log(f"SKIP (no prefix): {bsq_fp.name}")
        return

    hdr = find_hdr(bsq_fp)

    if hdr:
        log(f"OK: {bsq_fp.name}")
        return

    log(f"⚠️ BROKEN HDR: {bsq_fp.name}")

    src = find_source_tif(prefix)
    if not src:
        log(f"❌ NO SOURCE TIF: {prefix}")
        return

    try:
        # Remove broken files first
        bsq_fp.unlink(missing_ok=True)
        for h in [bsq_fp.with_suffix('.hdr'), Path(str(bsq_fp)+'.hdr')]:
            h.unlink(missing_ok=True)

        # Recreate BSQ + HDR
        subprocess.run([
            'gdal_translate',
            '-of', 'ENVI',
            '-co', 'INTERLEAVE=BSQ',
            str(src),
            str(bsq_fp)
        ], check=True, capture_output=True)

        # Validate again
        hdr_new = find_hdr(bsq_fp)
        if hdr_new:
            log(f"✅ REPAIRED: {bsq_fp.name}")
        else:
            log(f"❌ FAILED REPAIR (no hdr): {bsq_fp.name}")

    except Exception as e:
        log(f"❌ ERROR repairing {bsq_fp.name}: {e}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    bsq_files = list(coreg_folder.rglob("*_PLANET_BOA.bsq"))

    print(f"Found {len(bsq_files)} BSQ files")

    for bsq in bsq_files:
        repair_bsq(bsq)

    print("Repair finished.")


if __name__ == "__main__":
    main()