from osgeo import gdal
import os
from multiprocessing import Pool, cpu_count

# Root directory with subfolders
folder = "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw"

# Expected band names
band_names = ["blue", "green", "red", "nir"]

def process_raster(filepath):
    try:
        filename = os.path.basename(filepath)
        ds = gdal.Open(filepath, gdal.GA_Update)
        if ds is None:
            return f"❌ Failed to open {filename}"

        band_count = ds.RasterCount
        if band_count != len(band_names):
            return f"⚠ {filename} has {band_count} bands (expected {len(band_names)}) — skipping"

        # Rename bands
        for i, name in enumerate(band_names):
            band = ds.GetRasterBand(i + 1)
            if band:
                band.SetDescription(name)

        # Verify rename
        for i, name in enumerate(band_names):
            if ds.GetRasterBand(i + 1).GetDescription() != name:
                return f"❌ {filename}: Band {i+1} mismatch after renaming"

        ds = None  # Close dataset
        return f"✅ {filename}: Bands renamed to {', '.join(band_names)}"

    except Exception as e:
        return f"❌ Error processing {filepath}: {e}"

def find_rasters(root_folder):
    raster_files = []
    for root, dirs, files in os.walk(root_folder):
        for filename in files:
            if filename.endswith("_PLANET_BOA.tif"):
                raster_files.append(os.path.join(root, filename))
    return raster_files

if __name__ == "__main__":
    raster_files = find_rasters(folder)
    print(f"🔍 Found {len(raster_files)} rasters to process")

    if not raster_files:
        print("⚠ No matching rasters found. Exiting.")
        exit()

    # Use all available CPU cores (or limit with processes=N)
    with Pool(processes=cpu_count()) as pool:
        for result in pool.imap_unordered(process_raster, raster_files):
            print(result)

