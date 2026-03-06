#############################################################
## --- Script to rename NDVI and Blue TSS with dates --- ##
#############################################################

library(terra)

# Define base path
base_path <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/02/MH"
out_path <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/02/MH/renamed"


# List all VRT files ending with "_TSS.vrt"
tss_files <- list.files(base_path, pattern = "*._SENPLA_NDV_TSS\\.bsq$", full.names = TRUE)

# List all date files
date_files <- list.files(base_path, pattern = "_SENPLA\\.txt$", full.names = TRUE)

# Sort both lists
tss_files <- sort(tss_files)
date_files <- sort(date_files)

# Function to rename bands and save GeoTIFF
rename_tss_with_dates <- function(tss_path, out_path, date_path) {
  r <- rast(tss_path)
  dates <- read.table(date_path, header = FALSE, stringsAsFactors = FALSE)[,1]
  
  if (nlyr(r) != length(dates)) {
    stop("❌ Number of layers does not match number of dates in ", basename(date_path))
  }
  
  names(r) <- as.character(dates)
  
  out_file <- file.path(out_path, basename(tss_path))
  writeRaster(r, out_file, overwrite = TRUE, filetype = "ENVI", gdal = c("INTERLEAVE=BSQ"),
              datatype = "INT2S", NAflag = -9999)
  
  cat("✅ Saved:", basename(out_file), "\n")
}

# --- Auto-match each TSS file to a date file ---
# This assumes each TSS file has a corresponding date file with a shared part of the name
for (tss in tss_files) {

  # Find matching date file (based on shared substring)
  match_date <- date_files[grepl(substr(basename(tss), 1, 4), basename(date_files))]
  
  if (length(match_date) == 1) {
    rename_tss_with_dates(tss, out_path, match_date)
  } else {
    warning("⚠️ No unique matching date file for:", basename(tss))
  }
}
