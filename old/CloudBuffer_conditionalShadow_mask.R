library(terra)
library(jsonlite)
library(tidyterra)
library(future)
library(future.apply)

# Add these lines after library(future.apply)
options(future.globals.maxSize = +Inf)  # Allow large objects
options(future.rng.onMisuse = "ignore")

# years to be processed
years <- c("2017","2018","2019","2020","2022","2023","2024","2025")
pattern <- paste(years, collapse="|")

# list BOA files
files_list <- list.files(
  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/coregistered",
  pattern = "_BOA\\.bsq$",
  full.names = TRUE
)

# list JSON metadata
json_files_list <- list.files(
  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard",
  pattern = "\\.json$",
  full.names = TRUE
)

# list whiteness rasters
whiteness_list <- list.files(
  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/sites_white/",
  pattern = "_white\\.tif$",
  full.names = TRUE
)

# list UDM2 masks
udms <- list.files(
  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard",
  pattern = "_udm2_mask\\.tif$",
  full.names = TRUE
)

# FIXED PATHS for parallel workers
ref_path <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/mosaic/MH/PLANET_MOSAIC_4BANDS_PERIOD/final/PLANET_MOSAIC_20240701_20240715_DATA.tif"
mask_path <- "/mnt/CEPH_PROJECTS/Environtwin/gis/masks/MH_mask.tif"

# output folder
out_folder_mosaic <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/test/cloud_masks/shadow/mosaic/"
dir.create(out_folder_mosaic, showWarnings = FALSE)

# parameters
kernel_size <- 21

# ===========================
# Filter for target year
# ===========================
sr_files        <- files_list[grepl("2024", files_list)]
udm_files       <- udms[grepl("2024", udms)]
json_files      <- json_files_list[grepl("2024", json_files_list)]
whiteness_files <- whiteness_list[grepl("2024", whiteness_list)]

# ===========================
# Detect scenes with shadows (sequential, lightweight)
# ===========================
all_shadow <- list()
for (i in seq_along(json_files)) {
  json_file <- fromJSON(json_files[[i]])
  if (!is.null(json_file$properties$shadow_percent) &&
      json_file$properties$shadow_percent > 0) {
    id <- sub("_metadata.*$", "", basename(json_files[[i]]))
    all_shadow[[length(all_shadow) + 1]] <- data.frame(
      id = id,
      shadow_perc = json_file$properties$shadow_percent
    )
  }
}

shadow_ids <- character(0)
if (length(all_shadow) > 0) {
  test <- do.call(rbind, all_shadow)
  shadow_ids <- test$id
}

# ===========================
# Build ALL scene IDs from UDM
# ===========================
ids_all <- unique(sub("_PLANET.*$", "", basename(udm_files)))

# ===========================
# FIXED Processing function
# ===========================
process_shadow <- function(id, sr_files, udm_files, whiteness_files, 
                           out_folder, white_thresh, nir_thresh,
                           ref_path, mask_path, dark_factor = 0.6, kernel_size = 25) {
  
  cat(" → Processing:", id, "\n")
  
  # DEFINE KERNEL INSIDE FUNCTION
  kernel <- matrix(1, kernel_size, kernel_size)
  
  # RELOAD REFERENCES INSIDE FUNCTION
  ref_summer <- rast(ref_path, lyrs = 4)
  mask_raster <- rast(mask_path)
  
  # ---------------------------
  # Load UDM (ALWAYS)
  # ---------------------------
  udm_files_id <- udm_files[sub("_PLANET.*$", "", basename(udm_files)) %in% id]
  if (length(udm_files_id) == 0) {
    cat(" ⚠️  No UDM found for ID:", id, "\n")
    return(NULL)
  }
  udm <- rast(udm_files_id)
  udm <- resample(udm, mask_raster, method = "near")
  
  # ---------------------------
  # Cloud buffer 
  # ---------------------------
  cloud <- udm[[6]]
  cloud_buffer <- focal(cloud, w = kernel, fun = max, na.rm = TRUE)
  cloud_buffer <- round(cloud_buffer)
  names(cloud_buffer) <- "cloud_buffer"
  
  # ---------------------------
  # Shadow buffer
  # ---------------------------
  nir_files <- sr_files[sub("_PLANET.*$", "", basename(sr_files)) %in% id]
  white_files <- whiteness_files[sub("_PLANET.*$", "", basename(whiteness_files)) %in% id]
  
  if (length(nir_files) > 0 && length(white_files) > 0) {
    nir_target <- rast(nir_files, lyrs = 4)
    nir_target <- resample(nir_target, mask_raster, method = "bilinear")
    
    whiteness <- rast(white_files)
    whiteness <- resample(whiteness, mask_raster, method = "bilinear")
    
    shadow_mask <- (nir_target < (ref_summer * dark_factor)) &
      (nir_target < nir_thresh) &
      (whiteness < white_thresh)
    
    shadow_mask <- shadow_mask * 1
    sieved <- sieve(shadow_mask, threshold = 500, directions = 8)
    shadow_buffer <- focal(sieved, w = kernel, fun = max, na.rm = TRUE)
    shadow_buffer <- round(shadow_buffer)
    shadow_buffer <- crop(shadow_buffer, mask_raster)
    names(shadow_buffer) <- "shadow_buffer"
  } else {
    # Create empty shadow buffer if no NIR/white files
    shadow_buffer <- mask_raster * 0
    names(shadow_buffer) <- "shadow_buffer"
  }
  
  # ---------------------------
  # Stack and write output
  # ---------------------------
  udm_extended <- c(udm, cloud_buffer, shadow_buffer)
  names(udm_extended) <- c(names(udm), "cloud_buffer", "shadow_buffer")
  
  output_file <- file.path(out_folder, paste0(id, "_UDM_extended.tif"))
  writeRaster(udm_extended, output_file, overwrite = TRUE, datatype = "INT2S", NAflag = -9999)
  
  cat(" ✓ Saved:", basename(output_file), "\n")
  return(output_file)
}

# ===========================
# PARALLEL PROCESSING - FIXED
# ===========================
cat("🚀 Starting parallel processing with", length(ids_all), "IDs\n")
plan(multisession, workers = 2)  # FIXES RNG warning

results <- future_lapply(ids_all, function(id) {
  process_shadow(
    id = id,
    sr_files = sr_files,
    udm_files = udm_files,
    whiteness_files = whiteness_files,
    out_folder = out_folder_mosaic,
    white_thresh = 500,
    nir_thresh = 3000,
    ref_path = ref_path,
    mask_path = mask_path
  )
}, future.seed = TRUE)

plan(sequential)
cat("🎉 All processing complete! Check output in:", out_folder_mosaic, "\n")
