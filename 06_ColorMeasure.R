#############################
# Create Whitness layer from PLANET BOA
# Run in terminal to use more processes and run in parallel
############################
library(terra)
library(tools)
library(future.apply)

# === Setup ===
in_dir  <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/SA/coregistered/"
udm_dir <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/SA/standard"
out_dir <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/sites_whiteness/SA"

dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

# === terra performance settings ===
terraOptions(tempdir = "/tmp")
terraOptions(memfrac = 0.7)
terraOptions(progress = 1)

# === List input files ===
sr_files  <- list.files(in_dir, pattern = "_BOA\\.bsq$", full.names = TRUE)
udm_files <- list.files(udm_dir, pattern = "_PLANET_udm2_mask\\.tif$", full.names = TRUE)

# === Filter files by year ===
years <- c("2017","2018","2019","2020", "2021" ,"2022","2023","2024","2025")
pattern <- paste(years, collapse = "|")

sr_files  <- grep(pattern, sr_files, value = TRUE)
udm_files <- grep(pattern, udm_files, value = TRUE)

# === OPTIONAL: filter single year ===
# sr_files  <- sr_files[grepl("2025", sr_files)]
# udm_files <- udm_files[grepl("2025", udm_files)]

# === Match SR and UDM files correctly ===
get_scene_id <- function(x) {
  sub("_(BOA|PLA).*", "", file_path_sans_ext(basename(x)))
}

sr_ids  <- get_scene_id(sr_files)
udm_ids <- get_scene_id(udm_files)

common_ids <- intersect(sr_ids, udm_ids)

sr_files  <- sr_files[sr_ids %in% common_ids]
udm_files <- udm_files[udm_ids %in% common_ids]

# reorder UDM to match SR exactly
udm_files <- udm_files[match(get_scene_id(sr_files),
                             get_scene_id(udm_files))]

message("Found ", length(sr_files), " file pairs to process.")

################################################################################
# --- Whiteness function ---
################################################################################

white_layer_function <- function(data, udm) {
  ras  <- rast(data)
  mask_ras <- rast(udm)
  
  # Scale reflectance (Planet BOA = 0–10000)
  rgb <- ras[[1:3]] / 10000
  
  # Apply clear mask (ASSUMES band 1 = clear!)
  rgb_mask <- mask(rgb, mask_ras[[1]], maskvalues = 0)
  
  blue  <- rgb_mask[[1]]
  green <- rgb_mask[[2]]
  red   <- rgb_mask[[3]]
  
  whiteness <- abs(red - green) +
    abs(red - blue) +
    abs(green - blue)
  
  return(whiteness)
}

################################################################################
# --- Parallel Processing ---
################################################################################

workers <- 2   # safer for memory; increase cautiously

# Use multicore on Linux (better for terra)
plan(multicore, workers = workers)

log_file <- file.path(out_dir, "Whiteness_processing_log.txt")
cat("Whiteness processing started at", Sys.time(), "\n", file = log_file)

future_lapply(seq_along(sr_files), function(i) {
  
  sr_path  <- sr_files[i]
  udm_path <- udm_files[i]
  
  out_name <- file.path(
    out_dir,
    paste0(file_path_sans_ext(basename(sr_path)), "_white.tif")
  )
  
  # Skip if already exists
  if (file.exists(out_name)) {
    message("⏭ Skipping existing file: ", basename(out_name))
    return(NULL)
  }
  
  # Compute whiteness with proper error reporting
  white_raster <- tryCatch(
    white_layer_function(sr_path, udm_path),
    error = function(e) {
      message("❌ ERROR: ", basename(sr_path))
      message(e)
      cat("❌ ERROR:", sr_path, "\n", file = log_file, append = TRUE)
      return(NULL)
    }
  )
  
  if (inherits(white_raster, "SpatRaster")) {
    tryCatch({
      writeRaster(
        white_raster,
        out_name,
        overwrite = TRUE,
        datatype = "FLT4S",
        gdal = c("COMPRESS=LZW", "TILED=YES")
      )
      message("✅ Written: ", basename(out_name))
      cat("✅ Written:", out_name, "\n", file = log_file, append = TRUE)
      
    }, error = function(e) {
      message("❌ WRITE ERROR: ", basename(out_name))
      message(e)
      cat("❌ WRITE ERROR:", out_name, "\n", file = log_file, append = TRUE)
    })
    
  } else {
    message("⚠️ Skipped (no valid whiteness): ", basename(sr_path))
    cat("⚠️ Skipped:", sr_path, "\n", file = log_file, append = TRUE)
  }
  
}, future.seed = TRUE)

cat("Whiteness processing completed at", Sys.time(), "\n",
    file = log_file, append = TRUE)

message("✅ Whiteness processing completed successfully.")