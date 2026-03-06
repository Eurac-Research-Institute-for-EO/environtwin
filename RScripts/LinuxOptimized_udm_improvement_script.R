# ============================================================
# Stable Linux-optimized UDM mask processor
# ============================================================

# -----------------------------
# Libraries
# -----------------------------
library(terra)
library(future.apply)
library(tools)

# -----------------------------
# Global stability settings (must be on top!)
# -----------------------------
# Limit GDAL threads
Sys.setenv(GDAL_NUM_THREADS = "1", PROJ_NETWORK = "OFF")

# Create unique temp dir for this R session
tmpdir <- file.path("/dev/shm", paste0("terra_", Sys.getpid()))
dir.create(tmpdir, showWarnings = FALSE, recursive = TRUE)

# Terra options
terraOptions(
  progress = 1,
  memfrac  = 0.9,
  tempdir  = tmpdir
)

# Stable parallel setup
workers <- 2
plan(multisession, workers = workers)
# -----------------------------
# 1️⃣ Per-file UDM mask processing
# -----------------------------
process_udm_mask_fast <- function(sr_file, udm_file, out_dir, min_patch_size = 100000) {
  sr  <- rast(sr_file)
  udm <- rast(udm_file)
  base_name <- file_path_sans_ext(basename(udm_file))
  
  # Create binary clear mask (0/1) with NA preserved
  clear_mask <- ifel(
    is.na(udm[["clear"]]), NA,
    ifel(udm[["clear"]] == 1 & !is.na(sr[[1]]), 1, 0)
  )
  
  # Apply sieve filter
  sieved <- terra::sieve(
    clear_mask,
    threshold = min_patch_size,
    directions = 8
  )
  
  # Merge with other UDM bands 2–8
  merged <- c(sieved, udm[[2:8]])
  names(merged) <- names(udm)
  
  # Write final output GeoTIFF
  out_file <- file.path(out_dir, paste0(base_name, "_mask.tif"))
  writeRaster(
    merged,
    out_file,
    overwrite = TRUE,
    datatype = "INT2S",
    NAflag = -9999,
    gdal = c("COMPRESS=LZW", "TILED=YES", "NUM_THREADS=1")
  )
  
  cat(sprintf("[%s] ✅ Completed: %s\n", Sys.time(), base_name))
  invisible(out_file)
}

# -----------------------------
# 2️⃣ Parallel folder-level driver
# -----------------------------
process_folder_parallel_fast <- function(in_dir, out_dir,
                                         year = 2024,
                                         min_patch_size = 1000) {
  # Ensure output directory exists
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
  
  # List files
  sr_files  <- list.files(
    in_dir,
    pattern = "_BOA\\.tif$",
    #pattern = "^(202510|202511).*_PLANET_BOA\\.tif$",
    full.names = TRUE
  )
  
  udm_files <- list.files(
    in_dir,
    pattern = "_PLANET_udm2\\.tif$",
    #pattern = "^(202510|202511).*_PLANET_udm2\\.tif$",
    full.names = TRUE
  )
  
  # Filter by year(s)
  pattern <- paste0(year, collapse = "|")
  sr_files  <- sr_files[grepl(pattern, basename(sr_files))]
  udm_files <- udm_files[grepl(pattern, basename(udm_files))]
  
  if (length(sr_files) != length(udm_files))
    stop("❌ Mismatch in number of SR and UDM files!")
  
  # Pair files by common prefix
  #common_ids <- sub("*_PLA_BOA\\.tif$", "", basename(sr_files))
  common_ids <- sub("_PLA.*$", "", basename(sr_files))
  
  file_pairs <- lapply(common_ids, function(id) {
    sr  <- sr_files[grep(id, sr_files)]
    udm <- udm_files[grep(id, udm_files)]
    list(sr, udm)
  })
  
  cat(sprintf("\n🧠 Starting parallel processing with %d workers...\n", workers))
  cat(sprintf("📅 Processing year(s): %s\n\n", paste(year, collapse = ", ")))
  
  # Parallel processing with tryCatch for safety
  results <- future_lapply(seq_along(file_pairs), function(i) {
    tryCatch({
      sr_file  <- file_pairs[[i]][[1]]
      udm_file <- file_pairs[[i]][[2]]
      gc()  # free memory between tasks
      process_udm_mask_fast(sr_file, udm_file, out_dir, min_patch_size)
    }, error = function(e) {
      message("⚠️ Error in job ", i, ": ", conditionMessage(e))
      NULL
    })
  }, future.seed = TRUE)
  
  cat("\n🎉 All UDM masks processed successfully.\n")
  invisible(results)
}
# ------------------------------------------------------------
# 3️⃣  Example run
# ------------------------------------------------------------
#in_dir  <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0004_Y0002"
in_dir  <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard"
out_dir <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard"

min_patch_size <- 10000
workers <- 2

process_folder_parallel_fast(
  in_dir, out_dir,
  year = 2025,
  min_patch_size = min_patch_size
)
