library(terra)
library(tools)
library(future.apply)

# -----------------------------
# GDAL sieve filter helper
# -----------------------------
sieve_filter <- function(input, output, threshold = 800,
                         eight_connected = TRUE,
                         gdal_path = "/usr/bin/gdal_sieve.py") {
  if (!file.exists(input)) stop("Input file does not exist: ", input)
  threshold <- as.character(threshold)
  
  args <- c(gdal_path, "-st", threshold)
  if (eight_connected) args <- c(args, "-8")
  # Explicitly pass nodata = 255 (or the raster’s nodata value)
  args <- c(args, "-nomask")  # ensure nodata is not considered foreground
  args <- c(args, input, output)
  
  res <- system2("python3", args = args, stdout = TRUE, stderr = TRUE)
  if (!file.exists(output)) stop("GDAL sieve failed:\n", paste(res, collapse = "\n"))
  message("Sieve filter completed: ", output)
  invisible(output)
}

# -----------------------------
# Per-file UDM mask improvement & merge
# -----------------------------
process_udm_mask <- function(sr_file, udm_file, out_dir, min_patch_size = 800) {
  sr <- rast(sr_file)
  udm <- rast(udm_file)
  base_name <- file_path_sans_ext(basename(udm_file))
  
  #if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
  
  # -----------------------------
  # 1️⃣ Create clear mask (0/1) with NA preserved
  # -----------------------------
  clear_mask <- (udm[["clear"]] == 1) & !is.na(sr[[1]])
  
  clear_mask_int <- udm[["clear"]]           # copy structure
  clear_mask_int <- ifel(is.na(clear_mask_int), NA, ifel(clear_mask, 1, 0))
  
  temp_mask_file <- tempfile(fileext = ".tif")
  writeRaster(
    clear_mask_int,
    temp_mask_file,
    overwrite = TRUE,
    datatype = "INT1U",
    gdal = c("COMPRESS=LZW", "TILED=YES")
  )
  rm(clear_mask, clear_mask_int); gc()
  
  # -----------------------------
  # 2️⃣ Apply GDAL sieve filter
  # -----------------------------
  sieve_out_file <- file.path(out_dir, paste0(base_name, "_mask_clear.tif"))
  sieve_filter(temp_mask_file, sieve_out_file,
               threshold = min_patch_size, eight_connected = TRUE)
  file.remove(temp_mask_file)
  
  # -----------------------------
  # 3️⃣ Restore 0/1/NA after sieve
  # -----------------------------
  udm_file_sieve <- rast(sieve_out_file)
  udm_file_sieve_fixed <- ifel(is.na(udm_file_sieve), NA, ifel(udm_file_sieve == 1, 1, 0))
  
  file.remove(sieve_out_file)
  
  # -----------------------------
  # 4️⃣ Merge sieve mask + UDM bands 2–8
  # -----------------------------
  udm_bands_2_8 <- udm[[2:8]]
  
  merged <- c(udm_file_sieve_fixed, udm_bands_2_8)
  names(merged) <- names(udm)
  
 # Replace the end part "_3B_udm2_clip" with "_PLANET_udm2"
  #base_name <- sub("_3B_udm2_clip$", "_PLANET_udm2", base_name)
  
  # Create the final output file path
  merged_out_file <- file.path(out_dir, paste0(base_name, "_mask.tif"))
  
  merged_out_file <- file.path(out_dir, paste0(base_name, "_mask.tif"))
  writeRaster(
    merged,
    merged_out_file,
    overwrite = TRUE,
    gdal = c("COMPRESS=LZW", "TILED=YES")
  )
  
  message("✅ Completed UDM mask processing and merge for ", base_name)
  return(merged_out_file)
}

# -----------------------------
# Parallel folder-level driver
# -----------------------------
process_folder_parallel <- function(in_dir, out_dir,
                                    year = 2024,
                                    min_patch_size = 800,
                                    workers = 4) {
  sr_files <- list.files(in_dir, pattern = ".*_PLANET_BOA\\.tif$", full.names = TRUE)
  udm_files <- list.files(in_dir, pattern = ".*_PLANET_udm2\\.tif$", full.names = TRUE)
  
  pattern <- paste0(year, collapse = "|")
  sr_files  <- sr_files [grepl(pattern, basename(sr_files ))]
  udm_files <- udm_files[grepl(pattern, basename(udm_files))]
  
  if (length(sr_files) != length(udm_files))
    stop("Mismatch in number of SR and UDM files!")
  
  file_pairs <- lapply(seq_along(sr_files), function(i)
    list(sr_files[i], udm_files[i]))
  
  # Parallel execution across file pairs
  plan(multisession, workers = workers)
  
  results <- future_lapply(seq_along(file_pairs), function(i) {
    sr_file  <- file_pairs[[i]][[1]]
    udm_file <- file_pairs[[i]][[2]]
    try({
      process_udm_mask(sr_file, udm_file, out_dir,
                       min_patch_size = min_patch_size)
    }, silent = FALSE)
  }, future.seed = TRUE) 
  
  message("All UDM masks processed for year(s): ",
          paste(year, collapse = ", "))
  invisible(results)
}

# -----------------------------
# Example run
# -----------------------------
in_dir  <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/test_PA/"
out_dir <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/test_PA/X0004_Y0002"
min_patch_size <- 800
workers <- 4  # tune for your RAM/CPU

process_folder_parallel(in_dir, out_dir,
                        year = 2017:2025,
                        min_patch_size = min_patch_size,
                        workers = workers)

