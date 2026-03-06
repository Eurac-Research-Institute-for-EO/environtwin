library(terra)
library(future.apply)

# --------------------------------------------------
# Settings
# --------------------------------------------------
UDM_folders <- c("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard"
#  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X-001_Y-001/",
  #"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X-002_Y-002/",
  #"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0004_Y0002/",
#"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0006_Y0000",
#"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0006_Y0001",
#"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0007_Y-001",
#"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0007_Y0000",
#"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0008_Y-001",
#"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0008_Y0000",
#"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0009_Y-001"
)

out_root <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_daily/01/MH"
years_to_process <- 2017:2025
workers <- 2

scratch_path <- "/mnt/CEPH_PROJECTS/Environtwin/tmp"
dir.create(scratch_path, recursive = TRUE, showWarnings = FALSE)
terraOptions(tempdir = scratch_path)

# --------------------------------------------------
# Processing function for each date
# --------------------------------------------------
process_folder <- function(UDM_folder) {
  
  #folder_name <- basename(normalizePath(UDM_folder))
  out_folder <- file.path(out_root)
  dir.create(out_folder, recursive = TRUE, showWarnings = FALSE)
  
  # Collect UDM files
  udm_files <- list.files(
    UDM_folder,
    pattern = "*_PLANET_udm2_mask\\.tif$",
    full.names = TRUE
  )
  
  extract_year <- function(file) substr(basename(file), 1, 4)
  udm_files <- udm_files[extract_year(udm_files) %in% years_to_process]
  
  dates <- substr(basename(udm_files), 1, 8)
  unique_dates <- unique(dates)
  
  #target_date <- "20241124"
  #unqiue_date <- target_date
  
  # --------------------------------------------------
  # Function to process each date
  # --------------------------------------------------
  process_date_simple <- function(d) {
    
    worker_temp <- file.path(scratch_path, paste0("worker_", Sys.getpid()))
    dir.create(worker_temp, recursive = TRUE, showWarnings = FALSE)
    terraOptions(tempdir = worker_temp)
    
    files_for_date <- udm_files[dates == d]
    if (length(files_for_date) == 0) return(NULL)
    
    rasters <- lapply(files_for_date, function(f) {
      if (!file.exists(f)) return(NULL)
      r <- try(rast(f), silent = TRUE)
      if (inherits(r, "try-error") || all(is.na(values(r)))) return(NULL)
      r
    })
    
    # check for validity and remove udm2 that don't show any clear pixel
    valid_idx <- sapply(rasters, function(x) {
      if (is.null(x)) return(FALSE)
      mx <- global(x[[1]], "max", na.rm = TRUE)[1,1]
      !is.na(mx) && mx != 0
    })
    
    rasters <- rasters[valid_idx]
    
    if (length(rasters) == 0) {
      message("No valid rasters for date ", d, " - skipping.")
      return(NULL)
    }
    
    # --------------------------------------------------
    # Create count raster (+ layers of band1 from each image)
    # --------------------------------------------------
    count <- length(rasters)
    template <- rasters[[1]][[1]]
    count_raster <- template
    values(count_raster) <- count
    
    #band1_list <- lapply(rasters, function(r) r[[1]])
    #output_raster <- do.call(c, c(list(count_raster), band1_list))
    
    out_file_count <- file.path(out_folder, paste0(d, "_PLANET_udm2_count.tif"))
    
    writeRaster(
      count_raster, out_file_count,
      overwrite = TRUE,
      datatype = "INT2S",
      NAflag = -9999
    )
    
    # --------------------------------------------------
    # Output raster for the mosaic
    # --------------------------------------------------
    out_file <- file.path(out_folder, paste0(d, "_PLANET_udm2_mask.tif"))
    
    # --------------------------------------------------
    # Single raster case
    # --------------------------------------------------
    if (length(rasters) == 1) {
      writeRaster(
        rasters[[1]], out_file,
        overwrite = TRUE,
        datatype = "INT2S",
        NAflag = -9999
      )
      message("Copied single UDM raster for date ", d, " in folder ", out_folder)
      return(out_file)
    }
    
    # --------------------------------------------------
    # Mosaic (sum → binary)
    # --------------------------------------------------
    m <- try(mosaic(sprc(rasters), fun = "sum"), silent = TRUE)
    m[m >= 1] <- 1
    
    writeRaster(
      m, out_file,
      overwrite = TRUE,
      datatype = "INT2S",
      NAflag = -9999
    )
    
    message("✅ Mosaicked & saved UDM for date ", d, " in folder ",  out_folder)
    return(out_file)
  }
  
  # --------------------------------------------------
  # Run in parallel per folder
  # --------------------------------------------------
  plan(multisession, workers = workers)
  results <- future_lapply(unique_dates, process_date_simple, future.seed = TRUE)
  results <- Filter(Negate(is.null), results)
  
  message("Folder ", out_folder, " processed. Valid results: ", length(results))
  return(results)
}

# --------------------------------------------------
# Process all folders
# --------------------------------------------------
all_results <- lapply(UDM_folders, process_folder)
all_results <- unlist(all_results)
message("✅ All UDM folders processed. Total valid results: ", length(all_results))
