library(terra)
library(future.apply)

# --------------------------------------------------
# Settings
# --------------------------------------------------
BOA_folders <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard"

out_root <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_daily/02/MH"
years_to_process <-2017:2020
workers <- 2

scratch_path <- "/mnt/CEPH_PROJECTS/Environtwin/tmp"
dir.create(scratch_path, recursive = TRUE, showWarnings = FALSE)
terraOptions(tempdir = scratch_path)

# --------------------------------------------------
# Processing function for each date
# --------------------------------------------------
process_folder <- function(BOA_folder) {
  
  #folder_name <- basename(normalizePath(UDM_folder))
  out_folder <- file.path(out_root)
  dir.create(out_folder, recursive = TRUE, showWarnings = FALSE)
  
  # Collect UDM files
  udm_files <- list.files(
    BOA_folder,
    pattern = "*_PLA_udm2_mask\\.tif$",
    full.names = TRUE
  )
  
  # Collect BOA files
  boa_files <- list.files(
    BOA_folder,
    pattern = "*_BOA\\.tif$",
    full.names = TRUE
  )
  
  extract_year <- function(file) substr(basename(file), 1, 4)
  udm_files <- udm_files[extract_year(udm_files) %in% years_to_process]
  boa_files <- boa_files[extract_year(boa_files) %in% years_to_process]
  
  dates <- substr(basename(boa_files), 1, 8)
  unique_dates <- unique(dates)
  
  # --------------------------------------------------
  # Function to process each date
  # --------------------------------------------------
  process_date_simple <- function(d) {
    
    worker_temp <- file.path(scratch_path, paste0("worker_", Sys.getpid()))
    dir.create(worker_temp, recursive = TRUE, showWarnings = FALSE)
    terraOptions(tempdir = worker_temp)
    
    # find all files for one day
    files_for_date_boa <- boa_files[dates == d]
    files_for_date_udm <- udm_files[dates == d]
    
    if (length(files_for_date_boa) == 0) return(NULL)
    
    rasters_boa <- lapply(files_for_date_boa, function(f) {
      if (!file.exists(f)) return(NULL)
      r <- try(rast(f), silent = TRUE)
      if (inherits(r, "try-error") || all(is.na(values(r)))) return(NULL)
      r
    })
    
    ## load also udm files
    rasters_udm <- lapply(files_for_date_udm, function(f) {
      if (!file.exists(f)) return(NULL)
      r <- try(rast(f), silent = TRUE)
      if (inherits(r, "try-error") || all(is.na(values(r)))) return(NULL)
      r
    })
    
    # check for validity of the rasters, if the max of the image band is 0 than neglect it
    valid_idx <- sapply(rasters_udm, function(x) {
      if (is.null(x)) return(FALSE)
      mx <- global(x[[1:6]], "max", na.rm = TRUE)[1,1]
      !is.na(mx) && mx != 0
    })
    
    rasters_udm <- rasters_udm[valid_idx]
    rasters_boa <- rasters_boa[valid_idx]

    if (length(rasters_boa) == 0) {
      message("No valid rasters for date ", d, " - skipping.")
      return(NULL)
    }
    
    # --------------------------------------------------
    # Create count raster 
    # --------------------------------------------------
    count <- length(rasters_udm)
    template <- rasters_udm[[1]][[1]]
    count_raster <- template
    values(count_raster) <- count
    
    out_file_count <- file.path(out_folder, paste0(d, "_PLANET_udm2_count.tif"))
    
    writeRaster(
      count_raster, out_file_count,
      overwrite = TRUE,
      datatype = "INT2S",
      NAflag = -9999
    )
    
    # --------------------------------------------------
    # Output raster 
    # --------------------------------------------------
    # Build output filename 
    out_file <- file.path(out_folder, paste0(d, "_PLANET_udm2_blue_mask.tif"))
    
    # Handle single-raster case
    if (length(rasters_boa) == 1) {
      
      udm_masked <- rasters_udm[[1]]
      udm_masked[rasters_boa[[1]][[1]] > 900] <- 0
      
      writeRaster(
        udm_masked, out_file,
        overwrite = TRUE,
        datatype = "INT2S",
        NAflag = -9999
      )
      message("Copied single raster for date ", d)
      return(out_file)
    }
    
    
    # --------------------------------------------------
    # Mosaic multiple rasters (mean or sum — your choice)
    # --------------------------------------------------
    udm_masked <- lapply(seq_along(rasters_udm), function(i) {
      udm <- rasters_udm[[i]]
      udm[rasters_boa[[i]][[1]] > 900] <- 0
      udm
    })
    
    m <- try(mosaic(sprc(udm_masked), fun = "sum"), silent = TRUE)
    m[m >= 1] <- 1
    
    if (inherits(m, "try-error")) {
      message("❌ Mosaic failed for date ", d)
      return(NULL)
    }
    
    # Optionally, if you always expect 8 bands and want to preserve them:
    # names(m) <- names(rasters[[1]])  # keep consistent band names
    
    writeRaster(
      m, out_file,
      overwrite = TRUE,
      datatype = "INT2S",
      NAflag = -9999
    )
    
    message("✅ Mosaicked and saved BOA for date ", d)
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
all_results <- lapply(BOA_folders, process_folder)
all_results <- unlist(all_results)
message("✅ All  udm files processed. Total valid results: ", length(all_results))