library(terra)
library(future.apply)
library(purrr)

ndvi_folders <- c("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/01/X-001_Y-001/data",
                  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/01/X0006_Y0000/data",
                  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/01/X0006_Y0001/data",
                  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/01/X0008_Y-001/data",
                  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/01/X0008_Y0000/data")
                  
                '"/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/01/X-002_Y-002",
                  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/01/X0004_Y0002",
                  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/01/X0007_Y-001",
                  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/01/X0007_Y0000",
                  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/01/X0009_Y-001"'

out_root <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/indices/01"
years_to_process <- 2025
workers <- 2
scratch_path <- "/mnt/CEPH_PROJECTS/Environtwin/tmp"

dir.create(scratch_path, recursive = TRUE, showWarnings = FALSE)
dir.create(out_root, recursive = TRUE, showWarnings = FALSE)
terraOptions(tempdir = scratch_path)

process_folder <- function(ndvi_folder) {
  
  out_folder <- file.path(ndvi_folder)
  dir.create(out_folder, recursive = TRUE, showWarnings = FALSE)
  
  ndvi_files <- list.files(
    ndvi_folder,
    pattern = "^(202510|202511).*_PLA_masked_NDV\\.tif$",
    full.names = TRUE
  )
  
  extract_year <- function(file) substr(basename(file), 1, 4)
  ndvi_files <- ndvi_files[extract_year(ndvi_files) %in% years_to_process]
  dates <- substr(basename(ndvi_files), 1, 8)
  unique_dates <- unique(dates)
  
  process_date_simple <- function(d) {
    worker_temp <- file.path(scratch_path, paste0("worker_", Sys.getpid()))
    dir.create(worker_temp, recursive = TRUE, showWarnings = FALSE)
    terraOptions(tempdir = worker_temp)
    
    files_for_date <- ndvi_files[dates == d]
    if (length(files_for_date) == 0) return(NULL)
    
    rasters <- lapply(files_for_date, function(f) {
      if (!file.exists(f)) return(NULL)
      r <- try(rast(f), silent = TRUE)
      if (inherits(r, "try-error") || all(is.na(values(r)))) return(NULL)
      r
    }) |> compact()
    
    if (length(rasters) == 0) {
      message("No valid rasters for date ", d, " - skipping.")
      return(NULL)
    }
    
    out_file <- file.path(out_folder, paste0(d, "_PLANET_masked_NDV.tif"))
    
    if (length(rasters) == 1) {
      writeRaster(rasters[[1]], out_file, overwrite = TRUE, datatype = "INT2S", NAflag = -9999)
      message("Copied single raster for date ", d)
      return(out_file)
    }
    
    m <- try(mosaic(sprc(rasters), fun = "mean"), silent = TRUE)
    if (inherits(m, "try-error")) {
      message("❌ Mosaic failed for date ", d)
      return(NULL)
    }
    
    writeRaster(m, out_file, overwrite = TRUE, datatype = "INT2S", NAflag = -9999)
    message("✅ Mosaicked and saved BOA for date ", d)
    return(out_file)
  }
  
  future_lapply(unique_dates, process_date_simple, future.seed = TRUE) |> compact()
}

plan(multisession, workers = workers)
all_results <- lapply(ndvi_folders, process_folder) |> unlist()
message("✅ All folders processed. Total valid results: ", length(all_results))
