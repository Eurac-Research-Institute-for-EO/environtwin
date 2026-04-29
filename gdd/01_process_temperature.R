library(terra)
library(data.table)
library(sf)
library(parallel)
library(dplyr)
library(reshape2)

tmean_dir <- "/mnt/CEPH_PROJECTS/CLIMATE/GRIDS/TEMPERATURE/TIME_SERIES/UPLOAD"
prec_dir <- "/mnt/CEPH_PROJECTS/CLIMATE/GRIDS/TMIN"
out_dir <- "/mnt/CEPH_PROJECTS/Environtwin/GDD/SouthTyrol/temperature"

# st shapefile
shp <- st_read("/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/misc/boundaries/southtyrol.shp")

# ---- Helper: DOY range for each month ----
get_doy_range <- function(year, month) {
  days_in_months <- c(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
  if (as.numeric(year) %% 4 == 0 && (as.numeric(year) %% 100 != 0 || as.numeric(year) %% 400 == 0)) {
    days_in_months[2] <- 29
  }
  start_doy <- ifelse(month == 1, 1, sum(days_in_months[1:(month - 1)]) + 1)
  n_days <- days_in_months[month]
  return(list(start_doy = start_doy, n_days = n_days))
}

'filename_list <- list(
  "2015" = "DTMEAN_",
  "2016" = "DTMEAN_",
  "2017" = "DTMEAN_",
  "2018" = "DTMEAN_",
  "2019" = "DTMEAN_",
  "2020" = "DAILYTMEAN_",
  "2021" = "DAILYTMEAN_",
  "2022" = "DAILYTMEAN_",
  "2023" = "daily_tmean_",
  "2024" = "daily_tmean_",
  "2025" = "DAILY_TMEAN_"
)'

filename_list <- list(
  "2015" ="DAILYTMIN_", "2016" = "DAILYTMIN_",
  "2017" = "DAILYTMIN_", "2018" = "DAILYTMIN_", "2019" = "DAILYTMIN_",
  "2020" = "DAILYTMIN_", "2021" = "DAILYTMIN_", "2022" = "DAILYTMIN_",
  "2023" = "daily_tmin_", "2024" = "daily_tmin_", "2025" = "DAILY_TMIN_"
)


# ---- Main processor ----
process_climate <- function(years,
                            tmean_dir,
                            out_dir,
                            ref_epsg = "EPSG:32632",
                            n_cores = parallel::detectCores() - 1,
                            resample_to_ref = FALSE) {
  
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
  
  for (yr in years) {
    message("Processing year: ", yr)
    mos <- 1:12
    
    parallel::mclapply(mos, function(mo) {
      tryCatch({
        mo_str <- sprintf("%02d", mo)
        message("  Processing month: ", mo_str)
        
        # Build input filename (adjust if directory structure differs)
        tmean_file <- file.path(
          tmean_dir, yr,
          paste0(filename_list[[as.character(yr)]], yr, mo_str, ".nc")
        )
        
        if (!file.exists(tmean_file)) {
          warning("Skipping missing file: ", tmean_file)
          return(NULL)
        }
        
        # Load raster
        tmean_rast <- terra::rast(tmean_file)
        crs(tmean_rast) <- crs(shp)
        
        tmean_rast <- terra::mask(crop(tmean_rast, shp), shp)
        
        # Assign DOY names
        doy_info <- get_doy_range(yr, mo)
        doy_seq <- doy_info$start_doy:(doy_info$start_doy + doy_info$n_days - 1)
        names(tmean_rast) <- as.character(doy_seq)
        
        # Output file
        out_file <- file.path(out_dir, paste0("tmin_250m_", yr, "_", mo_str, ".tif"))
        writeRaster(tmean_rast, out_file, overwrite = TRUE)
        
        NULL
      }, error = function(e) {
        message("Error in year ", yr, ", month ", mo, ": ", e$message)
        return(NULL)
      })
    }, mc.cores = n_cores)
  }
  
  message("All processing complete. Output saved to: ", out_dir)
}

# ---- Run for 2015–2024 ----
years <- 2025

process_climate(
  years = years,
  tmean_dir = prec_dir,  # change directory if you want to process tmean
  out_dir = out_dir,
  n_cores = 1 
)
