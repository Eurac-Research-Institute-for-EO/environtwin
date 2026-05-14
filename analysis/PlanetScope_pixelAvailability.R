library(terra)
library(lubridate)

# Change accordingly
site <- "MH"
years <- 2025

in_dir <- paste0("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_daily/03/", site)
out_dir <- paste0("/mnt/CEPH_PROJECTS/Environtwin/FORCE/validation/", site)

# list boa files
boa_files <- list.files(in_dir, pattern = "*_BOA\\.tif$", full.names = TRUE)

for(i in years){
  cat("Processing year:", i, "\n")
  
  # filter files for this year via YYYYMMDD prefix
  year_files <- boa_files[substr(basename(boa_files), 1, 4) == as.character(i)]
  
  'check <- lapply(year_files, function(f) {
    r <- rast(f)
    data.frame(
      file = basename(f),
      xmin = ext(r)[1],
      xmax = ext(r)[2],
      ymin = ext(r)[3],
      ymax = ext(r)[4],
      nrow = nrow(r),
      ncol = ncol(r)
    )
  })
  
  do.call(rbind, check)'
  
  # read all daily rasters for this year
  r_stack <- rast(year_files, lyrs = 1)
  
  # count valid observations per pixel across layers
  n_valid <- app(r_stack, fun = function(x) sum(!is.na(x)))
  
  # ensure integer and give a meaningful name
  n_valid <- ifel(n_valid == 0, NA, n_valid)
  
  # plot
  plot(n_valid, main = paste("Valid observations per pixel (", i, ")", sep = ""))
  
  # save
  writeRaster(n_valid, file = file.path(out_dir, paste0("valid_obs_planet_", site, i, ".tif")), overwrite = TRUE)
}

#### keydate valid observations
# dates of interest (each year)
dates_of_interest <- c("06-25", "07-01", "07-08")  

for(i in years){
  cat("Processing year:", i, "\n")
  
  # pick files for this year
  year_files <- boa_files[substr(basename(boa_files), 1, 4) == as.character(i)]
  if(length(year_files) == 0) next
  
  # parse dates from YYYYMMDD
  fnames <- basename(year_files)
  dates <- ymd(substr(fnames, 1, 8))
  
  # build a list of selected file indices for all dates of interest
  keep <- logical(length(dates)); keep[] <- FALSE
  
  for(doi in dates_of_interest){
    target_date <- ymd(paste0(i, "-", doi))  # e.g., 2017-06-25
    # ±3‑day window
    window <- target_date %m+% months(0) + days(-5:5)
    keep <- keep | (dates %in% window)
  }
  
  # subset files to only those around 25 June, 1 July, 8 July
  subset_files <- year_files[keep]
  
  if(length(subset_files) == 0) {
    warning(paste("No files within ±5 days of 25 June, 1 July, or 8 July for year", i))
    next
  }
  
  # read and stack
  r_stack <- rast(subset_files, lyrs = 1)
  
  # count valid observations per pixel
  n_valid <- app(r_stack, fun = function(x) sum(!is.na(x)))
  n_valid <- app(n_valid, fun = as.integer)
  names(n_valid) <- paste0("valid_obs_", i)
  
  # set 0 to NA
  n_valid <- ifel(n_valid == 0, NA, n_valid)
  
  # plot
  plot(n_valid, main = paste("Valid observations between ", i))
  # save
  writeRaster(n_valid, file = file.path(out_dir, paste0("valid_obs_planet_keydates", i, ".tif")), overwrite = TRUE)
}
