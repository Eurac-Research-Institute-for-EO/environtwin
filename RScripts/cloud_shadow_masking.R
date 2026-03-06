library(tidyterra)

# years to be processed
years <- c("2017","2018", "2019", "2020", "2022", "2023", "2024", "2025")
pattern <- paste(years, collapse = "|")

# list BOA files, json and whiteness files
files_list <- list.files("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard", pattern = "_BOA\\.tif$",
                         full.names = T)

json_files_list <- list.files("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard", pattern = "\\.json$",
                             full.names = T)

whiteness_list <- list.files("/mnt/CEPH_PROJECTS/Environtwin/FORCE/sites_whiteness", pattern = "_white\\.tif$",
                       full.names = T)

# load reference raster and mask for alignment
#daily_ref_aut <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_daily/01/MH/20241017_PLANET_PSB.SD_BOA.tif",
#                lyrs = 4)
#daily_ref_sum <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_daily/01/MH/20240718_PLANET_PSB.SD_BOA.tif",
#                lyrs = 4)

# load mosaic references
mosaic_ref_sum <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/mosaic/MH/PLANET_MOSAIC_4BANDS_PERIOD/final/PLANET_MOSAIC_20170701_20170715_DATA.tif",
               lyrs = 4)

mosaic_ref_autumn <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/mosaic/MH/PLANET_MOSAIC_4BANDS_PERIOD/final/PLANET_MOSAIC_20251016_20251031_DATA.tif",
                                  lyrs = 4)

# load mask to resample everything to the same extent
mask <- rast("/mnt/CEPH_PROJECTS/Environtwin/gis/masks/MH_mask.tif")

# create output folder
out_folder_mosaic <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/test/cloud_masks/shadow/mosaic/"
out_folder_daily <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/test/cloud_masks/shadow/daily/"

dir.create(out_folder_mosaic)
dir.create(out_folder_daily)

# set Dark factor and kernel size
DARK_FACTOR <- 0.6

# kernel for cloud buffer
kernel <- matrix(1, nrow = 9, ncol = 9)

# === Filter for 2024 only ===
sr_files  <- files_list[grepl("2017", files_list)]
json_files <- json_files_list[grepl("2017", json_files_list)]
whiteness_files <- whiteness_list[grepl("2017", whiteness_list)]

################################################################################
# loop through json files and check for shadow
all_shadow <- list()

for (i in seq_along(json_files)) {
  
  json_file <- fromJSON(json_files[[i]])
  
  if (json_file$properties$shadow_percent > 0){
   
    # get id from files samples
    id <-  sub("_metadata.*$", "", basename(json_files[[i]]))
    
    cat("Processing:", id, "\n")
    
    all_shadow[[length(all_shadow) + 1]] <- data.frame(
      id = id,
      shadow_perc = json_file$properties$shadow_percent,
      stringsAsFactors = FALSE
    )
  }
}

test <- do.call(rbind, all_shadow)
ids <- test$id

# Create function to process for different modes
process_shadow <- function(id,
                           sr_files,
                           whiteness_files,
                           ref_summer = NULL,
                           ref_autumn = NULL,
                           mask,
                           out_folder,
                           mode,
                           white_thresh,
                           nir_thresh,
                           dark_factor = 0.6,
                           kernel_size = 9) {
  
  cat(" → Shadow masking:", id, "| mode:", mode, "\n")
  
  kernel <- matrix(1, kernel_size, kernel_size)
  
  # --- Load NIR ---
  nir_target <- rast(
    sr_files[sub("_PLA.*$", "", basename(sr_files)) %in% id],
    lyrs = 4
  )
  
  nir_target <- resample(nir_target, mask, method = "bilinear")
  
  # --- Load whiteness ---
  whiteness <- rast(
    whiteness_files[sub("_PLA.*$", "", basename(whiteness_files)) %in% id]
  )
  
  whiteness <- resample(whiteness, mask, method = "bilinear")
  
  # ===========================
  # Shadow logic by mode
  # ===========================
  
  if (mode == "summer") {
    
    shadow_mask <- (nir_target < (ref_summer * dark_factor)) &
      (nir_target < nir_thresh) &
      (whiteness < white_thresh)
    
  } else if (mode == "autumn") {
    
    shadow_mask <- (nir_target < (ref_autumn * dark_factor)) &
      (nir_target < nir_thresh) &
      (whiteness < white_thresh)
    
  } else {
    stop("Invalid mode")
  }
  
  shadow_mask <- shadow_mask * 1
  
  # --- Sieve ---
  sieved <- sieve(shadow_mask, threshold = 500, directions = 8)
  
  # --- Buffer ---
  cloud_buffer <- focal(sieved, w = kernel, fun = max, na.rm = TRUE)
  cloud_buffer <- crop(cloud_buffer, mask)
  
  writeRaster(
    cloud_buffer,
    file.path(paste0(out_folder, mode),
              paste0(id,
                     "_shadow_", mode,
                     "_nir", nir_thresh,
                     "_white", white_thresh,
                     ".tif")),
    overwrite = TRUE,
    datatype = "INT2S",
    NAflag = -9999
  )
}

################################################################################

###### --------- try function for different combinations -------- ##############

# 1. Mosaic references
## 1.1 Mode: only Summer as reference
## 1.2 Mode: only Autumn as reference
## 1.3 Mode: both

# 2. Mode: Daily Composites
## 2.1 Mode: only Summer as reference
## 2.2 Mode: only Autumn as reference
## 2.3 Mode: both

################################################################################

for (id in ids) {

  # 1.1 Summer only
process_shadow(
  id = "20170624_092523_1041", 
  sr_files = sr_files,
  whiteness_files = whiteness_files,
  
  ref_summer = mosaic_ref_sum,
  
  mask = mask,
  out_folder = out_folder_mosaic,
  mode = "summer",
  
  white_thresh = 500,
  nir_thresh = 3000
)

# 1.2 Autumn only

process_shadow(
  id = id,
  sr_files = sr_files,
  whiteness_files = whiteness_files,
  
  ref_autumn = mosaic_ref_autumn,
  
  mask = mask,
  out_folder = out_folder_mosaic,
  mode = "autumn",
  
  white_thresh = 500,
  nir_thresh = 3000
)

# 1.3 Both
'process_shadow(
  id = id,
  sr_files = sr_files,
  whiteness_files = whiteness_files,
  
  ref_summer = mosaic_ref_sum,
  ref_autumn = mosaic_ref_autumn,
  
  mask = mask,
  out_folder = out_folder_mosaic,
  mode = "combined",
  
  white_thresh = 500,
  nir_thresh = 3000
)'
}
########### Mode daily reference ########
# 2.1 Summer only 

process_shadow(
  id = id,
  sr_files = sr_files,
  whiteness_files = whiteness_files,
  
  ref_summer = daily_ref_sum,
  
  mask = mask,
  out_folder = out_folder_daily,
  mode = "summer",
  
  white_thresh = 500,
  nir_thresh = 3000
)

# 1.2 Autumn only

process_shadow(
  id = id,
  sr_files = sr_files,
  whiteness_files = whiteness_files,
  
  ref_autumn = daily_ref_autumn,
  
  mask = mask,
  out_folder = out_folder_daily,
  mode = "autumn",
  
  white_thresh = 500,
  nir_thresh = 3000
)

# 1.3 Both
process_shadow(
  id = id,
  sr_files = sr_files,
  whiteness_files = whiteness_files,
  
  ref_summer = daily_ref_sum,
  ref_autumn = daily_ref_autumn,
  
  mask = mask,
  out_folder = out_folder_daily,
  mode = "combined",
  
  white_thresh = 500,
  nir_thresh = 3000
)

