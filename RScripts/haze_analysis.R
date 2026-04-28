library(raster)

# define input folders
haze_files <- read.csv("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/Planet_status_info.csv")

sr_path <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/coregistered"
udm_path <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard"

sr_files <- list.files(sr_path, pattern = "_BOA\\.bsq$", full.names = TRUE)
udm_files <- list.files(udm_path, pattern = "_udm2_buffer\\.tif$", full.names = TRUE)
json_files <- list.files(udm_path, pattern = "_metadata\\.json$", full.names = TRUE)

haze_summary <- haze_files %>% 
  group_by(year) %>%
  summarise(
    total_images = n(),
    haze_images = sum(haze_light > 45 | haze_heavy > 45, na.rm = TRUE)
  )

# get id from image and create a new raster with the image bands, the haze bands and a layer giving info about the percentage of haze
ids <- haze_files %>% 
  filter(gc_present == "TRUE" & (haze_light > 45 | haze_heavy > 45)) %>% 
  pull(id)

for(i in ids){
  print(i)
  
  # --- SR raster ---
  sr_file <- sr_files[sub("_PLANET_BOA.*$", "", basename(sr_files)) == i]
  if(length(sr_file) == 0) next
  
  r <- rast(sr_file)
  
  # --- UDM raster ---
  udm_file <- udm_files[sub("_PLANET_udm2_buffer.*$", "", basename(udm_files)) == i]
  if(length(udm_file) == 0) next
  
  udm <- rast(udm_file)
  
  # --- JSON (FIX: use jsonlite, not rast) ---
  json_file <- json_files[sub("_metadata.*$", "", basename(json_files)) == i]
  if(length(json_file) == 0) next
  
  meta <- fromJSON(json_file)
  
  # haze percentage
  heavy_haze <- meta$properties$heavy_haze_percent
  ligth_haze <- meta$properties$light_haze_percent
  
  # --- Create constant raster ---
  perc_r <- rast(r[[1]])
  values(perc_r) <- percentage
  
  # --- Get haze values from CSV ---
  haze_row <- haze_files %>% filter(id == i)
  
  haze_light_val <- haze_row$haze_light
  haze_heavy_val <- haze_row$haze_heavy
  
  haze_light_r <- rast(r[[1]])
  values(haze_light_r) <- haze_light_val
  
  haze_heavy_r <- rast(r[[1]])
  values(haze_heavy_r) <- haze_heavy_val
  
  # --- Stack ---
  final <- c(r, udm, haze_light_r, haze_heavy_r, perc_r)
  
  names(final) <- c(
    names(r),
    names(udm),
    "haze_light",
    "haze_heavy",
    "haze_percent"
  )
  
  # --- Save ---
  out_path <- file.path("output_folder", paste0(i, "_stack.tif"))
  writeRaster(final, out_path, overwrite=TRUE)
}
