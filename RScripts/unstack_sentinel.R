library(terra)
library(future.apply)
library(purrr)

ndvi_folders <- c("/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level3/indices/environtwin/X0000_Y0003",
                  "/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level3/indices/environtwin/X0002_Y0003",
                  "/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level3/indices/environtwin/X0002_Y0005",
                  "/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level3/indices/environtwin/X0003_Y0004",
                  "/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level3/indices/environtwin/X0004_Y0004",
                  "/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/level3/indices/environtwin/X0000_Y0004")
  
out_root <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/S2_NDVI"
workers <- 2

unstack_s2 <- function(ndvi_folder) {
  
  folder_name <- basename(normalizePath(ndvi_folder))
  out_folder  <- file.path(out_root, folder_name)
  dir.create(out_folder, recursive = TRUE, showWarnings = FALSE)
  
  ndvi_files <- list.files(
    ndvi_folder,
    pattern = "^2025.*_NDV_TSS.*\\.tif$",
    full.names = TRUE
  )
  
  # Process every file found
  for (file in ndvi_files) {
    ndvi_stack <- rast(file)
    
    # Split into individual layers
    layers <- as.list(ndvi_stack)
    layer_names <- names(ndvi_stack)
    
    # Write each layer
    for (i in seq_along(layers)) {
      out_file <- file.path(out_folder, paste0(layer_names[i], ".tif"))
      writeRaster(layers[[i]], out_file, overwrite = TRUE)
    }
  }
  
  return(out_folder)
}

plan(multisession, workers = workers)

all_results <- future_lapply(ndvi_folders, unstack_s2)

message("✅ All folders processed. Output folders: ", length(all_results))