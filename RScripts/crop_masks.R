library(terra)

folder_names <- list(
  "MH" = "X-001_Y-001",
 "SA" = c("X0006_Y0000", "X0006_Y0001"), 
"TH" = "X0004_Y0002", 
"PG" = c("X0007_Y0000","X0007_Y-001"),
"R" = "X-002_Y-002", 
  "FSP" = "X0009_Y-001",
"AW" = c("X0008_Y0000", "X0008_Y-001")
)

in_dir <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/masks"
mask_dir <- "/mnt/CEPH_PROJECTS/Environtwin/gis/masks/"

out_dir_base <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/masks/"

# List frost / gdd / lafis
products <- list.dirs(in_dir, recursive = FALSE, full.names = TRUE)
products <- products[[2]]

for (prod_path in products) {
  
  prod_name <- basename(prod_path)
  cat("\nProcessing:", prod_name, "\n")
  
  for (mask in names(folder_names)) {
    
    mask_tiles <- as.vector(folder_names[[mask]])
    mask_file <- file.path(mask_dir, paste0(mask, "_mask.tif"))
    
    if (!file.exists(mask_file)) {
      cat("Missing mask:", mask_file, "\n")
      next
    }
    
    m <- rast(mask_file)
    
    cat("Mask:", mask, "\n")
    
    # 1️⃣ Use FIRST tile to determine filenames
    first_tile_path <- file.path(prod_path, mask_tiles[1])
    
    if (!dir.exists(first_tile_path)) {
      cat("Missing tile:", first_tile_path, "\n")
      next
    }
    
    filenames <- basename(list.files(first_tile_path, pattern="\\.tif$", full.names=TRUE))
    
    # 2️⃣ Process EACH file separately
    for (fname in filenames) {
      
      rasters_to_merge <- c()
      
      for (tile in mask_tiles) {
        
        tile_path <- file.path(prod_path, tile)
        
        fpath <- file.path(tile_path, fname)
        
        if (file.exists(fpath)) {
          rasters_to_merge <- c(rasters_to_merge, fpath)
        }
      }
      
      if (length(rasters_to_merge) == 0) next
      
      if (length(rasters_to_merge) == 1) {
        
        r_merged <- rast(rasters_to_merge)
        
      } else {
        
        #Load & merge
        r_list <- lapply(rasters_to_merge, rast)
        r_merged <- do.call(mosaic, r_list)
      }
      
      r_masked <- terra::mask(crop(r_merged, m), m)
      

      out_dir <- file.path(prod_path, mask)
      dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
      
      out_file <- file.path(out_dir, fname)
      
      writeRaster(r_masked, out_file, overwrite = TRUE)
      
      cat("Written:", fname, "\n")
    }
  }
}
