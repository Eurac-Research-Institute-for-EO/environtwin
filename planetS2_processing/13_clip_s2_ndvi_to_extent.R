library(terra)
library(tools)

ndvi_files <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/SEN2/"
out_dir_base <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/SEN2/"
mask_files <- "/mnt/CEPH_PROJECTS/Environtwin/gis/masks/"

sites <- c("AW", "SA")

for (i in 1:length(sites)) {  
  site <- sites[i]
  ras_files <- list.files(paste0(ndvi_files, site ,"/data/mosaic/"), pattern = "\\.vrt$", full.names = T)
  mask <- rast(paste0(mask_files, site, "_mask.tif"))
  
  for (j in 1:length(ras_files)){
    ras <- rast(ras_files[j])
    ras_clip <- crop(ras, mask, mask = TRUE)  
    base_name <- file_path_sans_ext(basename(ras_files[j]))
    out_file <- paste0(out_dir_base, site ,"/data/" ,base_name, ".tif")
    
    writeRaster(ras_clip, out_file, overwrite = TRUE)
  }
}
