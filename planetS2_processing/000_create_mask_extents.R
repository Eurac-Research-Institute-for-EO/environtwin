#############################################
## Create extent mask for test sites
#############################################
library(terra)
library(sf)

# Load raster and vector
ras <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X-001_Y-001/20170320_092734_0e26_PLANET_udm2.tif")
mals_outline <- st_read("/mnt/CEPH_PROJECTS/Environtwin/gis/Mals_zones_32632.shp")

# Convert raster extent to an sf polygon
ras_ext_sf <- st_as_sfc(st_bbox(ras))
ras_ext_sf <- st_sf(geometry = ras_ext_sf)
plot(ras_ext_sf)

# Merge raster extent polygon with shapefile
merged_sf <- st_intersection(mals_outline, ras_ext_sf)
plot(merged_sf)

# Rasterize using the original raster as template
r_mask <- rasterize(merged_sf, ras, field=1, background=0)

# Plot for checking
plot(r_mask)

writeRaster(r_mask, "/mnt/CEPH_PROJECTS/Environtwin/gis/mals_Pcube_mask.tif", overwrite=TRUE)

# Crop raster to vector boundaries so that it aligns with the cube
r_mask_crop <- crop(r_mask, vector_sub)
plot(r_mask_crop)

#writeRaster(r_mask_crop, "/mnt/CEPH_PROJECTS/Environtwin/gis/PA6_mask.tif", overwrite=TRUE)

r_mask_crop_vec <- as.polygons(r_mask, dissolve = TRUE)

writeVector(r_mask_crop, "/mnt/CEPH_PROJECTS/Environtwin/gis/mals", overwrite=TRUE)

# Plot
plot(r_mask_crop_vec)

# write to disk
writeRaster(r_mask, "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_small/mals_mask_large.tif", overwrite=TRUE)

#################### Rasterize polygons for PA
vector <- vect("/mnt/CEPH_PROJECTS/Environtwin/gis/misc/test_sites_4326.shp")
vector <- vect("/mnt/CEPH_PROJECTS/Environtwin/gis/wbs/outlines/mals_ohneBMS_32632.shp") 

# Reproject vector to raster CRS 
vector <- project(vector, crs(ras))

# Create an empty raster based on polygon extent
# subset vector
vector_sub <- vector[vector$group_id == 6, ]
#vector_sub <- vector[vector$group_id %in% c(4, 9), ]

#################################################################################################
#--------------------------------------------------------------
# 1. Create polygon-only mask (no raster input)
#--------------------------------------------------------------
# Subset polygon of interest (vector_sub already in correct CRS)
r <- rast(ext(vector), resolution = 3)  # choose resolution (units same as CRS)
crs(r) <- crs(vector)

# Rasterize polygon → 1 inside, 0 outside
r_mask <- rasterize(vector, ras, field = 1, background = 0)
plot(r_mask)

# Optional: mask to polygon (same result here, but keeps pure inside polygon)
r_masked <- crop(mask(r_mask, vector), vector)
plot(r_masked)
r_masked[is.na(r_masked)] <- 0   # set NA to 0 (binary mask)

# Save
writeRaster(r_masked, "/mnt/CEPH_PROJECTS/Environtwin/gis/masks/MH_mask.tif",overwrite = TRUE,
            datatype = "INT2S",
            NAflag = -9999)

plot(r_masked)

#--------------------------------------------------------------
# 2. Align mask with FORCE raster cube (same extent, resolution, CRS)
#--------------------------------------------------------------
#vector_sub <- vector[vector$group_id == 3, ]

ras1 <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0007_Y0000/20250625_103733_92_2526_PLANET_udm2_mask.tif")
ras2 <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X0007_Y-001/20250928_103547_43_252b_PLANET_udm2_mask.tif")

ras <- mosaic(ras1, ras2)
plot(ras[[1]])

# Reproject polygon if needed
vector_sub_ras <- project(vector, crs(ras))

# Directly rasterize to match the cube grid exactly 
r_mask_cube <- rasterize(vector_sub_ras, ras, field = 1, background = 0)

plot(r_mask_cube)

# Save the cube-aligned mask
writeRaster(r_mask_cube, "/mnt/CEPH_PROJECTS/Environtwin/gis/masks/MH_mask_cube.tif", datatype = "INT2S",
            NAflag = -9999, overwrite = TRUE)

