################################################################################

# Script to join wbs data with lafis 
# Script creates 4 outputs: 
#     1. centroids for each WBS shapefile
#     2. lafis_grassland_application_ST with WBS information
#     3. A new WBS file using lafis polygons 
#     4. lafis_grassland_application_MH_zones with wbs information

################################################################################
years <- c(2021)

# Function to load shapefiles 
load_shapefiles <- function(year) {
  list(
    wsb = st_read(glue("gis/wbs/CUAA_corrected/{year}/old/Wiesenbrueter_{year}.shp")),
    lafis = st_read(glue("/mnt/CEPH_PROJECTS/Environtwin/gis/ST/lafis/{year}/lafis_grassland_{year}_v4_32632_uniqueID.shp"))
  )
}

# Load all data
all_data <- lapply(years, load_shapefiles)
names(all_data) <- as.character(years)

# Create list to store centroids
centroid_list <- list()

for (i in seq_along(all_data)) {
  year <- names(all_data)[i]
  
  # Compute centroids ON the polygon layer
  centroid_list[[year]] <- st_point_on_surface(all_data[[i]]$wsb)
  
  outfile <- glue("/mnt/CEPH_PROJECTS/Environtwin/gis/wbs/CUAA_corrected/{year}/Wiesenbrueter_{year}_centroids.shp")
  message(glue("Writing {outfile}"))
  
  st_write(centroid_list[[year]], outfile, delete_layer = TRUE, quiet = TRUE)
}

######## join centroids with polygons from lafis
for (i in seq_along(centroid_list)) {
  year <- names(centroid_list)[i]
  wsb = centroid_list[[i]]
  lafis = all_data[[i]]$lafis
  
  if(crs(wsb) != crs(lafis))wsb <- st_transform(wsb, crs = 32632)
  
  # Spatial join: centroids → A polygons
  # Each centroid finds the polygon it falls within
  if (year != 2023){
    joined <- st_join(lafis,wsb, st_contains,
                      left = T) %>%
      mutate(application = if_else(!is.na(CUAA.y), 1, 0))
  } else {
    # only 2023
    joined <- st_join(lafis, wsb, st_contains, left = TRUE) %>%
      mutate(
        application = case_when(
          !is.na(CUAA.y) | !is.na(PPOL_CODIC) ~ 1L,
          TRUE ~ 0L
        )
      )
  }
  
  joined <- joined[, !grepl("\\.y$", names(joined))]
  names(joined) <- sub("\\.x$", "", names(joined)) 

  joined <- joined %>% 
    dplyr::select(-all_of(names(wsb)))
  
  outfile <- glue("/mnt/CEPH_PROJECTS/Environtwin/gis/lafis/MH/application/lafis_grassland_{year}_application_ST.shp")
  message(glue("Writing {outfile}"))
  st_write(joined, outfile, delete_layer = TRUE, quiet = TRUE)
  
  # write out only wbs applications
  wbs <- joined %>% 
    filter(application == 1)
  
  # Write the new cleaned polygons with attributes from B
  outfile <- glue("/mnt/CEPH_PROJECTS/Environtwin/gis/wbs/CUAA_corrected/{year}/Wiesenbrueter_{year}_lafis.shp")
  message(glue("Writing {outfile}"))
  st_write(wbs, outfile, delete_layer = TRUE, quiet = TRUE)
}

##################################################################################
#####################################################################
## Clip WSB the to zones 
# Load Mals zones once
mals <- st_read("/mnt/CEPH_PROJECTS/Environtwin/gis/outlines/Mals_zones_32632.shp") %>% 
  dplyr::select(fid, geometry)

names(mals) <- c("Zone", "geometry")

# Function to load shapefiles 
load_shapefiles <- function(year) {
  list(
    wsb = st_read(glue("/mnt/CEPH_PROJECTS/Environtwin/gis/wbs/CUAA_corrected/{year}/Wiesenbrueter_{year}_lafis.shp"))
  )
}

# Load all data
all_data <- lapply(years, load_shapefiles)
names(all_data) <- as.character(years)

# Clip function
clip_wsb <- function(wsb, mals, year) {
  message(glue("▶️ Processing {year} ..."))
  
  mals <- st_transform(mals, crs = st_crs(wsb))
  wsb <- st_make_valid(wsb)
  
  # intersect wbs files wiht mals zones --> creates multiple polygons with zone information, e.g. one polygon that
  # is in both zones is split and assigned to the two tones
  message("🔹 Clipping WSB fields to zones...")
  clipped <- st_intersection(wsb, mals)
  clipped <- clipped %>% filter(st_geometry_type(.) %in% c("POLYGON", "MULTIPOLYGON"))
  
  id_field <- "uniqu_d"
  if (is.na(id_field)) stop("❌ Could not find a field ID column.")
  
  # Compute area of each clipped piece
  clipped$piece_area <- as.numeric(st_area(clipped))
  
  # --- 3: Compute total area per original field --------------------------------
  total_area <- clipped %>%
    st_drop_geometry() %>% 
    group_by(.data[[id_field]]) %>%
    summarise(total_area = sum(piece_area), .groups = "drop")
  
  # join back with clipped 
  clipped <- clipped %>%
    left_join(total_area, by = id_field) %>%
    mutate(prop_area = piece_area / total_area)
  
  # --- 4: Determine major zone for each field ----------------------------------
  # Find the zone where, according to the piece area, the polygon actually belongs to
  major_zone_lookup <- clipped %>%
    st_drop_geometry() %>%
    group_by(.data[[id_field]]) %>%
    slice_max(piece_area, n = 1, with_ties = FALSE) %>%
    dplyr::select(.data[[id_field]], major_zone = Zone)
  
  clipped <- clipped %>% 
    left_join(major_zone_lookup, by = id_field)
  
  #clipped <- clipped %>%
  #  left_join(major_zone_lookup, by = id_field) %>%
  #  mutate(
  #    piece_area_m2 = as.numeric(piece_area),
  #    Zone = ifelse(prop_area < 1/7 & piece_area_m2 < 2000, major_zone, Zone)
  #  ) %>%
  #  dplyr::select(-piece_area, -total_area, -prop_area, -major_zone)
  
  # --- 5: Apply the 1/7 rule ----------------------------------------------------
  
  clipped <- clipped %>%
    mutate(
      Zone_final = ifelse(
        prop_area < 1/7,       # small fraction
        major_zone,            # → assign to major zone
        Zone                   # else keep original zone
      )
    )
  
  # --- 6: Union polygons back into final shapes --------------------------------
  final <- clipped %>%
    group_by(.data[[id_field]], Zone_final) %>%
    summarise(
      across(where(is.numeric), first),
      geometry = st_union(geometry),
      .groups = "drop"
    )
  
  #clipped <- clipped %>%
  #  left_join(major_zone_lookup, by = id_field) %>%
  #  mutate(
  #    piece_area_m2 = as.numeric(piece_area),
  #    Zone = ifelse(prop_area < 1/7 & piece_area_m2 < 2000, major_zone, Zone)
  #  ) %>%
  #  dplyr::select(-piece_area, -total_area, -prop_area, -major_zone)
  
  #clipped <- clipped %>%
  #  group_by(.data[[id_field]], Zone) %>%
  #  summarise(
  #    across(everything(), first),   # keeps first value of all other columns
  #    geometry = st_union(geometry),
  #    .groups = "drop"
  #  )
  
  outfile <- glue("/mnt/CEPH_PROJECTS/Environtwin/gis/wbs/wbs_with_zones/{year}/WBS_{year}_32632_zones.shp")
  message(glue("💾 Writing {outfile}"))
  st_write(clipped, outfile, delete_layer = TRUE, quiet = TRUE)
  
  return(clipped)
}

# Run for all years
for (i in years) {
  clip_wsb(all_data[[as.character(i)]]$wsb, mals, i)
}

#################################################################################
## Create application file only for mals heath with zones
#years <- c(2023)

# Function to load shapefiles 
load_shapefiles <- function(year) {
  list(
    wsb = st_read(glue("gis/wbs/wbs_with_zones/{year}/WBS_{year}_32632_zones.shp")),
    lafis = st_read(glue("/mnt/CEPH_PROJECTS/Environtwin/gis/ST/lafis/{year}/lafis_grassland_{year}_v4_32632_zones.shp"))
  )
}

# Load all data
all_data <- lapply(years, load_shapefiles)
names(all_data) <- as.character(years)

# Create list to store centroids
centroid_list <- list()

for (i in seq_along(all_data)) {
  year <- names(all_data)[i]
  
  # Compute centroids ON the polygon layer
  centroid_list[[year]] <- st_point_on_surface(all_data[[i]]$wsb)
}

######## join centroids with polygons from lafis
for (i in seq_along(centroid_list)) {
  year <- names(centroid_list)[i]
  wsb = centroid_list[[i]]
  lafis = all_data[[i]]$lafis
  
  if(crs(wsb) != crs(lafis))wsb <- st_transform(wsb, crs = 32632)
  
  # Spatial join: centroids → A polygons
  # Each centroid finds the polygon it falls within
  if (year != 2020){
    joined <- st_join(lafis, wsb, st_contains, left = TRUE) %>%
      dplyr::select(-ends_with(".y"), -uniqu_d) %>%
      rename_with(~ sub("\\.x$", "", .x), ends_with(".x"))
  } else {
    # only 2020
    joined <- st_join(lafis, wsb, st_contains, left = TRUE) %>%
      mutate(application = if_else(!is.na(unique_id.y), 1, 0))
    
    joined <- joined[, !grepl("\\.y$", names(joined))]
    names(joined) <- sub("\\.x$", "", names(joined)) 
  }
  
  outfile <- glue("/mnt/CEPH_PROJECTS/Environtwin/gis/lafis/MH/application/lafis_grassland_{year}_application_MH_zones.shp")
  message(glue("Writing {outfile}"))
  st_write(joined, outfile, delete_layer = TRUE, quiet = TRUE)
}
