################################################################################

## LAFIS and WBS data handling. Reprojecting, clipping and applying zones, 
## selecting only necessary data.

################################################################################

library(sf)
library(lwgeom)
library(dplyr)
library(glue)
library(purrr)

# Define years
years <- c(2021:2025)

# Load Mals zones once
mals <- st_read("/mnt/CEPH_PROJECTS/Environtwin/gis/outlines/Mals_zones_32632.shp") %>% 
  dplyr::select(fid, geometry)

names(mals) <- c("Zone", "geometry")

# Function to detect available LAFIS shapefiles for a year
detect_lafis_versions <- function(year, base_path = "/mnt/CEPH_PROJECTS/Environtwin/gis/ST/lafis/") {
  year_path <- glue("{base_path}/{year}")
  all_files <- list.files(year_path, pattern = "\\.shp$", full.names = TRUE)
  
  # Only keep the 3 expected shapefile naming patterns
  keep_files <- grep(
    glue("(lafis_grassland_{year}_v1\\.shp$)|(lafis_grassland_{year}_v4\\.shp$)"),
    all_files, value = TRUE
  )
  
  # If none exist, skip 
  if (length(keep_files) == 0) {
    message(glue("⚠️ No matching LAFIS shapefiles found for {year}"))
    return(NULL)
  }
  
  # Detect version label
  tibble(
    year = year,
    path = keep_files,
    version = dplyr::case_when(
      grepl("v1\\.shp$", keep_files) ~ "v1",
      grepl("v4\\.shp$", keep_files) ~ "v4"
    )
  )
}

# Function to load any LAFIS shapefile
load_lafis <- function(filepath) {
  st_read(filepath, quiet = TRUE)
}

# harmonise all column names
harmonise_lafis <- function(lafis, year) {
  
  # ---- Rename columns to a common schema -----------------------------
  if (year %in% c(2017, 2018, 2023)) {
    lafis <- lafis %>%
      rename(
        FID_std  = FID,
        DESCR = DESCRIPT_1
      ) %>%
      mutate(unique_id = paste0(CUAA, "-", FID_std)) %>%
      dplyr::select(
        FID_std, CUAA, CODE, DESCR, SHEET_28, unique_id, geometry
      )
  }
  
  if (year == 2019) {
    lafis <- lafis %>%
      rename(
        FID_std   = PROG_POLIG,
        DESCR = DESCRIPT00
      ) %>%
      mutate(unique_id = paste0(CUAA, "-", FID_std)) %>%
      dplyr::select(
        FID_std, CUAA, CODE, DESCR, SHEET_28, unique_id, geometry
      )
  }
  
  if (year %in% c(2020:2022, 2024)) {
    lafis <- lafis %>%
      rename(
        FID_std    = "PROG_POLIG",
        DESCR  = "DESCR_DE"
      ) %>%
      mutate(unique_id = paste0(CUAA, "-", FID_std)) %>%
      dplyr::select(
        FID_std, CUAA, CODE, DESCR, SHEET_28, unique_id, geometry
      )
  }
  
  if (year == 2025) {
    lafis <- lafis %>%
      rename(
        FID_std    = FID_1,
        DESCR  = DESCRIPT_1
      ) %>%
      mutate(unique_id = paste0(CUAA, "-", FID_std)) %>%
      dplyr::select(
        FID_std, CUAA, CODE, DESCR, SHEET_28, unique_id, geometry
      )
  }
  
  
  return(lafis)
}


# Clip function (same as before, slightly generalized)
clip_lafis <- function(lafis, mals, year, version, output_base = "/mnt/CEPH_PROJECTS/Environtwin/gis/ST/lafis/") {
  message(glue("Processing {year} ({version})..."))
  
  # --- 1: Make polygons valid ----------------------------------------------------------
  lafis <- st_transform(lafis, crs = st_crs(mals))
  lafis <- st_make_valid(lafis)
  #lafis <- clean_field_names(lafis)
  lafis <- harmonise_lafis(lafis, year)
  
  # saving lafis as 32632 with new id
  outfile <- glue("{output_base}/{year}/lafis_grassland_{year}_{version}_32632_uniqueID.shp")
  message(glue("Writing {outfile}"))
  st_write(lafis, outfile, delete_layer = TRUE, quiet = TRUE)
  
  # --- 2: Clipping --------------------------------------------------------------
  message("Clipping LAFIS fields to zones...")
  clipped <- st_intersection(lafis, mals)
  #clipped <- clipped %>% filter(st_geometry_type(.) %in% c("POLYGON", "MULTIPOLYGON"))
  
  id_field <- "unique_id"
  if (is.na(id_field)) stop("Could not find a field ID column.")
  
  # Compute area of each clipped piece
  clipped$piece_area <- as.numeric(st_area(clipped))
  
  # --- 3: Compute total area per original field --------------------------------
  total_area <- clipped %>%
    st_drop_geometry() %>% 
    group_by(.data[[id_field]]) %>%
    summarise(total_area = sum(piece_area), .groups = "drop")
  
  clipped <- clipped %>%
    left_join(total_area, by = id_field) %>%
    mutate(prop_area = piece_area / total_area)
  
  # --- 4: Determine major zone for each field ----------------------------------
  major_zone_lookup <- clipped %>%
    st_drop_geometry() %>%
    group_by(.data[[id_field]]) %>%
    slice_max(piece_area, n = 1, with_ties = FALSE) %>%
    dplyr::select(.data[[id_field]], major_zone = Zone)
  
  clipped <- clipped %>% left_join(major_zone_lookup, by = id_field)
  
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
    group_by(.data[[id_field]], Zone_final, CUAA, DESCR) %>%
    summarise(
      across(where(is.numeric), first),
      geometry = st_union(geometry),
      .groups = "drop"
    ) 
  
  outfile <- glue("{output_base}/{year}/lafis_grassland_{year}_{version}_32632_zones.shp")
  message(glue("Writing {outfile}"))
  st_write(final, outfile, delete_layer = TRUE, quiet = TRUE)
  
  return(clipped)
}

# Auto-detect and process all years + versions
all_versions <- map_df(years, detect_lafis_versions)

all_clipped <- list()

for (i in seq_len(nrow(all_versions))) {
  yr <- all_versions$year[i]
  ver <- all_versions$version[i]
  path <- all_versions$path[i]
  
  lafis <- load_lafis(path)
  clipped <- clip_lafis(lafis, mals, yr, ver)
  #simplified <- simplify_lafis(clipped, yr, ver)
  
  all_clipped[[paste0(yr, "_", ver)]] <- clipped
  #all_simplified[[paste0(yr, "_", ver)]] <- simplified
}

message("All years and versions processed successfully!")

###############################################################################
## clip lafis grassland with application info
# Function to load shapefiles 
# Function to load shapefiles 
load_shapefiles <- function(year) {
  list(
    wsb = st_read(glue("gis/wbs/CUAA_corrected/{year}/lafis_grassland_{year}_application_ST.shp"))
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
  
  outfile <- glue("/mnt/CEPH_PROJECTS/Environtwin/gis/wbs/CUAA_corrected/{year}/lafis_grassland_{year}_application_MH_zones.shp")
  message(glue("💾 Writing {outfile}"))
  st_write(clipped, outfile, delete_layer = TRUE, quiet = TRUE)
  
  return(clipped)
}

# Run for all years
for (i in years) {
  clip_wsb(all_data[[as.character(i)]]$wsb, mals, i)
}
