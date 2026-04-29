library(sf)
library(terra)
library(ggplot2)
library(ggpubr)
library(ggpmisc)
library(patchwork)

# Load polygon and DEM class data
dem_classes <- rast("/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/masks/dem_copernicus/DEM.vrt")
lafis_sub <- read_sf("/mnt/CEPH_PROJECTS/Environtwin/gis/analysis/lafis_fc.gpkg")

gdd_dir <- "/mnt/CEPH_PROJECTS/Environtwin/GDD/SouthTyrol/gdds/"

#lafis_dem_extr <- extract(dem_classes, lafis_full, fun = modal, na.rm = TRUE)
#lafis_dem_extr <- cbind(lafis_full, lafis_dem_extr)

# reclassfiy dem
# Create a reclassification matrix: [from, to, value]
rcl <- matrix(c(
  -Inf, 250, NA,       # Below 250 m become NA
  250, 800, 1,         # 1 = 250–800 m
  800, 1800, 2,        # 2 = 800–1800 m
  1800, 2200, 3,       # 3 = 1800–2200 m
  2200, Inf, NA        # Above 2200 m become NA
), ncol = 3, byrow = TRUE)

# Reclassify
dem_classes_new <- classify(dem_classes, rcl, right = TRUE)
plot(dem_classes_new)

# Save output if needed
writeRaster(dem_classes_new, "/mnt/CEPH_PROJECTS/Environtwin/dem_reclass.tif", overwrite =T)

# Extract dem classes per polygon once
dem_extr <- terra::extract(dem_classes_new, lafis_sub, fun = modal, na.rm = TRUE)
dem_extr_lafis <- cbind(lafis_sub, dem_extr)
names(dem_extr_lafis)
names(dem_extr_lafis)[24] <- "dem_class"

# Convert sf to SpatVector for terra compatibility
dem_extr_vect <- vect(as(dem_extr_lafis, "Spatial"))
years = 2020:2024

# Helper to assign GDD values at dynamic DOY columns per date column name
assign_gdd_at_doy <- function(df, doy_col_name, gdd_suffix) {
  gdd_values <- numeric(nrow(df))
  for(i in 1:nrow(df)) {
    doy_col <- paste0("DOY_", sprintf("%03d", df[[doy_col_name]][i]))
    if(doy_col %in% colnames(df)) {
      gdd_values[i] <- df[i, doy_col]
    } else {
      gdd_values[i] <- NA
    }
  }
  df[[paste0("GDD_at_", gdd_suffix)]] <- as.numeric(gdd_values)
  return(df)
}

function_gdd_multiyear <- function(year, gdd_dir, dem_vect, dem_df) {
  message(paste("Processing year:", year))
  
  # Load GDD raster for the year
  gdd_file <- paste0(gdd_dir, "GDD_BT5_", year, "_dynamic_cumulative.tif")
  gdd <- rast(gdd_file)
  
  # Extract mean GDD per polygon
  extracted_vals <- extract(gdd, dem_vect, fun = mean, na.rm = TRUE)
  
  # Combine extracted values (exclude ID)
  lafis_with_vals <- cbind(dem_df, extracted_vals[,-1])
  
  # Rename raster column names (DOY)
  start_idx <- ncol(dem_df) 
  end_idx <- ncol(lafis_with_vals) -1
  doy <- substr(names(lafis_with_vals)[start_idx:end_idx], 33, 35)
  doy_names <- paste0("DOY_", doy)
  names(lafis_with_vals)[start_idx:end_idx] <- doy_names
  
  # Define your date columns relevant for the years (adjust names as per your data)
  date_cols_map <- list(
    "2024" = "firstM",
    "2023" = "first2023",
    "2022" = "first2022",
    "2021" = "first2021",
    "2020" = "first2020"
  )
  
  # Convert the date columns to Date type (for all years, once)
  for(dc in unique(date_cols_map)) {
    if(dc %in% colnames(lafis_with_vals)) {
      if(!inherits(lafis_with_vals[[dc]], "Date")) {
        lafis_with_vals[[paste0(dc, "_date")]] <- as.Date(as.character(lafis_with_vals[[dc]]), format = "%Y%m%d")
      } else {
        lafis_with_vals[[paste0(dc, "_date")]] <- lafis_with_vals[[dc]]
      }
    } else {
      warning(paste("Column", dc, "not found in lafis_with_vals. Skipping."))
    }
  }
  
  # Extract DOY from the date columns
  for(year_k in names(date_cols_map)) {
    dc <- date_cols_map[[year_k]]
    doy_col_name <- paste0("DOY_", year_k)
    lafis_with_vals[[doy_col_name]] <- as.integer(format(lafis_with_vals[[paste0(dc, "_date")]], "%j"))
  }
  
  # Assign GDD_at_ columns using helper for all date columns
  for(year_k in names(date_cols_map)) {
    dc <- date_cols_map[[year_k]]
    doy_col_name <- paste0("DOY_", year_k)
    lafis_with_vals <- assign_gdd_at_doy(lafis_with_vals, doy_col_name, dc)
  }
  
  dem_levels <- c(1, 2, 3)
  dem_labels <- c(
    "C.–submontane (250–800 m a.s.l.)",
    "Montane zone (800–1800 m a.s.l.)",
    "Subalpine zone (1800–2200 m a.s.l.)"
  )
  
  # If dem_class may be numeric or character, coerce to numeric first (only if needed)
  # lafis_with_vals$dem_class <- as.numeric(as.character(lafis_with_vals$dem_class))
  lafis_with_vals$dem_class <- factor(lafis_with_vals$dem_class,
                                      levels = dem_levels,
                                      labels = dem_labels)
  
  # build the dynamic column names for the given year
  gdd_col <- paste0("GDD_at_", date_cols_map[[as.character(year)]])
  doy_col <- paste0("DOY_", year)
  
  # confirm columns exist
  if(! (gdd_col %in% colnames(lafis_with_vals) && doy_col %in% colnames(lafis_with_vals)) ) {
    stop("Missing required columns: ", paste(setdiff(c(gdd_col, doy_col), colnames(lafis_with_vals)), collapse = ", "))
  }
  
  # --- compute R2 per dem_class robustly ---
  r2_data <- lafis_with_vals %>%
    group_by(dem_class) %>%
    summarise(
      r2 = {
        # use .data pronoun to safely access dynamic names inside dplyr verbs
        gdd_var <- .data[[gdd_col]]
        doy_var <- .data[[doy_col]]
        
        # only fit model if enough non-NA observations
        ok <- which(!is.na(gdd_var) & !is.na(doy_var))
        if(length(ok) >= 3) {
          model <- lm(gdd_var[ok] ~ doy_var[ok])
          summary(model)$r.squared
        } else {
          NA_real_
        }
      },
      .groups = "drop"
    ) %>%
    mutate(label = paste0("R² = ", ifelse(is.na(r2), "NA", format(round(r2, 2), nsmall = 2))))
  

  # --- plotting (place r2 label inside each facet) ---
  p <- ggplot(lafis_with_vals, aes(
    x = !!rlang::sym(doy_col),
    y = !!rlang::sym(gdd_col),
    color = dem_class
  )) +
    geom_point(size = 2, alpha = 0.75) +
    geom_smooth(method = "lm", se = FALSE, linewidth = 0.9, color = "black") +
    # Put R2 in each facet: inherit.aes = FALSE is fine because r2_data contains dem_class
    geom_text(
      data = r2_data,
      aes(label = label),
      x = Inf, y = Inf,
      hjust = 1.1, vjust = 1.5,
      size = 4, color = "black",
      inherit.aes = FALSE
    ) +
    facet_wrap(~ dem_class, scales = "fixed") +
    labs(
      x = "Day of Year (DOY)",
      y = expression("Growing Degree Days (GDD, " * degree*C * ")"),
      color = "Elevation zone",
      title = paste("Relationship between DOY and GDD in", year)
    ) +
    theme_bw(base_size = 14) +
    theme(
      plot.title = element_text(face = "bold", size = 15, hjust = 0.5),
      axis.title = element_text(face = "bold"),
      axis.title.x = element_text(margin = margin(t = 15)),
      strip.text = element_text(face = "bold", size = 12),
      panel.grid.major = element_line(color = "gray85", linewidth = 0.4),
      panel.grid.minor = element_blank(),
      legend.position = "bottom",
      legend.title = element_text(face = "bold"),
      legend.text = element_text(size = 11)
    ) +
    scale_color_brewer(palette = "Dark2") +
    coord_cartesian(ylim = c(0, 1500))+
    scale_x_continuous(limits = c(100, 200), breaks = seq(100, 200, by = 20))
  
  print(p)
  
  ggsave(filename = paste0("/mnt/CEPH_PROJECTS/Environtwin/images/GDD_DOY/GDD_DOY_", year, ".png"), 
         plot = p, width = 10, height = 6, dpi = 80)
  
  
  return(lafis_with_vals)
}

all_results <- list()

for(y in years) {
  res <- function_gdd_multiyear(y, gdd_dir, dem_vect = dem_extr_vect, dem_df = dem_extr_lafis)
  res$year <- y
  all_results[[as.character(y)]] <- res
}

#####################################################################
##### For WSB data Mals Heath 
wsb <- read_sf("/mnt/CEPH_PROJECTS/Environtwin/gis/wsb_fc_final.shp")
zones <- read_sf("/mnt/CEPH_PROJECTS/Environtwin/gis/Mals_zones_32632.shp")

# Fix invalid geometries
zones <- st_make_valid(zones)
wsb   <- st_make_valid(wsb)

# Intersection (suppress warnings about mixed geometry)
wsb_zones <- suppressWarnings(st_intersection(wsb, zones))

# Drop empty geometries
wsb_zones <- wsb_zones[!st_is_empty(wsb_zones), ]

# Drop geometry collections and keep only polygons
wsb_zones <- st_collection_extract(wsb_zones, "POLYGON")

# Drop geometries that are too small or invalid (e.g., <4 points)
wsb_zones <- wsb_zones %>%
  filter(st_geometry_type(.) %in% c("POLYGON", "MULTIPOLYGON"))

# Drop zero-area or tiny polygons
wsb_zones <- wsb_zones[st_area(wsb_zones) > units::set_units(1, "m^2"), ]

# Repair topology if needed
wsb_zones <- st_make_valid(wsb_zones)

names(wsb_zones)[24] <- "Zones"

# Helper to assign GDD values at dynamic DOY columns per date column name
assign_gdd_at_doy <- function(df, doy_col_name, gdd_suffix) {
  gdd_values <- numeric(nrow(df))
  for(i in 1:nrow(df)) {
    doy_col <- paste0("DOY_", sprintf("%03d", df[[doy_col_name]][i]))
    if(doy_col %in% colnames(df)) {
      gdd_values[i] <- df[i, doy_col]
    } else {
      gdd_values[i] <- NA
    }
  }
  df[[paste0("GDD_at_", gdd_suffix)]] <- as.numeric(gdd_values)
  return(df)
}

function_gdd_multiyear <- function(year, gdd_dir, vect) {
  message(paste("Processing year:", year))
  
  # Load GDD raster for the year
  gdd_file <- paste0(gdd_dir, "GDD_BT5_", year, "_dynamic_cumulative.tif")
  gdd <- rast(gdd_file)
  
  # Extract mean GDD per polygon
  extracted_vals <- extract(gdd, vect, fun = mean, na.rm = TRUE)
  
  # Merge extract results back by ID (safe join)
  wsb_with_vals <- vect
  wsb_with_vals$ID <- 1:nrow(vect)
  extracted_vals <- extracted_vals[!duplicated(extracted_vals$ID), ]  # avoid duplicates
  wsb_with_vals <- merge(wsb_with_vals, extracted_vals, by = "ID", all.x = TRUE)
  
  # Rename raster column names (DOY)
  start_idx <- ncol(vect) 
  end_idx <- ncol(wsb_with_vals) -1
  doy <- substr(names(wsb_with_vals)[start_idx:end_idx], 33, 35)
  doy_names <- paste0("DOY_", doy)
  names(wsb_with_vals)[start_idx:end_idx] <- doy_names
  
  # Define your date columns relevant for the years (adjust names as per your data)
  date_cols_map <- list(
    "2024" = "first2024",
    "2023" = "first2023",
    "2022" = "first2022",
    "2021" = "first2021"
  )
  
  # Convert the date columns to Date type (for all years, once)
  for(dc in unique(date_cols_map)) {
    if(dc %in% colnames(wsb_with_vals)) {
      if(!inherits(wsb_with_vals[[dc]], "Date")) {
        wsb_with_vals[[paste0(dc, "_date")]] <- as.Date(as.character(wsb_with_vals[[dc]]), format = "%Y%m%d")
      } else {
        wsb_with_vals[[paste0(dc, "_date")]] <- wsb_with_vals[[dc]]
      }
    } else {
      warning(paste("Column", dc, "not found in wsb_with_vals. Skipping."))
    }
  }
  
  # Extract DOY from the date columns
  for(year_k in names(date_cols_map)) {
    dc <- date_cols_map[[year_k]]
    doy_col_name <- paste0("DOY_", year_k)
    wsb_with_vals[[doy_col_name]] <- as.integer(format(wsb_with_vals[[paste0(dc, "_date")]], "%j"))
  }
  
  # Assign GDD_at_ columns using helper for all date columns
  for(year_k in names(date_cols_map)) {
    dc <- date_cols_map[[year_k]]
    doy_col_name <- paste0("DOY_", year_k)
    wsb_with_vals <- assign_gdd_at_doy(wsb_with_vals, doy_col_name, dc)
  }
  

  # build the dynamic column names for the given year
  gdd_col <- paste0("GDD_at_", date_cols_map[[as.character(year)]])
  doy_col <- paste0("DOY_", year)
  
  # confirm columns exist
  if(! (gdd_col %in% colnames(wsb_with_vals) && doy_col %in% colnames(wsb_with_vals)) ) {
    stop("Missing required columns: ", paste(setdiff(c(gdd_col, doy_col), colnames(wsb_with_vals)), collapse = ", "))
  }
  
  r2_data <- wsb_with_vals %>%
    group_by(Zones) %>%
    summarise(
      r2 = {
        gdd_var <- .data[[gdd_col]]
        doy_var <- .data[[doy_col]]
        ok <- which(!is.na(gdd_var) & !is.na(doy_var))
        if (length(ok) >= 3) {
          summary(lm(gdd_var[ok] ~ doy_var[ok]))$r.squared
        } else NA_real_
      },
      .groups = "drop"
    ) %>%
    mutate(label = paste0("R² = ", ifelse(is.na(r2), "NA", format(round(r2, 2), nsmall = 2))))
  
  # --- plotting ---
  p <- ggplot(wsb_with_vals, aes(
    x = !!rlang::sym(doy_col),
    y = !!rlang::sym(gdd_col),
    color = as.factor(Zones)
  )) +
    geom_point(size = 2, alpha = 0.75) +
    geom_smooth(method = "lm", se = FALSE, linewidth = 0.9, color = "black") +
    geom_text(
      data = r2_data,
      aes(label = label),
      x = 102, y = 1450,
      hjust = 0, vjust = 1,
      size = 4, color = "black",
      inherit.aes = FALSE
    ) +
    facet_wrap(~ Zones, scales = "fixed") +
    labs(
      x = "Day of Year (DOY)",
      y = expression("Growing Degree Days (GDD, " * degree*C * ")"),
      color = "Elevation zone",
      title = paste("Relationship between DOY and GDD in", year)
    ) +
    scale_x_continuous(limits = c(100, 220), breaks = seq(100, 220, by = 20)) +
    coord_cartesian(ylim = c(0, 1500)) +
    scale_color_brewer(palette = "Dark2") +
    theme_bw(base_size = 14) +
    theme(
      plot.title = element_text(face = "bold", size = 15, hjust = 0.5),
      axis.title = element_text(face = "bold"),
      axis.title.x = element_text(margin = margin(t = 12)),
      axis.title.y = element_text(margin = margin(r = 12)),
      axis.text = element_text(color = "gray20"),
      strip.text = element_text(face = "bold", size = 12, hjust = 0.5),
      panel.grid.major = element_line(color = "gray85", linewidth = 0.4),
      panel.grid.minor = element_blank(),
      legend.position = "bottom",
      legend.title = element_text(face = "bold"),
      legend.text = element_text(size = 11)
    )
  
  print(p)
  
  ggsave(filename = paste0("/mnt/CEPH_PROJECTS/Environtwin/images/GDD_DOY/GDD_DOY_WSB_", year, ".png"), 
         plot = p, width = 10, height = 6, dpi = 80)
  
  
  return(wsb_with_vals)
}

all_results <- list()
years <- 2021:2024

for (y in years) {
  res <- function_gdd_multiyear(y, gdd_dir, wsb_zones)
  res$year <- y
  
  # Keep only relevant columns and ensure same schema
  keep_cols <- c("Zones", "year", 
                 paste0("DOY_", y), 
                 paste0("GDD_at_first", y))
  existing_cols <- intersect(keep_cols, colnames(res))
  res <- res[, existing_cols, drop = FALSE]
  
  all_results[[as.character(y)]] <- res
}
