library(sf)
library(terra)
library(ggplot2)
library(ggpubr)
library(ggpmisc)
library(patchwork)

# Load polygon and DEM class data
dem_classes <- rast("/mnt/CEPH_PROJECTS/SAO/SENTINEL-2/SentinelVegetationProducts/FORCE/masks/dem_copernicus/DEM.vrt")
lafis_sub <- read_sf("/mnt/CEPH_PROJECTS/Environtwin/gis/reference/ST_first_cut/lafis_fc.gpkg")

gdd_dir <- "/mnt/CEPH_PROJECTS/Environtwin/GDD/SouthTyrol/gdd/"

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
  gdd_file <- paste0(gdd_dir, "GDD_daily_cumulative_T5_", year, ".tif")
  gdd <- rast(gdd_file)
  
  # Extract mean GDD per polygon
  extracted_vals <- terra::extract(gdd, dem_vect, fun = mean, na.rm = TRUE)
  
  # Combine extracted values (exclude ID)
  lafis_with_vals <- cbind(dem_df, extracted_vals[,-1])
  
  # Define your date columns relevant for the years (adjust names as per your data)
  date_cols_map <- list(
    "2024" = "firstM",
    "2023" = "first2023",
    "2022" = "first2022",
    "2021" = "first2021",
    "2020" = "first2020"
  )
  
  # Initialize list to store top10 per column
  top10_per_col <- list()
  
  # Loop over each column in date_cols_map
  for(col in unname(date_cols_map)) {
    top10_per_col[[col]] <- lafis_with_vals %>%
      arrange(!!rlang::sym(col)) %>%   # sort by the current column
      slice_head(n = 10)               # take top 10
  }
  
  # Create a long-format dataset for all top 10 polygons per date column
  top10_all <- bind_rows(
    lapply(names(date_cols_map), function(year_k) {
      col_name <- date_cols_map[[year_k]]
      lafis_with_vals %>%
        arrange(!!rlang::sym(col_name)) %>%
        slice_head(n = 10) %>%
        mutate(
          year = year_k,
          sort_col = col_name
        )
    }),
    .id = "source"
  )
  
  # Rename raster column names (DOY)
  start_idx <- ncol(dem_df)+1
  end_idx <- ncol(top10_all) -3
  doy <- substr(names(top10_all)[start_idx:end_idx], 30, 33)
  doy_names <- paste0("DOY_", doy)
  names(top10_all)[start_idx:end_idx] <- doy_names
  
  # Convert the date columns to Date type (for all years, once)
  for(dc in unique(date_cols_map)) {
    if(dc %in% colnames(top10_all)) {
      if(!inherits(top10_all[[dc]], "Date")) {
        top10_all[[paste0(dc, "_date")]] <- as.Date(as.character(top10_all[[dc]]), format = "%Y%m%d")
      } else {
        top10_all[[paste0(dc, "_date")]] <- top10_all[[dc]]
      }
    } else {
      warning(paste("Column", dc, "not found in lafis_with_vals. Skipping."))
    }
  }
  
  # Extract DOY from the date columns
  for(year_k in names(date_cols_map)) {
    dc <- date_cols_map[[year_k]]
    doy_col_name <- paste0("DOY_", year_k)
    top10_all[[doy_col_name]] <- as.integer(format(top10_all[[paste0(dc, "_date")]], "%j"))
  }
  
  # Assign GDD_at_ columns using helper for all date columns
  for(year_k in names(date_cols_map)) {
    dc <- date_cols_map[[year_k]]
    doy_col_name <- paste0("DOY_", year_k)
    top10_all <- assign_gdd_at_doy(top10_all, doy_col_name, dc)
  }
  
  dem_levels <- c(1, 2, 3)
  dem_labels <- c(
    "C.–submontane (250–800 m a.s.l.)",
    "Montane zone (800–1800 m a.s.l.)",
    "Subalpine zone (1800–2200 m a.s.l.)"
  )
  
  # If dem_class may be numeric or character, coerce to numeric first (only if needed)
  # lafis_with_vals$dem_class <- as.numeric(as.character(lafis_with_vals$dem_class))
  top10_all$dem_class <- factor(top10_all$dem_class,
                                      levels = dem_levels,
                                      labels = dem_labels)
  
  # build the dynamic column names for the given year
  gdd_col <- paste0("GDD_at_", date_cols_map[[as.character(year)]])
  doy_col <- paste0("DOY_", year)
  
  # confirm columns exist
  if(! (gdd_col %in% colnames(top10_all) && doy_col %in% colnames(top10_all)) ) {
    stop("Missing required columns: ", paste(setdiff(c(gdd_col, doy_col), colnames(top10_all)), collapse = ", "))
  }
  
  # --- compute R2 per dem_class robustly ---
  r2_data <- top10_all %>%
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
  p <- ggplot(top10_all, aes(
    x = !!rlang::sym(doy_col),
    y = !!rlang::sym(gdd_col),
    color = year
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
