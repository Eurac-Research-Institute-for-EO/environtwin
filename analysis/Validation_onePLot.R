library(terra)
library(ggplot2)
library(cowplot)
library(dplyr)
library(sf)
library(patchwork)

years <- 2017:2025
sensor <- "PLA"   # change accordingly
site <- "MH"

refDOY_files <- paste0("/mnt/CEPH_PROJECTS/Environtwin/gis/reference/", site) 
predDOY_files <- paste0("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level4_sites/mowing/03_v14_v4/", site, "/final")
copernicusRef_files <- "/mnt/CEPH_PROJECTS/Environtwin/gis/reference/copernicus/"

out_dir <- paste0("/mnt/CEPH_PROJECTS/Environtwin/figures/validation/", site, "/")
dir.create(out_dir, recursive = TRUE)

#### --- 1. Write functions --- ####
# ---------------------------------------------------------
# Compute standard validation metrics
# ---------------------------------------------------------
compute_metrics <- function(pred_vec, ref_vec) {
  
  mae  <- mean(abs(pred_vec - ref_vec), na.rm = TRUE)
  rmse <- sqrt(mean((pred_vec - ref_vec)^2, na.rm = TRUE))
  cor_val <- cor(pred_vec, ref_vec, use = "complete.obs")
  
  list(
    mae = mae,
    rmse = rmse,
    cor = cor_val
  )
}

# -----
# ---------------------------------------------------------
# Create scatterplot for one year
# Returns a ggplot object (not saved yet)
# ---------------------------------------------------------
plot_scatter <- function(plot_data, year) {
  
  ggplot(plot_data, aes(x = ref, y = pred)) +
    
    # sampled points
    geom_point(
      colour = "darkgrey",
      alpha = 0.65,
      size = 2.2
    ) +
    
    # 1:1 reference line
    geom_abline(
      intercept = 0,
      slope = 1,
      color = "black",
      linetype = "dashed",
      linewidth = 0.6
    ) +
    
    # linear fit
    geom_smooth(
      method = "lm",
      se = TRUE,
      color = "darkred",
      alpha = 0.15,
      linewidth = 0.7
    ) +
    
    # white box for statistics (lower-right corner)
    annotate(
      "rect",
      xmin = 220, xmax = 295,
      ymin = 58, ymax = 128,
      fill = "white",
      alpha = 0.95
    ) +
    
    # metrics
    annotate(
      "text",
      x = 250, y = 100,
      label = paste("MAE:", round(plot_data$mae[1], 1)),
      size = 3,
      fontface = "bold"
    ) +
    
    annotate(
      "text",
      x = 250, y = 80,
      label = paste("RMSE:", round(plot_data$rmse[1], 1)),
      size = 3
    ) +
    
    annotate(
      "text",
      x = 250, y = 60,
      label = paste("R =", round(plot_data$cor[1], 3)),
      size = 3
    ) +
    # axis labels and panel title
    labs(
      x = "Reference DOY",
      y = "Predicted DOY",
      title = year
    ) +
    
    # same axis range for all panels
    coord_fixed(
      ratio = 1,
      xlim = c(50, 300),
      ylim = c(50, 300)
    ) +
    
    # clean base theme
    theme_cowplot() +
    
    theme(
      # keep gridlines
      panel.grid.major = element_line(color = "grey88", linewidth = 0.35),
      panel.grid.minor = element_line(color = "grey94", linewidth = 0.2),
      panel.background = element_rect(fill = "white"),
      
      # smaller panel title (year)
      plot.title = element_text(
        hjust = 0.5,
        size = 11,
        face = "bold"
      ),
      
      axis.title = element_text(size = 11),
      axis.text  = element_text(size = 9)
    )
}

# ---------------------------------------------------------
# Create residual histogram for one year
# Returns a ggplot object
# ---------------------------------------------------------
plot_residuals <- function(plot_data, year) {
  
  res_mean <- round(mean(plot_data$residual, na.rm = TRUE), 1)
  res_sd   <- round(sd(plot_data$residual, na.rm = TRUE), 1)
  
  ggplot(plot_data, aes(x = residual)) +
    
    geom_histogram(
      aes(y = after_stat(density)),
      bins = 50,
      fill = "darkgrey",
      alpha = 0.85,
      color = "black",
      linewidth = 0.2
    ) +
    
    geom_vline(
      xintercept = 0,
      color = "black",
      linetype = "dashed",
      linewidth = 0.7
    ) +
    
    annotate(
      "rect",
      xmin = 18, xmax = 50,
      ymin = 0.045, ymax = 0.06,
      fill = "white",
      alpha = 0.95
    ) +
    
    annotate(
      "text",
      x = 20, y = 0.055,
      label = paste("Mean:", res_mean, "days"),
      size = 3,
      hjust = 0
    ) +
    
    annotate(
      "text",
      x = 20, y = 0.030,
      label = paste("SD:", res_sd, "days"),
      size = 3,
      hjust = 0
    ) +
    labs(
      x = "Residual DOY",
      y = "Density",
      title = year
    ) +
    
    coord_cartesian(
      xlim = c(-60, 60),
      ylim = c(0, 0.13)
    ) +
    
    theme_cowplot() +
    theme(
      panel.grid.major = element_line(color = "grey88", linewidth = 0.35),
      panel.grid.minor = element_line(color = "grey94", linewidth = 0.2),
      panel.background = element_rect(fill = "white"),
      
      plot.title = element_text(
        hjust = 0.5,
        size = 11,
        face = "bold"
      ),
      
      axis.title = element_text(size = 11),
      axis.text  = element_text(size = 9)
    )
}

#### ------------------------------------------------------------
#### 3. Loop over years, compute validation, store plots
#### ------------------------------------------------------------

# Lists that will store one plot per year
scatter_plots  <- list()
residual_plots <- list()

for (i in years) {
  
  message("Processing year: ", i)
  
  #### ----------------------------------------------------------
  #### A. Load predicted DOY raster
  #### ----------------------------------------------------------
  
  predDOY_file <- list.files(
    predDOY_files,
    pattern = paste0(site, "_", sensor, "_", i, "_doy1\\.tif$"),
    full.names = TRUE
  )
  
  predDOY <- rast(predDOY_file)
  
  # Remove invalid predictions
  predDOY <- app(
    predDOY,
    fun = function(x) ifelse(x <= 0, NA, x)
  )
  
  #### ----------------------------------------------------------
  #### B. Use polygon reference for 2024–2025
  #### ----------------------------------------------------------
  
  if (i %in% c(2024, 2025)) {
    
    # Read polygon reference
    refPoly_file <- list.files(
      refDOY_files,
      pattern = paste0("lafis_mahd_poly_", i, "_", site, ".shp"),
      full.names = TRUE
    )
    
    refPoly <- st_read(refPoly_file, quiet = TRUE)
    
    # Convert mowing date to DOY
    refPoly$mowing_date <- as.Date(refPoly$pixdoy1, format = "%Y%m%d")
    refPoly$DOY <- as.integer(format(refPoly$mowing_date, "%j"))
    
    # Rasterize polygons to same grid as prediction
    refRaster <- rasterize(
      refPoly,
      predDOY,
      field = "DOY",
      touches = FALSE
    )
    
    refRaster <- app(
      refRaster,
      fun = function(x) ifelse(x < 0, NA, x)
    )
    
    #### --------------------------------------------------------
    #### Pixel-level validation
    #### --------------------------------------------------------
    
    valid_mask <- !is.na(predDOY) & !is.na(refRaster)
    
    pred_vec <- as.vector(predDOY[valid_mask])
    ref_vec  <- as.vector(refRaster[valid_mask])
    
    metrics <- compute_metrics(pred_vec, ref_vec)
    
    # Sample points for plotting
    n_sample <- min(50000, length(pred_vec))
    sample_idx <- sample(length(pred_vec), n_sample)
    
    plot_data <- data.frame(
      ref  = ref_vec[sample_idx],
      pred = pred_vec[sample_idx],
      mae  = metrics$mae,
      rmse = metrics$rmse,
      cor  = metrics$cor
    ) %>%
      mutate(residual = pred - ref) %>%
      filter(!is.na(ref), !is.na(pred))
    
    #### --------------------------------------------------------
    #### Store plots for this year
    #### --------------------------------------------------------
    
    scatter_plots[[as.character(i)]] <- plot_scatter(
      plot_data = plot_data,
      year = i
    )
    
    residual_plots[[as.character(i)]] <- plot_residuals(
      plot_data = plot_data,
      year = i
    )
    
    #### --------------------------------------------------------
    #### Save residual raster
    #### --------------------------------------------------------
    
    residual <- predDOY - refRaster
    
    writeRaster(
      residual,
      file.path(out_dir, paste0("valid_REF_PRED_", sensor, "_", i, ".tif")),
      overwrite = TRUE
    )
    
    #### --------------------------------------------------------
    #### Polygon-level validation (optional)
    #### --------------------------------------------------------
    
    mode_fun <- function(x) {
      x <- x[!is.na(x)]
      if (length(x) == 0) return(NA)
      ux <- unique(x)
      ux[which.max(tabulate(match(x, ux)))]
    }
    
    maj_poly <- terra::extract(
      predDOY,
      refPoly,
      fun = mode_fun,
      bind = TRUE
    )
    
    refPoly$maj_pred_DOY <- maj_poly[[names(maj_poly)[ncol(maj_poly)]]]
    refPoly$residual_poly <- refPoly$maj_pred_DOY - refPoly$DOY
    
  } else {
    
    #### --------------------------------------------------------
    #### C. Use Copernicus raster reference for pre-2024
    #### --------------------------------------------------------
    
    if (i %in% c(2022, 2023)) {
      ref_file <- list.files(
        paste0(
          copernicusRef_files,
          "CLMS_HRLVLCC_GRAMD1_S",
          i,
          "_R10m_E43N26_03035_V01_R00"
        ),
        pattern = "_R00\\.tif$",
        full.names = TRUE
      )
    } else {
      ref_file <- list.files(
        paste0(
          copernicusRef_files,
          "CLMS_HRLVLCC_GRAMD1_S",
          i,
          "_R10m_E43N26_03035_V02_R00/"
        ),
        pattern = "_R00\\.tif$",
        full.names = TRUE
      )
    }
    
    refCop <- rast(ref_file)
    
    # Reproject and resample to prediction raster
    refCop_prj <- project(refCop, crs(predDOY))
    refCop_resampled <- resample(crop(refCop_prj, predDOY), predDOY)
    
    # Remove invalid values
    refCop_clean <- ifel(
      refCop_resampled < 1 | refCop_resampled > 365,
      NA,
      refCop_resampled
    )
    
    #### --------------------------------------------------------
    #### Pixel-level validation
    #### --------------------------------------------------------
    
    valid_mask <- !is.na(predDOY) & !is.na(refCop_clean)
    
    pred_vec <- as.vector(predDOY[valid_mask])
    ref_vec  <- as.vector(refCop_clean[valid_mask])
    
    metrics <- compute_metrics(pred_vec, ref_vec)
    
    n_sample <- min(50000, length(pred_vec))
    sample_idx <- sample(length(pred_vec), n_sample)
    
    plot_data <- data.frame(
      ref  = ref_vec[sample_idx],
      pred = pred_vec[sample_idx],
      mae  = metrics$mae,
      rmse = metrics$rmse,
      cor  = metrics$cor
    ) %>%
      mutate(residual = pred - ref) %>%
      filter(!is.na(ref), !is.na(pred))
    
    #### --------------------------------------------------------
    #### Store plots for this year
    #### --------------------------------------------------------
    
    scatter_plots[[as.character(i)]] <- plot_scatter(
      plot_data = plot_data,
      year = i
    )
    
    residual_plots[[as.character(i)]] <- plot_residuals(
      plot_data = plot_data,
      year = i
    )
    
    #### --------------------------------------------------------
    #### Save residual raster
    #### --------------------------------------------------------
    
    residual <- refCop_clean - predDOY
    
    writeRaster(
      residual,
      file.path(out_dir, paste0("valid_COPERN_PRED_", sensor, "_", i, ".tif")),
      overwrite = TRUE
    )
  }
}

#### ------------------------------------------------------------
#### 4. Combine all yearly plots into one multi-panel figure
#### ------------------------------------------------------------
combined_scatter <- wrap_plots(
  scatter_plots,
  ncol = 3
) +
  plot_annotation(
    title = "DOY Accuracy (Pixel Level)"
  ) &
  theme(
    plot.title = element_text(
      hjust = 0.5,
      size = 18,
      face = "bold"
    )
  )

combined_residual <- wrap_plots(
  residual_plots,
  ncol = 3
) +
  plot_annotation(
    title = "Residual Distribution"
  ) &
  theme(
    plot.title = element_text(
      hjust = 0.5,
      size = 18,
      face = "bold"
    )
  )

#### ------------------------------------------------------------
#### 5. Display figures
#### ------------------------------------------------------------

combined_scatter
combined_residual

#### ------------------------------------------------------------
#### 6. Save combined figures
#### ------------------------------------------------------------

ggsave(
  file.path(out_dir, paste0("combined_scatter_", site, ".png")),
  combined_scatter,
  width = 18,
  height = 10,
  dpi = 300
)

ggsave(
  file.path(out_dir, paste0("combined_residual_", site, ".png")),
  combined_residual,
  width = 18,
  height = 10,
  dpi = 300
)
