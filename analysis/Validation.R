library(terra)
library(ggplot2)
library(cowplot)
library(dplyr)

years <- 2017:2025
sensor <- "PLA"   # change accordingly
site <- "MH"

refDOY_files <- paste0("/mnt/CEPH_PROJECTS/Environtwin/gis/reference/", site) 
predDOY_files <- paste0("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level4_sites/mowing/03_v14_v4/", site, "/final")
copernicusRef_files <- "/mnt/CEPH_PROJECTS/Environtwin/gis/reference/copernicus/"
webcam_files <- st_read("/mnt/CEPH_PROJECTS/Environtwin/gis/reference/MH/webcam/webcam_mowing.shp") 

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

# ---------------------------------------------------------
# Create scatterplot for one year
# Returns a ggplot object (not saved yet)
# ---------------------------------------------------------
plot_scatter <- function(plot_data, out_prefix, out_dir, site, year, type) {
  
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
    
    # white box for statistics
    annotate(
      "rect",
      xmin = 58, xmax = 132,
      ymin = 225, ymax = 295,
      fill = "white",
      alpha = 0.95
    ) +
    
    # metrics
    annotate(
      "text",
      x = 95, y = 285,
      label = paste("MAE:", round(plot_data$mae[1], 1)),
      size = 4,
      fontface = "bold"
    ) +
    
    annotate(
      "text",
      x = 95, y = 267,
      label = paste("RMSE:", round(plot_data$rmse[1], 1)),
      size = 4
    ) +
    
    annotate(
      "text",
      x = 95, y = 249,
      label = paste("R =", round(plot_data$cor[1], 3)),
      size = 4
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

# function for the residuals
plot_residuals <- function(plot_data, out_prefix, out_dir, site, year, type) {
  res_mean <- round(mean(plot_data$residual, na.rm = TRUE), 1)
  res_sd <- round(sd(plot_data$residual, na.rm = TRUE), 1)
  p <- ggplot(plot_data, aes(x = residual)) +
    geom_histogram(aes(y = after_stat(density)), bins = 50, fill = "darkgrey", alpha = 0.85, color = "black", size = 0.3) +
    #geom_density(alpha = 0.4, color = "red", size = 0.8, fill = NA) +
    geom_vline(xintercept = 0, color = "black", linetype = "dashed", size = 1) +
    # Inset stats box (top-right)
    annotate("rect", xmin = -60, xmax = -50, 
             ymin = Inf, ymax = Inf, fill = "white", alpha = 0.95, color = "white") +
    annotate("text", x = -60, y = 0.5,
             label = paste("Mean:", res_mean, "days"), size = 6, hjust = 0) +
    annotate("text", x = -60, y = 0.6,
             label = paste("SD:", res_sd, "days"), size = 6, hjust = 0) +
    labs(x = "Residual DOY (days)", y = "Density", title = paste("Residuals", year, type)) +
    theme_cowplot() + xlim(-60, 60) +
    theme(plot.title = element_text(hjust = 0.5, size = 14, face = "bold"),
          legend.position = "bottom")
  #print(p)
  #ggsave(file.path(out_dir, paste0(out_prefix, "_residual_", type, "_", site, "_", year, ".png")), 
  #       p, width = 12, height = 8, dpi = 300)
  return(p)
}

#### --- 2. Loop over years and generate plots --- ####
scatter_plots <- list()
residual_plots <- list()

for(i in years) {
  
  # Load predictions (shared)
  predDOY_file <- list.files(predDOY_files, pattern = paste0(site, "_", sensor, "_", i, "_doy1\\.tif$"), full.names = TRUE)
  predDOY <- rast(predDOY_file)
  predDOY <- app(predDOY, fun = function(x) { ifelse(x <= 0, NA, x) })
  
  # === SPECIAL CASE: MH 2023 WEBCAM ===
  if(i == 2023 && site == "MH") {
    
    webcam_files$mowing_date <- as.Date(webcam_files$mowdate1, format="%Y%m%d")
    webcam_files$DOY <- as.integer(format(webcam_files$mowing_date, "%j"))
    
    # Per-polygon evaluation (mode)
    mode_fun <- function(x) {
      x <- x[!is.na(x)]
      if (length(x) == 0) return(NA)
      ux <- unique(x)
      ux[which.max(tabulate(match(x, ux)))]
    }
    
    maj_poly <- terra::extract(predDOY, webcam_files, fun = mode_fun, bind = TRUE)
    webcam_files$maj_pred_DOY <- maj_poly$lyr.1
    webcam_files$residual_poly <- webcam_files$maj_pred_DOY - webcam_files$DOY
    
    valid_poly <- !is.na(webcam_files$maj_pred_DOY) & !is.na(webcam_files$DOY)
    poly_metrics <- compute_metrics(webcam_files$maj_pred_DOY[valid_poly], webcam_files$DOY[valid_poly])
    
    poly_data <- data.frame(
      ref = webcam_files$DOY[valid_poly],
      pred = webcam_files$maj_pred_DOY[valid_poly],
      residual = webcam_files$residual_poly[valid_poly],
      mae = poly_metrics$mae, 
      rmse = poly_metrics$rmse, 
      cor = poly_metrics$cor
    )
    
    # Create and save scatter plot
    plot1 <- ggplot(poly_data, aes(x = ref, y = pred)) +
      geom_point(alpha = 0.8, size = 3) +
      geom_abline(intercept = 0, slope = 1, color = "black", linetype = "dashed", size = 0.8) +
      geom_smooth(method = "lm", se = TRUE, color = "darkred", alpha = 0.2) +
      annotate("rect", xmin = 55, xmax = 130, ymin = 285, ymax = 335, fill = "white", alpha = 0.9) +
      annotate("text", x = 80, y = 300, label = paste("MAE:", round(poly_data$mae[1], 1)), size = 6, fontface = "bold") +
      annotate("text", x = 80, y = 280, label = paste("RMSE:", round(poly_data$rmse[1], 1)), size = 6) +
      annotate("text", x = 80, y = 260, label = paste("R =", round(poly_data$cor[1], 3)), size = 6) +
      labs(x = "Reference DOY", y = "Predicted DOY", title = paste("DOY Accuracy Polygon (Webcam)", i)) +
      theme_cowplot() + xlim(50, 300) + ylim(50, 300) + coord_fixed(ratio = 1) +
      theme(
        panel.grid.major = element_line(color = "grey85", linewidth = 0.4),
        panel.grid.minor = element_line(color = "grey92", linewidth = 0.2),
        panel.background = element_rect(fill = "white"),
        plot.title = element_text(hjust = 0.5, size = 14, face = "bold"),
        axis.title = element_text(size = 14)
      )
    
    ggsave(file.path(out_dir, paste0("WEBCAMPRED_scatter_2023_", site, "_", i, ".png")), 
           plot1, width = 12, height = 8, dpi = 300)
    
  } 
  
  # === LAFIS POLYGON REFERENCE (2024-2025) ===
  else if(i %in% c(2024, 2025)) {
    
    # Load & prepare reference polygons
    refPoly_file <- list.files(refDOY_files, pattern = paste0("lafis_mahd_poly_", i, "_", site, ".shp"), full.names = TRUE)
    refPoly <- st_read(refPoly_file)
    refPoly$mowing_date <- as.Date(refPoly$pixdoy1, format="%Y%m%d")
    refPoly$DOY <- as.integer(format(refPoly$mowing_date, "%j"))
    refRaster <- rasterize(refPoly, predDOY, field="DOY", touches=FALSE)
    refRaster <- app(refRaster, fun = function(x) { ifelse(x < 0, NA, x) })
    
    # Save polygons with results
    st_write(refPoly, paste0(out_dir, "poly_validation_", i, "_", site, ".shp"), 
             delete_dsn = TRUE)
    
    # 1. PER-PIXEL EVALUATION
    valid_mask <- !is.na(predDOY) & !is.na(refRaster)
    pred_vec <- as.vector(predDOY[valid_mask])
    ref_vec <- as.vector(refRaster[valid_mask])
    
    metrics <- compute_metrics(pred_vec, ref_vec)
    n_sample <- min(50000, length(pred_vec))
    sample_idx <- sample(length(pred_vec), n_sample)
    
    plot_data <- data.frame(
      ref = ref_vec[sample_idx],
      pred = pred_vec[sample_idx],
      mae = metrics$mae, rmse = metrics$rmse, cor = metrics$cor
    ) %>% mutate(residual = pred - ref) %>% filter(!is.na(pred) & !is.na(ref))
    
    # Plots
    #plot_scatter(plot_data, "(Pixel Level)", "doy_scatter_pixel", out_dir, site, i, "pixel")
    #plot_residuals(plot_data, "residual_pixel", out_dir, site, i, "pixel")
    
    scatter_plots[[as.character(i)]] <- plot_scatter(
      plot_data,
      #"(Copernicus, Pixel)",
      "copern_doy_scatter_pixel",
      out_dir,
      site,
      i,
      "pixel"
    )
    
    residual_plots[[as.character(i)]] <- plot_residuals(
      plot_data,
      "copern_residual_pixel",
      out_dir,
      site,
      i,
      "pixel"
    )
    
    # Save residual raster
    residual <- predDOY - refRaster
    writeRaster(residual, paste0(out_dir, "valid_REF_PRED_", sensor, "_", i, ".tif"), overwrite = TRUE)
    
    # 2. PER-POLYGON EVALUATION
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
    
    refPoly$maj_pred_DOY <- maj_poly$lyr.1
    refPoly$residual_poly <- refPoly$maj_pred_DOY - refPoly$DOY
    
    valid_poly <- !is.na(refPoly$maj_pred_DOY) & !is.na(refPoly$DOY)
    poly_metrics <- compute_metrics(refPoly$maj_pred_DOY[valid_poly], refPoly$DOY[valid_poly])
    
    poly_data <- data.frame(
      ref = refPoly$DOY[valid_poly],
      pred = refPoly$maj_pred_DOY[valid_poly],
      residual = refPoly$residual_poly[valid_poly],
      mae = poly_metrics$mae, rmse = poly_metrics$rmse, cor = poly_metrics$cor,
      index = refPoly$index[valid_poly]
    )
    
    plot3 <- ggplot(poly_data, aes(x = ref, y = pred, color = factor(index))) +
      geom_point(alpha = 0.8, size = 3)+
      geom_abline(intercept = 0, slope = 1, color = "black", linetype = "dashed", size = 0.8) +
      geom_smooth(method = "lm", se = TRUE, color = "darkred", alpha = 0.2) +
      annotate("rect", xmin = 55, xmax = 130, ymin = 285, ymax = 335, fill = "white", alpha = 0.9) +
      annotate("text", x = 80, y = 300, label = paste("MAE:", round(poly_data$mae[1], 1)), size = 6, fontface = "bold") +
      annotate("text", x = 80, y = 280, label = paste("RMSE:", round(poly_data$rmse[1], 1)), size = 6) +
      annotate("text", x = 80, y = 260, label = paste("R =", round(poly_data$cor[1], 3)), size = 6) +
      labs(x = "Reference DOY", y = "Predicted DOY", 
           title = paste("DOY Accuracy Polygon"),
           color = "Confidence") +
      theme_cowplot() + xlim(50, 300) + ylim(50, 300) + coord_fixed(ratio = 1) +
      theme(
        panel.grid.major = element_line(color = "grey85", linewidth = 0.4),
      panel.grid.minor = element_line(color = "grey92", linewidth = 0.2),
      panel.background = element_rect(fill = "white"),
        legend.position = "bottom",  # Or "right"
        legend.direction = "horizontal",
        legend.spacing.x = unit(0.5, "cm"),  # Horizontal spacing
        legend.spacing.y = unit(0.3, "cm"),  # Vertical spacing
        legend.text = element_text(size = 14),
        legend.title = element_text(size = 14),
        legend.key.height = unit(0.6, "cm"),  # Taller keys
        legend.key.width = unit(1.2, "cm"),
        plot.title = element_text(hjust = 0.5, size = 14, face = "bold"),
        axis.title = element_text(size = 14, vjust = 1)
      ) 
    
    print(plot3)
    
    ggsave(file.path(out_dir, paste0("RefPRED_scatter_poly", "_", site, "_", i, ".png")), 
          plot3, width = 12, height = 8, dpi = 300)
    
    # Plots Residuals
    #plot_scatter(poly_data, "(Polygon Level)", "doy_scatter_poly", out_dir, site, i, "poly")
    #plot_residuals(poly_data, "residual_poly", out_dir, site, i, "poly")
    
    # Save polygons
    #st_write(refPoly, paste0(out_dir, "poly_validation_", i, "_", site, ".shp"), delete_dsn = TRUE)
    
  } else {
    # === COPERNICUS RASTER REFERENCE (pre-2024) ===
    
    # Load & prepare Copernicus reference
    # if 2022 & 2023 V02
    # if 2017:2021 V01
    
    if(i %in% c(2022, 2023)){
      ref_file <- list.files(paste0(copernicusRef_files,"CLMS_HRLVLCC_GRAMD1_S" , i, "_R10m_E43N26_03035_V01_R00"), pattern = "_R00\\.tif$", full.names = TRUE)
      refCop <- rast(ref_file)
     } else
    {
      ref_file <- list.files(paste0(copernicusRef_files,"CLMS_HRLVLCC_GRAMD1_S" , i, "_R10m_E43N26_03035_V02_R00/"), 
                             pattern = "_R00\\.tif$", full.names = TRUE)
      refCop <- rast(ref_file)
    }
    
    #refCop <- ifel(refCop %in% c(0, 366), NA, refCop)
    
    # Resample to prediction raster
    refCop_prj <- project(refCop, crs(predDOY))
    refCop_resampled <- resample(crop(refCop_prj, predDOY), predDOY)
    refCop_num <- as.numeric(refCop_resampled)
    refCop_clean <- ifel(refCop_num < 1 | refCop_num > 365, NA, refCop_num)
    
    # Residual raster
    residual <- refCop_clean - predDOY  
    writeRaster(residual, paste0(out_dir, "valid_COPERN_PRED_", sensor, "_", i, ".tif"), overwrite = TRUE)
    
    # Per-pixel evaluation & plots
    valid_mask <- !is.na(predDOY) & !is.na(refCop_clean)
    pred_vec <- as.vector(predDOY[valid_mask])
    ref_vec <- as.vector(refCop_clean[valid_mask])
    
    metrics <- compute_metrics(pred_vec, ref_vec)
    n_sample <- min(100000, length(pred_vec))
    sample_idx <- sample(length(pred_vec), n_sample)
    
    plot_data <- data.frame(
      ref = ref_vec[sample_idx],
      pred = pred_vec[sample_idx],
      mae = metrics$mae, rmse = metrics$rmse, cor = metrics$cor
    ) %>% mutate(residual = pred - ref) %>% filter(!is.na(pred) & !is.na(ref))
    
    #plot_scatter(plot_data, "(Copernicus, Pixel)", "copern_doy_scatter_pixel", out_dir, site, i, "copernicus")
    #plot_residuals(plot_data, "copern_residual_pixel", out_dir, site, i, "copernicus")
    
    scatter_plots[[as.character(i)]] <- plot_scatter(
      plot_data,
      #"(Copernicus, Pixel)",
      "copern_doy_scatter_pixel",
      out_dir,
      site,
      i,
      "copernicus"
    )
    
    residual_plots[[as.character(i)]] <- plot_residuals(
      plot_data,
      "copern_residual_pixel",
      out_dir,
      site,
      i,
      "Copernicus"
    )
  }
}


combined_scatter <- wrap_plots(scatter_plots, ncol = 4) +
  plot_annotation(title = "DOY Accuracy (Pixel Level)") +
  plot_layout(guides = "collect")

combined_residual <- wrap_plots(residual_plots, ncol = 2)

combined_scatter
combined_residual
