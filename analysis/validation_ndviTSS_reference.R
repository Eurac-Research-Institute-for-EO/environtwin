################ -- NDVI time series + validation using copernicus + reference for 2024 and 2025 -- #########
#############################################################################################################
library(ggplot2)
library(terra)
library(sf)

years <- 2023
sensor <- "PLA"   # change accordingly
site <- "MH"    # change accordingly

# Set up a publication‑style theme
pub_theme <- theme_minimal() +
  theme(
    axis.title       = element_text(size = 16, face = "plain"),
    axis.text        = element_text(size = 14),
    axis.line        = element_line(linewidth = 0.5, colour = "black"),
    axis.ticks       = element_line(linewidth = 0.3, colour = "black"),
    panel.grid.major = element_line(linewidth = 0.1, colour = "grey80"),
    panel.grid.minor = element_blank(),
    legend.position  = "bottom",
    legend.direction = "horizontal",
    legend.title     = element_blank(),
    legend.text      = element_text(size = 14),
    plot.title       = element_text(size = 16, hjust = 0.0),
    plot.subtitle    = element_text(size = 14, hjust = 0.0),
    plot.margin      = margin(4, 4, 4, 4, unit = "pt")
  )

# load data
refDOY_files <- "/mnt/CEPH_PROJECTS/Environtwin/gis/reference"
predDOY_files <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level4_sites/mowing/03_v14_v4"
copernicusRef_files <- "/mnt/CEPH_PROJECTS/Environtwin/gis/reference/copernicus/"
ndvi_files <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/03"
webcam_files <- st_read("/mnt/CEPH_PROJECTS/Environtwin/gis/reference/MH/webcam/webcam_mowing.shp") 

# resiudals
#residual_files <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/validation"

# load points for validation
shp_files <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/validation/MH"

out_dir <- paste0("/mnt/CEPH_PROJECTS/Environtwin/FORCE/validation/", site, "/")

for(i in years) {
  
  # Load predictions (shared)
  predDOY_file <- list.files(paste0(predDOY_files,"/", site, "/final"), pattern = paste0(site, "_", sensor, "_", i, "_doy1\\.tif$"), full.names = TRUE)
  predDOY <- rast(predDOY_file)
  predDOY <- app(predDOY, fun = function(x) { ifelse(x <= 0, NA, x) })
  
  # Load NDVI files for this year and site
  ndvi_file <- list.files(paste0(ndvi_files, "/", site), 
                           pattern = paste0("^", i, ".*_TSA_", sensor, "_NDV_TSS\\.bsq$"), 
                           full.names = TRUE)
  ndvi <- rast(ndvi_file)
  
  dates_files <- list.files(paste0(ndvi_files, "/", site), 
                      pattern = paste0("dates_", i, "_" ,sensor, "\\.txt$"), 
                      full.names = TRUE)
  
  dates <- read.table(dates_files)
  
  # Extract DOY from V1
  date_str <- as.character(dates$V1)  
  
  doy <- as.numeric(
    sub(".*_(\\d+)_PLA", "\\1", date_str)
  )
  
  # load the points for validation
  shp_file <- list.files(shp_files, pattern = paste0("point_error_", i, "\\.shp$"), full.names = TRUE)
  shp <- st_read(shp_file)
  
  # Extract time series and other validation data for specific points
  ts_outliers <- terra::extract(ndvi, webcam_files)
  #names(ts_outliers) <- doy
  
  pred_vals <- terra::extract(predDOY, webcam_files)
  pred_vals_test <- pred_vals$lyr.1
  
  if (i %in% c(2024, 2025)) {
    # --- LAFIS POLYGON REFERENCE (2024–2025) ---
    refDOY_file <- list.files(paste0(refDOY_files, "/", site),
                              pattern = paste0(site, "_", i, "_doy\\.tif$"),
                              full.names = TRUE)
    refDOY   <- rast(refDOY_file)
    ref_vals <- extract(refDOY, shp)
    
  } else {
    # === COPERNICUS RASTER REFERENCE (pre-2024) ===
    
    # Load & prepare Copernicus reference
    # if 2022 & 2023 V02
    # if 2017:2021 V01
    
    if(i %in% c(2022, 2023)){
      ref_file <- list.files(paste0(copernicusRef_files,"CLMS_HRLVLCC_GRAMD1_S" , i, "_R10m_E43N26_03035_V01_R00"), pattern = "_R00\\.tif$", full.names = TRUE)
      refCop <- rast(ref_file)
    } else {
      ref_file <- list.files(paste0(copernicusRef_files,"CLMS_HRLVLCC_GRAMD1_S" , i, "_R10m_E43N26_03035_V02_R00/"), pattern = "_R00\\.tif$", full.names = TRUE)
      refCop <- rast(ref_file)
    }
    
    #refCop <- ifel(refCop %in% c(0, 400), NA, refCop)
    
    refCop_prj        <- project(refCop, crs(predDOY))
    refCop_resampled  <- resample(crop(refCop_prj, predDOY), predDOY)
    refCop_num        <- as.numeric(refCop_resampled)
    refCop_clean      <- ifel(refCop_num < 1 | refCop_num > 365, NA, refCop_num)
    
    copernicus_vals <- terra::extract(refCop_clean, webcam_files)
    copernicus_vals_test <- copernicus_vals$class_name
  }
  
  # --- 6. Plot NDVI time series for each sampled outlier pixel 
  for (j in 1:nrow(webcam_files)) {
    
    pixel_ts <- as.numeric(ts_outliers[j, -1])
    valid    <- !is.na(pixel_ts)
    
    df_plot <- data.frame(
      doy  = doy[valid],
      ndvi = pixel_ts[valid]
    )
    
    # --- Decide which validation source this year uses
    if (i %in% c(2024, 2025)) {
      events <- data.frame(
        doy    = c(ref_vals[j,], pred_vals[j,]),
        source = c("Reference", "Predicted")   
      )
      source_vals   <- c("Reference" = "forestgreen", "Predicted" = "#d73027")
      source_labels <- c("Reference", "Predicted")
      source_linetypes <- c("Reference" = "dashed", "Predicted" = "solid")
    } else {
      events <- data.frame(
        doy    = c(copernicus_vals_test[j], pred_vals_test[j]),
        source = c("Copernicus", "Predicted")   
      )
      source_vals   <- c("Copernicus" = "#4575b4", "Predicted" = "#d73027")
      source_labels <- c("Copernicus", "Predicted")
      source_linetypes <- c("Copernicus" = "dashed", "Predicted" = "solid")
    }
    
    p <- ggplot(df_plot, aes(x = doy, y = ndvi/10000)) +
      geom_line(color = "darkgrey", size = 0.8, alpha = 0.9) +
      geom_point(color = "black", size = 0.8, alpha = 0.6) +
      geom_vline(
        data = events,
        aes(xintercept = doy, colour = source, linetype = source),
        size = 0.8
      ) +
      scale_colour_manual(
        values = source_vals,
        labels = source_labels
      ) +
      scale_linetype_manual(
        values = source_linetypes,
        labels = source_labels
      ) +
      ylim(0,1)+
      labs(
        title    = "NDVI Time Series - Mowing Detection",
        subtitle = paste("Year:", i, " | Pixel", j),
        x = "Day of Year (DOY)",
        y = "NDVI"
      ) +
      pub_theme +
      theme(legend.position = "bottom")
    
    # Save high‑resolution, publication‑ready PNG
    ggsave(
      filename = file.path(out_dir, paste0("outlier_ndviTSS_", i, "_" ,j, ".png")),
      plot     = p,
      width    = 12,
      height   = 8,
      dpi      = 300,
      bg       = "white",
      limitsize = FALSE
    )
  }
}

########################################################################################
##### --- 2023 Webcam analysis --- #####
webcam_files <- st_read("/mnt/CEPH_PROJECTS/Environtwin/gis/reference/MH/webcam/webcam_mowing.shp") 
test <- webcam_files %>% 
  filter(fid == 89)
ndvi_files <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/03/MH/20230201-20231129_032-333_TSA_PLA_NDV_TSS.bsq")
predDOY_files <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level4_sites/mowing/03_v14_v4/MH/final/MH_PLA_2023_doy1.tif")

dates_files <- read.table("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/03/MH/dates_2023_PLA.txt")

# Extract DOY from V1
date_str <- as.character(dates_files$V1)  

doy <- as.numeric(
  sub(".*_(\\d+)_PLA", "\\1", date_str)
)

test$mowing_date <- as.Date(test$mowdate1, format="%Y%m%d")
test$DOY <- as.integer(format(test$mowing_date, "%j"))

# extract ndvi time series
ndvi_ts <- terra::extract(ndvi_files, test, fun = "mean")
predDOY <- terra::extract(predDOY_files, test, fun = "modal")
names(predDOY) <- c("ID", "DOY")

pixel_ts <- as.numeric(ndvi_ts[,-1])
valid    <- !is.na(pixel_ts)

df_plot <- data.frame(
  doy  = doy[valid],
  ndvi = pixel_ts[valid]
)

events <- data.frame(
  doy    = c(test$DOY, predDOY$DOY),
  source = c("Webcam", "Predicted")   
)

source_vals   <- c("Predicted" = "#4575b4", "Webcam" = "#d73027")
source_labels <- c("Predicted", "Webcam")
source_linetypes <- c("Predicted" = "dashed", "Webcam" = "solid")

# Set up a publication‑style theme
pub_theme <- theme_minimal() +
  theme(
    axis.title       = element_text(size = 16, face = "plain"),
    axis.text        = element_text(size = 14),
    axis.line        = element_line(linewidth = 0.5, colour = "black"),
    axis.ticks       = element_line(linewidth = 0.3, colour = "black"),
    panel.grid.major = element_line(linewidth = 0.1, colour = "grey80"),
    panel.grid.minor = element_blank(),
    legend.position  = "bottom",
    legend.direction = "horizontal",
    legend.title     = element_blank(),
    legend.text      = element_text(size = 14),
    plot.title       = element_text(size = 16, hjust = 0.0),
    plot.subtitle    = element_text(size = 14, hjust = 0.0),
    plot.margin      = margin(4, 4, 4, 4, unit = "pt")
  )

ggplot(df_plot, aes(x = doy, y = ndvi/10000)) +
  geom_line(color = "darkgrey", size = 0.8, alpha = 0.9) +
  geom_point(color = "black", size = 0.8, alpha = 0.6) +
  geom_vline(
    data = events,
    aes(xintercept = doy, colour = source, linetype = source),
    size = 0.8
  ) +
  scale_colour_manual(
    values = source_vals,
    labels = source_labels
  ) +
  scale_linetype_manual(
    values = source_linetypes,
    labels = source_labels
  ) +
  ylim(0,1)+
  labs(
    title    = "NDVI Time Series - Mowing Detection",
    x = "Day of Year (DOY)",
    y = "NDVI"
  ) +
  pub_theme +
  theme(legend.position = "bottom")

