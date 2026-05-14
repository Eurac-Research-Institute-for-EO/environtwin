library(terra)
library(ggplot2)
library(cowplot)
library(dplyr)
library(sf)

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
  
  list(mae = mae, rmse = rmse, cor = cor_val)
}

all_residuals <- list()
all_metrics <- list()

for (i in years) {
  
  message("Processing year: ", i)
  
  for (j in 1:4) {
    
    # ---- predicted raster for DOY j ----
    pred_file <- list.files(
      predDOY_files,
      pattern = paste0(site, "_", sensor, "_", i, "_doy", j, "\\.tif$"),
      full.names = TRUE
    )
    
    pred_r <- rast(pred_file)
    
    pred_r <- ifel(pred_r <= 0, NA, pred_r)
    
    # ---- reference raster for DOY j ----
    if (i %in% c(2022, 2023)) {
      ref_file <- list.files(
        paste0(copernicusRef_files, "CLMS_HRLVLCC_GRAMD", j , "_S" ,i, "_R10m_E43N26_03035_V01_R00"),
        pattern = paste0(".*_R00\\.tif$"),
        full.names = TRUE
      )
    } else {
      ref_file <- list.files(
        paste0(copernicusRef_files, "CLMS_HRLVLCC_GRAMD", j , "_S", i, "_R10m_E43N26_03035_V02_R00/"),
        pattern = paste0(".*_R00\\.tif$"),
        full.names = TRUE
      )
    }
    
    ref_r <- rast(ref_file)
    
    # ---- align reference to prediction ----
    ref_prj <- project(ref_r, crs(pred_r))
    ref_res <- resample(crop(ref_prj, pred_r), pred_r)
    
    ref_res <- ifel(ref_res < 1 | ref_res > 365, NA, ref_res)
    
    # ---- pixel-wise comparison ----
    pred_vals <- values(pred_r, mat = FALSE)
    ref_vals  <- values(ref_res, mat = FALSE)
    
    ok <- !is.na(pred_vals) & !is.na(ref_vals)
    
    pred_vec <- pred_vals[ok]
    ref_vec  <- ref_vals[ok]
    
    metrics <- compute_metrics(pred_vec, ref_vec)
    
    all_metrics[[length(all_metrics) + 1]] <- data.frame(
      year = i,
      doy = paste0("Event", j),
      mae = metrics$mae,
      rmse = metrics$rmse,
      cor = metrics$cor
    )
    
    all_residuals[[length(all_residuals) + 1]] <- data.frame(
      year = i,
      doy = paste0("Event", j),
      residual = pred_vec - ref_vec
    )
  }
}

residual_df <- bind_rows(all_residuals)
metrics_df   <- bind_rows(all_metrics)

set.seed(123)

residual_df_sample <- residual_df %>%
  slice_sample(n = 50000)

# ---- summary stats ----
bias_df <- residual_df_sample %>%
  group_by(year, doy) %>%
  summarise(
    bias = mean(residual, na.rm = TRUE),
    .groups = "drop"
  )

n_df <- residual_df_sample %>%
  group_by(year, doy) %>%
  summarise(
    n = n(),
    .groups = "drop"
  )

# ---- plot ----
p <- ggplot(residual_df_sample, aes(x = factor(doy), y = residual)) +
  
  # distribution
  geom_boxplot(
    width = 0.6,
    outlier.alpha = 0.25,
    fill = "grey85",
    color = "grey30",
    linewidth = 0.3
  ) +
  
  # mean bias (blue points)
  geom_point(
    data = bias_df,
    aes(x = factor(doy), y = bias),
    color = "darkred",
    size = 1.4
  ) +
  
  # zero reference line
  geom_hline(
    yintercept = 0,
    linetype = "dashed",
    color = "darkgrey",
    linewidth = 0.2
  ) +
  
  facet_wrap(~ year, scales = "fixed") +
  
  labs(
    x = "Mowing event",
    y = "Prediction error (days)",
    title = "Model residuals across mowing events"
  ) +
  
  theme_classic() +
  
  theme(
    plot.title = element_text(hjust = 0.5, face = "bold", size = 14),
    axis.title = element_text(face = "bold", size = 16),
    axis.text = element_text(color = "black", size = 14),
    strip.background = element_rect(fill = "grey95", color = NA),
    strip.text = element_text(face = "bold", size = 12),
    legend.position = "none"
  )

p

ggsave(
  file.path(out_dir, paste0("combined_residual_boxplots_AllEvents", site, ".png")),
  p,
  width = 18,
  height = 10,
  dpi = 300
)
