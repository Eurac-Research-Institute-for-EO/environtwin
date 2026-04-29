# ===============================
# PlanetScope vs Sentinel-2 Comparison
# ===============================

# ---- 1. Load libraries ----
library(terra)
library(ggplot2)
library(dplyr)
library(tidyr)

compare_planet <- function(r1, r2, name1 = "P1", name2 = "P2", sample_size = 50000, out_dir = NULL) {
  
  # Check geometry
  compareGeom(r1, r2, stopOnError = TRUE)
  
  # Convert to dataframe
  df <- as.data.frame(c(r1, r2), na.rm = TRUE)
  
  colnames(df) <- c(paste0(name1, "_", c("Blue","Green","Red","NIR")),
                    paste0(name2, "_", c("Blue","Green","Red","NIR")))
  
  # Sample
  set.seed(42)
  if (nrow(df) > sample_size) {
    df <- df[sample(nrow(df), sample_size), ]
  }
  
  # Reshape
  df_long <- df %>%
    mutate(pixel_id = row_number()) %>%  
    pivot_longer(
      cols = -pixel_id,
      names_to = c("Sensor","Band"),
      names_sep = "_",
      values_to = "Reflectance"
    ) %>%
    pivot_wider(
      id_cols = c(pixel_id, Band),
      names_from = Sensor,
      values_from = Reflectance
    )
  
  df_long$Band <- factor(df_long$Band,
                         levels = c("Blue", "Green", "Red", "NIR"))
  
  # Stats
  stats <- df_long %>%
    group_by(Band) %>%
    summarise(
      Bias = mean(.data[[name1]] - .data[[name2]]),
      RMSE = sqrt(mean((.data[[name1]] - .data[[name2]])^2)),
      R    = cor(.data[[name1]], .data[[name2]]),
      R2   = R^2,
      .groups = "drop"
    ) %>%
    mutate(label = paste0(
      "R²=", round(R2,3),
      "\nRMSE=", round(RMSE,3),
      "\nBias=", round(Bias,3)
    ))
  
  lims <- range(c(df_long[[name1]], df_long[[name2]]), na.rm = TRUE)
  
  # Plot
  p <- ggplot(df_long, aes(x = .data[[name1]], y = .data[[name2]])) +
    geom_point(alpha = 0.2, size = 0.4) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
    geom_smooth(method = "lm", color = "black", linewidth = 0.6) +
    facet_wrap(~Band) +
    coord_equal(xlim = lims, ylim = lims) +
    labs(
      title = paste("Radiometric Comparison:", name1, "vs", name2),
      x = name1,
      y = name2
    ) +
    theme_bw()
  
  p_final <- p +
    geom_text(
      data = stats,
      aes(x = Inf, y = -Inf, label = label),
      hjust = 1.1,
      vjust = -0.5,
      inherit.aes = FALSE
    )
  
  # Save automatically if folder provided
  if (!is.null(out_dir)) {
    filename <- file.path(out_dir,
                          paste0(name1, "_vs_", name2, ".png"))
    
    ggsave(filename,
           plot = p_final,
           width = 12,
           height = 8,
           dpi = 300)
  }
  
  
  return(list(plot = p_final, stats = stats))
}

# ---- 1. Load data ----

udm2024 <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard/20240714_102434_03_24aa_PLANET_udm2_buffer.tif")
udm2018 <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard/20180712_094123_1012_PLANET_udm2_buffer.tif")
udm2021 <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard/20210721_095618_82_105c_PLANET_udm2_buffer.tif")

planet2024 <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard/20240714_102434_03_24aa_PLANET_PSB.SD_BOA.tif")
planet2018 <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard/20180712_094123_1012_PLANET_PS2_BOA.tif")
planet2021 <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard/20210721_095618_82_105c_PLANET_PS2.SD_BOA.tif")

# ----- 2. Mask images ----
planet2024_masked <- mask(planet2024, udm2024[[1]] == 1, maskvalues = FALSE)
planet2018_masked <- mask(planet2018, udm2018[[1]] == 1, maskvalues = FALSE)
planet2021_masked <- mask(planet2021, udm2021[[1]] == 1, maskvalues = FALSE)

rasters <- list(
  P2018 = planet2018_masked,
  P2021 = planet2021_masked,
  P2024 = planet2024_masked
)

pairs <- combn(names(rasters), 2, simplify = FALSE)

results <- lapply(pairs, function(pair) {
  compare_planet(
    rasters[[pair[1]]],
    rasters[[pair[2]]],
    pair[1],
    pair[2],
    out_dir = "/mnt/CEPH_PROJECTS/Environtwin/figures/presentation/"
  )
})

print(results[[1]]$plot)
print(results[[2]]$plot)
print(results[[3]]$plot)






















sentinel <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/SEN2/20240714_LEVEL2_SEN2B_BOA.tif")
sentinel <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/SEN2/20180623_LEVEL2_SEN2B_BOA.tif")

#######################################################
#### 2. Mask Planet data


# ---- 3. Check geometry ----
compareGeom(planet, sentinel, stopOnError = TRUE)

# ---- 4. Convert to dataframe ----
df <- as.data.frame(c(planet, sentinel), na.rm = TRUE)
df <- as.data.frame(c(planet1, planet2), na.rm = TRUE)

# Rename columns (adjust if needed)
colnames(df) <- c("P_Blue","P_Green","P_Red","P_NIR",
                  "S_Blue","S_Green","S_Red","S_NIR")

colnames(df) <- c("P_Blue","P_Green","P_Red","P_NIR",
                  "P2_Blue","P2_Green","P2_Red","P2_NIR")

# ---- 5. Optional: sample pixels for speed ----
set.seed(42)
if (nrow(df) > 50000) {
  df <- df[sample(nrow(df), 50000), ]
}

# ---- 6. Reshape to long format ----
df_long <- df %>%
  mutate(pixel_id = row_number()) %>%  
  pivot_longer(
    cols = -pixel_id,
    names_to = c("Sensor","Band"),
    names_sep = "_",
    values_to = "Reflectance"
  ) %>%
  pivot_wider(
    id_cols = c(pixel_id, Band),        
    names_from = Sensor,
    values_from = Reflectance
  )

df_long$Band <- factor(df_long$Band,
                       levels = c("Blue", "Green", "Red", "NIR"))

# ---- 7. Compute statistics per band ----
stats <- df_long %>%
  group_by(Band) %>%
  summarise(
    Bias = mean(P - P2),
    RMSE = sqrt(mean((P - P2)^2)),
    MAE  = mean(abs(P - P2)),
    R    = cor(P, P2),
    R2   = R^2,
    .groups = "drop"
  )

stats$Band <- factor(stats$Band,
                     levels = c("Blue", "Green", "Red", "NIR"))

print(stats)

# ---- 8. Prepare annotation labels ----
stats <- stats %>%
  mutate(label = paste0(
    "R² = ", round(R2, 3), "\n",
    "RMSE = ", round(RMSE, 3), "\n",
    "Bias = ", round(Bias, 3)
  ))

lims <- range(c(df_long$P, df_long$P2), na.rm = TRUE)

# ---- 9. Create plot ----
p <- ggplot(df_long, aes(x = P, y = P2)) +
  geom_point(alpha = 0.2, size = 0.4) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
  geom_smooth(method = "lm", color = "black", linewidth = 0.6) +
  facet_wrap(~Band) +   
  coord_equal(xlim = lims, ylim = lims) +  
  labs(
    title = "Radiometric Comparison: PlanetScope generations",
    x = "PlanetScope Reflectance 2024",
    y = "PlanetScope Reflectance 2018"
  ) +
  theme_bw(base_size = 12) +
  theme(
    strip.text = element_text(face = "bold"),
    plot.title = element_text(face = "bold", hjust = 0.5)
  )
# ---- 10. Add statistics annotations ----
p_final <- p +
  geom_text(
    data = stats,
    aes(
      x = Inf, y = -Inf,
      label = label
    ),
    hjust = 1.1,
    vjust = -0.5,
    size = 3,
    inherit.aes = FALSE
  )

# ---- 11. Display plot ----
print(p_final)

# ---- 12. Save figure ----
ggsave("/mnt/CEPH_PROJECTS/Environtwin/figures/presentation/Planet2014_vs_Planet2018_comparison.png",
       plot = p_final,
       width = 12,
       height = 8,
       dpi = 300)

#######################################
# ---- 14. PLot residuals ----
df_long <- df_long %>%
  mutate(residual = P - S)

p_box <- ggplot(df_long, aes(x = Band, y = residual)) +
  
  geom_boxplot(outlier.size = 0.5) +
  
  geom_hline(yintercept = 0, linetype = "dashed") +
  
  labs(
    title = "Planet Sentinel Residuals per Band",
    x = "Band",
    y = "Residual (P - S)"
  ) +
  
  theme_bw(base_size = 12)

print(p_box)

ggsave("/mnt/CEPH_PROJECTS/Environtwin/figures/presentation/Planet_vs_Sentinel_residuals_2018.png",
       plot = p_final,
       width = 12,
       height = 8,
       dpi = 300)

# residual raster stack (P - S)
residual <- planet - sentinel

# name layers
names(residual) <- c("Blue", "Green", "Red", "NIR")

lims <- max(abs(values(residual)), na.rm = TRUE)

par(mfrow = c(2, 2), mar = c(3, 3, 3, 5))

plot(residual[[1]],
     main = "Blue Band Residual (P - S)",
     col = hcl.colors(100, "RdBu", rev = TRUE),
     zlim = c(-lims, lims))

plot(residual[[2]],
     main = "Green Band Residual (P - S)",
     col = hcl.colors(100, "RdBu", rev = TRUE),
     zlim = c(-lims, lims))

plot(residual[[3]],
     main = "Red Band Residual (P - S)",
     col = hcl.colors(100, "RdBu", rev = TRUE),
     zlim = c(-lims, lims))

plot(residual[[4]],
     main = "NIR Band Residual (P - S)",
     col = hcl.colors(100, "RdBu", rev = TRUE),
     zlim = c(-lims, lims))


writeRaster(residual, "/mnt/CEPH_PROJECTS/Environtwin/FORCE/residuals_2018.tif")
