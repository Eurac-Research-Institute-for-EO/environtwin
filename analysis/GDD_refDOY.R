library(terra)
ndvi <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/03/MH/renamed/20250302-20251122_061-326_TSA_PLA_NDV_TSS.bsq")
gdd  <- rast("/mnt/CEPH_PROJECTS/Environtwin/GDD/SouthTyrol/gdd/GDD_daily_cumulative_T5_2025.tif")
mowing <- rast("/mnt/CEPH_PROJECTS/Environtwin/gis/reference/MH/MH_2025_doy.tif")

# Set -1 and 0 to NA (nodata)
mowing_clean <- app(mowing, fun = function(x) { ifelse(x <= 0, NA, x) })

dates <- as.Date(substr(names(ndvi), 1, 8), "%Y%m%d")
doy_ndvi <- as.integer(format(dates, "%j"))

# Ensure alignment first (using fixed DOY extraction)
doy_gdd <- as.integer(sub(".*_(\\d+)$", "\\1", names(gdd)))
common_doy <- intersect(doy_ndvi, doy_gdd)
ndvi_idx <- which(doy_ndvi %in% common_doy)
gdd_idx <- which(doy_gdd %in% common_doy)
ndvi_ts <- ndvi[[ndvi_idx]]
gdd_ts <- gdd[[gdd_idx]]
doy_common <- sort(common_doy)

#  crop gdd to ndvi
gdd_crop <- crop(gdd_ts, ndvi_ts)
# Step 2: Resample to match NDVI exactly (critical step)
gdd_aligned <- resample(gdd_crop, ndvi_ts, method="bilinear")

# Sample 20 pixels (adjust size as needed; use method="regular" for grid)
sample_pts <- spatSample(ndvi$`20250807_219_PLA`, size=20, method="random", na.rm=TRUE, as.points=TRUE, values=FALSE)

# Extract time series (perfect alignment guaranteed)
ndvi_samples <- terra::extract(ndvi_ts, sample_pts)
gdd_samples <- terra::extract(gdd_aligned, sample_pts)

# Clean data frame
df_samples <- data.frame(
  doy = doy_common,
  date = as.Date(paste("2025", doy_common), "%Y %j")
)

# Transpose samples (pixels as columns)
ndvi_mat <- t(ndvi_samples[, -1])  # Remove ID column
gdd_mat <- t(gdd_samples[, -1])
colnames(ndvi_mat) <- paste0("NDVI_Pixel_", 1:ncol(ndvi_mat))
colnames(gdd_mat) <- paste0("GDD_Pixel_", 1:ncol(gdd_mat))

df_samples <- cbind(df_samples, ndvi_mat, gdd_mat)

# Long format for GDD (all pixels)
df_gdd_long <- df_samples %>%
  select(doy, starts_with("GDD_Pixel_")) %>%
  pivot_longer(-doy, names_to="pixel", values_to="gdd") %>%
  mutate(pixel = factor(sub("GDD_", "", pixel)))

# Plot ALL pixels together
ggplot(df_gdd_long, aes(x=doy, y=gdd, color=pixel, group=pixel)) +
  geom_line(linewidth=0.8, alpha=0.7) +
  labs(title="All Sample Pixels: Daily GDD Time Series (T5)",
       x="Day of Year 2025", y="Daily GDD (°C-days)") +
  theme_minimal() +
  theme(legend.position="bottom")

########## gdd + mowing
# Align to NDVI extent/resolution
mowing_aligned <- resample(crop(mowing_clean, ndvi_ts), ndvi_ts)

## Get mowing events per pixel from your existing sample_pts
mowing_samples <- terra::extract(mowing_aligned, sample_pts)

# Create mowing time series matrix (NA = no mowing, DOY = mowing event)
mowing_ts_df <- t(mowing_samples[, -1])  # Remove ID column
colnames(mowing_ts_df) <- paste0("Mowing_Pixel_", 1:ncol(mowing_ts_df))

# Find pixels WITH mowing events only
pixels_with_mowing <- c()
for(i in 1:ncol(mowing_ts_df)) {
  mows <- mowing_ts_df[,i]
  if(any(!is.na(mows))) {
    pixels_with_mowing <- c(pixels_with_mowing, i)
  }
}
cat("Pixels with mowing:", length(pixels_with_mowing), "\n")

# Split into two groups of 12 (or fewer)
n_plots <- length(pixels_with_mowing)
group1 <- pixels_with_mowing[1:min(12, n_plots)]
group2 <- pixels_with_mowing[13:min(24, n_plots)]

# Function for single pixel plot
plot_single_pixel <- function(pixel_id) {
  gdd_col <- paste0("GDD_Pixel_", pixel_id)
  mow_col <- paste0("Mowing_Pixel_", pixel_id)
  
  mows <- df_samples_mowing[[mow_col]]
  mow_days <- mows[!is.na(mows)]
  
  ggplot(df_samples_mowing, aes(x=doy, y=.data[[gdd_col]])) +
    geom_line(color="steelblue", linewidth=1.2) +
    geom_vline(xintercept=mow_days, color="red", linetype="dashed", linewidth=1.5) +
    labs(title=paste("Pixel", pixel_id), x="", y="") +
    theme_minimal() +
    theme(plot.title = element_text(size=10, hjust=0.5),
          axis.text.x = element_text(size=8))
}

# FIRST 3x4 GRID
p1 <- wrap_plots(lapply(group1, plot_single_pixel), ncol=4, nrow=3)
p1 <- p1 + plot_annotation(title="GDD + Mowing Events - Pixels 1-12 (WITH mowing only)")

# SECOND 3x4 GRID  
if(length(group2) > 0) {
  p2 <- wrap_plots(lapply(group2, plot_single_pixel), ncol=4, nrow=3)
  p2 <- p2 + plot_annotation(title="GDD + Mowing Events - Pixels 13+ (WITH mowing only)")
  
  # Combine both grids
  combined <- (p1 / p2) + plot_layout(heights=c(1,1))
  print(combined)
} else {
  print(p1)
}

####################################################################################
##### Reference shapefile + GDD
mowing <- st_read("/mnt/CEPH_PROJECTS/Environtwin/gis/reference/MH/lafis_mahd_point_2025_MH.shp")
zones <- st_read("/mnt/CEPH_PROJECTS/Environtwin/gis/outlines/Mals_zones_32632.shp") %>% 
  dplyr::select(fid, geometry)

clipped <- st_intersection(mowing, mals)

# Parse mowing dates (adjust column name - check with colnames(mowing))
clipped$mowing_date <- as.Date(clipped$pixdoy1, format="%Y%m%d")
clipped$mowing_doy <- as.integer(format(clipped$mowing_date, "%j"))

# Extract GDD + add zone info
gdd_extr <- extract(gdd_aligned, clipped)
gdd_extr$Zone <- clipped$fid  # Zone ID from intersection
gdd_extr$row_id <- 1:nrow(gdd_extr)  # Track original row
gdd_extr$doyRef <- clipped$mowing_doy

# CORRECT pattern for your cumulative GDD columns
gdd_cols <- grep("GDD_daily_cumulative_T5_2025", colnames(gdd_extr), value = TRUE)
cat("GDD columns found:", length(gdd_cols), "\n")

gdd_long <- gdd_extr %>%
  filter(!is.na(doyRef)) %>%  # Filter first
  select(ID, Zone, doyRef, all_of(gdd_cols)) %>%
  pivot_longer(cols = all_of(gdd_cols), names_to = "layer", values_to = "gdd") %>%
  mutate(
    doy = as.integer(sub(".*_(\\d+)$", "\\1", layer)),  # FIXED regex
    field_id = ID  # Use ID, not row_number()
  ) %>%
  filter(!is.na(gdd))

cat("Long format:", nrow(gdd_long), "rows\n")

# Find TOP 10 EARLIEST mowing fields per zone
top10_earliest <- gdd_extr %>%
  filter(!is.na(doyRef)) %>%
  group_by(Zone) %>%
  slice_min(doyRef, n = 5, with_ties = FALSE) %>%  # 10 earliest per zone
  arrange(Zone, doyRef) %>%
  ungroup()

cat("Top 10 earliest fields per zone:\n")
print(top10_earliest %>% select(Zone, ID, doyRef) %>% arrange(Zone, doyRef))

# Filter gdd_long to these top 10 earliest fields per zone
earliest_ids <- top10_earliest$ID
gdd_top10 <- gdd_long[gdd_long$field_id %in% earliest_ids, ]

# Set FIXED y-axis limits (same for all plots)
y_min <- 0
y_max <- max(gdd_top10$gdd, na.rm = TRUE) * 1.05  # 5% padding

# Function for 2x5 grid with SAME Y-AXIS
zone_top10 <- gdd_top10[gdd_top10$Zone == zone_id, ]

plot_top10_zone <- function(zone_id) {
  zone_top10 <- gdd_top10[gdd_top10$Zone == zone_id, ]
  
  field_order <- zone_top10 %>%
    group_by(ID) %>%
    summarise(doyRef = first(doyRef)) %>%
    arrange(doyRef)
  
  field_ids <- field_order$ID
  
  
  plots <- lapply(seq_along(field_ids), function(f_idx) {
    f_id <- field_ids[f_idx]
    f_data <- zone_top10[zone_top10$ID == f_id, ]
    mow_doy <- unique(f_data$doyRef)
    
    ggplot(f_data, aes(x = doy, y = gdd)) +
      geom_line(color = "darkgrey", linewidth = 1.4) +
      geom_vline(xintercept = mow_doy, 
                 color = "red", linetype = "dashed", linewidth = 1) +
      labs(subtitle = paste("Mowing DOY:", mow_doy),
           x = "", y = "") +
      scale_y_continuous(limits = c(y_min, y_max), expand = c(0, 0)) +
      theme_minimal() +
      theme(
        plot.title = element_text(size = 10, hjust = 0.5),
        plot.subtitle = element_text(size = 12, hjust = 0.5),
        axis.text.x = element_text(hjust = 1, size = 12),
        axis.text.y = element_text(hjust = 1, size = 12)
      )
  })
  
  wrap_plots(plots, ncol = 3, nrow = 2) +
    plot_annotation(
      title = paste("Zone", zone_id, "- Earliest Mowing Fields"),
      theme = theme(plot.title = element_text(size = 14, hjust = 0.5))
    )
}

# Create 3 plots for Zones 1, 2, 3
p1 <- plot_top10_zone(1)
p2 <- plot_top10_zone(2)
p3 <- plot_top10_zone(3)

# Arrange side-by-side
#combined_plots <- p1 | p2 | p3
print(p1)
