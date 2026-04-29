library(terra)
library(sf)
library(dplyr)
library(ggplot2)
library(viridis)
library(patchwork)

# Load AOI shapefile
tests <- st_read("/mnt/CEPH_PROJECTS/Environtwin/gis/mowing_evaluation_merge.shp")

#################################################################################
#### ---- find pairs of sentinel and planet data ---- ####

test_files <- list.files("/mnt/CEPH_PROJECTS/Environtwin/FORCE/test/40/test/", pattern = "_BOA\\.tif$",
                        full.names = T)
std_files <- list.files("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_daily/01/MH/", pattern = "_BOA\\.tif$",
                        full.names = T)

#test_ids <- sub("_PLA.*$", "", basename(test_files))
#std_ids <- sub("_PLA.*$", "", basename(std_files))

test_ids <- substr(basename(test_files), 1, 8)
std_ids <- substr(basename(std_files), 1, 8)

# find difference between the ids
common_ids <- intersect(std_ids, test_ids)

extract_year <- function(file) substr(basename(file), 1, 4)
test_files <- test_files[extract_year(test_files) %in% years]
std_files <- std_files[extract_year(std_files) %in% years]

dates <- substr(basename(test_files), 1, 8)
unique_dates <- unique(dates)

# Extract dates for TEST and Standard
test_dates <- unique(substr(basename(test_files), 1, 8))
std_dates  <- unique(substr(basename(std_files), 1, 8))

# Keep only dates that exist in both
common_dates <- intersect(test_dates, std_dates)

common_dates

test_files <- test_files[substr(basename(test_files), 1, 8) %in% common_dates]
std_files  <- std_files[substr(basename(std_files), 1, 8) %in% common_dates]

get_values <- function(files, source_name) {
  results <- lapply(files, function(f) {
    ras <- rast(f)
    date <- substr(basename(f), 1, 8)
    
    terra::extract(ras, tests, progress = FALSE) %>%
      mutate(source = source_name,
             date = date)
  })
  
  bind_rows(results)
}

test_extr <- get_values(test_files, "TEST")
std_extr  <- get_values(std_files, "Standard")

combined_df <- bind_rows(test_extr, std_extr)

long_df <- combined_df %>%
  pivot_longer(
    cols = -c(ID, source, date),
    names_to = "band",
    values_to = "value"
  ) %>%
  mutate(band = factor(band, levels = c("blue", "green", "red", "nir")))

# Aggregate duplicates: mean per polygon, band, date, source
long_df_agg <- long_df %>%
  group_by(ID, date, band, source) %>%
  summarise(value = mean(value, na.rm = TRUE), .groups = "drop")

# Loop over bands
bands <- levels(long_df_agg$band)
dates <- unique(long_df_agg$date)

# long_df_agg: polygon-level aggregated reflectance per date, band, source
# columns: ID | date | band | source | value

plot_radiometric_comparison <- function(long_df_agg, rescale = TRUE) {
  
  df <- long_df_agg
  
  # Optional rescaling if values > 1 (common for Planet/Sentinel BOA)
  if(rescale) {
    df <- df %>%
      mutate(value = value / 10000)
  }
  
  bands <- levels(df$band)
  dates <- unique(df$date)
  
  plots <- list()
  
  for(b in bands) {
    for(d in dates) {
      # Prepare scatter data
      scatter_data <- df %>%
        filter(band == b, date == d) %>%
        select(ID, source, value) %>%
        pivot_wider(names_from = source, values_from = value)
      
      # Skip if data incomplete
      if(!all(c("TEST","Standard") %in% names(scatter_data))) next
      
      # Compute band statistics safely
      band_stats <- scatter_data %>%
        filter(!is.na(TEST) & !is.na(Standard)) %>%  # keep only complete pairs
        summarise(
          bias = mean(Standard - TEST, na.rm = TRUE),
          mae  = mean(abs(Standard - TEST), na.rm = TRUE),
          rmse = sqrt(mean((Standard - TEST)^2, na.rm = TRUE)),
          r    = ifelse(n() > 1, cor(TEST, Standard, use = "complete.obs"), NA_real_)
        )
      
      # Format annotation text
      annot_text <- paste0(
        "bias = ", round(band_stats$bias, 4), "\n",
        "MAE  = ", round(band_stats$mae, 4), "\n",
        "RMSE = ", round(band_stats$rmse, 4), "\n",
        "r    = ", round(band_stats$r, 4)
      )
      
      # Scatter plot with annotation
      p_scatter <- ggplot(scatter_data, aes(x = TEST, y = Standard)) +
        geom_point(alpha = 0.5) +
        geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "red") +
        annotate(
          "text",
          x = -Inf,     # left edge
          y = Inf,      # top edge
          label = annot_text,
          hjust = -0.1, # push slightly right into panel
          vjust = 1.1   # push slightly down into panel
        ) +
        theme_classic()
        labs(title = paste0("Band: ", b, " | Date: ", d),
             x = "TEST Reflectance",
             y = "Standard Reflectance")
      
      # Violin plot
      #violin_data <- df %>% filter(band == b, date == d)
      #p_violin <- ggplot(violin_data, aes(x = source, y = value, fill = source)) +
      #  geom_violin(trim = FALSE) +
      #  geom_boxplot(width = 0.1, fill = "white", outlier.size = 0.5) +
      #  theme_classic() +
      #  labs(title = paste0("Band: ", b, " | Date: ", d),
      #       x = "Source", y = "Reflectance") +
      #  scale_fill_viridis_d(option = "D")
      
      # Combine scatter + violin vertically
      #p_combined <- p_scatter / p_violin
      plots[[paste(b,d,sep="_")]] <- p_scatter
    }
  }
  
  return(plots)
}

plots_all <- plot_radiometric_comparison(long_df_agg, rescale = TRUE)

# Show first plot
plots_all[[10]]
plots_all[[44]]

# Save all plots
for(nm in names(plots_all)){
  ggsave(filename = paste0("Radiometric_40Perc_", nm, ".png"),
         plot = plots_all[[nm]], width = 12, height = 8)
}
