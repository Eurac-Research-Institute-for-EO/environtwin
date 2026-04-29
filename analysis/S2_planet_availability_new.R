library(terra)
library(sf)
library(dplyr)
library(ggplot2)
library(viridis)
library(patchwork)
library(tidyr)
library(stringr)

# Load AOI
mals <- vect("/mnt/CEPH_PROJECTS/Environtwin/gis/outlines/mals_ohneBMS_32632.shp")
SA <- vect("/mnt/CEPH_PROJECTS/Environtwin/gis/boundaries/SA/SA.shp")

###### COVERAGE
get_coverage_timeseries_fast <- function(raster_dir, pattern, aoi, platform_name, type) {
  
  files <- list.files(raster_dir, pattern = pattern, full.names = TRUE)
  
  date_str <- stringr::str_extract(basename(files), "\\d{8}")
  dates <- as.Date(date_str, "%Y%m%d")
  
  out <- lapply(seq_along(files), function(i) {
    
    r <- rast(files[i], lyrs = 1)
    r <- crop(r, aoi)
    
    coverage_percent <- global(r, "notNA", na.rm = TRUE)[1,1] / ncell(r) * 100
    
    data.frame(Date = dates[i], coverage = coverage_percent)
  })
  
  bind_rows(out) %>%
    complete(Date = seq(min(Date), max(Date), by = "day"),
             fill = list(coverage = 0)) %>%
    mutate(platform = platform_name,
           type = type)
}

##### POTENTIAL COVEAGE ###############
### --- 1. PlanetScope
planet_cov_total <- get_coverage_timeseries_fast(
  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard",
  ".*PLANET.*_BOA\\.tif$",
  mals,
  "PlanetScope",
  "potential"
)

planet_cov_total <- planet_cov_total %>%
  group_by(Date, platform, type) %>%
  summarise(
    coverage = pmin(round(sum(coverage, na.rm = TRUE)), 100),
    .groups = "drop"
  )


### --- 2. Sentinel
s2_cov_total <- get_coverage_timeseries_fast(
  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/SEN2/",
  "SEN2.*_BOA\\.tif$",
  mals,
  "Sentinel-2",
  type = "potential"
)

s2_cov_total <- s2_cov_total %>%
  group_by(Date, platform, type) %>%
  summarise(
    coverage = pmin(round(sum(coverage, na.rm = TRUE)), 100),
    .groups = "drop"
  )


##### ACTUAL COVEAGE ###############
### --- 1. PlanetScope
planet_cov <- get_coverage_timeseries_fast(
  raster_dir = "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_daily/02/MH/",
  pattern = "_PLANET_BOA\\.tif$",
  aoi = mals,
  platform_name = "PlanetScope",
  type = "actual"
)

planet_cov <- planet_cov %>%
  group_by(Date, platform, type) %>%
  summarise(
    coverage = pmin(round(sum(coverage, na.rm = TRUE)), 100),
    .groups = "drop"
  ) 

### --- 2. Sentinel
s2_cov <- get_coverage_timeseries_fast(
  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3_sites/indices/SEN2/MH/data",
  "_SEN2.*_site\\.tif$",
  mals,
  "Sentinel-2",
  type = "actual"
)

s2_cov <- s2_cov %>%
  group_by(Date, platform, type) %>%
  summarise(
    coverage = pmin(round(sum(coverage, na.rm = TRUE)), 100),
    .groups = "drop"
  ) 

coverage_all <- bind_rows(
  planet_cov,   
  planet_cov_total,  
  s2_cov,
  s2_cov_total
)
  
yearly <- coverage_all %>%
  mutate(Year = format(Date, "%Y")) %>%
  group_by(platform, Year, type) %>%
  summarise(total_coverage = sum(coverage, na.rm = TRUE),
            .groups = "drop") %>%
  pivot_wider(names_from = type, values_from = total_coverage)

#################################################################################
#### ------ 3. Plots ------ #####
#################################################################################
coverage_all2 <- coverage_all %>%
  mutate(
    Year = format(Date, "%Y"),
    Month = as.integer(format(Date, "%m")),
    # 👇 key trick: force all dates into same year
    Date_fixed = as.Date(paste0("2000-", format(Date, "%m-%d")))
  ) %>%
  filter(Month >= 3, Month <= 11) %>%
  filter(Year %in% c("2025"))

start_date <- as.Date("2000-03-01")
end_date   <- as.Date("2000-11-30")

coverage_all2_colored <- coverage_all2 %>%
  mutate(
    fill_type = case_when(
      type == "potential" ~ "Potential",
      type == "actual" & platform == "Sentinel-2" ~ "Sentinel-2 Actual",
      type == "actual" & platform == "PlanetScope" ~ "PlanetScope Actual"
    )
  )

# Then plot:
ggplot(coverage_all2, aes(x = Date_fixed, y = coverage)) +
  
  geom_col(
    data = ~ dplyr::filter(.x, type == "potential"),
    aes(fill = "Potential"),
    width = 0.85,     # Consistent width
    linewidth = 0.8,  # Thinner, consistent
    alpha = 0.7,
    position = position_identity()  # 👈 Overlay (no stacking)
  ) +
  
  geom_col(
    data = ~ dplyr::filter(.x, type == "actual"),
    aes(fill = "Actual"),
    width = 0.85,     # Match width
    linewidth = 0.8,  # Fixed typo (was 08), consistent
    position = position_identity()  
  ) +
  
  facet_grid(platform ~ Year) +
  
  scale_x_date(
    limits = c(start_date, end_date),
    breaks = seq(start_date, end_date, by = "1 month"),
    date_labels = "%b",
    expand = expansion(mult = c(0, 0.02))
  ) +
  
  scale_y_continuous(
    expand = expansion(mult = c(0, 0.1)),
    labels = scales::label_percent(scale = 1)
  ) +
  
  scale_fill_manual(
    values = c(
      "Actual" = "#3DBD28",    # Darker sea green (your preference)
      "Potential" = "#D48A2C"  # Dark gray
    ),
    name = "Coverage"
  ) +
  
  labs(
    title = "Seasonal Coverage by Platform", 
    subtitle = "Actual vs. potential (Mar-Nov), 2024-2025",
    x = "Month",
    y = "Coverage (%)"
  ) +
  
  theme_classic(base_size = 12) +
  theme(
    strip.text = element_text(face = "bold", size = 11, hjust = 0.5),
    strip.background = element_rect(fill = "grey95", color = NA),
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_blank(),
    panel.grid.major.y = element_line(color = "grey90", linewidth = 0.3),
    axis.text = element_text(color = "black", size = 10),
    axis.text.x = element_text(hjust = 1),
    axis.text.y = element_text(vjust = 0.1, hjust = 1),
    axis.ticks = element_line(color = "black", linewidth = 0.3),
    axis.line = element_line(color = "black", linewidth = 0.3),
    legend.position = "top",
    legend.title = element_text(face = "bold", size = 11),
    legend.key.size = unit(0.8, "cm"),
    plot.title = element_text(face = "bold", size = 14, hjust = 0.5, margin = margin(b = 10)),
    plot.subtitle = element_text(size = 11, hjust = 0.5, margin = margin(b = 15)),
    panel.spacing = unit(0.8, "lines"),
    plot.margin = margin(15, 15, 15, 15)
  )

#################################################################################
#### -- make 2 seperate plots
#### -- 1. potential

# Potential only
p_potential <- ggplot(filter(coverage_all2, type == "potential" & platform %in% c("Sentinel-2", "PlanetScope")), 
                      aes(x = Date_fixed, y = coverage)) +
  geom_col(
    width = 0.85,
    linewidth = 0.5,
    fill = "#2E8B57",  # Dark gray
    alpha = 0.8
  ) +
  facet_grid(platform ~ Year) +
  scale_x_date(
    limits = c(start_date, end_date),
    breaks = seq(start_date, end_date, by = "1 month"),
    date_labels = "%b",
    expand = expansion(mult = c(0, 0.02))
  ) +
  scale_y_continuous(
    expand = expansion(mult = c(0, 0.1)),
    labels = scales::label_percent(scale = 1)
  ) +
  labs(
    title = "Potential Coverage: Sentinel & Planet",
    subtitle = "Mar-Nov",
    x = "Month", y = element_blank()
  ) +
  theme_classic(base_size = 12) +
  theme(
    strip.text = element_text(face = "bold", size = 11, hjust = 0.5),
    strip.background = element_rect(fill = "grey95", color = NA),
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_blank(),
    panel.grid.major.y = element_line(color = "grey90", linewidth = 0.3),
    axis.text = element_text(color = "black", size = 10),
    axis.text.x = element_text(angle = 45, hjust = 1),
    axis.ticks = element_line(color = "black", linewidth = 0.3),
    axis.line = element_line(color = "black", linewidth = 0.3),
    legend.position = "none",
    plot.title = element_text(face = "bold", size = 14, hjust = 0.5, margin = margin(b = 10)),
    plot.subtitle = element_text(size = 11, hjust = 0.5, margin = margin(b = 15)),
    panel.spacing = unit(0.8, "lines"),
    plot.margin = margin(15, 15, 15, 15)
  )

p_potential


## -- 2. actual

# Actual only  
# Actual only  
p_actual <- ggplot(
  filter(coverage_all2, type == "actual" & platform %in% c("Sentinel-2", "PlanetScope")), 
  aes(x = Date_fixed, y = coverage, fill = platform)  # 👈 Add fill = platform here
) +
  geom_col(
    width = 1,
    linewidth = 0.8,
    alpha = 0.9
  ) +
  facet_grid(platform ~ Year) +
  scale_x_date(
    limits = c(start_date, end_date),
    breaks = seq(start_date, end_date, by = "1 month"),
    date_labels = "%b",
    expand = expansion(mult = c(0, 0.02))
  ) +
  scale_y_continuous(
    expand = expansion(mult = c(0, 0.1)),
    labels = scales::label_percent(scale = 1)
  ) +
  scale_fill_manual(  # 👈 Match your platform_levels order
    values = c(
      "PlanetScope" = "#1E5F3A",    # Dark green (top row)
      "Sentinel-2" = "#6BAE6B"      # Medium green (your requested darkness)
    ),
    name = "Platform"
  ) +
  labs(
    title = "Actual Coverage: Sentinel & Planet", 
    subtitle = "Mar-Nov, 2024-2025",
    x = "Month", 
    y = element_blank()
  ) +
  theme_classic(base_size = 12) +
  theme(
    strip.text = element_text(face = "bold", size = 11, hjust = 0.5),
    strip.background = element_rect(fill = "grey95", color = NA),
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_blank(),
    panel.grid.major.y = element_line(color = "grey90", linewidth = 0.3),
    axis.text = element_text(color = "black", size = 10),
    axis.text.x = element_text(hjust = 1),
    axis.ticks = element_line(color = "black", linewidth = 0.3),
    axis.line = element_line(color = "black", linewidth = 0.3),
    legend.position = "none",  # Already no legend needed (platform in facets)
    plot.title = element_text(face = "bold", size = 14, hjust = 0.5, margin = margin(b = 10)),
    plot.subtitle = element_text(size = 11, hjust = 0.5, margin = margin(b = 15)),
    panel.spacing = unit(0.8, "lines"),
    plot.margin = margin(15, 15, 15, 15)
  )

p_actual

################################################################################

#### data avaiability

# Function for data availability
get_daily_availability <- function(
    path,
    pattern,
    platform_name
) {
  
  files <- list.files(path, pattern = pattern, full.names = TRUE)
  
  if (length(files) == 0) {
    return(data.frame())
  }
  
  dates <- as.Date(substr(basename(files), 1, 8), "%Y%m%d")
  
  data.frame(Date = dates) %>%
    count(Date, name = "n_images") %>%
    complete(Date = seq(min(Date), max(Date), by = "day"),
             fill = list(n_images = 0)) %>%
    mutate(platform = platform_name)
}

# apply to sentinel and planet
s2_avail <- get_daily_availability(
  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/SEN2/",
  "SEN2.*_BOA\\.tif$",
  "Sentinel-2"
)

pla_avail <- get_daily_availability(
  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard",
  "*_PLANET_.*_BOA\\.tif$",
  "PlanetScope"
)
# combine df
combined_availability <- bind_rows(s2_avail, pla_avail)

# PLot Data availability
daily_counts <- combined_availability %>%
  mutate(
    Date = as.Date(Date),
    Year = format(Date, "%Y")
  ) %>%
  group_by(platform, Date, Year) %>%
  summarise(n_images = sum(n_images), .groups = "drop")

platform_levels <- c(
  "Sentinel-2",
  #"PlanetScope",
  "PlanetScope"
)

daily_counts$platform <- factor(
  daily_counts$platform,
  levels = platform_levels
)

daily_counts_2 <- daily_counts %>%
  mutate(
    Year = format(Date, "%Y"),
    Month = as.integer(format(Date, "%m")),
    # 👇 key trick: force all dates into same year
    Date_fixed = as.Date(paste0("2000-", format(Date, "%m-%d")))
  ) %>%
  filter(Month >= 3, Month <= 11) %>%
  filter(Year %in% c("2024", "2025"))

start_date <- as.Date("2000-03-01")
end_date   <- as.Date("2000-11-30")

# Actual only  
ggplot(filter(daily_counts_2, platform %in% c("Sentinel-2", "PlanetScope")), 
       aes(x = Date_fixed, y = n_images, fill = platform)) +
  geom_col(
    aes(fill = platform),
    width = 0.85,
    linewidth = 0.4,
    alpha = 0.8,
    position = position_identity()  # Overlay instead of stack
  ) +
  facet_wrap(~ Year, ncol = 1, scales = "free_y") +  # Added scales = "free_y"
  scale_x_date(
    limits = c(start_date, end_date),
    breaks = seq(start_date, end_date, by = "1 month"),
    date_labels = "%b",
    expand = expansion(mult = c(0, 0.02))
  ) +
  scale_y_continuous(
    expand = expansion(mult = c(0, 0.1)),
    labels = scales::comma  # Format large numbers
  ) +
  scale_fill_manual(
    values = c(
      "PlanetScope" = "#8FBC8F",    # Light green
      "Sentinel-2" = "#1E5F3A"      # Dark green
    ),
    name = "Platform"
  ) +
  labs(
    title = "Data Availability: Sentinel-2 & PlanetScope",
    subtitle = "Daily images, March–November",
    x = "Month",
    y = "Images per day"
  ) +
  theme_classic(base_size = 12) +
  theme(
    strip.text = element_text(face = "bold", size = 11, hjust = 0.5),
    strip.background = element_rect(fill = "grey95", color = NA),
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_blank(),
    panel.grid.major.y = element_line(color = "grey90", linewidth = 0.3),
    axis.text = element_text(color = "black", size = 10),
    axis.text.x = element_text(hjust = 1),
    axis.ticks = element_line(color = "black", linewidth = 0.3),
    axis.line = element_line(color = "black", linewidth = 0.3),
    legend.position = "top",  # Show platform legend
    legend.title = element_text(face = "bold", size = 11),
    plot.title = element_text(face = "bold", size = 14, hjust = 0.5),
    plot.subtitle = element_text(size = 11, hjust = 0.5),
    panel.spacing = unit(1, "lines")
  )
