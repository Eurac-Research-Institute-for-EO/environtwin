# Load required libraries
library(jsonlite)
library(dplyr)
library(stringr)
library(ggplot2)
library(tidyr)

# Folder containing JSON files
folder_path <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard"

# Thresholds
CLOUD_THRESHOLD <- 50   
CLEAR_THRESHOLD <- 50

# List all JSON files
json_files <- list.files(folder_path, pattern = "\\.json$", full.names = TRUE)

# Initialize empty list to store data
all_data <- list()

# Read and combine JSON files
for (file in json_files) {
  
  # Load JSON file
  data <- fromJSON(file)
  
  # Extract fields
  #image_id <- json$id
  clear_percentage <- data$properties$clear_percent
  gc_present <- data$properties$ground_control
  haze_light <- data$properties$light_haze_percent
  haze_heavy <- data$properties$heavy_haze_percent
  
  if (haze_light > 45 && gc_present == TRUE) {
    
    all_data[[length(all_data) + 1]] <- file
  }
}

#####################################################################################
# then load standard data 
#####################################################################################

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

# apply to planet std
pla_std_avail <- get_daily_availability(
  "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard",
  "*_PLA_.*_BOA\\.tif$",
  "PlanetScope Standard"
)

dates_test <- as.Date(substr(basename(unlist(all_data)), 1, 8), "%Y%m%d")

pla_test_avail <- data.frame(Date = dates_test) %>%
  count(Date, name = "n_images") %>%
  complete(Date = seq(min(Date), max(Date), by = "day"),
           fill = list(n_images = 0)) %>%
  mutate(platform = "PlanetScope Test")

combined_availability <- bind_rows(pla_std_avail, pla_test_avail)

# PLot Data availability
daily_counts <- combined_availability %>%
  mutate(
    Date = as.Date(Date),
    Year = format(Date, "%Y")
  ) %>%
  group_by(platform, Date, Year) %>%
  summarise(n_images = sum(n_images), .groups = "drop")

platform_levels <- c(
  "PlanetScope Test",
  "PlanetScope Standard"
)

daily_counts$platform <- factor(
  daily_counts$platform,
  levels = platform_levels
)

# get available years
years_available <- sort(unique(daily_counts$Year))

# get maximum count of images to produce same y axis limits 
y_avail_max <- max(daily_counts$n_images, na.rm = TRUE)
y_avail_min <- 0

scientific_theme <- theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(size = 14, face = "bold", hjust = 0.5),
    strip.text = element_text(size = 12, face = "bold"),
    axis.title.y = element_text(size = 12),
    axis.title.x = element_blank(),
    axis.text.x  = element_text(size = 10),
    axis.text.y  = element_text(size = 11),
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_blank()
  )

for (i in years_available) {
  df_year <- daily_counts %>% filter(Year == i)
  
  start_date <- as.Date(paste0(i, "-03-01"))
  end_date   <- as.Date(paste0(i, "-11-30"))
  
  p <- ggplot(df_year,
              aes(x = Date, y = n_images, fill = platform)) +
    geom_col(width = 1) +
    labs(
      title = paste("Number of Standard and Test Images in", i),
      y = "Number of Images",
      fill = "Quality Type"
    ) +
    scale_fill_manual(
      values = c(
        "PlanetScope Test" = "#B76C74",
        "PlanetScope Standard" = "#6CB7B0"
      ),
      labels = c(
        "PlanetScope Test" = "Test images with > 50% clear percentage",
        "PlanetScope Standard" = "Standard images"
      )
    ) +
    scale_x_date(
      breaks = seq(start_date, end_date, by = "1 month"),
      date_labels = "%b",
      limits = c(start_date, end_date)
    ) +
    scientific_theme
  
  print(p)  # display the plot
  
  ggsave(
    filename = paste0("Planet_standard_test_availability_", i, ".png"),
    plot = p,
    width = 10,
    height = 8,
    dpi = 300
  )
}


