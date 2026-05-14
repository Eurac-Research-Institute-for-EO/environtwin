################################################################################

##### --- Create statistics for Planet images ---- #######
################################################################################

# Load required libraries
library(jsonlite)
library(dplyr)
library(stringr)
library(ggplot2)
library(tidyr)

# Test Folder containing JSON files
#folder_path <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/test"

# Standard Folder containing JSON files
folder_path_std <- "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/standard"

# List all JSON files
#json_files_test <- list.files(folder_path, pattern = "\\.json$", full.names = TRUE)
json_files_std <- list.files(folder_path_std, pattern = "\\.json$", full.names = TRUE)

# Initialize empty list to store data
all_data <- list()

# Function to load json files and get informations
process_info <- function(json_path) {
  print(json_path)
  
  # Load JSON file
  data <- fromJSON(json_path)
  
  # Extract fields
  image_id <- data$id
  acquisition_date <- data$properties$acquired
  publishing_status <- data$properties$publishing_stage
  quality_status <- data$properties$quality_category
  gc_present <- data$properties$ground_control
  haze_light <- data$properties$light_haze_percent
  haze_heavy <- data$properties$heavy_haze_percent
  
  year <- substr(acquisition_date, 1, 4)
  
  all_data[[length(all_data) + 1]] <- data.frame(
    path = json_path,
    id = image_id,
    year = year,
    publishing_stage = publishing_status,
    quality_status = quality_status,
    gc_present = gc_present,
    haze_light = haze_light,
    haze_heavy = haze_heavy,
    stringsAsFactors = FALSE
  )
}

# apply function to all json files in list 
#json_test <- lapply(json_files_test, process_info)
json_std <- lapply(json_files_std, process_info)

# create on df with all information and save as csv
df_info <- bind_rows(json_std)

write.csv(json_std, "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/Planet_haze_info.csv", row.names = FALSE)

################################################################################

#### ---- Create statistics for data ---- #####

# Initialize empty list to store data
all_data <- list()

# create function to run on both test and std data 
process_stats <- function(json_path){
  print(json_path)
  
  # Load JSON file
  data <- fromJSON(json_path)
  
  # Extract fields
  image_id <- data$id
  acquisition_date <- data$properties$acquired
  publishing_status <- data$properties$publishing_stage
  cloud_cover <- data$properties$cloud_cover * 100
  gc_present <- data$properties$ground_control
  #quality_status <- data$properties$quality_category
  haze_light <- data$properties$light_haze_percent
  haze_heavy <- data$properties$heavy_haze_percent
  
  year <- substr(acquisition_date, 1, 4)
  
  all_data[[length(all_data) + 1]] <- data.frame(
    id = image_id,
    year = year,
    publishing_stage = publishing_status,
    cloud_cover = cloud_cover,
    gc_present = gc_present,
    #quality_status = quality_status,
    haze_light = haze_light,
    haze_heavy = haze_heavy,
    stringsAsFactors = FALSE
  )
}

# apply to json files
#json_test <- lapply(json_files_test, process_stats)
json_std <- lapply(json_files_std, process_stats)

# Combine all into a single data frame
df_stats <- bind_rows(json_std)

# -------------------------------------------------
# 1) YEARLY STATISTICS 
# -------------------------------------------------
stats <- df_stats %>%
  group_by(year) %>%
  summarise(
    total_images = n(),
    no_ground_control_points = sum(gc_present == FALSE),
    high_haze = sum(haze_light > 45 | haze_heavy > 45),
    .groups = "drop"
  ) %>%
  arrange(year)

print(stats)
write.csv(stats, "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/yearly_statistics_haze.csv", row.names = FALSE)

# -------------------------------------------------
# 2) IDS PER YEAR — NO GROUND CONTROL
# -------------------------------------------------
ids_no_gc <- df_stats %>%
  filter(gc_present == FALSE) %>%
  group_by(year) %>%
  summarise(
    ids_no_gc = list(id),
    .groups = "drop"
  )

# -------------------------------------------------
# 3) IDS PER YEAR — LOW CLEAR PERCENTAGE
# -------------------------------------------------
ids_low_clear <- df_stats %>%
  filter(clear_percentage > 50)

# -------------------------------------------------
# 4) LONG FORMAT (ONE ID PER ROW)
# -------------------------------------------------
qc_ids_long <- df_stats %>%
  mutate(
    no_gc = gc_present == FALSE,
    low_clear = clear_percentage < 50
  ) %>%
  filter(no_gc | low_clear) %>%
  select(year, publishing_stage, id, no_gc, low_clear)

print(qc_ids_long)

# -------------------------------------------------
# 5) EXPORT RESULTS
# -------------------------------------------------
write.csv(qc_ids_long, "/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_sites_raw/MH/yearly_statistics.csv", row.names = FALSE)
# write.csv(qc_ids_long, "qc_failed_image_ids.csv", row.names = FALSE)

# -------------------------------------------------
# 6) Plot RESULTS
# -------------------------------------------------
# ---- Global scientific theme ----
scientific_theme <- theme_minimal(base_size = 11) +
  theme(
    plot.title = element_text(size = 13, face = "bold", hjust = 0.5),
    axis.title.y = element_text(size = 12),
    axis.text.x  = element_text(size = 10),
    axis.text.y  = element_text(size = 10),
    axis.title.x = element_blank(),
    panel.grid.minor = element_blank(),
    panel.grid.major.x = element_blank()
  )

ggplot(stats, aes(x = year, y = total_images, fill = quality_status)) +
  geom_col(position = "dodge") +
  labs(
    title = "Total Images per Year",
    y = "Number of Images",
    fill = "Publishing Stage"
  ) +
  scientific_theme

# stats about total images and no ground control points per year
stats_finalized_long <- stats %>%
  select(year, total_images, no_ground_control_points) %>%
  pivot_longer(
    cols = c(total_images, no_ground_control_points),
    names_to = "qc_type",
    values_to = "count"
  )
