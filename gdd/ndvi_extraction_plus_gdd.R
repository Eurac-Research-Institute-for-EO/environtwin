## check if required packages are installed and download or load packes
packages_installation <- function(pkg) {
  if(!requireNamespace(pkg, quietly = T)) {
    install.packages(pkg, dependencies = T)
  }
  library(pkg, character.only = T)
}

packages <- c("ggplot2", "dplyr", "sf","terra", "exactextractr")
lapply(packages, packages_installation)

### load raster image
ndvi_tss <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level3/X0000_Y0004/20240301-20241031_060-304_HL_TSA_PLANET_NDV_TSS.vrt")

# Load polygons as sf object
lafis_test <- st_read("gis/Mals_heath/2024/Wiesenbrueter_2024_lafis.shp")

# load date txt file
dates <- read.table("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level1/dates_2024.txt")
date_names <- as.character(dates[[1]])

ndvi_extract <- exact_extract(ndvi_tss, lafis_test, fun = 'mean', progress = TRUE, max_cells_in_memory = 1e7)
names(ndvi_extract) <- date_names

result_df <- cbind(st_drop_geometry(lafis_test), ndvi_extract) 

# Convert date columns from wide to long format for plotting
long_df <- result_df %>%
  pivot_longer(
    cols = -c(1:12),          # keep these columns as is
    names_to = "Date",
    values_to = "Value"
  )

long_df_grouped <- long_df %>% 
  group_by(FID_1, Date) %>%
  summarise(mean_value = mean(Value, na.rm = TRUE), .groups = 'drop')

# Convert Date column to class Date if not already
long_df_grouped$Date <- as.Date(long_df_grouped$Date, format = "%Y%m%d")

# Example: plot time series for one polygon (filter by polygon ID or attribute)
plot_df <- long_df_grouped %>% 
  filter(FID_1 == "ZRZRLF60T19F132X-2052293")

# Basic time series plot with ggplot2
ggplot(plot_df, aes(x = Date, y = mean_value)) +
  geom_point() +
  labs(title = paste("Time Series for Polygon"),
       x = "Date",
       y = "Mean Raster Value") +
  theme_minimal()

####################################
# add wsb gdd 2024 together with ndvi and plot

# Get min and max NDVI dates
gdd_sub_test <- gdd_sub %>%
  filter(format(Date, "%m") >= "03" & format(Date, "%m") <= "10")

# Calculate scaling factor to map second dataset to first dataset's range
scaleFactor <- max(gdd_sub_test$GDDs, na.rm = TRUE) / max(plot_df$mean_value, na.rm = TRUE)

ggplot() +
  geom_line(data = gdd_sub_test, aes(x = Date, y = GDDs, group = FID_1), color = "red") +
  geom_point(data = plot_df, aes(x = Date, y = mean_value * scaleFactor, group = FID_1), color = "blue") +
  scale_y_continuous(
    name = "GDD",
    limits = c(0, max(gdd_sub_test$GDDs, na.rm=TRUE)),
    sec.axis = sec_axis(~ . / scaleFactor, name = "NDVI")
  ) +
  theme_minimal() +
  theme(
    axis.title.y.left = element_text(color = "red"),
    axis.title.y.right = element_text(color = "blue")
  ) +
  labs(title = "GDD and NDVI Time Series with Dual Y Axes")



