## Script to transfer the excel table information of the Wiesenbrüter 2021 & 2022
## to the parcel polygon 

library(sf)
library(dplyr)
library(tidyverse)

# Read  cleaned data
#df <- read_csv("C:/Users/hdierkes/Desktop/GIS/Mals_heath/2022/cleaned_wsb_2022.csv", col_names = T)

df <- read_csv("X:/gis/wbs/WBS_original/2020/parcels_new.csv",
                col_names = T)


#write.csv(df, "C:/Users/hdierkes/Desktop/GIS/Mals_heath/2021/cleaned_wsb_2021.csv", row.names = FALSE)

# Read the parcel shapefile and the mals zones shapefile
mahd <- st_read("C:/Users/hdierkes/Desktop/GIS/parcels/ParcelsAggregate_polygon.shp")
mals <- st_read("C:/Users/hdierkes/Desktop/GIS/Mals_heath/Mals_zones.shp")

# clip parcels to mals 
clip <- st_intersection(mahd, mals)

# Ensure matching column types
clip <- clip %>%
  mutate(
    Region = as.character(PART_CCA00),
    Number = as.character(PART_CODIC),
    Zone = as.character(id),
    Unique = paste0(Region, "_", Number, "_", Zone)
  )

wsb <- df %>%
  mutate(
    Region = as.character(Region),
    Number = as.character(Number),
    Zone = as.character(Zone),
    wsb = 1,
    Unique = paste0(Region, "_", Number, "_", Zone)
  )

######## 2020 ###
clip <- clip %>%
  mutate(
    Region = as.character(PART_CCA00),
    Number = as.character(PART_CODIC),
    Unique = paste0(Region, "_", Number)
  )

wsb <- df %>%
  mutate(
    Region = as.character(Region),
    Number = as.character(Number),
    wsb = 1,
    Unique = paste0(Region, "_", Number)
  )


# half join the two data frames and get only the polygons that match the Region, Number and Zone of the excel table
clip_flagged <- clip %>%
  left_join(wsb %>% select(Unique, wsb), 
            by = c("Unique")) %>% 
  filter(wsb == 1)

# List missing polygons
target_numbers <- setdiff(wsb$Unique, clip_flagged$Unique)
target_numbers <- sub(".*_", "", target_numbers)

# Filter rows where Number is one of those
missing <- clip %>%
  filter(PART_CODIC %in% target_numbers) %>% 
  mutate(wsb = 1)

# align the two data frame columns to add the missing polygons
missing_aligned <- missing[, names(clip_flagged)]

# add the missing data
all_data <- bind_rows(clip_flagged, missing_aligned)

# see again the difference - if there is one
setdiff(df$Number, all_data$Number)

write_sf(clip_flagged, "C:/Users/hdierkes/Desktop/GIS/Mals_heath/2022/Wiesenbrueter_2022_parcels.shp", delete_layer = T)

write_sf(missing, "C:/Users/hdierkes/Desktop/GIS/Mals_heath/2021/Wiesenbrueter_2021_missing_r.shp", delete_layer = T)


################ 2020 ##########
write_sf(clip_flagged, "X:/gis/wbs/WBS_original/2020/Wiesenbrueter_2020_code.shp", delete_layer = T)
write_sf(missing, "X:/gis/wbs/WBS_original/2020/Wiesenbrueter_2020_code_missing.shp", delete_layer = T)
