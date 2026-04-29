########################################################################
## Analyse WSB and Lafis fields in the Mals Heath

########################################################################
library(dplyr)
library(purrr)
library(tidyr)
library(sf)
library(tibble)
library(PNWColors) 
library(terra)
library(ggplot2)

## -------------- 1. SET OPTIONS AND LOAD DATA ------------ #
# Define years you want to process
years <- 2020:2025

# Function to load shapefiles by year for WSB and LAFIS
load_shapefiles <- function(year) {
  list(
    lafis_wbs = st_read(glue::glue("/mnt/CEPH_PROJECTS/Environtwin/gis/lafis/MH/application/lafis_grassland_{year}_application_MH_zones.shp"))
    )
}

# Load all data
all_data <- lapply(years, load_shapefiles)
names(all_data) <- as.character(years)

# Load Mals zones once
mals <- st_read("/mnt/CEPH_PROJECTS/Environtwin/gis/outlines/Mals_zones_32632.shp") %>% 
  dplyr::select(fid, geometry)

names(mals) <- c("Zone", "geometry")

################################################################################
# Calcualte area_ha and rename column of 2020
for (yr in names(all_data)) {
  
  df <- all_data[[yr]]$lafis_wbs
  
  df <- df %>%
    mutate(
      # area (if not yet computed)
      area_ha = as.numeric(st_area(.)) / 10000,
    )
  
  if (yr == "2020") {
    df <- df %>% 
      dplyr::rename(unique_id = uniqu_d)
  }
  
  # write modified data back
  all_data[[yr]]$lafis_wbs <- df
}

################################################################################
# Group farmers into 4 groups: persisters, laties, quitters, triers
# ----- 1. Get the farmers per year 
# Extract unique id with appl == 1 from each year
cuaa_by_year <- map(all_data, ~{
  .x$lafis_wbs %>%
    st_drop_geometry() %>%
    filter(applctn == 1) %>%
    pull(unique_id) %>%
    unique()
})

# Convert to long table: CUAA × year × presence
presence_df <- enframe(cuaa_by_year, name = "year", value = "unique_id") %>%
  unnest(unique_id) %>%
  mutate(present = 1) %>%
  pivot_wider(
    names_from = year,
    values_from = present,
    values_fill = 0
  )

years <- sort(names(presence_df)[names(presence_df) != "unique_id"])
y_last2 <- tail(years, 2)
y_last3 <- tail(years, 3)
y_last4 <- tail(years, 4)
y_last5 <- tail(years, 5)
y_last <- tail(years, 1)
y1 = "2020"
y_prev <- setdiff(years, y_last)
y_first2 <- head(years, 2)

has_2_consecutive <- function(row, years) {
  any(sapply(seq_along(years)[-length(years)], function(i) {
    row[[years[i]]] == 1 && row[[years[i+1]]] == 1
  }))
}

last_one <- function(row) {
  which(row == 1) |> max()
}

quitters_condition <- apply(
  presence_df[years],
  1,
  function(row) {
    row <- as.numeric(row)
    
    r <- rle(row)
    
    # must have exactly one block of 1s
    one_block <- sum(r$values == 1) == 1
    
    # that block must be at least length 2
    long_enough <- any(r$values == 1 & r$lengths >= 2)
    
    # must end with 0 (they stopped)
    ends_with_zero <- tail(row, 1) == 0
    
    one_block && long_enough && ends_with_zero
  }
)

presence_df <- presence_df %>%
  mutate(
    group = case_when(
      
      # 1) Early adopters (2020–2025 all 1)
      if_all(all_of(years), ~ . == 1) ~ 1,
      
      # 2) Policy adopters (start 2021, then always 1)
      .data[[y1]] == 0 & if_all(all_of(y_last5), ~ . == 1) ~ 2,
      
      # 3) Late adopters (start in 2022+ and continue)
      ( 
        ( if_all(all_of(y_last3), ~ . == 1) &
            if_all(all_of(setdiff(years, y_last3)), ~ . == 0) ) | 
          ( if_all(all_of(y_last2), ~ . == 1)  & 
              if_all(all_of(setdiff(years, y_last2)), ~ . == 0) )  | 
          ( if_all(all_of(y_last4), ~ . == 1) & 
              if_all(all_of(setdiff(years, y_last4)), ~ . == 0) ) |
        ( if_all(all_of(y_last), ~ . == 1) &
            if_all(all_of(y_prev), ~ . == 0) )
      )~ 3,
      
      # 4) Quitters (your logic is fine)
      quitters_condition ~ 4,
      
      # 5) Triers (exactly one 1, but NOT in 2025)
      (
        rowSums(across(all_of(years))) == 1 & 
        !.data[[y_last]] == 1
      ) ~ 5,
      
      # 6) Undecided
      TRUE ~ 6
    )
  )

###################################################################################
##### plot the distribution of farmers groups 
################################################################################
# Extract one representative geometry per CUAA_new
################################################################################

# Build a clean table of unique CUAA_new × year 
geometry_df <- map_df(names(all_data), function(yr) {
  all_data[[yr]]$lafis_wbs %>%                   
    dplyr::select(unique_id, Zone, CUAA, geometry) %>%       # keep ONLY the needed columns
    mutate(year = yr)
})

# Join geometry to attribute table
geometry_df <- geometry_df %>%
  st_as_sf() %>%
  group_by(unique_id) %>%
  #slice(1) %>%              # FOR EACH farmer take exactly one polygon
  ungroup()


################################################################################
# Add presence_df info to each year's lafis_wbs
for (yr in names(all_data)) {
  
  df <- all_data[[yr]]$lafis_wbs
  
  # Join presence_df by CUAA
  df <- df %>%
    left_join(presence_df, by = "unique_id")
  
  # Update the list
  all_data[[yr]]$lafis_wbs <- df
}

################################################################################
# plot the farmers groups 
farmers_sf <- geometry_df %>%
  left_join(presence_df, by = "unique_id") %>%
  st_as_sf() %>% 
  mutate(
    group = ifelse(is.na(group), 0, group),
    group = factor(group,
                   levels = c(1, 2, 3, 4, 5, 6, 0),
                   labels = c(
                     "Seit 2020 dabei",
                     "Seit 2021 dabei",
                     "In den letzten 4 Jahren",
                     "Nach 2 Jahren aufgehört",
                     "1 Jahr probiert",
                     "Unentschlossen",
                     "Nicht teilgenommen"
                   ))
  )

write_sf(farmers_sf, "test_wbs_lafis_participation_v3.shp", overwrite = T)

ggplot() +
  geom_sf(data = mals, fill = NA, color = "grey", linewidth = 0.4) +  
  geom_sf(
    data = farmers_sf,
    aes(fill = group),
    color = NA,
    alpha = 0.9
  ) +
  # color scale
  scale_fill_manual(values = c(
    "Seit 2020 dabei"           = "#F39C12",  
    "Seit 2021 dabei" = "#53EAFD", 
    "In den letzten 4 Jahren"     = "#2B7FFF",  
    "Nach 2 Jahren aufgehört"     = "#D32F2F",  
    "1 Jahr probiert"             = "#31C950",  
    "Unentschlossen"              = "#7F22FE",  
    "Nicht teilgenommen"          = "#ECF0F1"   
  ))+
  labs(fill = "Farmer groups") +
  theme_minimal() +
  theme(
    legend.title = element_text(size = 14, face = "bold"),
    legend.text = element_text(size = 12),
    legend.key.size = unit(1.2, "cm"),
    legend.spacing.y = unit(0.5, "cm"),
    plot.title = element_text(size = 18, face = "bold"),
    panel.grid = element_blank()
  )

# stats a# stats a# stats about different groups 
fields_per_group <- farmers_sf %>%
  distinct(unique_id, group) %>%   
  count(group, name = "n_fields") %>%
  arrange(group)

farmers_per_group <- farmers_sf %>%
  distinct(CUAA, group) %>%   
  count(group, name = "n_fields") %>%
  arrange(group)

# fields per farmer per group
fields_long <- farmers_sf %>%
  bind_rows(.id = "year") %>%
  st_drop_geometry() %>%
  group_by(unique_id, year) %>%
  summarise(
    n_fields = n(),
    .groups = "drop"
  ) %>%
  left_join(presence_df %>% select(unique_id, group), by = "unique_id") %>% 
  na.omit() %>% 
  mutate(
    group = factor(group, levels = c(1,2,3,4,5,6))
  )

group_labels <- c(
  "Seit 2020 dabei",
  "Seit 2021 dabei",
  "In den letzten 4 Jahren",
  "Nach 2 Jahren aufgehört",
  "1 Jahr probiert",
  "Unentschlossen",
  "Nicht teilgenommen"
)

plot_fields <- ggplot(fields_long, aes(x = group, y = n_fields, fill = group)) +
  geom_violin(trim = TRUE, scale = "width", color = "black") +
  stat_summary(
    fun = median,
    geom = "point",
    color = "white",
    size = 2
  ) +
  scale_fill_manual(values = c(
    "1" = "#32BA13",
    "2" = "#40633D",
    "3" = "#F20000",
    "4" = "#820505",
    "5" = "#00FBFF",
    "6" = "#2000F2",
    "7" = "#FFE000"
  ),
  labels = group_labels
  ) +
  scale_x_discrete() +  
  labs(
    x = "Gruppe",
    y = "Anzahl Felder",
    fill = "Gruppe"
  ) +
  theme_bw(base_size = 14) +
  theme(
    legend.position = "right",
    legend.title = element_text(size = 14, face = "bold"),
    legend.text = element_text(size = 12),
    legend.key.size = unit(1.2, "cm"),
    legend.spacing.y = unit(0.5, "cm"),
    axis.text = element_text(color = "black", size = 14),
    axis.title = element_text(face = "bold"),
    panel.grid.major = element_line(color = "grey90"),
    panel.grid.minor = element_blank()
  )

plot_fields

################################################################################
#### ------ COmbine all plots into one ---------- ######
################################################################################
# Combine all years into long table
fields_long <- map2_dfr(
  all_data,
  names(all_data),
  ~ .x$lafis_wbs %>%
    st_drop_geometry() %>%
    dplyr::select(unique_id, applctn, area_ha, Zone, CUAA) %>%
    mutate(year = as.integer(.y))
)

# Remove NAs
fields_long <- fields_long %>%
  mutate(
    applctn = coalesce(applctn, 0),
    appl = factor(applctn, levels = c(0, 1), labels = c("Nicht teilgenommen", "Teilgenommen")),
    year = factor(year)
  )

#Define clean colors: muted gray for 0, muted blue for 1
appl_colors <- c("Nicht teilgenommen" = "#c3c5c7", "Teilgenommen" = "#126fcc")

# ---------------- Plot 1: Number of fields per year (stacked by appl) ----------------
fields_summary <- fields_long %>%
  group_by(year, appl, Zone) %>%
  summarise(n_fields = n(), .groups = "drop")

plot1 <- ggplot(fields_summary, aes(x = year, y = n_fields, fill = appl)) +
  geom_col(width = 0.65, color = "grey30", linewidth = 0.2) +
  scale_fill_manual(values = appl_colors) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.05))) +
  labs(
    x = "Jahr",
    y = "Felderanzahl",
    fill = element_blank()
  ) +
  facet_grid(rows = vars(Zone), labeller = label_wrap_gen(10)) +
  theme_classic(base_size = 14) +
  theme(
    axis.text = element_text(color = "black"),
    axis.title = element_text(face = "bold"),
    
    # subtle gridlines
    panel.grid.major.y = element_line(color = "grey85", linewidth = 0.3),
    panel.grid.major.x = element_blank(),
    
    # facet styling
    strip.text = element_text(face = "bold"),
    #strip.background = element_blank(),
    
    # legend
    legend.position = "top",
    legend.title = element_text(face = "bold"),
    
    # spacing
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10))
  )

plot1

# ---------------- Plot 2: Number of farmers per year (only appl == 1) ----------------
farmers_summary <- fields_long %>%
  #filter(appl == "Teilgenommen") %>%
  group_by(year, Zone, appl) %>%
  summarise(n_farmers = n_distinct(CUAA), .groups = "drop")

plot2 <- ggplot(farmers_summary, aes(x = year, y = n_farmers, fill = appl)) +
  geom_col(width = 0.65, color = "grey30", linewidth = 0.2) +
  scale_fill_manual(values = appl_colors) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.05))) +
  labs(
    y = "Anzahl Bauern",
    fill = element_blank()
  ) +
  facet_grid(rows = vars(Zone), labeller = label_wrap_gen(10)) +
  theme_classic(base_size = 14) +
  theme(
    axis.text = element_text(color = "black"),
    axis.title = element_text(face = "bold"),
    
    panel.grid.major.y = element_line(color = "grey85", linewidth = 0.3),
    panel.grid.major.x = element_blank(),
    
    strip.text = element_text(face = "bold"),
    #strip.background = element_blank(),
    
    # legend
    legend.position = "none",
    
    axis.title.x = element_text(margin = margin(t = 10)),
    axis.title.y = element_text(margin = margin(r = 10))
  )

plot2

# ---------------- Plot 3: Violin plot of field sizes ----------------
# Precompute medians
medians_df <- fields_long %>%
  group_by(year, appl, Zone) %>%
  summarise(median_area = median(area_ha), .groups = "drop")

plot3 <- ggplot(fields_long, aes(x = year, y = area_ha, fill = appl)) +
  geom_violin(trim = FALSE, scale = "width", color = "black", position = position_dodge(width = 0.9)) +
  geom_point(data = medians_df, aes(x = year, y = median_area, group = appl),
             position = position_dodge(width = 0.9),
             color = "white", size = 2) +
  scale_fill_manual(values = c("Nicht teilgenommen" = "#c3c5c7", "Teilgenommen" = "#126fcc")) +
  labs(
    x = "Jahr",
    y = "Feldgröße (ha)"
  ) +
  theme_bw(base_size = 14) +
  theme(
    panel.grid.major = element_line(color = "grey90"),
    panel.grid.minor = element_blank(),
    axis.text = element_text(color = "black", size = 14),
    axis.title = element_text(face = "bold", size = 14),
    strip.text = element_text(size = 14, face = "bold"),
    legend.position = "none",
    axis.title.x = element_text(margin = margin(t = 10)),  # space above x-axis title
    axis.title.y = element_text(margin = margin(r = 10))
  ) +
  facet_grid(rows=vars(Zone)) 

plot3

# ---------------- Combine into 3 columns using patchwork ----------------
combined_plot <- plot1 + plot2 + facet_grid(rows=vars(Zone), ) 

combined_plot

plot1 <- plot1 + theme(plot.margin = margin(r = 20))  # 20 pts space on the right
plot2 <- plot2 + theme(plot.margin = margin(l = 20))  # 20 pts space on the left

combined_plot <- plot1 + plot2
combined_plot

##################################################################################
#### check the location of the fields in terms of and slope
dem <- rast("/mnt/CEPH_PROJECTS/GRITA/Data/GIS/DEM/copernicus_slope_percent_ST.tif")

wbs <- farmers_sf %>% 
  filter(group != "Nicht teilgenommen")

dem_vals_poly <- terra::extract(
  dem, 
  wbs, 
  fun = mean, 
  na.rm = TRUE
)

dem_vals_poly <- dem_vals_poly %>% 
  mutate(
    uniqu_d = wbs$unique_id[ID],
    CUAA_new = wbs$CUAA[ID]
  ) %>% 
  mutate(
    slope_class = cut(
      copernicus_slope_percent_utm,
      breaks = c(0, 5, 10, 15, 20, 30, 50, Inf),
      labels = c("0–5%", "5–10%", "10–15%", "15–20%", "20–30%", "30–50%", ">50%")
    )
  )

# analyse the different slope classes and plot it
slope_summary <- dem_vals_poly %>% 
  distinct(uniqu_d, slope_class) %>% 
  count(slope_class, name = "n_fields")

ggplot(slope_summary, aes(x = slope_class, y = n_fields)) +
  geom_col() +
  labs(
    x = "Hangneigung",
    y = "Anzahl Felder",
    title = "Verteilung der Felder in Bezug auf Hangneigung"
  ) +
  theme_minimal()

wbs_slope <- wbs %>% 
  mutate(
    slope = dem_vals_poly$copernicus_slope_percent_utm,
    slope_class = dem_vals_poly$slope_class
  )

ggplot(wbs_slope) +
  geom_sf(data = mals, fill = NA, color = "grey", linewidth = 0.4) +  
  geom_sf(aes(fill = slope_class),
          color = NA,
          alpha = 0.9) +
    scale_fill_manual(
    values = c(
      "0–5%"   = "#1a9850",  # green
      "5–10%"  = "#91cf60",  # light green
      "10–15%" = "#d9ef8b",  # yellow-green
      "15–20%" = "#fee08b",  # yellow
      "20–30%" = "#fc8d59",  # orange
      "30–50%" = "#d73027",  # red
      ">50%"   = "#7f0000"   # dark red
    )
  )+
  labs(fill = "Hangneigung") +
  theme_minimal() +
  theme(
    legend.title = element_text(size = 14, face = "bold"),
    legend.text = element_text(size = 12),
    legend.key.size = unit(1.2, "cm"),
    legend.spacing.y = unit(0.5, "cm"),
    panel.grid = element_blank()
  )

################# calcualte roughness of fields ###############################
#### check the location of the fields in terms of and slope
tri <- rast("/mnt/CEPH_PROJECTS/Environtwin/FORCE/TRI_test.tif")

wbs <- farmers_sf %>% 
  filter(group != "Nicht teilgenommen")

tri_vals_poly <- terra::extract(
  tri, 
  wbs, 
  fun = mean, 
  na.rm = TRUE
)

summary(tri_vals_poly$TRI_test)
quantile(tri_vals_poly$TRI_test, probs = seq(0, 1, 0.1), na.rm = TRUE)
hist(tri_vals_poly$TRI_test, breaks = 50)

qs <- quantile(tri_vals_poly$TRI_test,
               probs = seq(0, 1, 0.2),
               na.rm = TRUE)

tri_vals_poly <- tri_vals_poly %>% 
  mutate(
    uniqu_d = wbs$unique_id[ID],
    CUAA_new = wbs$CUAA[ID]
  ) %>% 
  mutate(
    tri_class = cut(
      TRI_test,
      breaks = c(0, 1, 5, 10, Inf),
      labels = c("0-1", "1-5", "5-10" ,">10")
    )
  )

# analyse the different slope classes and plot it
tri_summary <- tri_vals_poly %>% 
  distinct(uniqu_d, tri_class) %>% 
  count(tri_class, name = "n_fields")

wbs_tri <- wbs %>% 
  mutate(
    tri = tri_vals_poly$TRI_test,
    tri_class = tri_vals_poly$tri_class
  )

ggplot(wbs_tri) +
  geom_sf(data = mals, fill = NA, color = "grey", linewidth = 0.4) +  
  geom_sf(aes(fill = tri_class),
          color = NA,
          alpha = 0.9) +
  scale_fill_manual(
    values = c(
      "0-1"   = "#1a9850",  # green
      "1-5"  = "#91cf60",  # light green
      "5-10" = "#fee08b",  # yellow-green
      ">10" = "#fc8d59"
    )
  )+
  labs(fill = "Ruggedness level (TRI)") +
  theme_minimal() +
  theme(
    legend.title = element_text(size = 14, face = "bold"),
    legend.text = element_text(size = 12),
    legend.key.size = unit(1.2, "cm"),
    legend.spacing.y = unit(0.5, "cm"),
    panel.grid = element_blank()
  )
