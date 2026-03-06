############################ Automated download for PlanetScope data ################################

## Script to check for missing scenes of Planet data and to download them accordingly.
## It first gets all ids from the existing stored images and than checks via API Quick Search which 
## scenes would be still available. 
## The missing scene ids are than used to get the asset information for each of them and only the 
## scence where the surface reflectance product is available are then chosen for the download.

############################# General set up ########################################################
#install.packages("textshaping")
#install.packages("svglite")
#install.packages("leafpop")
#install.packages("s2")

## check if required packages are installed and download or load packes
packages_installation <- function(pkg) {
  if(!requireNamespace(pkg, quietly = T)) {
    install.packages(pkg, dependencies = T)
  }
  library(pkg, character.only = T)
}

packages <- c("geojsonsf", "httr", "sf","jsonlite", "stringr")
lapply(packages, packages_installation)

api_key <- "PLAK523d2874893f4fda96b9f6e66c32edae"

###################################################################################
## Get all the item ids from the data in the folders
files_list <- list.files("/mnt/CEPH_PROJECTS/Environtwin/FORCE/level2_raw/X-001_Y-001",
                         pattern = "_PLANET_BOA\\.tif$", full.names = TRUE)

# Loop over files and get band names
band_names_list <- lapply(files_list, function(f) {
  r <- rast(f)
  list(file = f, bands = names(r))
})

# Get the base filenames
raster_names <- basename(files_list)

# Extract first 18 characters from filenames
raster_sub <- sub("_3B.*", "", raster_names)
raster_sub_sub <- data.frame(
  id = raster_sub,
  acquired = as.Date(substr(raster_sub, 1, 8), format = "%Y%m%d")
) %>%
  mutate(
    year = as.integer(format(acquired, "%Y")),
    month = as.integer(format(acquired, "%m"))
  ) %>%   
  filter(year < 2025) %>%        # only before 2025
  filter(month >= 3 & month <= 11)  # only Mar–Nov

###################################################################################
## Now get all ids that you can get from quick search for the period 2017-2025
# Setup Planet Data API base URL
url <- "https://api.planet.com/data/v1/quick-search/"

##### 1. Select your area of interest from shapefile and transfer to json format #####
# Load shapefile and select aoi
aois <- st_read("gis/test_sites_4326.shp") 

# select id 
#mapview(aois)    # click on polygon of interest and get group_id
selected_aoi <- aois[aois$group_id == 10,]

# transfer polygon information into json file format
polygon_coords <- unclass(selected_aoi$geometry[[1]])[[1]]
coords <- unclass(polygon_coords)

##### 2. Set up a cloud percentage, date range, the item and asset that should be used in the request #####
cloud_cover <- 0.7            # cloud filter equal or less than 70%
item_name <- "PSScene"        # 8 band imagery, item to use for the request

# Set date range that should be covered
date_start <- as.Date("2017-03-01")
date_end <- as.Date("2024-09-10") 

##### 3. Create your filters #####
geom_filter <- list(
  type = "GeometryFilter",
  field_name = "geometry",
  config = list(
    type = "Polygon",
    coordinates = list(coords)
  )
)

cloud_filter <- list(
  type = "RangeFilter",
  field_name = "cloud_cover",
  config = list(lte = cloud_cover)
)

date_filter <- list(
  type = "DateRangeFilter",
  field_name = "acquired",
  config = list(
    gte = format(date_start, "%Y-%m-%dT00:00:00.000Z"),
    lte = format(date_end, "%Y-%m-%dT00:00:00.000Z")
  )
)

# Combine all filters
filter_all <- list(
  type = "AndFilter",
  config = list(geom_filter, cloud_filter, date_filter)
)

##### 4. Build and send the search request #####
search_request <- list(
  item_types = list(item_name),
  filter = filter_all
)

# only for visualization and debugging!!
# transfrom the search request into a nicer json format and print request
#pretty_body <- toJSON(search_request, auto_unbox = TRUE, pretty = TRUE)
#cat("Body:\n", pretty_body, "\n\n")

# to get all features, apply pagination because by default only the first 250 elements are displayed
all_features <- list()

# send search request
res <- POST(
  url = url,
  authenticate(api_key, ""),
  body = jsonlite::toJSON(search_request, auto_unbox = TRUE),
  content_type_json()
)

# Parse the response of the first page
parsed_res <- content(res, "parsed", simplifyVector = FALSE)

# Save as GeoJSON
#write_json(parsed_res, path = "request_output.geojson", pretty = TRUE, auto_unbox = TRUE)

# Collect first page of features
all_features <- append(all_features, parsed_res$features)

# Pagination, get the next results
next_url <- parsed_res$`_links`$`_next`

while (!is.null(next_url)) {
  cat("Fetching next page...\n")
  res <- GET(
    url = next_url,
    authenticate(api_key, "")
  )
  parsed_res <- content(res, "parsed", simplifyVector = FALSE)
  all_features <- append(all_features, parsed_res$features)
  next_url <- parsed_res$`_links`$`_next`
}

full_collection <- list(
  type = "FeatureCollection",
  features = all_features
)

# Extract all IDs -> needed for ordering and downloading the data
asset_ids <- sapply(all_features, function(feature) feature$id)

# subset the asset ids names to only march until november
asset_ids <- data.frame(
  id = asset_ids,
  acquired = as.Date(substr(asset_ids, 1, 8), format = "%Y%m%d")
) %>%
  mutate(month = as.integer(format(acquired, "%m"))) %>%   # extract month
  filter(month >= 3 & month <= 11) %>%                     # keep Mar–Nov
  dplyr::select(-month)  

# Extract first 18 characters from raster_sub_sub and asset_ids
raster_ids_prefix <- substr(raster_sub_sub$id, 1, 18)
asset_ids_prefix <- substr(asset_ids$id, 1, 18)

# Keep only asset_ids that are NOT already in raster_sub_sub
asset_ids_filtered <- asset_ids %>%
  filter(!asset_ids_prefix %in% raster_ids_prefix)

# Function to get available product bundles for one item_id
get_item_bundles <- function(item_id, item_type, api_key) {
  url <- paste0("https://api.planet.com/data/v1/item-types/", item_type, "/items/", item_id)
  
  res <- GET(url, 
             add_headers(Authorization = paste("api-key", api_key))
  )
  if (status_code(res) != 200) {
    warning(paste("Failed for item_id:", item_id, "status:", status_code(res)))
    return(data.frame(item_id = item_id, bundle = NA, stringsAsFactors = FALSE))
  }
  
  data <- content(res, as = "parsed", type = "application/json")
  bundles <- data$assets
  
  if (length(bundles) == 0) {
    return(data.frame(item_id = item_id, bundle = NA, stringsAsFactors = FALSE))
  }
  
  return(data.frame(item_id = item_id, bundle = bundles, stringsAsFactors = FALSE))
}

# Loop over unmatched_ids to get the ids that haven't been passed to the ordering process
item_type <- "PSScene"  # change if needed
bundles_by_id <- lapply(asset_ids_filtered$id, function(id) get_item_bundles(id, item_type, api_key))

# transform into df
df <- bundles_by_id %>%
  bind_rows() %>%               # combine all small data.frames into one big df
  pivot_longer(
    cols = starts_with("bundle"),   # all product columns
    names_to = "bundle_type", 
    values_to = "product"
  ) %>%
  dplyr::filter(!is.na(product)) %>%   # remove missing products
  dplyr::select(item_id, product)

# IDs that contain either ortho_analytic_4b_sr or ortho_analytic_8b_sr
df_selected_ids <- df %>% 
  filter(product %in% c("ortho_analytic_4b_sr", "ortho_analytic_8b_sr"))

# Unique IDs in full dataset with all products
df_unique <- unique(df$item_id)

# Unique IDs that have one of the two products
df_selected_ids_unique <- unique(df_selected_ids$item_id)

# IDs that do NOT have those products --> only to check which products are available 
diff_df <- setdiff(df_unique, df_selected_ids_unique)

# Get full product info for those IDs
missing_ids <- df %>%  
  filter(item_id %in% diff_df) 

names_diff <- raster_sub_sub %in% df_selected_ids_unique

#####################################################################################################
############################# Order data with Planet's order API ####################################
# Set Order API URL
planet_order_url <- "https://api.planet.com/compute/ops/orders/v2"

# First test authentication for the order_url with your API key
response <- GET(
  planet_order_url,
  add_headers(Authorization = paste("api-key", api_key))
)

# Check status
status_code(response)   # 200 means, you're ready to go

# Convert your coordinate matrix to a list of coordinate pairs to parse into the GeoJSON
coords_list <- unname(split(coords, row(coords)))

# Convert to GeoJSON-style: list of list of coordinates
geojson_coords <- list(lapply(coords_list, function(x) as.numeric(x)))

# define clip tool
clip_tool <- list(
  clip = list(
    aoi = list(
      type = "Polygon",
      coordinates = geojson_coords
    )
  )
)

# define harmonization tool
harmonize_tool <- list(
  harmonize = list(
    target_sensor = "Sentinel-2"
  )
)

### Due to a huge number of images, split the order into batches. Otherwise you run 
### into request error problems with exceeding rate limits
# Extract scene ids and acquisition dates
scene_info <- data.frame(
  id = df_selected_ids_unique,
  acquired = as.Date(substr(df_selected_ids_unique, 1, 8), format = "%Y%m%d")
)

# Split into batches
batch_size <- 50
asset_batches <- split(scene_info, ceiling(seq_along(scene_info$id) / batch_size))
#asset_batches <- asset_batches[c(2, 4, 7:10)]

##### 1. Write helper functions to request and activate data #####

# Write the order request function with multiple product bundles and fallback
submit_order <- function(batch, batch_num) {
  
  products <- lapply(seq_len(nrow(batch)), function(i) {
    scene_id <- batch$id[i]
    acquired <- batch$acquired[i]
    
    # Decide bundle: 8-band for newer, 4-band for older
    bundle <- if (acquired >= as.Date("2022-04-01")) {
      "analytic_8b_sr_udm2"
    } else {
      "analytic_sr_udm2"
    }
    
    list(
      item_ids = list(scene_id),
      item_type = "PSScene",
      product_bundle = bundle
    )
  })
  
  order_request <- list(
    name = paste0("Malser Heide Harmonization new missing szenes ", batch_num), 
    products = products,
    tools = list(clip_tool, harmonize_tool),
    delivery = list(
      archive_type = "zip",
      archive_filename = paste0("batch_", batch_num),
      single_archive = TRUE
    ),
    partial  = TRUE
  )
  
  order_res <- POST(
    url = planet_order_url, 
    body = jsonlite::toJSON(order_request, auto_unbox = TRUE),
    add_headers(
      Authorization = paste("api-key", api_key),
      `Content-Type` = "application/json"
    )
  )
  
  content(order_res, "parsed", simplify2Vector = TRUE)
}

##### 2. Run functions to order data #####
order_urls <- list()
order_urls_fail <- list()

for (i in seq_along(asset_batches)) {
  cat("Submitting batch", i, "...\n")
  res <- submit_order(asset_batches[[i]], i)
  print(res)  
  
  if (!is.null(res$`_links`$`_self`)) {
    order_urls <- append(order_urls, list(res$`_links`$`_self`))
    cat("Order URL:", res$`_links`$`_self`, "\n")
  } else {
    cat("Failed to submit batch", i, "\n")
    order_urls_fail <- append(order_urls_fail, list(res))
  }
  
  Sys.sleep(5)  # Delay to avoid rate limits
}

#saveRDS(order_urls, "planet_order_urls.rds")

##### 3. Download data #####
out_dir <- "PLANET/MalserHeide/"

if (!dir.exists(out_dir)) {
  dir.create(out_dir)
}

# Create a directory for each year of data downlaod
out_year <- file.path(paste0(out_dir,"Missing"))

if(!dir.exists(out_year)){
  dir.create(out_year, recursive = T, showWarnings = F)
}

for (i in seq_along(order_urls)){
  url_download <- order_urls[[i]]
  
  dest_path <- file.path(out_year, paste0("batch_", i, ".zip"))
  
  cat("Downloading URL:", url_download, "\n")
  
  response <- GET(url_download, authenticate(api_key, "")) 
  
  if (status_code(response) == 200) {
    json_data <- content(response, as = "text", encoding = "UTF-8")
    parsed_json <- fromJSON(json_data, flatten = TRUE)
    
    # Extract full download url
    download_url <- parsed_json$`_links`$results$location
    
    # Download zip file into different batch folders
    download.file(download_url[[1]], destfile = dest_path, mode = "wb")
    
    cat("Download complete for batch", i, "\n")
  } else {
    cat("Download failed for batch", i, "with status", status_code(response), "\n")
  }
}

############################################################################
## If you need to get the links again for the download, you can do it here:

## get all links
get_all_orders <- function(api_key) {
  base_url <- "https://api.planet.com/compute/ops/orders/v2"
  session <- httr::handle(base_url)  # persistent connection
  
  # Initial request
  res <- GET(base_url, authenticate(api_key, ""), handle = session)
  
  if (status_code(res) != 200) {
    stop("Failed to fetch data: ", status_code(res))
  }
  
  orders_list <- content(res, as = "parsed", type = "application/json")
  all_orders <- orders_list[["orders"]]
  
  # Follow pagination links while "next" exists in _links
  while (!is.null(orders_list[["_links"]]) && !is.null(orders_list[["_links"]][["next"]])) {
    next_url <- orders_list[["_links"]][["next"]]
    
    # If next_url is just a path, prepend base URL
    if (!grepl("^http", next_url)) {
      next_url <- paste0(base_url, next_url)
    }
    
    # Wait 1 second to avoid hammering the server
    Sys.sleep(1)
    
    res <- GET(next_url, authenticate(api_key, ""), handle = session)
    
    if (status_code(res) != 200) {
      stop("Failed to fetch next page: ", status_code(res))
    }
    
    orders_list <- content(res, as = "parsed", type = "application/json")
    
    # Append new orders to all_orders
    all_orders <- c(all_orders, orders_list[["orders"]])
  }
  
  return(all_orders)
}

all_orders <- get_all_orders(api_key) 
## Check which orders you need and subset the all_orders list 
all_orders_sub <- all_orders[c(1:31)]      #change accordingly

# extract the links of the orders to order urls and get back up to download the data
order_urls <- sapply(all_orders_sub, function(order) order[["_links"]][["_self"]])
