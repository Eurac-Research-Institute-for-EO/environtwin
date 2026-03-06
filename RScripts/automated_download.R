############################ Automated download for PlanetScope data ################################

## This is a script to download various PlanetScope data for a specified area of interest.
## The script loads a polygon shapefile with various test sites in south tirol and plots them in a mapview.
## From that mapview click on a polygon of interest and find out the group_id and subset the shapefile to your aoi.
## Set up a date range, cloud filter and the specific product that you want to request.
## Make a quick search for the available products and the ids.
## The ID's are needed to then order and download the data from the website as a zip file.

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

packages <- c("geojsonsf", "httr", "sf","jsonlite", "mapview")
lapply(packages, packages_installation)

# Set Workspace 
getwd()
setwd("/mnt/CEPH_PROJECTS/Environtwin")

# set API key 
api_key <- "s"


############################# Quick Search with Planet's data API ####################################
# Setup Planet Data API base URL
url <- "https://api.planet.com/data/v1/quick-search/"

##### 1. Select your area of interest from shapefile and transfer to json format #####
# Load shapefile and select aoi
aois <- st_read("gis/misc/test_sites_4326.shp") 

# select id 
mapview(aois)    # click on polygon of interest and get group_id
selected_aoi <- aois[aois$group_id == 10,]
#mapview(selected_aoi)   

# transfer polygon information into json file format
polygon_coords <- unclass(selected_aoi$geometry[[1]])[[1]]
coords <- unclass(polygon_coords)

##### 2. Set up a cloud percentage, date range, the item and asset that should be used in the request #####
cloud_cover <- 0.7            # cloud filter equal or less than 70%
item_name <- "PSScene"        # 8 band imagery, item to use for the request

# Set date range that should be covered
date_start <- as.Date("2025-11-01")
date_end <- as.Date("2025-11-30") 

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

composite_tool <- list(
  composite = list(
    group_by = "order"
  )
)

### Due to a huge number of images, split the order into batches. Otherwise you run 
### into request error problems with exceeding rate limits
# Extract scene ids and acquisition dates
scene_info <- data.frame(
  id = sapply(all_features, function(f) f$id),
  acquired = as.Date(sapply(all_features, function(f) f$properties$acquired))
)

# Split into batches
batch_size <- 100
asset_batches <- split(scene_info, ceiling(seq_along(scene_info$id) / batch_size))

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
    name = paste0("Site 10 - 2025 Composite 2", batch_num), 
    products = products,
    tools = list(clip_tool, harmonize_tool, composite_tool),
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
out_dir <- "PLANET/"

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

  dest_path <- file.path(out_year, paste0("batch_0", i, ".zip"))
  
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

#################################################################################################
## In case you deleted the history or you just want to download data again
## you can check the last orders and then transfer them to order_url again and download the data

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
all_orders_sub <- all_orders[c(1:193)]      #change accordingly

# extract the links of the orders to order urls and get back up to download the data
order_urls <- sapply(all_orders_sub, function(order) order[["_links"]][["_self"]])

#####################################################################################
########################## Failed orders subsetting
# Helper function to extract only field -> details -> message
get_failed_messages <- function(x) {
  msgs <- character(0)
  
  # Only check inside field -> details
  if (!is.null(x$field$details)) {
    details_list <- x$field$details
    
    # Each details entry may contain a list with $message
    for (item in details_list) {
      if (!is.null(item$message)) {
        msgs <- c(msgs, as.character(item$message))
      }
    }
  }
  return(msgs)
}

# 1. extract all messages from order_urls_fail
all_failed_messages <- unlist(lapply(order_urls_fail, get_messages))

# 2. parse asset ids out of messages
# adjust pattern if your messages have a different format
failed_ids <- unique(gsub("^.*no access to assets:\\s*", "", all_failed_messages, ignore.case = TRUE))

# (optional) trim whitespace
failed_ids <- trimws(failed_ids)

# Step 1: remove the product part "/[...]" 
temp_ids <- gsub("/\\[.*$", "", failed_ids)

# Step 2: remove the "PSScene/" prefix
clean_failed_ids <- gsub("^PSScene/", "", temp_ids)

# Find batches that contain failed numeric asset IDs
batches_with_failures_idx <- which(
  sapply(asset_batches, function(df) any(df$id %in% clean_failed_ids))
)

# Subset only those problematic batches
subset_batches <- asset_batches[batches_with_failures_idx]

# Remove failed rows from those batches (cleaned batch)
cleaned_batches <- lapply(subset_batches, function(df) {
  df[!(df$id %in% clean_failed_ids), ]
})

##### 2. Run functions to order data #####
order_urls <- list()
order_urls_fail <- list()

for (i in seq_along(cleaned_batches)) {
  cat("Submitting batch", i, "...\n")
  res <- submit_order(cleaned_batches[[i]], i)
  
  if (!is.null(res$`_links`$`_self`)) {
    order_urls <- append(order_urls, list(res$`_links`$`_self`))
    cat("Order URL:", res$`_links`$`_self`, "\n")
  } else {
    cat("Failed to submit batch", i, "\n")
    order_urls_fail <- append(order_urls_fail, list(res))
  }
  
  Sys.sleep(5)  # Delay to avoid rate limits
}
