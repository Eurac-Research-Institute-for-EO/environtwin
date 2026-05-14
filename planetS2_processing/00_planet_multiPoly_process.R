############################ Automated download for PlanetScope data - ALL AOIs ################################

## Processes ALL group_ids automatically from test_sites_4326.shp

############################# General set up ########################################################
packages_installation <- function(pkg) {
  if(!requireNamespace(pkg, quietly = T)) {
    install.packages(pkg, dependencies = T)
  }
  library(pkg, character.only = T)
}

packages <- c("geojsonsf", "httr", "sf","jsonlite")
lapply(packages, packages_installation)

# Set Workspace 
setwd("/mnt/CEPH_PROJECTS/Environtwin")
api_key <- ""  # Your API key

#####################################################################################################
## HELPER FUNCTIONS #####################################################
get_failed_messages <- function(x) {
  msgs <- character(0)
  details_list <- x$field$Details
  for (item in details_list) {
    msgs <- c(msgs, as.character(item$message))
  }
  return(msgs)
}

# search for products function
get_all_orders <- function(api_key) {
  base_url <- "https://api.planet.com/compute/ops/orders/v2"
  session <- httr::handle(base_url)
  res <- GET(base_url, authenticate(api_key, ""), handle = session)
  if (status_code(res) != 200) stop("Failed to fetch data: ", status_code(res))
  orders_list <- content(res, as = "parsed", type = "application/json")
  all_orders <- orders_list[["orders"]]
  
  while (!is.null(orders_list[["_links"]]) && !is.null(orders_list[["_links"]][["next"]])) {
    next_url <- orders_list[["_links"]][["next"]]
    if (!grepl("^http", next_url)) next_url <- paste0(base_url, next_url)
    Sys.sleep(1)
    res <- GET(next_url, authenticate(api_key, ""), handle = session)
    if (status_code(res) != 200) stop("Failed to fetch next page: ", status_code(res))
    orders_list <- content(res, as = "parsed", type = "application/json")
    all_orders <- c(all_orders, orders_list[["orders"]])
  }
  return(all_orders)
}

process_single_aoi <- function(group_id, api_key, cloud_cover = 0.7,
                               date_start = as.Date("2017-03-01"),
                               date_end = as.Date("2025-11-30"),
                               batch_size = 100,
                               max_retries_outer = 12,    # outer attempts
                               retry_delay = 300L,        # 5 min
                               polling_interval = 300,    # poller sleep
                               max_wait_hours_internal = 0.1) { # short internal wait

  cat("\n=== PROCESSING AOI group_id =", group_id, "===\n")
  
  # --- 1. SEARCH PRODUCTS ---
  selected_aoi <- aois[aois$group_id == group_id,]
  polygon_coords <- unclass(selected_aoi$geometry[[1]])[[1]]
  coords <- unclass(polygon_coords)
  
  url <- "https://api.planet.com/data/v1/quick-search/"
  
  # Create filter
  geom_filter <- list(type = "GeometryFilter", field_name = "geometry",
                      config = list(type = "Polygon", coordinates = list(coords)))
  cloud_filter <- list(type = "RangeFilter", field_name = "cloud_cover",
                       config = list(lte = cloud_cover))
  date_filter <- list(type = "DateRangeFilter", field_name = "acquired",
                      config = list(gte = format(date_start, "%Y-%m-%dT00:00:00.000Z"),
                                    lte = format(date_end, "%Y-%m-%dT00:00:00.000Z")))
  filter_all <- list(type = "AndFilter", config = list(geom_filter, cloud_filter, date_filter))
  
  # create search request and send it
  search_request <- list(item_types = list("PSScene"), filter = filter_all)
  res <- POST(url, authenticate(api_key, ""), body = toJSON(search_request, auto_unbox = TRUE), content_type_json())
  
  if (status_code(res) != 200) stop("Search failed: ", status_code(res), "\n", content(res, "text"))
  
  parsed_res <- content(res, "parsed", simplifyVector = FALSE)
  all_features <- parsed_res$features
  next_url <- parsed_res$`_links`$`_next`
  
  # get all PSScences available
  while (!is.null(next_url)) {
    cat("Fetching next page...\n")
    res <- GET(next_url, authenticate(api_key, ""))
    if (status_code(res) != 200) stop("Search next page failed: ", status_code(res))
    parsed_res <- content(res, "parsed", simplifyVector = FALSE)
    all_features <- append(all_features, parsed_res$features)
    next_url <- parsed_res$`_links`$`_next`
    Sys.sleep(1)
  }
  if (length(all_features) == 0) {
    cat("No features found for AOI", group_id, "\n"); return(NULL)
  }
  
  scene_info <- data.frame(
    id = sapply(all_features, function(f) f$id),
    acquired = as.Date(sapply(all_features, function(f) f$properties$acquired)),
    stringsAsFactors = FALSE
  )
  
  asset_batches <- split(scene_info, ceiling(seq_along(scene_info$id) / batch_size))
  
  ##############################################################################
  # --- 2. ORDER data ---
  planet_order_url <- "https://api.planet.com/compute/ops/orders/v2"
  
  coords_list <- unname(split(coords, row(coords)))
  geojson_coords <- list(lapply(coords_list, function(x) as.numeric(x)))
  
  clip_tool <- list(clip = list(aoi = list(type = "Polygon", coordinates = geojson_coords)))
  harmonize_tool <- list(harmonize = list(target_sensor = "Sentinel-2"))
  
  # Submit order
  submit_order <- function(batch, batch_num) {
    products <- lapply(seq_len(nrow(batch)), function(i) {
      scene_id <- batch$id[i]; acquired <- batch$acquired[i]
      bundle <- if (acquired >= as.Date("2022-04-01")) "analytic_8b_sr_udm2" else "analytic_sr_udm2"
      list(item_ids = list(scene_id), item_type = "PSScene", product_bundle = bundle)
    })
    order_request <- list(
      name = paste0("BMS ", group_id, "- 2017:2025 ", batch_num),
      products = products,
      tools = list(clip_tool, harmonize_tool),
      delivery = list(archive_type = "zip", archive_filename = paste0("batch_", batch_num), single_archive = TRUE),
      partial = TRUE
    )
    order_res_raw <- POST(url = planet_order_url,
                          body = toJSON(order_request, auto_unbox = TRUE),
                          add_headers(Authorization = paste("api-key", api_key), `Content-Type` = "application/json"))
    sc <- status_code(order_res_raw)
    if (sc == 202) {
      parsed <- content(order_res_raw, "parsed", simplifyVector = FALSE)
      return(parsed)
    } else {
      txt <- tryCatch(content(order_res_raw, "text", encoding = "UTF-8"), error = function(e) "")
      cat("Order submit error (status", sc, "):", substr(txt, 1, 300), "\n")
      parsed_try <- tryCatch(content(order_res_raw, "parsed", simplifyVector = FALSE), error = function(e) NULL)
      return(parsed_try)
    }
  }
  
  # create empty lists for failed and succeeded orders
  order_urls <- list(); order_urls_fail <- list()
  
  # submit in batches
  for (i in seq_along(asset_batches)) {
    cat("Submitting batch", i, "of", length(asset_batches), "...\n")
    res <- tryCatch(submit_order(asset_batches[[i]], i), error = function(e) { cat("submit_order error:", e$message, "\n"); NULL })
    if (!is.null(res) && !is.null(res$`_links`$`_self`)) {
      order_urls <- append(order_urls, list(res$`_links`$`_self`))
      cat("Order URL:", res$`_links`$`_self`, "\n")
    } else {
      cat("Failed to submit batch", i, "\n"); order_urls_fail <- append(order_urls_fail, list(res))
    }
    Sys.sleep(5)
  }
  
  # fallback re-submit removed IDs 
  if (length(order_urls_fail) > 0) {
    msgs <- unlist(lapply(order_urls_fail, function(x) { if (is.null(x)) return(NA_character_); tryCatch(get_failed_messages(x), error = function(e) NA_character_) }))
    msgs <- msgs[!is.na(msgs)]
    if (length(msgs) > 0) {
      failed_ids <- unique(gsub("^.*no access to assets:\\s*", "", msgs, ignore.case = TRUE))
      failed_ids <- trimws(gsub("/\\[.*$", "", gsub("^PSScene/", "", failed_ids)))
      batches_with_failures_idx <- which(sapply(asset_batches, function(df) any(df$id %in% failed_ids)))
      cleaned_batches <- lapply(asset_batches[batches_with_failures_idx], function(df) df[!(df$id %in% failed_ids), ])
      for (i in seq_along(cleaned_batches)) {
        cat("Re-submitting cleaned batch", i, "...\n")
        res <- tryCatch(submit_order(cleaned_batches[[i]], i), error = function(e) { cat("submit_order error:", e$message, "\n"); NULL })
        if (!is.null(res) && !is.null(res$`_links`$`_self`)) {
          order_urls <- append(order_urls, list(res$`_links`$`_self`)); cat("Order URL:", res$`_links`$`_self`, "\n")
        } else { cat("Failed re-submit for cleaned batch", i, "\n"); order_urls_fail <- append(order_urls_fail, list(res)) }
        Sys.sleep(5)
      }
    } else cat("No parsable failure messages to clean; skipping re-submit.\n")
  }
  if (length(order_urls) == 0) { cat("No successful order submissions for AOI", group_id, "\n"); return(NULL) }
  
  # poller for status of order
  get_order_status <- function(order_urls, api_key, group_id, max_wait_hours = 24, polling_interval = polling_interval) {
    start_time <- Sys.time()
    while (Sys.time() - start_time < max_wait_hours * 3600) {
      cat("Checking order status at", format(Sys.time()), "\n")
      ready_count <- 0; failed_count <- 0
      for (i in seq_along(order_urls)) {
        status_res <- GET(order_urls[[i]], authenticate(api_key, ""))
        if (status_code(status_res) == 200) {
          status_data <- content(status_res, "parsed", simplifyVector = FALSE)
          state <- status_data$state
          cat(sprintf("Order %d/%d: %s\n", i, length(order_urls), state))
          if (state %in% c("success", "partial")) ready_count <- ready_count + 1
          else if (state %in% c("failed", "cancelled")) {
            failed_count <- failed_count + 1
            msgs <- tryCatch(get_failed_messages(status_data), error = function(e) NA_character_)
            cat("  WARNING: Order", i, "failed:", paste(msgs, collapse = "; "), "\n")
          } else cat("  Progressing:", state, "\n")
        } else cat("  HTTP", status_code(status_res), "for order", i, "\n")
      }
      if (ready_count == length(order_urls) && failed_count == 0) { cat("ALL ORDERS READY!\n"); return(TRUE) }
      if (failed_count > 0) { cat("SOME ORDERS FAILED - returning 'failed'\n"); return("failed") }
      cat("Still processing... waiting", polling_interval, "seconds\n"); Sys.sleep(polling_interval)
    }
    cat("TIMEOUT after", max_wait_hours, "hours\n"); return("timeout")
  }
  
  # Retry until TRUE
  attempt <- 1; order_status <- NULL
  
  while (attempt <= max_retries_outer) {
    cat(sprintf("Order status outer attempt %d/%d\n", attempt, max_retries_outer))
    order_status <- get_order_status(order_urls, api_key, group_id, max_wait_hours = max_wait_hours_internal, polling_interval = min(20, polling_interval))
    if (identical(order_status, TRUE)) { cat("All orders ready, proceeding to downloads.\n"); break }
    cat("get_order_status returned:", as.character(order_status), "\n")
    if (attempt < max_retries_outer) { cat("Will retry after", retry_delay, "seconds...\n"); Sys.sleep(retry_delay); attempt <- attempt + 1 } else { cat("Max outer retries reached; aborting downloads.\n"); break }
  }
  
  ##############################################################################
  # ---  3. DOWNLOAD only when TRUE
  out_dir <- paste0("/mnt/CEPH_PROJECTS/Environtwin/PLANET/BMS/", group_id)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  
  if (!identical(order_status, TRUE)) { 
    cat("Orders not fully ready (status:", as.character(order_status), "). No downloads attempted.\n"); return(order_urls) }
  
  for (i in seq_along(order_urls)) {
    dest_path <- file.path(out_dir, paste0("batch_0", i, ".zip"))
    
    if (file.exists(dest_path)) { 
      cat("Already exists:", dest_path, "\n"); next 
      }
    
    cat("Downloading batch", i, "\n")
    
    response <- GET(order_urls[[i]], authenticate(api_key, ""))
    
    if (status_code(response) != 200) {
      cat("  Failed to get order status for download: HTTP", status_code(response), "\n"); next 
      }
    
    parsed_json <- tryCatch(fromJSON(content(response, "text", encoding = "UTF-8"), flatten = TRUE), error = function(e) NULL)
    
    if (is.null(parsed_json)) { 
      cat("  Failed to parse order JSON for batch", i, "\n"); next 
      }
    if (!parsed_json$state %in% c("success", "partial")) { 
      cat("  Skipping; order not final:", parsed_json$state, "\n"); next 
      }
    if (!is.null(parsed_json$`_links`$results) && length(parsed_json$`_links`$results) > 0) {
      dl_url <- parsed_json$`_links`$results$location[[1]]
      
      tryCatch({ download.file(dl_url, destfile = dest_path, mode = "wb"); cat("✓ Download complete for batch", i, "\n") },
               error = function(e) { cat("✗ Download failed for batch", i, ":", e$message, "\n") })
    } else cat("⚠ No results link for batch", i, "(", parsed_json$state, ")\n")
    Sys.sleep(1)
  }
  cat("AOI", group_id, "COMPLETE\n"); return(order_urls)
}

#####################################################################################################
## RUN ALL AOIs AUTOMATICALLY ##################################################
aois <- st_read("gis/misc/test_sites_4326.shp") 
aoi_ids <- unique(st_read("gis/misc/test_sites_4326.shp")$group_id)
aoi_ids <- aoi_ids[26:30]

results <- lapply(aoi_ids, function(id) {
  process_single_aoi(id, api_key)
})
