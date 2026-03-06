### get lastest orders
list_order_urls <- function(api_key, limit = 500) {
  url <- paste0("https://api.planet.com/compute/ops/orders/v2/?limit=", limit)
  
  res <- GET(url, authenticate(api_key, ""))
  content_data <- content(res, as = "parsed", type = "application/json")
  
  orders <- content_data$orders
  
  # Extract order URLs
  order_urls <- sapply(orders, function(x) x$`_links`$`_self`)
  return(order_urls)
}

order_urls <- list_order_urls(api_key)
print(order_urls)

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

all_oorders <- get_all_orders(api_key)
all_orders_sub <- all_oorders[c(1, 5:12)]

order_urls <- sapply(all_orders_sub, function(order) order[["_links"]][["_self"]])




get_all_image_links <- function(order_url, api_key) {
  res <- httr::GET(order_url, httr::add_headers(Authorization = paste("api-key", api_key)))
  status <- content(res, as = "parsed", simplifyVector = TRUE)
  
  if (status$state != "success") {
    cat("Order not ready yet:", order_url, "\n")
    return(NULL)
  }
  
  links <- status$results
  sapply(links, function(x) x$link)
}

download_all_images <- function(links, output_dir = "planet_files") {
  dir.create(output_dir, showWarnings = FALSE)
  
  for (link in links) {
    file_name <- basename(link)
    file_path <- file.path(output_dir, file_name)
    download.file(link, destfile = file_path, mode = "wb")
    cat("✔ Downloaded:", file_name, "\n")
  }
}


for (i in seq_along(order_urls)) {
  cat("Processing order", i, "...\n")
  links <- get_all_image_links(order_urls[[i]], api_key)
  
  if (!is.null(links)) {
    download_all_images(links, output_dir = paste0("batch_", i))
  }
}

extract_individual_links <- function(order_status) {
  links <- order_status$results
  sapply(links, function(x) x$link)
}

url <- "https://api.planet.com/data/v1/item-types/PSScene/items"

response <- GET(url, authenticate(api_key, ""))
item_types <- fromJSON(content(response, as = "text", encoding = "UTF-8"))

# Print the item types
print(item_types$item_types$name)

#### delete an order
order_url_delete <- "https://api.planet.com/compute/ops/orders/v2/c16b34f1-007d-4d44-a2c1-57a3fa71c3c1"  # Replace with your actual URL

cancel_body <- list(state = "cancelled")

res <- httr::PUT(
  url = order_url_delete,
  body = jsonlite::toJSON(cancel_body, auto_unbox = TRUE),
  httr::add_headers(
    Authorization = paste("api-key", api_key),
    `Content-Type` = "application/json"
  )
)

# Check response
if (httr::status_code(res) %in% c(200, 202)) {
  cat("Order successfully cancelled.\n")
} else {
  cat("Failed to cancel order. Status code:", httr::status_code(res), "\n")
  print(httr::content(res))
}



# Set Order API URL
planet_order_url <- "https://api.planet.com/compute/ops/orders/v2"

# First test authentication for the order_url with your API key
response <- GET(
  planet_order_url,
  add_headers(Authorization = paste("api-key", api_key))
)

# Check status
status_code(response) 

## get all links
get_all_orders <- function(api_key) {
  url <- "https://api.planet.com/compute/ops/orders/v2"
  
  # Initial request
  res <- GET(url,
             add_headers(Authorization = paste("api-key", api_key))
  )
  
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
      next_url <- paste0(url, next_url)
    }
    
    # Wait 1 second to avoid hammering the server
    Sys.sleep(1)
    
    res <- GET(next_url, 
               add_headers(Authorization = paste("api-key", api_key))
    )
    
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
all_orders_sub <- all_orders[c(1:57)]      #change accordingly

# Extract all item_ids from subset of orders
item_ids_list <- lapply(all_orders_sub, function(order) {
  products <- order[["products"]]
  unlist(lapply(products, function(p) p[["item_ids"]]))
})

# Flatten 
all_item_ids <- as.data.frame(unique(unlist(item_ids_list)))
