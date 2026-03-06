############################################################
## Create a data cube definition file from Sentinel grid by 
## extracting the tile id X0000_Y0004 for Mals Heath.
## Then separating the tile into 4X4 squares and getting the 
## upper left corner coordinate of the square where Mals heath 
## is located and creating a 4000 X 4000 square and calcualtign
##  the datacube.
############################################################

# Load packages
library(raster)
library(sf)

# Load your raster
r <- rast("/mnt/CEPH_PROJECTS/Environtwin/gis/masks/mals_mask.tif")
s2_grid <- st_read("/mnt/CEPH_PROJECTS/sao/SENTINEL-2/SentinelVegetationProducts/FORCE/misc/shp/grid.shp")

# Check CRS and match if needed
if (st_crs(s2_grid) != crs(r)) {
  s2_grid <- st_transform(s2_grid, st_crs(r))
}

# ---- Plot ----
# First plot the grid
plot(st_geometry(s2_grid), border = "red", lwd = 0.8)

# Then add raster on top, semi-transparent
plot(r, add = TRUE, alpha = 0.7, legend = F)

# Get the Mals Heath tile
poly3 <- s2_grid %>% 
  filter(Tile_ID == "X0000_Y0004")
plot(poly3, add = T, border = "green")

# turn into vector object
poly3 <- vect(poly3)

# Define number of splits
ncol_split <- 4  # horizontal
nrow_split <- 4  # vertical

# Calculate break points
x_breaks <- seq(xmin(poly3), xmax(poly3), length.out = ncol_split + 1)
y_breaks <- seq(ymin(poly3), ymax(poly3), length.out = nrow_split + 1)

# Create polygons
polygons <- list()
id <- 1
for (i in 1:ncol_split) {
  for (j in 1:nrow_split) {
    polygons[[id]] <- rbind(
      c(x_breaks[i], y_breaks[j]),
      c(x_breaks[i+1], y_breaks[j]),
      c(x_breaks[i+1], y_breaks[j+1]),
      c(x_breaks[i], y_breaks[j+1]),
      c(x_breaks[i], y_breaks[j])
    )
    id <- id + 1
  }
}

# Convert to SpatVector
grid <- vect(polygons, type = "polygons")
grid$id <- 1:length(polygons)

# Plot to check
plot(grid, border = "red")
plot(r,add = TRUE)

# Get the third polygon
poly3 <- grid[12]
plot(poly3, add = T, border = "green")

# Extract upper-left corner (xmin, ymax)
ul_coords <- c(xmin(poly3), ymax(poly3))

create_force_def_square <- function(raster_path, def_file = "datacube.def",
                                    target_cols = 4000, target_rows = 4000,
                                    auto_tile = TRUE, block_fraction = 10, min_block = 500,
                                    origin_coords = NULL) {
  
  # 1. Load raster
  r <- rast(raster_path)
  
  # 2. Extract resolution
  res_xy <- res(r)
  
  # 3. Use custom origin if provided, else use raster extent
  if (!is.null(origin_coords)) {
    origin_x <- origin_coords[[1]]
    origin_y <- origin_coords[[2]]
  } else {
    origin_x <- xmin(r)
    origin_y <- ymax(r)
  }
  
  # 4. Compute new xmax/ymin based on upper-left corner
  xmax_new <- origin_x + target_cols * res_xy[1]
  ymin_new <- origin_y - target_rows * res_xy[2]
  
  # 5. Convert origin to lon/lat
  s <- st_sfc(st_point(c(xmax_new, ymin_new)), crs = crs(r))
  s_ll <- st_transform(s, 4326)
  origin_lon <- st_coordinates(s_ll)[1,1]
  origin_lat <- st_coordinates(s_ll)[1,2]
  
  # 6. Tile and block size
  if (auto_tile) {
    # Use exact cube width (ensures one tile only)
    tile_size <- target_cols * res_xy[1]
    block_size <- max(min_block, round(tile_size / block_fraction, -2))
  } else {
    tile_size <- 12000
    block_size <- 1200
  }
  
  # 7. Write FORCE .def file
  proj_wkt <- as.character(crs(r, proj=TRUE))
  cat(
    proj_wkt, "\n",
    origin_lon, "\n",
    origin_lat, "\n",
    xmax_new, "\n",
    ymin_new, "\n",
    tile_size, "\n",
    block_size, "\n",
    file = def_file
  )
}


# Create datacube definition with rectangle 3 as origin
create_force_def_square(
  raster_path = "/mnt/CEPH_PROJECTS/Environtwin/gis/masks/mals_mask.tif",
  def_file = "/mnt/CEPH_PROJECTS/Environtwin/FORCE/P_level2/datacube-definition.prj",
  target_cols = 4000,
  target_rows = 4000,
  origin_coords = ul_coords
)


