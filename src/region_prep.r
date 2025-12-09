#' Region Preparation: region_prep
#'
#' Preparing Rasters of regions in Peru used for regionalized modelling of land use
#' and land cover transitions
#'
#' @author Ben Black
#' @export

region_prep <- function(config = get_config()) {
  ensure_dir(config[["reg_dir"]])

  # Load reference grid
  ref_grid <- terra::rast(config[["ref_grid_path"]])

  # Load region shapefile
  reg_vect <- terra::vect(file.path(config[["reg_dir"]], "regions.shp"))

  # Extract region names
  regions_pretty <- reg_vect$Workshop_r
  regions_clean <- tolower(gsub(" ", "_", regions_pretty))

  # Assign 1-based numeric IDs
  region_vals <- seq_along(regions_clean) # 1,2,3,4,...
  reg_vect$region_id <- region_vals

  # Save mapping to JSON
  todf <- data.frame(
    value = region_vals,
    label = regions_clean,
    pretty = regions_pretty
  )
  jsonlite::write_json(
    todf,
    file.path(config[["reg_dir"]], "regions.json"),
    pretty = TRUE
  )

  message("Rasterizing regions...")

  # Rasterize using numeric region_id
  reg_rast <- terra::rasterize(reg_vect, ref_grid, field = "region_id")
  reg_rast <- terra::mask(reg_rast, ref_grid)

  # Identify gap cells: ref_grid == 0 AND reg_rast == NA
  message("Identifying gap cells...")
  # gap_cells <- is.na(reg_rast) & (ref_grid == 0)
  # n_gaps <- terra::global(gap_cells, "sum", na.rm = TRUE)[[1]]

  # # Set 0 to NA
  # gap_cells <- terra::ifel(gap_cells == 1, 1, NA)

  # # Save debug raster
  # write_raster(
  #   gap_cells,
  #   file.path(config[["reg_dir"]], "region_gap_cells.tif"),
  #   overwrite = TRUE
  # )

  gap_cells <- terra::rast(file.path(
    config[["reg_dir"]],
    "region_gap_cells.tif"
  ))

  # # Calcuklate distance-based rasters from each polygon to gap cells
  # # more efficient to do with gdal command line tools outside of R
  # message("Computing distance-based assignments for gap cells only...")
  # temp_dir <- file.path(config[["reg_dir"]], "temp_distances")
  # ensure_dir(temp_dir)
  # # Create distance rasters for each polygon
  # dist_rasters <- list() # for (i in seq_along(reg_vect)) {
  # message( # " Processing region ", # i, # "/", # length(reg_vect),
  # ": ", # regions_pretty[i] # ) #
  # Create a raster with 1 for this polygon, NA elsewhere
  # poly_rast <- terra::rasterize(reg_vect[i], ref_grid, field = 1)
  # poly_rast <- terra::mask(poly_rast, ref_grid)
  # Set gap cells to 0 (target value) and keep polygon as 1
  # Non-gap, non-polygon cells remain NA and won't be used
  # poly_rast_with_gaps <- terra::ifel(gap_cells == 1, 0, poly_rast)
  # Calculate distance from polygon (value=1) to gap cells (value=0)
  # dist_rast <- terra::distance(poly_rast_with_gaps, target = 0)
  # Save to temp file to avoid memory issues
  # temp_file <- file.path(temp_dir, paste0("dist_", i, ".tif"))
  # terra::writeRaster(dist_rast, temp_file, overwrite = TRUE)
  # dist_rasters[[i]] <- temp_file
  # }

  message("Assigning regions based on minimum distance...")

  dist_rasts <- list.files(
    config[["reg_dir"]],
    pattern = "dist_.*\\.tif$",
    full.names = TRUE
  )

  # Load distance rasters
  dist_stack <- terra::rast(dist_rasts)

  # Mask to gap cells
  dist_stack <- terra::mask(dist_stack, gap_cells)

  # Find nearest region: no `-1` since using 1-based IDs
  nearest_region <- terra::which.min(dist_stack)

  # Fill gaps
  reg_rast_final <- terra::ifel(gap_cells == 1, nearest_region, reg_rast)

  # Save final raster
  outpath <- file.path(config[["reg_dir"]], "regions.tif")
  terra::writeRaster(
    reg_rast_final,
    outpath,
    overwrite = TRUE,
    wopt = list(
      datatype = "INT2U",
      NAflag = 65535,
      gdal = c("COMPRESS=LZW", "ZLEVEL=9", "TILED=YES", "PREDICTOR=2")
    )
  )

  message("Completed conversion of regions shapefile to raster: ", outpath)
}
