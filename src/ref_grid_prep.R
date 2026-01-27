#' Prepare Reference Grid by Aggregation
#' This function loads the current reference grid, aggregates it to a target cell size,
#' and saves the new reference grid to a specified path
#' based on the provided configuration.
#' #' @param config A list containing configuration parameters, including:
#' #'   - ref_grid_path: Path to the current reference grid raster file.
#' #'   - ref_grid_target_cellsize: Target cell size (in meters) for aggregation.
#' #    - ref_grid_agg_path: Path to save the aggregated reference grid raster file.
#' @return None. The function saves the aggregated reference grid to disk.
ref_grid_prep <- function(
  config = get_config()
) {
  # Load current reference grid
  ref_grid_current <- terra::rast(config[["ref_grid_path"]])

  # Calculate aggregation factor to reach target cellsize
  target_cellsize <- config[["ref_grid_target_cellsize"]]
  cell_area_m2 <- terra::cellSize(ref_grid_current, unit = "m")
  current_cellsize_actual <- sqrt(terra::global(
    cell_area_m2,
    "mean",
    na.rm = TRUE
  )[[1]])
  agg_factor <- round(target_cellsize / current_cellsize_actual)

  # Aggregate the reference grid
  # Using "min" because ref_grid only has 0 (valid) or NA (invalid)
  # This preserves valid cells (0) if ANY source cell was valid
  ref_grid_agg <- terra::aggregate(
    ref_grid_current,
    fact = agg_factor,
    fun = "min",
    na.rm = TRUE
  )

  # Save the new reference grid
  # Write compressed GeoTIFF with an integer data type and explicit NA flag
  write_raster(
    ref_grid_agg,
    filename = config[["ref_grid_agg_path"]]
  )
}
