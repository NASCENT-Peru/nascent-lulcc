#' LULC_data_prep
#'
#' Preparing land use land cover rasters for all historic periods in the the Swiss Areal
#' Statistiks and aggregating LULC rasters to new classification scheme
#' @author Ben Black
#' @export

lulc_data_prep <- function(config = get_config(), refresh_cache = FALSE) {
  ensure_dir(config[["rasterized_lulc_dir"]])
  ensure_dir(config[["aggregated_lulc_dir"]])

  # Load in the grid to use use for re-projecting the CRS and extent of predictor data
  Ref_grid <- terra::rast(config[["ref_grid_path"]])

  # Load JSON, disable simplification so nested lists remain lists
  scheme <- jsonlite::fromJSON(
    config[["LULC_aggregation_path"]],
    simplifyVector = FALSE
  )

  # Build map_numeric safely
  map_numeric <- setNames(
    lapply(scheme, function(cl) {
      vapply(cl$original_classes, function(x) x$value, numeric(1))
    }),
    vapply(scheme, function(cl) cl$value, numeric(1))
  )

  map_numeric[["NA"]] <- 0 # <-- add explicit rule: source 0 becomes NA

  # Build a 2-column reclass matrix for exact substitutions: old_value -> new_value
  rcl <- do.call(
    rbind,
    lapply(names(map_numeric), function(k) {
      vals <- map_numeric[[k]]
      if (!length(vals)) {
        return(NULL)
      }
      # Map "NA" key to NA_integer_ in the 'new' column; otherwise use numeric code
      newval <- if (identical(k, "NA")) NA_integer_ else as.integer(k) # <--
      cbind(old = as.integer(vals), new = newval)
    })
  )
  if (is.null(rcl)) {
    stop("The aggregation map contains no source values.")
  }
  rcl <- as.matrix(rcl)

  # List input rasters
  tifs <- list.files(
    config[["rasterized_lulc_dir"]],
    pattern = "\\.tif(f)?$",
    full.names = TRUE,
    ignore.case = TRUE
  )
  if (!length(tifs)) {
    stop("No .tif files found in: ", config[["rasterized_lulc_dir"]])
  }

  # Process each raster
  for (f in tifs) {
    # replace lulc_ with lulc_agg_ in the output filename
    out_name <- gsub("lulc_", "lulc_agg_", basename(f))
    out_path <- file.path(config[["aggregated_lulc_dir"]], out_name)
    if (file.exists(out_path)) {
      message("Skipping existing: ", out_path)
      next
    }
    message("Processing: ", f)
    r <- terra::rast(f)

    # Exact-value mapping; values not present in rcl become NA via `others=NA`
    r_agg <- terra::classify(r, rcl = rcl, others = NA)

    # Check that the raster has the correct extent, resolution, and CRS
    if (!all(terra::ext(r_agg) == terra::ext(Ref_grid))) {
      r_agg <- terra::crop(r_agg, Ref_grid)
      r_agg <- terra::extend(r_agg, Ref_grid)
    }
    if (!all(terra::res(r_agg) == terra::res(Ref_grid))) {
      r_agg <- terra::resample(r_agg, Ref_grid, method = "near")
    }
    if (!all(terra::crs(r_agg) == terra::crs(Ref_grid))) {
      r_agg <- terra::project(r_agg, Ref_grid, method = "near")
    }

    # Write compressed GeoTIFF with an integer data type and explicit NA flag
    write_raster(
      r_agg,
      filename = out_path
    )
    message("Wrote: ", out_path)
  }
  message("Done.")
}
