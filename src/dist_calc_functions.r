#' Distance Calculation Functions
#'
#' Functions for computing distance-to-feature rasters using GDAL utilities
#' on HPC systems. These functions are optimized for large-scale processing
#' with heavy compression and integer precision.
#'
#' @author Manuel Kurmann and Ben Black

#' Log a message with timestamp to file and/or console
#' @param msg Character. Message to log.
#' @param log_file Character. Path to log file (optional).
#' @param also_console Logical. Whether to also print to console.
log_msg <- function(msg, log_file = NULL, also_console = TRUE) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  line <- paste0(timestamp, " | ", msg, "\n")
  if (!is.null(log_file)) {
    dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)
    cat(line, file = log_file, append = TRUE)
  }
  if (also_console) cat(line)
}

#' Remove auxiliary GDAL files associated with a dataset
#' @param path Character. Path to the main dataset file.
#' @return Invisible logical FALSE.
rm_ds <- function(path) {
  if (!nzchar(path)) {
    return(invisible(FALSE))
  }
  side <- c("", ".aux.xml", ".ovr", ".msk", ".idx", ".json")
  suppressWarnings(invisible(file.remove(paste0(path, side))))
}

#' Process a single shapefile to compute distance raster
#' @param shp_path Character. Path to input shapefile.
#' @param out_basename Character. Base name for output raster (without extension).
#' @param log_file Character. Path to log file (optional).
#' @param ref SpatRaster. Reference grid for spatial extent and resolution.
#' @param tmp_dir Character. Temporary directory for intermediate files.
#' @param n_threads Integer. Number of threads for parallel processing.
#' @param co_heavy_int Character vector. GDAL compression options.
#' @param nodata_u32 Character. NoData value for UInt32 rasters.
#' @param gdalwarp Character. Path to gdalwarp executable.
#' @param gdal_prox Character. Path to gdal_proximity executable.
#' @param gdal_calc Character. Path to gdal_calc executable (optional).
#' @param prefix Character vector. Prefix for gdal_proximity command.
#' @param calc_prefix Character vector. Prefix for gdal_calc command.
#' @param has_geo_units Logical. Whether GDAL supports -distunits flag.
#' @param pix_m Numeric. Pixel size in meters for metric projection.
#' @param ext_ref SpatExtent. Extent of reference grid.
#' @return Character path to output raster, or NULL if failed.
process_shapefile <- function(
  shp_path,
  out_basename,
  log_file = NULL,
  ref,
  tmp_dir,
  n_threads,
  co_heavy_int,
  nodata_u32,
  gdalwarp,
  gdal_prox,
  gdal_calc,
  prefix,
  calc_prefix,
  has_geo_units,
  pix_m,
  ext_ref
) {
  if (!file.exists(shp_path)) {
    log_msg(paste("! Missing shapefile:", shp_path), log_file)
    return(NULL)
  }

  out_dir <- dirname(shp_path)
  out_dist_path <- file.path(out_dir, paste0(out_basename, ".tif"))

  # --- Early exit if output already exists -----------------------------------
  if (file.exists(out_dist_path)) {
    log_msg(paste("↷ Skipping (already exists):", out_dist_path), log_file)
    # Still show header so the run log lists what was used
    print(tryCatch(terra::rast(out_dist_path), error = function(e) {
      paste("  (could not read:", e$message, ")")
    }))
    return(out_dist_path)
  }

  log_msg(
    paste("\n=== Processing:", shp_path, "->", out_dist_path, "==="),
    log_file
  )

  # Load vector; align CRS to ref
  v <- terra::vect(shp_path)
  if (!terra::same.crs(ref, v)) {
    v <- terra::project(v, terra::crs(ref))
  }

  # 1) Rasterize vector -> byte mask (WGS84)
  mask_path <- file.path(
    tmp_dir,
    paste0(out_basename, "_mask_byte_wgs84.tif")
  )
  rm_ds(mask_path)
  invisible(terra::rasterize(
    v,
    ref,
    field = 1,
    background = 0,
    filename = mask_path,
    overwrite = TRUE,
    wopt = list(datatype = "INT1U", gdal = co_heavy_int)
  ))
  rm(v)
  gc()

  # 2) Reproject mask to metric grid (EPSG:3395) with square meter pixels
  mask_metric_vrt <- file.path(
    tmp_dir,
    paste0(out_basename, "_mask_metric.vrt")
  )
  rm_ds(mask_metric_vrt)
  warp_args1 <- c(
    "-overwrite",
    "-of",
    "VRT",
    "-s_srs",
    "EPSG:4326",
    "-t_srs",
    "EPSG:3395",
    "-tr",
    sprintf("%.6f", pix_m),
    sprintf("%.6f", pix_m),
    "-r",
    "near",
    mask_path,
    mask_metric_vrt,
    "-multi",
    "-wo",
    paste0("NUM_THREADS=", n_threads)
  )
  invisible(system2(gdalwarp, warp_args1))

  # 3) Proximity on metric grid (UInt32 meters)
  prox_metric_path_px <- file.path(
    tmp_dir,
    paste0(out_basename, "_distance_metric_px.tif")
  )
  prox_metric_path_m <- file.path(
    tmp_dir,
    paste0(out_basename, "_distance_metric_m.tif")
  )
  rm_ds(prox_metric_path_px)
  rm_ds(prox_metric_path_m)

  prox_args <- c(
    mask_metric_vrt,
    prox_metric_path_px,
    "-values",
    "1",
    "-of",
    "GTiff",
    "-ot",
    "UInt32",
    "-co",
    co_heavy_int[1],
    "-co",
    co_heavy_int[2],
    "-co",
    co_heavy_int[3],
    "-co",
    co_heavy_int[4],
    "-co",
    co_heavy_int[5],
    "-co",
    co_heavy_int[6],
    "-co",
    co_heavy_int[7],
    "-nodata",
    nodata_u32
  )
  if (has_geo_units) {
    prox_args <- c(prox_args, "-distunits", "GEO")
  }
  invisible(system2(gdal_prox, c(prefix, prox_args)))

  if (has_geo_units) {
    file.rename(prox_metric_path_px, prox_metric_path_m) # already meters
  } else {
    if (gdal_calc == "") {
      stop("GDAL lacks -distunits and gdal_calc.py is unavailable.")
    }
    scale_args <- c(
      calc_prefix,
      "-A",
      prox_metric_path_px,
      "--calc",
      sprintf("numpy.rint(A*%.8f)", pix_m),
      "--type=UInt32",
      paste0("--NoDataValue=", nodata_u32),
      "--outfile",
      prox_metric_path_m,
      "--co",
      co_heavy_int[1],
      "--co",
      co_heavy_int[2],
      "--co",
      co_heavy_int[3],
      "--co",
      co_heavy_int[4],
      "--co",
      co_heavy_int[5],
      "--co",
      co_heavy_int[6],
      "--co",
      co_heavy_int[7],
      "--overwrite"
    )
    invisible(system2(gdal_calc, scale_args))
    rm_ds(prox_metric_path_px)
  }

  # 4) Warp meters back to exact WGS84 ref grid (keep integers)
  rm_ds(out_dist_path)
  ext_vals <- c(
    terra::xmin(ext_ref),
    terra::ymin(ext_ref),
    terra::xmax(ext_ref),
    terra::ymax(ext_ref)
  )
  warp_back_args <- c(
    "-overwrite",
    "-t_srs",
    "EPSG:4326",
    "-te",
    sprintf("%.10f", ext_vals[1]),
    sprintf("%.10f", ext_vals[2]),
    sprintf("%.10f", ext_vals[3]),
    sprintf("%.10f", ext_vals[4]),
    "-ts",
    as.character(terra::ncol(ref)),
    as.character(terra::nrow(ref)),
    "-r",
    "near",
    "-ot",
    "UInt32",
    "-srcnodata",
    nodata_u32,
    "-dstnodata",
    nodata_u32,
    prox_metric_path_m,
    out_dist_path,
    "-co",
    co_heavy_int[1],
    "-co",
    co_heavy_int[2],
    "-co",
    co_heavy_int[3],
    "-co",
    co_heavy_int[4],
    "-co",
    co_heavy_int[5],
    "-co",
    co_heavy_int[6],
    "-co",
    co_heavy_int[7],
    "-multi",
    "-wo",
    paste0("NUM_THREADS=", n_threads)
  )
  invisible(system2(gdalwarp, warp_back_args))

  # 5) Mask where ref == 0 -> NA (write UInt32 with NoData=nodata_u32)
  tmpout <- paste0(out_dist_path, ".tmp.tif")
  rm_ds(tmpout)

  dist_r <- terra::rast(out_dist_path)
  masked <- terra::mask(
    dist_r,
    ref,
    filename = tmpout,
    overwrite = TRUE,
    wopt = list(
      datatype = "INT4U",
      NAflag = as.numeric(nodata_u32),
      gdal = co_heavy_int
    )
  )
  rm(dist_r, masked)
  gc()

  # Atomic replace
  rm_ds(out_dist_path)
  if (!file.rename(tmpout, out_dist_path)) {
    ok <- file.copy(tmpout, out_dist_path, overwrite = TRUE)
    rm_ds(tmpout)
    if (!ok) stop("Failed to move masked temp file into place.")
  }

  # Cleanup temps
  rm_ds(mask_path)
  rm_ds(mask_metric_vrt)
  rm_ds(prox_metric_path_m)

  print(terra::rast(out_dist_path))
  log_msg(paste("✓ Done:", out_dist_path), log_file)
  invisible(out_dist_path)
}
