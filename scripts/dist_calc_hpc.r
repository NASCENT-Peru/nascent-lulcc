#' Compute distance-to-hydrological-features rasters in a batch on HPC
#' is called by the sbatch script: scripts/run_dist_calc.sh
#' which requires the conda environment (evs/dist_calc_env.yml) to be created with the script: scripts/setup_dist_calc_env.sh.
# Integer meters + heavy compression (DEFLATE level 9, predictor=2)
library(terra)
library(yaml)

# functions --------------------------------------------------------------

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
process_shapefile <- function(shp_path, out_basename) {
  if (!file.exists(shp_path)) {
    message("! Missing shapefile: ", shp_path)
    return(NULL)
  }

  out_dir <- dirname(shp_path)
  out_dist_path <- file.path(out_dir, paste0(out_basename, ".tif")) # e.g., dist_to_dams.tif

  # --- Early exit if output already exists -----------------------------------
  if (file.exists(out_dist_path)) {
    message("↷ Skipping (already exists): ", out_dist_path)
    # Still show header so the run log lists what was used
    print(tryCatch(rast(out_dist_path), error = function(e) {
      paste("  (could not read:", e$message, ")")
    }))
    return(out_dist_path)
  }

  message("\n=== Processing: ", shp_path, " -> ", out_dist_path, " ===")

  # Load vector; align CRS to ref
  v <- vect(shp_path)
  if (!same.crs(ref, v)) {
    v <- project(v, crs(ref))
  }

  # 1) Rasterize vector -> byte mask (WGS84)
  mask_path <- file.path(
    tmp_dir,
    paste0(out_basename, "_mask_byte_wgs84.tif")
  )
  rm_ds(mask_path)
  invisible(rasterize(
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
  ext_vals <- c(xmin(ext_ref), ymin(ext_ref), xmax(ext_ref), ymax(ext_ref))
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
    as.character(ncol(ref)),
    as.character(nrow(ref)),
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

  dist_r <- rast(out_dist_path)
  masked <- mask(
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

  print(rast(out_dist_path))
  message("✓ Done: ", out_dist_path)
  invisible(out_dist_path)
}


# Main -------------------------------------------------------------------

# -------- Load reference grid -------------------------------------------------
ref_grid_path <- "ref_grid.tif" # WGS84 lon/lat ref (EPSG:4326 expected)
ref <- rast(ref_grid_path)
stopifnot(is.lonlat(ref)) # assumes EPSG:4326 in GDAL calls

# -------- Env & threading -----------------------------------------------------
n_threads <- as.integer(Sys.getenv("SLURM_CPUS_PER_TASK", "1"))
tmp_dir <- Sys.getenv("TMPDIR", unset = tempdir())
terraOptions(
  tempdir = tmp_dir,
  memfrac = 0.2,
  todisk = TRUE,
  threads = n_threads
)

# Compression & datatype settings
# (Change to COMPRESS=ZSTD, ZSTD_LEVEL=19 if your GDAL supports it.)
co_heavy_int <- c(
  "TILED=YES",
  "BLOCKXSIZE=512",
  "BLOCKYSIZE=512",
  "COMPRESS=DEFLATE",
  "ZLEVEL=9",
  "PREDICTOR=2",
  "BIGTIFF=YES"
)
nodata_u32 <- "4294967295" # UInt32 NoData sentinel

# -------- Locate GDAL utilities ----------------------------------------------
gdalwarp <- Sys.which("gdalwarp")
if (gdalwarp == "") {
  stop("gdalwarp not found")
}

gdal_prox <- Sys.which("gdal_proximity")
prefix <- character(0)
if (gdal_prox == "") {
  gdal_prox <- Sys.which("gdal_proximity.py")
}
if (gdal_prox == "") {
  py <- Sys.which("python3")
  if (py == "") {
    py <- Sys.which("python")
  }
  if (py != "") {
    gdal_prox <- py
    prefix <- c("-m", "osgeo_utils.gdal_proximity")
  }
}
if (gdal_prox == "") {
  stop("gdal_proximity not found")
}

gdal_calc <- Sys.which("gdal_calc.py")
calc_prefix <- character(0)
if (gdal_calc == "") {
  py <- Sys.which("python3")
  if (py == "") {
    py <- Sys.which("python")
  }
  if (py != "") {
    gdal_calc <- py
    calc_prefix <- c("-m", "osgeo_utils.gdal_calc")
  }
}

# Check -distunits support once
help_out <- tryCatch(
  system2(gdal_prox, c(prefix, "--help"), stdout = TRUE, stderr = TRUE),
  error = function(e) ""
)
has_geo_units <- any(grepl("distunits", help_out, ignore.case = TRUE))

# -------- Pre-compute metric pixel size from ref ------------------------------
res_deg <- res(ref)
ext_ref <- ext(ref)
lat_mid <- (ymin(ext_ref) + ymax(ext_ref)) / 2 * pi / 180
m_per_deg_lat <- 111132.92 -
  559.82 * cos(2 * lat_mid) +
  1.175 * cos(4 * lat_mid)
m_per_deg_lon <- 111412.84 * cos(lat_mid) - 93.5 * cos(3 * lat_mid)
dx_m <- res_deg[1] * m_per_deg_lon
dy_m <- res_deg[2] * m_per_deg_lat
pix_m <- min(dx_m, dy_m) # meter-sized square pixel on EPSG:3395 grid

# -------- Define shapefiles to process ----------------------------------------

# Replace this base directory for your cluster path:
base_dir <- "./"

# predictor yaml file path
predictor_yaml_path <- file.path("pred_data.yaml")

# read predictor yaml
predictor_yaml <- yaml::yaml.load_file(predictor_yaml_path)

# subset to grouping "hydrological"
hydro_predictors <- Filter(
  function(x) !is.null(x$grouping) && x$grouping == "hydrological",
  predictor_yaml
)

# Construct full vector paths
hydro_vect_paths <- sapply(
  hydro_predictors,
  function(x) {
    file.path(
      base_dir,
      x$raw_dir,
      x$raw_filename
    )
  },
  USE.NAMES = TRUE
)

# extract socioeconomic predictors subsetting to only those where intermediate_path is not null
socio_economic_predictors <- Filter(
  function(x) {
    !is.null(x$grouping) &&
      x$grouping == "socioeconomic" &&
      !is.null(x$intermediate_path)
  },
  predictor_yaml
)

# get full paths for socioeconomic predictors
socio_econ_paths <- sapply(
  socio_economic_predictors,
  function(x) {
    file.path(
      base_dir,
      x$intermediate_path
    )
  },
  USE.NAMES = TRUE
)

# seperate infrastructure predictors
infra_predictors <- Filter(
  function(x) {
    !is.null(x$grouping) &&
      x$grouping == "infrastructure"
  },
  predictor_yaml
)

# get full paths for infrastructure predictors
infra_paths <- sapply(
  infra_predictors,
  function(x) {
    file.path(
      base_dir,
      x$raw_dir,
      x$raw_filename
    )
  },
  USE.NAMES = TRUE
)

# subset to only arirports as roads have been done already
airport_path <- infra_paths[grepl("airport", names(infra_paths))]


# combine the sets of predictors
preds_list <- c(
  #hydro_vect_paths,
  socio_econ_paths
  #airport_path
)

# convert to named vector
preds_to_process <- as.character(preds_list) # drop names for safety
names(preds_to_process) <- names(preds_list) # preserve original names


# -------- Run function for each named layer -------------------------------------------
existing <- preds_to_process[file.exists(preds_to_process)]
if (length(existing) == 0L) {
  stop("No shapefiles found at:\n", paste(preds_to_process, collapse = "\n"))
}

message("Found ", length(existing), " shapefile(s).")
results <- lapply(names(existing), function(nm) {
  shp <- existing[[nm]]
  tryCatch(process_shapefile(shp, nm), error = function(e) {
    message("✗ Error on ", shp, ": ", conditionMessage(e))
    NULL
  })
})

message("\nBatch finished. Outputs (non-NULL):")
print(Filter(Negate(is.null), results))
