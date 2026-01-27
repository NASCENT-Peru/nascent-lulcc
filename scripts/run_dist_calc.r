#!/usr/bin/env Rscript
# run_dist_calc.r
# Run distance calculation for hydrological and socioeconomic features
#
# This script computes distance rasters from vector features using GDAL
# utilities. It is optimized for HPC systems with multi-threading support.

# Capture start time
start_time <- Sys.time()

cat("\n========================================\n")
cat("Starting Distance Calculation\n")
cat("========================================\n\n")

# Diagnostics: show R version and library paths
cat("R version and path:\n")
print(R.version.string)
cat("R executable:\n")
print(Sys.which("R"))
cat(".libPaths():\n")
print(.libPaths())
cat("\n")

# Load required packages
required_pkgs <- c("terra", "yaml")

missing_pkgs <- setdiff(required_pkgs, rownames(installed.packages()))
if (length(missing_pkgs) > 0) {
  cat("WARNING: The following packages are missing in this R environment:\n")
  print(missing_pkgs)
  cat("Attempting to install missing packages into R_LIBS_USER...\n")
  repos <- "https://cloud.r-project.org"
  for (p in missing_pkgs) {
    tryCatch(
      {
        install.packages(
          p,
          repos = repos,
          lib = Sys.getenv("R_LIBS_USER", unset = .libPaths()[1])
        )
      },
      error = function(e) {
        cat(sprintf("ERROR installing package %s: %s\n", p, e$message))
      }
    )
  }
}

# Load packages
for (p in required_pkgs) {
  if (!suppressWarnings(requireNamespace(p, quietly = TRUE))) {
    stop(sprintf(
      "Required package '%s' is not available even after attempted install. 
       Please install it in the environment: %s",
      p,
      Sys.getenv("CONDA_PREFIX", unset = "(unknown)")
    ))
  }
  library(p, character.only = TRUE)
}

cat("All required packages loaded successfully.\n\n")

# Set working directory to project root
script_path <- commandArgs(trailingOnly = FALSE)
script_path <- script_path[grepl("--file=", script_path)]
if (length(script_path) > 0) {
  script_dir <- dirname(sub("--file=", "", script_path))
  project_root <- dirname(script_dir)
} else {
  # Fallback: assume we're running from scripts directory
  project_root <- getwd()
  if (basename(project_root) == "scripts") {
    project_root <- dirname(project_root)
  }
}
setwd(project_root)
cat(sprintf("Working directory set to: %s\n", getwd()))

# Source required functions
src_files <- c(
  "src/setup.r",
  "src/utils.r",
  "src/dist_calc_functions.r"
)

for (src_file in src_files) {
  cat(sprintf("Sourcing %s...\n", src_file))
  tryCatch(
    {
      source(src_file)
      cat(sprintf("%s sourced successfully.\n", src_file))
    },
    error = function(e) {
      cat(sprintf("ERROR sourcing %s: %s\n", src_file, e$message))
      quit(status = 1)
    }
  )
}
cat("\n")

# Get configuration
cat("Loading configuration...\n")
config <- tryCatch(
  {
    get_config()
  },
  error = function(e) {
    cat(sprintf("ERROR getting config: %s\n", e$message))
    quit(status = 1)
  }
)
cat("Configuration loaded successfully.\n\n")

# Run distance calculation
cat("========================================\n")
cat("Running Distance Calculation\n")
cat("========================================\n\n")

result <- tryCatch(
  {
    # Create log directory and file
    log_dir <- file.path(config$data_basepath, "logs", "dist_calc")
    dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)
    log_file <- file.path(
      log_dir,
      sprintf("dist_calc_%s.log", format(Sys.time(), "%Y%m%d_%H%M%S"))
    )

    log_msg("========================================", log_file)
    log_msg("Distance Calculation HPC Script Started", log_file)
    log_msg("========================================", log_file)
    log_msg(paste("Data basepath:", config$data_basepath), log_file)
    log_msg(paste("Log file:", log_file), log_file)
    log_msg("", log_file)

    # -------- Load reference grid -------------------------------------------------
    ref_grid_path <- config$ref_grid_path
    log_msg(paste("Loading reference grid from:", ref_grid_path), log_file)
    if (!file.exists(ref_grid_path)) {
      log_msg(
        paste("ERROR: Reference grid not found at:", ref_grid_path),
        log_file
      )
      stop("Reference grid not found at: ", ref_grid_path)
    }

    ref <- rast(ref_grid_path)
    stopifnot(is.lonlat(ref)) # assumes EPSG:4326 in GDAL calls
    log_msg(
      paste("✓ Reference grid loaded:", ncol(ref), "cols x", nrow(ref), "rows"),
      log_file
    )
    log_msg("", log_file)

    # -------- Env & threading -----------------------------------------------------
    n_threads <- as.integer(Sys.getenv("SLURM_CPUS_PER_TASK", "1"))
    tmp_dir <- Sys.getenv("TMPDIR", unset = tempdir())

    log_msg(paste("Threads available:", n_threads), log_file)
    log_msg(paste("Temporary directory:", tmp_dir), log_file)

    terraOptions(
      tempdir = tmp_dir,
      memfrac = 0.2,
      todisk = TRUE,
      threads = n_threads
    )

    # Compression & datatype settings
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
    log_msg("Locating GDAL utilities...", log_file)

    gdalwarp <- Sys.which("gdalwarp")
    if (gdalwarp == "") {
      log_msg("ERROR: gdalwarp not found", log_file)
      stop("gdalwarp not found")
    }
    log_msg(paste("  ✓ gdalwarp:", gdalwarp), log_file)

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
      log_msg("ERROR: gdal_proximity not found", log_file)
      stop("gdal_proximity not found")
    }
    log_msg(paste("  ✓ gdal_proximity:", gdal_prox), log_file)

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
    log_msg(paste("  ✓ GDAL -distunits support:", has_geo_units), log_file)
    log_msg("", log_file)

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

    log_msg("Loading predictor metadata...", log_file)
    # predictor yaml file path
    predictor_yaml_path <- config$pred_table_path
    log_msg(paste("Predictor YAML:", predictor_yaml_path), log_file)

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
          config$data_basepath,
          config$predictors_raw_dir,
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
          config$data_basepath,
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
          config$data_basepath,
          config$predictors_raw_dir,
          x$raw_dir,
          x$raw_filename
        )
      },
      USE.NAMES = TRUE
    )

    # subset to only airports as roads have been done already
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

    log_msg("", log_file)
    log_msg(
      paste("Total predictors to process:", length(preds_to_process)),
      log_file
    )
    log_msg("========================================", log_file)
    log_msg("", log_file)

    # -------- Run function for each named layer -------------------------------------------
    existing <- preds_to_process[file.exists(preds_to_process)]
    if (length(existing) == 0L) {
      log_msg("ERROR: No shapefiles found", log_file)
      log_msg(
        paste("Searched paths:", paste(preds_to_process, collapse = "\n")),
        log_file
      )
      stop(
        "No shapefiles found at:\n",
        paste(preds_to_process, collapse = "\n")
      )
    }

    log_msg(
      paste("Found", length(existing), "shapefile(s) to process"),
      log_file
    )
    log_msg("", log_file)

    results <- lapply(names(existing), function(nm) {
      shp <- existing[[nm]]
      tryCatch(
        process_shapefile(
          shp_path = shp,
          out_basename = nm,
          log_file = log_file,
          ref = ref,
          tmp_dir = tmp_dir,
          n_threads = n_threads,
          co_heavy_int = co_heavy_int,
          nodata_u32 = nodata_u32,
          gdalwarp = gdalwarp,
          gdal_prox = gdal_prox,
          gdal_calc = gdal_calc,
          prefix = prefix,
          calc_prefix = calc_prefix,
          has_geo_units = has_geo_units,
          pix_m = pix_m,
          ext_ref = ext_ref
        ),
        error = function(e) {
          log_msg(paste("✗ Error on", shp, ":", conditionMessage(e)), log_file)
          NULL
        }
      )
    })

    log_msg("", log_file)
    log_msg("========================================", log_file)
    log_msg("Batch processing finished", log_file)
    log_msg(
      paste(
        "Successful outputs:",
        sum(!sapply(results, is.null)),
        "/",
        length(results)
      ),
      log_file
    )
    log_msg("========================================", log_file)

    message("\nBatch finished. Outputs (non-NULL):")
    print(Filter(Negate(is.null), results))

    list(status = "success", error = NA, results = results)
  },
  error = function(e) {
    cat(sprintf("ERROR in Distance Calculation: %s\n", e$message))
    list(status = "error", error = e$message, results = NULL)
  }
)

# Report results
end_time <- Sys.time()
total_elapsed <- difftime(end_time, start_time, units = "mins")

cat("\n========================================\n")
cat("Distance Calculation Summary\n")
cat("========================================\n")
cat(sprintf("Total runtime: %.2f minutes\n", as.numeric(total_elapsed)))
cat(sprintf("Status: %s\n", result$status))

if (result$status == "error" && !is.na(result$error)) {
  cat(sprintf("Error: %s\n", result$error))
} else if (result$status == "success" && !is.null(result$results)) {
  n_success <- sum(!sapply(result$results, is.null))
  n_total <- length(result$results)
  cat(sprintf("Successfully processed: %d / %d files\n", n_success, n_total))
}

# Save summary
summary_file <- file.path(
  "logs",
  sprintf(
    "dist_calc_summary_%s.txt",
    Sys.getenv("SLURM_JOB_ID", unset = "local")
  )
)

if (!dir.exists("logs")) {
  dir.create("logs", recursive = TRUE)
}

sink(summary_file)
cat(sprintf("Job ID: %s\n", Sys.getenv("SLURM_JOB_ID", unset = "local")))
cat(sprintf("Start time: %s\n", start_time))
cat(sprintf("End time: %s\n", end_time))
cat(sprintf("Runtime: %.2f minutes\n", as.numeric(total_elapsed)))
cat(sprintf("Status: %s\n", result$status))
if (result$status == "error" && !is.na(result$error)) {
  cat(sprintf("Error: %s\n", result$error))
} else if (result$status == "success" && !is.null(result$results)) {
  n_success <- sum(!sapply(result$results, is.null))
  n_total <- length(result$results)
  cat(sprintf("Successfully processed: %d / %d files\n", n_success, n_total))
}
sink()

cat(sprintf("\nSummary saved to: %s\n", summary_file))

# Exit with appropriate code
exit_code <- ifelse(result$status == "error", 1, 0)
cat(sprintf("\nDistance calculation completed with exit code: %d\n", exit_code))
quit(status = exit_code)
