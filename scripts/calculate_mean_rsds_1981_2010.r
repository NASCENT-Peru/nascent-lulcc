# Packages
library(terra)
library(stringr)

# ----------------------------- Paths -----------------------------------------
input_dir <- file.path("srad", "monthly_single_years")
output_dir <- file.path("srad", "monthly_averages_1981_2010")
ref_grid_path <- "./ref_grid.tif"

if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# --------------------------- Load reference ----------------------------------
ref <- rast(ref_grid_path)

# --------------------------- Collect inputs ----------------------------------
all_files <- list.files(input_dir, pattern = "\\.tif$", full.names = TRUE)
if (length(all_files) == 0) {
  stop("No .tif files found in: ", input_dir)
}

get_month <- function(path) {
  m <- str_match(basename(path), "(?<!\\d)(0[1-9]|1[0-2])(?!\\d)")[, 2]
  if (is.na(m)) {
    stop("Could not extract month from filename: ", basename(path))
  }
  m
}

months <- sprintf("%02d", 1:12)

# ---------------------- Determine which months are missing --------------------
existing_out <- list.files(
  output_dir,
  pattern = "^rsds_1981_2010_\\d{2}\\.tif$",
  full.names = FALSE
)
existing_mm <- na.omit(str_match(
  existing_out,
  "^rsds_1981_2010_(\\d{2})\\.tif$"
)[, 2])
months_to_do <- setdiff(months, existing_mm)

if (length(months_to_do) == 0) {
  message("All 12 monthly outputs already exist in: ", output_dir)
} else {
  # terraOptions(memfrac = 0.5) # You can keep this, but the new method is less dependent on it.

  for (mm in months_to_do) {
    # --- Create a unique, clean temporary directory FOR THIS MONTH ONLY ---
    temp_dir <- file.path(
      tempdir(),
      paste0("terra_month_", mm, "_", as.integer(Sys.time()))
    )
    if (!dir.exists(temp_dir)) {
      dir.create(temp_dir)
    }
    terraOptions(tempdir = temp_dir)

    out_name <- sprintf("rsds_1981_2010_%s.tif", mm)
    out_path <- file.path(output_dir, out_name)
    if (file.exists(out_path)) {
      message(sprintf("Skipping month %s (exists): %s", mm, out_name))
      next
    }

    message(sprintf("-------------------\nProcessing month %s...", mm))

    # 1) Filter files for the current month
    month_files <- all_files[vapply(
      all_files,
      function(p) get_month(p) == mm,
      logical(1L)
    )]
    if (length(month_files) < 2) {
      warning(sprintf("Not enough files to calculate a mean for month %s", mm))
      next
    }

    # 2) *** NEW: PRE-FLIGHT CHECK FOR GEOMETRY CONSISTENCY ***
    message("  (1/5) Checking file consistency...")
    tryCatch(
      {
        first_rast_info <- rast(month_files[1])
        template_geom <- list(
          crs = crs(first_rast_info),
          ext = as.character(ext(first_rast_info)), # as.character for easy comparison
          res = res(first_rast_info)
        )

        for (i in 2:length(month_files)) {
          current_rast_info <- rast(month_files[i])
          if (
            !identical(crs(current_rast_info), template_geom$crs) ||
              !identical(
                as.character(ext(current_rast_info)),
                template_geom$ext
              ) ||
              !identical(res(current_rast_info), template_geom$res)
          ) {
            stop(sprintf(
              "Inconsistent geometry found for month %s! File:\n%s\ndoes not match the first file in the series. Please standardize all input files.",
              mm,
              month_files[i]
            ))
          }
        }
        rm(first_rast_info, current_rast_info) # clean up
      },
      error = function(e) {
        # This makes the script stop gracefully instead of crashing
        stop(e$message)
      }
    )

    # 3) *** NEW: MORE EFFICIENT MEAN CALCULATION ***
    # This uses a single, optimized terra command. It processes the stack
    # chunk-by-chunk from disk without a leaky R `for` loop.
    message("  (2/5) Calculating mean across years...")
    rst_stack <- rast(month_files)
    rst_mean <- app(
      rst_stack,
      "mean",
      na.rm = TRUE,
      # Write intermediate result to disk immediately to free RAM
      filename = file.path(temp_dir, "mean.tif"),
      overwrite = TRUE
    )

    # 4) Reproject & resample to the reference grid
    message("  (3/5) Reprojecting to reference grid...")
    rst_proj <- project(
      rst_mean,
      ref,
      method = "bilinear",
      filename = file.path(temp_dir, "proj.tif"),
      overwrite = TRUE
    )

    # 5) Mask to reference grid
    message("  (4/5) Masking...")
    rst_masked <- mask(
      rst_proj,
      ref,
      filename = file.path(temp_dir, "masked.tif"),
      overwrite = TRUE
    )

    # 6) Write final result
    message("  (5/5) Writing final output...")
    writeRaster(
      rst_masked,
      filename = out_path,
      overwrite = FALSE, # Fails if file exists from a previous run
      gdal = c("COMPRESS=DEFLATE", "PREDICTOR=2", "ZLEVEL=6")
    )

    message(sprintf("Month %s processed successfully → %s", mm, out_name))

    # 7) *** NEW: AGGRESSIVE CLEANUP AT THE END OF EACH LOOP ***
    rm(rst_stack, rst_mean, rst_proj, rst_masked)

    # This command asks terra to remove any temp files it knows about
    terra::tmpFiles(remove = TRUE)

    # This removes the entire temporary directory for this month
    unlink(temp_dir, recursive = TRUE, force = TRUE)

    gc() # Final garbage collection
  }

  message(
    "-------------------\nDone. Processed months: ",
    paste(months_to_do, collapse = ", ")
  )
}
