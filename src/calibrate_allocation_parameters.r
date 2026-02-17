#' Calculate_allocation_parameters:
#'
#' Using land use data from calibration (historic) periods to calculate parameters for
#' Dinamica's Patcher and Expander algorithmns Mean patch size, patch size variance,
#' patch isometry, percentage of transitons in new patches vs. expansion of existing
#' patches
#'
#' @param config a list of configuration params
#'
#' @details
#' 1. Estimate values for the allocation parameters and then apply random perturbation
#' to generate sets of values to test with monte-carlo simulation
#' 2. perform simulations with all parameter sets
#' 3. Identify best performing parameter sets and save copies of tables
#' to be used in scenario simulations
#'
#' @author Ben Black

calibrate_allocation_parameters <- function(
  config = get_config(),
  refresh_cache = FALSE
) {
  #### A - Calculate allocation parameters for all time periods ####
  # vector years of lulc data
  lulc_years <-
    list.files(
      config[["aggregated_lulc_dir"]],
      full.names = FALSE,
      pattern = ".tif"
    ) |>
    stringr::str_extract("\\d{4}")

  # Load list of historic lulc rasters
  lulc_rasters <- lapply(
    list.files(
      config[["aggregated_lulc_dir"]],
      full.names = TRUE,
      pattern = ".tif"
    ),
    terra::rast
  )
  names(lulc_rasters) <- lulc_years

  # create folders for results
  ensure_dir(config[["simulation_param_dir"]])
  ensure_dir(config[["calibration_param_dir"]])

  # loop over config$data_periods to create list of raster combinations
  lulc_change_periods <- lapply(
    config[["data_periods"]],
    function(period) {
      years <- as.numeric(stringr::str_split(period, "_")[[1]])
      c(
        paste0(years[1]),
        paste0(years[2])
      )
    }
  )
  names(lulc_change_periods) <- config[["data_periods"]]

  # Apply nested function to calculate allocation parameters for all periods, by region, for all transitions
  Allocation_params_by_period <- mapply(
    calculate_allocation_params_for_periods,
    comparison_years = lulc_change_periods,
    period_name = names(lulc_change_periods),
    MoreArgs = list(
      lulc_rasters = lulc_rasters,
      refresh_cache = refresh_cache
    ),
    SIMPLIFY = FALSE
  )

  # # load the similarity values produced from the validation process inside Dinamica
  # # for each simulation
  # calibration_results <-
  #   fs::dir_ls(
  #     path = work_dir,
  #     glob = "**/similarity_value*.csv", # grandparent folder is simulation_id
  #     recurse = TRUE
  #   ) |>
  #   rlang::set_names(\(x) stringr::str_split_i(x, "/", -3)) |> # get simulation_id from folder name
  #   purrr::map(
  #     readr::read_csv, # these files are actually just a float without header
  #     col_names = "similarity_value",
  #     col_types = "d"
  #   ) |>
  #   dplyr::bind_rows(.id = "simulation_id") |>
  #   dplyr::filter(simulation_id != "summary")

  # # summary statistics
  # calibration_summary <- data.frame(
  #   Mean = mean(calibration_results[["similarity_value"]]),
  #   `Standard Deviation` = sd(calibration_results[["similarity_value"]]),
  #   Minimum = min(calibration_results[["similarity_value"]]),
  #   Maximum = max(calibration_results[["similarity_value"]]),
  #   n = length(calibration_results[["similarity_value"]])
  # )

  # # save summary statistics
  # readr::write_csv(
  #   calibration_summary,
  #   file.path(work_dir, "validation_summary.csv")
  # )

  # # select best performing simulation_ID
  # best_sim_ID <- calibration_results[
  #   which.max(calibration_results[["similarity_value"]]),
  # ][["simulation_id"]]

  # # Use this sim ID to create parameter tables for all simulation time points
  # # in the Simulation folder

  # # get exemplar table
  # param_table <- read.csv(
  #   list.files(
  #     file.path(config[["calibration_param_dir"]], best_sim_ID),
  #     full.names = TRUE,
  #     pattern = "2020"
  #   )
  # )
  # colnames(param_table) <- c(
  #   "From*",
  #   "To*",
  #   " Mean_Patch_Size",
  #   "Patch_Size_Variance",
  #   "Patch_Isometry",
  #   "Perc_expander",
  #   "Perc_patcher"
  # )

  # # get simulation start and end times from control table
  # simulation_control <- readr::read_csv(config[["ctrl_tbl_path"]])
  # simulation_start <- min(simulation_control$scenario_start.real)
  # simulation_end <- max(simulation_control$scenario_end.real)
  # scenario_IDs <- unique(simulation_control$scenario_id.string)

  # # loop over scenario IDs and simulation time points creating allocation param tables
  # sapply(
  #   scenario_IDs,
  #   function(scenario_id) {
  #     sapply(
  #       seq(simulation_start, simulation_end, step_length),
  #       function(simulation_year) {
  #         save_dir <- file.path(config[["simulation_param_dir"]], scenario_id)
  #         ensure_dir(save_dir)
  #         file_name <- file.path(
  #           save_dir,
  #           paste0("allocation_param_table_", simulation_year, ".csv")
  #         )
  #         readr::write_csv(param_table, file = file_name)
  #       }
  #     )
  #   }
  # )
}

#' Main function to calculate allocation parameters for a given time period
#' @param comparison_years vector of the names of the two years to compare
#' @param lulc_rasters list of rasters
#' @param period_name name of the time period
#' @param use_regions logical, whether to calculate parameters by region
#' @param temp_dir directory for temporary raster files
#' @return data frame of allocation parameters
calculate_allocation_params_for_periods <- function(
  comparison_years,
  lulc_rasters,
  period_name,
  use_regions = isTRUE(config[["regionalization"]]),
  temp_dir = config[["temp_dir"]],
  refresh_cache = FALSE
) {
  # Handle both list and vector input for comparison_years
  # (mapply passes vectors, but manual calls might pass lists)
  if (is.list(comparison_years) && !is.data.frame(comparison_years)) {
    comparison_years <- comparison_years[[1]]
  }

  message(paste(rep("=", 80), collapse = ""))
  message(sprintf(
    "STARTING ALLOCATION PARAMETER CALCULATION FOR PERIOD: %s",
    period_name
  ))
  message(paste(rep("=", 80), collapse = ""))

  # Create debug directory for this period
  debug_dir <- file.path(
    config[["calibration_param_dir"]],
    period_name,
    "debug"
  )
  ensure_dir(debug_dir)

  # Ensure temp directory exists
  if (!dir.exists(temp_dir)) {
    message(sprintf("Creating temp directory: %s", temp_dir))
    ensure_dir(temp_dir)
  }

  # Load LULC rasters for time period
  message(sprintf(
    "Loading LULC rasters for years: %s",
    paste(comparison_years, collapse = ", ")
  ))

  # Find exact matches for the years
  yr1_idx <- which(names(lulc_rasters) == comparison_years[1])
  yr2_idx <- which(names(lulc_rasters) == comparison_years[2])

  if (length(yr1_idx) == 0) {
    stop(sprintf(
      "Year %s not found in LULC rasters. Available years: %s",
      comparison_years[1],
      paste(names(lulc_rasters), collapse = ", ")
    ))
  }
  if (length(yr2_idx) == 0) {
    stop(sprintf(
      "Year %s not found in LULC rasters. Available years: %s",
      comparison_years[2],
      paste(names(lulc_rasters), collapse = ", ")
    ))
  }

  yr1 <- lulc_rasters[[yr1_idx]]
  yr2 <- lulc_rasters[[yr2_idx]]
  message("  ✓ LULC rasters loaded")

  # --- Set up regions ---
  if (use_regions) {
    message("Loading region configuration...")
    regions <- jsonlite::fromJSON(file.path(
      config[["reg_dir"]],
      "regions.json"
    ))
    region_names <- regions$label
    message(sprintf(
      "  ✓ Regionalization ENABLED: %d regions (%s)",
      length(region_names),
      paste(region_names, collapse = ", ")
    ))

    # load the region raster
    region_rast <- terra::rast(list.files(
      config[["reg_dir"]],
      pattern = "regions.tif$",
      full.names = TRUE
    ))
  } else {
    region_names <- "National extent"
    regions <- NULL
    region_rast <- NULL
    message("  ✓ Regionalization DISABLED: Processing national extent")
  }

  # Load summary of transition feature selection
  message("Loading transition feature selection summary...")
  transitions <- readRDS(
    file.path(
      config[["feature_selection_dir"]],
      sprintf(
        "transition_feature_selection_summary_%s.rds",
        period_name
      )
    )
  )

  # Filter out transitions that failed feature selection
  transitions_all <- transitions %>%
    dplyr::filter(
      !is.na(selected_predictors) &
        selected_predictors != "" &
        nchar(trimws(selected_predictors)) > 0
    )

  message(sprintf(
    "  ✓ Loaded %d valid transitions (filtered from %d total)",
    nrow(transitions),
    nrow(transitions_all)
  ))

  # check that transition parquet directory exists
  transitions_dir <- file.path(
    config[["trans_pre_pred_filter_dir"]],
    period_name
  )

  if (!dir.exists(transitions_dir)) {
    stop(
      "Transition parquet directory not found: ",
      transitions_dir,
      "\nPlease run transition_dataset_prep() first."
    )
  }

  # --- Process all regions ---
  message(sprintf(
    "\nProcessing %d region(s) sequentially...",
    length(region_names)
  ))

  process_all_regions(
    region_names = region_names,
    regions = regions,
    yr1 = yr1,
    yr2 = yr2,
    region_rast = region_rast,
    transitions = transitions,
    transitions_dir = transitions_dir,
    period_name = period_name,
    temp_dir = temp_dir,
    debug_dir = debug_dir,
    refresh_cache = refresh_cache
  )

  message(
    "\n✓ COMPLETED ALLOCATION PARAMETER ESTIMATION FOR ALL TRANSITIONS, REGIONS, AND TIME PERIODS"
  )
  message(paste(rep("=", 80), collapse = ""))
}

#' Process all regions sequentially
#' @param region_names vector of region names
#' @param regions regions data frame with value and label columns
#' @param yr1 LULC raster for initial year
#' @param yr2 LULC raster for final year
#' @param region_rast region raster
#' @param transitions transitions data frame
#' @param transitions_dir directory containing transition parquet files
#' @param period_name name of the time period
#' @param temp_dir directory for temporary files
#' @param debug_dir directory for debug/log files
#' @param refresh_cache if TRUE, overwrite all cached files; if FALSE, reuse existing caches
process_all_regions <- function(
  region_names,
  regions,
  yr1,
  yr2,
  region_rast,
  transitions,
  transitions_dir,
  period_name,
  temp_dir,
  debug_dir,
  refresh_cache = FALSE
) {
  lapply(seq_along(region_names), function(region_idx) {
    process_single_region(
      region_idx = region_idx,
      region_names = region_names,
      regions = regions,
      yr1 = yr1,
      yr2 = yr2,
      region_rast = region_rast,
      transitions = transitions,
      transitions_dir = transitions_dir,
      period_name = period_name,
      temp_dir = temp_dir,
      debug_dir = debug_dir,
      refresh_cache = refresh_cache
    )
  })
}
#' Process a single region
#' @param region_idx index of the region
#' @param region_names vector of region names
#' @param regions regions data frame
#' @param yr1 LULC raster for initial year
#' @param yr2 LULC raster for final year
#' @param region_rast region raster
#' @param transitions transitions data frame
#' @param transitions_dir directory containing transition parquet files
#' @param period_name name of the time period
#' @param temp_dir directory for temporary files
#' @param debug_dir directory for debug/log files
process_single_region <- function(
  region_idx,
  region_names,
  regions,
  yr1,
  yr2,
  region_rast,
  transitions,
  transitions_dir,
  period_name,
  temp_dir,
  debug_dir,
  refresh_cache = FALSE
) {
  region_label <- region_names[region_idx]
  region_val <- as.integer(regions$value[region_idx])

  message(sprintf(
    "\n[Region %d/%d] Processing: %s (ID=%d)",
    region_idx,
    length(region_names),
    region_label,
    region_val
  ))

  # --- Step 1: Create region-specific masked rasters ---
  message("  [1/4] Creating region-specific masked rasters...")

  # Save to temporary files (required for future package compatibility)
  yr1_masked_path <- file.path(
    temp_dir,
    sprintf("yr1_region_%d_%s.tif", region_val, period_name)
  )
  yr2_masked_path <- file.path(
    temp_dir,
    sprintf("yr2_region_%d_%s.tif", region_val, period_name)
  )

  # Check if cached files exist and handle based on refresh_cache
  if (
    !refresh_cache &&
      file.exists(yr1_masked_path) &&
      file.exists(yr2_masked_path)
  ) {
    message(sprintf(
      "        ✓ Using cached region masked rasters for %s",
      region_label
    ))
  } else {
    # Create region mask (cells in region = 1, outside = NA)
    region_mask <- terra::ifel(region_rast == region_val, 1L, NA)

    # Mask the LULC rasters to this region only (outside region = NA)
    yr1_region <- terra::mask(yr1, region_mask)
    yr2_region <- terra::mask(yr2, region_mask)

    # OPTIMIZATION: Trim to bounding box of non-NA cells
    # This reduces file size and all subsequent I/O operations
    # Both rasters share the same NA pattern (region mask), so trimming is safe
    yr1_region <- terra::trim(yr1_region, padding = 0)
    yr2_region <- terra::trim(yr2_region, padding = 0)

    terra::writeRaster(
      yr1_region,
      yr1_masked_path,
      overwrite = TRUE,
      wopt = list(datatype = "INT2U", gdal = c("COMPRESS=LZW"))
    )
    terra::writeRaster(
      yr2_region,
      yr2_masked_path,
      overwrite = TRUE,
      wopt = list(datatype = "INT2U", gdal = c("COMPRESS=LZW"))
    )

    message(sprintf(
      "        ✓ Rasters masked for region %s (cell IDs preserved)",
      region_label
    ))

    # Clean up in-memory rasters
    rm(yr1_region, yr2_region, region_mask)
    gc(verbose = FALSE)
  }

  # --- Step 2: Prepare for transition-specific data loading ---
  message("  [2/4] Preparing transition data access...")
  message(sprintf(
    "        Dataset path: %s (region filter: %d)",
    transitions_dir,
    region_val
  ))

  # Filter transitions to those relevant for this region
  region_transitions <- transitions %>%
    dplyr::filter(
      if (is.character(region)) {
        region == region_label
      } else {
        region == region_val
      }
    )

  message(sprintf(
    "  [3/4] Found %d transitions for this region",
    nrow(region_transitions)
  ))

  # --- Step 3.5: Pre-compute shared cache files IN PARALLEL ---
  message(
    "  [3.5/4] Pre-computing shared cache files (post_class_patches, neighbor_count)..."
  )

  # Get unique destination classes (to_val) for this region
  unique_to_classes <- unique(region_transitions$To.)
  message(sprintf(
    "        Found %d unique destination classes",
    length(unique_to_classes)
  ))

  # Setup parallel backend for cache creation
  n_cores_cache <- min(4, length(unique_to_classes), parallel::detectCores())
  message(sprintf("        Using %d cores for cache creation", n_cores_cache))

  if (n_cores_cache > 1) {
    future::plan(future::multisession, workers = n_cores_cache)
  }

  # Pre-compute cache files in parallel (one worker per destination class)
  furrr::future_walk(
    unique_to_classes,
    .options = furrr::furrr_options(
      seed = TRUE,
      packages = c("terra") # Explicitly load terra in workers
    ),
    function(
      to_val,
      temp_dir,
      region_label,
      period_name,
      yr1_path,
      yr2_path,
      refresh
    ) {
      # Ensure paths are character strings
      yr2_path <- as.character(yr2_path)
      temp_dir <- as.character(temp_dir)

      post_class_patches_path <- file.path(
        temp_dir,
        sprintf(
          "post_class_patches_region_%s_to_%d_%s.tif",
          region_label,
          to_val,
          period_name
        )
      )
      neighbor_count_path <- file.path(
        temp_dir,
        sprintf(
          "neighbor_count_region_%s_to_%d_%s.tif",
          region_label,
          to_val,
          period_name
        )
      )

      # Create if doesn't exist OR if refresh_cache is TRUE
      if (
        refresh ||
          !file.exists(post_class_patches_path) ||
          !file.exists(neighbor_count_path)
      ) {
        # Verify yr2 raster file exists
        if (!file.exists(yr2_path)) {
          stop(sprintf("yr2 masked raster not found: %s", yr2_path))
        }

        # Each worker loads its own copy of the rasters
        yr2_worker <- terra::rast(yr2_path)

        # Create post_class_patches
        post_class_patches <- yr2_worker == to_val
        post_class_patches[post_class_patches == 0] <- NA
        terra::writeRaster(
          post_class_patches,
          post_class_patches_path,
          overwrite = TRUE,
          wopt = list(datatype = "INT1U", gdal = c("COMPRESS=LZW"))
        )

        # Create neighbor_count
        neighbor_count <- terra::focal(
          post_class_patches,
          w = matrix(1, nrow = 3, ncol = 3),
          fun = "sum",
          na.rm = TRUE,
          na.policy = "omit"
        )
        neighbor_count[is.na(neighbor_count)] <- 0
        terra::writeRaster(
          neighbor_count,
          neighbor_count_path,
          overwrite = TRUE,
          wopt = list(datatype = "INT1U", gdal = c("COMPRESS=LZW"))
        )

        rm(post_class_patches, neighbor_count, yr2_worker)
        gc(verbose = FALSE)
      }
    },
    temp_dir = as.character(temp_dir),
    region_label = as.character(region_label),
    period_name = as.character(period_name),
    yr1_path = as.character(yr1_masked_path),
    yr2_path = as.character(yr2_masked_path),
    refresh = refresh_cache
  )

  # Reset to sequential before next parallel section
  future::plan(future::sequential)

  message("        ✓ All cache files created")

  # --- Step 4: Parallelize over transitions WITHIN this region ---
  message("  [4/4] Processing transitions in parallel...")
  message("        (Cache files pre-computed, workers will only read them)")

  results <- process_region_transitions(
    region_transitions = region_transitions,
    transitions_dir = transitions_dir,
    region_val = region_val,
    yr1_masked_path = yr1_masked_path,
    yr2_masked_path = yr2_masked_path,
    region_label = region_label,
    debug_dir = debug_dir,
    period_name = period_name,
    temp_dir = temp_dir,
    config = config,
    refresh_cache = refresh_cache
  )

  # Clean up temporary files
  gc(verbose = FALSE)

  # Clean up temporary raster files
  if (file.exists(yr1_masked_path)) {
    unlink(yr1_masked_path)
  }
  if (file.exists(yr2_masked_path)) {
    unlink(yr2_masked_path)
  }

  message(sprintf(
    "  ✓ Completed region %s (%d transitions processed)\n",
    region_label,
    nrow(region_transitions)
  ))

  # Combine RDS files for all transitions and create CSV
  combine_region_params_to_csv(
    region_label = region_label,
    region_val = region_val,
    period_name = period_name,
    transitions = transitions,
    config = config
  )
}

#' Combine RDS files for a region into a single CSV with all transitions
#' @param region_label label of the region
#' @param region_val numeric region value
#' @param period_name name of the time period
#' @param transitions data frame containing all possible transitions
#' @param config configuration list
combine_region_params_to_csv <- function(
  region_label,
  region_val,
  period_name,
  transitions,
  config
) {
  message(sprintf(
    "  [5/5] Combining transition parameters into CSV for region %s...",
    region_label
  ))

  # Read all RDS files for this region and period
  rds_pattern <- sprintf(
    "allocation_params_region_%s_from_.*_to_.*\\.rds",
    region_label
  )
  rds_dir <- file.path(config[["calibration_param_dir"]], period_name)
  rds_files <- list.files(
    path = rds_dir,
    pattern = rds_pattern,
    full.names = TRUE
  )

  if (length(rds_files) == 0) {
    warning(sprintf(
      "No RDS files found for region %s in period %s",
      region_label,
      period_name
    ))
    return(NULL)
  }

  # Read and combine all RDS files
  observed_params <- lapply(rds_files, readRDS) |>
    dplyr::bind_rows()

  # Create complete transition list with From* and To* values
  all_transitions <- transitions |>
    dplyr::select(`From.`, `To.`) |>
    dplyr::distinct()

  # Left join to get all transitions, filling missing with 0s
  complete_params <- all_transitions |>
    dplyr::left_join(
      observed_params |>
        dplyr::select(-Region),
      by = c("From." = "From*", "To." = "To*")
    ) |>
    dplyr::mutate(
      ` Mean_Patch_Size` = dplyr::coalesce(` Mean_Patch_Size`, 0),
      `Patch_Size_Variance` = dplyr::coalesce(`Patch_Size_Variance`, 0),
      `Patch_Isometry` = dplyr::coalesce(`Patch_Isometry`, 0),
      `Perc_expander` = dplyr::coalesce(`Perc_expander`, 0),
      `Perc_patcher` = dplyr::coalesce(`Perc_patcher`, 0)
    ) |>
    dplyr::rename(
      `From*` = `From.`,
      `To*` = `To.`
    )

  # Create output directory structure: period_name/region_name/
  output_dir <- file.path(
    config[["calibration_param_dir"]],
    period_name,
    region_label
  )
  ensure_dir(output_dir)

  # Save as CSV
  output_file <- file.path(output_dir, "allocation_params.csv")
  readr::write_csv(complete_params, output_file)

  message(sprintf(
    "        ✓ Saved allocation parameters CSV: %s (%d transitions)",
    output_file,
    nrow(complete_params)
  ))

  return(output_file)
}

#' Process all transitions for a single region (parallel)
#' @param region_transitions data frame of transitions for this region
#' @param transitions_dir path to transition parquet dataset
#' @param region_val numeric region filter value
#' @param yr1_masked_path path to yr1 temp raster file
#' @param yr2_masked_path path to yr2 temp raster file
#' @param region_label label of the region
#' @param debug_dir directory for debug/log files
#' @param period_name name of the time period
#' @param temp_dir directory for temporary files
#' @param config configuration list
#' @param refresh_cache if TRUE, overwrite all cached files; if FALSE, reuse existing caches
#' @return results data frame
process_region_transitions <- function(
  region_transitions,
  transitions_dir,
  region_val,
  yr1_masked_path,
  yr2_masked_path,
  region_label,
  debug_dir,
  period_name,
  temp_dir,
  config,
  refresh_cache = FALSE
) {
  # Set up parallel processing
  # Use number of cores from SLURM or default to half of available cores
  n_cores <- as.integer(Sys.getenv(
    "SLURM_CPUS_PER_TASK",
    parallel::detectCores() / 2
  ))

  # Determine parallel strategy
  if (n_cores > 1) {
    # ALWAYS use multisession (even on Unix/HPC)
    # multicore uses forking which can cause OOM issues on HPC clusters
    # multisession creates separate R sessions with isolated memory
    future::plan(future::multisession, workers = n_cores)
    strategy <- "multisession"

    message(sprintf(
      "    ✓ Parallel processing ENABLED: %d workers using %s strategy",
      n_cores,
      strategy
    ))
    message(
      "      (Using multisession for HPC compatibility and memory isolation)"
    )
  } else {
    future::plan(future::sequential)
    message(
      "    ⚠ Parallel processing DISABLED: Running sequentially (n_cores=1)"
    )
  }

  # Verify the actual plan
  current_plan <- class(future::plan())[1]
  message(sprintf("    Current future plan: %s", current_plan))

  # Create worker_logs directory
  worker_log_dir <- file.path(debug_dir, "worker_logs")

  results <- furrr::future_map_dfr(
    seq_len(nrow(region_transitions)),
    .options = furrr::furrr_options(
      seed = TRUE,
      packages = c("terra", "dplyr") # Explicitly load packages in workers
    ),
    function(
      i,
      yr1_path,
      yr2_path,
      region_label,
      transitions,
      log_dir,
      period_name,
      trans_dir,
      region_val,
      temp_dir_path,
      cfg
    ) {
      # Ensure paths are character strings (not factors or other types)
      yr1_path <- as.character(yr1_path)
      yr2_path <- as.character(yr2_path)

      # Initialize trans_name and log file BEFORE error handling
      trans_name <- paste0(
        transitions[["Initial_class"]][i],
        "-",
        transitions[["Final_class"]][i]
      )

      log_file <- initialize_worker_log(
        log_dir,
        paste0(region_label, "_", trans_name)
      )

      # Wrap in tryCatch for better error reporting
      tryCatch(
        {
          calculate_single_transition_params(
            i = i,
            region_transitions = transitions,
            transitions_dir = trans_dir,
            region_val = region_val,
            trans_name = trans_name,
            yr1_masked_path = yr1_path,
            yr2_masked_path = yr2_path,
            region_label = region_label,
            log_file = log_file,
            period_name = period_name,
            temp_dir = temp_dir_path,
            config = cfg,
            refresh_cache = refresh
          )
        },
        error = function(e) {
          err_msg <- sprintf(
            "ERROR in worker processing transition %d (%s): %s\n%s",
            i,
            trans_name,
            e$message,
            paste(capture.output(traceback()), collapse = "\n")
          )
          cat(err_msg, file = stderr())

          # Try to write error to log file
          tryCatch(
            cat(err_msg, file = log_file, append = TRUE),
            error = function(e2) NULL
          )

          # Re-throw with more context
          stop(sprintf(
            "Worker %d failed for transition %s: %s",
            i,
            trans_name,
            e$message
          ))
        }
      )
    },
    yr1_path = as.character(yr1_masked_path),
    yr2_path = as.character(yr2_masked_path),
    region_label = as.character(region_label),
    transitions = region_transitions,
    log_dir = as.character(worker_log_dir),
    period_name = as.character(period_name),
    trans_dir = as.character(transitions_dir),
    region_val = region_val,
    temp_dir_path = as.character(temp_dir),
    cfg = config,
    refresh = refresh_cache
  )

  # Reset to sequential plan after parallel processing
  future::plan(future::sequential)

  return(results)
}

#' Calculate allocation parameters for a single transition
#' @param i index of transition
#' @param region_transitions data frame of transitions
#' @param transitions_dir path to transition parquet dataset
#' @param region_val numeric region filter value
#' @param trans_name name of the transition column
#' @param yr1_path path to yr1 raster file
#' @param yr2_path path to yr2 raster file
#' @param region_label label of the region
#' @param log_file path to log file for this worker
#' @param period_name name of the time period
#' @param temp_dir directory for temporary files
#' @param config configuration list
#' @param refresh_cache if TRUE, overwrite all cached files; if FALSE, reuse existing caches
calculate_single_transition_params <- function(
  i,
  region_transitions,
  transitions_dir,
  region_val,
  trans_name,
  yr1_masked_path,
  yr2_masked_path,
  region_label,
  log_file = NULL,
  period_name,
  temp_dir,
  config,
  refresh_cache = FALSE
) {
  from_val <- region_transitions[["From."]][i]
  to_val <- region_transitions[["To."]][i]
  from_class <- region_transitions[["Initial_class"]][i]
  to_class <- region_transitions[["Final_class"]][i]

  log_msg(
    sprintf(
      "Processing transition: %s (region: %s, from: %d, to: %d)",
      trans_name,
      region_label,
      from_val,
      to_val
    ),
    log_file
  )

  # Verify file paths are valid before attempting to load
  if (!file.exists(yr1_masked_path)) {
    stop(sprintf("yr1 masked raster not found: %s", yr1_masked_path))
  }
  if (!file.exists(yr2_masked_path)) {
    stop(sprintf("yr2 masked raster not found: %s", yr2_masked_path))
  }

  # Load rasters for the two years
  lulc_ant <- terra::rast(yr1_masked_path)
  lulc_post <- terra::rast(yr2_masked_path)

  # --- Begin inlined compute_alloc_params_single logic with caching ---
  # Create binary raster of transition cells (anterior class -> posterior class)
  trans_cells <- (lulc_ant == from_val) & (lulc_post == to_val)
  trans_cells[trans_cells == 0] <- NA

  # Keep lulc_post for neighbor analysis (need to see final state)
  rm(lulc_ant)
  gc(verbose = FALSE)

  # Count total transition cells
  n_trans_cells_result <- terra::global(trans_cells, "sum", na.rm = TRUE)
  n_trans_cells <- as.numeric(n_trans_cells_result[1, 1])

  if (is.na(n_trans_cells) || n_trans_cells == 0) {
    alloc_params <- list(
      mean_patch_size = 0,
      patch_size_variance = 0,
      patch_isometry = 0,
      frac_expander = 0,
      frac_patcher = 0
    )
  } else {
    # Caching paths for intermediates (region-specific)
    post_class_patches_path <- file.path(
      temp_dir,
      sprintf(
        "post_class_patches_region_%s_to_%d_%s.tif",
        region_label,
        to_val,
        period_name
      )
    )
    neighbor_count_path <- file.path(
      temp_dir,
      sprintf(
        "neighbor_count_region_%s_to_%d_%s.tif",
        region_label,
        to_val,
        period_name
      )
    )

    # Load pre-computed cache files (created sequentially before parallel processing)
    # These files are guaranteed to exist and are read-only, so no race conditions
    post_class_patches <- terra::rast(post_class_patches_path)
    neighbor_count <- terra::rast(neighbor_count_path)

    # Ensure NA values are replaced (defensive, should already be done in cache)
    neighbor_count[is.na(neighbor_count)] <- 0

    # Subtract 1 from neighbor_count at transition cells
    # because focal sum includes the cell itself (center of 3x3 window)
    # We want to know if there are OTHER cells of the destination class nearby
    neighbor_count_at_trans <- neighbor_count * trans_cells
    neighbor_count_at_trans[!is.na(neighbor_count_at_trans)] <-
      neighbor_count_at_trans[!is.na(neighbor_count_at_trans)] - 1

    # Classify transition cells as expanders or patchers
    # Expander: has at least 1 OTHER cell of destination class as neighbor (neighbor_count >= 1 after subtracting self)
    # Patcher: has no other cells of destination class as neighbors (neighbor_count == 0 after subtracting self)
    is_expander <- (trans_cells == 1) & (neighbor_count_at_trans >= 1)
    is_patcher <- (trans_cells == 1) & (neighbor_count_at_trans == 0)

    # Free memory from cached rasters and lulc_post
    rm(post_class_patches, neighbor_count, neighbor_count_at_trans, lulc_post)
    gc(verbose = FALSE) # Calculate percentages
    n_expanders_result <- terra::global(is_expander, "sum", na.rm = TRUE)
    n_expanders <- as.numeric(n_expanders_result[1, 1])
    n_patchers_result <- terra::global(is_patcher, "sum", na.rm = TRUE)
    n_patchers <- as.numeric(n_patchers_result[1, 1])

    # Handle NA results from global (shouldn't happen after neighbor_count NA fix, but defensive)
    n_expanders <- ifelse(is.na(n_expanders), 0, n_expanders)
    n_patchers <- ifelse(is.na(n_patchers), 0, n_patchers)

    frac_expander <- n_expanders / n_trans_cells
    frac_patcher <- n_patchers / n_trans_cells

    # Free memory from boolean rasters
    rm(is_expander, is_patcher)
    gc(verbose = FALSE)

    # Calculate patch statistics using internal cpp function
    # Get actual cell area in square meters (handles both projected and unprojected data)
    # For unprojected data, this accounts for latitude-dependent cell size
    cell_area_m2 <- terra::cellSize(trans_cells, unit = "m")[1]
    cellsize <- sqrt(cell_area_m2)[[1]] # Convert area to linear dimension for C++ function

    # Trim to bounding box of transition cells before matrix conversion
    # Note: lulc_ant/lulc_post were already trimmed to region extent,
    # but trans_cells may be even sparser (only cells that actually transitioned)
    trans_cells_cropped <- terra::trim(trans_cells, padding = 0)

    # Convert to matrix for C++ processing
    trans_cells_mat <- terra::as.matrix(trans_cells_cropped, wide = TRUE)
    storage.mode(trans_cells_mat) <- "integer"

    # Free memory from rasters before processing matrix
    rm(trans_cells, trans_cells_cropped)
    gc(verbose = FALSE)

    cl.data <- calculate_class_stats_cpp(trans_cells_mat, cellsize = cellsize)

    # Free memory from matrix after processing
    rm(trans_cells_mat)
    gc(verbose = FALSE)

    # Convert patch areas from m² to number of cells
    # C++ function returns areas in m² (cellsize² × number of cells)
    # Divide by cell area to get back to number of cells
    cell_area_m2_value <- cell_area_m2[[1]]

    mpa <- if (!is.null(cl.data) && nrow(cl.data) > 0) {
      cl.data$mean.patch.area[1] / cell_area_m2_value
    } else {
      0
    }
    sda <- if (!is.null(cl.data) && nrow(cl.data) > 0) {
      cl.data$sd.patch.area[1] / cell_area_m2_value
    } else {
      0
    }
    iso <- if (
      !is.null(cl.data) &&
        nrow(cl.data) > 0 &&
        !is.na(cl.data$aggregation.index[1])
    ) {
      cl.data$aggregation.index[1] / 70
    } else {
      0
    }

    alloc_params <- list(
      mean_patch_size = mpa,
      patch_size_variance = sda,
      patch_isometry = iso,
      frac_expander = frac_expander,
      frac_patcher = frac_patcher
    )
  }

  # Logging
  log_msg(
    sprintf(
      "  Mean patch size: %.2f cells, SD: %.2f cells, Isometry: %.4f, Expander: %.1f%%, Patcher: %.1f%%",
      alloc_params$mean_patch_size,
      alloc_params$patch_size_variance,
      alloc_params$patch_isometry,
      100 * alloc_params$frac_expander,
      100 * alloc_params$frac_patcher
    ),
    log_file
  )

  log_msg(
    sprintf(
      "✓ Completed transition %s (region %s)",
      trans_name,
      region_label
    ),
    log_file
  )

  transition_params <- tibble::tibble_row(
    Region = region_label,
    "From*" = from_val,
    "To*" = to_val,
    " Mean_Patch_Size" = alloc_params$mean_patch_size,
    "Patch_Size_Variance" = alloc_params$patch_size_variance,
    "Patch_Isometry" = alloc_params$patch_isometry,
    "Perc_expander" = 100 * alloc_params$frac_expander,
    "Perc_patcher" = 100 * alloc_params$frac_patcher
  )

  # save as rds
  trans_param_path <- file.path(
    config[["calibration_param_dir"]],
    period_name,
    sprintf(
      "allocation_params_region_%s_from_%d_to_%d.rds",
      region_label,
      from_val,
      to_val
    )
  )
  saveRDS(
    transition_params,
    file = trans_param_path
  )
  log_msg(
    sprintf(
      "  Allocation parameters saved to: %s",
      trans_param_path
    ),
    log_file
  )
}
