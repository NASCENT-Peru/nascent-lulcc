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
  config = get_config()
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
  ensure_dir(file.path(config[["calibration_param_dir"]], "periodic"))

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
    MoreArgs = list(lulc_rasters = lulc_rasters),
    SIMPLIFY = FALSE
  )

  #### B - Creating patch size parameter tables for calibration ####

  # Whilst we have estimated values of percentage of transitions corresponding to
  # expansion of existing patches vs. occurring in new patches, we don't know how
  # accurate these are so it is desirable to perform calibration by monte-carlo
  # simulation using random permutations of these values

  # IMPORTANT
  # For Dinamica the % expansion values must be expressed as decimals
  # so they are converted in this loop

  # First create a lookup table to control looping over the simulations
  calibration_control_table <- data.frame(matrix(ncol = 11, nrow = 0))
  colnames(calibration_control_table) <- c(
    "simulation_num.",
    "scenario_id.string",
    "simulation_id.string",
    "model_mode.string",
    "scenario_start.real",
    "scenario_end.real",
    "step_length.real",
    "parallel_tpc.string",
    "spatial_interventions.string",
    "deterministic_trans.string",
    "completed.string"
  )

  # reload allocation parameter tables
  Allocation_params_by_period <- lapply(
    list.files(
      file.path(config[["calibration_param_dir"]], "periodic"),
      full.names = TRUE
    ),
    read.csv
  )
  names(Allocation_params_by_period) <- config[["data_periods"]]

  # reloading also causes r to mess up the column names, readjust
  # Adjust col names
  Allocation_params_by_period <- lapply(
    Allocation_params_by_period,
    function(params_period) {
      colnames(params_period) <- c(
        "From*",
        "To*",
        " Mean_Patch_Size",
        "Patch_Size_Variance",
        "Patch_Isometry",
        "Perc_expander",
        "Perc_patcher"
      )
      params_period$Perc_expander <- params_period$Perc_expander / 100
      params_period$Perc_patcher <- params_period$Perc_patcher / 100
      params_period
    }
  )

  # because we are most interested in calibrating the patch size values for the
  # time period that will be used in simulations we will run the calibration using
  # the params from the most recent data period
  # we have initially agreed to use 5 year time steps
  scenario_start <- 2010
  scenario_end <- 2020
  step_length <- 5

  # vector sequence of time points and suffix
  Time_steps <- seq(scenario_start, scenario_end, step_length)

  # seperate vector of time points into those relevant for each calibration period
  Time_points_by_period <- lapply(config[["data_periods"]], function(period) {
    dates <- as.numeric(stringr::str_split(period, "_")[[1]])
    Time_steps[
      sapply(Time_steps, function(year) {
        if (year > dates[1] & year <= dates[2]) {
          TRUE
        } else if ((scenario_end - dates[2]) < step_length) {
          TRUE
        } else {
          FALSE
        }
      })
    ]
  })
  names(Time_points_by_period) <- config[["data_periods"]]

  # remove any time periods that are empty
  Time_points_by_period <- Time_points_by_period[
    lapply(Time_points_by_period, length) > 0
  ]

  # subset the list of allocation params tables by the names of the time_periods
  Allocation_params_by_period <- Allocation_params_by_period[names(
    Time_points_by_period
  )]

  # create seperate files of the estimated allocation parameters for each time point
  # under the ID: v1

  # loop over the list of years for each time point saving a copy of the
  # corresponding parameter table foreach one
  ensure_dir(file.path(config[["calibration_param_dir"]], "v1"))
  sapply(seq_along(Time_points_by_period), function(period_indices) {
    sapply(Time_points_by_period[[period_indices]], function(x) {
      file_name <- file.path(
        config[["calibration_param_dir"]],
        "v1",
        paste0(
          "allocation_param_table_",
          x,
          ".csv"
        )
      )
      readr::write_csv(
        Allocation_params_by_period[[period_indices]],
        file = file_name
      )
    })
  })

  # create a sequence of names for the number of monte-carlo simulations
  mc_sims <- sapply(seq(2, 100, 1), function(x) paste0("v", x))

  # loop over the mc_sim names and perturb the allocation params
  # saving a table for every time point in the calibration period
  sapply(mc_sims, function(Sim_name) {
    # inner loop over time periods and parameter tables
    mapply(
      function(Time_steps, calibration_param_dir, param_table) {
        # create folder for saving param tables for MC sim name
        dir.create(file.path(calibration_param_dir, Sim_name), recursive = TRUE)

        # random perturbation of % expander
        # (increase of decrease value by random amount in normal distribution with mean
        # = 0 and sd = 0.05 effectively 5% bounding)
        param_table$Perc_expander <-
          param_table$Perc_expander +
          rnorm(length(param_table$Perc_expander), mean = 0, sd = 0.05)

        # if any values are greater than 1 or less than 0 then set to these values
        param_table$Perc_expander <- sapply(
          param_table$Perc_expander,
          function(x) {
            if (x > 1) {
              x <- 1
            } else if (x < 0) {
              0
            } else {
              x
            }
          }
        )

        # recalculate % patcher so that total does not exceed 1 (100%)
        param_table$Perc_patcher <- 1 - param_table$Perc_expander

        # inner loop over individual time points
        sapply(Time_steps, function(x) {
          file_name <- file.path(
            calibration_param_dir,
            Sim_name,
            paste0("allocation_param_table_", x, ".csv")
          )
          readr::write_csv(param_table, file = file_name)
        }) # close loop over time points
      },
      Time_steps = Time_points_by_period,
      calibration_param_dir = config[["calibration_param_dir"]],
      param_table = Allocation_params_by_period,
      SIMPLIFY = FALSE
    ) # close loop over time periods
  }) # close loop over simulation IDs.

  # Now add entries for these MC simulations into the calibration control table
  # add v1 to mc_sims
  mc_sims <- c("v1", mc_sims)

  # add rows for MC sim_names
  for (i in seq_along(mc_sims)) {
    calibration_control_table[i, "simulation_id.string"] <- mc_sims[i]
    calibration_control_table[i, "simulation_num."] <- i
  }

  # fill in remaining columns
  calibration_control_table$scenario_id.string <- "CALIBRATION"
  calibration_control_table$scenario_start.real <- scenario_start
  calibration_control_table$scenario_end.real <- scenario_end
  calibration_control_table$step_length.real <- step_length
  calibration_control_table$model_mode.string <- "Calibration"
  calibration_control_table$parallel_tpc.string <- "N"
  calibration_control_table$completed.string <- "N"
  calibration_control_table$spatial_interventions.string <- "N"
  calibration_control_table$deterministic_trans.string <- "N"

  # save table
  readr::write_csv(
    calibration_control_table,
    config[["calibration_ctrl_tbl_path"]]
  )

  #### C - Perform lulc simulations for calibration ####
  work_dir <- Sys.getenv("EVOLAND_CALIBRATION_DIR", unset = "calibration")
  run_evoland_dinamica_sim(
    calibration = TRUE,
    work_dir = work_dir
  )

  # because the simulations may fail without the system command returning an error
  # (if the error occurs in Dinamica) then check the control table to see
  # if/how many simulations have failed
  updated_control_tbl <- read.csv(
    fs::path(work_dir, default_ctrl_tbl_path())
  )

  if (errs <- sum(updated_control_tbl$completed.string == "ERROR")) {
    stop(
      sum(errs),
      " of ",
      nrow(updated_control_tbl),
      " simulations have failed to run till completion, go check the logs."
    )
  }

  #### D - Evaluate calibration, selecting best parameter set ####

  # load the similarity values produced from the validation process inside Dinamica
  # for each simulation
  calibration_results <-
    fs::dir_ls(
      path = work_dir,
      glob = "**/similarity_value*.csv", # grandparent folder is simulation_id
      recurse = TRUE
    ) |>
    rlang::set_names(\(x) stringr::str_split_i(x, "/", -3)) |> # get simulation_id from folder name
    purrr::map(
      readr::read_csv, # these files are actually just a float without header
      col_names = "similarity_value",
      col_types = "d"
    ) |>
    dplyr::bind_rows(.id = "simulation_id") |>
    dplyr::filter(simulation_id != "summary")

  # summary statistics
  calibration_summary <- data.frame(
    Mean = mean(calibration_results[["similarity_value"]]),
    `Standard Deviation` = sd(calibration_results[["similarity_value"]]),
    Minimum = min(calibration_results[["similarity_value"]]),
    Maximum = max(calibration_results[["similarity_value"]]),
    n = length(calibration_results[["similarity_value"]])
  )

  # save summary statistics
  readr::write_csv(
    calibration_summary,
    file.path(work_dir, "validation_summary.csv")
  )

  # select best performing simulation_ID
  best_sim_ID <- calibration_results[
    which.max(calibration_results[["similarity_value"]]),
  ][["simulation_id"]]

  # Use this sim ID to create parameter tables for all simulation time points
  # in the Simulation folder

  # get exemplar table
  param_table <- read.csv(
    list.files(
      file.path(config[["calibration_param_dir"]], best_sim_ID),
      full.names = TRUE,
      pattern = "2020"
    )
  )
  colnames(param_table) <- c(
    "From*",
    "To*",
    " Mean_Patch_Size",
    "Patch_Size_Variance",
    "Patch_Isometry",
    "Perc_expander",
    "Perc_patcher"
  )

  # get simulation start and end times from control table
  simulation_control <- readr::read_csv(config[["ctrl_tbl_path"]])
  simulation_start <- min(simulation_control$scenario_start.real)
  simulation_end <- max(simulation_control$scenario_end.real)
  scenario_IDs <- unique(simulation_control$scenario_id.string)

  # loop over scenario IDs and simulation time points creating allocation param tables
  sapply(
    scenario_IDs,
    function(scenario_id) {
      sapply(
        seq(simulation_start, simulation_end, step_length),
        function(simulation_year) {
          save_dir <- file.path(config[["simulation_param_dir"]], scenario_id)
          ensure_dir(save_dir)
          file_name <- file.path(
            save_dir,
            paste0("allocation_param_table_", simulation_year, ".csv")
          )
          readr::write_csv(param_table, file = file_name)
        }
      )
    }
  )
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
  temp_dir = config[["temp_dir"]]
) {
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
  yr1 <- lulc_rasters[[grep(comparison_years[1], names(lulc_rasters))]]
  yr2 <- lulc_rasters[[grep(comparison_years[2], names(lulc_rasters))]]
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

  results <- process_all_regions(
    region_names = region_names,
    regions = regions,
    yr1 = yr1,
    yr2 = yr2,
    region_rast = region_rast,
    transitions = transitions,
    transitions_dir = transitions_dir,
    period_name = period_name,
    temp_dir = temp_dir,
    debug_dir = debug_dir
  )

  message("\n✓ COMPLETED ALLOCATION PARAMETER CALCULATION")
  message(paste(rep("=", 80), collapse = ""))

  # Save results
  readr::write_csv(
    results,
    file = file.path(
      config[["calibration_param_dir"]],
      "periodic",
      paste0("allocation_parameters_", period_name, ".csv")
    )
  )
  message(sprintf(
    "Results saved to: allocation_parameters_%s.csv",
    period_name
  ))

  return(results)
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
#' @return combined results from all regions
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
  debug_dir
) {
  results <- lapply(seq_along(region_names), function(region_idx) {
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
      debug_dir = debug_dir
    )
  }) %>%
    dplyr::bind_rows()

  return(results)
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
#' @return results data frame for this region
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
  debug_dir
) {
  region_label <- region_names[region_idx]
  region_filter_val <- as.integer(regions$value[region_idx])

  message(sprintf(
    "\n[Region %d/%d] Processing: %s (ID=%d)",
    region_idx,
    length(region_names),
    region_label,
    region_filter_val
  ))

  # --- Step 1: Create region-specific masked rasters ---
  message("  [1/4] Creating region-specific masked rasters...")

  # Create region mask (cells in region = 1, outside = NA)
  region_mask <- terra::ifel(region_rast == region_filter_val, 1L, NA)

  # Mask the LULC rasters to this region only (outside region = NA)
  # Note: We don't crop the extent - this keeps cell IDs consistent with
  # the parquet data, eliminating the need for cell ID mapping
  yr1_region <- terra::mask(yr1, region_mask)
  yr2_region <- terra::mask(yr2, region_mask)

  # Save to temporary files (required for future package compatibility)
  yr1_temp_path <- file.path(
    temp_dir,
    sprintf("yr1_region_%d_%s.tif", region_filter_val, period_name)
  )
  yr2_temp_path <- file.path(
    temp_dir,
    sprintf("yr2_region_%d_%s.tif", region_filter_val, period_name)
  )

  terra::writeRaster(
    yr1_region,
    yr1_temp_path,
    overwrite = TRUE,
    wopt = list(datatype = "INT2U", gdal = c("COMPRESS=LZW"))
  )
  terra::writeRaster(
    yr2_region,
    yr2_temp_path,
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

  # --- Step 2: Prepare for transition-specific data loading ---
  message("  [2/4] Preparing transition data access...")
  message(sprintf(
    "        Dataset path: %s (region filter: %d)",
    transitions_dir,
    region_filter_val
  ))

  # Filter transitions to those relevant for this region
  region_transitions <- transitions %>%
    dplyr::filter(
      if (is.character(region)) {
        region == region_label
      } else {
        region == region_filter_val
      }
    )

  message(sprintf(
    "  [3/4] Found %d transitions for this region",
    nrow(region_transitions)
  ))

  # --- Step 3: Parallelize over transitions WITHIN this region ---
  message("  [4/4] Processing transitions in parallel...")
  message("        (Each worker loads only its specific transition data)")
  region_results <- process_region_transitions(
    region_transitions = region_transitions,
    transitions_dir = transitions_dir,
    region_filter_val = region_filter_val,
    yr1_temp_path = yr1_temp_path,
    yr2_temp_path = yr2_temp_path,
    region_label = region_label,
    debug_dir = debug_dir,
    period_name = period_name
  )

  # Clean up temporary files
  gc(verbose = FALSE)

  # Clean up temporary raster files
  if (file.exists(yr1_temp_path)) {
    unlink(yr1_temp_path)
  }
  if (file.exists(yr2_temp_path)) {
    unlink(yr2_temp_path)
  }

  message(sprintf(
    "  ✓ Completed region %s (%d transitions processed)\n",
    region_label,
    nrow(region_transitions)
  ))

  return(region_results)
}

#' Process all transitions for a single region (parallel)
#' @param region_transitions data frame of transitions for this region
#' @param transitions_dir path to transition parquet dataset
#' @param region_filter_val numeric region filter value
#' @param yr1_temp_path path to yr1 temp raster file
#' @param yr2_temp_path path to yr2 temp raster file
#' @param region_label label of the region
#' @param debug_dir directory for debug/log files
#' @param period_name name of the time period
#' @return results data frame
process_region_transitions <- function(
  region_transitions,
  transitions_dir,
  region_filter_val,
  yr1_temp_path,
  yr2_temp_path,
  region_label,
  debug_dir,
  period_name
) {
  furrr::future_map_dfr(
    seq_len(nrow(region_transitions)),
    .options = furrr::furrr_options(seed = TRUE),
    function(
      i,
      yr1_path,
      yr2_path,
      region_label,
      transitions,
      debug_dir_path,
      period_name,
      trans_dir,
      region_val
    ) {
      # Create log file for this transition (inside parallel worker)
      trans_name <- paste0(
        transitions[["Initial_class"]][i],
        "-",
        transitions[["Final_class"]][i]
      )
      log_file <- file.path(
        debug_dir_path,
        sprintf("log_%s_%s.txt", region_label, trans_name)
      )

      calculate_single_transition_params(
        i = i,
        region_transitions = transitions,
        transitions_dir = trans_dir,
        region_filter_val = region_val,
        trans_name = trans_name,
        yr1_path = yr1_path,
        yr2_path = yr2_path,
        region_label = region_label,
        log_file = log_file,
        period_name = period_name
      )
    },
    yr1_path = yr1_temp_path,
    yr2_path = yr2_temp_path,
    region_label = region_label,
    transitions = region_transitions,
    debug_dir_path = debug_dir,
    period_name = period_name,
    trans_dir = transitions_dir,
    region_val = region_filter_val
  )
}

#' Calculate allocation parameters for a single transition
#' @param i index of transition
#' @param region_transitions data frame of transitions
#' @param transitions_dir path to transition parquet dataset
#' @param region_filter_val numeric region filter value
#' @param trans_name name of the transition column
#' @param yr1_path path to yr1 raster file
#' @param yr2_path path to yr2 raster file
#' @param region_label label of the region
#' @param log_file path to log file for this worker
#' @param period_name name of the time period
#' @return tibble row with allocation parameters
calculate_single_transition_params <- function(
  i,
  region_transitions,
  transitions_dir,
  region_filter_val,
  trans_name,
  yr1_path,
  yr2_path,
  region_label,
  log_file = NULL,
  period_name
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

  # Load ONLY this specific transition's data (huge memory savings!)
  # Only select: cell_id + this transition column
  # Only filter: region + transition value == 1
  log_msg(
    sprintf(
      "  Loading transition data for %s (only cells where transition occurred)...",
      trans_name
    ),
    log_file
  )

  region_transition_cells <- tryCatch(
    {
      arrow::open_dataset(
        transitions_dir,
        partitioning = arrow::hive_partition(region = arrow::int32())
      ) %>%
        dplyr::filter(region == region_filter_val) %>%
        dplyr::select(cell_id, !!trans_name) %>%
        dplyr::filter(!!rlang::sym(trans_name) == 1) %>%
        dplyr::pull(cell_id) %>%
        unique()
    },
    error = function(e) {
      log_msg(
        sprintf("  ERROR loading transition data: %s", e$message),
        log_file
      )
      integer(0) # Return empty vector on error
    }
  )

  log_msg(
    sprintf(
      "  Found %d transition cells ",
      length(region_transition_cells)
    ),
    log_file
  )

  # Load region-specific rasters inside parallel worker
  # (terra SpatRaster objects are not exportable with future)
  log_msg("  Loading masked rasters...", log_file)
  yr1_region <- terra::rast(yr1_path)
  yr2_region <- terra::rast(yr2_path)

  # Create binary classification rasters efficiently using terra::app()
  # This skips NA cells entirely (much faster than nested ifel)
  # bin1: 1 where yr1 == to_val, 0 elsewhere (within region, NA outside)
  # bin2: 1 where yr2 == to_val, 0 elsewhere (within region, NA outside)
  log_msg("  Creating binary rasters...", log_file)
  bin1 <- terra::app(
    yr1_region,
    fun = function(x) {
      ifelse(x == to_val, 1L, 0L)
    }
  )

  bin2 <- terra::app(
    yr2_region,
    fun = function(x) {
      ifelse(x == to_val, 1L, 0L)
    }
  )

  # identify patches, then reduce to binary indicators for adjacency logic
  log_msg("  Identifying patches...", log_file)
  cl1 <- terra::patches(bin1, directions = 8, zeroAsNA = TRUE)
  cl2 <- terra::patches(bin2, directions = 8, zeroAsNA = TRUE)
  cl1b <- cl1
  cl2b <- cl2
  cl1b[cl1b > 0] <- 1
  cl1b[is.na(cl1b[])] <- 0
  cl2b[cl2b > 0] <- 1
  cl2b[is.na(cl2b[])] <- 0

  # create combined code as in original logic: 0,1,10,11 by using 1 and 10
  cl2b10 <- cl2b
  cl2b10[cl2b10 > 0] <- 10
  yr1_yr2_patches <- cl1b + cl2b10

  # Extract values at region transition cells and determine which are 'new patch' (10)
  trans_vals <- terra::values(yr1_yr2_patches)[
    region_transition_cells,
    ,
    drop = TRUE
  ]
  new_patch_cells <- region_transition_cells[which(trans_vals == 10)]

  # adjacency check: for each new_patch cell test if any neighbour is in cl1b (old patch)
  log_msg(
    sprintf(
      "  Calculating expander/patcher percentages (%d new patches)...",
      length(new_patch_cells)
    ),
    log_file
  )
  n_expansion <- 0L
  if (length(new_patch_cells) > 0) {
    for (cell in new_patch_cells) {
      nc <- terra::adjacent(
        yr1_yr2_patches,
        cells = cell,
        directions = 8,
        pairs = FALSE
      )
      if (length(nc) > 0 && any(cl1b[nc] == 1, na.rm = TRUE)) {
        n_expansion <- n_expansion + 1L
      }
    }
  }
  total_trans <- length(region_transition_cells)
  perc_expander <- (n_expansion / total_trans) * 100
  perc_patcher <- ((total_trans - n_expansion) / total_trans) * 100

  log_msg(
    sprintf(
      "  Expander: %.1f%%, Patcher: %.1f%%",
      perc_expander,
      perc_patcher
    ),
    log_file
  )

  # Patch size metrics for the final class in yr2 (restricted to region)
  # Use clumps from cl2 (patch ids) and compute areas
  log_msg("  Calculating patch size metrics...", log_file)
  f <- terra::freq(cl2)
  if (is.null(f) || nrow(f) == 0) {
    mpa <- NA_real_
    sda <- NA_real_
    log_msg("  Warning: No patches found for patch size calculation", log_file)
  } else {
    fdf <- as.data.frame(f)
    # Filter out NA values if present
    fdf <- fdf[!is.na(fdf$value), ]
    cellsize <- terra::res(cl2)[1]
    areas_ha <- (fdf$count * (cellsize * cellsize)) / 10000
    mpa <- mean(areas_ha, na.rm = TRUE)
    sda <- stats::sd(areas_ha, na.rm = TRUE)
    log_msg(
      sprintf(
        "  Mean patch size: %.2f ha, SD: %.2f ha",
        mpa,
        sda
      ),
      log_file
    )
  }

  # Aggregation index via landscapemetrics (requires categorical raster)
  # Create mask raster with to_val where present, NA elsewhere
  log_msg("  Calculating aggregation index...", log_file)
  mask_r <- terra::app(
    yr2_region,
    fun = function(x) {
      ifelse(x == to_val, as.integer(to_val), NA_integer_)
    },
    na.rm = TRUE # Skip NA cells
  )
  ai_val <- tryCatch(
    {
      ai_df <- landscapemetrics::lsm_c_ai(mask_r)
      if (inherits(ai_df, "data.frame") && nrow(ai_df) > 0) {
        ai_df$value[1]
      } else {
        NA_real_
      }
    },
    error = function(e) {
      log_msg(
        sprintf("  Warning: Error calculating AI: %s", e$message),
        log_file
      )
      NA_real_
    }
  )
  # Patch isometry — map to previous scale: original code divided aggregation.index by 70
  iso <- if (is.na(ai_val)) NA_real_ else ai_val / 70

  log_msg(
    sprintf(
      "  Aggregation index: %.2f, Isometry: %.4f",
      if (is.na(ai_val)) -999 else ai_val,
      if (is.na(iso)) -999 else iso
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
    " Mean_Patch_Size" = mpa,
    "Patch_Size_Variance" = sda,
    "Patch_Isometry" = iso,
    "Perc_expander" = perc_expander,
    "Perc_patcher" = perc_patcher
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
  return(transition_params)
}
