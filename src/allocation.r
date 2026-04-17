#' Allocation Step: LULC Simulation via Dinamica EGO
#'
#' Orchestrates the allocation of land use/land cover transitions using
#' Dinamica EGO, processing multiple scenarios, timesteps, and regions.
#'
#' @author Ben Black

#' Top-level entry point for the allocation step
#'
#' @param config Configuration list from get_config()
#' @return NULL (called for side effects)
run_allocation <- function(config = get_config()) {
  message("\n========================================")
  message("Starting LULC Allocation Simulations")
  message("========================================\n")

  # Load regions
  regions <- jsonlite::fromJSON(file.path(config[["reg_dir"]], "regions.json"))
  region_rast <- terra::rast(list.files(
    config[["reg_dir"]],
    pattern = "regions.tif$",
    full.names = TRUE
  ))

  for (scenario in config[["scenario_names"]]) {
    t_start <- proc.time()
    message(sprintf("\n--- Scenario: %s ---", scenario))

    run_allocation_for_scenario(
      scenario = scenario,
      regions = regions,
      region_rast = region_rast,
      config = config
    )

    elapsed <- (proc.time() - t_start)[["elapsed"]]
    message(sprintf(
      "Scenario %s completed in %.1f minutes",
      scenario,
      elapsed / 60
    ))
  }

  message("\n========================================")
  message("All allocation simulations complete")
  message("========================================\n")
}


#' Run allocation for a single scenario across all timesteps
#'
#' @param scenario Scenario name (e.g., "BAU")
#' @param regions Regions data frame with value and label columns
#' @param region_rast Region raster
#' @param config Configuration list
#' @return NULL (called for side effects)
run_allocation_for_scenario <- function(
  scenario,
  regions,
  region_rast,
  config
) {
  sim_dir <- file.path(config[["simulation_output_dir"]], scenario)
  ensure_dir(sim_dir)

  # Load initial LULC raster for the simulation start year
  start_year <- config[["simulation_start_year"]]
  end_year <- config[["simulation_end_year"]]
  step_length <- config[["step_length"]]

  lulc_files <- list.files(
    config[["aggregated_lulc_dir"]],
    pattern = "\\.tif$",
    full.names = TRUE
  )
  initial_lulc_file <- lulc_files[grepl(as.character(start_year), lulc_files)]
  if (length(initial_lulc_file) == 0) {
    stop(sprintf(
      "No LULC raster found for start year %d in %s",
      start_year,
      config[["aggregated_lulc_dir"]]
    ))
  }

  current_lulc_path <- initial_lulc_file[1]
  message(sprintf("Initial LULC: %s", current_lulc_path))

  # Build timestep pairs
  year_starts <- seq(start_year, end_year - step_length, by = step_length)
  year_ends <- year_starts + step_length

  # Calibration period: use the last (most recent) data period
  calibration_period <- config[["data_periods"]][length(config[[
    "data_periods"
  ]])]

  for (i in seq_along(year_starts)) {
    year_ant <- year_starts[i]
    year_post <- year_ends[i]
    message(sprintf(
      "\n  Timestep %d/%d: %d -> %d",
      i,
      length(year_starts),
      year_ant,
      year_post
    ))

    current_lulc_path <- run_allocation_one_timestep(
      scenario = scenario,
      year_ant = year_ant,
      year_post = year_post,
      current_lulc_path = current_lulc_path,
      regions = regions,
      region_rast = region_rast,
      calibration_period = calibration_period,
      sim_dir = sim_dir,
      config = config
    )

    message(sprintf(
      "  Timestep %d -> %d complete: %s",
      year_ant,
      year_post,
      current_lulc_path
    ))
  }
}


#' Run allocation for a single timestep across all regions
#'
#' @param scenario Scenario name
#' @param year_ant Anterior year
#' @param year_post Posterior year
#' @param current_lulc_path Path to current LULC raster
#' @param regions Regions data frame
#' @param region_rast Region raster
#' @param calibration_period Calibration period string (e.g., "2018_2022")
#' @param sim_dir Simulation output directory for this scenario
#' @param config Configuration list
#' @return Path to the mosaiced posterior raster
run_allocation_one_timestep <- function(
  scenario,
  year_ant,
  year_post,
  current_lulc_path,
  regions,
  region_rast,
  calibration_period,
  sim_dir,
  config
) {
  timestep_dir <- file.path(sim_dir, as.character(year_post))
  ensure_dir(timestep_dir)

  current_lulc <- terra::rast(current_lulc_path)
  region_names <- regions$label
  region_vals <- as.integer(regions$value)

  #todo within the parallel step use the log_msg functionality as per utils.r and other scripts.
  # Process each region (parallel via furrr if available)
  posterior_paths <- furrr::future_map(
    seq_along(region_names),
    function(idx) {
      region_label <- region_names[idx]
      region_val <- region_vals[idx]
      region_suffix <- gsub(" ", "_", tolower(region_label))

      region_work_dir <- file.path(
        timestep_dir,
        paste0("region_", region_suffix)
      )
      ensure_dir(region_work_dir)

      message(sprintf("    Region: %s (ID=%d)", region_label, region_val))

      # Mask and trim current LULC to region
      region_mask <- terra::ifel(region_rast == region_val, 1L, NA)
      lulc_region <- terra::mask(current_lulc, region_mask)
      lulc_region <- terra::trim(lulc_region, padding = 0)

      anterior_path <- file.path(region_work_dir, "anterior.tif")
      #todo consider using the project write_raster function from utils.r here to ensure consistent datatype and compression settings across all rasters. We should also consider using it for all subsequent raster writes in this script, including the probability maps and the final posterior rasters.
      terra::writeRaster(
        lulc_region,
        anterior_path,
        overwrite = TRUE,
        wopt = list(datatype = "INT2U", gdal = c("COMPRESS=LZW"))
      )

      # Prepare all Dinamica input files
      setup_allocation_inputs(
        work_dir = region_work_dir,
        region_label = region_label,
        region_val = region_val,
        scenario = scenario,
        year_ant = year_ant,
        year_post = year_post,
        anterior_path = anterior_path,
        calibration_period = calibration_period,
        config = config
      )

      # Run Dinamica
      posterior_path <- run_allocation_dinamica(region_work_dir)

      rm(lulc_region, region_mask)
      gc(verbose = FALSE)

      return(posterior_path)
    },
    .options = furrr::furrr_options(seed = TRUE)
  )

  # Mosaic region posteriors back to full extent
  message("    Mosaicing region posteriors...")
  region_rasters <- lapply(posterior_paths, terra::rast)

  # Extend all to the full extent of the original LULC, then merge
  full_extent <- terra::ext(current_lulc)
  region_rasters <- lapply(region_rasters, function(r) {
    terra::extend(r, full_extent)
  })

  if (length(region_rasters) == 1) {
    mosaiced <- region_rasters[[1]]
  } else {
    mosaiced <- do.call(terra::merge, region_rasters)
  }

  # Save mosaiced result
  output_path <- file.path(timestep_dir, sprintf("posterior_%d.tif", year_post))
  terra::writeRaster(
    mosaiced,
    output_path,
    overwrite = TRUE,
    wopt = list(datatype = "INT2U", gdal = c("COMPRESS=LZW"))
  )

  rm(region_rasters, mosaiced)
  gc(verbose = FALSE)

  return(output_path)
}


#' Prepare all Dinamica input files in work_dir
#'
#' @param work_dir Working directory for this region/timestep
#' @param region_label Region label string
#' @param region_val Region integer value
#' @param scenario Scenario name
#' @param year_ant Anterior year
#' @param year_post Posterior year
#' @param anterior_path Path to the anterior LULC raster
#' @param calibration_period Calibration period string
#' @param config Configuration list
#' @return NULL (called for side effects)
setup_allocation_inputs <- function(
  work_dir,
  region_label,
  region_val,
  scenario,
  year_ant,
  year_post,
  anterior_path,
  calibration_period,
  config
) {
  # 1. Copy transition rates CSV
  scalar_str <- sprintf("%.1f", config[["selected_scalar"]])
  trans_rate_src <- file.path(
    config[["trans_rate_table_dir"]],
    paste0("simulation-lulc-areas-scalar-", scalar_str, "x"),
    scenario,
    region_label,
    paste0(scenario, "-", region_label, "-trans_rates-", year_ant, ".csv")
  )
  if (!file.exists(trans_rate_src)) {
    stop(sprintf("Transition rate file not found: %s", trans_rate_src))
  }
  trans_rates_dst <- file.path(work_dir, "trans_rates.csv")
  file.copy(trans_rate_src, trans_rates_dst, overwrite = TRUE)

  trans_rates_df <- read.csv(trans_rates_dst, check.names = FALSE)

  # 2. Extract expansion table from allocation params
  alloc_params_path <- file.path(
    config[["calibration_param_dir"]],
    calibration_period,
    region_label,
    "allocation_params.csv"
  )
  if (!file.exists(alloc_params_path)) {
    stop(sprintf("Allocation params file not found: %s", alloc_params_path))
  }
  alloc_params <- read.csv(alloc_params_path, check.names = FALSE)

  # Expansion table: From*, To*, Perc_expander
  expansion_tbl <- alloc_params[, c("From*", "To*", "Perc_expander")]
  # Clamp to (1e-6, 1-1e-6) - values are already in 0-100 range as percentage
  expansion_tbl[["Perc_expander"]] <- pmax(
    1e-4,
    pmin(100 - 1e-4, expansion_tbl[["Perc_expander"]])
  )
  write.csv(
    expansion_tbl,
    file.path(work_dir, "expansion_table.csv"),
    row.names = FALSE
  )

  # 3. Patcher table: From*, To*, Mean_Patch_Size, Patch_Size_Variance, Patch_Isometry
  patcher_cols <- c(
    "From*",
    "To*",
    " Mean_Patch_Size",
    "Patch_Size_Variance",
    "Patch_Isometry"
  )
  patcher_tbl <- alloc_params[, patcher_cols]
  # Replace NAs/zeros with sensible defaults for unobserved transitions
  patcher_tbl[[" Mean_Patch_Size"]] <- ifelse(
    is.na(patcher_tbl[[" Mean_Patch_Size"]]) |
      patcher_tbl[[" Mean_Patch_Size"]] == 0,
    1,
    patcher_tbl[[" Mean_Patch_Size"]]
  )
  patcher_tbl[["Patch_Size_Variance"]] <- ifelse(
    is.na(patcher_tbl[["Patch_Size_Variance"]]),
    0,
    patcher_tbl[["Patch_Size_Variance"]]
  )
  patcher_tbl[["Patch_Isometry"]] <- ifelse(
    is.na(patcher_tbl[["Patch_Isometry"]]),
    0.5,
    patcher_tbl[["Patch_Isometry"]]
  )
  write.csv(
    patcher_tbl,
    file.path(work_dir, "patcher_table.csv"),
    row.names = FALSE
  )

  # 4. Generate probability maps
  generate_probability_maps(
    work_dir = work_dir,
    region_label = region_label,
    region_val = region_val,
    scenario = scenario,
    year_ant = year_ant,
    calibration_period = calibration_period,
    anterior_path = anterior_path,
    trans_rates_df = trans_rates_df,
    config = config
  )
}


#' Generate probability maps for all transitions in a region
#'
#' Loads fitted tidymodels workflows, predicts transition probabilities,
#' normalizes, optionally applies spatial interventions, and saves as TIFs.
#'
#' @param work_dir Working directory for this region/timestep
#' @param region_label Region label string
#' @param region_val Region integer value
#' @param scenario Scenario name
#' @param year_ant Anterior year
#' @param calibration_period Calibration period string
#' @param anterior_path Path to the anterior LULC raster
#' @param trans_rates_df Data frame of transition rates (From*, To*, Rate)
#' @param config Configuration list
#' @return Path to the probability_map_dir
generate_probability_maps <- function(
  work_dir,
  region_label,
  region_val,
  scenario,
  year_ant,
  calibration_period,
  anterior_path,
  trans_rates_df,
  config
) {
  prob_map_dir <- file.path(work_dir, "probability_map_dir")
  ensure_dir(prob_map_dir)

  region_suffix <- gsub(" ", "_", tolower(region_label))

  # Load LULC schema for class ID <-> name mapping
  lulc_schema <- jsonlite::fromJSON(
    config[["lulc_aggregation_path"]],
    simplifyVector = FALSE
  )
  class_value_to_name <- setNames(
    sapply(lulc_schema, function(x) x$class_name),
    sapply(lulc_schema, function(x) x$value)
  )
  class_name_to_value <- setNames(
    sapply(lulc_schema, function(x) x$value),
    sapply(lulc_schema, function(x) x$class_name)
  )

  # Discover fitted model files for this region
  model_dir <- file.path(config[["transition_model_dir"]], calibration_period)
  model_files <- list.files(
    model_dir,
    pattern = sprintf(".*_%s\\.rds$", region_suffix),
    full.names = TRUE
  )
  if (length(model_files) == 0) {
    stop(sprintf(
      "No fitted model RDS files found for region '%s' in %s",
      region_suffix,
      model_dir
    ))
  }

  # Parse transition names from filenames
  # e.g., "forested_areas_to_low_intensity_agricultural_areas_central.rds"
  model_info <- data.frame(
    file_path = model_files,
    trans_name = sub(
      sprintf("_%s\\.rds$", region_suffix),
      "",
      basename(model_files)
    ),
    stringsAsFactors = FALSE
  )

  message(sprintf(
    "    Found %d transition models for %s",
    nrow(model_info),
    region_label
  ))

  # Load all models and extract required predictor names
  fitted_models <- list()
  all_required_preds <- character()

  for (i in seq_len(nrow(model_info))) {
    trans_name <- model_info$trans_name[i]
    fitted_wf <- readRDS(model_info$file_path[i])
    fitted_models[[trans_name]] <- fitted_wf

    # Extract predictor names from the fitted workflow's recipe
    recipe_info <- workflows::extract_recipe(fitted_wf)$var_info
    pred_names <- recipe_info$variable[recipe_info$role == "predictor"]
    model_info$pred_names[i] <- list(pred_names)
    all_required_preds <- union(all_required_preds, pred_names)
  }

  message(sprintf(
    "    %d unique predictors required across all models",
    length(all_required_preds)
  ))

  # Load the anterior LULC raster for this region
  anterior <- terra::rast(anterior_path)

  # Get cell IDs and XY coordinates from the anterior raster
  # (non-NA cells only)
  anterior_df <- terra::as.data.frame(
    anterior,
    cells = TRUE,
    xy = TRUE,
    na.rm = TRUE
  )
  names(anterior_df) <- c("cell_id", "x", "y", "lulc_class")

  if (nrow(anterior_df) == 0) {
    warning(sprintf(
      "No valid cells in anterior raster for region %s",
      region_label
    ))
    return(prob_map_dir)
  }

  # Identify neighbourhood predictors vs static/dynamic predictors
  nhood_pred_names <- grep("_nhood_", all_required_preds, value = TRUE)
  parquet_pred_names <- setdiff(all_required_preds, nhood_pred_names)

  # Load static + dynamic predictor data from parquets
  pred_data <- NULL
  if (length(parquet_pred_names) > 0) {
    # Open parquet datasets
    static_preds_pq_path <- file.path(
      config[["predictors_prepped_dir"]],
      "parquet_data",
      "static"
    )
    # For simulation beyond calibration period, use the last available dynamic year
    period_start_year <- as.integer(
      stringr::str_extract(calibration_period, "^[0-9]{4}")
    )
    dynamic_preds_pq_path <- file.path(
      config[["predictors_prepped_dir"]],
      "parquet_data",
      "dynamic",
      period_start_year
    )

    ds_static <- arrow::open_dataset(
      static_preds_pq_path,
      format = "parquet",
      partitioning = arrow::hive_partition(region = arrow::int32())
    )
    ds_dynamic <- arrow::open_dataset(
      dynamic_preds_pq_path,
      format = "parquet",
      partitioning = arrow::hive_partition(
        scenario = arrow::utf8(),
        region = arrow::int32()
      )
    )

    # Use the national-grid cell_ids from the reference grid, not the trimmed raster
    # We need to map the trimmed anterior cell IDs to national grid cell IDs
    ref_grid <- terra::rast(config[["ref_grid_path"]])
    ref_grid_df <- terra::as.data.frame(
      ref_grid,
      cells = TRUE,
      xy = TRUE,
      na.rm = FALSE
    )
    names(ref_grid_df) <- c("ref_cell_id", "ref_x", "ref_y", "ref_val")

    # Match anterior cells to reference grid by XY coordinates
    anterior_xy <- anterior_df[, c("x", "y")]
    ref_cell_ids <- terra::cellFromXY(ref_grid, as.matrix(anterior_xy))

    pred_data <- load_predictor_data(
      ds_static = ds_static,
      ds_dynamic = ds_dynamic,
      cell_ids = ref_cell_ids,
      preds = parquet_pred_names,
      region_value = region_val,
      scenario = scenario
    )
    # Add local cell_id for joining back
    pred_data$local_cell_id <- anterior_df$cell_id[
      match(pred_data$cell_id, ref_cell_ids)
    ]
  }

  # Compute neighbourhood predictors if needed
  nhood_data <- NULL
  if (length(nhood_pred_names) > 0) {
    nhood_data <- compute_neighbourhood_predictors(
      anterior = anterior,
      nhood_pred_names = nhood_pred_names,
      lulc_schema = lulc_schema,
      config = config
    )
    # nhood_data has cell_id (local) + nhood predictor columns
  }

  # Combine predictor data into a single data frame keyed on local cell_id
  combined_df <- data.frame(
    local_cell_id = anterior_df$cell_id,
    lulc_class = anterior_df$lulc_class,
    x = anterior_df$x,
    y = anterior_df$y
  )
  if (!is.null(pred_data) && nrow(pred_data) > 0) {
    pred_cols <- setdiff(names(pred_data), c("cell_id", "local_cell_id"))
    combined_df <- merge(
      combined_df,
      pred_data[, c("local_cell_id", pred_cols), drop = FALSE],
      by = "local_cell_id",
      all.x = TRUE
    )
  }
  if (!is.null(nhood_data) && nrow(nhood_data) > 0) {
    nhood_cols <- setdiff(names(nhood_data), "cell_id")
    nhood_data_renamed <- nhood_data
    names(nhood_data_renamed)[
      names(nhood_data_renamed) == "cell_id"
    ] <- "local_cell_id"
    combined_df <- merge(
      combined_df,
      nhood_data_renamed[, c("local_cell_id", nhood_cols), drop = FALSE],
      by = "local_cell_id",
      all.x = TRUE
    )
  }

  # Build transition-to-row mapping from trans_rates_df
  # Each row in trans_rates_df corresponds to a numbered probability map
  trans_rates_df$row_num <- seq_len(nrow(trans_rates_df))

  # Initialize probability columns for normalization
  # Each unique "To" class gets a probability column
  unique_to_classes <- unique(trans_rates_df[["To*"]])
  prob_col_names <- paste0("prob_", unique_to_classes)
  for (pcol in prob_col_names) {
    combined_df[[pcol]] <- 0
  }

  # Predict probabilities for each transition model
  message("    Predicting transition probabilities...")
  for (i in seq_len(nrow(model_info))) {
    trans_name <- model_info$trans_name[i]
    fitted_wf <- fitted_models[[trans_name]]
    preds_needed <- model_info$pred_names[[i]]

    # Parse "from" and "to" class names from the transition name
    # Format: "{from_class}_to_{to_class}"
    parts <- strsplit(trans_name, "_to_")[[1]]
    from_class_name <- parts[1]
    to_class_name <- parts[2]

    from_value <- as.integer(class_name_to_value[from_class_name])
    to_value <- as.integer(class_name_to_value[to_class_name])

    if (is.na(from_value) || is.na(to_value)) {
      warning(sprintf(
        "Could not map transition '%s' to class values, skipping",
        trans_name
      ))
      next
    }

    # Subset to cells of the "from" class
    from_mask <- combined_df$lulc_class == from_value
    if (sum(from_mask) == 0) {
      next
    }

    from_data <- combined_df[from_mask, , drop = FALSE]

    # Predict probabilities
    pred_result <- predict(fitted_wf, from_data, type = "prob")
    # Second column is probability of transition occurring
    prob_values <- pred_result[[2]]
    prob_values[is.na(prob_values)] <- 0

    # Accumulate into the appropriate prob column
    prob_col <- paste0("prob_", to_value)
    if (prob_col %in% names(combined_df)) {
      combined_df[[prob_col]][from_mask] <- combined_df[[prob_col]][from_mask] +
        prob_values
    }
  }

  # Normalize probabilities so rows sum to <= 1
  message("    Normalizing probabilities...")
  normalize_probability_maps(combined_df, prob_col_names)

  #todo integrate more recent approach to spatiall intervention from NCCS project
  # Apply spatial interventions if configured
  # if (!is.null(config[["spat_prob_perturb_dir"]]) &&
  #     dir.exists(config[["spat_prob_perturb_dir"]])) {
  #   interventions_path <- file.path(
  #     config[["spat_prob_perturb_dir"]],
  #     "spatial_interventions.csv"
  #   )
  #   if (file.exists(interventions_path)) {
  #     interventions <- readr::read_csv2(interventions_path, col_types = "cccccdcc")
  #     combined_df <- lulcc.spatprobmanipulation(
  #       Interventions = interventions,
  #       scenario_id = scenario,
  #       Raster_prob_values = combined_df,
  #       Simulation_time_step = as.character(year_ant)
  #     )
  #     # Re-normalize after interventions
  #     normalize_probability_maps(combined_df, prob_col_names)
  #   }
  # }

  # Save probability maps as TIFs
  message("    Saving probability maps...")
  crs_str <- terra::crs(anterior)

  for (row_idx in seq_len(nrow(trans_rates_df))) {
    from_val <- trans_rates_df[["From*"]][row_idx]
    to_val <- trans_rates_df[["To*"]][row_idx]
    rate <- trans_rates_df[["Rate"]][row_idx]

    # Skip self-transitions (diagonal) or zero-rate transitions
    if (from_val == to_val) {
      next
    }
    if (rate == 0) {
      next
    }

    from_name <- class_value_to_name[as.character(from_val)]
    to_name <- class_value_to_name[as.character(to_val)]

    prob_col <- paste0("prob_", to_val)

    # Get data for cells of the "from" class
    from_mask <- combined_df$lulc_class == from_val
    map_data <- data.frame(
      x = combined_df$x[from_mask],
      y = combined_df$y[from_mask],
      prob = combined_df[[prob_col]][from_mask]
    )
    map_data$prob[is.na(map_data$prob)] <- 0

    # Also include cells NOT of the "from" class with prob = 0
    non_from_data <- data.frame(
      x = combined_df$x[!from_mask],
      y = combined_df$y[!from_mask],
      prob = 0
    )
    map_data <- rbind(map_data, non_from_data)

    # Rasterize
    prob_raster <- terra::rast(
      as.matrix(map_data[, c("x", "y", "prob")]),
      type = "xyz",
      crs = crs_str
    )

    # File naming: {row_number}_{from}_to_{to}.tif
    # Row number is the 1-indexed position in trans_rates.csv
    tif_name <- sprintf("%d_%d_to_%d.tif", row_idx, from_val, to_val)
    tif_path <- file.path(prob_map_dir, tif_name)

    terra::writeRaster(
      prob_raster,
      tif_path,
      overwrite = TRUE,
      NAflag = -999
    )
  }

  message(sprintf("    Probability maps saved to: %s", prob_map_dir))
  return(prob_map_dir)
}


#' Compute neighbourhood predictors from the current LULC raster
#'
#' @param anterior LULC raster for the current region
#' @param nhood_pred_names Character vector of required neighbourhood predictor names
#' @param lulc_schema Parsed LULC schema list
#' @param config Configuration list
#' @return Data frame with cell_id and neighbourhood predictor columns
compute_neighbourhood_predictors <- function(
  anterior,
  nhood_pred_names,
  lulc_schema,
  config
) {
  message("    Computing neighbourhood predictors...")

  # Load focal matrices
  focal_matrices <- readRDS(file.path(
    config[["preds_tools_dir"]],
    "neighbourhood_matrices",
    "all_matrices.rds"
  ))
  focal_matrices <- unlist(focal_matrices, recursive = FALSE)
  names(focal_matrices) <- sapply(names(focal_matrices), function(x) {
    stringr::str_split(x, "[.]")[[1]][2]
  })

  # Build lookup: class abbreviation -> aggregated ID
  class_name_to_value <- setNames(
    sapply(lulc_schema, function(x) x$value),
    sapply(lulc_schema, function(x) x$class_name)
  )

  # Parse neighbourhood predictor names to identify which class + matrix to use
  # Expected format: {class_name}_nhood_{matrix_id}
  nhood_rasters <- list()
  for (pred_name in nhood_pred_names) {
    # Split into class name and matrix id
    parts <- stringr::str_match(pred_name, "^(.+)_nhood_(.+)$")
    if (is.na(parts[1, 1])) {
      warning(sprintf("Cannot parse nhood predictor name: %s", pred_name))
      next
    }
    class_name <- parts[1, 2]
    matrix_id <- parts[1, 3]

    class_val <- class_name_to_value[class_name]
    if (is.na(class_val)) {
      warning(sprintf(
        "Unknown class '%s' in nhood predictor: %s",
        class_name,
        pred_name
      ))
      next
    }

    if (!matrix_id %in% names(focal_matrices)) {
      warning(sprintf(
        "Unknown matrix '%s' in nhood predictor: %s",
        matrix_id,
        pred_name
      ))
      next
    }

    # Binary raster for this class
    class_raster <- anterior == class_val

    # Apply focal operation
    focal_layer <- terra::focal(
      x = class_raster,
      w = focal_matrices[[matrix_id]],
      na.rm = FALSE,
      expand = TRUE,
      fillvalue = 0
    )

    nhood_rasters[[pred_name]] <- focal_layer
  }

  if (length(nhood_rasters) == 0) {
    return(data.frame(cell_id = integer()))
  }

  # Convert to data frames and merge
  nhood_dfs <- mapply(
    function(rast, name) {
      df <- terra::as.data.frame(rast, cells = TRUE, na.rm = TRUE)
      names(df) <- c("cell_id", name)
      df
    },
    nhood_rasters,
    names(nhood_rasters),
    SIMPLIFY = FALSE
  )

  result <- Reduce(
    function(x, y) merge(x, y, by = "cell_id", all = TRUE),
    nhood_dfs
  )

  return(result)
}


#' Normalize probability columns so row sums do not exceed 1
#'
#' @param df Data frame (modified in place via reference semantics if data.table,
#'   or returned if regular data.frame)
#' @param prob_cols Character vector of probability column names
#' @return NULL (modifies df by reference if data.table, or returns modified df)
normalize_probability_maps <- function(df, prob_cols) {
  prob_cols_present <- intersect(prob_cols, names(df))
  if (length(prob_cols_present) == 0) {
    return(invisible(NULL))
  }

  row_sums <- rowSums(df[, prob_cols_present, drop = FALSE], na.rm = TRUE)
  needs_rescale <- row_sums > 1

  if (any(needs_rescale)) {
    for (col in prob_cols_present) {
      df[[col]][needs_rescale] <- df[[col]][needs_rescale] /
        row_sums[needs_rescale]
    }
  }

  invisible(NULL)
}
