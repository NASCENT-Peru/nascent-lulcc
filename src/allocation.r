#' Allocation Step: LULC Simulation via Dinamica EGO
#'
#' Orchestrates the allocation of land use/land cover transitions using
#' Dinamica EGO, processing multiple scenarios, timesteps, and regions.
#'
#' @author Ben Black
#'
#'
# testing vars
scenario <- "BAU"
i <- 1
idx <- 1
#work_dir = region_work_dir
j <- 1
k <- 1

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
  region_rast_path <- list.files(
    config[["reg_dir"]],
    pattern = "regions.tif$",
    full.names = TRUE
  )

  for (scenario in config[["scenario_names"]]) {
    t_start <- proc.time()
    message(sprintf("\n--- Scenario: %s ---", scenario))

    run_allocation_for_scenario(
      scenario = scenario,
      regions = regions,
      region_rast_path = region_rast_path,
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
#' @param region_rast_path Path for regions raster
#' @param config Configuration list
#' @return NULL (called for side effects)
run_allocation_for_scenario <- function(
  scenario,
  regions,
  region_rast_path,
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
      region_rast_path = region_rast_path,
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
#' @param region_rast_path Path to regions raster
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
  region_rast_path,
  calibration_period,
  sim_dir,
  config
) {
  timestep_dir <- file.path(sim_dir, as.character(year_post))
  ensure_dir(timestep_dir)

  # Extract region names and values for iteration
  region_names <- regions$label
  region_vals <- as.integer(regions$value)

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
      log_file <- initialize_worker_log(
        file.path(region_work_dir, "worker_logs"),
        region_suffix
      )

      log_msg(
        sprintf("    Region: %s (ID=%d)", region_label, region_val),
        log_file
      )

      # load region raster
      region_rast <- terra::rast(region_rast_path)

      # Load current LULC raster
      current_lulc <- terra::rast(current_lulc_path)

      # Mask and trim current LULC to region
      lulc_region <- terra::mask(
        current_lulc,
        region_rast,
        maskvalues = region_val,
        inverse = TRUE
      )
      lulc_region <- terra::trim(lulc_region, padding = 0)

      anterior_path <- file.path(region_work_dir, "anterior.tif")
      #todo consider using the project write_raster function from utils.r here to ensure consistent datatype and compression settings across all rasters. We should also consider using it for all subsequent raster writes in this script, including the probability maps and the final posterior rasters.
      terra::writeRaster(
        lulc_region,
        anterior_path,
        overwrite = TRUE,
        wopt = list(datatype = "INT2U", gdal = c("COMPRESS=LZW"))
      )

      #todo pass the log_file to the functions called within setup_allocation_inputs and run_allocation_dinamica so they can log messages there as well, instead of just in this main loop. This will give us more visibility into what's happening inside those functions, especially if something goes wrong.
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
        config = config,
        log_file = log_file
      )

      # Run Dinamica
      posterior_path <- run_allocation_dinamica(region_work_dir)
      log_msg(
        sprintf("    Completed region: %s (ID=%d)", region_label, region_val),
        log_file
      )

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
#' @param log_file Path to the log file for this worker/region
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
  config,
  log_file
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
    stop(log_msg(
      sprintf("Transition rate file not found: %s", trans_rate_src),
      log_file
    ))
  }
  trans_rates_dst <- file.path(work_dir, "trans_rates.csv")
  file.copy(trans_rate_src, trans_rates_dst, overwrite = TRUE)

  trans_rates_df <- read.csv(trans_rates_dst, check.names = FALSE)

  # sort by id_trans
  trans_rates_df <- trans_rates_df[order(trans_rates_df[["id_trans"]]), ]

  # 2. Extract expansion table from allocation params
  alloc_params_path <- file.path(
    config[["calibration_param_dir"]],
    calibration_period,
    region_label,
    "allocation_params.csv"
  )
  if (!file.exists(alloc_params_path)) {
    stop(log_msg(
      sprintf("Allocation params file not found: %s", alloc_params_path),
      log_file
    ))
  }
  alloc_params <- read.csv(alloc_params_path, check.names = FALSE)

  # sort by id_trans
  alloc_params <- alloc_params[order(alloc_params[["id_trans"]]), ]

  # subset alloc_params to only values of id_trans that are present in trans_rates_df
  alloc_params <- alloc_params[
    alloc_params[["id_trans"]] %in% trans_rates_df[["id_trans"]],
  ]

  # warn if any id_trans values in trans_rates_df are missing from alloc_params
  missing_alloc_params <- setdiff(
    trans_rates_df[["id_trans"]],
    alloc_params[["id_trans"]]
  )
  if (length(missing_alloc_params) > 0) {
    warning(log_msg(
      sprintf(
        "The following id_trans values are present in trans_rates_df but missing from alloc_params: %s",
        paste(missing_alloc_params, collapse = ", ")
      ),
      log_file
    ))
  }

  # Expansion table: From*, To*, Perc_expander
  expansion_tbl <- alloc_params[, c("From*", "To*", "Perc_expander")]
  write.csv(
    expansion_tbl,
    file.path(work_dir, "expansion_table.csv"),
    row.names = FALSE
  )
  log_msg(
    sprintf("  Expansion table written with %d rows", nrow(expansion_tbl)),
    log_file
  )

  # 3. Patcher table: From*, To*, Mean_Patch_Size, Patch_Size_Variance, Patch_Isometry
  patcher_cols <- c(
    "From*",
    "To*",
    "Mean_Patch_Size",
    "Patch_Size_Variance",
    "Patch_Isometry"
  )

  patcher_tbl <- alloc_params[, patcher_cols]
  # Replace NAs/zeros with sensible defaults for unobserved transitions
  patcher_tbl[["Mean_Patch_Size"]] <- ifelse(
    is.na(patcher_tbl[["Mean_Patch_Size"]]) |
      patcher_tbl[["Mean_Patch_Size"]] == 0,
    1,
    patcher_tbl[["Mean_Patch_Size"]]
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
  log_msg(
    sprintf("  Patcher table written with %d rows", nrow(patcher_tbl)),
    log_file
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
    config = config,
    log_file = log_file
  )
}


#' Generate probability maps for all transitions in a region
#'
#' Loads fitted tidymodels workflows one at a time, predicts transition
#' probabilities at only the sparse set of cells currently in each transition's
#' "from" class, normalizes across transitions per cell, and saves per-transition
#' probability rasters as TIFs.
#'
#' Memory-saving design (vs. a naive "load all models + wide prob columns"
#' approach):
#'   * One fitted model resident at a time; released after predicting.
#'   * Predictor data loaded only for the subset of cells currently in the
#'     relevant "from" class, and only for the columns that model requires.
#'   * Long-format sparse accumulation for normalization (no wide prob_<to>
#'     columns materialized over the full ~42M-cell region raster).
#'
#' @param work_dir Working directory for this region/timestep
#' @param region_label Region label string
#' @param region_val Region integer value
#' @param scenario Scenario name (e.g. "BAU") — mapped to SSP string via
#'   `config$scenario_to_ssp_mapping` before being passed to the dynamic
#'   predictor parquet.
#' @param year_ant Anterior year
#' @param calibration_period Calibration period string
#' @param anterior_path Path to the anterior LULC raster
#' @param trans_rates_df Data frame of transition rates (From*, To*, Rate)
#' @param config Configuration list
#' @param log_file Path to the log file for this worker/region
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
  config,
  log_file
) {
  prob_map_dir <- file.path(work_dir, "probability_map_dir")
  ensure_dir(prob_map_dir)

  region_suffix <- gsub(" ", "_", tolower(region_label))

  # Scenario -> SSP for dynamic predictor parquet partition filter
  ssp_name <- config[["scenario_to_ssp_mapping"]][[scenario]]
  if (is.null(ssp_name)) {
    stop(log_msg(
      sprintf(
        "No scenario_to_ssp_mapping entry for scenario '%s'",
        scenario
      ),
      log_file
    ))
  }
  #if year_ant is < 2022 then ssp_name == baseline
  if (year_ant <= 2022) {
    ssp_name <- "baseline"
  }

  # Load LULC schema for class ID <-> name mapping
  lulc_schema <- jsonlite::fromJSON(
    config[["lulc_aggregation_path"]],
    simplifyVector = FALSE
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
    stop(log_msg(
      sprintf(
        "No fitted model RDS files found for region '%s' in %s",
        region_suffix,
        model_dir
      ),
      log_file
    ))
  }

  # Build per-transition metadata (no RDS reads here — models are loaded
  # on-demand inside the prediction loop, one at a time, to keep peak memory
  # bounded by a single model's size).
  model_info <- data.table::data.table(
    file_path = model_files,
    trans_name = sub(
      sprintf("_%s\\.rds$", region_suffix),
      "",
      basename(model_files)
    )
  )
  model_info[,
    c("anterior_class", "posterior_class") := data.table::tstrsplit(
      trans_name,
      "-",
      fixed = TRUE
    )
  ]
  model_info[,
    `:=`(
      from_val = as.integer(class_name_to_value[anterior_class]),
      to_val = as.integer(class_name_to_value[posterior_class])
    )
  ]
  # id_trans is not strictly needed for the prediction step, but it's helpful metadata
  model_info <- merge(
    model_info,
    trans_rates_df[, c("From*", "To*", "id_trans")],
    by.x = c("from_val", "to_val"),
    by.y = c("From*", "To*"),
    all.x = FALSE, # to avoid keeping models that don't have a corresponding transition rate (e.g. zero-rate or not in this scenario)
    sort = FALSE
  )

  # what id_trans values are present in trans_rates_df but missing from model_info? these represent transitions that have a transition rate but no fitted model (e.g. zero-rate or not in this scenario). we should log these so we know which transitions are being skipped in the prediction step.
  missing_models <- setdiff(
    trans_rates_df[["id_trans"]],
    model_info[["id_trans"]]
  )
  if (length(missing_models) > 0L) {
    stop(log_msg(
      sprintf(
        "The following id_trans values are present in trans_rates_df but missing from model_info (i.e. no fitted model found for these transitions, likely due to zero-rate or not being in this scenario): %s",
        paste(missing_models, collapse = ", ")
      ),
      log_file
    ))
  }

  # Load the anterior LULC raster for this region
  anterior <- terra::rast(anterior_path)

  # Lightweight sparse index of all non-NA cells: cell_id, x, y, lulc_class,
  # ref_cell_id. This is the only full-extent table held.
  anterior_dt <- data.table::setDT(
    terra::as.data.frame(anterior, cells = TRUE, xy = TRUE, na.rm = TRUE)
  )
  data.table::setnames(
    anterior_dt,
    old = seq_along(anterior_dt),
    new = c("cell_id", "x", "y", "lulc_class")
  )

  if (nrow(anterior_dt) == 0L) {
    warning(log_msg(
      sprintf(
        "No valid cells in anterior raster for region %s",
        region_label
      ),
      log_file
    ))
    return(prob_map_dir)
  }

  # Attach ref_cell_id (national grid) now, once — needed for any model that
  # references parquet predictors. Cost is negligible vs. per-transition
  # recomputation.
  ref_grid <- terra::rast(config[["ref_grid_path"]])
  anterior_dt[,
    ref_cell_id := terra::cellFromXY(ref_grid, cbind(x, y))
  ]
  data.table::setkey(anterior_dt, cell_id)

  # Open parquet datasets lazily (no data pulled until filtered + collected)
  static_preds_pq_path <- file.path(
    config[["predictors_prepped_dir"]],
    "parquet_data",
    "static"
  )

  dynamic_preds_pq_path <- file.path(
    config[["predictors_prepped_dir"]],
    "parquet_data",
    "dynamic",
    year_ant
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

  # Neighbourhood SpatRasters computed on first use and cached for the
  # remainder of the region run. Values depend only on the anterior LULC,
  # not on which transition references them. Focal matrices are read once
  # here, not on every cache miss.
  nhood_raster_cache <- new.env(parent = emptyenv())
  focal_matrices <- NULL
  get_nhood_raster <- function(pred_name) {
    if (!exists(pred_name, envir = nhood_raster_cache, inherits = FALSE)) {
      if (is.null(focal_matrices)) {
        fm <- readRDS(file.path(
          config[["preds_tools_dir"]],
          "neighbourhood_matrices",
          "all_matrices.rds"
        ))
        fm <- unlist(fm, recursive = FALSE)
        names(fm) <- vapply(
          names(fm),
          function(x) stringr::str_split(x, "[.]")[[1]][2],
          character(1)
        )
        focal_matrices <<- fm
      }
      rast <- compute_single_nhood_raster(
        anterior = anterior,
        pred_name = pred_name,
        focal_matrices = focal_matrices,
        class_name_to_value = class_name_to_value
      )
      assign(pred_name, rast, envir = nhood_raster_cache)
    }
    get(pred_name, envir = nhood_raster_cache)
  }

  # Map (from_val, to_val) pairs to their trans_rates.csv row index so we can
  # write TIFs with the row-number prefix that Dinamica's probability-map
  # cube expects.
  trans_rates_dt <- data.table::as.data.table(trans_rates_df)
  trans_rates_dt[, row_idx := seq_len(.N)]

  # Per-transition: load model -> predict at sparse from-class cells -> release
  log_msg("    Predicting transition probabilities...", log_file)
  gather <- vector("list", nrow(model_info))
  for (j in seq_len(nrow(model_info))) {
    mi <- model_info[j]
    trans_name <- mi$trans_name
    from_val <- mi$from_val
    to_val <- mi$to_val
    log_msg(
      sprintf(
        "      Transition %d -> %d: predicting with model '%s'",
        from_val,
        to_val,
        basename(mi$file_path)
      ),
      log_file
    )

    if (is.na(from_val) || is.na(to_val)) {
      warning(log_msg(
        sprintf(
          "Could not map transition '%s' to class values, skipping",
          trans_name
        ),
        log_file
      ))
      next
    }

    # trans_rates.csv row index (required for TIF filename prefix)
    row_idx_row <- trans_rates_dt[
      `From*` == from_val & `To*` == to_val,
      row_idx
    ]
    if (length(row_idx_row) == 0L) {
      # Not in trans_rates (e.g. zero-rate or not in this scenario) - skip
      next
    }
    row_idx <- row_idx_row[[1L]]

    # Sparse set of cells currently in this "from" class
    from_idx <- anterior_dt[lulc_class == from_val]
    if (nrow(from_idx) == 0L) {
      next
    }

    # Load model (we need its predictor names anyway, so load once + use
    # immediately + release at end of iteration).
    fitted_wf <- readRDS(mi$file_path)
    preds_needed <- fitted_wf$predictor_names
    if (is.null(preds_needed)) {
      preds_needed <- tryCatch(
        setdiff(
          workflows::extract_recipe(fitted_wf)$var_info$variable,
          "response"
        ),
        error = function(e) character()
      )
    }
    nhood_needed <- grep("_nhood_", preds_needed, value = TRUE)
    parquet_needed <- setdiff(preds_needed, nhood_needed)

    # Start from_data with the sparse index (copy so we can augment with
    # predictors without mutating anterior_dt).
    from_data <- data.table::copy(from_idx)

    if (length(parquet_needed) > 0L) {
      log_msg(
        sprintf(
          "        Loading predictor data from Parquet for predictors: %s",
          paste(parquet_needed, collapse = ", ")
        ),
        log_file
      )
      pred_data <- data.table::as.data.table(load_predictor_data(
        ds_static = ds_static,
        ds_dynamic = ds_dynamic,
        cell_ids = from_data$ref_cell_id,
        preds = parquet_needed,
        region_value = region_val,
        scenario = ssp_name
      ))
      if ("cell_id" %in% names(pred_data)) {
        data.table::setnames(pred_data, "cell_id", "ref_cell_id")
      }
      # Right-join: preserve every from_data row; missing predictors -> NA
      from_data <- pred_data[from_data, on = "ref_cell_id"]
    }

    if (length(nhood_needed) > 0L) {
      log_msg(
        sprintf(
          "        Computing/loading neighbourhood rasters for predictors: %s",
          paste(nhood_needed, collapse = ", ")
        ),
        log_file
      )
      nhood_stack <- terra::rast(lapply(nhood_needed, get_nhood_raster))
      nhood_vals <- terra::extract(
        nhood_stack,
        as.matrix(from_data[, .(x, y)])
      )
      nhood_vals <- data.table::as.data.table(nhood_vals)
      if ("ID" %in% names(nhood_vals)) {
        nhood_vals[, ID := NULL]
      }
      from_data[, (nhood_needed) := nhood_vals[, nhood_needed, with = FALSE]]
    }

    # Predict, then release the model
    # ranger's predict.ranger accesses $importance.mode unconditionally; if the
    # slot was dropped during model-trimming or a ranger version change, the
    # `%in%` test returns logical(0) and the enclosing `if` errors. Restore a
    # safe default so prediction proceeds.
    if (
      inherits(fitted_wf$model$fit, "ranger") &&
        !length(fitted_wf$model$fit$importance.mode)
    ) {
      fitted_wf$model$fit$importance.mode <- "none"
    }
    pred_result <- predict(fitted_wf$model, from_data, type = "prob")
    prob_values <- pred_result[[2L]]
    prob_values[is.na(prob_values)] <- 0
    prob_values <- pmax(0, pmin(1, prob_values))

    log_msg(
      "Appending predictions to gather table for normalization...",
      log_file
    )
    gather[[j]] <- data.table::data.table(
      row_idx = row_idx,
      from_val = from_val,
      to_val = to_val,
      cell_id = from_data$cell_id,
      x = from_data$x,
      y = from_data$y,
      prob = prob_values
    )

    rm(fitted_wf, pred_result, from_data, pred_data)
    gc(verbose = FALSE)
  }

  # Normalize across transitions per cell (long-format, sparse)
  log_msg("    Normalizing probabilities...", log_file)
  normalized <- data.table::rbindlist(gather, use.names = TRUE, fill = TRUE)
  if (nrow(normalized) == 0L) {
    log_msg("    No predictions produced; skipping map writes", log_file)
    return(prob_map_dir)
  }
  normalized[, tot_prob := sum(prob), by = cell_id]
  normalized[tot_prob > 1, prob := prob / tot_prob]
  normalized[, tot_prob := NULL]

  #todo integrate more recent approach to spatial intervention from NCCS project
  # (placeholder retained from previous implementation)

  # Write one TIF per trans_rates row, preserving the numeric prefix required
  # by Dinamica's CreateCubeOfProbabilityMaps submodel.
  # Prefixing with 001, 002... so these files are sorted the same as the
  # transition, expansion, and patcher tables on all sorts of filesystems.
  log_msg("    Saving probability maps...", log_file)
  for (k in seq_len(nrow(trans_rates_dt))) {
    from_val <- trans_rates_dt[["From*"]][k]
    to_val <- trans_rates_dt[["To*"]][k]
    rate <- trans_rates_dt[["Rate"]][k]
    id_trans <- trans_rates_dt[["id_trans"]][k]

    if (from_val == to_val || rate == 0) {
      next
    }

    dt_j <- normalized[row_idx == k]
    if (nrow(dt_j) == 0L) {
      next
    }

    tif_path <- file.path(
      prob_map_dir,
      sprintf("%03d_id_trans_%d.tif", k, id_trans)
    )

    terra::rasterize(
      x = as.matrix(dt_j[, .(x, y)]),
      y = anterior,
      values = dt_j[["prob"]],
      fun = "first"
    ) |>
      terra::writeRaster(
        filename = tif_path,
        overwrite = TRUE,
        NAflag = -999
      )
    log_msg(
      sprintf(
        "      Transition %d -> %d (id_trans=%d): %d cells, mean prob=%.4f, saved to %s",
        from_val,
        to_val,
        id_trans,
        nrow(dt_j),
        mean(dt_j[["prob"]]),
        tif_path
      ),
      log_file
    )
  }

  log_msg(sprintf("    Probability maps saved to: %s", prob_map_dir), log_file)
  return(prob_map_dir)
}


#' Compute a single neighbourhood predictor SpatRaster
#'
#' Parses `{class_name}_nhood_{matrix_id}`, applies the named focal matrix to a
#' binary mask of the target class, and returns the resulting SpatRaster.
#' Caller is responsible for caching (this function does no caching itself).
#'
#' @param anterior LULC raster for the current region
#' @param pred_name Neighbourhood predictor name — `{class_name}_nhood_{matrix_id}`
#' @param focal_matrices Named list of focal weight matrices
#'   (matrix_id -> weight matrix)
#' @param class_name_to_value Named integer vector mapping class name -> LULC
#'   class value
#' @return SpatRaster of the focal output (layer name == `pred_name`)
compute_single_nhood_raster <- function(
  anterior,
  pred_name,
  focal_matrices,
  class_name_to_value
) {
  parts <- stringr::str_match(pred_name, "^(.+)_nhood_(.+)$")
  if (is.na(parts[1, 1])) {
    stop(sprintf("Cannot parse nhood predictor name: %s", pred_name))
  }
  class_name <- parts[1, 2]
  matrix_id <- parts[1, 3]

  class_val <- class_name_to_value[class_name]
  if (is.na(class_val)) {
    stop(sprintf(
      "Unknown class '%s' in nhood predictor: %s",
      class_name,
      pred_name
    ))
  }
  if (!matrix_id %in% names(focal_matrices)) {
    stop(sprintf(
      "Unknown matrix '%s' in nhood predictor: %s",
      matrix_id,
      pred_name
    ))
  }

  class_raster <- anterior == class_val
  focal_layer <- terra::focal(
    x = class_raster,
    w = focal_matrices[[matrix_id]],
    na.rm = FALSE,
    expand = TRUE,
    fillvalue = 0
  )
  names(focal_layer) <- pred_name
  focal_layer
}
