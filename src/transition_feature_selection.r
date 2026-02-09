#' Modularized Feature Selection for LULCC Transitions
#'
#' @author: Ben Black (adapted)

#' Main orchestrator function for transition feature selection across all periods
#'
#' @param config Configuration list
#' @return Data frame with feature selection summary for all periods
#' @export
transition_feature_selection <- function(
  config = get_config(),
  use_regions = isTRUE(config[["regionalization"]]),
  refresh_cache = FALSE,
  save_debug = TRUE,
  fs_dir = file.path(config[["feature_selection_dir"]]),
  do_collinearity = TRUE, # Whether to perform collinearity filtering
  do_grrf = TRUE # Whether to perform GRRF feature selection
) {
  message("\n========================================")
  message("Starting Feature Selection Pipeline")
  message("========================================\n")

  periods_to_process <- list(config[["data_periods"]])
  #periods_to_process <- periods_to_process[3]

  message(sprintf(
    "Processing %d periods",
    length(periods_to_process)
  ))
  message(sprintf(
    "Regionalization: %s\n",
    ifelse(use_regions, "ENABLED", "DISABLED")
  ))

  # create a debug directory for intermediate results
  ensure_dir(fs_dir)

  # Process each period (sequential to avoid memory issues with large datasets)
  purrr::map(
    periods_to_process,
    function(period) {
      # Call feature selection for this period
      perform_feature_selection(
        period = period,
        use_regions = use_regions,
        config = config,
        fs_dir = fs_dir,
        save_debug = save_debug,
        do_collinearity = do_collinearity,
        do_grrf = do_grrf,
        refresh_cache = refresh_cache
      )
    }
  )

  message("\n========================================")
  message("Feature Selection Complete")
  message("========================================")

  # Reset parallel plan
  future::plan(sequential)

  return(final_summary)
}

#' Extract names by group type from predictor table
#' @param tbl Predictor table list
#' @param group Grouping type to extract
#' @return Character vector of predictor base names
get_preds <- function(tbl, group) {
  purrr::map_chr(
    tbl,
    ~ {
      if (
        !is.null(.x$grouping) && !is.null(.x$base_name) && .x$grouping == group
      ) {
        .x$base_name
      } else {
        NA_character_
      }
    }
  ) |>
    purrr::discard(is.na)
}

#' Perform feature selection for a single period
#'
#' @param period Period name (e.g., "2018_2022")
#' @param use_regions Boolean indicating if regionalization is active
#' @param config Configuration list
#' @param fs_dir Directory for debug outputs
#' @param save_debug Boolean indicating if debug outputs should be saved
#' @param do_collinearity Whether to perform collinearity filtering
#' @param do_grrf Whether to perform GRRF feature selection
#' @return NULL (results saved to disk)
perform_feature_selection <- function(
  period,
  use_regions,
  config,
  fs_dir,
  refresh_cache = FALSE,
  save_debug = TRUE,
  do_collinearity = TRUE, # Whether to perform collinearity filtering
  do_grrf = TRUE # Whether to perform GRRF feature selection
) {
  message("\n########################################")
  message(sprintf("# PERIOD: %s", period))
  message("########################################\n")

  # Create period-specific directory
  period_dir <- file.path(fs_dir, period)
  ensure_dir(period_dir)

  # --- Load metadata ---
  transitions_info <- readRDS(config[["viable_transitions_lists"]])[[period]]
  message(sprintf(
    "Loaded %d viable transitions for period %s",
    nrow(transitions_info),
    period
  ))

  lulc_schema <- jsonlite::fromJSON(
    config[["LULC_aggregation_path"]],
    simplifyVector = FALSE
  )
  message(
    "Loaded LULC schema"
  )

  # get the values of class_name where nhood_class == FALSE
  nhood_lulcs <- lapply(lulc_schema, function(x) {
    if (isTRUE(x$nhood_class)) {
      return(x$class_name)
    } else {
      return(NA)
    }
  })
  nhood_lulcs <- nhood_lulcs[!is.na(nhood_lulcs)]

  # Load predictor table
  pred_table_raw <- yaml::yaml.load_file(config[["pred_table_path"]])
  message("Loaded predictor table")

  # remove all entries without a path
  pred_table_raw <- pred_table_raw[sapply(pred_table_raw, function(x) {
    !is.null(x$path)
  })]

  # get period specific predictors

  # extract first year from period string
  period_start_year <- as.integer(stringr::str_extract(period, "^[0-9]{4}"))
  period_preds <- pred_table_raw[sapply(pred_table_raw, function(x) {
    x$period == period_start_year
  })]

  # get static predictors
  static_preds <- pred_table_raw[sapply(pred_table_raw, function(x) {
    x$period == "all"
  })]

  # Combine predictor lists
  pred_table <- c(static_preds, period_preds)

  soil_preds <- get_preds(pred_table, "soil")
  nhood_preds <- get_preds(pred_table, "neighbourhood")
  climate_preds <- get_preds(pred_table, "climatic")

  # Soil groups: group by prefix before "_"
  soil_groups <- split(soil_preds, stringr::str_extract(soil_preds, "^[^_]+"))

  # Neighbourhood groups: group by presence of lulc name
  nhood_groups <- purrr::map(nhood_lulcs, function(lulc) {
    purrr::keep(nhood_preds, ~ stringr::str_detect(.x, lulc))
  })
  names(nhood_groups) <- nhood_lulcs

  # Remove empty groups
  nhood_groups <- purrr::discard(nhood_groups, ~ length(.x) == 0)
  names(nhood_groups) <- nhood_lulcs[nhood_lulcs %in% names(nhood_groups)]

  # Suitability predictors (not soil or neighbourhood)
  suitability_preds <- purrr::map_chr(
    pred_table,
    ~ {
      if (
        .x$pred_category == "suitability" &&
          !.x$grouping %in% c("neighbourhood", "climatic", "soil")
      ) {
        .x$base_name
      } else {
        NA_character_
      }
    }
  ) |>
    purrr::discard(is.na)

  # Build final vector
  pred_categories <- c(
    setNames(rep("suitability", length(suitability_preds)), suitability_preds),
    setNames(rep("climatic", length(climate_preds)), climate_preds),
    purrr::imap(soil_groups, ~ setNames(rep(.y, length(.x)), .x)) %>%
      purrr::reduce(c),
    purrr::imap(nhood_groups, ~ setNames(rep(.y, length(.x)), .x)) %>%
      purrr::reduce(c)
  )

  message(sprintf("%d viable transitions", nrow(transitions_info)))
  message(sprintf(
    "Identified %d predictors for this period\n",
    length(pred_categories)
  ))

  # --- Set up file paths ---
  transitions_pq_path <- file.path(
    config[["trans_pre_pred_filter_dir"]],
    period
  )
  static_preds_pq_path <- file.path(
    config[["predictors_prepped_dir"]],
    "parquet_data",
    "static"
  )
  dynamic_preds_pq_path <- file.path(
    config[["predictors_prepped_dir"]],
    "parquet_data",
    "dynamic",
    period_start_year
  )

  # Verify files exist
  stopifnot(
    file.exists(transitions_pq_path),
    file.exists(static_preds_pq_path),
    file.exists(dynamic_preds_pq_path)
  )

  # Check contents of transitions directory
  trans_files <- list.files(transitions_pq_path, recursive = FALSE)
  message(sprintf(
    "Files in transitions directory: %s",
    paste(trans_files, collapse = ", ")
  ))

  # Check for parquet files specifically
  parquet_files <- list.files(
    transitions_pq_path,
    pattern = "\\.parquet$",
    recursive = TRUE
  )
  if (length(parquet_files) == 0) {
    stop(sprintf(
      "No parquet files found in %s. Found files: %s",
      transitions_pq_path,
      paste(trans_files, collapse = ", ")
    ))
  }
  message(sprintf(
    "Found %d parquet files in transitions directory",
    length(parquet_files)
  ))

  # --- Set up regions ---
  if (use_regions) {
    regions_path <- file.path(config[["reg_dir"]], "regions.json")
    if (!file.exists(regions_path)) {
      stop(sprintf("Regions file not found at: %s", regions_path))
    }
    regions <- jsonlite::fromJSON(regions_path)
    region_names <- regions$label
    message(sprintf(
      "Processing %d regions: %s\n",
      length(region_names),
      paste(region_names, collapse = ", ")
    ))
  } else {
    region_names <- "National extent"
    message("Processing national extent (no regionalization)\n")
  }

  message(sprintf(
    "Starting processing of %d transitions...\n",
    nrow(transitions_info)
  ))

  # --- Parallel processing of transitions ---

  task_grid <- tidyr::expand_grid(
    transition = transitions_info$Trans_name,
    region = region_names
  )
  message(sprintf(
    "Total transition-region tasks to process: %d",
    nrow(task_grid)
  ))

  # Determine number of cores from SLURM or fallback
  n_cores <- as.integer(Sys.getenv("SLURM_CPUS_PER_TASK", "4"))

  if (n_cores == 1) {
    message("Running sequentially (for debugging)")
    message("Beginning processing of transitions...")
  } else {
    message(sprintf(
      "Using up to %d parallel workers for transition processing",
      n_cores
    ))
    # Use furrr for parallel map
    future::plan(multisession, workers = n_cores)
    options(future.rng.onMisuse = "ignore")
    message("Beginning parallel processing of transitions...")
  }

  # Process transitions - use purrr for sequential, furrr for parallel
  results <- if (n_cores == 1) {
    # Sequential processing with better error messages
    purrr::map_dfr(
      seq_len(nrow(task_grid)),
      function(i) {
        row <- task_grid[i, ]

        log_file <- initialize_worker_log(
          file.path(period_dir, "worker_logs"),
          paste0(row$transition, "_", row$region)
        )

        # Wrap in tryCatch for better error reporting
        tryCatch(
          {
            # Open datasets inside worker (safe, parallel-friendly)
            ds_transitions <- arrow::open_dataset(
              transitions_pq_path,
              format = "parquet",
              partitioning = arrow::hive_partition(region = arrow::int32())
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

            process_single_transition(
              transition_name = row$transition,
              region = row$region,
              use_regions = use_regions,
              ds_transitions = ds_transitions,
              ds_static = ds_static,
              ds_dynamic = ds_dynamic,
              pred_categories = pred_categories,
              period = period,
              config = config,
              period_dir = period_dir,
              save_debug = save_debug,
              do_collinearity = do_collinearity,
              do_grrf = do_grrf,
              refresh_cache = refresh_cache,
              log_file = log_file
            )
          },
          error = function(e) {
            # Print detailed error to console
            cat(sprintf(
              "\n\n========================================\n"
            ))
            cat(sprintf(
              "ERROR processing %s | %s\n",
              row$transition,
              row$region
            ))
            cat(sprintf("========================================\n"))
            cat(sprintf("Error message: %s\n", e$message))
            cat(sprintf("Call: %s\n", paste(deparse(e$call), collapse = " ")))
            cat("\nTraceback:\n")
            print(rlang::trace_back())
            cat("\n========================================\n\n")

            # Also log to file
            error_log_file <- file.path(
              period_dir,
              "worker_logs",
              paste0(row$transition, "_", row$region, "_ERROR.log")
            )
            dir.create(
              dirname(error_log_file),
              recursive = TRUE,
              showWarnings = FALSE
            )
            cat(
              sprintf(
                "Error: %s\nCall: %s\n\nTraceback:\n%s",
                e$message,
                paste(deparse(e$call), collapse = " "),
                paste(capture.output(rlang::trace_back()), collapse = "\n")
              ),
              file = error_log_file
            )

            # Return error summary row
            create_summary_row(
              period = period,
              region = row$region,
              transition_name = row$transition,
              n_observations = 0,
              n_transitions = 0,
              n_initial_predictors = length(pred_categories),
              status = "worker_error",
              error_details = e$message,
              period_dir = period_dir,
              debug_path = NULL,
              save_debug = FALSE
            )
          }
        )
      }
    )
  } else {
    # Parallel processing with furrr
    furrr::future_map_dfr(
      seq_len(nrow(task_grid)),
      function(i) {
        row <- task_grid[i, ]

        # Wrap entire worker in tryCatch to capture any errors
        tryCatch(
          {
            log_file <- initialize_worker_log(
              file.path(period_dir, "worker_logs"),
              paste0(row$transition, "_", row$region)
            )

            # Open datasets inside worker (safe, parallel-friendly)
            ds_transitions <- arrow::open_dataset(
              transitions_pq_path,
              format = "parquet",
              partitioning = arrow::hive_partition(region = arrow::int32())
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

            process_single_transition(
              transition_name = row$transition,
              region = row$region,
              use_regions = use_regions,
              ds_transitions = ds_transitions,
              ds_static = ds_static,
              ds_dynamic = ds_dynamic,
              pred_categories = pred_categories,
              period = period,
              config = config,
              period_dir = period_dir,
              save_debug = save_debug,
              do_collinearity = do_collinearity,
              do_grrf = do_grrf,
              refresh_cache = refresh_cache,
              log_file = log_file
            )
          },
          error = function(e) {
            # Create error summary row
            error_msg <- sprintf(
              "Worker error for %s | %s: %s\nCall: %s",
              row$transition,
              row$region,
              e$message,
              paste(deparse(e$call), collapse = " ")
            )

            # Try to log to file if possible
            tryCatch(
              {
                log_file <- file.path(
                  period_dir,
                  "worker_logs",
                  paste0(row$transition, "_", row$region, "_error.log")
                )
                dir.create(
                  dirname(log_file),
                  recursive = TRUE,
                  showWarnings = FALSE
                )
                cat(error_msg, file = log_file, append = TRUE)
                cat("\n\nFull traceback:\n", file = log_file, append = TRUE)
                cat(
                  paste(capture.output(traceback()), collapse = "\n"),
                  file = log_file,
                  append = TRUE
                )
              },
              error = function(e2) {
                # Silently fail if logging fails
              }
            )

            # Return error summary row
            create_summary_row(
              period = period,
              region = row$region,
              transition_name = row$transition,
              n_observations = 0,
              n_transitions = 0,
              n_initial_predictors = length(pred_categories),
              status = "worker_error",
              error_details = e$message,
              period_dir = period_dir,
              debug_path = NULL,
              save_debug = FALSE
            )
          }
        )
      },
      .options = furrr::furrr_options(seed = TRUE)
    )
  }

  # Return to sequential plan after this period
  future::plan(future::sequential)

  # list all rds files in period_dir
  period_fs_files <- list.files(
    path = period_dir,
    pattern = ".rds$",
    full.names = TRUE
  )

  # Read and combine all period results
  period_summary <- dplyr::bind_rows(purrr::map_dfr(
    period_fs_files,
    function(fs_file) {
      fs_sum <- readRDS(fs_file)
      # if fs_sum is just a df then return it, but if it is a list then return the object fs_sum$summary
      if (is.data.frame(fs_sum)) {
        return(fs_sum)
      } else if (is.list(fs_sum) && "summary" %in% names(fs_sum)) {
        return(fs_sum$summary)
      } else {
        stop(sprintf("Invalid feature selection result format in %s", fs_file))
      }
    }
  ))

  # seperate any entries where status does not equal success
  failed_transitions <- period_summary %>%
    dplyr::filter(status != "success")

  if (nrow(failed_transitions) > 0) {
    message(sprintf(
      "\n%d transitions failed during feature selection:",
      nrow(failed_transitions)
    ))
    print(
      failed_transitions %>%
        dplyr::select(
          period,
          region,
          transition,
          status,
          error_details
        )
    )
    # save failed transitions to a separate rds file
    failed_output_path <- file.path(
      fs_dir,
      sprintf("feature_selection_failed_%s.rds", period)
    )
    saveRDS(failed_transitions, failed_output_path)
  }

  # for successful transitions add extra details from transitions_info, dropping unnecessary columns
  successful_transitions <- period_summary %>%
    dplyr::filter(status == "success") %>%
    dplyr::left_join(
      transitions_info,
      by = c("transition" = "Trans_name")
    ) %>%
    dplyr::select(-c(error_details, Rate, status))

  # Save results
  success_output_path <- file.path(
    fs_dir,
    sprintf(
      "transition_feature_selection_summary_%s.rds",
      period
    )
  )
  saveRDS(successful_transitions, success_output_path)
  message(sprintf(
    "\nFeature selection summary for period %s saved to:\n  %s",
    period,
    success_output_path
  ))
}


#' Process feature selection for a single transition-region (optimized with batch loading)
#' @param transition_name Name of the transition
#' @param region Region name (if applicable)
#' @param use_regions Boolean indicating if regionalization is active
#' @param ds_transitions Arrow dataset for transitions
#' @param ds_static Arrow dataset for static predictors
#' @param ds_dynamic Arrow dataset for dynamic predictors
#' @param pred_categories Named vector of predictor categories
#' @param period Period name
#' @param config Configuration list
#' @param period_dir Directory for debug outputs
#' @param save_debug Boolean indicating if debug outputs should be saved
#' @param refresh_cache Boolean indicating if cached results should be recalculated
#' @return NULL (results saved to disk)
process_single_transition <- function(
  transition_name,
  region,
  use_regions,
  ds_transitions,
  ds_static,
  ds_dynamic,
  pred_categories,
  period,
  config,
  period_dir,
  save_debug = TRUE,
  do_collinearity = TRUE,
  do_grrf = TRUE,
  refresh_cache = FALSE,
  log_file = NULL
) {
  debug_path <- file.path(
    period_dir,
    sprintf("fs_summary_%s_%s.rds", transition_name, region)
  )

  if (!refresh_cache && file.exists(debug_path)) {
    log_msg(
      sprintf(
        "Loading cached results for %s | %s",
        transition_name,
        if (use_regions) region else "National"
      ),
      log_file
    )
    return(readRDS(debug_path))
  }

  if (refresh_cache && file.exists(debug_path)) {
    log_msg(
      sprintf(
        "Cache exists but refresh=TRUE. Recomputing %s | %s",
        transition_name,
        region
      ),
      log_file
    )
  }

  log_msg(
    sprintf(
      "\n===== %s | Region: %s =====",
      transition_name,
      if (use_regions) region else "National"
    ),
    log_file
  )

  # Resolve region_value if requested
  region_value <- NULL
  if (use_regions) {
    regions_path <- file.path(config[["reg_dir"]], "regions.json")
    if (!file.exists(regions_path)) {
      stop(sprintf("Regions file not found at: %s", regions_path))
    }
    regions <- jsonlite::fromJSON(regions_path)
    region_value <- as.integer(regions$value[match(region, regions$label)])
    if (is.na(region_value) || length(region_value) != 1) {
      stop(sprintf("Invalid region mapping for region '%s'", region))
    }
  }

  # STEP 1: Load transition table (cell_id, response)
  trans_df <- load_transition_data(
    ds = ds_transitions,
    transition_name,
    region_value,
    use_regions,
    log_file = log_file
  )

  if (nrow(trans_df) == 0) {
    return(create_summary_row(
      period,
      region,
      transition_name,
      0,
      0,
      length(pred_categories),
      status = "no_transition_data",
      period_dir = period_dir,
      debug_path = debug_path,
      save_debug = save_debug
    ))
  }
  # STEP 2: Non-random downsampling to 1.5x majority-to-minority ratio
  log_msg(
    "Applying non-random downsampling (majority = 1.5 × minority)",
    log_file
  )

  # Count each class
  n1 <- sum(trans_df$response == 1)
  n0 <- sum(trans_df$response == 0)

  if (n1 == 0 | n0 == 0) {
    log_msg("One class is empty — skipping downsampling.", log_file)
  } else if (n0 > (1.5 * n1)) {
    # Majority is 0 — keep all 1s, thin out 0s to 1.5 × n1
    idx1 <- which(trans_df$response == 1)
    idx0 <- which(trans_df$response == 0)

    target_majority <- ceiling(1.5 * n1)

    # Sort majority by stable spatial ID (or coordinate proxy)
    ord <- order(trans_df$cell_id[idx0])
    idx0_sorted <- idx0[ord]

    # Evenly spaced thinning to preserve coverage
    spacing <- ceiling(length(idx0_sorted) / target_majority)
    keep_0 <- idx0_sorted[seq(1, length(idx0_sorted), by = spacing)]

    # Combine and subset
    keep_idx <- c(idx1, keep_0)
    trans_df <- trans_df[keep_idx, , drop = FALSE]

    log_msg(
      sprintf(
        "Downsampled -> %d rows (1s=%d, 0s=%d, ratio=%.2f)",
        nrow(trans_df),
        sum(trans_df$response == 1),
        sum(trans_df$response == 0),
        sum(trans_df$response == 0) / sum(trans_df$response == 1)
      ),
      log_file
    )
  } else {
    log_msg(
      "Majority already ≤ 1.5× minority — no downsampling applied.",
      log_file
    )
  }

  # index by cell_id
  trans_df <- trans_df %>%
    dplyr::arrange(cell_id) %>%
    dplyr::distinct(cell_id, .keep_all = TRUE)

  # Build cell_id-aligned response and weight tables
  class_counts <- table(trans_df$response)
  weights <- setNames(max(class_counts) / class_counts, names(class_counts))
  weight_vec <- weights[as.character(trans_df$response)]
  weights_df <- tibble::tibble(cell_id = trans_df$cell_id, weight = weight_vec)

  # Basic validation
  n_trans <- sum(trans_df$response == 1)
  if (nrow(trans_df) < 100 || n_trans < 10) {
    return(create_summary_row(
      period,
      region,
      transition_name,
      nrow(trans_df),
      n_trans,
      length(pred_categories),
      status = if (nrow(trans_df) < 100) {
        "insufficient_observations"
      } else {
        "insufficient_transitions"
      },
      period_dir = period_dir,
      debug_path = debug_path,
      save_debug = save_debug
    ))
  }

  log_msg(
    sprintf(
      "Data ready: %d obs | %d transitions | %d candidate predictors",
      nrow(trans_df),
      n_trans,
      length(pred_categories)
    ),
    log_file
  )

  # STEP 3: Category-wise collinearity filtering (batched)
  collin_selected <- names(pred_categories)
  if (do_collinearity) {
    log_msg(
      "Running category-wise collinearity filtering (batched)...",
      log_file
    )

    collin_result <- tryCatch(
      category_wise_collin_filter_batched(
        ds_static = ds_static,
        ds_dynamic = ds_dynamic,
        predictor_names = names(pred_categories),
        categories = pred_categories,
        response_df = trans_df,
        weights_df = weights_df,
        region_value = region_value,
        scenario = "baseline",
        corcut = 0.7,
        log_file = log_file
      ),
      error = function(e) {
        log_msg(
          sprintf("Collinearity filtering exception: %s", e$message),
          log_file
        )
        list(selected = character(0), error_log = list(all = e$message))
      }
    )

    # collin_result is a list(selected=..., category_results=..., error_log=...)
    collin_selected <- if (
      is.list(collin_result) && "selected" %in% names(collin_result)
    ) {
      collin_result$selected
    } else {
      character(0)
    }

    if (length(collin_selected) == 0) {
      details <- if (
        is.list(collin_result) && "error_log" %in% names(collin_result)
      ) {
        paste(unlist(collin_result$error_log), collapse = "; ")
      } else {
        "no predictors selected"
      }
      log_msg("  FAILED: No predictors passed collinearity stage", log_file)
      log_msg(sprintf("  Details: %s", details), log_file)
      return(create_summary_row(
        period,
        region,
        transition_name,
        nrow(trans_df),
        n_trans,
        length(pred_categories),
        n_after_collinearity = 0,
        status = "collinearity_filtering_failed",
        error_details = details,
        period_dir = period_dir,
        debug_path = debug_path,
        save_debug = save_debug
      ))
    }

    log_msg(
      sprintf(
        "Selected %d predictors after collinearity",
        length(collin_selected)
      ),
      log_file
    )
  } else {
    log_msg(
      "Skipping collinearity filtering (do_collinearity = FALSE)",
      log_file
    )
  }

  # STEP 4: GRRF selection
  grrf_selected <- collin_selected
  grrf_error <- NULL

  if (do_grrf) {
    log_msg(
      sprintf(
        "Preparing %d predictors for GRRF",
        length(collin_selected)
      ),
      log_file
    )

    preds_df <- load_predictor_data(
      ds_static = ds_static,
      ds_dynamic = ds_dynamic,
      cell_ids = trans_df$cell_id,
      preds = collin_selected,
      region_value = region_value,
      scenario = "baseline",
      log_file = log_file
    )

    # Join predictors to response
    joined <- dplyr::inner_join(preds_df, trans_df, by = "cell_id")

    # Ensure binary numeric response
    joined$response <- as.numeric(joined$response)
    if (!all(joined$response %in% c(0, 1))) {
      stop("Response column contains non-binary values after join")
    }

    if (nrow(joined) == 0) {
      log_msg("Zero rows remain after join. Likely misalignment.", log_file)
      grrf_selected <- character(0)
    } else {
      # Call GRRF with tryCatch
      grrf_selected <- tryCatch(
        grrff_filter(
          joined = joined,
          predictor_cols = collin_selected,
          response_vec = joined$response,
          gamma = 0.5,
          log_file = log_file
        ),
        error = function(e) {
          grrf_error <<- e$message
          log_msg(sprintf("GRRF selection failed: %s", e$message), log_file)
          character(0)
        }
      )
    }

    if (length(grrf_selected) == 0) {
      log_msg(
        sprintf("GRRF failed for %s | %s", transition_name, region),
        log_file
      )
    } else {
      log_msg(
        sprintf("GRRF selected %d predictors", length(grrf_selected)),
        log_file
      )
    }
  }

  # STEP 5: Update focal predictors
  focal_preds <- grep("nhood", grrf_selected, value = TRUE)
  if (length(focal_preds) > 0) {
    tryCatch(
      update_focal_lookup(
        focal_preds,
        period,
        transition_name,
        region,
        config,
        log_file
      ),
      error = function(e) {
        log_msg(sprintf("update_focal_lookup warning: %s", e$message), log_file)
      }
    )
  }

  # STEP 6: Create summary row and save debug
  summary_row <- create_summary_row(
    period,
    region,
    transition_name,
    nrow(trans_df),
    n_trans,
    length(pred_categories),
    n_after_collinearity = length(collin_selected),
    n_after_grrf = length(grrf_selected),
    selected_predictors = paste(grrf_selected, collapse = "; "),
    focal_predictors = paste(focal_preds, collapse = "; "),
    status = ifelse(length(grrf_selected) > 0, "success", "grrf_failed"),
    error_details = grrf_error,
    period_dir = period_dir,
    debug_path = debug_path,
    save_debug = TRUE, # Always save to include collin-selected predictors
    selected_predictors_collinearity = paste(collin_selected, collapse = "; "),
    selected_predictors_grrf = paste(grrf_selected, collapse = "; ")
  )

  if (save_debug) {
    tryCatch(saveRDS(summary_row, debug_path), error = function(e) {
      log_msg(sprintf("Failed to save debug RDS: %s", e$message), log_file)
    })
  }
}


#' Category-wise collinearity filtering with batched predictor loading
#' @param ds_static Arrow dataset for static predictors
#' @param ds_dynamic Arrow dataset for dynamic predictors
#' @param predictor_names Vector of all predictor names
#' @param categories Named vector of predictor categories
#' @param response_vec Response vector
#' @param weights Weight vector
#' @param region_value Region value for filtering
#' @param scenario Scenario name
#' @param corcut Correlation cutoff threshold
#' @return Character vector of selected predictor names with diagnostic attributes
category_wise_collin_filter_batched <- function(
  ds_static,
  ds_dynamic,
  predictor_names,
  categories,
  response_df,
  weights_df = NULL,
  region_value = NULL,
  scenario = NULL,
  corcut = 0.7,
  log_file = NULL
) {
  predictor_map <- split(predictor_names, categories[predictor_names])
  all_selected <- c()
  cat_res <- list()
  errors <- list()

  for (cat in names(predictor_map)) {
    preds <- predictor_map[[cat]]
    log_msg(sprintf("[CAT] %s | %d predictors", cat, length(preds)), log_file)

    if (length(preds) == 0) {
      cat_res[[cat]] <- list(n_input = 0, n_selected = 0, error = "no_preds")
      next
    }

    pred_df <- load_predictor_data(
      ds_static = ds_static,
      ds_dynamic = ds_dynamic,
      cell_ids = response_df$cell_id,
      preds = preds,
      region_value = region_value,
      scenario = scenario,
      log_file = log_file
    )

    if (nrow(pred_df) == 0) {
      errors[[cat]] <- "no_pred_rows"
      cat_res[[cat]] <- list(
        n_input = length(preds),
        n_selected = 0,
        error = "empty_pred_df"
      )
      next
    }

    joined <- pred_df %>% dplyr::inner_join(response_df, by = "cell_id")
    if (!is.null(weights_df)) {
      joined <- joined %>% dplyr::left_join(weights_df, by = "cell_id")
    }

    if (nrow(joined) == 0) {
      errors[[cat]] <- "join_zero"
      cat_res[[cat]] <- list(
        n_input = length(preds),
        n_selected = 0,
        error = "zero_after_join"
      )
      next
    }

    pred_cols <- intersect(preds, names(joined))
    if (length(pred_cols) == 0) {
      errors[[cat]] <- "no_pred_cols"
      cat_res[[cat]] <- list(
        n_input = length(preds),
        n_selected = 0,
        error = "no_columns"
      )
      next
    }

    # Call collin_filter with tryCatch for error handling
    res <- tryCatch(
      collin_filter(
        joined,
        pred_cols,
        response_col = "response",
        weight_col = "weight",
        corcut = corcut,
        log_file = log_file
      ),
      error = function(e) {
        list(selected = character(0), diagnostics = list(error = e$message))
      }
    )

    # extract selected predictors
    sel <- res$selected
    if (length(sel) > 0) {
      all_selected <- c(all_selected, sel)
    }

    # add category results along with diagnostics
    cat_res[[cat]] <- list(
      n_input = length(preds),
      n_selected = length(sel),
      error = res$diagnostics$error %||% NULL
    )
    if (length(sel) == 0) {
      errors[[cat]] <- res$diagnostics$error %||% "no_selection"
    }
  }

  # return results from accross categories, within categories and errors
  list(
    selected = unique(all_selected),
    category_results = cat_res,
    error_log = errors
  )
}


#' Filter covariates based on collinearity (binary outcome)
#'
#' @param joined Data frame with predictors and response
#' @param pred_names Column names of predictors to filter
#' @param response_vec Response vector (binary 0/1)
#' @param weights Weight vector
#' @param corcut Correlation cutoff threshold
#' @return Character vector of selected predictor names (never a list)
collin_filter <- function(
  joined,
  pred_names,
  response_col = "response",
  weight_col = NULL,
  corcut = 0.7,
  log_file = NULL
) {
  stopifnot("cell_id" %in% names(joined))
  stopifnot(response_col %in% names(joined))

  pred_names <- dplyr::intersect(pred_names, names(joined))
  if (length(pred_names) == 0) {
    return(list(
      selected = character(0),
      diagnostics = list(error = "no_predictors")
    ))
  }

  joined <- joined %>% dplyr::filter(!is.na(.data[[response_col]]))
  joined$weight <- if (!is.null(weight_col) && weight_col %in% names(joined)) {
    joined[[weight_col]]
  } else {
    1
  }

  failed <- c()
  reasons <- c()
  pvs <- setNames(rep(1, length(pred_names)), pred_names)

  for (p in pred_names) {
    x <- joined[[p]]
    keep <- !is.na(x) & !is.na(joined[[response_col]])
    x <- x[keep]
    y <- joined[[response_col]][keep]
    w <- joined$weight[keep]

    if (length(unique(x)) < 3) {
      failed <- c(failed, p)
      reasons <- c(reasons, "low_var")
      next
    }
    if (length(x) < 10) {
      failed <- c(failed, p)
      reasons <- c(reasons, "too_few_obs")
      next
    }

    fit <- tryCatch(
      glm(
        y ~ if (length(unique(x)) > 5) poly(x, 2, raw = TRUE) else x,
        family = binomial(),
        weights = w
      ),
      error = function(e) e
    )

    if (inherits(fit, "error")) {
      failed <- c(failed, p)
      reasons <- c(reasons, "glm_fail")
      next
    }

    sm <- summary(fit)$coefficients
    pv <- max(sm[-1, 4], na.rm = TRUE)
    pvs[p] <- pv
  }

  ok <- names(pvs[pvs < 1.0])
  if (length(ok) == 0) {
    return(list(
      selected = character(0),
      diagnostics = list(
        failed = failed,
        reason = reasons,
        error = "all_glm_failed"
      )
    ))
  }

  ok_sorted <- names(sort(pvs[ok]))

  # Correlation filter
  m <- suppressWarnings(abs(cor(
    joined %>% dplyr::select(all_of(ok_sorted)),
    use = "pairwise.complete.obs"
  )))
  m[is.na(m)] <- 0

  sel <- c()
  rem <- ok_sorted
  while (length(rem) > 0) {
    cur <- rem[1]
    sel <- c(sel, cur)
    if (length(rem) == 1) {
      break
    }
    cors <- m[cur, rem[-1]]
    rem <- rem[-1][cors <= corcut]
  }

  list(
    selected = sel,
    diagnostics = list(failed = failed, reason = reasons, pvals = pvs)
  )
}


#' Load transition data from the parquet file for a specific transition and region
#' @param ds_transitions Arrow dataset for transitions
#' @param transition_name Name of the transition
#' @param region_value Numeric region value (if applicable)
#' @param use_regions Boolean indicating if regionalization is active
#' @return Data frame with cell_id and response columns
load_transition_data <- function(
  ds,
  transition_name,
  region_value = NULL,
  use_regions = FALSE,
  log_file = NULL
) {
  log_msg(
    sprintf(
      "[TRANS] Loading %s | region=%s",
      transition_name,
      ifelse(is.null(region_value), "ALL", region_value)
    ),
    log_file
  )

  q <- ds %>% dplyr::select(cell_id, region, all_of(transition_name))
  log_msg("  Query constructed", log_file)

  if (use_regions && !is.null(region_value)) {
    q <- q %>% dplyr::filter(region == !!region_value)
  }
  log_msg("  Region filter applied", log_file)

  out <- tryCatch(
    {
      q %>%
        dplyr::filter(!is.na(.data[[transition_name]])) %>%
        dplyr::collect() %>%
        dplyr::select(cell_id, response = all_of(transition_name), region) %>%
        dplyr::distinct(cell_id, .keep_all = TRUE) %>%
        dplyr::arrange(cell_id)
    },
    error = function(e) {
      log_msg(
        paste(
          "Failed to load transition:",
          transition_name,
          "|",
          e$message
        ),
        log_file
      )
      tibble::tibble(cell_id = integer(), response = integer())
    }
  )

  if (nrow(out) == 0) {
    log_msg(sprintf("[TRANS] ZERO ROWS for %s", transition_name), log_file)
  }

  out
}


#' Load predictor data (both static and dynamic) from the parquet file for specific cell IDs
#' @param ds_static Arrow dataset for static predictors
#' @param ds_dynamic_period Arrow dataset for dynamic predictors
#' @param cell_ids Vector of cell IDs to load
#' @param preds Vector of predictor variable names to load
#' @param region_value Numeric region value (if applicable)
#' @param scenario Scenario name (if applicable)
#' @return Data frame with cell_id and predictor columns
load_predictor_data <- function(
  ds_static,
  ds_dynamic,
  cell_ids,
  preds,
  region_value = NULL,
  scenario = NULL,
  log_file = NULL
) {
  preds <- unique(as.character(preds))
  if (length(preds) == 0) {
    return(tibble(cell_id = integer()))
  }

  static_cols <- intersect(preds, names(ds_static$schema))
  dyn_cols <- intersect(preds, names(ds_dynamic$schema))

  q_static <- ds_static %>% dplyr::select(cell_id, region, all_of(static_cols))
  q_dyn <- ds_dynamic %>%
    dplyr::select(cell_id, region, scenario, all_of(dyn_cols))

  if (!is.null(region_value)) {
    q_static <- q_static %>% dplyr::filter(region == !!region_value)
    q_dyn <- q_dyn %>% dplyr::filter(region == !!region_value)
  }
  if (!is.null(scenario)) {
    q_dyn <- q_dyn %>% dplyr::filter(.data$scenario == !!scenario)
  }

  q_static <- q_static %>% dplyr::filter(cell_id %in% !!cell_ids)
  q_dyn <- q_dyn %>% dplyr::filter(cell_id %in% !!cell_ids)

  static_df <- if (length(static_cols) > 0) {
    q_static %>%
      dplyr::collect() %>%
      dplyr::select(-region) %>%
      dplyr::distinct(cell_id, .keep_all = TRUE) %>%
      dplyr::arrange(cell_id)
  } else {
    tibble::tibble(cell_id = integer())
  }

  dyn_df <- if (length(dyn_cols) > 0) {
    q_dyn %>%
      dplyr::collect() %>%
      dplyr::select(-region, -scenario) %>%
      dplyr::distinct(cell_id, .keep_all = TRUE) %>%
      dplyr::arrange(cell_id)
  } else {
    tibble::tibble(cell_id = integer())
  }

  joined <- dplyr::full_join(static_df, dyn_df, by = "cell_id")
  keep <- c("cell_id", intersect(preds, names(joined)))

  df <- joined %>% dplyr::select(all_of(keep)) %>% dplyr::arrange(cell_id)

  # Enforce numeric predictors
  for (nm in setdiff(names(df), "cell_id")) {
    if (!is.numeric(df[[nm]])) {
      tmp <- suppressWarnings(as.numeric(df[[nm]]))
      if (all(is.na(tmp) == is.na(df[[nm]]))) {
        df[[nm]] <- tmp
        log_msg(sprintf("[PRED] Coerced '%s' to numeric", nm), log_file)
      } else {
        stop(sprintf("[PRED] Column '%s' cannot be coerced to numeric", nm))
      }
    }
  }

  df
}


#' Guided Regularized Random Forest feature selection (optimized - returns names only)
#'
#' @param joined Full joined data frame (cell_id + response + predictors)
#' @param predictor_cols Column names of predictors to use
#' @param response_vec Response vector (binary 0/1)
#' @param gamma Importance coefficient (0-1)
#' @return Character vector of selected predictor names
grrff_filter <- function(
  joined,
  predictor_cols,
  response_vec,
  gamma = 0.5,
  log_file = NULL
) {
  # Extract only the needed predictors
  cov_mat <- as.data.frame(joined[, predictor_cols, drop = FALSE])
  response_fac <- droplevels(as.factor(response_vec))

  rm(response_vec)
  gc(verbose = FALSE)

  # Remove rows with NAs
  na_rows <- rowSums(is.na(cov_mat)) > 0
  if (any(na_rows)) {
    log_msg(
      sprintf("  Removing %d rows with missing values", sum(na_rows)),
      log_file
    )
    cov_mat <- cov_mat[!na_rows, , drop = FALSE]
    response_fac <- response_fac[!na_rows]
    rm(na_rows)
    gc(verbose = FALSE)

    if (nrow(cov_mat) < 100) {
      log_msg("Insufficient complete cases after removing NAs", log_file)
      rm(cov_mat, response_fac)
      gc(verbose = FALSE)
      return(character(0))
    }
  }

  # Remove zero-variance predictors
  zero_var <- vapply(cov_mat, function(x) length(unique(x)) <= 1, logical(1))
  if (any(zero_var)) {
    log_msg(
      sprintf("  Removing %d zero-variance predictors", sum(zero_var)),
      log_file
    )
    cov_mat <- cov_mat[, !zero_var, drop = FALSE]
    predictor_cols <- predictor_cols[!zero_var]
    rm(zero_var)
    gc(verbose = FALSE)

    if (ncol(cov_mat) == 0) {
      log_msg(
        "No predictors left after removing zero-variance columns",
        log_file
      )
      rm(cov_mat, response_fac)
      gc(verbose = FALSE)
      return(character(0))
    }
  }

  # Compute class-level weights for GRRF
  class_counts <- table(response_fac)
  classwt <- setNames(max(class_counts) / class_counts, names(class_counts))

  # Adaptive mtry
  n_pred <- ncol(cov_mat)
  mtry_val <- min(max(floor(sqrt(n_pred)), 1), n_pred)

  # Initial RF to get variable importance
  log_msg(
    sprintf(
      "  Running initial RF with %d predictors and %d observations",
      n_pred,
      nrow(cov_mat)
    ),
    log_file
  )

  rf <- RRF::RRF(
    x = cov_mat,
    y = response_fac,
    flagReg = 0,
    ntree = 100,
    mtry = mtry_val,
    nodesize = 10,
    keep.forest = FALSE,
    keep.inbag = FALSE
  )

  imp_raw <- rf$importance[, "MeanDecreaseGini"]
  imp_norm <- imp_raw / max(imp_raw)

  coef_reg <- (1 - gamma) + gamma * imp_norm

  rm(rf, imp_raw)
  gc(verbose = FALSE)

  # Guided Regularized RF
  log_msg("  Running guided regularized RF", log_file)

  mdl_rf <- tryCatch(
    {
      RRF::RRF(
        x = cov_mat,
        y = response_fac,
        classwt = classwt,
        coefReg = coef_reg,
        flagReg = 1,
        ntree = 150,
        mtry = mtry_val,
        nodesize = 10,
        keep.forest = FALSE,
        keep.inbag = FALSE
      )
    },
    error = function(e) {
      log_msg(sprintf("GRRF failed: %s", e$message), log_file)
      return(NULL)
    }
  )

  if (is.null(mdl_rf)) {
    rm(cov_mat, response_fac, coef_reg, imp_norm)
    gc(verbose = FALSE)
    return(character(0))
  }

  # Extract predictors with positive importance
  rf_imp <- mdl_rf$importance[, "MeanDecreaseGini"]
  selected_names <- names(rf_imp[rf_imp > 0])

  # Sort by importance descending
  result <- selected_names[order(rf_imp[selected_names], decreasing = TRUE)]

  # Clean up memory
  rm(mdl_rf, rf_imp, selected_names, cov_mat, response_fac, coef_reg, imp_norm)
  gc(verbose = FALSE)

  return(result)
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

#' Create a standardized summary row
create_summary_row <- function(
  period,
  region,
  transition_name,
  n_observations,
  n_transitions,
  n_initial_predictors,
  n_after_collinearity = NA,
  n_after_grrf = NA,
  selected_predictors = "",
  focal_predictors = "",
  status = "success",
  error_details = NA_character_,
  period_dir = NULL,
  debug_path = NULL,
  save_debug = TRUE,
  selected_predictors_collinearity = NULL,
  selected_predictors_grrf = NULL,
  log_file = NULL
) {
  summary_row <- tibble::tibble(
    period = period,
    region = region,
    transition = transition_name,
    n_observations = n_observations,
    n_transitions = n_transitions,
    n_initial_predictors = n_initial_predictors,
    n_after_collinearity = n_after_collinearity,
    n_after_grrf = n_after_grrf,
    selected_predictors_collinearity = if (
      is.null(selected_predictors_collinearity)
    ) {
      NA_character_
    } else {
      selected_predictors_collinearity
    },
    selected_predictors_grrf = if (is.null(selected_predictors_grrf)) {
      NA_character_
    } else {
      selected_predictors_grrf
    },
    selected_predictors = selected_predictors,
    focal_predictors = focal_predictors,
    status = status,
    error_details = if (is.null(error_details) || length(error_details) == 0) {
      NA_character_
    } else {
      error_details
    }
  )

  if (save_debug && !is.null(debug_path)) {
    # Ensure directory exists
    period_dir_actual <- dirname(debug_path)
    if (!dir.exists(period_dir_actual)) {
      dir.create(period_dir_actual, recursive = TRUE, showWarnings = FALSE)
    }

    debug_data <- list(
      summary = summary_row,
      timestamp = Sys.time()
    )

    saveRDS(debug_data, debug_path)
    log_msg(
      sprintf("  Debug info saved to: %s", basename(debug_path)),
      log_file
    )
  }

  return(summary_row)
}

#' Update focal layer lookup
update_focal_lookup <- function(
  focal_preds,
  period,
  transition,
  region,
  config,
  log_file = NULL
) {
  lookup_path <- file.path(
    config[["preds_tools_dir"]],
    "neighbourhood_details_for_dynamic_updating",
    "focal_layer_lookup.rds"
  )

  if (!file.exists(lookup_path)) {
    log_msg("Focal layer lookup file not found", log_file)
    return(invisible(NULL))
  }

  focal_lookup <- readRDS(lookup_path) %>% dplyr::filter(period == !!period)

  focal_subset <- focal_lookup %>%
    dplyr::filter(grepl(paste(focal_preds, collapse = "|"), layer_name)) %>%
    dplyr::mutate(transition = transition, region = region)

  output_path <- file.path(
    config[["preds_tools_dir"]],
    "neighbourhood_details_for_dynamic_updating",
    sprintf("%s_focals_for_updating.rds", period)
  )

  if (file.exists(output_path)) {
    existing <- readRDS(output_path)
    focal_subset <- dplyr::bind_rows(existing, focal_subset)
  }

  saveRDS(focal_subset, output_path)
  invisible(NULL)
}
