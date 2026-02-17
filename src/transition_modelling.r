# library(tidymodels)
# library(ranger)
# library(glmnet)
# library(xgboost)
# library(purrr)
# library(dplyr)
# library(yardstick)

#' Transition modelling for Land Use Land Cover Change
#' @param config A list containing configuration parameters
#' @param refresh_cache Logical, whether to refresh cached datasets and overwrite existing model files
#' @param model_dir Directory to save trained models
#' @param eval_dir Directory to save model evaluation results
#' @param use_regions Logical, whether to use regionalization
#' @param model_specs_path Path to model specifications YAML file
#' @param periods_to_process Character vector of time periods to process
#' @return None. Saves model evaluation summary to eval_dir.
#' @details
#' This function orchestrates the transition modelling process for specified time periods.
#' It reads model specifications, processes each period sequentially to manage memory usage,
#' and saves the combined evaluation results.
#'
#' The function includes intelligent caching: if a model file already exists for a
#' transition-region combination, it will be skipped unless refresh_cache = TRUE.
#' This allows for efficient re-runs when only some models need to be updated.
#' @export
transition_modelling <- function(
  config = get_config(),
  refresh_cache = FALSE,
  model_dir = config[["transition_model_dir"]],
  eval_dir = config[["transition_model_eval_dir"]],
  use_regions = config[["regionalization"]],
  model_specs_path = config[["model_specs_path"]],
  periods_to_process = config[["data_periods"]]
) {
  # create model and eval directories if they do not exist
  ensure_dir(model_dir)
  ensure_dir(eval_dir)

  # read the model_specs yaml file
  models_specs <- yaml::yaml.load_file(model_specs_path)

  message(sprintf(
    "Processing %d periods: %s\n",
    length(periods_to_process),
    paste(periods_to_process, collapse = ", ")
  ))
  message(sprintf(
    "Regionalization: %s\n",
    ifelse(use_regions, "ENABLED", "DISABLED")
  ))

  # Process each period (sequential to avoid memory issues with large datasets)
  results_list <- purrr::map(
    periods_to_process,
    function(period) {
      perform_transition_modelling(
        period = period,
        use_regions = use_regions,
        config = config,
        model_dir = model_dir,
        eval_dir = eval_dir,
        refresh_cache = refresh_cache
      )
    }
  )

  # Combine all period results
  final_summary <- dplyr::bind_rows(results_list)

  # Save results
  output_path <- file.path(
    eval_dir,
    "transition_modelling_evalaution_summary.rds"
  )
  saveRDS(final_summary, output_path)

  message("Transition modelling completed for all specified periods.")
}

#' Wrapper function to perform transition modelling for a given period
#' @param period Character string specifying the time period for modelling
#' @param use_regions Logical, whether to use regionalization
#' @param config A list containing configuration parameters
#' @param refresh_cache Logical, whether to refresh cached datasets
#' @return A list of model evaluation results
perform_transition_modelling <- function(
  period,
  use_regions,
  config,
  model_dir = model_dir,
  eval_dir = eval_dir,
  refresh_cache = FALSE,
  model_specs_path = config[["model_specs_path"]]
) {
  # Directories for saving models, evaluations, and debug info
  model_dir <- file.path(
    config[["transition_model_dir"]],
    period
  )
  eval_dir <- file.path(
    config[["transition_model_eval_dir"]],
    period
  )
  debug_dir <- file.path(
    model_dir,
    "debug_logs"
  )
  ensure_dir(model_dir)
  ensure_dir(eval_dir)
  ensure_dir(debug_dir)

  # Load predictor table
  pred_table_raw <- yaml::yaml.load_file(config[["pred_table_path"]])
  message("Loaded predictor table")

  # remove all entries without a path
  pred_table_raw <- pred_table_raw[sapply(pred_table_raw, function(x) {
    !is.null(x$path)
  })]

  # get period specific predictors
  period_preds <- pred_table_raw[sapply(pred_table_raw, function(x) {
    x$period == period
  })]

  # get static predictors
  static_preds <- pred_table_raw[sapply(pred_table_raw, function(x) {
    x$period == "all"
  })]

  # Combine predictor lists
  pred_table <- c(static_preds, period_preds)

  # load summary of transition feature selection
  fs_summary <- readRDS(
    file.path(
      config[["feature_selection_dir"]],
      sprintf(
        "transition_feature_selection_summary_%s.rds",
        period
      )
    )
  )

  # Filter out transitions that failed feature selection
  fs_summary_all <- fs_summary
  fs_summary <- fs_summary %>%
    dplyr::filter(
      !is.na(selected_predictors) &
        selected_predictors != "" &
        nchar(trimws(selected_predictors)) > 0
    ) %>%
    # Order by increasing number of observations for quick testing
    dplyr::arrange(n_observations)

  message(sprintf(
    "Loaded feature selection summary: %d total combinations, %d passed feature selection",
    nrow(fs_summary_all),
    nrow(fs_summary)
  ))

  if (nrow(fs_summary) == 0) {
    stop(
      "No transitions passed feature selection. Check feature selection results."
    )
  }

  message(sprintf(
    "Processing %d successful transitions, ordered by n_observations (%d to %d)",
    nrow(fs_summary),
    min(fs_summary$n_observations),
    max(fs_summary$n_observations)
  ))

  # Parquet file paths
  transitions_pq_path <- file.path(
    config[["trans_pre_pred_filter_dir"]],
    period
  )
  static_preds_pq_path <- file.path(
    config[["predictors_prepped_dir"]],
    "parquet_data",
    "static"
  )

  # Extract first year from period string for dynamic predictors
  period_start_year <- as.integer(stringr::str_extract(period, "^[0-9]{4}"))

  dynamic_preds_pq_path <- file.path(
    config[["predictors_prepped_dir"]],
    "parquet_data",
    "dynamic",
    period_start_year
  )

  # Verify files exist with informative error messages
  missing_paths <- c()

  if (!file.exists(transitions_pq_path)) {
    missing_paths <- c(
      missing_paths,
      sprintf(
        "Transitions: %s",
        transitions_pq_path
      )
    )
  }

  if (!file.exists(static_preds_pq_path)) {
    missing_paths <- c(
      missing_paths,
      sprintf(
        "Static predictors: %s",
        static_preds_pq_path
      )
    )
  }

  if (!file.exists(dynamic_preds_pq_path)) {
    missing_paths <- c(
      missing_paths,
      sprintf(
        "Dynamic predictors: %s",
        dynamic_preds_pq_path
      )
    )
  }

  if (length(missing_paths) > 0) {
    error_msg <- sprintf(
      "Required parquet data directories not found:\n  %s\n\nPlease ensure data preparation steps have been completed.",
      paste(missing_paths, collapse = "\n  ")
    )
    message(error_msg)
    stop(error_msg)
  }

  message(sprintf(
    "Data paths verified:\n  Transitions: %s\n  Static: %s\n  Dynamic: %s",
    transitions_pq_path,
    static_preds_pq_path,
    dynamic_preds_pq_path
  ))

  # --- Set up regions ---
  if (use_regions) {
    regions <- jsonlite::fromJSON(file.path(
      config[["reg_dir"]],
      "regions.json"
    ))
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

  # --- Parallel processing of transitions ---

  # Determine number of cores from SLURM or fallback (default to 1 for safety)
  n_cores <- as.integer(Sys.getenv("SLURM_CPUS_PER_TASK", "1"))

  # Use safer parallel strategy
  if (n_cores > 1 && .Platform$OS.type == "unix") {
    # Use multicore on Unix systems
    future::plan(future::multicore, workers = n_cores)
    message(sprintf("Using multicore with %d parallel workers", n_cores))
  } else if (n_cores > 1) {
    # Use multisession on Windows (safer)
    future::plan(future::multisession, workers = n_cores)
    message(sprintf("Using multisession with %d parallel workers", n_cores))
  } else {
    # Single-threaded for safety
    future::plan(future::sequential)
    message("Using single-threaded processing")
  }

  options(future.rng.onMisuse = "ignore")

  # Build vector of row indices to iterate over
  task_ids <- seq_len(nrow(fs_summary))

  message(sprintf(
    "Starting processing of %d transitions...\n",
    length(task_ids)
  ))

  # Parallel over region × transition combinations from fs_summary
  transitions_model_results <- furrr::future_map_dfr(
    task_ids,
    function(i) {
      # Extract the row (this row defines ONE task)
      task <- fs_summary[i, ]
      trans_name <- task$transition
      region <- task$region

      # Create worker-specific log file
      log_file <- initialize_worker_log(
        file.path(debug_dir),
        paste0(trans_name, "_", region)
      )

      log_msg(
        sprintf("Starting task: transition=%s region=%s", trans_name, region),
        log_file
      )

      # --- Each worker opens its own datasets ---
      log_msg("Opening Arrow datasets...", log_file)

      ds_transitions <- arrow::open_dataset(
        transitions_pq_path,
        partitioning = arrow::hive_partition(region = arrow::int32())
      )

      ds_static <- arrow::open_dataset(
        static_preds_pq_path,
        partitioning = arrow::hive_partition(region = arrow::int32())
      )

      ds_dynamic <- arrow::open_dataset(
        dynamic_preds_pq_path,
        partitioning = arrow::hive_partition(
          scenario = arrow::utf8(),
          region = arrow::int32()
        )
      )

      log_msg("Arrow datasets opened successfully\n", log_file)

      # --- Run the actual model ---
      tryCatch(
        {
          model_single_transition(
            trans_name = trans_name,
            refresh_cache = refresh_cache,
            region = region,
            use_regions = use_regions,
            ds_transitions = ds_transitions,
            ds_static = ds_static,
            ds_dynamic = ds_dynamic,
            period = period,
            config = config,
            model_dir = model_dir,
            eval_dir = eval_dir,
            log_file = log_file,
            fs_summary = fs_summary
          )
        },
        error = function(e) {
          error_msg <- sprintf(
            "ERROR in transition modelling for %s-%s: %s",
            trans_name,
            region,
            e$message
          )
          log_msg(error_msg, log_file)
          log_msg(
            sprintf(
              "Full error traceback: %s",
              paste(traceback(), collapse = "\n")
            ),
            log_file
          )

          # Return error result instead of stopping
          list(
            transition = trans_name,
            region = region,
            status = "error",
            error_message = e$message,
            cv_metrics = NULL,
            test_metrics = NULL
          )
        }
      )
    },
    .options = furrr::furrr_options(seed = TRUE)
  )

  future::plan(future::sequential) # Reset to sequential

  # Log summary of processed vs skipped models
  total_tasks <- length(task_ids)
  skipped_models <- sum(
    sapply(transitions_model_results, function(x) {
      !is.null(x$skipped) && x$skipped == TRUE
    }),
    na.rm = TRUE
  )
  processed_models <- total_tasks - skipped_models

  message(sprintf(
    "\nModel processing summary for period %s:",
    periods_to_process
  ))
  message(sprintf(
    "  Total transition-region combinations: %d",
    total_tasks
  ))
  message(sprintf(
    "  Models processed: %d",
    processed_models
  ))
  message(sprintf(
    "  Models skipped (existing): %d",
    skipped_models
  ))
  if (skipped_models > 0) {
    message(sprintf(
      "  To reprocess existing models, set refresh_cache = TRUE"
    ))
  }

  return(transitions_model_results)
}

#' Model a single transition for a given region
#' @param trans_name Name of the transition to model
#' @param refresh_cache Logical, whether to refresh cached datasets and overwrite existing model files
#' @param region Name of the region to model (NULL for national extent)
#' @param use_regions Logical, whether regionalization is used
#' @param ds_transitions Arrow dataset for transition data
#' @param ds_static Arrow dataset for static predictors
#' @param ds_dynamic Arrow dataset for dynamic predictors
#' @param period Character string specifying the time period for modelling
#' @param config A list containing configuration parameters
#' @param model_dir Directory to save models
#' @param eval_dir Directory to save evaluation results
#' @param save_debug Logical, whether to save debug information
#' @param log_file Path to log file for recording messages
#' @param fs_summary Data frame summarizing feature selection results
#' @return A list containing model fitting and evaluation results, or a skipped status if model exists
#' @details
#' This function performs the following steps:
#' 1. Checks if model file already exists and skips if refresh_cache = FALSE
#' 2. Loads transition data for the specified transition and region
#' 3. Retrieves selected predictor names from feature selection summary
#' 4. Loads predictor data for the specified predictors and region
#' 5. Calls `multi_spec_trans_modelling()` to fit and evaluate multiple model specifications
#' 6. Returns the results from the modelling process
#'
#' If a model file already exists for the transition-region combination and refresh_cache = FALSE,
#' the function will skip processing and return a status indicating the model was skipped.
#' @export
model_single_transition <- function(
  trans_name,
  refresh_cache = FALSE,
  region = NULL,
  use_regions = FALSE,
  ds_transitions,
  ds_static,
  ds_dynamic,
  period,
  config,
  model_dir,
  eval_dir,
  save_debug = FALSE,
  log_file = NULL,
  fs_summary,
  model_specs_path = config[["model_specs_path"]]
) {
  log_msg(
    sprintf(
      "modelling transition: %s | Region: %s\n",
      trans_name,
      ifelse(is.null(region), "National extent", region)
    ),
    log_file
  )

  # Construct model file path for existence check
  region_suffix <- ifelse(
    is.null(region),
    "national",
    gsub(" ", "_", tolower(region))
  )
  model_filename <- sprintf("%s_%s.rds", trans_name, region_suffix)
  model_path <- file.path(model_dir, model_filename)

  # Check if model file already exists and skip if refresh_cache = FALSE
  if (!refresh_cache && file.exists(model_path)) {
    log_msg(
      sprintf(
        "  Model file already exists: %s (skipping - set refresh_cache=TRUE to overwrite)",
        model_path
      ),
      log_file
    )

    # Return a simplified result indicating the model was skipped
    return(list(
      transition = trans_name,
      region = ifelse(is.null(region), "National extent", region),
      status = "skipped_existing_model",
      model_path = model_path,
      skipped = TRUE,
      cv_metrics = NULL,
      test_metrics = NULL,
      final_model = list(
        model_path = model_path,
        model_type = "existing",
        status = "skipped"
      )
    ))
  }

  # Resolve region_value if requested
  region_value <- NULL
  if (use_regions) {
    regions <- jsonlite::fromJSON(file.path(
      config[["reg_dir"]],
      "regions.json"
    ))
    region_value <- as.integer(regions$value[match(region, regions$label)])
    if (is.na(region_value) || length(region_value) != 1) {
      stop(sprintf("Invalid region mapping for region '%s'", region))
    }
  }

  # Load transition table (cell_id, response)
  trans_df <- load_transition_data(
    ds = ds_transitions,
    trans_name,
    region_value,
    use_regions,
    log_file = log_file
  )

  # Debug: Count actual transitions in raw data
  raw_transitions_count <- sum(trans_df$response == 1, na.rm = TRUE)
  raw_total_count <- nrow(trans_df)

  log_msg(
    sprintf(
      "  Loaded transition data: %d total rows, %d transitions (1's) in raw data\n",
      raw_total_count,
      raw_transitions_count
    ),
    log_file
  )

  # get predictor names from feature selection summary
  fs_row <- fs_summary %>%
    dplyr::filter(
      transition == trans_name,
      region == ifelse(is.null(region), "National extent", region)
    )

  if (nrow(fs_row) == 0) {
    error_msg <- sprintf(
      "No feature selection results found for %s in region %s",
      trans_name,
      region
    )
    log_msg(error_msg, log_file)
    return(list(
      transition = trans_name,
      region = region,
      status = "error",
      error_message = error_msg,
      cv_metrics = NULL,
      test_metrics = NULL
    ))
  }

  selected_preds_string <- fs_row$selected_predictors[1]

  # Check if feature selection failed (empty or NA selected predictors)
  if (
    is.na(selected_preds_string) ||
      selected_preds_string == "" ||
      nchar(trimws(selected_preds_string)) == 0
  ) {
    error_msg <- sprintf(
      "Feature selection failed for %s-%s: no predictors selected (n_transitions=%d, n_observations=%d)",
      trans_name,
      region,
      fs_row$n_transitions[1],
      fs_row$n_observations[1]
    )
    log_msg(error_msg, log_file)
    return(list(
      transition = trans_name,
      region = region,
      status = "skipped_no_predictors",
      error_message = error_msg,
      cv_metrics = NULL,
      test_metrics = NULL,
      n_transitions = fs_row$n_transitions[1],
      n_observations = fs_row$n_observations[1]
    ))
  }

  pred_names <- selected_preds_string %>%
    # split string to vector
    strsplit(split = ";") %>%
    # remove white space
    lapply(trimws) %>%
    unlist()

  log_msg(
    sprintf(
      "  %d predictors selected by feature selection (n_transitions=%d, n_observations=%d after downsampling)\n",
      length(pred_names),
      fs_row$n_transitions[1],
      fs_row$n_observations[1]
    ),
    log_file
  )

  # Load predictor data
  predictor_data <- load_predictor_data(
    ds_static = ds_static,
    ds_dynamic = ds_dynamic,
    cell_ids = trans_df$cell_id,
    preds = pred_names,
    region_value = region_value,
    scenario = "baseline",
    log_file = log_file
  )

  # drop cell_id column
  predictor_data <- predictor_data %>%
    dplyr::select(-cell_id)

  log_msg("  Loaded predictor data", log_file)

  transition_data <- predictor_data %>%
    dplyr::mutate(response = as.factor(trans_df$response))
  rm(trans_df)
  rm(predictor_data)
  log_msg("  Prepared transition dataset for modelling", log_file)

  # Load model specifications
  model_specs <- yaml::yaml.load_file(model_specs_path)
  log_msg("Loaded model specifications", log_file)

  # call multi_spec_trans_modelling
  results <- multi_spec_trans_modelling(
    transition_data = transition_data,
    model_specs = model_specs,
    log_file = log_file
  )

  # # save the result object as rds
  # saveRDS(
  #   results,
  #   file = "test_results.rds"
  # )

  # Fit best model to full dataset and save (model_path already constructed above)
  best_model_info <- fit_and_save_best_model(
    results = results,
    full_data = transition_data,
    output_path = model_path,
    log_file = log_file,
    max_final_fit_size = model_specs$global$max_final_fit_size
  )

  # Add best model info to results
  results$final_model <- best_model_info
  return(results)
}

#' Fit and evaluate multiple specifications of statical models for a single transition
#'
#' @param transition_data Data frame containing response variable and predictors
#' @param model_specs List containing model specifications loaded from YAML
#' @return List containing results for all models, replicates, and performance metrics
multi_spec_trans_modelling <- function(
  transition_data,
  model_specs,
  log_file = NULL
) {
  # Validate transition data
  log_msg(
    sprintf(
      "  Data shape: %d rows x %d columns",
      nrow(transition_data),
      ncol(transition_data)
    ),
    log_file
  )
  log_msg(
    sprintf(
      "  Response levels: %s",
      paste(levels(transition_data$response), collapse = ", ")
    ),
    log_file
  )
  log_msg(
    sprintf(
      "  Response table: %s",
      paste(
        names(table(transition_data$response)),
        "=",
        table(transition_data$response),
        collapse = ", "
      )
    ),
    log_file
  )

  # Check for any missing values
  na_counts <- sapply(transition_data, function(x) sum(is.na(x)))
  if (any(na_counts > 0)) {
    log_msg(
      sprintf(
        "  Missing values detected in: %s",
        paste(names(na_counts)[na_counts > 0], collapse = ", ")
      ),
      log_file
    )

    # Remove rows with missing values in predictors (keep response column)
    predictor_cols <- names(transition_data)[
      names(transition_data) != "response"
    ]
    rows_before <- nrow(transition_data)
    transition_data <- transition_data[
      complete.cases(transition_data[predictor_cols]),
    ]
    rows_after <- nrow(transition_data)

    log_msg(
      sprintf(
        "  Removed %d rows with missing values (%d -> %d rows, %.1f%% remaining)",
        rows_before - rows_after,
        rows_before,
        rows_after,
        100 * rows_after / rows_before
      ),
      log_file
    )

    # Check if we still have enough data and both classes
    response_table_after <- table(transition_data$response)
    if (nrow(transition_data) < 100) {
      stop(sprintf(
        "Too few observations remaining after removing missing values: %d",
        nrow(transition_data)
      ))
    }
    if (length(response_table_after) < 2) {
      stop("Only one response class remaining after removing missing values")
    }

    log_msg(
      sprintf(
        "  After removing missing values - Response table: %s",
        paste(
          names(response_table_after),
          "=",
          response_table_after,
          collapse = ", "
        )
      ),
      log_file
    )

    # Force garbage collection after missing value removal
    gc(verbose = FALSE)
  } else {
    log_msg("  No missing values detected", log_file)
  }

  # Set global seed if provided
  seed <- if (is.null(model_specs$global$random_seed)) {
    NULL
  } else {
    model_specs$global$random_seed
  }
  if (!is.null(seed)) {
    set.seed(seed)
  }

  # Initialize results storage
  results <- list(
    model_fits = list(), # fitted model objects
    cv_metrics = list(), # cross-validation metrics
    test_metrics = list(), # test set metrics
    best_params = list(), # best hyperparameters
    model_specs = model_specs # store model specifications
  )

  # Loop through replicates
  for (rep in 1:model_specs$global$num_replicates) {
    log_msg(
      sprintf(
        "Processing replicate %d/%d",
        rep,
        model_specs$global$num_replicates
      ),
      log_file
    )

    # Balance data if specified
    if (model_specs$global$balance_adjustment) {
      log_msg("  Balancing data", log_file)
      data_balanced <- balance_data(
        transition_data,
        minority_multiplier = model_specs$global$minority_multiplier,
        minority_proportion = model_specs$global$minority_proportion,
        log_file = log_file
      )
    } else {
      log_msg("  No balancing applied", log_file)
      data_balanced <- transition_data
      if (!is.null(model_specs$global$sample_size)) {
        log_msg(
          sprintf(
            "  Sampling down to %d total sample size",
            model_specs$global$sample_size
          ),
          log_file
        )
        data_balanced <- data_balanced %>%
          dplyr::slice_sample(n = model_specs$global$sample_size)
      }
    }

    # Split data
    log_msg("  Splitting data into training and testing sets", log_file)
    data_split <- rsample::initial_split(
      data_balanced,
      prop = if (is.null(model_specs$global$train_proportion)) {
        0.75
      } else {
        model_specs$global$train_proportion
      },
      strata = response
    )
    train_data <- rsample::training(data_split)
    test_data <- rsample::testing(data_split)
    rm(data_balanced)

    # Create cross-validation folds
    log_msg("  Creating cross-validation folds", log_file)
    cv_folds <- rsample::vfold_cv(
      train_data,
      v = if (is.null(model_specs$global$cv_folds)) {
        5
      } else {
        model_specs$global$cv_folds
      },
      strata = response
    )

    # Process each model type
    for (model_name in names(model_specs$models)) {
      log_msg(sprintf("  Fitting %s model", model_name), log_file)

      model_config <- model_specs$models[[model_name]]

      # Fit model and get results
      model_results <- fit_model_with_tuning(
        train_data = train_data,
        test_data = test_data,
        cv_folds = cv_folds,
        model_name = model_name,
        model_config = model_config,
        metrics_config = model_specs$metrics,
        rep = rep,
        log_file = log_file
      )

      # Store results
      results$model_fits[[paste0(
        model_name,
        "_rep",
        rep
      )]] <- model_results$final_fit
      results$cv_metrics[[paste0(
        model_name,
        "_rep",
        rep
      )]] <- model_results$cv_metrics
      results$test_metrics[[paste0(
        model_name,
        "_rep",
        rep
      )]] <- model_results$test_metrics
      results$best_params[[paste0(
        model_name,
        "_rep",
        rep
      )]] <- model_results$best_params
    }
  }

  # Aggregate results across replicates
  results$aggregated <- aggregate_results(results, model_specs)
  log_msg("Aggregated results across replicates", log_file)

  return(results)
}

#' Balance dataset using downsampling strategy
#'
#' @param data Data frame with response variable
#' @param minority_multiplier Multiplier for minority class size (e.g., 1.5)
#' @param minority_proportion Proportion of minority class in final sample (e.g., 0.4 for 40%)
#' @return Balanced data frame
balance_data <- function(
  data,
  minority_multiplier = NULL,
  minority_proportion = NULL,
  log_file = NULL
) {
  # Count classes
  class_counts <- table(data$response)
  minority_class <- names(which.min(class_counts))
  majority_class <- names(which.max(class_counts))
  minority_size <- min(class_counts)

  # Determine sampling strategy
  if (!is.null(minority_multiplier)) {
    log_msg(
      sprintf(
        "  Balancing using minority multiplier: %.2f",
        minority_multiplier
      ),
      log_file
    )
    # Strategy 1: Downsample majority to minority_multiplier * minority_size
    n_minority <- minority_size
    n_majority <- as.integer(round(minority_size * minority_multiplier))
    total_size <- n_minority + n_majority
  } else if (!is.null(minority_proportion)) {
    log_msg(
      sprintf(
        "  Balancing using minority proportion: %.2f%%",
        100 * minority_proportion
      ),
      log_file
    )
    # Strategy 2: Set minority to specified proportion of total
    # minority_proportion = n_minority / (n_minority + n_majority)
    # If we keep all minority: total_size = minority_size / minority_proportion
    total_size <- ceiling(minority_size / minority_proportion)
    n_minority <- minority_size
    n_majority <- as.integer(total_size - n_minority)
  } else {
    log_msg(" Error: No balancing parameters specified", log_file)
    stop("Must specify either minority_multiplier or minority_proportion")
  }

  log_msg(
    sprintf(
      "  Sampling %d minority class, %d majority class (total: %d, ratio: %.2f%%)",
      as.integer(n_minority),
      as.integer(n_majority),
      as.integer(total_size),
      100 * n_minority / total_size
    ),
    log_file
  )

  # Sample from each class
  balanced_data <- data %>%
    dplyr::group_by(response) %>%
    {
      minority_data <- dplyr::filter(., response == minority_class) %>%
        dplyr::slice_sample(
          n = n_minority,
          replace = n_minority > minority_size
        )

      majority_data <- dplyr::filter(., response == majority_class) %>%
        dplyr::slice_sample(n = n_majority, replace = FALSE)

      dplyr::bind_rows(minority_data, majority_data)
    } %>%
    dplyr::ungroup()

  return(balanced_data)
}

#' Fit model with hyperparameter tuning
#' @param train_data Training data frame
#' @param test_data Testing data frame
#' @param cv_folds Cross-validation folds
#' @param model_name Name of the model type (e.g., "glm", "rf", "xgboost")
#' @param model_config Model configuration list
#' @param metrics_config Metrics configuration list
#' @param rep Replicate number
#' @param log_file Path to log file for recording messages
fit_model_with_tuning <- function(
  train_data,
  test_data,
  cv_folds,
  model_name,
  model_config,
  metrics_config,
  rep,
  log_file = NULL
) {
  # Create recipe
  recipe_obj <- recipes::recipe(response ~ ., data = train_data) %>%
    recipes::step_normalize(recipes::all_numeric_predictors()) %>%
    recipes::step_zv(recipes::all_predictors())

  # Create model specification
  model_spec <- create_model_spec(model_name, model_config, log_file)

  # Create workflow
  wf <- workflows::workflow() %>%
    workflows::add_recipe(recipe_obj) %>%
    workflows::add_model(model_spec)

  # Check if tuning is needed
  tune_params <- tune::extract_parameter_set_dials(model_spec)

  if (nrow(tune_params) > 0) {
    log_msg("  Hyperparameter tuning required", log_file)
    # Tune hyperparameters
    tune_results <- tune_model(
      wf = wf,
      cv_folds = cv_folds,
      model_config = model_config,
      metrics_config = metrics_config,
      train_data = train_data,
      log_file = log_file
    )

    # temporarily save tuning results to disk
    # saveRDS(
    #   tune_results,
    #   file = sprintf("tune_results_%s_rep%d.rds", model_name, rep)
    # )

    # Get available metrics and select optimization metric
    available_metrics <- tune::collect_metrics(tune_results)$.metric %>%
      base::unique()

    # Get optimization metric and ensure it's a character string
    opt_metric <- if (is.null(metrics_config$optimization_metric)) {
      "roc_auc"
    } else {
      metrics_config$optimization_metric
    }
    if (base::is.list(opt_metric)) {
      opt_metric <- base::unlist(opt_metric)[[1]]
    }
    opt_metric <- base::as.character(opt_metric)

    if (!opt_metric %in% available_metrics) {
      if (!is.null(log_file)) {
        log_msg(
          base::sprintf(
            "Warning: optimization metric '%s' not found. Using 'roc_auc'",
            opt_metric
          ),
          log_file
        )
      }
      opt_metric <- "roc_auc"
    }

    all_metrics <- tune::collect_metrics(tune_results)

    # Filter to optimization metric
    metric_results <- all_metrics %>%
      dplyr::filter(.metric == opt_metric) %>%
      dplyr::arrange(dplyr::desc(mean))

    if (base::nrow(metric_results) == 0) {
      base::stop("Optimization metric '", opt_metric, "' not found in results")
    }

    # Extract best parameter values (first row after sorting by mean descending)
    param_cols <- base::names(metric_results)[
      !base::names(metric_results) %in%
        base::c(
          "mean",
          "n",
          "std_err",
          ".metric",
          ".estimator",
          ".config",
          ".iter"
        )
    ]
    best_params <- metric_results[1, param_cols, drop = FALSE]

    if (!is.null(log_file)) {
      log_msg(
        base::sprintf(
          "Best %s: %.4f with params: %s",
          opt_metric,
          metric_results$mean[1],
          base::paste(
            base::names(best_params),
            "=",
            best_params[1, ],
            collapse = ", "
          )
        ),
        log_file
      )
    }

    # Get best parameters
    best_result <- tune::show_best(tune_results, metric = opt_metric, n = 1)

    # Finalize workflow
    final_wf <- tune::finalize_workflow(wf, best_params)
  } else {
    log_msg("  No hyperparameter tuning required", log_file)
    # No tuning needed
    tune_results <- NULL
    best_params <- NULL
    final_wf <- wf
  }

  # Fit final model
  log_msg("  Fitting final model on training data", log_file)
  final_fit <- parsnip::fit(final_wf, data = train_data)

  # Calculate CV metrics
  log_msg("  Calculating cross-validation metrics", log_file)
  if (!is.null(tune_results)) {
    cv_metrics <- tune::collect_metrics(tune_results) %>%
      dplyr::filter(
        if (!is.null(best_params)) {
          # Match best parameters
          Reduce(
            `&`,
            purrr::map2(
              names(best_params),
              best_params,
              ~ get(.x) == .y
            )
          )
        } else {
          TRUE
        }
      )
  } else {
    # Calculate metrics manually
    cv_metrics <- tune::fit_resamples(
      final_wf,
      resamples = cv_folds,
      metrics = create_metric_set(metrics_config)
    ) %>%
      tune::collect_metrics()
  }

  # Evaluate on test set
  log_msg("  Evaluating final model on test data", log_file)
  test_predictions <- stats::predict(final_fit, test_data, type = "prob") %>%
    dplyr::bind_cols(stats::predict(final_fit, test_data)) %>%
    dplyr::bind_cols(test_data %>% dplyr::select(response))
  test_metrics <- calculate_test_metrics(test_predictions, metrics_config)

  log_msg("  Model fitting and evaluation complete", log_file)

  return(list(
    final_fit = final_fit,
    cv_metrics = cv_metrics,
    test_metrics = test_metrics,
    best_params = best_params,
    tune_results = tune_results
  ))
}

#' Create model specification based on model type
create_model_spec <- function(
  model_name,
  model_config,
  log_file = NULL
) {
  params <- model_config$parameters

  # Helper to check if parameter should be tuned (is a vector/list)
  should_tune <- function(param_value) {
    !is.null(param_value) && base::length(param_value) > 1
  }

  # Helper to get single value or tune placeholder
  get_param <- function(param_value, default = NULL) {
    if (should_tune(param_value)) {
      return(tune::tune())
    } else if (!is.null(param_value)) {
      # Force evaluation and return first element
      val <- param_value[[1]]
      return(val)
    } else {
      return(default)
    }
  }

  # Create model spec based on type
  spec <- base::switch(
    model_name,
    "glm" = {
      # Force evaluation of parameters
      penalty_param <- get_param(params$penalty, 0)
      mixture_param <- get_param(params$mixture, 1)

      parsnip::logistic_reg(
        penalty = !!penalty_param,
        mixture = !!mixture_param
      ) %>%
        parsnip::set_engine("glmnet") %>%
        parsnip::set_mode("classification")
    },

    "rf" = {
      # Force evaluation of all parameters
      mtry_param <- get_param(params$mtry, NULL)
      trees_param <- get_param(params$trees, 500)
      min_n_param <- get_param(params$min_n, 5)
      sample_fraction_param <- get_param(params$sample.fraction, 1.0)

      # Create base model spec
      base_spec <- parsnip::rand_forest(
        mtry = !!mtry_param,
        trees = !!trees_param,
        min_n = !!min_n_param
      ) %>%
        parsnip::set_mode("classification")

      # Build engine args list
      engine_args <- base::list(
        importance = if (is.null(params$importance)) {
          "impurity"
        } else {
          params$importance
        },
        replace = if (is.null(params$replace)) TRUE else params$replace
      )

      # Only add sample.fraction if it's being tuned or has a non-default value
      if (
        should_tune(params$sample.fraction) || !is.null(params$sample.fraction)
      ) {
        engine_args$sample.fraction <- sample_fraction_param
      }

      if (!is.null(params$seed)) {
        engine_args$seed <- params$seed
      }

      # Add engine with args
      base_spec <- base::do.call(
        parsnip::set_engine,
        base::c(list(object = base_spec, engine = "ranger"), engine_args)
      )

      base_spec
    },

    "xgboost" = {
      # Force evaluation of parameters
      mtry_param <- get_param(params$mtry, NULL)
      trees_param <- get_param(params$trees, 100)
      min_n_param <- get_param(params$min_n, 5)
      tree_depth_param <- get_param(params$tree_depth, 6)
      learn_rate_param <- get_param(params$learn_rate, 0.3)
      loss_reduction_param <- get_param(params$loss_reduction, NULL)

      parsnip::boost_tree(
        mtry = !!mtry_param,
        trees = !!trees_param,
        min_n = !!min_n_param,
        tree_depth = !!tree_depth_param,
        learn_rate = !!learn_rate_param,
        loss_reduction = !!loss_reduction_param
      ) %>%
        parsnip::set_engine("xgboost") %>%
        parsnip::set_mode("classification")
    },

    base::stop("Unknown model type: ", model_name)
  )

  return(spec)
}

#' Tune model hyperparameters
#' @param wf Workflow object
#' @param cv_folds Cross-validation folds
#' @param model_config Model configuration list
#' @param metrics_config Metrics configuration list
#' @param train_data Training data frame
#' @param log_file Path to log file for recording messages
#' @return Tuning results object
#' @details
#' This function creates a tuning grid based on the model configuration,
#' performs grid search using cross-validation folds, and returns the tuning results.
#' @export
tune_model <- function(
  wf,
  cv_folds,
  model_config,
  metrics_config,
  train_data,
  log_file = NULL
) {
  # Create tuning grid
  param_grid <- create_tuning_grid(wf, model_config, train_data, log_file)

  log_msg(
    base::sprintf(
      "Starting grid search with %d parameter combinations across %d CV folds",
      base::nrow(param_grid),
      base::nrow(cv_folds)
    ),
    log_file
  )

  # Create control object with verbose output
  ctrl <- tune::control_grid(
    save_pred = TRUE,
    verbose = TRUE,
    event_level = "second",
    allow_par = FALSE # Set to TRUE if using nested parallelization
  )

  # Perform grid search
  start_time <- base::Sys.time()

  tune_results <- tune::tune_grid(
    wf,
    resamples = cv_folds,
    grid = param_grid,
    metrics = create_metric_set(metrics_config),
    control = ctrl
  )

  end_time <- base::Sys.time()
  elapsed <- base::difftime(end_time, start_time, units = "mins")

  log_msg(
    base::sprintf(
      "Grid search completed in %.2f minutes",
      base::as.numeric(elapsed)
    ),
    log_file
  )

  # Log best results
  best_result <- tune::show_best(
    tune_results,
    metric = if (is.null(metrics_config$optimization_metric)) {
      "roc_auc"
    } else {
      metrics_config$optimization_metric
    },
    n = 1
  )
  log_msg(
    base::sprintf(
      "Best %s: %.4f",
      if (is.null(metrics_config$optimization_metric)) {
        "roc_auc"
      } else {
        metrics_config$optimization_metric
      },
      best_result$mean[1]
    ),
    log_file
  )

  return(tune_results)
}

#' Create tuning grid from configuration
#' @param wf Workflow object
#' @param model_config Model configuration list
#' @param train_dat Training data frame
#' @param log_file Path to log file for recording messages
#' @return Data frame representing the tuning grid
#' @details
#' This function constructs a tuning grid based on the model configuration.
#' It identifies parameters that require tuning, finalizes any parameters
#' that depend on the training data (e.g., mtry), and generates a grid
#' of all combinations of parameter values.
#' @export
create_tuning_grid <- function(
  wf,
  model_config,
  train_dat,
  log_file = NULL
) {
  params <- model_config$parameters

  # Identify which parameters need tuning (have multiple values)
  tune_params <- base::list()
  for (param_name in base::names(params)) {
    param_value <- params[[param_name]]
    if (!is.null(param_value) && base::length(param_value) > 1) {
      tune_params[[param_name]] <- param_value
    }
  }

  if (base::length(tune_params) == 0) {
    return(NULL)
  }

  # Check if we need to finalize parameters (e.g., mtry)
  param_set <- tune::extract_parameter_set_dials(wf)

  if (base::nrow(param_set) > 0) {
    # Check which parameters need finalization
    needs_finalization <- param_set %>%
      dplyr::filter(base::sapply(object, function(x) dials::has_unknowns(x)))

    if (base::nrow(needs_finalization) > 0) {
      # Finalize parameters using the training data
      # Get number of predictors (excluding response variable)
      n_predictors <- base::ncol(train_dat) - 1

      param_set <- param_set %>%
        dials::finalize(train_dat %>% dplyr::select(-response))

      log_msg(
        base::sprintf(
          "  Finalized %d parameters using training data (%d predictors)",
          base::nrow(needs_finalization),
          n_predictors
        ),
        log_file
      )
    }
  }

  # Create grid from all combinations of parameter values
  grid <- base::expand.grid(tune_params, stringsAsFactors = FALSE)

  # Rename columns to match tidymodels parameter names
  name_map <- base::c(
    "min.node.size" = "min_n",
    "num.trees" = "trees",
    "min_n" = "min_n",
    "tree_depth" = "tree_depth",
    "learn_rate" = "learn_rate",
    "mtry" = "mtry",
    "penalty" = "penalty",
    "mixture" = "mixture",
    "sample.fraction" = "sample.fraction"
  )

  for (old_name in base::names(grid)) {
    if (old_name %in% base::names(name_map)) {
      new_name <- name_map[[old_name]]
      if (new_name != old_name && !new_name %in% base::names(grid)) {
        grid[[new_name]] <- grid[[old_name]]
        grid[[old_name]] <- NULL
      }
    }
  }

  # Filter grid to ensure mtry values are valid (if mtry is being tuned)
  if ("mtry" %in% base::names(grid)) {
    n_predictors <- base::ncol(train_dat) - 1
    grid <- grid %>%
      dplyr::filter(mtry <= n_predictors)

    if (base::nrow(grid) == 0) {
      base::stop(
        "All mtry values exceed number of predictors (",
        n_predictors,
        "). ",
        "Please adjust mtry values in configuration."
      )
    }
  }

  log_msg(
    base::sprintf(
      "  Created tuning grid with %d parameter combinations",
      base::nrow(grid)
    ),
    log_file
  )

  return(grid)
}


#' Create metric set from configuration
#' @param metrics_config Metrics configuration list
#' @return Yardstick metric set function
#' @details
#' This function constructs a yardstick metric set based on the specified metrics
#' in the configuration. If no metrics are specified, a default set is used.
#' @export
create_metric_set <- function(metrics_config) {
  metric_names <- if (is.null(metrics_config$metrics)) {
    base::c("roc_auc", "accuracy", "precision", "recall", "f_meas")
  } else {
    metrics_config$metrics
  }

  # Ensure metric_names is a character vector (YAML sometimes creates lists)
  if (base::is.list(metric_names)) {
    metric_names <- base::unlist(metric_names)
  }

  # Available metric functions
  available_metrics <- base::c(
    "roc_auc",
    "accuracy",
    "precision",
    "recall",
    "f_meas",
    "kap",
    "mcc",
    "specificity",
    "sensitivity"
  )

  # Validate requested metrics
  invalid_metrics <- metric_names[!metric_names %in% available_metrics]
  if (base::length(invalid_metrics) > 0) {
    base::warning(base::sprintf(
      "Unknown metrics: %s. These will be ignored.",
      base::paste(invalid_metrics, collapse = ", ")
    ))
    metric_names <- metric_names[metric_names %in% available_metrics]
  }

  # Build the metric set call manually to avoid issues with pre-configured functions
  if ("roc_auc" %in% metric_names && "accuracy" %in% metric_names) {
    yardstick::metric_set(
      yardstick::roc_auc,
      yardstick::accuracy,
      yardstick::precision,
      yardstick::recall,
      yardstick::f_meas
    )
  } else if ("roc_auc" %in% metric_names) {
    yardstick::metric_set(
      yardstick::roc_auc,
      yardstick::precision,
      yardstick::recall
    )
  } else {
    yardstick::metric_set(
      yardstick::accuracy,
      yardstick::precision,
      yardstick::recall,
      yardstick::f_meas
    )
  }
}

#' Calculate test set metrics
#' @param predictions Data frame with test set predictions
#' @param metrics_config Metrics configuration list
#' @return Data frame with calculated metrics
#' @details
#' This function computes performance metrics on the test set predictions
#' using the specified metrics in the configuration.
#' @export
calculate_test_metrics <- function(
  predictions,
  metrics_config
) {
  metrics <- create_metric_set(metrics_config)

  # Set global option for event_level to handle binary classification properly
  old_event_level <- getOption("yardstick.event_level")
  options(yardstick.event_level = "second")

  result <- tryCatch(
    {
      metrics(
        predictions,
        truth = response,
        estimate = .pred_class,
        .pred_1,
        event_level = "second"
      )
    },
    finally = {
      # Restore original option
      if (is.null(old_event_level)) {
        options(yardstick.event_level = NULL)
      } else {
        options(yardstick.event_level = old_event_level)
      }
    }
  )

  return(result)
}

#' Aggregate results across replicates
#' @param results Results list from multi_spec_trans_modelling
#' @param model_specs Model specifications list
#' @return List containing aggregated CV and test metrics
#' @details
#' This function aggregates cross-validation and test metrics across replicates
#' for each model specification, calculating mean, standard deviation, minimum,
#' and maximum values.
#' @export
aggregate_results <- function(
  results,
  model_specs
) {
  # Aggregate CV metrics
  cv_summary <- purrr::map_dfr(names(results$cv_metrics), function(name) {
    parts <- strsplit(name, "_rep")[[1]]
    model_name <- parts[1]
    rep_num <- as.integer(parts[2])

    results$cv_metrics[[name]] %>%
      dplyr::mutate(model = model_name, replicate = rep_num)
  }) %>%
    dplyr::group_by(model, .metric) %>%
    dplyr::summarise(
      mean = mean(mean, na.rm = TRUE),
      sd = stats::sd(mean, na.rm = TRUE),
      min = min(mean, na.rm = TRUE),
      max = max(mean, na.rm = TRUE),
      .groups = "drop"
    )

  # Aggregate test metrics
  test_summary <- purrr::map_dfr(
    names(results$test_metrics),
    function(name) {
      parts <- strsplit(name, "_rep")[[1]]
      model_name <- parts[1]
      rep_num <- as.integer(parts[2])

      results$test_metrics[[name]] %>%
        dplyr::mutate(model = model_name, replicate = rep_num)
    }
  ) %>%
    dplyr::group_by(model, .metric) %>%
    dplyr::summarise(
      mean = mean(.estimate, na.rm = TRUE),
      sd = stats::sd(.estimate, na.rm = TRUE),
      min = min(.estimate, na.rm = TRUE),
      max = max(.estimate, na.rm = TRUE),
      .groups = "drop"
    )

  return(list(
    cv_summary = cv_summary,
    test_summary = test_summary
  ))
}

# Null-coalescing operator for handling NULL values
`%||%` <- function(lhs, rhs) {
  if (is.null(lhs)) rhs else lhs
}

# Simple logging function for enhanced model saving
log_msg <- function(message, log_file = NULL) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  formatted_msg <- sprintf("%s | %s", timestamp, message)

  if (!is.null(log_file)) {
    cat(formatted_msg, "\n", file = log_file, append = TRUE)
  } else {
    cat(formatted_msg, "\n")
  }
}

#' Enhanced model saving with tidypredict and butcher support
#' This creates ultra-minimal model files by using tidypredict when possible
#' and falls back to butcher for other models. It includes detailed logging for diagnostics.
#' @param final_workflow The final fitted workflow object
#' @param best_model_name The name of the best model type (e.g., "glm", "rf", "xgboost")
#' @param full_data_clean The full cleaned dataset used for training (for diagnostics)
#' @param output_path The file path to save the minimal model object
#' @param log_file Optional path to a log file for recording messages
#' @details
#' This function attempts to create an ultra-minimal model object using tidypredict for supported
#' model types (GLM, RF, XGBoost). If tidypredict is not supported or efficient for the model type,
#' it falls back to using butcher to reduce the model size. Detailed logging is included to provide insights into the process and any issues encountered.
save_minimal_model <- function(
  final_workflow,
  best_model_name,
  full_data_clean,
  output_path,
  log_file = NULL
) {
  # Check if tidypredict can be used
  can_use_tidypredict <- best_model_name %in%
    c("glm", "rf", "xgboost") &&
    requireNamespace("tidypredict", quietly = TRUE)

  # Disable tidypredict for RF models with many observations (it's inefficient)
  if (best_model_name == "rf" && nrow(full_data_clean) > 10000) {
    log_msg(
      sprintf(
        "Disabling tidypredict for RF with %d observations (tidypredict is inefficient for large RF models)",
        nrow(full_data_clean)
      ),
      log_file
    )
    can_use_tidypredict <- FALSE
  }

  log_msg(
    sprintf(
      "Model type: %s, tidypredict supported: %s",
      best_model_name,
      can_use_tidypredict
    ),
    log_file
  )

  if (can_use_tidypredict) {
    log_msg("=== USING TIDYPREDICT FOR ULTRA-MINIMAL STORAGE ===", log_file)

    tryCatch(
      {
        library(tidypredict)

        # Extract recipe and model
        trained_recipe <- workflows::extract_recipe(final_workflow)
        trained_recipe$template <- NULL # Remove training data
        model_fit <- workflows::extract_fit_parsnip(final_workflow)
        predictor_names <- names(full_data_clean)[
          names(full_data_clean) != "response"
        ]

        # Ensure we have proper response levels for all model types
        response_levels <- if ("response" %in% names(full_data_clean)) {
          levels(full_data_clean$response)
        } else {
          # Fallback: get from model fit if available
          model_fit$lvl %||% c("0", "1")
        }

        if (best_model_name == "glm") {
          # For GLM, get coefficients and create SQL/dplyr expression
          pred_expr <- tidypredict::tidypredict_fit(model_fit)
          coefficients <- coef(model_fit$fit)

          minimal_obj <- list(
            model_type = "tidypredict_glm",
            predictor_names = predictor_names,
            prediction_expr = pred_expr,
            coefficients = coefficients,
            recipe = trained_recipe,
            response_levels = response_levels
          )

          log_msg("✓ Created tidypredict GLM (coefficients only)", log_file)
        } else if (best_model_name == "rf") {
          # For Random Forest, create decision tree expressions
          pred_expr <- tidypredict::tidypredict_fit(model_fit)

          minimal_obj <- list(
            model_type = "tidypredict_rf",
            predictor_names = predictor_names,
            prediction_expr = pred_expr,
            recipe = trained_recipe,
            response_levels = response_levels
          )

          log_msg("✓ Created tidypredict RF (decision expressions)", log_file)
        } else if (best_model_name == "xgboost") {
          # For XGBoost, need special handling due to serialization issues
          tryCatch(
            {
              # Enhanced diagnostics for XGBoost
              log_msg("XGBoost tidypredict diagnostics:", log_file)
              log_msg(
                sprintf("  - XGBoost version: %s", packageVersion("xgboost")),
                log_file
              )
              log_msg(
                sprintf(
                  "  - tidypredict version: %s",
                  packageVersion("tidypredict")
                ),
                log_file
              )
              log_msg(
                sprintf(
                  "  - model_fit class: %s",
                  paste(class(model_fit), collapse = ", ")
                ),
                log_file
              )
              log_msg(
                sprintf(
                  "  - model_fit$fit class: %s",
                  paste(class(model_fit$fit), collapse = ", ")
                ),
                log_file
              )
              log_msg(
                sprintf(
                  "  - data rows: %d, cols: %d",
                  nrow(full_data_clean),
                  ncol(full_data_clean)
                ),
                log_file
              )
              log_msg(
                sprintf(
                  "  - predictor_names length: %d",
                  length(predictor_names)
                ),
                log_file
              )
              log_msg(
                sprintf(
                  "  - response_levels: %s",
                  paste(response_levels, collapse = ", ")
                ),
                log_file
              )

              # Try to create tidypredict expression
              log_msg("  - Attempting tidypredict_fit...", log_file)
              pred_expr <- tidypredict::tidypredict_fit(model_fit)

              log_msg(
                sprintf(
                  "  - tidypredict_fit succeeded, expr type: %s",
                  class(pred_expr)
                ),
                log_file
              )
              log_msg(
                sprintf("  - Expression length: %d", length(pred_expr)),
                log_file
              )

              minimal_obj <- list(
                model_type = "tidypredict_xgboost",
                predictor_names = predictor_names,
                prediction_expr = pred_expr,
                recipe = trained_recipe,
                response_levels = response_levels
              )

              log_msg(
                "✓ Created tidypredict XGBoost (gradient boosting expressions)",
                log_file
              )
            },
            error = function(xgb_error) {
              log_msg(
                sprintf("XGBoost tidypredict failed: %s", xgb_error$message),
                log_file
              )
              log_msg("XGBoost error details:", log_file)
              log_msg(
                sprintf("  - Error class: %s", class(xgb_error)),
                log_file
              )
              if (!is.null(xgb_error$call)) {
                log_msg(
                  sprintf("  - Error call: %s", deparse(xgb_error$call)),
                  log_file
                )
              }
              # Force fallback to butcher approach
              stop("XGBoost tidypredict conversion failed")
            }
          )
        }

        # Check size
        tidypredict_size_mb <- as.numeric(object.size(minimal_obj)) / (1024^2)
        log_msg(
          sprintf("Tidypredict object size: %.2f MB", tidypredict_size_mb),
          log_file
        )

        # Save the ultra-minimal object
        saveRDS(minimal_obj, output_path, compress = "xz")
        file_size_mb <- file.info(output_path)$size / (1024^2)

        log_msg(
          sprintf("✓ Saved tidypredict model: %.2f MB file", file_size_mb),
          log_file
        )
        return(minimal_obj)
      },
      error = function(e) {
        log_msg(sprintf("Tidypredict failed: %s", e$message), log_file)
        log_msg("Falling back to butcher approach...", log_file)
      }
    )
  }

  # Fallback: Use butcher + aggressive manual cleanup
  log_msg("=== USING BUTCHER + MANUAL CLEANUP ===", log_file)

  # Apply butcher operations if available
  if (requireNamespace("butcher", quietly = TRUE)) {
    library(butcher)

    butchered_workflow <- final_workflow
    butchered_workflow <- butcher::axe_call(butchered_workflow)
    butchered_workflow <- butcher::axe_fitted(butchered_workflow)
    butchered_workflow <- butcher::axe_data(butchered_workflow)
    butchered_workflow <- butcher::axe_env(butchered_workflow)

    log_msg("✓ Applied butcher operations", log_file)

    # Extract components from butchered workflow
    trained_recipe <- workflows::extract_recipe(butchered_workflow)
    fitted_model <- butchered_workflow$fit$fit
  } else {
    # No butcher available
    trained_recipe <- workflows::extract_recipe(final_workflow)
    fitted_model <- final_workflow$fit$fit
    log_msg("Butcher not available, using manual cleanup only", log_file)
  }

  # Remove training data from recipe
  trained_recipe$template <- NULL

  # Debug: Log the fitted model class to diagnose XGBoost detection
  log_msg(
    sprintf(
      "Fitted model class for cleanup: %s",
      paste(class(fitted_model), collapse = ", ")
    ),
    log_file
  )

  # Check for XGBoost models FIRST (before ranger, as they're more problematic)
  if (
    "_xgb.Booster" %in%
      class(fitted_model) ||
      any(grepl("xgboost|xgb", class(fitted_model), ignore.case = TRUE))
  ) {
    # Special handling for XGBoost models due to serialization issues
    log_msg(
      "XGBoost model detected - applying special serialization handling...",
      log_file
    )

    tryCatch(
      {
        # For XGBoost, we need to serialize the booster object properly
        if (requireNamespace("xgboost", quietly = TRUE)) {
          # Check if this is a parsnip xgboost model
          if ("_xgb.Booster" %in% class(fitted_model)) {
            # Direct xgboost booster object
            xgb_model <- fitted_model$fit
          } else if (
            !is.null(fitted_model$fit) &&
              inherits(fitted_model$fit, "xgb.Booster")
          ) {
            # Parsnip wrapped xgboost
            xgb_model <- fitted_model$fit
          } else {
            stop("Cannot identify XGBoost model structure")
          }

          # Serialize the XGBoost model to raw format (this avoids pointer issues)
          xgb_raw <- xgboost::xgb.save.raw(xgb_model)

          # Replace the fitted model with serialized version + metadata
          fitted_model$fit <- NULL # Remove problematic booster object
          fitted_model$xgb_raw <- xgb_raw # Store serialized model
          fitted_model$xgb_metadata <- list(
            nfeatures = xgb_model$nfeatures %||% ncol(full_data_clean) - 1,
            params = xgb_model$params %||% list()
          )

          log_msg(
            "✓ Serialized XGBoost model to avoid pointer issues",
            log_file
          )
        } else {
          stop("xgboost package not available for serialization")
        }
      },
      error = function(e) {
        log_msg(
          sprintf("XGBoost serialization failed: %s", e$message),
          log_file
        )
        stop("Cannot serialize XGBoost model: ", e$message)
      }
    )
  } else if ("_ranger" %in% class(fitted_model)) {
    log_msg("Aggressive ranger cleanup...", log_file)

    original_size_mb <- as.numeric(object.size(fitted_model)) / (1024^2)

    # Remove the massive predictions matrix (biggest culprit)
    if (!is.null(fitted_model$fit$predictions)) {
      pred_size_mb <- as.numeric(object.size(fitted_model$fit$predictions)) /
        (1024^2)
      log_msg(
        sprintf("  Removing predictions matrix: %.1f MB", pred_size_mb),
        log_file
      )
      fitted_model$fit$predictions <- NULL
    }

    # Remove inbag.counts
    if (!is.null(fitted_model$fit$inbag.counts)) {
      inbag_size_mb <- as.numeric(object.size(fitted_model$fit$inbag.counts)) /
        (1024^2)
      log_msg(
        sprintf("  Removing inbag.counts: %.1f MB", inbag_size_mb),
        log_file
      )
      fitted_model$fit$inbag.counts <- NULL
    }

    # Remove all non-essential components
    essential_ranger <- c(
      "forest",
      "treetype",
      "num.trees",
      "mtry",
      "min.node.size"
    )
    all_components <- names(fitted_model$fit)

    for (comp in setdiff(all_components, essential_ranger)) {
      if (!is.null(fitted_model$fit[[comp]])) {
        comp_size_mb <- as.numeric(object.size(fitted_model$fit[[comp]])) /
          (1024^2)
        if (comp_size_mb > 0.01) {
          log_msg(
            sprintf("  Removing %s: %.2f MB", comp, comp_size_mb),
            log_file
          )
        }
        fitted_model$fit[[comp]] <- NULL
      }
    }

    final_size_mb <- as.numeric(object.size(fitted_model)) / (1024^2)
    log_msg(
      sprintf(
        "  Ranger cleanup: %.1f MB → %.1f MB",
        original_size_mb,
        final_size_mb
      ),
      log_file
    )
  }

  # Create minimal object structure
  minimal_obj <- list(
    model_type = paste0("butchered_", best_model_name),
    recipe = trained_recipe,
    model = fitted_model,
    response_levels = levels(full_data_clean$response),
    predictor_names = names(full_data_clean)[
      names(full_data_clean) != "response"
    ]
  )

  # Force garbage collection
  gc(verbose = FALSE)

  obj_size_mb <- as.numeric(object.size(minimal_obj)) / (1024^2)
  log_msg(sprintf("Final object size: %.2f MB", obj_size_mb), log_file)

  # Save with compression
  saveRDS(minimal_obj, output_path, compress = "xz")
  file_size_mb <- file.info(output_path)$size / (1024^2)

  log_msg(
    sprintf("✓ Saved butchered model: %.2f MB file", file_size_mb),
    log_file
  )

  return(minimal_obj)
}

#' Enhanced prediction function for all model types based on testing insights
#' This function handles predictions for tidypredict models (GLM, RF, XGBoost) and butchered models,
#' applying appropriate transformations to ensure correct probability outputs and class predictions
#' @param model_path Path to the saved minimal model RDS file
#' @param new_data New data frame for prediction (unprocessed)
#' @param type Type of prediction: "prob" for probabilities, "class" for class labels
#' @return Data frame with predictions (.pred_0, .pred_1 for probabilities or .pred_class for class labels)
predict_minimal_model <- function(model_path, new_data, type = "prob") {
  # Load model and validate
  model_obj <- readRDS(model_path)

  # Validate inputs
  if (!type %in% c("prob", "class")) {
    stop("type must be either 'prob' or 'class'")
  }

  # Always apply preprocessing first using saved recipe
  processed_data <- recipes::bake(model_obj$recipe, new_data = new_data)
  n_obs <- nrow(processed_data)

  # Handle tidypredict models
  if (model_obj$model_type == "tidypredict_glm") {
    # GLM: Single linear expression with logistic transformation
    library(tidypredict)

    tryCatch(
      {
        # Evaluate linear predictor using manual evaluation (more reliable)
        linear_pred <- eval(model_obj$prediction_expr, envir = processed_data)

        # Apply logistic transformation to get probabilities
        prob_1 <- 1 / (1 + exp(-linear_pred))
        prob_0 <- 1 - prob_1

        if (type == "prob") {
          return(tibble(.pred_0 = prob_0, .pred_1 = prob_1))
        } else {
          pred_classes <- ifelse(
            prob_1 > 0.5,
            model_obj$response_levels[2],
            model_obj$response_levels[1]
          )
          return(tibble(
            .pred_class = factor(
              pred_classes,
              levels = model_obj$response_levels
            )
          ))
        }
      },
      error = function(e) {
        stop(sprintf("GLM prediction failed: %s", e$message))
      }
    )
  } else if (model_obj$model_type == "tidypredict_rf") {
    # Random Forest: Multiple tree expressions to evaluate and average
    library(tidypredict)

    tryCatch(
      {
        n_trees <- length(model_obj$prediction_expr)
        all_predictions <- numeric(n_obs)

        # Evaluate each tree expression and accumulate predictions
        for (i in seq_len(n_trees)) {
          tree_result <- eval(
            model_obj$prediction_expr[[i]],
            envir = processed_data
          )

          # Convert character predictions to numeric if needed
          if (is.character(tree_result)) {
            tree_result <- as.numeric(
              tree_result == model_obj$response_levels[2]
            )
          }

          all_predictions <- all_predictions + tree_result
        }

        # Average across all trees to get final probabilities
        prob_1 <- all_predictions / n_trees
        prob_0 <- 1 - prob_1

        if (type == "prob") {
          return(tibble(.pred_0 = prob_0, .pred_1 = prob_1))
        } else {
          pred_classes <- ifelse(
            prob_1 > 0.5,
            model_obj$response_levels[2],
            model_obj$response_levels[1]
          )
          return(tibble(
            .pred_class = factor(
              pred_classes,
              levels = model_obj$response_levels
            )
          ))
        }
      },
      error = function(e) {
        stop(sprintf("Random Forest prediction failed: %s", e$message))
      }
    )
  } else if (model_obj$model_type == "tidypredict_xgboost") {
    # XGBoost: Boosted tree expressions with logit transformation
    library(tidypredict)

    tryCatch(
      {
        # XGBoost tidypredict expressions typically return logits
        if (is.list(model_obj$prediction_expr)) {
          # Multiple expressions (boosting rounds)
          n_rounds <- length(model_obj$prediction_expr)
          all_logits <- numeric(n_obs)

          for (i in seq_len(n_rounds)) {
            round_result <- eval(
              model_obj$prediction_expr[[i]],
              envir = processed_data
            )
            all_logits <- all_logits + round_result
          }

          logits <- all_logits
        } else {
          # Single combined expression
          logits <- eval(model_obj$prediction_expr, envir = processed_data)
        }

        # Apply sigmoid transformation to convert logits to probabilities
        prob_1 <- 1 / (1 + exp(-logits))
        prob_0 <- 1 - prob_1

        if (type == "prob") {
          return(tibble(.pred_0 = prob_0, .pred_1 = prob_1))
        } else {
          pred_classes <- ifelse(
            prob_1 > 0.5,
            model_obj$response_levels[2],
            model_obj$response_levels[1]
          )
          return(tibble(
            .pred_class = factor(
              pred_classes,
              levels = model_obj$response_levels
            )
          ))
        }
      },
      error = function(e) {
        stop(sprintf("XGBoost prediction failed: %s", e$message))
      }
    )
  } else if (grepl("^butchered_", model_obj$model_type)) {
    # Butchered models: Use standard tidymodels workflow prediction
    tryCatch(
      {
        # Extract the original model type
        original_model_type <- gsub("^butchered_", "", model_obj$model_type)

        # Special handling for serialized XGBoost models
        if (
          original_model_type == "xgboost" && !is.null(model_obj$model$xgb_raw)
        ) {
          # Deserialize XGBoost model from raw format
          library(xgboost)

          # Load the serialized booster
          xgb_model <- xgb.load.raw(model_obj$model$xgb_raw)

          # Convert processed data to DMatrix format
          feature_matrix <- as.matrix(processed_data)
          dtest <- xgb.DMatrix(data = feature_matrix)

          # Make predictions directly with XGBoost
          raw_predictions <- predict(xgb_model, dtest)

          # Convert to probabilities (XGBoost outputs logits for binary classification)
          prob_1 <- 1 / (1 + exp(-raw_predictions))
          prob_0 <- 1 - prob_1

          if (type == "prob") {
            return(tibble(.pred_0 = prob_0, .pred_1 = prob_1))
          } else {
            pred_classes <- ifelse(
              prob_1 > 0.5,
              model_obj$response_levels[2],
              model_obj$response_levels[1]
            )
            return(tibble(
              .pred_class = factor(
                pred_classes,
                levels = model_obj$response_levels
              )
            ))
          }
        }

        # Standard workflow recreation for non-XGBoost or non-serialized models
        # Create appropriate model specification
        if (original_model_type == "glm") {
          model_spec <- parsnip::logistic_reg() %>%
            parsnip::set_engine("glmnet") %>%
            parsnip::set_mode("classification")
        } else if (original_model_type == "rf") {
          model_spec <- parsnip::rand_forest() %>%
            parsnip::set_engine("ranger") %>%
            parsnip::set_mode("classification")
        } else if (original_model_type == "xgboost") {
          model_spec <- parsnip::boost_tree() %>%
            parsnip::set_engine("xgboost") %>%
            parsnip::set_mode("classification")
        } else {
          stop(sprintf("Unknown butchered model type: %s", original_model_type))
        }

        # Recreate workflow for prediction
        temp_workflow <- workflows::workflow() %>%
          workflows::add_model(model_spec) %>%
          workflows::add_recipe(model_obj$recipe)

        # Manually fit the workflow with the saved model
        temp_workflow$fit <- list(fit = model_obj$model)
        class(temp_workflow) <- c("workflow", class(temp_workflow))

        # Make predictions
        if (type == "prob") {
          return(predict(temp_workflow, processed_data, type = "prob"))
        } else {
          return(predict(temp_workflow, processed_data, type = "class"))
        }
      },
      error = function(e) {
        stop(sprintf("Butchered model prediction failed: %s", e$message))
      }
    )
  } else {
    stop(sprintf("Unknown model type: %s", model_obj$model_type))
  }
}

#' Fit best model to full dataset and save for prediction
#'
#' @param results Results object from multi_spec_trans_modelling
#' @param full_data Complete dataset (response + predictors) to fit final model
#' @param output_path Path to save the fitted model workflow (.rds file)
#' @param log_file Optional path to log file
#' @param max_final_fit_size Maximum number of observations for final fit (default: 50000)
#' @return Path to saved model file
#' @details
#' This function:
#' 1. Identifies the best performing model based on optimization metric
#' 2. Extracts the best hyperparameters for that model
#' 3. Creates a representative sample of full dataset that preserves class imbalance
#' 4. Fits workflow with those parameters on the sampled dataset
#' 5. Saves the fitted workflow as an RDS file for future predictions
fit_and_save_best_model <- function(
  results,
  full_data,
  output_path,
  log_file = NULL,
  max_final_fit_size = 50000
) {
  # Get optimization metric from model specs
  opt_metric <- if (is.null(results$model_specs$metrics$optimization_metric)) {
    "roc_auc"
  } else {
    results$model_specs$metrics$optimization_metric
  }
  if (base::is.list(opt_metric)) {
    opt_metric <- base::unlist(opt_metric)[[1]]
  }
  opt_metric <- base::as.character(opt_metric)

  log_msg(
    base::sprintf("Selecting best model based on %s", opt_metric),
    log_file
  )

  # Find best model from aggregated test results
  best_model_row <- results$aggregated$test_summary %>%
    dplyr::filter(.metric == opt_metric) %>%
    dplyr::arrange(dplyr::desc(mean)) %>%
    dplyr::slice(1)

  best_model_name <- best_model_row$model
  best_metric_value <- best_model_row$mean

  log_msg(
    base::sprintf(
      "Best model: %s with %s = %.4f",
      best_model_name,
      opt_metric,
      best_metric_value
    ),
    log_file
  )

  # Find the best replicate for this model
  replicate_results <- purrr::map_dfr(
    base::names(results$test_metrics),
    function(name) {
      if (base::grepl(base::paste0("^", best_model_name, "_rep"), name)) {
        parts <- base::strsplit(name, "_rep")[[1]]
        rep_num <- base::as.integer(parts[2])

        metric_val <- results$test_metrics[[name]] %>%
          dplyr::filter(.metric == opt_metric) %>%
          dplyr::pull(.estimate)

        base::data.frame(
          replicate_name = name,
          replicate = rep_num,
          metric_value = metric_val
        )
      } else {
        NULL
      }
    }
  )

  best_replicate_name <- replicate_results %>%
    dplyr::arrange(dplyr::desc(metric_value)) %>%
    dplyr::slice(1) %>%
    dplyr::pull(replicate_name)

  log_msg(
    base::sprintf(
      "Using parameters from best replicate: %s",
      best_replicate_name
    ),
    log_file
  )

  # Extract best parameters from that replicate
  best_params <- results$best_params[[best_replicate_name]]

  # Get model configuration
  model_config <- results$model_specs$models[[best_model_name]]

  log_msg("Fitting final model to full dataset", log_file)

  # Remove missing values from full dataset (same as in multi_spec_trans_modelling)
  predictor_cols <- names(full_data)[names(full_data) != "response"]
  full_data_clean <- full_data[complete.cases(full_data[predictor_cols]), ]

  log_msg(
    sprintf(
      "  Removed %d rows with missing values from full dataset (%d -> %d rows)",
      nrow(full_data) - nrow(full_data_clean),
      nrow(full_data),
      nrow(full_data_clean)
    ),
    log_file
  )

  # Check response distribution
  response_table <- table(full_data_clean$response)
  n_minority <- min(response_table)
  n_majority <- max(response_table)
  minority_class <- names(which.min(response_table))
  majority_class <- names(which.max(response_table))
  total_obs <- nrow(full_data_clean)
  true_imbalance_ratio <- n_minority / n_majority

  log_msg(
    sprintf(
      "  Full dataset: %d obs, minority=%d (%s), majority=%d (%s), ratio=%.4f",
      total_obs,
      n_minority,
      minority_class,
      n_majority,
      majority_class,
      true_imbalance_ratio
    ),
    log_file
  )

  # Determine if sampling is needed
  if (total_obs <= max_final_fit_size) {
    log_msg(
      sprintf(
        "  Using full dataset (%d <= %d max)",
        total_obs,
        max_final_fit_size
      ),
      log_file
    )
    final_fit_data <- full_data_clean
  } else {
    # Sample to preserve true imbalance ratio
    # Strategy: Keep ALL minority class, sample majority to meet target size
    target_majority <- min(
      max_final_fit_size - n_minority, # Don't exceed max size
      n_majority # Don't exceed available majority samples
    )

    # Ensure we maintain the true imbalance ratio as closely as possible
    # If we have room, sample majority proportionally
    if (n_minority + target_majority > max_final_fit_size) {
      target_majority <- max_final_fit_size - n_minority
    }

    sampled_imbalance_ratio <- n_minority / target_majority

    log_msg(
      sprintf(
        "  Sampling to %d obs: %d minority (all), %d majority (%.1f%% of available)",
        n_minority + target_majority,
        n_minority,
        target_majority,
        100 * target_majority / n_majority
      ),
      log_file
    )

    log_msg(
      sprintf(
        "  True imbalance ratio: %.4f, Sampled ratio: %.4f (%.1f%% preserved)",
        true_imbalance_ratio,
        sampled_imbalance_ratio,
        100 * sampled_imbalance_ratio / true_imbalance_ratio
      ),
      log_file
    )

    # Perform stratified sampling
    final_fit_data <- full_data_clean %>%
      dplyr::group_by(response) %>%
      {
        minority_data <- dplyr::filter(., response == minority_class)

        majority_data <- dplyr::filter(., response == majority_class) %>%
          dplyr::slice_sample(n = target_majority, replace = FALSE)

        dplyr::bind_rows(minority_data, majority_data)
      } %>%
      dplyr::ungroup()

    # Verify the sampling
    final_response_table <- table(final_fit_data$response)
    log_msg(
      sprintf(
        "  Sampled data: %s",
        paste(
          names(final_response_table),
          "=",
          final_response_table,
          collapse = ", "
        )
      ),
      log_file
    )
  }

  # Check data size
  data_size_mb <- object.size(final_fit_data) / 1024^2
  log_msg(sprintf("  Final fit dataset size: %.1f MB", data_size_mb), log_file)

  # Create recipe (same preprocessing as during tuning)
  log_msg("  Creating recipe for preprocessing", log_file)
  recipe_obj <- recipes::recipe(response ~ ., data = final_fit_data) %>%
    recipes::step_normalize(recipes::all_numeric_predictors()) %>%
    recipes::step_zv(recipes::all_predictors())

  # Create model specification with best parameters
  if (best_model_name == "glm") {
    # Extract parameters safely - handle case where no tuning was performed
    penalty_val <- if (is.null(best_params) || is.null(best_params$penalty)) {
      # Use default from model config if no tuning was performed
      if (!is.null(model_config$parameters$penalty)) {
        as.numeric(model_config$parameters$penalty[[1]])
      } else {
        0.01 # Default fallback
      }
    } else if (is.list(best_params$penalty)) {
      as.numeric(best_params$penalty[[1]])
    } else {
      as.numeric(best_params$penalty)
    }

    mixture_val <- if (is.null(best_params) || is.null(best_params$mixture)) {
      # Use default from model config if no tuning was performed
      if (!is.null(model_config$parameters$mixture)) {
        as.numeric(model_config$parameters$mixture[[1]])
      } else {
        1 # Default fallback
      }
    } else if (is.list(best_params$mixture)) {
      as.numeric(best_params$mixture[[1]])
    } else {
      as.numeric(best_params$mixture)
    }

    model_spec <- parsnip::logistic_reg(
      penalty = penalty_val,
      mixture = mixture_val
    ) %>%
      parsnip::set_engine("glmnet") %>%
      parsnip::set_mode("classification")
  } else if (best_model_name == "rf") {
    # Extract parameters safely - handle NULL best_params
    mtry_val <- if (is.null(best_params) || is.null(best_params$mtry)) {
      if (!is.null(model_config$parameters$mtry)) {
        as.integer(model_config$parameters$mtry)
      } else {
        4 # default value
      }
    } else if (is.list(best_params$mtry)) {
      as.integer(best_params$mtry[[1]])
    } else {
      as.integer(best_params$mtry)
    }

    trees_val <- if (is.null(best_params) || is.null(best_params$trees)) {
      if (!is.null(model_config$parameters$trees)) {
        as.integer(model_config$parameters$trees)
      } else {
        500 # default value
      }
    } else if (is.list(best_params$trees)) {
      as.integer(best_params$trees[[1]])
    } else {
      as.integer(best_params$trees)
    }

    min_n_val <- if (is.null(best_params) || is.null(best_params$min_n)) {
      if (!is.null(model_config$parameters$min_n)) {
        as.integer(model_config$parameters$min_n)
      } else {
        5 # default value
      }
    } else if (is.list(best_params$min_n)) {
      as.integer(best_params$min_n[[1]])
    } else {
      as.integer(best_params$min_n)
    }

    model_spec <- parsnip::rand_forest(
      mtry = mtry_val,
      trees = trees_val,
      min_n = min_n_val
    ) %>%
      parsnip::set_engine(
        "ranger",
        importance = if (is.null(model_config$parameters$importance)) {
          "impurity"
        } else {
          model_config$parameters$importance
        },
        replace = if (is.null(model_config$parameters$replace)) {
          TRUE
        } else {
          model_config$parameters$replace
        },
        sample.fraction = if (
          is.null(model_config$parameters$sample.fraction)
        ) {
          1.0
        } else {
          model_config$parameters$sample.fraction
        },
        seed = model_config$parameters$seed
      ) %>%
      parsnip::set_mode("classification")
  } else if (best_model_name == "xgboost") {
    # Extract parameters safely - handle NULL best_params
    mtry_val <- if (is.null(best_params) || is.null(best_params$mtry)) {
      if (!is.null(model_config$parameters$mtry)) {
        as.integer(model_config$parameters$mtry)
      } else {
        4 # default value
      }
    } else if (is.list(best_params$mtry)) {
      as.integer(best_params$mtry[[1]])
    } else {
      as.integer(best_params$mtry)
    }

    trees_val <- if (is.null(best_params) || is.null(best_params$trees)) {
      if (!is.null(model_config$parameters$trees)) {
        as.integer(model_config$parameters$trees)
      } else {
        100 # default value
      }
    } else if (is.list(best_params$trees)) {
      as.integer(best_params$trees[[1]])
    } else {
      as.integer(best_params$trees)
    }

    min_n_val <- if (is.null(best_params) || is.null(best_params$min_n)) {
      if (!is.null(model_config$parameters$min_n)) {
        as.integer(model_config$parameters$min_n)
      } else {
        5 # default value
      }
    } else if (is.list(best_params$min_n)) {
      as.integer(best_params$min_n[[1]])
    } else {
      as.integer(best_params$min_n)
    }

    tree_depth_val <- if (
      is.null(best_params) || is.null(best_params$tree_depth)
    ) {
      if (!is.null(model_config$parameters$tree_depth)) {
        as.integer(model_config$parameters$tree_depth)
      } else {
        6 # default value
      }
    } else if (is.list(best_params$tree_depth)) {
      as.integer(best_params$tree_depth[[1]])
    } else {
      as.integer(best_params$tree_depth)
    }

    learn_rate_val <- if (
      is.null(best_params) || is.null(best_params$learn_rate)
    ) {
      if (!is.null(model_config$parameters$learn_rate)) {
        as.numeric(model_config$parameters$learn_rate)
      } else {
        0.1 # default value
      }
    } else if (is.list(best_params$learn_rate)) {
      as.numeric(best_params$learn_rate[[1]])
    } else {
      as.numeric(best_params$learn_rate)
    }

    model_spec <- parsnip::boost_tree(
      mtry = mtry_val,
      trees = trees_val,
      min_n = min_n_val,
      tree_depth = tree_depth_val,
      learn_rate = learn_rate_val
    ) %>%
      parsnip::set_engine("xgboost") %>%
      parsnip::set_mode("classification")
  } else {
    base::stop("Unknown model type: ", best_model_name)
  }

  # Create workflow
  log_msg("  Creating workflow with recipe and model specification", log_file)
  workflow_obj <- workflows::workflow() %>%
    workflows::add_recipe(recipe_obj) %>%
    workflows::add_model(model_spec)

  # Fit workflow with error handling
  log_msg(
    sprintf("  Fitting workflow to %d observations...", nrow(full_data_clean)),
    log_file
  )
  start_time <- Sys.time()

  final_workflow <- tryCatch(
    {
      parsnip::fit(workflow_obj, data = final_fit_data)
    },
    error = function(e) {
      log_msg(
        sprintf("  ERROR fitting final workflow: %s", e$message),
        log_file
      )
      stop("Failed to fit final model: ", e$message)
    }
  )

  end_time <- Sys.time()
  elapsed <- difftime(end_time, start_time, units = "secs")
  log_msg(
    sprintf(
      "  Workflow fitted successfully in %.2f seconds",
      as.numeric(elapsed)
    ),
    log_file
  )

  # Create output directory if needed
  output_dir <- base::dirname(output_path)
  if (!base::dir.exists(output_dir)) {
    log_msg(sprintf("  Creating output directory: %s", output_dir), log_file)
    base::dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  }

  # Check if tidypredict can be used for this model type
  can_use_tidypredict <- best_model_name %in% c("glm", "rf", "xgboost") # tidypredict supports these

  log_msg(
    sprintf("  Preparing model for saving to: %s", output_path),
    log_file
  )

  if (can_use_tidypredict) {
    log_msg(
      sprintf(
        "  Using tidypredict for ultra-minimal storage (%s)",
        best_model_name
      ),
      log_file
    )

    # Load tidypredict if available
    if (!requireNamespace("tidypredict", quietly = TRUE)) {
      log_msg(
        "  tidypredict not available, falling back to butcher approach",
        log_file
      )
      can_use_tidypredict <- FALSE
    }
  }

  # Check initial model size
  initial_size_mb <- as.numeric(object.size(final_workflow)) / (1024^2)
  log_msg(
    sprintf("  Initial workflow size: %.1f MB", initial_size_mb),
    log_file
  )

  # Create minimal workflow for prediction by extracting only essential components
  minimal_workflow <- tryCatch(
    {
      # Extract the trained recipe from mold (contains preprocessing parameters)
      # The mold$blueprint$recipe has the actual trained preprocessing steps
      if (!is.null(final_workflow$pre$mold$blueprint$recipe)) {
        trained_recipe <- final_workflow$pre$mold$blueprint$recipe
      } else {
        trained_recipe <- final_workflow$pre$actions$recipe$recipe
      }

      # Remove the large template (training data) from recipe if present
      trained_recipe$template <- NULL

      # Extract the fitted model
      fitted_model <- final_workflow$fit$fit

      # For ranger models, remove the massive predictions matrix and other large components
      if ("_ranger" %in% class(fitted_model)) {
        log_msg(
          "  Trimming ranger model components to reduce size...",
          log_file
        )

        # Remove out-of-bag predictions (this can be 15M+ rows!)
        if (!is.null(fitted_model$fit$predictions)) {
          log_msg("    Removing predictions matrix to save space", log_file)
          fitted_model$fit$predictions <- NULL
        }

        # Remove variable importance if not essential for prediction
        if (!is.null(fitted_model$fit$variable.importance)) {
          log_msg("    Removing variable importance to save space", log_file)
          fitted_model$fit$variable.importance <- NULL
        }

        # Remove call object
        if (!is.null(fitted_model$fit$call)) {
          fitted_model$fit$call <- NULL
        }

        # Remove inbag.counts if present (another potentially large matrix)
        if (!is.null(fitted_model$fit$inbag.counts)) {
          fitted_model$fit$inbag.counts <- NULL
        }
      }

      # For xgboost models, minimal cleanup
      if ("_xgb.Booster" %in% class(fitted_model)) {
        log_msg("  Trimming xgboost model components...", log_file)
        # XGBoost cleanup would go here if needed
      }

      # Create a minimal structure with only what's needed for prediction
      list(
        recipe = trained_recipe,
        model = fitted_model,
        model_type = best_model_name,
        response_levels = levels(full_data_clean$response),
        predictor_names = names(full_data_clean)[
          names(full_data_clean) != "response"
        ],
        # Store essential blueprint info but without the large data
        blueprint = list(
          intercept = final_workflow$pre$mold$blueprint$intercept,
          composition = final_workflow$pre$mold$blueprint$composition,
          ptypes = final_workflow$pre$mold$blueprint$ptypes
        ),
        # Store minimal metadata for reconstruction
        workflow_class = class(final_workflow),
        recipe_class = class(trained_recipe),
        model_class = class(fitted_model)
      )
    },
    error = function(e) {
      log_msg(
        sprintf(
          "  WARNING: Could not create minimal workflow, saving full workflow: %s",
          e$message
        ),
        log_file
      )
      final_workflow
    }
  )

  # Check final model size
  final_size_mb <- as.numeric(object.size(minimal_workflow)) / (1024^2)
  log_msg(sprintf("  Minimal workflow size: %.1f MB", final_size_mb), log_file)
  log_msg(
    sprintf(
      "  Size reduction: %.1f MB (%.1f%%)",
      initial_size_mb - final_size_mb,
      ((initial_size_mb - final_size_mb) / initial_size_mb) * 100
    ),
    log_file
  )
  log_msg("  Created minimal workflow for efficient storage", log_file)

  tryCatch(
    {
      # Use enhanced model saving with tidypredict support
      source(file.path("src", "enhanced_model_saving.r"))

      saved_model <- save_minimal_model(
        final_workflow = final_workflow,
        best_model_name = best_model_name,
        full_data_clean = full_data_clean,
        output_path = output_path,
        log_file = log_file
      )

      log_msg(
        sprintf("✓ Saved enhanced minimal model to: %s", output_path),
        log_file
      )
    },
    error = function(e) {
      log_msg(sprintf("  ERROR saving model: %s", e$message), log_file)
      stop("Failed to save model: ", e$message)
    }
  )

  # Return metadata about saved model
  return(base::list(
    model_path = output_path,
    model_type = best_model_name,
    best_params = best_params,
    test_metric = opt_metric,
    test_value = best_metric_value
  ))
}

#' Load saved model and make predictions
#'
#' @param model_path Path to saved model workflow (.rds file)
#' @param new_data Data frame with predictor variables (must match training data structure)
#' @param type Type of prediction: "class" for class labels, "prob" for probabilities
#' @return Data frame with predictions
#' @details
#' This function loads a saved workflow and generates predictions on new data.
#' The new_data must have the same predictor columns as the training data.
#' @examples
#' # For class predictions
#' predictions <- predict_with_saved_model(
#'   model_path = "models/transition_urban_growth.rds",
#'   new_data = new_predictor_data,
#'   type = "class"
#' )
#'
#' # For probability predictions
#' predictions <- predict_with_saved_model(
#'   model_path = "models/transition_urban_growth.rds",
#'   new_data = new_predictor_data,
#'   type = "prob"
#' )
predict_with_saved_model <- function(model_path, new_data, type = "prob") {
  # Load workflow
  if (!base::file.exists(model_path)) {
    base::stop("Model file not found: ", model_path)
  }

  model_obj <- base::readRDS(model_path)

  # Check if this is a tidypredict model and use enhanced prediction
  if (
    !is.null(model_obj$model_type) &&
      model_obj$model_type %in%
        c("tidypredict_glm", "tidypredict_rf", "tidypredict_xgboost")
  ) {
    # Source enhanced prediction functions
    source(file.path("src", "enhanced_model_saving.r"))
    return(predict_minimal_model(model_path, new_data, type))
  }

  # Check if this is a minimal model or full workflow
  if (
    base::is.list(model_obj) &&
      "recipe" %in% base::names(model_obj) &&
      "model" %in% base::names(model_obj)
  ) {
    # This is a minimal model - reconstruct workflow for prediction

    # Verify predictor columns match
    if (!base::all(model_obj$predictor_names %in% base::names(new_data))) {
      missing_predictors <- model_obj$predictor_names[
        !model_obj$predictor_names %in% base::names(new_data)
      ]
      base::stop(
        "Missing predictor columns in new_data: ",
        base::paste(missing_predictors, collapse = ", ")
      )
    }

    # Select and order predictors to match training
    new_data_processed <- new_data[, model_obj$predictor_names, drop = FALSE]

    # Apply preprocessing using the saved recipe
    # The recipe contains the trained preprocessing steps
    preprocessed_data <- tryCatch(
      {
        # Use bake to apply the trained recipe to new data
        recipes::bake(model_obj$recipe, new_data_processed)
      },
      error = function(e) {
        # If bake fails, try to reconstruct preprocessing manually
        # This handles cases where the recipe structure is different
        if (
          !is.null(model_obj$blueprint) &&
            model_obj$blueprint$composition == "tibble"
        ) {
          # Apply normalization if trained recipe has the parameters
          processed <- new_data_processed
          for (i in seq_along(model_obj$recipe$steps)) {
            step <- model_obj$recipe$steps[[i]]
            if (
              "step_normalize" %in%
                class(step) &&
                step$trained &&
                !is.null(step$means)
            ) {
              # Apply normalization
              for (var in names(step$means)) {
                if (var %in% names(processed)) {
                  processed[[var]] <- (processed[[var]] - step$means[[var]]) /
                    step$sds[[var]]
                }
              }
            }
          }
          processed
        } else {
          base::stop("Error applying preprocessing recipe: ", e$message)
        }
      }
    )

    # Make predictions using the fitted model
    if (type == "prob") {
      predictions <- stats::predict(
        model_obj$model,
        preprocessed_data,
        type = "prob"
      )
      # Ensure column names match expected format
      if (base::ncol(predictions) == 2) {
        base::names(predictions) <- base::paste0(
          ".pred_",
          model_obj$response_levels
        )
      }
    } else if (type == "class") {
      predictions <- stats::predict(
        model_obj$model,
        preprocessed_data,
        type = "class"
      )
      # Convert to factor with correct levels
      predictions <- base::data.frame(
        .pred_class = base::factor(
          predictions[[1]],
          levels = model_obj$response_levels
        )
      )
    } else {
      base::stop("type must be 'prob' or 'class'")
    }
  } else {
    # This is a full workflow (legacy format)
    if (type == "prob") {
      predictions <- stats::predict(model_obj, new_data, type = "prob")
    } else if (type == "class") {
      predictions <- stats::predict(model_obj, new_data, type = "class")
    } else {
      base::stop("type must be 'prob' or 'class'")
    }
  }

  return(predictions)
}

#' Save results to file
save_results <- function(results, output_dir) {
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  # Save aggregated results
  utils::write.csv(
    results$aggregated$cv_summary,
    file.path(output_dir, "cv_metrics_summary.csv"),
    row.names = FALSE
  )

  utils::write.csv(
    results$aggregated$test_summary,
    file.path(output_dir, "test_metrics_summary.csv"),
    row.names = FALSE
  )

  # Save full results as RDS
  saveRDS(results, file.path(output_dir, "full_results.rds"))

  message("Results saved to: ", output_dir)
}
