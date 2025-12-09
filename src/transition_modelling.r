# library(tidymodels)
# library(ranger)
# library(glmnet)
# library(xgboost)
# library(purrr)
# library(dplyr)
# library(yardstick)

#' Transition modelling for Land Use Land Cover Change
#' @param config A list containing configuration parameters
#' @param refresh_cache Logical, whether to refresh cached datasets
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

  # subset to period 3 for testing
  periods_to_process <- periods_to_process[3]

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
  dynamic_preds_pq_path <- file.path(
    config[["predictors_prepped_dir"]],
    "parquet_data",
    "dynamic",
    period
  )

  # Verify files exist
  stopifnot(
    file.exists(transitions_pq_path),
    file.exists(static_preds_pq_path),
    file.exists(dynamic_preds_pq_path)
  )

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

  # Determine number of cores from SLURM or fallback
  n_cores <- as.integer(Sys.getenv("SLURM_CPUS_PER_TASK", "4"))
  message(sprintf(
    "Using up to %d parallel workers for transition processing",
    n_cores
  ))

  # Use furrr for parallel map
  future::plan(multicore, workers = n_cores)
  options(future.rng.onMisuse = "ignore")

  # Build vector of row indices to iterate over
  task_ids <- seq_len(nrow(fs_summary))

  message(sprintf(
    "Starting parallel processing of %d transitions...\n",
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
  return(transitions_model_results)
}

#' Model a single transition for a given region
#' @param trans_name Name of the transition to model
#' @param refresh_cache Logical, whether to refresh cached datasets
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
#' @return A list containing model fitting and evaluation results
#' @details
#' This function performs the following steps:
#' 1. Loads transition data for the specified transition and region
#' 2. Retrieves selected predictor names from feature selection summary
#' 3. Loads predictor data for the specified predictors and region
#' 4. Calls `multi_spec_trans_modelling()` to fit and evaluate multiple model specifications
#' 5. Returns the results from the modelling process
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
  log_msg(
    sprintf(
      "  Loaded transition data: %d rows\n",
      nrow(trans_df)
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
      "  %d predictors selected by feature selection (n_transitions=%d, n_observations=%d)\n",
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

  # call multi_spec_trans_modelling
  results <- multi_spec_trans_modelling(
    transition_data = transition_data,
    model_specs_path = model_specs_path,
    log_file = log_file
  )

  # # save the result object as rds
  # saveRDS(
  #   results,
  #   file = "test_results.rds"
  # )

  # Fit best model to full dataset and save
  region_suffix <- ifelse(
    is.null(region),
    "national",
    gsub(" ", "_", tolower(region))
  )
  model_filename <- sprintf("%s_%s.rds", trans_name, region_suffix)
  model_path <- file.path(model_dir, model_filename)

  best_model_info <- fit_and_save_best_model(
    results = results,
    full_data = transition_data,
    output_path = model_path,
    log_file = log_file
  )

  # Add best model info to results
  results$final_model <- best_model_info
  return(results)
}

#' Fit and evaluate multiple specifications of statical models for a single transition
#'
#' @param transition_data Data frame containing response variable and predictors
#' @param model_specs_path Path to JSON configuration file
#' @return List containing results for all models, replicates, and performance metrics
multi_spec_trans_modelling <- function(
  transition_data,
  model_specs_path,
  log_file = NULL
) {
  # Load model specifications
  model_specs <- yaml::yaml.load_file(model_specs_path)
  log_msg("Loaded model specifications", log_file)

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
    total_size <- minority_size * (1 + minority_multiplier)
    n_minority <- minority_size
    n_majority <- minority_size * minority_multiplier
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
    n_majority <- total_size - n_minority
  } else {
    log_msg(" Error: No balancing parameters specified", log_file)
    stop("Must specify either minority_multiplier or minority_proportion")
  }

  log_msg(
    sprintf(
      "  Sampling %d minority class, %d majority class (total: %d, ratio: %.2f%%)",
      n_minority,
      n_majority,
      total_size,
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
  param_grid <- create_tuning_grid(wf, model_config, train_data)

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
      n_predictors <- base::ncol(train_data) - 1

      param_set <- param_set %>%
        dials::finalize(train_data %>% dplyr::select(-response))

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
    n_predictors <- base::ncol(train_data) - 1
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

  metric_functions <- base::list(
    yardstick::roc_auc(event_level = "second"),
    accuracy = yardstick::accuracy,
    precision = yardstick::precision,
    recall = yardstick::recall,
    f_meas = yardstick::f_meas,
    kap = yardstick::kap,
    mcc = yardstick::mcc,
    specificity = yardstick::specificity,
    sensitivity = yardstick::sensitivity
  )

  # Validate that all requested metrics are available
  invalid_metrics <- metric_names[
    !metric_names %in% base::names(metric_functions)
  ]
  if (base::length(invalid_metrics) > 0) {
    base::warning(base::sprintf(
      "Unknown metrics: %s. These will be ignored.",
      base::paste(invalid_metrics, collapse = ", ")
    ))
    metric_names <- metric_names[
      metric_names %in% base::names(metric_functions)
    ]
  }

  selected_metrics <- metric_functions[metric_names]

  base::do.call(yardstick::metric_set, selected_metrics)
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

  metrics(predictions, truth = response, estimate = .pred_class, .pred_1)
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

#' Fit best model to full dataset and save for prediction
#'
#' @param results Results object from multi_spec_trans_modelling
#' @param full_data Complete dataset (response + predictors) to fit final model
#' @param output_path Path to save the fitted model workflow (.rds file)
#' @param log_file Optional path to log file
#' @return Path to saved model file
#' @details
#' This function:
#' 1. Identifies the best performing model based on optimization metric
#' 2. Extracts the best hyperparameters for that model
#' 3. Creates and fits a workflow with those parameters on the full dataset
#' 4. Saves the fitted workflow as an RDS file for future predictions
fit_and_save_best_model <- function(
  results,
  full_data,
  output_path,
  log_file = NULL
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

  # Create recipe (same preprocessing as during tuning)
  recipe_obj <- recipes::recipe(response ~ ., data = full_data) %>%
    recipes::step_normalize(recipes::all_numeric_predictors()) %>%
    recipes::step_zv(recipes::all_predictors())

  # Create model specification with best parameters
  if (best_model_name == "glm") {
    model_spec <- parsnip::logistic_reg(
      penalty = best_params$penalty[[1]],
      mixture = best_params$mixture[[1]]
    ) %>%
      parsnip::set_engine("glmnet") %>%
      parsnip::set_mode("classification")
  } else if (best_model_name == "rf") {
    model_spec <- parsnip::rand_forest(
      mtry = best_params$mtry[[1]],
      trees = best_params$trees[[1]],
      min_n = best_params$min_n[[1]]
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
    model_spec <- parsnip::boost_tree(
      mtry = best_params$mtry[[1]],
      trees = best_params$trees[[1]],
      min_n = best_params$min_n[[1]],
      tree_depth = best_params$tree_depth[[1]],
      learn_rate = best_params$learn_rate[[1]]
    ) %>%
      parsnip::set_engine("xgboost") %>%
      parsnip::set_mode("classification")
  } else {
    base::stop("Unknown model type: ", best_model_name)
  }

  # Create and fit workflow
  final_workflow <- workflows::workflow() %>%
    workflows::add_recipe(recipe_obj) %>%
    workflows::add_model(model_spec) %>%
    parsnip::fit(data = full_data)

  # Create output directory if needed
  output_dir <- base::dirname(output_path)
  if (!base::dir.exists(output_dir)) {
    base::dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  }

  # Save workflow
  base::saveRDS(final_workflow, output_path)
  log_msg(base::sprintf("Saved final model to: %s", output_path), log_file)

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

  workflow_fitted <- base::readRDS(model_path)

  # Make predictions
  if (type == "prob") {
    predictions <- stats::predict(workflow_fitted, new_data, type = "prob")
  } else if (type == "class") {
    predictions <- stats::predict(workflow_fitted, new_data, type = "class")
  } else {
    base::stop("type must be 'prob' or 'class'")
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

# # Instantiate wrapper function over process of modelling prep, fitting,
# # evaluation, saving and completeness checking
# multi_spec_trans_modelling <- function(
#   model_settings = model_list[1],
#   period = periods_to_process[1],
#   config = config,
#   regionalization = config[["regionalization"]],
#   global_settings = global_settings
# ) {
#   ### =========================================================================
#   ### A- Prepare model specifications
#   ### =========================================================================

#   # print model name and parameters
#   sprintf(
#     "Starting modelling for model: %s\n with parameters: \n %s",
#     names(model_settings),
#     paste(
#       names(model_settings$paramaters),
#       unlist(model_settings$parameters),
#       sep = ": ",
#       collapse = "\n"
#     )
#   ) %>%
#     message()

#   # load results of transition feature selection
#   fs_results <- list.files(
#     path = file.path(config[["feature_selection_dir"]]),
#     pattern = ".rds$",
#     full.names = TRUE
#   ) %>%
#     lapply(readRDS) %>%
#     # subset to entries 'transition, selected_predictors
#     lapply(function(x) x[, c("transition", "selected_predictors")]) %>%
#     dplyr::bind_rows()

#   # join to transitions info using Trans_name column
#   transitions_info <- transitions_info %>%
#     dplyr::left_join(
#       fs_results,
#       by = c("Trans_name" = "transition")
#     )

#   # finalise folder paths
#   FS_string <- ifelse(Feature_selection_employed, "filtered", "unfiltered")

#   if (Correct_balance) {
#     model_folder <- file.path(
#       config[["transition_model_dir"]],
#       paste0(toupper(Model_type), "_models"),
#       paste0(model_scale, "_", FS_string)
#     )
#     eval_results_folder <- file.path(
#       config[["transition_model_eval_dir"]],
#       paste0(toupper(Model_type), "_model_evaluation_downsampled"),
#       paste0(model_scale, "_", FS_string)
#     )
#   } else {
#     model_folder <- file.path(
#       config[["transition_model_dir"]],
#       paste0(toupper(Model_type), "_models_non_adjusted"),
#       paste0(model_scale, "_", FS_string)
#     )
#     eval_results_folder <- file.path(
#       config[["transition_model_eval_dir"]],
#       paste0(toupper(Model_type), "_model_evaluation_non_adjusted"),
#       paste0(model_scale, "_", FS_string)
#     )
#   }

#   ### =========================================================================
#   ### B- Performing modelling
#   ### =========================================================================

#   # Now opening loop over datasets
#   Modelling_outputs <- furrr::future_map(
#     .x = Data_paths_for_period,
#     .options = furrr::furrr_options(seed = TRUE, scheduling = FALSE),
#     .f = function(Dataset_path) {
#       message(
#         "Modelling transition: ",
#         stringr::str_remove(basename(Dataset_path), ".rds")
#       )

#       # load dataset
#       Trans_dataset <- readRDS(Dataset_path)
#       Trans_name <- stringr::str_remove(basename(Dataset_path), ".rds")

#       ### =========================================================================
#       ### B.1 - Attach model parameters
#       ### =========================================================================

#       # Attach  a list of model parameters('model_settings')
#       # for each type of model specifcied in the parameter grid
#       Trans_dataset[["model_settings"]] <- lulcc.setparams(
#         transition_result = Trans_dataset$trans_result,
#         covariate_names = names(Trans_dataset$cov_data),
#         model_name = Model_type,
#         # Parameter tuning grid (all possible combinations will be evaluated)
#         param_grid = config[["param_grid_path"]],
#         weights = 1
#       )

#       message("Modelling parameters defined")

#       ### =========================================================================
#       ### B.2- Fit, evaluate and save models
#       ### =========================================================================

#       # Apply function for fitting and evaluating models and saving results
#       # the output returned is a list of errors caught by try()
#       Trans_model_capture <- lulcc.fitevalsave(
#         Transition_dataset = Trans_dataset,
#         trans_name = Trans_name,
#         replicatetype = "splitsample",
#         reps = 5,
#         balance_class = Correct_balance,
#         Downsampling_bounds = list(lower = 0.05, upper = 60),
#         Data_period = Data_period,
#         model_folder = model_folder,
#         eval_results_folder = eval_results_folder,
#         Model_type = Model_type
#       )

#       gc()
#       return(Trans_model_capture)
#     }
#   ) # close loop over trnasition datasets

#   ### =========================================================================
#   ### B.3- Update model specification table to reflect that this specification
#   ### of models is complete
#   ### =========================================================================

#   # check for failures in the Modelling outputs
#   Modelling_check <- unlist(Modelling_outputs)

#   if (all(Modelling_check == "Success")) {
#     # load model spec table and replace the values in the 'completed' column
#     model_spec_table <- readr::read_csv(config[["model_specs_path"]])

#     # find the correct row
#     model_spec_table$modelling_completed[
#       model_spec_table$detail_model_tag == model_specs$detail_model_tag
#     ] <- "Y"

#     readr::write_csv(
#       model_spec_table,
#       file = config[["model_specs_path"]]
#     )

#     message(
#       "Model fitting and evaluation for:",
#       model_specs$detail_model_tag,
#       "completed without errors"
#     )
#   } else {
#     # count number of errors
#     Num_errors <- length(Modelling_check[Modelling_check != "Success"])

#     # print error message
#     warning(
#       Num_errors,
#       " errors occurred in model fitting and evaluation for the model specification: \n",
#       model_specs$detail_model_tag,
#       "\n please consult saved modelling output file: \n",
#       paste0(
#         eval_results_folder,
#         model_specs$detail_model_tag,
#         "_modelling_output_summary.rds"
#       )
#     )

#     if (Num_errors < 10) {
#       message(
#         "As the number of errors was <10 the model spec table has been updated
#            to indicate that this specification has been completed, the likely cause
#            of error is transition dataset with insufficient number of transition
#            instances (1's) or where feature selection has reduced to a single predictor variable"
#       )
#       # load model spec table and replace the values in the 'completed' column
#       model_spec_table <- readr::read_csv(config[["model_specs_path"]])

#       # find the correct row
#       model_spec_table$Modelling_completed[
#         model_spec_table$Detail_model_tag == model_specs$detail_model_tag
#       ] <- "Y"

#       # add a warning
#       model_spec_table$Num_errors <- Num_errors

#       # save
#       readr::write_csv(
#         model_spec_table,
#         file = config[["model_specs_path"]]
#       )
#     }

#     # save modelling outputs for user inspection
#     saveRDS(
#       Modelling_outputs,
#       paste0(
#         eval_results_folder,
#         model_specs$detail_model_tag,
#         "_modelling_output_summary.rds"
#       )
#     )
#   }
# } # close wrapper function
