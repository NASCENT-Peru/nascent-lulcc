#' LULCC setup: environment-aware configuration
#'
#' Automatically detects environment (local vs HPC) and loads appropriate config
#'
#' @param config_file optional path to specific config file
#' @param force_environment optional environment override ("local" or "hpc")
#'
#' @return list of configuration parameters
#'
#' @export

get_config <- function(config_file = NULL, force_environment = NULL) {
  # Auto-detect environment if not forced
  if (is.null(force_environment)) {
    environment <- detect_environment()
  } else {
    environment <- force_environment
  }

  # Determine config file if not specified
  if (is.null(config_file)) {
    # Find project root directory (look for DESCRIPTION file or git repo)
    project_root <- find_project_root()

    config_file <- switch(
      environment,
      "local" = file.path(project_root, "config", "local_config.yaml"),
      "hpc" = file.path(project_root, "config", "hpc_config.yaml"),
      stop("Unknown environment: ", environment)
    )
  }

  # Check if config file exists
  if (!file.exists(config_file)) {
    stop("Configuration file not found: ", config_file)
  }

  # Load YAML configuration
  if (!requireNamespace("yaml", quietly = TRUE)) {
    stop("yaml package required but not available")
  }

  yaml_config <- yaml::read_yaml(config_file)

  # Build full configuration by expanding paths
  config <- build_full_config(yaml_config)

  message("Loaded configuration for environment: ", environment)
  message("Data base path: ", config$data_basepath)

  return(config)
}

#' Find project root directory
#'
#' @return character path to project root
find_project_root <- function() {
  # Start from current working directory
  current_dir <- getwd()

  # Look for indicators of project root
  root_indicators <- c("DESCRIPTION", ".git", "README.md", "config")

  # Search upward through directory tree
  search_dir <- current_dir
  max_levels <- 10 # Prevent infinite loops
  level <- 0

  while (level < max_levels) {
    # Check for any root indicator
    for (indicator in root_indicators) {
      indicator_path <- file.path(search_dir, indicator)
      if (file.exists(indicator_path) || dir.exists(indicator_path)) {
        return(search_dir)
      }
    }

    # Move up one directory
    parent_dir <- dirname(search_dir)
    if (parent_dir == search_dir) {
      # Reached filesystem root
      break
    }
    search_dir <- parent_dir
    level <- level + 1
  }

  # If not found, use current working directory as fallback
  warning(
    "Could not find project root, using current working directory: ",
    current_dir
  )
  return(current_dir)
}

#' Detect current environment (local vs HPC)
#'
#' @return character string: "local" or "hpc"
detect_environment <- function() {
  # Check for HPC-specific indicators
  hpc_indicators <- c(
    file.exists("/cluster"), # Common HPC mount point
    Sys.getenv("SLURM_JOB_ID") != "", # SLURM job environment
    Sys.getenv("PBS_JOBID") != "", # PBS job environment
    grepl("euler", Sys.getenv("HOSTNAME"), ignore.case = TRUE), # Euler hostname
    grepl("eu-", Sys.info()["nodename"], ignore.case = TRUE) # Euler node pattern
  )

  if (any(hpc_indicators)) {
    return("hpc")
  } else {
    return("local")
  }
}

#' Build full configuration from YAML structure
#'
#' @param yaml_config parsed YAML configuration
#' @return expanded configuration list
build_full_config <- function(yaml_config) {
  # Extract base path
  base_path <- yaml_config$data_basepath

  # Get project root for config files
  project_root <- find_project_root()

  # Helper function to build file paths
  build_path <- function(base, ...) {
    file.path(base, ...)
  }

  # Helper function to build project root paths
  build_project_path <- function(...) {
    file.path(project_root, ...)
  }

  # Build full configuration list (same structure as original setup.r)
  config <- list(
    # Base paths
    data_basepath = base_path,
    historic_lulc_basepath = build_path(
      base_path,
      yaml_config$historic_lulc_basepath
    ),
    environment = yaml_config$environment,

    # Main directories (expand relative paths)
    reg_dir = build_path(base_path, yaml_config$reg_dir),
    predictors_raw_dir = build_path(
      base_path,
      yaml_config$predictors_dir,
      "raw"
    ),
    predictors_intermediate_dir = build_path(
      base_path,
      yaml_config$predictors_dir,
      "intermediate"
    ),
    predictors_prepped_dir = build_path(
      base_path,
      yaml_config$predictors_dir,
      "prepared"
    ),
    outputs_dir = build_path(base_path, yaml_config$outputs_dir),
    tools_dir = build_path(base_path, yaml_config$tools_dir),
    allocation_pars_dir = build_path(
      base_path,
      yaml_config$allocation_pars_dir
    ),

    # Tool file paths
    LULC_aggregation_path = build_path(
      base_path,
      yaml_config$LULC_aggregation_path
    ),
    model_specs_path = build_project_path(yaml_config$model_specs_path), # Project root path
    param_grid_path = build_path(base_path, yaml_config$param_grid_path),
    pred_table_path = build_project_path(yaml_config$pred_table_path), # Project root path
    predict_param_grid_path = build_path(
      base_path,
      yaml_config$predict_param_grid_path
    ),
    predict_model_specs_path = build_path(
      base_path,
      yaml_config$predict_model_specs_path
    ),
    ancillary_data_table = build_project_path(yaml_config$ancillary_data_table), # Project root path
    spat_ints_path = build_path(base_path, yaml_config$spat_ints_path),
    viable_transitions_lists = build_path(
      base_path,
      yaml_config$viable_transitions_lists
    ),
    model_lookup_path = build_path(base_path, yaml_config$model_lookup_path),
    glacial_area_change_xlsx = build_path(
      base_path,
      yaml_config$glacial_area_change_xlsx
    ),
    scenario_area_mods_csv = build_path(
      base_path,
      yaml_config$scenario_area_mods_csv
    ),
    calibration_ctrl_tbl_path = build_path(
      base_path,
      yaml_config$calibration_ctrl_tbl_path
    ),
    ctrl_tbl_path = build_path(base_path, yaml_config$ctrl_tbl_path),

    # Spatial reference
    ref_grid_path = build_path(base_path, yaml_config$ref_grid_path),
    ref_grid_agg_path = build_path(base_path, yaml_config$ref_grid_agg_path),
    ref_grid_target_cellsize = yaml_config$ref_grid_target_cellsize,

    # Derived directory paths
    rasterized_lulc_dir = build_path(
      base_path,
      yaml_config$historic_lulc_basepath,
      "original"
    ),
    aggregated_lulc_dir = build_path(
      base_path,
      yaml_config$historic_lulc_basepath,
      "aggregated"
    ),
    bioreg_zip_local = build_path(base_path, yaml_config$bioreg_zip_local),

    # Predictor subdirectories
    glacial_change_path = build_path(base_path, "glacial_change"),
    ch_geoms_path = build_path(
      base_path,
      yaml_config$predictors_dir,
      "raw",
      "ch_geoms"
    ),
    raw_pop_dir = build_path(
      base_path,
      yaml_config$predictors_dir,
      "raw",
      "socio_economic",
      "population"
    ),
    raw_employment_dir = build_path(
      base_path,
      yaml_config$predictors_dir,
      "raw",
      "socio_economic",
      "employment"
    ),
    preds_tools_dir = build_path(
      base_path,
      yaml_config$predictors_dir,
      "tools"
    ),
    prepped_lyr_path = build_path(
      base_path,
      yaml_config$predictors_dir,
      "prepared",
      "layers"
    ),
    prepped_fte_dir = build_path(
      base_path,
      yaml_config$predictors_dir,
      "prepared",
      "socio_economic",
      "employment"
    ),
    prepped_pred_stacks = build_path(
      base_path,
      yaml_config$predictors_dir,
      "prepared",
      "stacks"
    ),
    pred_db_path = build_path(base_path, yaml_config$pred_db_path),

    # Calibration and simulation directories
    calibration_param_dir = build_path(
      base_path,
      yaml_config$allocation_pars_dir,
      "calibration"
    ),
    simulation_param_dir = build_path(
      base_path,
      yaml_config$allocation_pars_dir,
      "simulation"
    ),

    # temp dir
    temp_dir = build_path(
      base_path,
      yaml_config$temp_dir
    ),

    # Transition directories
    trans_rate_table_dir = build_path(
      base_path,
      "transition_tables",
      "prepared_trans_rates"
    ),
    trans_rates_raw_dir = build_path(
      base_path,
      yaml_config$trans_rates_raw_dir
    ),
    trans_rate_extrapol_dir = build_path(
      base_path,
      "transition_tables",
      "extrapolations"
    ),
    best_trans_area_tables = build_path(
      base_path,
      yaml_config$best_trans_area_tables
    ),
    trans_pre_pred_filter_dir = build_path(
      base_path,
      "transition_datasets",
      "pre_predictor_filtering"
    ),
    trans_post_pred_filter_dir = build_path(
      base_path,
      "transition_datasets",
      "post_predictor_filtering"
    ),

    # Model directories
    feature_selection_dir = build_path(
      base_path,
      yaml_config$outputs_dir,
      "feature_selection"
    ),
    transition_model_dir = build_path(base_path, "transition_models"),
    transition_model_eval_dir = build_path(
      base_path,
      yaml_config$outputs_dir,
      "transition_model_eval"
    ),
    prediction_models_dir = build_path(
      base_path,
      yaml_config$outputs_dir,
      "transition_models",
      "prediction_models"
    ),
    validation_dir = build_path(
      base_path,
      yaml_config$outputs_dir,
      "validation"
    ),
    spat_prob_perturb_path = build_path(base_path, "spat_prob_perturb"),

    # Ancillary spatial data
    ancillary_spatial_dir = build_path(
      base_path,
      yaml_config$ancillary_spatial_dir
    ),

    # Model configuration
    step_length = yaml_config$step_length,
    scenario_names = yaml_config$scenario_names,
    data_periods = yaml_config$data_periods,
    regionalization = yaml_config$regionalization,
    inclusion_threshold = yaml_config$inclusion_threshold,

    # Simulation transition rates configuration
    simulation_trans_rates = yaml_config$simulation_trans_rates,

    # System configuration
    reference_crs = yaml_config$reference_crs,

    # Remote URLs
    bioreg_zip_remote = yaml_config$bioreg_zip_remote
  )

  return(config)
}

#' Create directory structure based on configuration
#'
#' @param config configuration list from get_config()
#' @param create_dirs logical, whether to create directories
create_directory_structure <- function(config, create_dirs = TRUE) {
  if (!create_dirs) {
    return(invisible(NULL))
  }

  # Extract all directory paths from config
  dir_paths <- c(
    config$data_basepath,
    config$historic_lulc_basepath,
    config$reg_dir,
    config$predictors_raw_dir,
    config$predictors_intermediate_dir,
    config$predictors_prepped_dir,
    config$outputs_dir,
    config$tools_dir,
    config$allocation_pars_dir,
    config$rasterized_lulc_dir,
    config$aggregated_lulc_dir,
    config$glacial_change_path,
    config$ch_geoms_path,
    config$raw_pop_dir,
    config$raw_employment_dir,
    config$preds_tools_dir,
    config$prepped_lyr_path,
    config$prepped_fte_dir,
    config$prepped_pred_stacks,
    config$calibration_param_dir,
    config$simulation_param_dir,
    config$trans_rate_table_dir,
    config$trans_rates_raw_dir,
    config$trans_rate_extrapol_dir,
    config$feature_selection_dir,
    config$transition_model_dir,
    config$transition_model_eval_dir,
    config$prediction_models_dir,
    config$validation_dir,
    config$ancillary_spatial_dir,
    dirname(config$spat_prob_perturb_path),
    dirname(config$pred_db_path)
  )

  # Remove duplicates and create directories
  unique_dirs <- unique(dir_paths)

  for (dir_path in unique_dirs) {
    if (!dir.exists(dir_path)) {
      dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
      message("Created directory: ", dir_path)
    }
  }

  invisible(NULL)
}
