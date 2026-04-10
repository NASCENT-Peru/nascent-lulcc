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
    data_basepath = base_path,
    environment = yaml_config$environment,
    simulation_trans_rates_params = yaml_config$simulation_trans_rates_params
  )

  # loop over the input_dirs_paths and output_dirs_paths and build paths relative to the base path, unlist and append to config
  input_dirs = unlist(lapply(
    yaml_config$input_dirs,
    function(path) {
      dir_path <- build_path(base_path, path)
      if (!dir.exists(dir_path)) {
        dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
        message("Created directory: ", dir_path)
      }
      return(dir_path)
    }
  ))
  config <- c(config, input_dirs)

  output_dirs = unlist(lapply(
    yaml_config$output_dirs,
    function(path) {
      dir_path <- build_path(base_path, path)
      if (!dir.exists(dir_path)) {
        dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
        message("Created directory: ", dir_path)
      }
      return(dir_path)
    }
  ))
  config <- c(config, output_dirs)

  # loop over the input_output_files_paths and build paths relative to the base path, unlist and append to config
  input_output_files_paths = unlist(lapply(
    yaml_config$input_output_files_paths,
    function(path) {
      file_path <- build_path(base_path, path)
      return(file_path)
    }
  ))
  config <- c(config, input_output_files_paths)

  # loop over the config_files_paths, creating paths relative to the project root
  config_files_paths = lapply(
    c(yaml_config$config_files_paths),
    function(path) {
      build_project_path(path)
    }
  )
  config <- c(config, config_files_paths)

  # add the configuration_settings and the simulation_trans_rates_params as is
  config <- c(config, yaml_config$configuration_settings)
  return(config)
}
