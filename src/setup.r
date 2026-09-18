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

# ----------------------------------------------------------------------------
# Stage 7 runtime path contract
# ----------------------------------------------------------------------------
# YAML config remains the authoritative path map (D-12, D-13). A small,
# explicit set of environment variables is the only escape hatch for
# machine-specific scratch/temp/Dinamica values:
#
#   TERRA_TEMP            terra::terraOptions(tempdir=...)        D-13
#   HPC_SCRATCH_ROOT      data root on HPC scratch filesystem      D-15
#   HPC_TMP_ROOT          per-job tmp root on HPC                  D-15
#   DINAMICA_EGO_8_HOME   Dinamica install dir or wrapper/SIF      D-09, D-13
#   DINAMICA_BACKEND      "auto" | "local" | "hpc" backend hint    D-09
#
# These names are documented in `.env.template` and consumed by later plans
# (pre-flight gate, exec_dinamica backend selection, smoke test). The
# resolver below is the single contract surface: callers MUST go through
# `get_stage7_runtime_paths()` rather than reading the env vars ad hoc, so
# semantics, defaults, and validation stay in one place (D-12, D-14).
.STAGE7_ENV_KEYS <- c(
  "TERRA_TEMP",
  "HPC_SCRATCH_ROOT",
  "HPC_TMP_ROOT",
  "DINAMICA_EGO_8_HOME",
  "DINAMICA_BACKEND"
)

#' Resolve the Stage 7 runtime path/env contract
#'
#' Returns the consolidated machine-specific runtime paths needed by Stage 7
#' (allocation + prep). YAML config is authoritative for repository-relative
#' paths; this resolver returns only the values that genuinely vary by host
#' and are therefore exposed as environment overrides.
#'
#' @param config Optional config list (from `get_config()`); used only as a
#'   fallback source for `terra_temp` if no env override is set and a
#'   config-driven default exists.
#' @return Named list with elements: `terra_temp`, `hpc_scratch_root`,
#'   `hpc_tmp_root`, `dinamica_ego_8_home`, `dinamica_backend`. Values that
#'   are not set fall back to documented, safe defaults; values that MUST be
#'   set on HPC (e.g., `hpc_scratch_root`) are returned as `""` so callers
#'   can fail-fast in pre-flight rather than silently using a bad default.
#' @export
get_stage7_runtime_paths <- function(config = NULL) {
  getenv <- function(key, default = "") {
    val <- Sys.getenv(key, unset = NA_character_)
    if (is.na(val) || !nzchar(val)) default else val
  }

  terra_temp_default <- if (!is.null(config) && !is.null(config[["terra_temp"]])) {
    config[["terra_temp"]]
  } else {
    tempdir()
  }

  list(
    terra_temp           = getenv("TERRA_TEMP", default = terra_temp_default),
    hpc_scratch_root     = getenv("HPC_SCRATCH_ROOT", default = ""),
    hpc_tmp_root         = getenv("HPC_TMP_ROOT", default = ""),
    dinamica_ego_8_home  = getenv("DINAMICA_EGO_8_HOME", default = ""),
    dinamica_backend     = getenv("DINAMICA_BACKEND", default = "auto")
  )
}

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
#' Signals are checked in order of reliability so this stays in lockstep with
#' the shell layer (`scripts/setup_environments.sh`, `scripts/hpc_common.sh`),
#' which decide "on HPC" the same way:
#'   1. `HPC_SCRATCH_ROOT` set — the authoritative, mount-/host-agnostic path
#'      contract every HPC stage already requires (sourced from `.env`). This is
#'      what the shell uses; keeping R aligned prevents the two layers from
#'      disagreeing on a login node (where the launcher runs the region probe
#'      BEFORE any SLURM job exists).
#'   2. Scheduler job context (`SLURM_JOB_ID` / `SLURM_CLUSTER_NAME` /
#'      `PBS_JOBID`) — set inside a job (SLURM_CLUSTER_NAME is often present in
#'      the login shell too).
#'   3. Scratch-mount presence and hostname pattern — last-resort heuristics for
#'      a bare login shell with none of the above set. Both are configurable so
#'      a new cluster needs no code change: `HPC_MOUNT_HINTS` (colon-separated
#'      dir list; default covers ZALF `/beegfs` plus common roots) and
#'      `HPC_HOSTNAME_PATTERN` (regex; default matches ETH Euler). Note mount
#'      probes are unreliable on login nodes with auto-mounted parallel
#'      filesystems — prefer setting `HPC_SCRATCH_ROOT`.
#'
#' Override entirely with `get_config(force_environment = "hpc" | "local")`.
#'
#' @return character string: "local" or "hpc"
detect_environment <- function() {
  # 1. Authoritative contract variable (matches the shell layer).
  if (nzchar(Sys.getenv("HPC_SCRATCH_ROOT"))) {
    return("hpc")
  }

  # 2. Scheduler job context.
  scheduler_vars <- c("SLURM_JOB_ID", "SLURM_CLUSTER_NAME", "PBS_JOBID")
  if (any(nzchar(Sys.getenv(scheduler_vars)))) {
    return("hpc")
  }

  # 3a. Known scratch mount roots (configurable; default is ZALF /beegfs + the
  # common parallel-filesystem roots).
  mount_hints <- strsplit(
    Sys.getenv("HPC_MOUNT_HINTS", unset = "/beegfs:/cluster:/lustre:/gpfs"),
    ":",
    fixed = TRUE
  )[[1L]]
  mount_hints <- mount_hints[nzchar(mount_hints)]
  if (length(mount_hints) && any(dir.exists(mount_hints))) {
    return("hpc")
  }

  # 3b. Hostname pattern (configurable; default matches ETH Euler login/compute
  # nodes).
  host_pattern <- Sys.getenv("HPC_HOSTNAME_PATTERN", unset = "euler|eu-")
  host_strings <- c(Sys.getenv("HOSTNAME"), Sys.info()[["nodename"]])
  if (nzchar(host_pattern) &&
    any(grepl(host_pattern, host_strings, ignore.case = TRUE))) {
    return("hpc")
  }

  "local"
}

#' Expand `${VAR}` placeholders in a string using environment variables
#'
#' YAML config is authoritative for paths (D-12, D-13), but a small set of
#' machine-specific overrides may appear inline as `${VAR}` placeholders so
#' the same YAML file works for any operator. Unset or empty placeholders
#' raise a clear error so we fail fast at config-load time rather than
#' silently producing a broken path (D-15).
#'
#' @param x A character scalar (typically a path read from YAML).
#' @return The string with `${VAR}` references replaced by `Sys.getenv("VAR")`.
expand_env_placeholders <- function(x) {
  if (!is.character(x) || length(x) != 1L || is.na(x)) {
    return(x)
  }
  pattern <- "\\$\\{([A-Za-z_][A-Za-z0-9_]*)\\}"
  m <- gregexpr(pattern, x, perl = TRUE)
  if (m[[1L]][1L] == -1L) {
    return(x)
  }
  matches <- regmatches(x, m)[[1L]]
  for (mt in matches) {
    var_name <- sub(pattern, "\\1", mt, perl = TRUE)
    val <- Sys.getenv(var_name, unset = NA_character_)
    if (is.na(val) || !nzchar(val)) {
      stop(
        "Config references unset environment variable: ",
        var_name,
        ". Source `.env` or set ",
        var_name,
        " before loading config."
      )
    }
    x <- sub(pattern, val, x, perl = TRUE)
  }
  x
}

#' Build full configuration from YAML structure
#'
#' @param yaml_config parsed YAML configuration
#' @return expanded configuration list
build_full_config <- function(yaml_config) {
  # Extract base path (allow `${VAR}` env-driven values per D-13)
  base_path <- expand_env_placeholders(yaml_config$data_basepath)

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
