#!/usr/bin/env Rscript
# run_simulation_trans_rates_prep.r
# Run simulation transition rates preparation pipeline. Assumes environment activated and R from that env is used.

# Capture start time
start_time <- Sys.time()

cat("\n========================================\n")
cat("Starting Simulation Transition Rates Preparation Pipeline\n")
cat("========================================\n\n")

# Diagnostics: show R used and library paths
cat("R version and path:\n")
print(R.version.string)
cat("R executable:\n")
print(Sys.which("R"))
cat(".libPaths():\n")
print(.libPaths())
cat("\n")

cat("Checking CVXR installation...\n")

install_if_missing <- function(pkg) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    cat(sprintf("Installing %s...\n", pkg))
    install.packages(pkg, repos = "https://cloud.r-project.org/")
  } else {
    cat(sprintf("%s already installed.\n", pkg))
  }
}

# Install CVXR first (needed for convex optimization)
install_if_missing("CVXR")

# Load required packages (fail fast with informative messages)
required_pkgs <- c(
  "dplyr",
  "stringr",
  "future",
  "future.apply",
  "yaml",
  "jsonlite",
  "arrow",
  "tibble",
  "tidyselect",
  "tidyr",
  "purrr",
  "readxl",
  "readr",
  "ggplot2",
  "scales",
  "CVXR",
  "ggrepel",
  "patchwork"
)

missing_pkgs <- setdiff(required_pkgs, rownames(installed.packages()))
if (length(missing_pkgs) > 0) {
  cat("WARNING: The following packages are missing in this R environment:\n")
  print(missing_pkgs)
  cat("Attempting to install missing packages into R_LIBS_USER...\n")
  repos <- "https://cloud.r-project.org"
  for (p in missing_pkgs) {
    tryCatch(
      {
        install.packages(
          p,
          repos = repos,
          lib = Sys.getenv("R_LIBS_USER", unset = .libPaths()[1])
        )
      },
      error = function(e) {
        cat(sprintf("ERROR installing package %s: %s\n", p, e$message))
      }
    )
  }
}

# Load packages
for (p in required_pkgs) {
  if (!suppressWarnings(requireNamespace(p, quietly = TRUE))) {
    stop(sprintf(
      "Required package '%s' is not available even after attempted install. 
       Please install it in the environment: %s",
      p,
      Sys.getenv("CONDA_PREFIX", unset = "(unknown)")
    ))
  }
  library(p, character.only = TRUE)
}

cat("All required packages loaded successfully.\n\n")

# Set working directory to project root
script_path <- commandArgs(trailingOnly = FALSE)
script_path <- script_path[grepl("--file=", script_path)]
if (length(script_path) > 0) {
  script_dir <- dirname(sub("--file=", "", script_path))
  project_root <- dirname(script_dir)
} else {
  project_root <- getwd()
  if (basename(project_root) == "scripts") {
    project_root <- dirname(project_root)
  }
}
setwd(project_root)
cat(sprintf("Working directory set to: %s\n", getwd()))

# Source all required functions from src/
src_files <- c(
  "src/setup.r",
  "src/utils.r",
  "src/simulation_trans_rates_prep.r"
)

for (src_file in src_files) {
  cat(sprintf("Sourcing %s...\n", src_file))
  tryCatch(
    {
      source(src_file)
      cat(sprintf("%s sourced successfully.\n", src_file))
    },
    error = function(e) {
      cat(sprintf("ERROR sourcing %s: %s\n", src_file, e$message))
      quit(status = 1)
    }
  )
}
cat("\n")

# Get configuration
cat("Loading configuration...\n")
config <- tryCatch(
  {
    get_config()
  },
  error = function(e) {
    cat(sprintf("ERROR getting config: %s\n", e$message))
    quit(status = 1)
  }
)

cat("Configuration loaded successfully.\n")
cat(sprintf(
  "  Regionalization: %s\n",
  ifelse(isTRUE(config[["regionalization"]]), "ENABLED", "DISABLED")
))
cat(sprintf(
  "  Transition rate table dir: %s\n",
  config[["trans_rate_table_dir"]]
))
cat(sprintf(
  "  Scenario names: %s\n\n",
  paste(config[["scenario_names"]], collapse = ", ")
))

# Run simulation transition rates preparation pipeline
cat("Starting simulation transition rates preparation...\n\n")
tryCatch(
  {
    simulation_trans_rates_prep(config = config)
  },
  error = function(e) {
    cat(sprintf(
      "\n\nERROR in simulation transition rates preparation: %s\n",
      e$message
    ))
    cat("Traceback:\n")
    traceback()
    quit(status = 1)
  }
)

# Report results
end_time <- Sys.time()
elapsed <- difftime(end_time, start_time, units = "hours")

cat("\n========================================\n")
cat("Pipeline Completed Successfully\n")
cat("========================================\n")
cat(sprintf("Total runtime: %.2f hours\n", as.numeric(elapsed)))

# Save summary statistics
summary_file <- file.path(
  config[["trans_rate_table_dir"]],
  sprintf("run_summary_%s.txt", Sys.getenv("SLURM_JOB_ID", unset = "local"))
)

# Create directory if ensure_dir function is available, otherwise use dir.create
if (exists("ensure_dir", mode = "function")) {
  ensure_dir(dirname(summary_file))
} else {
  if (!dir.exists(dirname(summary_file))) {
    dir.create(dirname(summary_file), recursive = TRUE, showWarnings = FALSE)
  }
}

sink(summary_file)
cat(sprintf("Job ID: %s\n", Sys.getenv("SLURM_JOB_ID", unset = "local")))
cat(sprintf("Start time: %s\n", start_time))
cat(sprintf("End time: %s\n", end_time))
cat(sprintf("Runtime: %.2f hours\n", as.numeric(elapsed)))
cat(sprintf(
  "Transition rate tables saved to: %s\n",
  config[["trans_rate_table_dir"]]
))
sink()

cat(sprintf("\nSummary saved to: %s\n", summary_file))
cat("\nDone!\n")

quit(status = 0)
