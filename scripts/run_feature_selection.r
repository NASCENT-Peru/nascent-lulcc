#!/usr/bin/env Rscript
# run_feature_selection.r
# Run feature selection pipeline. Assumes environment activated and R from that env is used.

# Capture start time
start_time <- Sys.time()

cat("\n========================================\n")
cat("Starting Feature Selection Pipeline\n")
cat("========================================\n\n")

# Diagnostics: show R used and library paths
cat("R version and path:\n")
print(R.version.string)
cat("R executable:\n")
print(Sys.which("R"))
cat(".libPaths():\n")
print(.libPaths())
cat("\n")

cat("Checking RRF/GRRF installation...\n")

install_if_missing <- function(pkg) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    cat(sprintf("Installing %s...\n", pkg))
    install.packages(pkg, repos = "https://cloud.r-project.org/")
  } else {
    cat(sprintf("%s already installed.\n", pkg))
  }
}

install_if_missing("RRF") # CRAN

# If RRF is NOT on CRAN on your image, install from GitHub:
if (!require("RRF", quietly = TRUE)) {
  cat("RRF not on CRAN -- installing from GitHub...\n")
  if (!require("devtools", quietly = TRUE)) {
    install.packages("devtools", repos = "https://cloud.r-project.org/")
  }
  devtools::install_github("blackdoor894/RRF")
}

# load required packages (fail fast with informative messages)
required_pkgs <- c(
  "dplyr",
  "stringr",
  "future",
  "RRF",
  "yaml",
  "jsonlite",
  "arrow",
  "tibble",
  "tidyselect",
  "tidyr"
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

# Source setup script
cat("Sourcing setup.r...\n")
tryCatch(
  {
    source("setup.r")
    cat("setup.r sourced successfully.\n\n")
  },
  error = function(e) {
    cat(sprintf("ERROR sourcing setup.r: %s\n", e$message))
    quit(status = 1)
  }
)

# Source utils.r
cat("Sourcing utils.r...\n")
tryCatch(
  {
    source("utils.r")
    cat("utils.r sourced successfully.\n\n")
  },
  error = function(e) {
    cat(sprintf("ERROR sourcing utils.r: %s\n", e$message))
    quit(status = 1)
  }
)

# Source feature selection functions
cat("Sourcing transition_feature_selection.r...\n")
tryCatch(
  {
    source("transition_feature_selection.r")
    cat("transition_feature_selection.r sourced successfully.\n\n")
  },
  error = function(e) {
    cat(sprintf(
      "ERROR sourcing transition_feature_selection.r: %s\n",
      e$message
    ))
    quit(status = 1)
  }
)

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
  "  Feature selection dir: %s\n\n",
  config[["feature_selection_dir"]]
))

# Run feature selection pipeline
cat("Starting feature selection...\n\n")
results <- tryCatch(
  {
    transition_feature_selection(
      config = config,
      use_regions = isTRUE(config[["regionalization"]]),
      refresh_cache = FALSE,
      save_debug = TRUE,
      fs_dir = file.path(config[["feature_selection_dir"]]),
      do_collinearity = TRUE,
      do_grrf = TRUE
    )
  },
  error = function(e) {
    cat(sprintf("\n\nERROR in feature selection: %s\n", e$message))
    cat("Traceback:\n")
    traceback()
    quit(status = 1)
  }
)

# Report results
end_time <- Sys.time()
elapsed <- difftime(end_time, start_time, units = "mins")

cat("\n========================================\n")
cat("Pipeline Completed Successfully\n")
cat("========================================\n")
cat(sprintf("Total runtime: %.2f minutes\n", as.numeric(elapsed)))
cat(sprintf("Results: %d rows\n", nrow(results)))
cat(sprintf("Successful: %d\n", sum(results$status == "success")))
cat(sprintf("Failed: %d\n", sum(results$status != "success")))

# Save summary statistics
summary_file <- file.path(
  config[["feature_selection_dir"]],
  sprintf("run_summary_%s.txt", Sys.getenv("SLURM_JOB_ID", unset = "local"))
)

sink(summary_file)
cat(sprintf("Job ID: %s\n", Sys.getenv("SLURM_JOB_ID", unset = "local")))
cat(sprintf("Start time: %s\n", start_time))
cat(sprintf("End time: %s\n", end_time))
cat(sprintf("Runtime: %.2f minutes\n", as.numeric(elapsed)))
cat(sprintf("Total transitions: %d\n", nrow(results)))
cat(sprintf("Successful: %d\n", sum(results$status == "success")))
cat(sprintf("Failed: %d\n", sum(results$status != "success")))
cat("\nStatus breakdown:\n")
print(table(results$status))
sink()

cat(sprintf("\nSummary saved to: %s\n", summary_file))
cat("\nDone!\n")

quit(status = 0)
