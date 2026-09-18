#!/usr/bin/env Rscript
# run_calibrate_allocation_parameters.r
# Run calibrate allocation parameters pipeline. Assumes environment activated and R from that env is used.

# Capture start time
start_time <- Sys.time()

cat("\n========================================\n")
cat("Starting Calibrate Allocation Parameters Pipeline\n")
cat("========================================\n\n")

# Diagnostics: show R used and library paths
cat("R version and path:\n")
print(R.version.string)
cat("R executable:\n")
print(Sys.which("R"))
cat(".libPaths():\n")
print(.libPaths())
cat("\n")

cat("Checking Rcpp installation...\n")

install_if_missing <- function(pkg) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    cat(sprintf("Installing %s...\n", pkg))
    install.packages(pkg, repos = "https://cloud.r-project.org/")
  } else {
    cat(sprintf("%s already installed.\n", pkg))
  }
}

# Install Rcpp first (needed for C++ compilation)
install_if_missing("Rcpp")

# Load required packages (fail fast with informative messages)
required_pkgs <- c(
  "dplyr",
  "stringr",
  "future",
  "furrr",
  "yaml",
  "jsonlite",
  "arrow",
  "tibble",
  "tidyselect",
  "tidyr",
  "purrr",
  "terra",
  "Rcpp"
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
  "src/utils.r"
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

# Compile C++ code before sourcing calibrate_allocation_parameters.r
cat("Compiling C++ functions...\n")
cpp_path <- "src/patch_stats.cpp"
cat(sprintf("Compiling %s...\n", cpp_path))
tryCatch(
  {
    Rcpp::sourceCpp(cpp_path)
    cat("C++ functions compiled and loaded successfully.\n\n")
  },
  error = function(e) {
    cat(sprintf("ERROR compiling %s: %s\n", cpp_path, e$message))
    quit(status = 1)
  }
)

src_files <- c("src/calibrate_allocation_parameters.r")

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
cat(sprintf("  Calibration param dir: %s\n", config[["calibration_param_dir"]]))
cat(sprintf("  Temp dir: %s\n\n", config[["temp_dir"]]))

# Run calibrate allocation parameters pipeline
cat("Starting calibrate allocation parameters...\n\n")
tryCatch(
  {
    calibrate_allocation_parameters(config = config)
  },
  error = function(e) {
    cat(sprintf(
      "\n\nERROR in calibrate allocation parameters: %s\n",
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
cat("Estimation of Allocation Parameters Completed Successfully\n")
cat("========================================\n")
cat(sprintf("Total runtime: %.2f hours\n", as.numeric(elapsed)))

# Save summary statistics
summary_file <- file.path(
  config[["calibration_param_dir"]],
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
  "Allocation parameters saved to: %s\n",
  config[["calibration_param_dir"]]
))
sink()

cat(sprintf("\nSummary saved to: %s\n", summary_file))
cat("\nDone!\n")

quit(status = 0)
