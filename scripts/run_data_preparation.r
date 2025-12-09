#!/usr/bin/env Rscript
# run_data_preparation.r
# Run complete data preparation pipeline for LULCC modelling

# Capture start time
start_time <- Sys.time()

cat("\n========================================\n")
cat("Starting Data Preparation Pipeline\n")
cat("========================================\n\n")

# Diagnostics: show R used and library paths
cat("R version and path:\n")
print(R.version.string)
cat("R executable:\n")
print(Sys.which("R"))
cat(".libPaths():\n")
print(.libPaths())
cat("\n")

# Load required packages (fail fast with informative messages)
required_pkgs <- c(
  "dplyr",
  "stringr",
  "yaml",
  "jsonlite",
  "terra",
  "fs",
  "arrow",
  "tibble"
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
# Get script directory from command line args or current working directory
script_path <- commandArgs(trailingOnly = FALSE)
script_path <- script_path[grepl("--file=", script_path)]
if (length(script_path) > 0) {
  script_dir <- dirname(sub("--file=", "", script_path))
  project_root <- dirname(script_dir)
} else {
  # Fallback: assume we're running from scripts directory
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
  "src/lulc_data_prep.r",
  "src/region_prep.r",
  "src/ancilliary_data_prep.r",
  "src/calibration_predictor_prep.r",
  "src/create_predictor_parquets.r",
  "src/transition_identification.r",
  "src/transition_dataset_prep.r"
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

cat("Configuration loaded successfully.\n\n")

# Run data preparation pipeline steps in sequence
steps <- list(
  list(
    name = "LULC Data Preparation",
    func = function() lulc_data_prep(config = config, refresh_cache = FALSE)
  ),
  list(
    name = "Region Preparation",
    func = function() region_prep(config = config)
  ),
  list(
    name = "Ancillary Data Preparation",
    func = function() {
      ancillary_data_prep(
        config = config,
        refresh_cache = FALSE,
        terra_temp = Sys.getenv("TERRA_TEMP", unset = tempdir())
      )
    }
  ),
  list(
    name = "Calibration Predictor Preparation",
    func = function() {
      calibration_predictor_prep(config = config, refresh_cache = FALSE)
    }
  ),
  list(
    name = "Predictor Parquet Creation",
    func = function() {
      create_predictor_parquets(config = config, refresh_cache = FALSE)
    }
  ),
  list(
    name = "Transition Identification",
    func = function() transition_identification(config = config)
  ),
  list(
    name = "Transition Dataset Preparation",
    func = function() transition_dataset_prep(config = config)
  )
)

# Execute each step
results <- data.frame(
  step = character(0),
  status = character(0),
  error_msg = character(0),
  runtime_mins = numeric(0)
)

for (i in seq_along(steps)) {
  step <- steps[[i]]
  cat(sprintf("\n========================================\n"))
  cat(sprintf("Step %d: %s\n", i, step$name))
  cat(sprintf("========================================\n"))

  step_start <- Sys.time()

  result <- tryCatch(
    {
      step$func()
      list(status = "success", error = NA)
    },
    error = function(e) {
      cat(sprintf("ERROR in %s: %s\n", step$name, e$message))
      list(status = "error", error = e$message)
    }
  )

  step_end <- Sys.time()
  runtime <- as.numeric(difftime(step_end, step_start, units = "mins"))

  results <- rbind(
    results,
    data.frame(
      step = step$name,
      status = result$status,
      error_msg = ifelse(is.na(result$error), "", result$error),
      runtime_mins = runtime
    )
  )

  cat(sprintf("Step completed: %s (%.2f minutes)\n", result$status, runtime))

  if (result$status == "error") {
    cat(sprintf("STOPPING pipeline due to error in: %s\n", step$name))
    break
  }
}

# Report final results
end_time <- Sys.time()
total_elapsed <- difftime(end_time, start_time, units = "mins")

cat("\n========================================\n")
cat("Data Preparation Pipeline Summary\n")
cat("========================================\n")
cat(sprintf("Total runtime: %.2f minutes\n", as.numeric(total_elapsed)))
cat(sprintf("Successful steps: %d\n", sum(results$status == "success")))
cat(sprintf("Failed steps: %d\n", sum(results$status == "error")))

if (nrow(results) > 0) {
  cat("\nStep-by-step results:\n")
  for (i in 1:nrow(results)) {
    status_symbol <- ifelse(results$status[i] == "success", "✓", "✗")
    cat(sprintf(
      "  %s %-35s (%.2f min)\n",
      status_symbol,
      results$step[i],
      results$runtime_mins[i]
    ))
    if (results$status[i] == "error" && results$error_msg[i] != "") {
      cat(sprintf("    Error: %s\n", results$error_msg[i]))
    }
  }
}

# Save summary
summary_file <- file.path(
  "logs",
  sprintf(
    "data_prep_summary_%s.txt",
    Sys.getenv("SLURM_JOB_ID", unset = "local")
  )
)

if (!dir.exists("logs")) {
  dir.create("logs", recursive = TRUE)
}

sink(summary_file)
cat(sprintf("Job ID: %s\n", Sys.getenv("SLURM_JOB_ID", unset = "local")))
cat(sprintf("Start time: %s\n", start_time))
cat(sprintf("End time: %s\n", end_time))
cat(sprintf("Runtime: %.2f minutes\n", as.numeric(total_elapsed)))
cat(sprintf("Successful steps: %d\n", sum(results$status == "success")))
cat(sprintf("Failed steps: %d\n", sum(results$status == "error")))
if (sum(results$status == "error") > 0) {
  cat("\nFailed steps:\n")
  failed_steps <- results[results$status == "error", ]
  for (i in 1:nrow(failed_steps)) {
    cat(sprintf(
      "  - %s: %s\n",
      failed_steps$step[i],
      failed_steps$error_msg[i]
    ))
  }
}
sink()

cat(sprintf("\nSummary saved to: %s\n", summary_file))

# Exit with appropriate code
exit_code <- ifelse(sum(results$status == "error") > 0, 1, 0)
cat(sprintf(
  "\nData preparation pipeline completed with exit code: %d\n",
  exit_code
))
quit(status = exit_code)
