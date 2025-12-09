#!/usr/bin/env Rscript
# run_ancillary_data_prep.r
# Run ancillary data preparation

# Capture start time
start_time <- Sys.time()

cat("\n========================================\n")
cat("Starting Ancillary Data Preparation\n")
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
  "fs"
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
  "src/ancilliary_data_prep.r"
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

# Run ancillary data preparation
cat("========================================\n")
cat("Running Ancillary Data Preparation\n")
cat("========================================\n")

result <- tryCatch(
  {
    ancillary_data_prep(
      config = config,
      refresh_cache = FALSE,
      terra_temp = Sys.getenv("TERRA_TEMP", unset = tempdir())
    )
    list(status = "success", error = NA)
  },
  error = function(e) {
    cat(sprintf("ERROR in Ancillary Data Preparation: %s\n", e$message))
    list(status = "error", error = e$message)
  }
)

# Report results
end_time <- Sys.time()
total_elapsed <- difftime(end_time, start_time, units = "mins")

cat("\n========================================\n")
cat("Ancillary Data Preparation Summary\n")
cat("========================================\n")
cat(sprintf("Total runtime: %.2f minutes\n", as.numeric(total_elapsed)))
cat(sprintf("Status: %s\n", result$status))

if (result$status == "error" && !is.na(result$error)) {
  cat(sprintf("Error: %s\n", result$error))
}

# Save summary
summary_file <- file.path(
  "logs",
  sprintf(
    "ancillary_data_prep_summary_%s.txt",
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
cat(sprintf("Status: %s\n", result$status))
if (result$status == "error" && !is.na(result$error)) {
  cat(sprintf("Error: %s\n", result$error))
}
sink()

cat(sprintf("\nSummary saved to: %s\n", summary_file))

# Exit with appropriate code
exit_code <- ifelse(result$status == "error", 1, 0)
cat(sprintf(
  "\nAncillary data preparation completed with exit code: %d\n",
  exit_code
))
quit(status = exit_code)
