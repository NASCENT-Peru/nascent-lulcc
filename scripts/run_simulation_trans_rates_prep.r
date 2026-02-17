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
  "CVXR"
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
# Try multiple possible paths for setup.r
setup_paths <- c(
  "../src/setup.r", # Expected path when run from scripts/
  "src/setup.r", # If run from project root
  file.path(dirname(dirname(getwd())), "src", "setup.r"), # Alternative
  file.path(Sys.getenv("SLURM_SUBMIT_DIR", "."), "src", "setup.r") # Using SLURM submit dir
)

setup_found <- FALSE
for (setup_path in setup_paths) {
  if (file.exists(setup_path)) {
    cat(sprintf("Sourcing %s...\n", setup_path))
    tryCatch(
      {
        source(setup_path)
        cat("setup.r sourced successfully.\n\n")
        setup_found <- TRUE
        break
      },
      error = function(e) {
        cat(sprintf("ERROR sourcing %s: %s\n", setup_path, e$message))
      }
    )
  }
}

if (!setup_found) {
  cat("ERROR: Could not find or source setup.r in any expected location.\n")
  cat("Working directory:", getwd(), "\n")
  cat("SLURM_SUBMIT_DIR:", Sys.getenv("SLURM_SUBMIT_DIR", "not set"), "\n")
  quit(status = 1)
}

# Source utils.r (optional, may not exist)
utils_paths <- c(
  "../src/utils.r",
  "src/utils.r",
  file.path(dirname(dirname(getwd())), "src", "utils.r"),
  file.path(Sys.getenv("SLURM_SUBMIT_DIR", "."), "src", "utils.r")
)

utils_found <- FALSE
for (utils_path in utils_paths) {
  if (file.exists(utils_path)) {
    cat(sprintf("Sourcing %s...\n", utils_path))
    tryCatch(
      {
        source(utils_path)
        cat("utils.r sourced successfully.\n\n")
        utils_found <- TRUE
        break
      },
      error = function(e) {
        cat(sprintf("WARNING sourcing utils.r: %s\n", e$message))
      }
    )
  }
}

if (!utils_found) {
  cat("utils.r not found in any expected location (skipping)\n\n")
}

# Source simulation transition rates preparation functions
sim_trans_paths <- c(
  "../src/simulation_trans_rates_prep.r",
  "src/simulation_trans_rates_prep.r",
  file.path(
    dirname(dirname(getwd())),
    "src",
    "simulation_trans_rates_prep.r"
  ),
  file.path(
    Sys.getenv("SLURM_SUBMIT_DIR", "."),
    "src",
    "simulation_trans_rates_prep.r"
  )
)

sim_trans_found <- FALSE
for (sim_trans_path in sim_trans_paths) {
  if (file.exists(sim_trans_path)) {
    cat(sprintf("Sourcing %s...\n", sim_trans_path))
    tryCatch(
      {
        source(sim_trans_path)
        cat("simulation_trans_rates_prep.r sourced successfully.\n\n")
        sim_trans_found <- TRUE
        break
      },
      error = function(e) {
        cat(sprintf("ERROR sourcing %s: %s\n", sim_trans_path, e$message))
      }
    )
  }
}

if (!sim_trans_found) {
  cat(
    "ERROR: Could not find or source simulation_trans_rates_prep.r in any expected location.\n"
  )
  cat("Working directory:", getwd(), "\n")
  cat("SLURM_SUBMIT_DIR:", Sys.getenv("SLURM_SUBMIT_DIR", "not set"), "\n")
  quit(status = 1)
}

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
