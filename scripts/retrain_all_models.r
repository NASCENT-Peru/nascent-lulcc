#!/usr/bin/env Rscript
# retrain_all_models.r
# Re-train all transition models using the new mlr3 pipeline (D-07, D-08).
#
# This script is the operator utility for kicking off mlr3 re-training of all
# transition-region model pairs (~140–160 pairs), replacing the now-obsolete
# tidymodels .rds files with mlr3 .qs files.
#
# transition_modelling() handles internal parallelism via furrr::future_map().
# This script is single-session only — do not run multiple instances.
#
# Usage:
#   Rscript scripts/retrain_all_models.r
#     Re-train all transition models (skip already-trained models)
#
#   Rscript scripts/retrain_all_models.r --force
#     Force re-train ALL models, even if .qs files already exist
#
#   Rscript scripts/retrain_all_models.r --dry-run
#     Print the expected transition-region pairs without training
#
#   Rscript scripts/retrain_all_models.r --region "Andes"
#     Re-train only the Andes region (filters viable_transitions_lists.csv)
#
#   Rscript scripts/retrain_all_models.r --dry-run --region "Andes"
#     Print only the Andes transition-region pairs without training
#
#   Rscript scripts/retrain_all_models.r --force --region "Andes"
#     Force re-train all Andes transitions

# ---------------------------------------------------------------------------
# Capture start time
# ---------------------------------------------------------------------------
start_time <- Sys.time()

cat("\n========================================\n")
cat("Re-training All Transition Models (mlr3)\n")
cat("========================================\n\n")

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
.cli_args     <- commandArgs(trailingOnly = TRUE)
force_retrain <- "--force"   %in% .cli_args
dry_run       <- "--dry-run" %in% .cli_args
region_idx    <- which(.cli_args == "--region")
region_filter <- if (length(region_idx) > 0 && region_idx < length(.cli_args)) {
  .cli_args[[region_idx + 1L]]
} else {
  NULL
}

# ---------------------------------------------------------------------------
# Working directory resolution (run_transition_modelling.r pattern)
# ---------------------------------------------------------------------------
script_path <- commandArgs(trailingOnly = FALSE)
script_path <- script_path[grepl("--file=", script_path)]
if (length(script_path) > 0) {
  script_dir   <- dirname(sub("--file=", "", script_path))
  project_root <- dirname(script_dir)
} else {
  project_root <- getwd()
  if (basename(project_root) == "scripts") project_root <- dirname(project_root)
}
setwd(project_root)
cat(sprintf("Working directory set to: %s\n", getwd()))

# ---------------------------------------------------------------------------
# Load required packages
# Must be loaded before sourcing transition_modelling.r so that %>% and other
# operators are available in the global search path for furrr workers.
# Mirrors the loading block in run_transition_modelling.r.
# ---------------------------------------------------------------------------
required_pkgs <- c(
  "dplyr", "stringr", "future", "furrr", "yaml", "jsonlite",
  "arrow", "tibble", "tidyselect", "tidyr", "purrr",
  "ranger", "glmnet", "xgboost", "qs",
  "mlr3", "mlr3learners", "mlr3tuning", "paradox", "bbotk"
)
for (.pkg in required_pkgs) {
  if (!requireNamespace(.pkg, quietly = TRUE)) {
    cat(sprintf("ERROR: required package '%s' is not installed.\n", .pkg))
    quit(status = 1)
  }
  suppressPackageStartupMessages(library(.pkg, character.only = TRUE))
}
cat("All required packages loaded successfully.\n\n")

# ---------------------------------------------------------------------------
# Source required files
# No runtime package installation — environment provisioning is done externally
# (run_allocation.r Plan 01-03, RESEARCH Pitfall 3).
# ---------------------------------------------------------------------------
src_files <- c("src/setup.r", "src/utils.r", "src/transition_modelling.r")
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

# %||% is defined at file scope in src/transition_modelling.r (line 2404).
# It is available after sourcing above — no local definition needed.

# ---------------------------------------------------------------------------
# Load configuration
# ---------------------------------------------------------------------------
cat("Loading configuration...\n")
config <- tryCatch(
  get_config(),
  error = function(e) {
    cat(sprintf("ERROR getting config: %s\n", e$message))
    quit(status = 1)
  }
)
cat("Configuration loaded successfully.\n")

# Display resolved settings
cat(sprintf("  Model dir:    %s\n", config[["transition_model_dir"]]))
cat(sprintf("  Eval dir:     %s\n", config[["transition_model_eval_dir"]]))
cat(sprintf("  Force retrain: %s\n", force_retrain))
cat(sprintf("  Dry run:       %s\n", dry_run))
cat(sprintf("  Region filter: %s\n", region_filter %||% "all"))
cat("\n")

# ---------------------------------------------------------------------------
# Region filter (D-08): filter viable_transitions_lists to a single region
# by reading the CSV, filtering rows, writing a temp file, and overriding
# config[["viable_transitions_lists"]] before calling transition_modelling().
# ---------------------------------------------------------------------------
if (!is.null(region_filter)) {
  vt_path <- config[["viable_transitions_lists"]]
  if (!file.exists(vt_path)) {
    cat(sprintf("ERROR: viable_transitions_lists.csv not found at %s\n", vt_path))
    quit(status = 1)
  }
  viable_all    <- utils::read.csv(vt_path, stringsAsFactors = FALSE)
  viable_region <- viable_all[viable_all$region_name %in% region_filter, ]
  if (nrow(viable_region) == 0L) {
    cat(sprintf(
      "ERROR: --region '%s' matched 0 rows in viable_transitions_lists.\n",
      region_filter
    ))
    cat(sprintf(
      "Available region_name values: %s\n",
      paste(sort(unique(viable_all$region_name)), collapse = ", ")
    ))
    quit(status = 1)
  }
  temp_viable <- tempfile(fileext = ".csv")
  utils::write.csv(viable_region, temp_viable, row.names = FALSE)
  config[["viable_transitions_lists"]] <- temp_viable
  cat(sprintf(
    "Region filter applied: '%s' — %d of %d viable transitions will be re-trained\n\n",
    region_filter, nrow(viable_region), nrow(viable_all)
  ))
}

# ---------------------------------------------------------------------------
# Dry-run mode: print expected transition-region pairs without training.
# When --region is also provided, the region filter above has already overridden
# config[["viable_transitions_lists"]] to point at the temp filtered CSV,
# so this dry-run reads only the filtered subset.
# ---------------------------------------------------------------------------
if (dry_run) {
  viable_path <- config[["viable_transitions_lists"]]
  if (!file.exists(viable_path)) {
    cat(sprintf("Dry run: viable_transitions_lists.csv not found at %s\n", viable_path))
    cat("Cannot enumerate transition-region pairs without this file.\n")
    quit(status = 0)
  }
  viable <- utils::read.csv(viable_path, stringsAsFactors = FALSE)
  # Filter: non-self transitions, non-zero/non-NA rate, exclude whole_map rows
  viable <- viable[
    !is.na(viable$rate_2018_2022) &
    viable$rate_2018_2022 != 0 &
    viable$region_name != "whole_map" &
    viable$from_lulc != viable$to_lulc,
  ]
  cat(sprintf("[DRY RUN] Would re-train %d transition-region pairs:\n", nrow(viable)))
  for (i in seq_len(nrow(viable))) {
    cat(sprintf("  %s -> %s (%s)\n",
      viable$from_lulc[[i]], viable$to_lulc[[i]], viable$region_name[[i]]))
  }
  cat("\nRe-run without --dry-run to execute training.\n")
  quit(status = 0)
}

# ---------------------------------------------------------------------------
# Main call: transition_modelling()
# Region filtering (if --region was set) is already reflected in
# config[["viable_transitions_lists"]], so transition_modelling() will only
# train the requested region's pairs without further changes.
# ---------------------------------------------------------------------------
cat("Starting mlr3 model re-training...\n")
tryCatch(
  transition_modelling(
    config             = config,
    refresh_cache      = force_retrain,
    use_regions        = NULL,
    model_specs_path   = NULL,
    periods_to_process = NULL
  ),
  error = function(e) {
    cat(sprintf("FATAL ERROR in transition_modelling: %s\n", conditionMessage(e)))
    quit(status = 1)
  }
)

# ---------------------------------------------------------------------------
# Summary file
# ---------------------------------------------------------------------------
end_time <- Sys.time()
elapsed  <- difftime(end_time, start_time, units = "hours")

cat("\n========================================\n")
cat("Re-training Completed Successfully\n")
cat("========================================\n")
cat(sprintf("Total runtime: %.2f hours\n", as.numeric(elapsed)))

summary_file <- file.path(
  config[["transition_model_eval_dir"]],
  sprintf("retrain_summary_%s.txt", Sys.getenv("SLURM_JOB_ID", unset = "local"))
)
if (!dir.exists(dirname(summary_file))) {
  dir.create(dirname(summary_file), recursive = TRUE, showWarnings = FALSE)
}
writeLines(c(
  sprintf("Job ID:  %s", Sys.getenv("SLURM_JOB_ID", unset = "local")),
  sprintf("Start:   %s", format(start_time)),
  sprintf("End:     %s", format(end_time)),
  sprintf("Runtime: %.2f hours", as.numeric(elapsed)),
  sprintf("Force:   %s", force_retrain),
  sprintf("Dry run: %s", dry_run),
  sprintf("Region:  %s", region_filter %||% "all")
), summary_file)

cat(sprintf("\nSummary saved to: %s\n", summary_file))
cat("\nDone!\n")

quit(status = 0)
