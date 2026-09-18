#!/usr/bin/env Rscript
#' Cross-Stage Transition Pipeline Audit
#'
#' Usage:
#'   Rscript scripts/audit_transition_pipeline.r \
#'     --region costa_peruana \
#'     --scenario BAU \
#'     --period 2018_2022
#'
#' Reads output artifacts from pipeline stages 1-4 and reports transition set
#' consistency. Exits 0 if all stages agree; exits 1 if any set difference exists.
#'
#' Pipeline stages:
#'   Stage 1: viable_transitions_lists.csv (transition_identification output)
#'   Stage 2: transition_feature_selection_summary_{period}.rds
#'   Stage 3: transition_modelling_reconciliation_{period}.rds
#'   Stage 4: {scenario}-{region}-trans_rates-{year}.csv (per-timestep rate CSV)
#'
#' Set difference comparisons emitted (all keyed by id_trans):
#'   Stage 1 vs Stage 2 (transition name -> id_trans lookup via Stage 1)
#'   Stage 1 vs Stage 3 (direct id_trans)
#'   Stage 1 vs Stage 4 (direct id_trans)
#'
#' Note: Stage 2 has no id_trans column directly; the script joins back to
#' Stage 1 by the "from_lulc-to_lulc" transition name. This means Stage 2 set
#' difference reporting will only count transitions that also exist in Stage 1.

# ============================================================
# 1. Parse CLI arguments
# ============================================================
args <- commandArgs(trailingOnly = TRUE)

parse_arg <- function(args, flag) {
  idx <- which(args == flag)
  if (length(idx) == 0 || idx >= length(args)) return(NULL)
  args[[idx + 1]]
}

print_usage <- function() {
  cat("Usage: Rscript scripts/audit_transition_pipeline.r --region <r> --scenario <s> --period <p>\n")
  cat("Example: Rscript scripts/audit_transition_pipeline.r --region costa_peruana --scenario BAU --period 2018_2022\n")
}

if ("--help" %in% args || "-h" %in% args) {
  print_usage()
  quit(status = 0)
}

region_arg   <- parse_arg(args, "--region")
scenario_arg <- parse_arg(args, "--scenario")
period_arg   <- parse_arg(args, "--period")

if (is.null(region_arg) || is.null(scenario_arg) || is.null(period_arg)) {
  print_usage()
  quit(status = 1)
}

cat(sprintf("\n=== Transition Pipeline Audit ===\n"))
cat(sprintf("Region:   %s\n", region_arg))
cat(sprintf("Scenario: %s\n", scenario_arg))
cat(sprintf("Period:   %s\n\n", period_arg))

# ============================================================
# 2. Load config
# ============================================================
# Source setup to get get_config(); adjust path if setup.r is elsewhere.
if (file.exists("src/setup.r")) {
  source("src/setup.r")
} else {
  stop("Cannot find src/setup.r. Run this script from the project root.")
}
config <- get_config()

# ============================================================
# 3. Stage 1: viable_transitions_lists.csv
# ============================================================
vt_path <- config[["viable_transitions_lists"]]
if (!file.exists(vt_path)) stop(sprintf("Stage 1 artifact not found: %s", vt_path))

vt <- read.csv(vt_path, stringsAsFactors = FALSE, check.names = FALSE)
rate_col <- paste0("rate_", period_arg)
if (!rate_col %in% names(vt)) {
  stop(sprintf("Column '%s' not found in viable_transitions_lists.csv", rate_col))
}

vt_viable <- vt[
  vt$region_name == "whole_map" &
    vt$from_lulc != vt$to_lulc &
    !is.na(vt[[rate_col]]),
]
stage1_ids <- sort(unique(vt_viable$id_trans[!is.na(vt_viable$id_trans)]))
# Also keep transition names for Stage 2 join
vt_viable$transition <- paste(vt_viable$from_lulc, vt_viable$to_lulc, sep = "-")

# ============================================================
# 4. Stage 2: feature_selection_summary_{period}.rds (status == "success")
# ============================================================
# Config key is `feature_selection_dir` (confirmed against src/setup.r and
# config/local_config.yaml line 33; the plan's `transition_feature_selection_dir`
# is not defined — the actual key is shorter).
fs_dir <- config[["feature_selection_dir"]]
fs_path <- file.path(fs_dir, sprintf("transition_feature_selection_summary_%s.rds", period_arg))
if (!file.exists(fs_path)) {
  cat(sprintf("WARNING: Stage 2 artifact not found: %s\n", fs_path))
  cat("  Run transition_feature_selection() first to produce this artifact.\n")
  stage2_n   <- NA_integer_
  stage2_ids <- integer(0)
} else {
  fs_summary <- readRDS(fs_path)
  # Filter to successful rows for this region. Some summary files only contain
  # successful rows (no status column), so guard against that shape.
  if ("status" %in% names(fs_summary)) {
    fs_success <- fs_summary[fs_summary$region == region_arg & fs_summary$status == "success", ]
  } else {
    fs_success <- fs_summary[fs_summary$region == region_arg, ]
  }
  stage2_n   <- nrow(fs_success)
  # Map transition names back to id_trans via Stage 1 lookup
  stage2_ids <- sort(unique(
    vt_viable$id_trans[vt_viable$transition %in% fs_success$transition]
  ))
  stage2_ids <- stage2_ids[!is.na(stage2_ids)]
}

# ============================================================
# 5. Stage 3: reconciliation RDS (model_status == "success")
# ============================================================
recon_path <- file.path(
  config[["transition_model_eval_dir"]],
  period_arg,
  sprintf("transition_modelling_reconciliation_%s.rds", period_arg)
)
if (!file.exists(recon_path)) {
  cat(sprintf("WARNING: Stage 3 artifact not found: %s\n", recon_path))
  cat("  Run transition_modelling() first to produce this artifact.\n")
  stage3_ids <- integer(0)
} else {
  recon <- readRDS(recon_path)
  stage3_success <- recon[recon$region == region_arg & recon$model_status == "success", ]
  stage3_ids <- sort(unique(stage3_success$id_trans[!is.na(stage3_success$id_trans)]))
}

# ============================================================
# 6. Stage 4: rate CSV (first timestep)
# ============================================================
start_year <- config[["simulation_start_year"]]
rate_csv_path <- file.path(
  config[["trans_rate_table_dir"]],
  scenario_arg,
  region_arg,
  sprintf("%s-%s-trans_rates-%d.csv", scenario_arg, region_arg, start_year)
)
if (!file.exists(rate_csv_path)) {
  cat(sprintf("WARNING: Stage 4 artifact not found: %s\n", rate_csv_path))
  cat("  Run simulation_trans_rates_prep() first.\n")
  stage4_ids <- integer(0)
} else {
  rate_csv <- read.csv(rate_csv_path, check.names = FALSE, stringsAsFactors = FALSE)
  stage4_ids <- sort(unique(rate_csv$id_trans[!is.na(rate_csv$id_trans)]))
}

# ============================================================
# 7. Build count table (all four stages)
# ============================================================
stage_counts <- data.frame(
  stage    = c(
    "1 (viable_transitions_lists.csv)",
    "2 (feature_selection_summary, status=success)",
    "3 (reconciliation, model_status=success)",
    "4 (rate CSV, first timestep)"
  ),
  artifact = c(vt_path, fs_path, recon_path, rate_csv_path),
  n_transitions = c(
    length(stage1_ids),
    if (is.na(stage2_n)) NA_integer_ else length(stage2_ids),
    length(stage3_ids),
    length(stage4_ids)
  ),
  stringsAsFactors = FALSE
)

cat("=== Transition Counts by Stage ===\n")
print(stage_counts, row.names = FALSE)
cat("\n")

# ============================================================
# 8. Set difference report
# ============================================================
has_diff <- FALSE

# Stage 1 vs Stage 2
diff_1_to_2 <- setdiff(stage1_ids, stage2_ids)
diff_2_to_1 <- setdiff(stage2_ids, stage1_ids)
if (length(diff_1_to_2) > 0 || length(diff_2_to_1) > 0) {
  has_diff <- TRUE
  cat("=== SET DIFFERENCES: Stage 1 vs Stage 2 ===\n")
  if (length(diff_1_to_2) > 0) {
    cat(sprintf("  In Stage 1 but NOT in Stage 2 (id_trans): %s\n",
                paste(diff_1_to_2, collapse = ", ")))
    cat("  (These transitions failed or were missing from feature selection)\n")
  }
  if (length(diff_2_to_1) > 0) {
    cat(sprintf("  In Stage 2 but NOT in Stage 1 (id_trans): %s\n",
                paste(diff_2_to_1, collapse = ", ")))
  }
  cat("\n")
} else {
  cat("  Stage 1 vs Stage 2: OK (sets match)\n")
}

# Stage 1 vs Stage 3
diff_1_to_3 <- setdiff(stage1_ids, stage3_ids)
diff_3_to_1 <- setdiff(stage3_ids, stage1_ids)
if (length(diff_1_to_3) > 0 || length(diff_3_to_1) > 0) {
  has_diff <- TRUE
  cat("=== SET DIFFERENCES: Stage 1 vs Stage 3 ===\n")
  if (length(diff_1_to_3) > 0) {
    cat(sprintf("  In Stage 1 but NOT in Stage 3 (id_trans): %s\n",
                paste(diff_1_to_3, collapse = ", ")))
  }
  if (length(diff_3_to_1) > 0) {
    cat(sprintf("  In Stage 3 but NOT in Stage 1 (id_trans): %s\n",
                paste(diff_3_to_1, collapse = ", ")))
  }
  cat("\n")
} else {
  cat("  Stage 1 vs Stage 3: OK (sets match)\n")
}

# Stage 1 vs Stage 4
diff_1_to_4 <- setdiff(stage1_ids, stage4_ids)
diff_4_to_1 <- setdiff(stage4_ids, stage1_ids)
if (length(diff_1_to_4) > 0 || length(diff_4_to_1) > 0) {
  has_diff <- TRUE
  cat("=== SET DIFFERENCES: Stage 1 vs Stage 4 ===\n")
  if (length(diff_1_to_4) > 0) {
    cat(sprintf("  In Stage 1 but NOT in Stage 4 (id_trans): %s\n",
                paste(diff_1_to_4, collapse = ", ")))
    cat("  (These may be legitimately excluded by forbidden_from_classes or unmodelled transitions)\n")
  }
  if (length(diff_4_to_1) > 0) {
    cat(sprintf("  In Stage 4 but NOT in Stage 1 (id_trans): %s\n",
                paste(diff_4_to_1, collapse = ", ")))
  }
  cat("\n")
} else {
  cat("  Stage 1 vs Stage 4: OK (sets match)\n")
}

# ============================================================
# 9. Exit code
# ============================================================
cat("\n")
if (has_diff) {
  cat("AUDIT RESULT: FAIL -- set differences detected (see above)\n")
  quit(status = 1)
} else {
  cat("AUDIT RESULT: PASS -- all stages agree on transition set\n")
  quit(status = 0)
}
