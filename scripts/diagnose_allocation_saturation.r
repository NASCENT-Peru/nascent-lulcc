#!/usr/bin/env Rscript
#' Post-hoc allocation saturation diagnostic.
#'
#' Classifies each active allocation transition as aligned_ok,
#' saturation_bound, or exempt using the same metric helper that writes the
#' inline saturation_summary.csv during allocation.
#'
#' The script reads a region work_dir produced by allocation:
#' trans_rates.csv, anterior.tif, posterior.tif, probability_map_dir/, and
#' optionally a Dinamica log containing the Remaining-Transitions matrix. It
#' writes saturation_diagnosis.csv beside saturation_summary.csv and prints a
#' compact console summary for operator review.
#'
#' Usage:
#'   Rscript scripts/diagnose_allocation_saturation.r <work_dir>
#'   Rscript scripts/diagnose_allocation_saturation.r <work_dir> --dinamica-log <log_path>
#'
#'   work_dir        Region work_dir produced by allocation.
#'   --dinamica-log  Optional path to logs/dinamica-*.log for
#'                   Remaining-Transitions cross-validation.

# ---------------------------------------------------------------------------
# Set working directory to project root (needs to happen before sourcing
# `src/*.r`).
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Source helpers. The order matters: setup gives get_config(), utils gives
# log_msg(), and saturation_diagnostics.r consumes log_msg().
# ---------------------------------------------------------------------------
src_files <- c(
  "src/setup.r",
  "src/utils.r",
  "src/saturation_diagnostics.r"
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
    }
  )
}
cat("\n")

usage <- "Usage: Rscript scripts/diagnose_allocation_saturation.r <work_dir> [--dinamica-log <path>]"

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1L) {
  stop(usage, call. = FALSE)
}

work_dir <- args[[1L]]
dinamica_log_path <- NULL
i <- 2L
while (i <= length(args)) {
  arg <- args[[i]]
  if (identical(arg, "--dinamica-log")) {
    if (i + 1L > length(args)) {
      stop("--dinamica-log requires a path argument", call. = FALSE)
    }
    dinamica_log_path <- args[[i + 1L]]
    i <- i + 1L
  } else if (startsWith(arg, "--dinamica-log=")) {
    dinamica_log_path <- sub("^--dinamica-log=", "", arg)
  } else {
    stop(sprintf("Unknown argument: %s\n%s", arg, usage), call. = FALSE)
  }
  i <- i + 1L
}

if (!dir.exists(work_dir)) {
  stop(sprintf("work_dir does not exist: %s", work_dir), call. = FALSE)
}

required_files <- c("trans_rates.csv", "anterior.tif", "posterior.tif")
for (artifact in required_files) {
  artifact_path <- file.path(work_dir, artifact)
  if (!file.exists(artifact_path)) {
    stop(sprintf("Required artifact missing: %s", artifact_path), call. = FALSE)
  }
}

prob_map_dir <- file.path(work_dir, "probability_map_dir")
if (!dir.exists(prob_map_dir)) {
  stop(sprintf("Required artifact missing: %s", prob_map_dir), call. = FALSE)
}

if (!is.null(dinamica_log_path) && !file.exists(dinamica_log_path)) {
  stop(sprintf("dinamica log does not exist: %s", dinamica_log_path), call. = FALSE)
}

parse_remaining_transitions_matrix <- function(log_path) {
  lines <- readLines(log_path, warn = FALSE)
  if (!any(grepl("Remaining[- ]Transitions", lines))) {
    return(data.frame(
      from_val = integer(),
      to_val = integer(),
      remaining_cells = integer()
    ))
  }

  header_idx <- grep("^[[:space:]]*From[[:space:]]*\\\\[[:space:]]*To[[:space:]]*\\|", lines)
  if (length(header_idx) == 0L) {
    return(data.frame(
      from_val = integer(),
      to_val = integer(),
      remaining_cells = integer()
    ))
  }
  header_idx <- header_idx[[length(header_idx)]]
  to_classes <- strsplit(
    sub("^.*\\|", "", lines[[header_idx]]),
    "[[:space:]]+"
  )[[1L]]
  to_classes <- to_classes[nzchar(to_classes)]
  to_classes <- suppressWarnings(as.integer(to_classes))

  out <- list()
  out_i <- 1L
  row_i <- header_idx + 1L
  while (
    row_i <= length(lines) &&
      grepl("^[[:space:]]*[0-9]+[[:space:]]*\\|", lines[[row_i]])
  ) {
    line <- lines[[row_i]]
    from_val <- as.integer(sub(
      "^[[:space:]]*([0-9]+)[[:space:]]*\\|.*$",
      "\\1",
      line
    ))
    cells <- strsplit(
      sub("^[[:space:]]*[0-9]+[[:space:]]*\\|", "", line),
      "[[:space:]]+"
    )[[1L]]
    cells <- cells[nzchar(cells)]
    n_cells <- min(length(cells), length(to_classes))
    if (n_cells > 0L) {
      for (j in seq_len(n_cells)) {
        cell <- cells[[j]]
        if (identical(cell, "XXXX")) next
        remaining <- if (identical(cell, "--")) {
          0L
        } else {
          suppressWarnings(as.integer(gsub(",", "", cell)))
        }
        if (is.na(remaining)) next
        out[[out_i]] <- data.frame(
          from_val = from_val,
          to_val = to_classes[[j]],
          remaining_cells = remaining,
          stringsAsFactors = FALSE
        )
        out_i <- out_i + 1L
      }
    }
    row_i <- row_i + 1L
  }

  if (length(out) == 0L) {
    data.frame(from_val = integer(), to_val = integer(), remaining_cells = integer())
  } else {
    do.call(rbind, out)
  }
}

config <- get_config()
sim_params <- config[["simulation_trans_rates_params"]]
threshold <- sim_params[["saturation_threshold"]]
if (is.null(threshold)) threshold <- 0.90
exempt <- sim_params[["saturation_exempt"]]
if (is.null(exempt)) exempt <- list()

summary_path <- file.path(work_dir, "saturation_summary.csv")
if (file.exists(summary_path)) {
  message(sprintf("Found inline saturation summary: %s", summary_path))
}

filtered_df <- utils::read.csv(
  file.path(work_dir, "trans_rates.csv"),
  check.names = FALSE
)
anterior_path <- file.path(work_dir, "anterior.tif")
posterior_path <- file.path(work_dir, "posterior.tif")

metrics_df <- compute_per_transition_metrics(
  filtered_df = filtered_df,
  prob_map_dir = prob_map_dir,
  anterior_path = anterior_path,
  posterior_path = posterior_path,
  threshold = threshold,
  exempt = exempt
)

metrics_df$saturation_class <- ifelse(
  metrics_df$exempt,
  "exempt",
  ifelse(metrics_df$floor_met, "aligned_ok", "saturation_bound")
)

if (!is.null(dinamica_log_path)) {
  remaining_df <- parse_remaining_transitions_matrix(dinamica_log_path)
  metrics_df$remaining_cells <- NA_integer_
  if (nrow(remaining_df) > 0L) {
    match_key <- paste(metrics_df$from_val, metrics_df$to_val, sep = "->")
    remaining_key <- paste(remaining_df$from_val, remaining_df$to_val, sep = "->")
    matched <- match(match_key, remaining_key)
    metrics_df$remaining_cells <- remaining_df$remaining_cells[matched]
  }
  dinamica_frac <- metrics_df$placed_cells /
    (metrics_df$placed_cells + metrics_df$remaining_cells)
  metrics_df$dinamica_log_drift <- dinamica_frac - metrics_df$placed_frac
} else {
  metrics_df$dinamica_log_drift <- NA_real_
}

out_path <- file.path(work_dir, "saturation_diagnosis.csv")
utils::write.csv(metrics_df, out_path, row.names = FALSE)
message(sprintf("Wrote diagnosis CSV: %s", out_path))

cat("\n")
cat(sprintf("Saturation diagnosis (%d transitions):\n", nrow(metrics_df)))
print(table(metrics_df$saturation_class))
cat("\n")
cols <- c(
  "id_trans",
  "from_val",
  "to_val",
  "demanded_cells",
  "placed_cells",
  "placed_frac",
  "saturation_class"
)
print(metrics_df[, cols], row.names = FALSE)

quit(status = 0L)
