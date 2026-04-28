#!/usr/bin/env Rscript
#' Summarise allocation-step profiling logs.
#'
#' Walks worker_logs/*.log under a simulation output directory, parses the
#' `PROFILE region=... stage=... elapsed=...s rss_*=...MB peak_rss=...MB`
#' lines emitted by prof_toc(), and the
#' `PROFILE_MEM region=... summary rss=...MB peak_rss=...MB`
#' lines emitted by prof_mem_summary(), and produces a per-(region, stage)
#' summary table with mean elapsed time, mean RSS delta, and max peak RSS.
#'
#' Usage:
#'   Rscript scripts/summarise_allocation_profile.r <sim_dir> [out_csv]
#'
#'   sim_dir  Path to a scenario simulation directory, e.g.
#'            outputs/simulations/BAU. The script recursively scans for
#'            worker_logs/*.log under it. (Passing the parent
#'            outputs/simulations is also fine — all scenarios are merged.)
#'   out_csv  Optional output CSV path. Defaults to <sim_dir>/profile_summary.csv.
#'
#' The `region_total` row is the one to use when sizing SLURM
#' `--mem-per-cpu`: max_peak_rss_MB at that stage is the worst-case per-worker
#' resident memory observed across the run.
#'
#' The "Per-region end-of-run peak RSS" section uses the dedicated
#' PROFILE_MEM summary lines (emitted once per region after all stages
#' complete) and is the most reliable single number for --mem-per-cpu sizing.

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1L) {
  stop(
    "Usage: Rscript scripts/summarise_allocation_profile.r ",
    "<sim_dir> [out_csv]",
    call. = FALSE
  )
}

sim_dir <- args[[1L]]
if (!dir.exists(sim_dir)) {
  stop(sprintf("sim_dir does not exist: %s", sim_dir), call. = FALSE)
}
out_csv <- if (length(args) >= 2L) {
  args[[2L]]
} else {
  file.path(sim_dir, "profile_summary.csv")
}

log_files <- list.files(
  sim_dir,
  pattern = "^worker_.*\.log$",
  recursive = TRUE,
  full.names = TRUE
)
if (length(log_files) == 0L) {
  stop(
    sprintf("No worker_*.log files found under %s", sim_dir),
    call. = FALSE
  )
}
message(sprintf("Found %d worker log file(s)", length(log_files)))

# Match: PROFILE region=... stage=... elapsed=...s rss_before=...MB
# rss_after=...MB rss_delta=...MB peak_rss=...MB ...
# All memory fields tolerate "NA" so Windows dev logs still parse.
profile_re <- paste0(
  "PROFILE region=([^ ]+) stage=([^ ]+) elapsed=([0-9.]+)s ",
  "rss_before=(NA|[0-9.\-]+)MB ",
  "rss_after=(NA|[0-9.\-]+)MB ",
  "rss_delta=(NA|[+\-0-9.]+)MB ",
  "peak_rss=(NA|[0-9.\-]+)MB"
)

# Match: PROFILE_MEM region=... summary rss=...MB peak_rss=...MB
profile_mem_re <- paste0(
  "PROFILE_MEM region=([^ ]+) summary ",
  "rss=(NA|[0-9.\-]+)MB ",
  "peak_rss=(NA|[0-9.\-]+)MB"
)

parse_one <- function(path) {
  lines <- tryCatch(
    readLines(path, warn = FALSE),
    error = function(e) character()
  )
  if (length(lines) == 0L) {
    return(list(profile = NULL, profile_mem = NULL))
  }

  # Parse PROFILE lines
  m <- regmatches(lines, regexec(profile_re, lines))
  hits <- m[lengths(m) == 8L]
  to_num <- function(x) {
    x[x == "NA"] <- NA_character_
    suppressWarnings(as.numeric(x))
  }
  profile_df <- if (length(hits) > 0L) {
    mat <- do.call(rbind, hits)
    data.frame(
      log_file = path,
      region = mat[, 2L],
      stage = mat[, 3L],
      elapsed_s = to_num(mat[, 4L]),
      rss_before_MB = to_num(mat[, 5L]),
      rss_after_MB = to_num(mat[, 6L]),
      rss_delta_MB = to_num(mat[, 7L]),
      peak_rss_MB = to_num(mat[, 8L]),
      stringsAsFactors = FALSE
    )
  } else {
    NULL
  }

  # Parse PROFILE_MEM lines
  mm <- regmatches(lines, regexec(profile_mem_re, lines))
  mem_hits <- mm[lengths(mm) == 4L]
  profile_mem_df <- if (length(mem_hits) > 0L) {
    mat2 <- do.call(rbind, mem_hits)
    data.frame(
      log_file = path,
      region = mat2[, 2L],
      rss_MB = to_num(mat2[, 3L]),
      peak_rss_MB = to_num(mat2[, 4L]),
      stringsAsFactors = FALSE
    )
  } else {
    NULL
  }

  list(profile = profile_df, profile_mem = profile_mem_df)
}

parsed <- lapply(log_files, parse_one)
all_rows <- do.call(rbind, lapply(parsed, `[[`, "profile"))
all_mem_rows <- do.call(rbind, lapply(parsed, `[[`, "profile_mem"))

if (is.null(all_rows) || nrow(all_rows) == 0L) {
  stop("No PROFILE lines parsed from any worker log.", call. = FALSE)
}
message(sprintf("Parsed %d PROFILE lines", nrow(all_rows)))
if (!is.null(all_mem_rows) && nrow(all_mem_rows) > 0L) {
  message(sprintf("Parsed %d PROFILE_MEM lines", nrow(all_mem_rows)))
}

# Aggregate: per (region, stage), keep n, mean elapsed, mean delta, max peak.
agg <- aggregate(
  cbind(elapsed_s, rss_delta_MB, peak_rss_MB) ~ region + stage,
  data = all_rows,
  FUN = function(x) {
    c(
      n = length(x),
      mean = mean(x, na.rm = TRUE),
      max = suppressWarnings(max(x, na.rm = TRUE))
    )
  },
  na.action = na.pass
)

summary_df <- data.frame(
  region = agg$region,
  stage = agg$stage,
  n = as.integer(agg$elapsed_s[, "n"]),
  mean_elapsed_s = round(agg$elapsed_s[, "mean"], 3L),
  mean_rss_delta_MB = round(agg$rss_delta_MB[, "mean"], 1L),
  max_peak_rss_MB = round(agg$peak_rss_MB[, "max"], 1L),
  stringsAsFactors = FALSE
)
# Sort: region_total first per region, then by stage name
stage_order <- ifelse(summary_df$stage == "region_total", 0L, 1L)
summary_df <- summary_df[
  order(summary_df$region, stage_order, summary_df$stage),
]
rownames(summary_df) <- NULL

dir.create(dirname(out_csv), recursive = TRUE, showWarnings = FALSE)
write.csv(summary_df, out_csv, row.names = FALSE)
message(sprintf("Wrote summary CSV: %s", out_csv))

cat("\n")
cat(sprintf("Profiling summary (%d region x stage rows):\n", nrow(summary_df)))
print(summary_df, row.names = FALSE)

# Per-region peak from PROFILE_MEM end-of-run summary lines (preferred).
# Falls back to max peak_rss_MB across all PROFILE stages if no PROFILE_MEM
# lines are present.
cat("\nPer-region end-of-run peak RSS (use this for --mem-per-cpu sizing):\n")
if (!is.null(all_mem_rows) && nrow(all_mem_rows) > 0L) {
  per_region <- aggregate(
    peak_rss_MB ~ region,
    data = all_mem_rows,
    FUN = function(x) suppressWarnings(max(x, na.rm = TRUE))
  )
  per_region$peak_rss_MB <- round(per_region$peak_rss_MB, 1L)
  cat("  (from PROFILE_MEM summary lines)\n")
} else {
  cat("  (PROFILE_MEM lines not found — falling back to max across PROFILE stages)\n")
  per_region <- aggregate(
    peak_rss_MB ~ region,
    data = all_rows,
    FUN = function(x) suppressWarnings(max(x, na.rm = TRUE))
  )
  per_region$peak_rss_MB <- round(per_region$peak_rss_MB, 1L)
}
print(per_region, row.names = FALSE)
