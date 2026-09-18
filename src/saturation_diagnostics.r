# ---------------------------------------------------------------------------
# Saturation diagnostics — per-transition probability-map and placed-cell
# metrics shared by the inline end-of-region summary (called from
# run_allocation_one_timestep() in src/allocation.r) and the standalone
# scripts/diagnose_allocation_saturation.r post-hoc diagnostic.
#
# All three functions are pure helpers: this file does NOT source allocation.r
# or any heavy module. The caller is responsible for sourcing src/utils.r
# (for log_msg()) before sourcing this file, so the inline hook and the
# standalone CLI can share the same metric implementation.
#
# Metric definitions (CONTEXT.md D-06, RESEARCH.md "Pattern 4"):
#   coverage           - fraction of valid (non-NA) cells with prob > 0
#   p50, p90, p95, p99 - quantile of the probability map distribution
#   pmax               - max(prob_vals) (the 1.0 quantile)
#   demanded_cells     - round(Rate * count(anterior_vals == from_val))
#                        (Pitfall 3: the denominator is the FROM-class cell
#                        count, NOT total valid cells in the region)
#   placed_cells       - count((anterior_vals == from_val) & (posterior_vals == to_val))
#   placed_frac        - placed_cells / demanded_cells (NA when demanded == 0)
#   demand_vs_capacity - demanded_cells / count(prob_vals >= p50)
#
# Implementation notes:
#   - All read.csv() calls use check.names = FALSE so Dinamica's `From*` and
#     `To*` key-column markers survive the round-trip (Pitfall 4).
#   - All raster reads use terra::values(mat = FALSE) once per raster, then
#     all aggregation is base-R vector ops (RESEARCH.md "Shared Patterns").
#   - Logging goes through log_msg() loaded from src/utils.r; never
#     message() or cat() directly. The standalone script may pass
#     log_file = NULL to send messages to stdout only.
#   - This module computes only the per-transition metrics listed above
#     (coverage, quantiles, demand-vs-capacity, placed-vs-demanded). Patch
#     geometry, neighbourhood statistics, and any spatial-structure metrics
#     are explicitly deferred (CONTEXT.md D-07).
#   - No top-level package loads (CONVENTIONS.md "No package-load calls in
#     src/"); namespaces are referenced explicitly via terra::, stats::,
#     utils::.
#
# Usage:
#   source("src/utils.r")
#   source("src/saturation_diagnostics.r")
#   write_saturation_summary(
#     work_dir       = "outputs/simulations/BAU/2026/region_costa_peruana",
#     scenario       = "BAU",
#     region_label   = "costa_peruana",
#     region_suffix  = "costa_peruana",
#     year_ant       = 2022L,
#     anterior_path  = "outputs/simulations/.../anterior.tif",
#     posterior_path = "outputs/simulations/.../posterior.tif",
#     config         = get_config(),
#     log_file       = "outputs/simulations/.../worker.log"
#   )
# ---------------------------------------------------------------------------


# Compute per-transition saturation metrics for one (scenario, region, year_ant)
# allocation run.
#
# Iterates filtered_df row-by-row (k = 1..N) and assembles one data.frame row
# per active transition. The row's probability map is loaded from
# file.path(prob_map_dir, sprintf("%03d_id_trans_%d.tif", k, id_trans)), which
# matches the alphabetic-prefix-equals-row-position contract Dinamica enforces
# in CreateCubeOfProbabilityMaps (see PLAN.md alignment contract).
#
# Returns a data.frame with exactly the 16 columns documented in
# RESEARCH.md "Data Structures and Join Keys" for saturation_summary.csv:
#   id_trans, from_val, to_val, rate, demanded_cells, placed_cells,
#   placed_frac, coverage, p50, p90, p95, p99, pmax, demand_vs_capacity,
#   exempt (logical), floor_met (logical).
compute_per_transition_metrics <- function(
  filtered_df,
  prob_map_dir,
  anterior_path,
  posterior_path,
  threshold,
  exempt
) {
  # Defensive arg checks — fail loud rather than write garbage rows.
  if (!is.data.frame(filtered_df)) {
    stop("compute_per_transition_metrics: filtered_df must be a data.frame")
  }
  required_cols <- c("From*", "To*", "Rate", "id_trans")
  missing_cols <- setdiff(required_cols, colnames(filtered_df))
  if (length(missing_cols) > 0L) {
    stop(sprintf(
      "compute_per_transition_metrics: filtered_df missing required column(s): %s",
      paste(missing_cols, collapse = ", ")
    ))
  }
  if (!file.exists(anterior_path)) {
    stop(sprintf(
      "compute_per_transition_metrics: anterior_path not found: %s",
      anterior_path
    ))
  }
  if (!file.exists(posterior_path)) {
    stop(sprintf(
      "compute_per_transition_metrics: posterior_path not found: %s",
      posterior_path
    ))
  }
  if (!is.numeric(threshold) || length(threshold) != 1L) {
    stop("compute_per_transition_metrics: threshold must be a length-1 numeric")
  }
  if (is.null(exempt)) {
    exempt <- list()
  }

  # Pull each raster's cell vector ONCE; all per-transition aggregation
  # below is O(rows * cells) base-R vector ops, never re-walking the raster.
  anterior <- terra::rast(anterior_path)
  posterior <- terra::rast(posterior_path)
  anterior_vals <- terra::values(anterior, mat = FALSE)
  posterior_vals <- terra::values(posterior, mat = FALSE)

  metrics <- lapply(seq_len(nrow(filtered_df)), function(k) {
    from_val <- filtered_df[["From*"]][[k]]
    to_val <- filtered_df[["To*"]][[k]]
    rate <- filtered_df[["Rate"]][[k]]
    id_t <- filtered_df[["id_trans"]][[k]]

    # area_t = FROM-class cell count (Pitfall 3 — NOT total valid cells)
    area_t <- sum(anterior_vals == from_val, na.rm = TRUE)
    demanded_cells <- as.integer(round(rate * area_t))
    placed_cells <- as.integer(sum(
      anterior_vals == from_val & posterior_vals == to_val,
      na.rm = TRUE
    ))
    placed_frac <- if (demanded_cells > 0L) {
      placed_cells / demanded_cells
    } else {
      NA_real_
    }

    # Load this transition's probability map (the %03d prefix MUST match k)
    tif_path <- file.path(
      prob_map_dir,
      sprintf("%03d_id_trans_%d.tif", k, id_t)
    )
    if (file.exists(tif_path)) {
      prob_rast <- terra::rast(tif_path)
      prob_vals <- terra::values(prob_rast, mat = FALSE)
      prob_vals <- prob_vals[!is.na(prob_vals)]
      n_valid <- length(prob_vals)
    } else {
      prob_vals <- numeric(0L)
      n_valid <- 0L
    }

    coverage <- if (n_valid > 0L) {
      sum(prob_vals > 0) / n_valid
    } else {
      NA_real_
    }

    quants <- if (n_valid > 0L) {
      stats::quantile(prob_vals, c(0.5, 0.9, 0.95, 0.99, 1.0), na.rm = TRUE)
    } else {
      rep(NA_real_, 5L)
    }
    cells_above_p50 <- if (n_valid > 0L && !is.na(quants[[1L]])) {
      sum(prob_vals >= quants[[1L]], na.rm = TRUE)
    } else {
      0L
    }
    demand_vs_capacity <- if (cells_above_p50 > 0L) {
      demanded_cells / cells_above_p50
    } else {
      NA_real_
    }

    # exempt match: list of {from_lulc, to_lulc, reason} records
    is_exempt <- if (length(exempt) > 0L) {
      any(vapply(
        exempt,
        function(e) {
          isTRUE(e$from_lulc == from_val) && isTRUE(e$to_lulc == to_val)
        },
        logical(1L)
      ))
    } else {
      FALSE
    }
    floor_met <- !is.na(placed_frac) && placed_frac >= threshold

    data.frame(
      id_trans = id_t,
      from_val = from_val,
      to_val = to_val,
      rate = rate,
      demanded_cells = demanded_cells,
      placed_cells = placed_cells,
      placed_frac = placed_frac,
      coverage = coverage,
      p50 = unname(quants[[1L]]),
      p90 = unname(quants[[2L]]),
      p95 = unname(quants[[3L]]),
      p99 = unname(quants[[4L]]),
      pmax = unname(quants[[5L]]),
      demand_vs_capacity = demand_vs_capacity,
      exempt = is_exempt,
      floor_met = floor_met,
      stringsAsFactors = FALSE
    )
  })

  do.call(rbind, metrics)
}


# End-of-region orchestrator. Reads the work_dir's filtered trans_rates.csv,
# computes per-transition metrics via compute_per_transition_metrics(), writes
# the result to file.path(work_dir, "saturation_summary.csv"), and emits the
# AUDIT log line via log_audit_saturation_line().
#
# Returns invisible(summary_df) so callers can chain further inspection.
write_saturation_summary <- function(
  work_dir,
  scenario,
  region_label,
  region_suffix,
  year_ant,
  anterior_path,
  posterior_path,
  config,
  log_file
) {
  trans_rates_path <- file.path(work_dir, "trans_rates.csv")
  if (!file.exists(trans_rates_path)) {
    stop(sprintf(
      "write_saturation_summary: trans_rates.csv not found in work_dir: %s",
      trans_rates_path
    ))
  }
  prob_map_dir <- file.path(work_dir, "probability_map_dir")

  filtered_df <- utils::read.csv(trans_rates_path, check.names = FALSE)
  threshold <- config[["simulation_trans_rates_params"]][["saturation_threshold"]]
  exempt <- config[["simulation_trans_rates_params"]][["saturation_exempt"]]
  if (is.null(threshold)) {
    threshold <- 0.90
  }
  if (is.null(exempt)) {
    exempt <- list()
  }

  summary_df <- compute_per_transition_metrics(
    filtered_df = filtered_df,
    prob_map_dir = prob_map_dir,
    anterior_path = anterior_path,
    posterior_path = posterior_path,
    threshold = threshold,
    exempt = exempt
  )

  out_path <- file.path(work_dir, "saturation_summary.csv")
  utils::write.csv(summary_df, out_path, row.names = FALSE)
  log_msg(
    sprintf(
      "Wrote saturation_summary.csv (%d rows): %s",
      nrow(summary_df),
      out_path
    ),
    log_file
  )

  log_audit_saturation_line(
    summary = summary_df,
    scenario = scenario,
    region_label = region_label,
    year_ant = year_ant,
    threshold = threshold,
    log_file = log_file
  )

  invisible(summary_df)
}


# Emit a single structured AUDIT log line summarising the saturation outcome
# for one (scenario, region, year_ant) run. Format follows the Phase 3.2 AUDIT
# log line grammar (RESEARCH.md "AUDIT log line shape", PATTERNS.md
# "AUDIT log line shape"):
#
#   AUDIT stage=saturation scenario=%s region=%s year_ant=%d
#         active=%d met=%d exempt=%d failed=%d threshold=%.2f
#
# The cross-stage audit script (scripts/audit_transition_pipeline.r) can pick
# the line up by grepping for the literal prefix "AUDIT stage=saturation".
log_audit_saturation_line <- function(
  summary,
  scenario,
  region_label,
  year_ant,
  threshold,
  log_file
) {
  if (!is.data.frame(summary)) {
    stop("log_audit_saturation_line: summary must be a data.frame")
  }
  required_cols <- c("floor_met", "exempt")
  missing_cols <- setdiff(required_cols, colnames(summary))
  if (length(missing_cols) > 0L) {
    stop(sprintf(
      "log_audit_saturation_line: summary missing required column(s): %s",
      paste(missing_cols, collapse = ", ")
    ))
  }

  n_active <- nrow(summary)
  n_met <- sum(summary$floor_met, na.rm = TRUE)
  n_exempt <- sum(summary$exempt, na.rm = TRUE)
  n_failed <- sum(!summary$floor_met & !summary$exempt, na.rm = TRUE)

  log_msg(
    sprintf(
      "AUDIT stage=saturation scenario=%s region=%s year_ant=%d active=%d met=%d exempt=%d failed=%d threshold=%.2f",
      as.character(scenario),
      as.character(region_label),
      as.integer(year_ant),
      as.integer(n_active),
      as.integer(n_met),
      as.integer(n_exempt),
      as.integer(n_failed),
      as.numeric(threshold)
    ),
    log_file
  )
  invisible(NULL)
}
