# Phase 5 Plan 14 (D-23): end-to-end fixture tests for
# scripts/verify_intervention_smoke.r.
#
# The verifier is the phase's regression gate for every future scenario and
# region, so the thing it must never do is say PASS about nothing (WR-07). These
# blocks drive the real script as a subprocess against a synthetic run output
# tree and pin:
#   - WR-07  a mask whose zone contains no cells is a FAIL, not a vacuous PASS;
#   - WR-07b maps_checked counts only assertions that had cells to assert on;
#   - CR-03  "argument is of length zero" / "subscript out of bounds" are fatal
#            forbidden markers (the exact crash signature of the missing
#            Prob_adjust_* defect);
#   - D-22   the 05-13 telemetry CSV exists, matches the AUDIT lines and shows
#            mean_after = 0 for Absolute-to-0 target rows.
#
# IN-06: the repository root is resolved WITHOUT base R's null-coalescing
# operator, which would be taken from base at file-source time, before
# src/utils.r is available, and which the engine deliberately avoids.

library(testthat)

.smoke_repo_root <- local({
  has_script <- function(p) {
    !is.null(p) && nzchar(p) &&
      file.exists(file.path(p, "scripts", "verify_intervention_smoke.r"))
  }
  cand <- tryCatch(
    normalizePath(testthat::test_path("..", ".."), winslash = "/", mustWork = FALSE),
    error = function(e) NULL
  )
  if (is.null(cand) || identical(cand, "")) cand <- NULL
  if (!has_script(cand)) {
    alt <- normalizePath(file.path(getwd(), "..", ".."), winslash = "/", mustWork = FALSE)
    if (has_script(alt)) {
      cand <- alt
    } else {
      here <- normalizePath(getwd(), winslash = "/", mustWork = FALSE)
      if (has_script(here)) cand <- here
    }
  }
  if (is.null(cand)) cand <- normalizePath(getwd(), winslash = "/", mustWork = FALSE)
  cand
})

.smoke_script <- file.path(.smoke_repo_root, "scripts", "verify_intervention_smoke.r")
.smoke_rscript <- file.path(R.home("bin"), "Rscript")

# The 25-column telemetry contract published by plan 05-13.
.smoke_tel_cols <- c(
  "scenario", "region", "year", "intervention_id", "rank", "type", "zone",
  "mask", "target_class", "n_target", "n_changed", "mean_before", "mean_after",
  "sd_before", "sd_after", "p05_delta", "p25_delta", "p50_delta", "p75_delta",
  "p95_delta", "min_delta", "max_delta", "sum_abs_delta", "prob_mass_before",
  "prob_mass_after"
)

# --------------------------------------------------------------------------
# Raster fixtures: the same 4x5 grid idiom as test-spatial-interventions.R, but
# the verifier crops the mask to the probability map and compares geometry
# directly (no cell_index), so mask and map only have to share this one grid.

.smoke_grid <- function() {
  terra::rast(nrows = 4, ncols = 5, xmin = 0, xmax = 5, ymin = 0, ymax = 4)
}

# Cells the mask marks as inside. Chosen so the zone is non-empty on the happy
# path and trivially emptied (cells = integer(0)) for the WR-07 fixture.
.smoke_mask_cells <- c(2L, 7L, 13L)

.smoke_write_raster <- function(path, values) {
  r <- .smoke_grid()
  terra::values(r) <- values
  suppressWarnings(terra::writeRaster(r, path, overwrite = TRUE))
  path
}

.smoke_write_mask <- function(path, cells = .smoke_mask_cells) {
  v <- rep(NA_real_, 20L)
  if (length(cells) > 0L) v[cells] <- 1
  .smoke_write_raster(path, v)
}

# A probability map that is exactly 0 on the masked cells and positive
# elsewhere: what an Absolute-to-0 Inside intervention is supposed to produce.
.smoke_write_prob <- function(path, zero_cells = .smoke_mask_cells, fill = 0.5) {
  v <- rep(fill, 20L)
  if (length(zero_cells) > 0L) v[zero_cells] <- 0
  .smoke_write_raster(path, v)
}

.smoke_audit_iv <- function(region_label, scenario, year) {
  sprintf(
    paste0(
      "2026-01-01 00:00:00 AUDIT stage=intervention region=%s scenario=%s ",
      "year=%d id=iv_abs rank=1 type=Absolute zone=Inside to_vals=105,104 ",
      "mask=mask_a.tif rows_target=6 rows_changed=6 delta_mean=-0.27 ",
      "delta_med=-0.19 delta_sd=0.192146 delta_min=-0.6 delta_max=-0.1 ",
      "n_inc=0 n_dec=6 sum_abs_delta=1.62"
    ),
    region_label, scenario, as.integer(year)
  )
}

.smoke_audit_summary <- function(region_label, scenario, year) {
  sprintf(
    paste0(
      "2026-01-01 00:00:01 AUDIT stage=intervention_summary region=%s ",
      "scenario=%s year=%d n_interventions=1 cells_sum_gt1=0"
    ),
    region_label, scenario, as.integer(year)
  )
}

.smoke_telemetry <- function(scenario, region, year) {
  data.frame(
    scenario = rep(scenario, 2L),
    region = rep(region, 2L),
    year = rep(as.integer(year), 2L),
    intervention_id = rep("iv_abs", 2L),
    rank = c(1L, 1L),
    type = rep("Absolute", 2L),
    zone = rep("Inside", 2L),
    mask = rep("mask_a.tif", 2L),
    target_class = c(105L, 104L),
    n_target = c(3L, 3L),
    n_changed = c(3L, 3L),
    mean_before = c(0.4, 0.14),
    mean_after = c(0, 0),
    sd_before = c(0.2, 0.04),
    sd_after = c(0, 0),
    p05_delta = c(-0.58, -0.176),
    p25_delta = c(-0.5, -0.16),
    p50_delta = c(-0.4, -0.14),
    p75_delta = c(-0.3, -0.12),
    p95_delta = c(-0.22, -0.104),
    min_delta = c(-0.6, -0.18),
    max_delta = c(-0.2, -0.1),
    sum_abs_delta = c(1.2, 0.42),
    prob_mass_before = c(1.2, 0.42),
    prob_mass_after = c(0, 0),
    stringsAsFactors = FALSE
  )
}

.smoke_write_telemetry <- function(path, tel) {
  utils::write.csv(tel, path, row.names = FALSE, quote = FALSE, na = "")
  path
}

# --------------------------------------------------------------------------
# The full run-output fixture.

.smoke_fixture <- function(root, scenario = "BAU", region = "r1", year = 2028L,
                           region_label = "R1", mask_cells = .smoke_mask_cells) {
  output_root <- file.path(root, "out")
  region_dir <- file.path(
    output_root, scenario, as.character(year), paste0("region_", region)
  )
  prob_dir <- file.path(region_dir, "probability_map_dir")
  mask_dir <- file.path(root, "masks")
  iv_dir <- file.path(root, "interventions")
  dir.create(prob_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(mask_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(iv_dir, recursive = TRUE, showWarnings = FALSE)

  .smoke_write_raster(file.path(region_dir, "posterior.tif"), rep(1, 20L))
  mask_path <- .smoke_write_mask(file.path(mask_dir, "mask_a.tif"), mask_cells)

  # One row per target class. The verifier reconstructs the map filename from
  # the 1-based row index and id_trans, so row order is load-bearing.
  tr <- data.frame(
    From_lulc = c(101L, 101L),
    To_lulc = c(105L, 104L),
    Rate = c(0.1, 0.2),
    id_trans = c(11L, 12L),
    stringsAsFactors = FALSE
  )
  utils::write.csv(tr, file.path(region_dir, "trans_rates.csv"), row.names = FALSE)
  .smoke_write_prob(file.path(prob_dir, "001_id_trans_11.tif"), mask_cells)
  .smoke_write_prob(file.path(prob_dir, "002_id_trans_12.tif"), mask_cells)

  log_path <- file.path(region_dir, "worker_1.log")
  writeLines(
    c(
      "2026-01-01 00:00:00 Applying intervention: iv_abs",
      .smoke_audit_iv(region_label, scenario, year),
      .smoke_audit_summary(region_label, scenario, year)
    ),
    log_path
  )

  entry <- list(
    Intervention_stage = "Allocation",
    Intervention_ID = "iv_abs",
    Intervention_ranking = 1L,
    Mask_type = "Static",
    Intervention_mask = "mask_a.tif",
    Time_steps_implemented = list(as.integer(year)),
    Prob_adjust_type = "Absolute",
    Prob_adjust_value = 0,
    Prob_adjust_zone = "Inside",
    Transition_target_classes = list(
      "built_up_and_barren_lands", "high_intensity_agricultural_areas"
    )
  )
  yaml_path <- file.path(iv_dir, paste0(scenario, "_interventions.yml"))
  yaml::write_yaml(list(entry), yaml_path)

  csv_path <- file.path(
    region_dir,
    sprintf("intervention_prob_deltas_%s_%s_%d.csv", scenario, region, as.integer(year))
  )
  .smoke_write_telemetry(csv_path, .smoke_telemetry(scenario, region, year))

  list(
    scenario = scenario, region = region, year = as.integer(year),
    region_label = region_label, output_root = output_root,
    region_dir = region_dir, prob_dir = prob_dir, mask_dir = mask_dir,
    iv_dir = iv_dir, mask_path = mask_path, log_path = log_path,
    yaml_path = yaml_path, csv_path = csv_path
  )
}

# --------------------------------------------------------------------------
# Driving the real script.

.run_verifier <- function(fx, extra = character(0)) {
  args <- c(
    shQuote(.smoke_script),
    "--output-root", shQuote(fx$output_root),
    "--mask-dir", shQuote(fx$mask_dir),
    "--interventions-dir", shQuote(fx$iv_dir),
    "--scenario", fx$scenario,
    "--region", fx$region,
    "--year", as.character(fx$year),
    extra
  )
  out <- suppressWarnings(
    system2(.smoke_rscript, args, stdout = TRUE, stderr = TRUE)
  )
  status <- attr(out, "status")
  if (is.null(status)) status <- 0L
  list(status = as.integer(status), out = paste(out, collapse = "\n"))
}

# Exit status 2 is the script's "usage or sourcing error" code. It is a genuine
# environment problem ONLY when the project config could not be loaded; an
# unknown-flag or usage rejection is a real result and must be allowed to fail
# the expectations below.
.skip_if_config_unavailable <- function(res) {
  if (identical(res$status, 2L) &&
        grepl("ERROR sourcing|get_config|Loaded configuration for environment",
              res$out) &&
        !grepl("Unknown flag|Unknown argument|requires a value", res$out)) {
    testthat::skip(
      "verify_intervention_smoke could not load the project config in this environment"
    )
  }
  invisible(NULL)
}

.smoke_read_csv <- function(path) {
  utils::read.csv(path, check.names = FALSE, stringsAsFactors = FALSE)
}

# --------------------------------------------------------------------------

test_that("CR-03s: the forbidden marker list carries both length-zero messages", {
  # Static: runs even when the subprocess blocks skip.
  src <- paste(readLines(.smoke_script, warn = FALSE), collapse = "\n")
  expect_true(grepl("argument is of length zero", src, fixed = TRUE))
  expect_true(grepl("subscript out of bounds", src, fixed = TRUE))
})

test_that("WR-07: a mask with no cells in the zone fails instead of passing vacuously", {
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root, mask_cells = integer(0))
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(res$out, "0 non-NA cells in zone", fixed = TRUE)
})

test_that("WR-07b: maps_checked counts only non-vacuous assertions", {
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root, mask_cells = integer(0))
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_false(grepl("maps_checked=1", res$out, fixed = TRUE))
  expect_false(grepl("maps_checked=2", res$out, fixed = TRUE))
})

test_that("CR-03: 'argument is of length zero' is a forbidden marker", {
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root)
  cat("2026-01-01 00:00:02 ERROR: argument is of length zero\n",
      file = fx$log_path, append = TRUE)
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(res$out, "forbidden marker", fixed = TRUE)
  expect_match(res$out, "argument is of length zero", fixed = TRUE)
})

test_that("CR-03: 'subscript out of bounds' is a forbidden marker", {
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root)
  cat("2026-01-01 00:00:02 ERROR: subscript out of bounds\n",
      file = fx$log_path, append = TRUE)
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(res$out, "forbidden marker", fixed = TRUE)
  expect_match(res$out, "subscript out of bounds", fixed = TRUE)
})

test_that("D-22: a missing telemetry CSV fails the verifier", {
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root)
  file.remove(fx$csv_path)
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(res$out, "intervention_prob_deltas", fixed = TRUE)
})

test_that("D-22b: telemetry rows must match the AUDIT lines", {
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root)

  # (i) a missing (intervention_id, target_class) pair.
  tel <- .smoke_read_csv(fx$csv_path)
  .smoke_write_telemetry(fx$csv_path, tel[tel$target_class != 104L, , drop = FALSE])
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(res$out, "iv_abs", fixed = TRUE)

  # (ii) restored rows, but sum(n_target) now disagrees with AUDIT rows_target.
  tel <- .smoke_telemetry(fx$scenario, fx$region, fx$year)
  tel$n_target[[1]] <- 2L
  .smoke_write_telemetry(fx$csv_path, tel)
  res2 <- .run_verifier(fx)
  .skip_if_config_unavailable(res2)
  expect_identical(res2$status, 1L)
  expect_match(res2$out, "rows_target", fixed = TRUE)
})

test_that("D-22c: Absolute-to-0 rows must report mean_after = 0", {
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root)
  tel <- .smoke_telemetry(fx$scenario, fx$region, fx$year)
  tel$mean_after[[1]] <- 0.3
  .smoke_write_telemetry(fx$csv_path, tel)
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(res$out, "mean_after", fixed = TRUE)
})

test_that("the intact fixture is a real PASS with a non-zero maps_checked", {
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root)
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 0L)
  expect_match(res$out, "PASS verify_intervention_smoke", fixed = TRUE)
  expect_match(res$out, "maps_checked=2", fixed = TRUE)
  expect_match(res$out, "n_zone=3", fixed = TRUE)
})
