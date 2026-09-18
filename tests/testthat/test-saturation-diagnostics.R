library(testthat)
library(terra)

# Phase 3.3 Plan 01 saturation diagnostic helper tests.
#
# These tests build small inline terra::rast(matrix(...)) fixtures rather than
# committing binary rasters. They cover ALLOC-07 placed-vs-demanded arithmetic
# and ALLOC-10 probability-map quality metrics, exempt matching, and AUDIT
# grammar.

.repo_root <- (function() {
  here <- tryCatch(normalizePath(sys.frame(1)$ofile %||% "."), error = function(e) ".")
  if (is.null(here) || identical(here, "")) here <- "."
  is_dir <- tryCatch(file.info(here)$isdir, error = function(e) NA)
  if (isTRUE(is_dir)) here <- file.path(here, "x")
  normalizePath(file.path(dirname(dirname(dirname(here)))), mustWork = FALSE)
})()

.env <- new.env(parent = baseenv())
sys.source(file.path(.repo_root, "src", "utils.r"), envir = .env)
sys.source(file.path(.repo_root, "src", "saturation_diagnostics.r"), envir = .env)

.build_synthetic_rasters <- function(
  anterior_matrix,
  posterior_matrix,
  prob_matrix,
  prob_filename = "001_id_trans_1.tif",
  scratch_dir
) {
  anterior <- terra::rast(matrix(as.vector(anterior_matrix), nrow = nrow(anterior_matrix), ncol = ncol(anterior_matrix)))
  posterior <- terra::rast(matrix(as.vector(posterior_matrix), nrow = nrow(posterior_matrix), ncol = ncol(posterior_matrix)))
  prob <- terra::rast(matrix(as.vector(prob_matrix), nrow = nrow(prob_matrix), ncol = ncol(prob_matrix)))

  anterior_path <- file.path(scratch_dir, "anterior.tif")
  posterior_path <- file.path(scratch_dir, "posterior.tif")
  prob_path <- file.path(scratch_dir, prob_filename)
  terra::writeRaster(anterior, anterior_path, overwrite = TRUE, NAflag = -999)
  terra::writeRaster(posterior, posterior_path, overwrite = TRUE, NAflag = -999)
  terra::writeRaster(prob, prob_path, overwrite = TRUE, NAflag = -999)

  list(
    anterior_path = anterior_path,
    posterior_path = posterior_path,
    prob_map_dir = scratch_dir
  )
}

.filtered_df <- function(from_val = 101L, to_val = 102L, rate = 0.5, id_trans = 1L) {
  data.frame(
    "From*" = from_val,
    "To*" = to_val,
    Rate = rate,
    id_trans = id_trans,
    check.names = FALSE
  )
}

test_that("compute_per_transition_metrics returns the documented 16-column schema", {
  scratch <- withr::local_tempdir()
  fixtures <- .build_synthetic_rasters(
    anterior_matrix = matrix(101L, nrow = 10, ncol = 10),
    posterior_matrix = matrix(101L, nrow = 10, ncol = 10),
    prob_matrix = matrix(0.5, nrow = 10, ncol = 10),
    scratch_dir = scratch
  )

  result <- .env$compute_per_transition_metrics(
    filtered_df = .filtered_df(rate = 0.1),
    prob_map_dir = fixtures$prob_map_dir,
    anterior_path = fixtures$anterior_path,
    posterior_path = fixtures$posterior_path,
    threshold = 0.9,
    exempt = list()
  )

  expect_s3_class(result, "data.frame")
  expect_identical(
    names(result),
    c(
      "id_trans", "from_val", "to_val", "rate",
      "demanded_cells", "placed_cells", "placed_frac",
      "coverage", "p50", "p90", "p95", "p99", "pmax",
      "demand_vs_capacity", "exempt", "floor_met"
    )
  )
})

test_that("compute_per_transition_metrics computes placed demand arithmetic on a hand-checkable 10x10 fixture", {
  scratch <- withr::local_tempdir()
  anterior <- cbind(matrix(101L, nrow = 10, ncol = 5), matrix(102L, nrow = 10, ncol = 5))
  posterior <- anterior
  posterior[1:4, 1:5] <- 102L
  prob <- cbind(matrix(0.6, nrow = 10, ncol = 5), matrix(0, nrow = 10, ncol = 5))
  fixtures <- .build_synthetic_rasters(anterior, posterior, prob, scratch_dir = scratch)

  result <- .env$compute_per_transition_metrics(
    filtered_df = .filtered_df(rate = 0.5),
    prob_map_dir = fixtures$prob_map_dir,
    anterior_path = fixtures$anterior_path,
    posterior_path = fixtures$posterior_path,
    threshold = 0.9,
    exempt = list()
  )

  expect_equal(result$demanded_cells, 25L)
  expect_equal(result$placed_cells, 20L)
  expect_equal(result$placed_frac, 20 / 25)
  expect_false(result$floor_met)
  expect_false(result$exempt)
})

test_that("compute_per_transition_metrics excludes NA from coverage numerator and denominator", {
  scratch <- withr::local_tempdir()
  fixtures <- .build_synthetic_rasters(
    anterior_matrix = matrix(101L, nrow = 2, ncol = 2),
    posterior_matrix = matrix(101L, nrow = 2, ncol = 2),
    prob_matrix = matrix(c(0, 0, 0.5, NA_real_), nrow = 2, ncol = 2),
    scratch_dir = scratch
  )

  result <- .env$compute_per_transition_metrics(
    filtered_df = .filtered_df(rate = 1.0),
    prob_map_dir = fixtures$prob_map_dir,
    anterior_path = fixtures$anterior_path,
    posterior_path = fixtures$posterior_path,
    threshold = 0.9,
    exempt = list()
  )

  expect_equal(result$coverage, 1 / 3)
})

test_that("compute_per_transition_metrics reports quantiles using stats::quantile defaults", {
  scratch <- withr::local_tempdir()
  probs <- c(0.1, 0.5, 0.9, 0.95, 0.99)
  fixtures <- .build_synthetic_rasters(
    anterior_matrix = matrix(101L, nrow = 1, ncol = 5),
    posterior_matrix = matrix(101L, nrow = 1, ncol = 5),
    prob_matrix = matrix(probs, nrow = 1, ncol = 5),
    scratch_dir = scratch
  )
  expected <- stats::quantile(probs, c(0.5, 0.9, 0.95, 0.99), na.rm = TRUE)

  result <- .env$compute_per_transition_metrics(
    filtered_df = .filtered_df(rate = 0.0),
    prob_map_dir = fixtures$prob_map_dir,
    anterior_path = fixtures$anterior_path,
    posterior_path = fixtures$posterior_path,
    threshold = 0.9,
    exempt = list()
  )

  expect_equal(result$p50, unname(expected[[1L]]), tolerance = 1e-6)
  expect_equal(result$p90, unname(expected[[2L]]), tolerance = 1e-6)
  expect_equal(result$p95, unname(expected[[3L]]), tolerance = 1e-6)
  expect_equal(result$p99, unname(expected[[4L]]), tolerance = 1e-6)
  expect_equal(result$pmax, 0.99, tolerance = 1e-6)
})

test_that("compute_per_transition_metrics marks matching saturation_exempt entries", {
  scratch <- withr::local_tempdir()
  fixtures <- .build_synthetic_rasters(
    anterior_matrix = matrix(101L, nrow = 10, ncol = 10),
    posterior_matrix = matrix(101L, nrow = 10, ncol = 10),
    prob_matrix = matrix(0.5, nrow = 10, ncol = 10),
    scratch_dir = scratch
  )

  result <- .env$compute_per_transition_metrics(
    filtered_df = .filtered_df(rate = 0.5),
    prob_map_dir = fixtures$prob_map_dir,
    anterior_path = fixtures$anterior_path,
    posterior_path = fixtures$posterior_path,
    threshold = 0.9,
    exempt = list(list(from_lulc = 101L, to_lulc = 102L, reason = "test_exempt"))
  )

  expect_true(result$exempt)
  expect_false(result$floor_met)
})

test_that("compute_per_transition_metrics leaves non-matching saturation_exempt entries false", {
  scratch <- withr::local_tempdir()
  fixtures <- .build_synthetic_rasters(
    anterior_matrix = matrix(101L, nrow = 10, ncol = 10),
    posterior_matrix = matrix(101L, nrow = 10, ncol = 10),
    prob_matrix = matrix(0.5, nrow = 10, ncol = 10),
    scratch_dir = scratch
  )

  result <- .env$compute_per_transition_metrics(
    filtered_df = .filtered_df(rate = 0.5),
    prob_map_dir = fixtures$prob_map_dir,
    anterior_path = fixtures$anterior_path,
    posterior_path = fixtures$posterior_path,
    threshold = 0.9,
    exempt = list(
      list(from_lulc = 103L, to_lulc = 104L, reason = "other"),
      list(from_lulc = 105L, to_lulc = 106L, reason = "another")
    )
  )

  expect_false(result$exempt)
})

test_that("log_audit_saturation_line emits the Phase 3.3 AUDIT grammar", {
  summary <- data.frame(
    id_trans = 1L,
    from_val = 101L,
    to_val = 102L,
    rate = 0.5,
    demanded_cells = 25L,
    placed_cells = 20L,
    placed_frac = 0.8,
    coverage = 0.5,
    p50 = 0.5,
    p90 = 0.9,
    p95 = 0.95,
    p99 = 0.99,
    pmax = 0.99,
    demand_vs_capacity = 1.0,
    exempt = FALSE,
    floor_met = FALSE,
    stringsAsFactors = FALSE
  )

  captured <- capture.output(
    .env$log_audit_saturation_line(
      summary = summary,
      scenario = "BAU",
      region_label = "test_region",
      year_ant = 2022L,
      threshold = 0.90,
      log_file = NULL
    )
  )

  expect_match(
    captured,
    "AUDIT stage=saturation scenario=BAU region=test_region year_ant=2022 active=1 met=0 exempt=0 failed=1 threshold=0\\.90"
  )
})
