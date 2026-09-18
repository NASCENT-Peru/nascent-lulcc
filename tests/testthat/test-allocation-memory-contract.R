library(testthat)

.repo_root <- (function() {
  here <- tryCatch(normalizePath(sys.frame(1)$ofile %||% "."), error = function(e) ".")
  if (is.null(here) || identical(here, "")) here <- "."
  is_dir <- tryCatch(file.info(here)$isdir, error = function(e) NA)
  if (isTRUE(is_dir)) here <- file.path(here, "x")
  normalizePath(file.path(dirname(dirname(dirname(here)))), mustWork = FALSE)
})()

allocation_text <- paste(
  readLines(file.path(.repo_root, "src", "allocation.r"), warn = FALSE),
  collapse = "\n"
)

test_that("allocation.r defines parent preload and neighbourhood write helpers", {
  expect_match(allocation_text, "write_nhood_tif <- function\\(")
  expect_match(allocation_text, "load_allocation_models <- function\\(")
})

test_that("workers reopen neighbourhood rasters from TIF paths", {
  expect_match(allocation_text, "terra::rast\\(nhood_paths")
  expect_false(grepl("nhood_raster_cache <- new\\.env", allocation_text))
})

test_that("parent-stage preload and baseline markers exist", {
  expect_match(allocation_text, "stage=preload_models", fixed = TRUE)
  expect_match(allocation_text, "stage=nhood_precompute", fixed = TRUE)
  expect_match(allocation_text, "stage=parent_baseline", fixed = TRUE)
  expect_match(allocation_text, "ALLOCATION_WORKER_RSS_BUDGET_MB", fixed = TRUE)
})

test_that("parent baseline marker appears before future_map", {
  baseline_pos <- regexpr("stage=parent_baseline", allocation_text, fixed = TRUE)[[1]]
  future_map_pos <- regexpr("furrr::future_map(", allocation_text, fixed = TRUE)[[1]]
  expect_gt(baseline_pos, 0L)
  expect_gt(future_map_pos, 0L)
  expect_lt(baseline_pos, future_map_pos)
})

test_that("preloaded models are threaded through the worker path", {
  expect_match(allocation_text, "models_list = region_input\\$models_list")
  expect_match(allocation_text, "fitted_wf <- mi\\$model_obj\\[\\[1L\\]\\]")
})
