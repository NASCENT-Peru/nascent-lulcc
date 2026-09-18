library(testthat)

# Phase 3.3 Plan 02 structural regression checks.
#
# This file locks ALLOC-06 (single-source filtered writer with a gap-free
# %03d sequence) and ALLOC-08 (missing alloc_params rows fail with stop()).
# It intentionally uses text-pattern assertions, following
# test-allocation-runtime-contract.R, so it never runs allocation itself.

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
run_script_text <- paste(
  readLines(file.path(.repo_root, "scripts", "run_allocation.r"), warn = FALSE),
  collapse = "\n"
)

test_that("setup_allocation_inputs filters persistence and zero-rate rows from the work_dir trans_rates copy (D-03)", {
  expect_match(allocation_text, "From\\*.*!=.*To\\*")
  expect_match(allocation_text, "Rate.*!=.*0")
})

test_that("setup_allocation_inputs no longer file.copies the Stage 4 source CSV into the work_dir (D-02)", {
  expect_no_match(allocation_text, "file\\.copy\\(trans_rate_src")
  expect_match(allocation_text, "write\\.csv\\(.*trans_rates")
})

test_that("setup_allocation_inputs sorts the filtered df by id_trans ascending before writing", {
  expect_match(allocation_text, "order(trans_rates_df[[\"id_trans\"]])", fixed = TRUE)
})

test_that("generate_probability_maps no longer skips persistence or zero-rate rows in the TIF writer loop (D-02 mechanical consequence)", {
  expect_no_match(allocation_text, "if \\(from_val == to_val \\|\\| rate == 0\\)")
  expect_match(allocation_text, "for (k in seq_len(nrow(trans_rates_dt)))", fixed = TRUE)
})

test_that("generate_probability_maps writes an all-NA TIF for active rows with no predictions, keeping the %03d sequence gap-free (Pitfall 2)", {
  expect_match(
    allocation_text,
    "WARN id_trans=%d row=%d has no predictions; wrote empty TIF",
    fixed = TRUE
  )
  expect_match(allocation_text, "terra::setValues(anterior, NA_real_)", fixed = TRUE)
})

test_that("missing alloc_params id_trans triggers stop() with the full missing list, not a warning (D-04)", {
  expect_match(allocation_text, "stop(log_msg(", fixed = TRUE)
  expect_no_match(allocation_text, "warning\\(log_msg\\(.*missing from alloc_params")
  expect_match(allocation_text, "missing_alloc_params", fixed = TRUE)
  expect_match(allocation_text, "match(trans_rates_df", fixed = TRUE)
  expect_match(allocation_text, "stopifnot(identical(alloc_params", fixed = TRUE)
})

test_that("run_allocation_one_timestep calls write_saturation_summary() after Dinamica returns (D-05 inline surface)", {
  expect_match(allocation_text, "write_saturation_summary(", fixed = TRUE)
  expect_match(allocation_text, "t_saturation <- prof_tic()", fixed = TRUE)
  expect_match(allocation_text, "stage=saturation_summary", fixed = TRUE)
  expect_match(allocation_text, "AUDIT stage=allocation_filter", fixed = TRUE)
})

test_that("run_allocation.r src_files vector sources src/saturation_diagnostics.r between utils.r and allocation.r", {
  expect_match(run_script_text, '"src/saturation_diagnostics.r"', fixed = TRUE)
  utils_pos <- regexpr('"src/utils.r"', run_script_text, fixed = TRUE)[[1L]]
  sat_pos <- regexpr('"src/saturation_diagnostics.r"', run_script_text, fixed = TRUE)[[1L]]
  alloc_pos <- regexpr('"src/allocation.r"', run_script_text, fixed = TRUE)[[1L]]

  expect_gt(utils_pos, 0L)
  expect_gt(sat_pos, 0L)
  expect_gt(alloc_pos, 0L)
  expect_gt(sat_pos, utils_pos)
  expect_lt(sat_pos, alloc_pos)
})
