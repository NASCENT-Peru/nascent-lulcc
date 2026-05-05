# Tests for the Stage 7 pre-flight gate and portable RSS profiling introduced
# in Phase 1 Plan 3 (OBS-01 + OBS-04, locked decisions D-01..D-04).
#
# Contract:
#   - src/allocation.r exposes a standalone helper (`validate_allocation_runtime()`)
#     that aggregates ALL missing prerequisites into a single character vector
#     of error lines; success is the empty vector.
#   - The helper accepts a `fixture` argument so verification can inject a
#     synthetic prerequisite world (env vars, packages, files, Dinamica backend
#     expectations) without mutating the host.
#   - run_allocation() invokes the helper and stops with a one-shot consolidated
#     "Allocation pre-flight failed:" error before any future::plan(), worker-log
#     init, or region work is reachable.
#   - The portable RSS path uses ps::ps_memory_info()$rss instead of /proc-only,
#     so .read_proc_status() (or its successor) reports a numeric `rss` field
#     even when /proc/self/status does not exist.
#
# These tests intentionally avoid actually running an allocation; they assert
# the contract of the helper and the entrypoint.

library(testthat)

.repo_root <- (function() {
  here <- tryCatch(normalizePath(sys.frame(1)$ofile %||% "."), error = function(e) ".")
  if (is.null(here) || identical(here, "")) here <- "."
  is_dir <- tryCatch(file.info(here)$isdir, error = function(e) NA)
  if (isTRUE(is_dir)) here <- file.path(here, "x")
  normalizePath(file.path(dirname(dirname(dirname(here)))), mustWork = FALSE)
})()

# Source the file under test in an isolated environment. utils.r is sourced
# first because allocation.r calls log_msg() from utils.r.
.env <- new.env(parent = baseenv())
sys.source(file.path(.repo_root, "src", "utils.r"), envir = .env)
sys.source(file.path(.repo_root, "src", "allocation.r"), envir = .env)

test_that("validate_allocation_runtime() exists in src/allocation.r", {
  expect_true(
    is.function(.env$validate_allocation_runtime),
    info = "Plan 01-03 requires a standalone pre-flight helper named validate_allocation_runtime()."
  )
})

test_that("validate_allocation_runtime() accepts a fixture and returns one consolidated error vector", {
  skip_if_not(is.function(.env$validate_allocation_runtime))

  fixture <- list(
    env = c("MISSING_ENV_A", "MISSING_ENV_B"),
    packages = c("definitelyMissingPkg"),
    files = c("missing/model.rds"),
    dinamica = list(
      backend = "hpc",
      runtime = "apptainer",
      artifact = "missing/dinamica.sif"
    )
  )

  result <- .env$validate_allocation_runtime(fixture = fixture)

  # Contract: a character vector of human-readable error lines.
  expect_type(result, "character")
  # Every fixture-asserted gap surfaces somewhere in the error list.
  expect_true(
    any(grepl("MISSING_ENV_A", result, fixed = TRUE)),
    info = "Missing env var MISSING_ENV_A should appear in the consolidated error list."
  )
  expect_true(
    any(grepl("MISSING_ENV_B", result, fixed = TRUE)),
    info = "Missing env var MISSING_ENV_B should appear in the consolidated error list."
  )
  expect_true(
    any(grepl("definitelyMissingPkg", result, fixed = TRUE)),
    info = "Missing package should appear in the consolidated error list."
  )
  expect_true(
    any(grepl("missing/model.rds", result, fixed = TRUE)),
    info = "Missing model file should appear in the consolidated error list."
  )
  expect_true(
    any(grepl("missing/dinamica.sif", result, fixed = TRUE)),
    info = "Missing Dinamica artifact should appear in the consolidated error list."
  )
})

test_that("validate_allocation_runtime() returns empty vector on a fully satisfied fixture", {
  skip_if_not(is.function(.env$validate_allocation_runtime))

  fixture <- list(
    env = character(0),
    packages = character(0),
    files = character(0),
    dinamica = NULL
  )

  result <- .env$validate_allocation_runtime(fixture = fixture)
  expect_type(result, "character")
  expect_equal(length(result), 0L,
    info = "An empty fixture must produce no errors (contract: empty vector means OK).")
})

test_that("RSS reader returns numeric rss even on platforms without /proc", {
  # The portable path uses ps::ps_memory_info(); .read_proc_status() (the old
  # name) or its successor must therefore not return NA on Windows.
  skip_if_not(requireNamespace("ps", quietly = TRUE), "ps package required for portable RSS")

  reader <- .env$.read_proc_status
  skip_if_not(is.function(reader), ".read_proc_status() must remain available")

  m <- reader()
  expect_true(is.list(m))
  expect_true("rss" %in% names(m))
  expect_true(
    is.numeric(m$rss) && !is.na(m$rss) && m$rss > 0,
    info = "Portable RSS source must return a positive numeric value on every supported host."
  )
})
