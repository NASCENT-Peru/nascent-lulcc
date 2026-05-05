# Tests for the structured breadcrumb/sentinel logging surface and the
# log_file-aware Dinamica helper introduced in Phase 1 Plan 3 Task 2
# (OBS-02, OBS-03, locked decisions D-05, D-06, D-07, D-08).
#
# Contract:
#   - run_allocation_dinamica() accepts an explicit `log_file` argument and
#     emits structured DINAMICA_START / DINAMICA_LOG_PATH / DINAMICA_EXIT or
#     DINAMICA_FAIL breadcrumbs into that log (OBS-03, D-05, D-07).
#   - run_allocation_dinamica() supports a `dry_run = TRUE` mode so tests
#     can assert breadcrumb emission without invoking DinamicaConsole or
#     touching disk in a heavyweight way.
#   - src/utils.r exposes a worker-state breadcrumb API:
#         worker_state_init(scenario, region, timestep)
#         worker_state_set(stage, transition = NA)
#         worker_state_flush_sentinel(log_file, reason)
#     and the sentinel record is a single line tagged "SENTINEL" carrying
#     last-known scenario/region/timestep/transition (D-06, OBS-02).

library(testthat)

.repo_root <- (function() {
  here <- tryCatch(normalizePath(sys.frame(1)$ofile %||% "."), error = function(e) ".")
  if (is.null(here) || identical(here, "")) here <- "."
  is_dir <- tryCatch(file.info(here)$isdir, error = function(e) NA)
  if (isTRUE(is_dir)) here <- file.path(here, "x")
  normalizePath(file.path(dirname(dirname(dirname(here)))), mustWork = FALSE)
})()

.env <- new.env(parent = baseenv())
sys.source(file.path(.repo_root, "src", "utils.r"), envir = .env)
sys.source(file.path(.repo_root, "src", "dinamica_utils.r"), envir = .env)

test_that("worker_state_* helpers exist and emit a SENTINEL line on flush", {
  expect_true(is.function(.env$worker_state_init),
    info = "src/utils.r must define worker_state_init().")
  expect_true(is.function(.env$worker_state_set),
    info = "src/utils.r must define worker_state_set().")
  expect_true(is.function(.env$worker_state_flush_sentinel),
    info = "src/utils.r must define worker_state_flush_sentinel().")

  tmp <- tempfile(fileext = ".log")
  on.exit(unlink(tmp), add = TRUE)
  .env$worker_state_init(scenario = "BAU", region = "Andes", timestep = 2030)
  .env$worker_state_set(stage = "predict", transition = "forest_to_crop")
  .env$worker_state_flush_sentinel(log_file = tmp, reason = "test-injected")

  lines <- readLines(tmp, warn = FALSE)
  expect_true(any(grepl("SENTINEL", lines)),
    info = "Sentinel record must be tagged SENTINEL.")
  expect_true(any(grepl("BAU", lines)),
    info = "Sentinel must contain scenario.")
  expect_true(any(grepl("Andes", lines)),
    info = "Sentinel must contain region.")
  expect_true(any(grepl("2030", lines)),
    info = "Sentinel must contain timestep.")
  expect_true(any(grepl("forest_to_crop", lines)),
    info = "Sentinel must contain last-known transition.")
})

test_that("run_allocation_dinamica() accepts log_file and emits structured DINAMICA_* breadcrumbs in dry-run mode", {
  expect_true(is.function(.env$run_allocation_dinamica),
    info = "src/dinamica_utils.r must export run_allocation_dinamica().")

  log_file <- tempfile(fileext = ".log")
  on.exit(unlink(log_file), add = TRUE)
  fake_model <- tempfile(fileext = ".ego-decoded")
  writeLines("placeholder", fake_model)
  on.exit(unlink(fake_model), add = TRUE)

  # dry_run=TRUE must run the breadcrumb path WITHOUT spawning DinamicaConsole
  # so this test can run on any host.
  result <- tryCatch(
    .env$run_allocation_dinamica(model_path = fake_model, log_file = log_file, dry_run = TRUE),
    error = function(e) e
  )

  lines <- readLines(log_file, warn = FALSE)
  expect_true(
    any(grepl("DINAMICA_(START|LOG_PATH|EXIT|FAIL)", lines)),
    info = "Dinamica helper must emit at least one structured DINAMICA_* breadcrumb."
  )
})
