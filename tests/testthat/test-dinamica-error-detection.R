# Tests for the three-pattern post-hoc Dinamica error-string detector
# introduced in Phase 1.1 Plan 01 Task 2 (OBS-02; D-107, D-108).
#
# Contract:
#   - src/dinamica_utils.r exposes the constant DINAMICA_ERROR_PATTERNS as a
#     character vector of the three strings DinamicaConsole / bin/DinamicaEGO.sh
#     prints when they hit an unrecoverable error (the binary exits 0 anyway).
#   - src/dinamica_utils.r exposes the internal helper
#     .check_dinamica_post_run(res, write_logfile, logfile_path) which:
#       * reads back the teed log file when write_logfile=TRUE
#       * falls back to paste(res$stdout, res$stderr) when write_logfile=FALSE
#       * greps for each DINAMICA_ERROR_PATTERNS entry via grepl(fixed=TRUE)
#       * raises stop() if any pattern matches OR if res$status != 0
#       * stop() message includes the matched pattern names AND the logfile
#         path (when supplied) so operators see actionable diagnostics.
#   - exec_dinamica() calls the helper exactly once after processx returns.
#
# These assertions are RED before the Plan 01.1-01 Task 2 implementation
# lands and GREEN after it lands.

library(testthat)

.repo_root <- (function() {
  here <- tryCatch(normalizePath(sys.frame(1)$ofile %||% "."), error = function(e) ".")
  if (is.null(here) || identical(here, "")) here <- "."
  is_dir <- tryCatch(file.info(here)$isdir, error = function(e) NA)
  if (isTRUE(is_dir)) here <- file.path(here, "x")
  normalizePath(file.path(dirname(dirname(dirname(here)))), mustWork = FALSE)
})()

.env <- new.env(parent = baseenv())
sys.source(file.path(.repo_root, "src", "setup.r"), envir = .env)
sys.source(file.path(.repo_root, "src", "utils.r"), envir = .env)
sys.source(file.path(.repo_root, "src", "dinamica_utils.r"), envir = .env)

test_that("DINAMICA_ERROR_PATTERNS contains the three D-107 strings", {
  skip_if_not(exists("DINAMICA_ERROR_PATTERNS", envir = .env, inherits = FALSE))
  expect_true(
    setequal(
      .env$DINAMICA_ERROR_PATTERNS,
      c(
        "Dinamica EGO exited with an error",
        "terminate called after throwing",
        "std::exception"
      )
    )
  )
})

test_that(".check_dinamica_post_run fires when log contains 'std::exception' and exit=0 (D-107)", {
  skip_if_not(is.function(.env$.check_dinamica_post_run))

  tmp <- withr::local_tempdir()
  logfile <- file.path(tmp, "dinamica.log")
  writeLines(
    c(
      "Loading model...",
      "terminate called after throwing an instance of 'std::exception'",
      "  what():  ...some Dinamica error..."
    ),
    logfile
  )
  res <- list(status = 0L, stdout = "", stderr = "")

  expect_error(
    .env$.check_dinamica_post_run(
      res            = res,
      write_logfile  = TRUE,
      logfile_path   = logfile
    ),
    regexp = "std::exception"
  )
})

test_that(".check_dinamica_post_run fires when log contains 'Dinamica EGO exited with an error' and exit=0 (D-107)", {
  skip_if_not(is.function(.env$.check_dinamica_post_run))

  tmp <- withr::local_tempdir()
  logfile <- file.path(tmp, "dinamica.log")
  writeLines(
    c(
      "Running model 1 of 1...",
      "Dinamica EGO exited with an error: parse failure on line 12.",
      "(end of log)"
    ),
    logfile
  )
  res <- list(status = 0L, stdout = "", stderr = "")

  expect_error(
    .env$.check_dinamica_post_run(
      res            = res,
      write_logfile  = TRUE,
      logfile_path   = logfile
    ),
    regexp = "Dinamica EGO exited with an error"
  )
})

test_that(".check_dinamica_post_run fires when log contains 'terminate called after throwing' and exit=0 (D-107)", {
  skip_if_not(is.function(.env$.check_dinamica_post_run))

  tmp <- withr::local_tempdir()
  logfile <- file.path(tmp, "dinamica.log")
  writeLines(
    c(
      "Reading inputs...",
      "terminate called after throwing an instance of 'DFF::Exception'",
      "Aborted"
    ),
    logfile
  )
  res <- list(status = 0L, stdout = "", stderr = "")

  expect_error(
    .env$.check_dinamica_post_run(
      res            = res,
      write_logfile  = TRUE,
      logfile_path   = logfile
    ),
    regexp = "terminate called after throwing"
  )
})

test_that(".check_dinamica_post_run does NOT fire when log is clean and exit=0", {
  skip_if_not(is.function(.env$.check_dinamica_post_run))

  tmp <- withr::local_tempdir()
  logfile <- file.path(tmp, "dinamica.log")
  writeLines(
    c(
      "Loading model...",
      "Running model 1 of 1...",
      "Dinamica EGO 8.7.0 model completed successfully",
      "Wrote posterior.tif"
    ),
    logfile
  )
  res <- list(status = 0L, stdout = "", stderr = "")

  expect_silent(
    .env$.check_dinamica_post_run(
      res            = res,
      write_logfile  = TRUE,
      logfile_path   = logfile
    )
  )
})

test_that(".check_dinamica_post_run falls back to res$stdout+res$stderr when write_logfile=FALSE (D-108)", {
  skip_if_not(is.function(.env$.check_dinamica_post_run))

  res <- list(
    status = 0L,
    stdout = "Loading model...\nterminate called after throwing an instance of 'std::exception'\n",
    stderr = ""
  )

  expect_error(
    .env$.check_dinamica_post_run(
      res            = res,
      write_logfile  = FALSE,
      logfile_path   = NULL
    ),
    regexp = "std::exception"
  )
})

test_that(".check_dinamica_post_run stop message includes the matched pattern name AND the logfile path", {
  skip_if_not(is.function(.env$.check_dinamica_post_run))

  tmp <- withr::local_tempdir()
  logfile <- file.path(tmp, "dinamica.log")
  writeLines(
    c("terminate called after throwing an instance of 'std::exception'"),
    logfile
  )
  res <- list(status = 0L, stdout = "", stderr = "")

  err <- tryCatch(
    .env$.check_dinamica_post_run(
      res            = res,
      write_logfile  = TRUE,
      logfile_path   = logfile
    ),
    error = function(e) e
  )
  expect_s3_class(err, "error")
  # The matched pattern should appear in the message (at least one of the
  # three; here we expect both std::exception and terminate-called).
  expect_match(conditionMessage(err), "std::exception", fixed = TRUE)
  expect_match(conditionMessage(err), "terminate called after throwing", fixed = TRUE)
  expect_match(conditionMessage(err), basename(logfile), fixed = TRUE)
})

test_that(".check_dinamica_post_run fires on non-zero exit even if log is clean", {
  skip_if_not(is.function(.env$.check_dinamica_post_run))

  tmp <- withr::local_tempdir()
  logfile <- file.path(tmp, "dinamica.log")
  writeLines(c("Loading model...", "Worker SIGKILLed by SLURM"), logfile)
  res <- list(status = 137L, stdout = "", stderr = "")

  expect_error(
    .env$.check_dinamica_post_run(
      res            = res,
      write_logfile  = TRUE,
      logfile_path   = logfile
    ),
    regexp = "exit 137"
  )
})
