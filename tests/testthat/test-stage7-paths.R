# Tests for the shared Stage 7 path/env contract introduced in Phase 1 Plan 1
# (PIPE-01, PIPE-03 + canonical_refs D-12, D-13, D-14, D-15).
#
# Contract: src/setup.r exposes one resolver `get_stage7_runtime_paths()` that
# returns a named list of all machine-specific runtime paths required by
# Stage 7 (allocation + prep), each driven by an explicit env-var override
# with safe, documented defaults.

library(testthat)

# Resolve repo root from this test file's location so tests work both when run
# directly via Rscript and via testthat::test_dir().
.repo_root <- (function() {
  here <- tryCatch(normalizePath(sys.frame(1)$ofile %||% "."), error = function(e) ".")
  if (file.info(here)$isdir %||% FALSE) here <- file.path(here, "x")
  # tests/testthat/<file>.R -> tests/testthat -> tests -> repo
  normalizePath(file.path(dirname(dirname(dirname(here)))), mustWork = FALSE)
})()

# Source the file under test in an isolated environment so we don't pollute the
# global search path during testing.
.env <- new.env(parent = baseenv())
sys.source(file.path(.repo_root, "src", "setup.r"), envir = .env)

test_that("get_stage7_runtime_paths() exists in src/setup.r", {
  expect_true(
    is.function(.env$get_stage7_runtime_paths),
    info = "src/setup.r must define a callable resolver named get_stage7_runtime_paths()."
  )
})

test_that("get_stage7_runtime_paths() returns the documented contract keys", {
  skip_if_not(is.function(.env$get_stage7_runtime_paths))

  # Run with all overrides cleared so we exercise the documented defaults path.
  withr::local_envvar(
    TERRA_TEMP = NA,
    HPC_SCRATCH_ROOT = NA,
    HPC_TMP_ROOT = NA,
    DINAMICA_EGO_8_HOME = NA,
    DINAMICA_BACKEND = NA
  )

  paths <- .env$get_stage7_runtime_paths()

  expect_type(paths, "list")

  # All five contract keys must be present so later plans can rely on them.
  required_keys <- c(
    "terra_temp",
    "hpc_scratch_root",
    "hpc_tmp_root",
    "dinamica_ego_8_home",
    "dinamica_backend"
  )
  expect_true(
    all(required_keys %in% names(paths)),
    info = sprintf(
      "Resolver must return all contract keys; missing: %s",
      paste(setdiff(required_keys, names(paths)), collapse = ", ")
    )
  )
})

test_that("get_stage7_runtime_paths() honours TERRA_TEMP env override", {
  skip_if_not(is.function(.env$get_stage7_runtime_paths))

  fake_tmp <- tempfile("terra_temp_test_")
  withr::local_envvar(TERRA_TEMP = fake_tmp)

  paths <- .env$get_stage7_runtime_paths()
  expect_identical(paths$terra_temp, fake_tmp)
})

test_that("touched config/template files no longer hardcode user-specific paths", {
  files <- c(
    "src/setup.r",
    "config/local_config.yaml",
    "config/hpc_config.yaml",
    ".env.template"
  )
  for (rel in files) {
    txt <- readLines(file.path(.repo_root, rel), warn = FALSE)
    joined <- paste(txt, collapse = "\n")
    expect_false(
      grepl("/[^[:space:]]*/black", joined),
      info = sprintf("%s must not hardcode /.../black/...", rel)
    )
    expect_false(
      grepl("E:/terra_temp", joined, fixed = TRUE),
      info = sprintf("%s must not hardcode E:/terra_temp", rel)
    )
  }
})

test_that(".env.template documents the Stage 7 override names", {
  txt <- readLines(file.path(.repo_root, ".env.template"), warn = FALSE)
  joined <- paste(txt, collapse = "\n")
  for (key in c(
    "TERRA_TEMP",
    "HPC_SCRATCH_ROOT",
    "HPC_TMP_ROOT",
    "DINAMICA_EGO_8_HOME"
  )) {
    expect_true(
      grepl(key, joined, fixed = TRUE),
      info = sprintf(".env.template must document override %s", key)
    )
  }
})
