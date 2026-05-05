# Tests for the unified Dinamica launcher and central log handling introduced
# in Phase 1 Plan 4 Task 1 (INFRA-01, PIPE-07; locked decisions D-07, D-09,
# D-10, D-11).
#
# Contract:
#   - src/dinamica_utils.r exposes resolve_dinamica_launch(model_path, ...) as
#     a non-executing helper that returns the selected backend, runtime
#     command, exact .sif artifact path, full argument vector, and central
#     logfile path.
#   - On HPC, DINAMICA_EGO_8_HOME is treated as the absolute path to the
#     external Dinamica .sif image. resolve_dinamica_launch() consumes that
#     path directly as the container image argument to apptainer/singularity
#     exec.
#   - The helper accepts a runtime_override (e.g. "apptainer") and a
#     probe_runtime = FALSE flag so dry-run callers can prove HPC command
#     resolution on a workstation without apptainer/singularity installed.
#   - The central logfile path lives under <project_root>/logs/ with a
#     timestamped *_dinamica.log basename, matching the PIPE-07 contract.
#   - exec_dinamica() routes through resolve_dinamica_launch() rather than
#     re-deriving its own launch contract.
#
# These assertions are RED before the Plan 01-04 Task 1 implementation lands
# and GREEN after it lands.

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

test_that("resolve_dinamica_launch() exists and is exported by src/dinamica_utils.r", {
  expect_true(
    is.function(.env$resolve_dinamica_launch),
    info = "src/dinamica_utils.r must define resolve_dinamica_launch() as the single launch contract resolver."
  )
})

test_that("resolve_dinamica_launch() resolves the HPC backend with DINAMICA_EGO_8_HOME as the .sif image", {
  skip_if_not(is.function(.env$resolve_dinamica_launch))

  model <- file.path(.repo_root, "dinamica", "dinamica_model", "allocation.ego-decoded")
  withr::local_envvar(
    DINAMICA_EGO_8_HOME = "/tmp/dinamica.sif",
    DINAMICA_BACKEND    = "hpc"
  )

  launch <- .env$resolve_dinamica_launch(
    model_path       = model,
    backend          = "hpc",
    runtime_override = "apptainer",
    probe_runtime    = FALSE
  )

  expect_type(launch, "list")
  expect_identical(launch$backend, "hpc")
  expect_identical(launch$runtime, "apptainer")

  # The .sif path is the value of DINAMICA_EGO_8_HOME and must appear verbatim
  # in the resolved argument vector.
  expect_identical(
    normalizePath(launch$artifact_path, winslash = "/", mustWork = FALSE),
    normalizePath("/tmp/dinamica.sif", winslash = "/", mustWork = FALSE)
  )
  expect_true(any(grepl("DinamicaConsole", launch$args)),
    info = "Resolved args must include DinamicaConsole as the in-container command.")
  expect_true(any(grepl("/tmp/dinamica\\.sif", launch$args)),
    info = "Resolved args must include the .sif image path.")

  # Central logfile lands under <repo>/logs/.
  expect_match(launch$log_file, "logs[/\\\\].+\\.log$",
    info = "Central log path must resolve under logs/ with a .log basename.")
})

test_that("resolve_dinamica_launch() resolves the local backend with direct DinamicaConsole launch", {
  skip_if_not(is.function(.env$resolve_dinamica_launch))

  model <- file.path(.repo_root, "dinamica", "dinamica_model", "allocation.ego-decoded")
  withr::local_envvar(
    DINAMICA_EGO_8_HOME = "/opt/dinamica/install",
    DINAMICA_BACKEND    = "local"
  )

  launch <- .env$resolve_dinamica_launch(
    model_path       = model,
    backend          = "local",
    runtime_override = NULL,
    probe_runtime    = FALSE
  )

  expect_identical(launch$backend, "local")
  expect_identical(launch$runtime, "DinamicaConsole")
  # Local backend must not return apptainer/singularity in the resolved args.
  expect_false(any(grepl("apptainer|singularity", launch$args)),
    info = "Local backend must launch DinamicaConsole directly, no container runtime.")
})

test_that("resolve_dinamica_launch() falls back to singularity when override == 'singularity'", {
  skip_if_not(is.function(.env$resolve_dinamica_launch))

  model <- file.path(.repo_root, "dinamica", "dinamica_model", "allocation.ego-decoded")
  withr::local_envvar(
    DINAMICA_EGO_8_HOME = "/tmp/dinamica.sif"
  )

  launch <- .env$resolve_dinamica_launch(
    model_path       = model,
    backend          = "hpc",
    runtime_override = "singularity",
    probe_runtime    = FALSE
  )
  expect_identical(launch$runtime, "singularity")
})

test_that("resolve_dinamica_launch() fails clearly on HPC when DINAMICA_EGO_8_HOME is unset", {
  skip_if_not(is.function(.env$resolve_dinamica_launch))

  model <- file.path(.repo_root, "dinamica", "dinamica_model", "allocation.ego-decoded")
  withr::local_envvar(
    DINAMICA_EGO_8_HOME = NA,
    DINAMICA_BACKEND    = "hpc"
  )

  expect_error(
    .env$resolve_dinamica_launch(
      model_path       = model,
      backend          = "hpc",
      runtime_override = "apptainer",
      probe_runtime    = FALSE
    ),
    regexp = "DINAMICA_EGO_8_HOME"
  )
})
