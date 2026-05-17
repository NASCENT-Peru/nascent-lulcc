# tests/testthat/test-dinamica-launch-contract-mirror.R
#
# Phase 1.1 -- RESEARCH §Target 7 mirror assertion.
#
# Proves that src/dinamica_utils.r:resolve_dinamica_launch() (R-side launch
# contract) and scripts/smoke_test_dinamica.sh (shell-side LAUNCH_CMD)
# produce the same launch shape for the same inputs. Drift between the two
# is the Pitfall 1 failure mode this test catches.
#
# Locked decisions covered: D-104 (launch shape), D-105 (staged-home/tmp),
# D-106 (absolute model path).
#
# Skip behaviour:
#   - Skips cleanly if resolve_dinamica_launch() is not loadable.
#   - Skips cleanly if `bash` is not on PATH (Windows hosts without
#     Git-Bash / WSL).

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

.smoke_script <- file.path(.repo_root, "scripts", "smoke_test_dinamica.sh")
.have_bash <- nzchar(Sys.which("bash"))

.resolve_pair <- function() {
  # Returns a list(launch_args, resolved_line, shell_out) -- both sides
  # resolved against the same fixed inputs.
  scratch <- withr::local_tempdir()

  withr::local_envvar(
    DINAMICA_EGO_8_HOME = "/tmp/dinamica.sif",
    DINAMICA_BACKEND    = "hpc",
    HPC_SCRATCH_ROOT    = scratch
  )

  launch <- .env$resolve_dinamica_launch(
    model_path       = "/abs/test.ego",
    backend          = "hpc",
    runtime_override = "apptainer",
    probe_runtime    = FALSE
  )

  shell_out <- suppressWarnings(system2(
    "bash",
    c(.smoke_script,
      "--dry-run",
      "--runtime", "apptainer",
      "--artifact", "/tmp/dinamica.sif",
      "--ego", "/abs/test.ego"),
    stdout = TRUE, stderr = TRUE,
    env    = c(paste0("HPC_SCRATCH_ROOT=", scratch))
  ))

  resolved_line <- grep("^resolved command", shell_out, value = TRUE)
  if (length(resolved_line) == 0L) resolved_line <- ""

  list(launch_args = launch$args, resolved_line = resolved_line, shell_out = shell_out)
}

test_that("R-side and shell-side both include --home in launch args", {
  skip_if_not(is.function(.env$resolve_dinamica_launch))
  skip_if_not(.have_bash, "bash not on PATH; skipping mirror assertion")

  p <- .resolve_pair()
  expect_true(any(grepl("--home", p$launch_args, fixed = TRUE)),
              info = "R-side resolve_dinamica_launch() args must contain --home")
  expect_match(p$resolved_line, "--home", fixed = TRUE,
               info = "Shell-side smoke test resolved command must contain --home")
})

test_that("R-side and shell-side both include DINAMICA_EGO_8_TEMP_DIR=", {
  skip_if_not(is.function(.env$resolve_dinamica_launch))
  skip_if_not(.have_bash, "bash not on PATH; skipping mirror assertion")

  p <- .resolve_pair()
  expect_true(any(grepl("DINAMICA_EGO_8_TEMP_DIR=", p$launch_args, fixed = TRUE)),
              info = "R-side args must contain DINAMICA_EGO_8_TEMP_DIR=<staged-tmp>")
  expect_match(p$resolved_line, "DINAMICA_EGO_8_TEMP_DIR=", fixed = TRUE,
               info = "Shell-side resolved command must contain DINAMICA_EGO_8_TEMP_DIR=")
})

test_that("R-side and shell-side both include bin/DinamicaEGO.sh and bash", {
  skip_if_not(is.function(.env$resolve_dinamica_launch))
  skip_if_not(.have_bash, "bash not on PATH; skipping mirror assertion")

  p <- .resolve_pair()
  expect_true(any(grepl("bin/DinamicaEGO.sh", p$launch_args, fixed = TRUE)),
              info = "R-side args must include bin/DinamicaEGO.sh in the bash -c payload")
  expect_match(p$resolved_line, "bin/DinamicaEGO.sh", fixed = TRUE)
  expect_match(p$resolved_line, "bash", fixed = TRUE)
})

test_that("R-side and shell-side both reference the same .sif path", {
  skip_if_not(is.function(.env$resolve_dinamica_launch))
  skip_if_not(.have_bash, "bash not on PATH; skipping mirror assertion")

  p <- .resolve_pair()
  expect_true(any(grepl("/tmp/dinamica.sif", p$launch_args, fixed = TRUE)),
              info = "R-side args must include the .sif path /tmp/dinamica.sif")
  expect_match(p$resolved_line, "/tmp/dinamica.sif", fixed = TRUE,
               info = "Shell-side resolved command must include the .sif path")
})

test_that("R-side args include the absolute model path /abs/test.ego (D-106)", {
  skip_if_not(is.function(.env$resolve_dinamica_launch))
  skip_if_not(.have_bash, "bash not on PATH; skipping mirror assertion")

  p <- .resolve_pair()
  # The absolute path appears inside the bash -c payload (the last arg).
  payload <- p$launch_args[length(p$launch_args)]
  expect_match(payload, "/abs/test.ego", fixed = TRUE,
               info = "Bash -c payload (last arg) must contain the absolute model path")
})
