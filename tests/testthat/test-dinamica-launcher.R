# Tests for the unified Dinamica launcher and central log handling introduced
# in Phase 1 Plan 4 Task 1 (INFRA-01, PIPE-07; locked decisions D-07, D-09,
# D-10, D-11) and re-shaped in Phase 1.1 Plan 01 Task 1 (INFRA-01 / OBS-02;
# locked decisions D-104, D-105, D-106).
#
# Contract:
#   - src/dinamica_utils.r exposes resolve_dinamica_launch(model_path, ...) as
#     a non-executing helper that returns the selected backend, runtime
#     command, exact .sif artifact path, full argument vector, and central
#     logfile path.
#   - On HPC, DINAMICA_EGO_8_HOME is treated as the absolute path to the
#     external Dinamica .sif image. resolve_dinamica_launch() builds the
#     apptainer launch shape:
#         apptainer exec --home <staged-home> \
#                         --env DINAMICA_EGO_8_TEMP_DIR=<staged-tmp> \
#                         <sif> bash -c \
#                         'cd /opt/dinamica/usr && bin/DinamicaEGO.sh <abs-model> [flags]'
#     (D-104). Direct `apptainer exec <sif> DinamicaConsole <model>` is no
#     longer used — that shape produced silent std::exception failures on
#     the upstream image.
#   - The function seeds <HPC_SCRATCH_ROOT>/dinamica-home and
#     <HPC_SCRATCH_ROOT>/dinamica-tmp idempotently, including a minimal
#     .dinamica_ego_8.conf (D-105).
#   - The model path interpolated into the bash -c payload is always
#     absolute (D-106).
#   - The helper accepts a runtime_override (e.g. "apptainer") and a
#     probe_runtime = FALSE flag so dry-run callers can prove HPC command
#     resolution on a workstation without apptainer/singularity installed.
#   - The central logfile path lives under <project_root>/logs/ with a
#     timestamped *_dinamica.log basename, matching the PIPE-07 contract.
#   - exec_dinamica() routes through resolve_dinamica_launch() rather than
#     re-deriving its own launch contract.
#
# These assertions are RED before the Plan 01.1-01 Task 1 implementation
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

test_that("resolve_dinamica_launch() exists and is exported by src/dinamica_utils.r", {
  expect_true(
    is.function(.env$resolve_dinamica_launch),
    info = "src/dinamica_utils.r must define resolve_dinamica_launch() as the single launch contract resolver."
  )
})

test_that("resolve_dinamica_launch() HPC backend uses --home + --env + bash -c + bin/DinamicaEGO.sh (D-104)", {
  skip_if_not(is.function(.env$resolve_dinamica_launch))

  model <- file.path(.repo_root, "dinamica", "dinamica_model", "allocation.ego-decoded")
  staged_root <- tempfile("hpc_scratch_")
  dir.create(staged_root, recursive = TRUE, showWarnings = FALSE)
  withr::local_envvar(
    DINAMICA_EGO_8_HOME = "/tmp/dinamica.sif",
    DINAMICA_BACKEND    = "hpc",
    HPC_SCRATCH_ROOT    = staged_root
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

  # D-104 mandate — the new apptainer launch shape.
  expect_true(any(grepl("^--home$", launch$args)),
    info = "Resolved args must contain --home (D-104).")
  expect_true(any(grepl("^--env$", launch$args)),
    info = "Resolved args must contain --env (D-104).")
  expect_true(any(grepl("^DINAMICA_EGO_8_TEMP_DIR=", launch$args)),
    info = "Resolved args must set DINAMICA_EGO_8_TEMP_DIR via --env (D-105).")
  expect_true(any(launch$args == "bash"),
    info = "Resolved args must invoke bash (D-104).")
  expect_true(any(launch$args == "-c"),
    info = "Resolved args must invoke bash with -c payload (D-104).")
  expect_true(any(grepl("cd /opt/dinamica/usr && bin/DinamicaEGO.sh", launch$args, fixed = TRUE)),
    info = "bash -c payload must cd into /opt/dinamica/usr and invoke bin/DinamicaEGO.sh (D-104).")
  expect_true(any(grepl("/tmp/dinamica.sif", launch$args, fixed = TRUE)),
    info = "Resolved args must include the .sif image path.")

  # Direct DinamicaConsole invocation is forbidden in the HPC launch path (D-104).
  expect_false(any(launch$args == "DinamicaConsole"),
    info = "HPC launch must NOT invoke DinamicaConsole directly (D-104).")

  # Central logfile lands under <repo>/logs/.
  expect_match(launch$log_file, "logs[/\\\\].+\\.log$",
    info = "Central log path must resolve under logs/ with a .log basename.")
})

test_that("resolve_dinamica_launch() HPC uses absolute model path even when given a relative one (D-106)", {
  skip_if_not(is.function(.env$resolve_dinamica_launch))

  staged_root <- tempfile("hpc_scratch_")
  dir.create(staged_root, recursive = TRUE, showWarnings = FALSE)
  withr::local_envvar(
    DINAMICA_EGO_8_HOME = "/tmp/dinamica.sif",
    DINAMICA_BACKEND    = "hpc",
    HPC_SCRATCH_ROOT    = staged_root
  )

  launch <- .env$resolve_dinamica_launch(
    model_path       = "model.ego",  # relative path
    backend          = "hpc",
    runtime_override = "apptainer",
    probe_runtime    = FALSE
  )

  # The bash -c payload is the last arg; it must contain an absolute path.
  payload <- launch$args[length(launch$args)]
  expect_true(grepl("bin/DinamicaEGO.sh", payload, fixed = TRUE),
    info = "Last arg must be the bash -c payload invoking bin/DinamicaEGO.sh.")

  # Pull the path after bin/DinamicaEGO.sh. It must be absolute — on POSIX
  # that means starting with `/`; on Windows normalizePath() returns `C:/...`.
  # Both shapes are "absolute" per R's normalizePath contract; the test
  # accepts either so it works on workstation and HPC.
  # Optional `-disable-parallel-steps` / `-log-level <n>` flags may be spliced
  # between `bin/DinamicaEGO.sh` and the absolute model path (D-104). The
  # `(?:-\S+\s+)*` allows zero or more leading flag tokens before the path.
  expect_true(
    grepl(
      "bin/DinamicaEGO\\.sh\\s+(?:-\\S+\\s+)*['\"]?(?:/|[A-Za-z]:/)",
      payload, perl = TRUE
    ),
    info = sprintf(
      "Model path in bash -c payload must be absolute (D-106). payload=%s",
      payload
    )
  )

  # Direct sanity check: the resolved payload must NOT contain a leading
  # "model.ego" relative form (i.e., it must have been replaced).
  expect_false(
    grepl("DinamicaEGO\\.sh\\s+['\"]?model\\.ego", payload),
    info = "Relative model path must have been normalized to absolute before splicing."
  )
})

test_that("resolve_dinamica_launch() HPC creates staged-home + staged-tmp idempotently (D-105)", {
  skip_if_not(is.function(.env$resolve_dinamica_launch))

  model <- file.path(.repo_root, "dinamica", "dinamica_model", "allocation.ego-decoded")
  staged_root <- tempfile("hpc_scratch_")
  dir.create(staged_root, recursive = TRUE, showWarnings = FALSE)
  withr::local_envvar(
    DINAMICA_EGO_8_HOME = "/tmp/dinamica.sif",
    DINAMICA_BACKEND    = "hpc",
    HPC_SCRATCH_ROOT    = staged_root
  )

  # First call creates the directories + the conf file.
  .env$resolve_dinamica_launch(
    model_path       = model,
    backend          = "hpc",
    runtime_override = "apptainer",
    probe_runtime    = FALSE
  )

  staged_home <- file.path(staged_root, "dinamica-home")
  staged_tmp  <- file.path(staged_root, "dinamica-tmp")
  conf_path   <- file.path(staged_home, ".dinamica_ego_8.conf")

  expect_true(dir.exists(staged_home),
    info = "<HPC_SCRATCH_ROOT>/dinamica-home must exist after resolve_dinamica_launch() (D-105).")
  expect_true(dir.exists(staged_tmp),
    info = "<HPC_SCRATCH_ROOT>/dinamica-tmp must exist after resolve_dinamica_launch() (D-105).")
  expect_true(file.exists(conf_path),
    info = ".dinamica_ego_8.conf must be seeded in staged-home (D-105).")

  # Verify exact content.
  conf_lines <- readLines(conf_path)
  expect_identical(
    conf_lines,
    c(
      'AlternativePathForR = "/usr/local/bin/Rscript"',
      'ClConfig = "0"',
      'MemoryAllocationPolicy = "1"',
      'RCranMirror = "https://cloud.r-project.org/"'
    )
  )

  # Second call must be idempotent — directories and conf file remain.
  conf_mtime_before <- file.info(conf_path)$mtime
  .env$resolve_dinamica_launch(
    model_path       = model,
    backend          = "hpc",
    runtime_override = "apptainer",
    probe_runtime    = FALSE
  )
  expect_true(dir.exists(staged_home))
  expect_true(dir.exists(staged_tmp))
  expect_true(file.exists(conf_path))
  # File must NOT have been rewritten (idempotent seed; only writes when missing).
  expect_equal(file.info(conf_path)$mtime, conf_mtime_before,
    info = "Idempotent seed must NOT rewrite the conf file on a second call.")
})

test_that(".ensure_dinamica_pyenv_executable() no-ops when the PyEnvironment is absent", {
  skip_if_not(is.function(.env$.ensure_dinamica_pyenv_executable))

  # A fresh staged-home has no PyEnvironment yet (Dinamica extracts it during
  # the run). The guard must return invisibly without erroring.
  staged_home <- tempfile("dinamica_home_")
  dir.create(staged_home, recursive = TRUE, showWarnings = FALSE)
  expect_true(.env$.ensure_dinamica_pyenv_executable(staged_home))
})

test_that("resolve_dinamica_launch() HPC self-heals a non-executable staged PyEnvironment", {
  skip_if_not(is.function(.env$resolve_dinamica_launch))
  # Exec-bit semantics are POSIX; Windows does not model the unix +x bit.
  skip_on_os("windows")

  model <- file.path(.repo_root, "dinamica", "dinamica_model", "allocation.ego-decoded")
  staged_root <- tempfile("hpc_scratch_")
  pyenv <- file.path(staged_root, "dinamica-home", ".local", "share",
                     "Dinamica EGO 8", "PyEnvironment")
  py_bin_dir <- file.path(pyenv, "bin", "python3")
  dir.create(py_bin_dir, recursive = TRUE, showWarnings = FALSE)
  py_bin <- file.path(py_bin_dir, "python3.12")
  writeLines("#!/bin/sh\nexit 0", py_bin)
  # Reproduce the beegfs symptom: interpreter written without the execute bit.
  Sys.chmod(py_bin, mode = "0644", use_umask = FALSE)
  expect_false(file.access(py_bin, mode = 1L) == 0L)

  withr::local_envvar(
    DINAMICA_EGO_8_HOME = "/tmp/dinamica.sif",
    DINAMICA_BACKEND    = "hpc",
    HPC_SCRATCH_ROOT    = staged_root
  )

  .env$resolve_dinamica_launch(
    model_path       = model,
    backend          = "hpc",
    runtime_override = "apptainer",
    probe_runtime    = FALSE
  )

  expect_true(file.access(py_bin, mode = 1L) == 0L,
    info = "resolve_dinamica_launch() must restore the +x bit on the staged interpreter.")
})

test_that("resolve_dinamica_launch() HPC fails clearly when HPC_SCRATCH_ROOT is unset (D-105)", {
  skip_if_not(is.function(.env$resolve_dinamica_launch))

  model <- file.path(.repo_root, "dinamica", "dinamica_model", "allocation.ego-decoded")
  withr::local_envvar(
    DINAMICA_EGO_8_HOME = "/tmp/dinamica.sif",
    DINAMICA_BACKEND    = "hpc",
    HPC_SCRATCH_ROOT    = NA
  )

  expect_error(
    .env$resolve_dinamica_launch(
      model_path       = model,
      backend          = "hpc",
      runtime_override = "apptainer",
      probe_runtime    = FALSE
    ),
    regexp = "HPC_SCRATCH_ROOT"
  )
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
  expect_identical(launch$command, "DinamicaConsole")
  # Local backend must not return apptainer/singularity in the resolved args.
  expect_false(any(grepl("apptainer|singularity", launch$args)),
    info = "Local backend must launch DinamicaConsole directly, no container runtime.")
  # Local backend must NOT include --home or --env (D-105 last clause).
  expect_false(any(grepl("^--home$", launch$args)),
    info = "Local backend must NOT include --home (D-105).")
  expect_false(any(grepl("^--env$", launch$args)),
    info = "Local backend must NOT include --env (D-105).")
})

test_that("resolve_dinamica_launch() falls back to singularity when override == 'singularity'", {
  skip_if_not(is.function(.env$resolve_dinamica_launch))

  model <- file.path(.repo_root, "dinamica", "dinamica_model", "allocation.ego-decoded")
  staged_root <- tempfile("hpc_scratch_")
  dir.create(staged_root, recursive = TRUE, showWarnings = FALSE)
  withr::local_envvar(
    DINAMICA_EGO_8_HOME = "/tmp/dinamica.sif",
    HPC_SCRATCH_ROOT    = staged_root
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

test_that("resolve_dinamica_launch() return list keys are unchanged", {
  skip_if_not(is.function(.env$resolve_dinamica_launch))

  model <- file.path(.repo_root, "dinamica", "dinamica_model", "allocation.ego-decoded")
  staged_root <- tempfile("hpc_scratch_")
  dir.create(staged_root, recursive = TRUE, showWarnings = FALSE)
  withr::local_envvar(
    DINAMICA_EGO_8_HOME = "/tmp/dinamica.sif",
    DINAMICA_BACKEND    = "hpc",
    HPC_SCRATCH_ROOT    = staged_root
  )

  launch <- .env$resolve_dinamica_launch(
    model_path       = model,
    backend          = "hpc",
    runtime_override = "apptainer",
    probe_runtime    = FALSE
  )

  expect_true(
    setequal(
      names(launch),
      c("backend", "runtime", "artifact_path", "command", "args", "log_file", "env")
    ),
    info = "Return list keys must remain: backend, runtime, artifact_path, command, args, log_file, env."
  )
})
