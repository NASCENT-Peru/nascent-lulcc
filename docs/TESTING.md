<!-- generated-by: gsd-doc-writer -->
# Testing

The nascent-lulcc pipeline uses [testthat](https://testthat.r-lib.org/) (edition 3) as its R testing framework, supplemented by a pure-bash shell test suite for infrastructure-level checks. Tests are primarily contract tests that validate pipeline contracts against source files, environment YAML declarations, and live R function behaviour — the test suite does not require a full HPC environment, Dinamica EGO, or real raster data to run.

## Test Framework and Setup

| Component | Detail |
|---|---|
| R test framework | `testthat` >= 3.0.0 (declared in `DESCRIPTION` `Suggests`) |
| R package edition | testthat edition 3 (`Config/testthat/edition: 3` in `DESCRIPTION`) |
| Shell test runner | pure bash (`tests/shell/`) — no R or Dinamica required |
| Entry point | `tests/testthat.R` |

**Required packages for the full test suite:**

- `testthat` (>= 3.0.0) — always required
- `withr` — required by Dinamica launcher and error-detection tests
- `ps` — required by the portable RSS memory test in `test-allocation-preflight.R`
- `mlr3`, `mlr3learners`, `qs` — required by the integration tests in `test-mlr3-training-pipeline.R` and `test-mlr3-predict-dispatch.R`; tests are skipped gracefully when these are absent

Tests that depend on optional packages use `skip_if_not_installed()` and `skip_if_not()` guards, so the suite runs (with some tests skipped) even without a full mlr3 installation.

## Running Tests

### Full R test suite

Run all testthat tests from the repository root:

```bash
Rscript -e 'testthat::test_dir("tests/testthat")'
```

Alternatively, run via the entry point script:

```bash
Rscript tests/testthat.R
```

### Single test file

```bash
Rscript -e 'testthat::test_file("tests/testthat/test-allocation-preflight.R")'
```

### Shell infrastructure tests

The shell test suite runs independently and requires only `bash` (no R, Dinamica, micromamba, or apptainer):

```bash
bash tests/shell/test-setup-environments-hpc-refusal.sh
```

The script prints `PASS`/`FAIL` per case and exits non-zero if any case fails.

### Running on HPC (ETH Euler)

Load the appropriate environment and invoke Rscript from an interactive session or a SLURM job:

```bash
# Example: activate the allocation environment, then run tests
micromamba run -n allocation_env Rscript -e 'testthat::test_dir("tests/testthat")'
```

## Test File Overview

All R tests live under `tests/testthat/`. Shell tests live under `tests/shell/`.

| File | What it tests |
|---|---|
| `test-prep-paths.R` | Source file contracts: `calibration_predictor_prep.r` and `simulation_trans_rates_prep.r` no longer hardcode Windows paths (`E:/terra_temp`, `E:/nascent-lulcc`) |
| `test-stage7-paths.R` | `src/setup.r` exposes `get_stage7_runtime_paths()` returning the five Stage 7 runtime path keys; env-var overrides honoured; config files contain no user-specific hardcoded paths |
| `test-allocation-env-canonical.R` | `environments/allocation_env.yml` declares the full MEM-06 prediction-time package set and pins `r-xgboost=1.7`; submit scripts activate `allocation_env` not `transition_model_env` |
| `test-mlr3-env-contract.R` | `allocation_env.yml` and `transition_model_env.yml` contain all required Phase 2 mlr3 packages; `max_training_rows` present in both config files |
| `test-mlr3-training-pipeline.R` | `src/transition_modelling.r` defines `train_mlr3_transition()`; no tidymodels/workflows library calls; model output uses `.qs` extension; integration test produces a valid `.qs` model file with correct structure |
| `test-mlr3-predict-dispatch.R` | `allocation.r` dispatches to the mlr3 branch before the tidypredict branch; uses `predict_newdata()` and named `prob[, "1"]` indexing; model loader calls `qs::qread()` for `.qs` files |
| `test-allocation-preflight.R` | `src/allocation.r` exposes `validate_allocation_runtime()` that returns a consolidated error vector; accepts a fixture for injection testing; portable RSS reader returns numeric `rss` on all platforms |
| `test-allocation-breadcrumbs.R` | `src/utils.r` exposes `worker_state_init()`, `worker_state_set()`, `worker_state_flush_sentinel()`; sentinel log line tagged `SENTINEL` with scenario/region/timestep/transition; `run_allocation_dinamica()` emits structured `DINAMICA_*` breadcrumbs |
| `test-allocation-runtime-contract.R` | `allocation.r` defines `select_allocation_plan()` with multicore/multisession backends; `run_allocation.r` pins native threads before the first `future::plan()`; strict-globals gate wired to `ALLOCATION_DEV_STRICT_GLOBALS`; smoke filters exist for `ALLOCATION_REGION_FILTER` and `ALLOCATION_YEAR_POST_FILTER` |
| `test-allocation-memory-contract.R` | `allocation.r` defines `write_nhood_tif()`, `load_allocation_models()`; workers reopen neighbourhood rasters from TIF paths; parent-stage preload and baseline markers present and ordered correctly |
| `test-dinamica-error-detection.R` | `src/dinamica_utils.r` exposes `DINAMICA_ERROR_PATTERNS` (three D-107 strings) and `.check_dinamica_post_run()`; helper fires on matching log content even when exit code is 0; handles logfile and stdout/stderr fallback modes; error message includes pattern name and logfile path |
| `test-dinamica-launcher.R` | `src/dinamica_utils.r` exposes `resolve_dinamica_launch()` returning the correct HPC apptainer launch shape (D-104: `--home`, `--env`, `bash -c`, `bin/DinamicaEGO.sh`); staged-home/tmp created idempotently (D-105); model path always absolute (D-106); local backend uses direct `DinamicaConsole` without container args |
| `test-dinamica-launch-contract-mirror.R` | Cross-language mirror: R-side `resolve_dinamica_launch()` and shell-side `scripts/smoke_test_dinamica.sh` produce matching launch shapes for the same inputs (skipped on Windows due to CRLF checkout) |
| `tests/shell/test-setup-environments-hpc-refusal.sh` | `scripts/setup_environments.sh` refuses to fall back to `$PROJECT_ROOT/.envs` on HPC when `HPC_SCRATCH_ROOT` is unset; workstation fallback path still works; HPC success path with `HPC_SCRATCH_ROOT` set |

## Writing New Tests

### File naming

Follow the existing convention: `test-<subject>.R` using kebab-case in `tests/testthat/`. Shell tests follow `test-<subject>.sh` in `tests/shell/`.

### Source isolation pattern

Each test file resolves the repository root from the test file's own path (so tests work both via `test_dir()` and direct `Rscript` invocation) and sources the file under test into an isolated `new.env()`:

```r
.repo_root <- (function() {
  here <- tryCatch(normalizePath(sys.frame(1)$ofile %||% "."), error = function(e) ".")
  if (file.info(here)$isdir %||% FALSE) here <- file.path(here, "x")
  normalizePath(file.path(dirname(dirname(dirname(here)))), mustWork = FALSE)
})()

.env <- new.env(parent = baseenv())
sys.source(file.path(.repo_root, "src", "my_module.r"), envir = .env)
```

### Integration test guards

Wrap integration tests that require optional packages with `skip_if_not_installed()`:

```r
test_that("integration test with mlr3", {
  skip_if_not_installed("mlr3")
  skip_if_not_installed("qs")
  # ... test body
})
```

### Text-grep pattern

For contract tests that assert source file content without executing it (the preferred lightweight pattern for CI), read the file as text and use `grepl()`:

```r
content <- paste(readLines(file.path(.repo_root, "src", "my_file.r"), warn = FALSE), collapse = "\n")
expect_true(grepl("expected_function_name", content, fixed = TRUE))
```

### Shell test pattern

Shell tests use the `run_case` helper pattern established in `test-setup-environments-hpc-refusal.sh`: each case specifies expected exit code, stdout regex, and stderr regex. Add new cases to an existing shell test file or create a new one following the same pattern.

## Coverage Requirements

No automated coverage thresholds are configured. The test suite focuses on contract fidelity — asserting that specific functions exist, source files contain required patterns, environment YAML files declare required packages, and runtime helpers return values matching the documented API contracts — rather than line-level coverage of the full pipeline.

## CI Integration

No CI pipeline is currently configured (no `.github/workflows/` directory). Tests are expected to be run manually before committing changes that affect the contracted surface areas listed in the test file overview above, and on the ETH Euler HPC cluster when validating HPC-specific behaviour such as the Dinamica launcher and environment setup scripts.
