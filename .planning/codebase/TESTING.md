# TESTING — nascent-lulcc
_Last updated: 2026-04-30_

## Summary
The project is structured as an R package (`evoland`) and declares `testthat (>= 3.0.0)` as a suggested dependency, with `Config/testthat/edition: 3` in `DESCRIPTION`. However, no test files or `tests/` directory are present in the repository — the test infrastructure is declared but entirely unimplemented. There is no CI pipeline configured (no `.github/workflows/`, no `.travis.yml`, no other CI config).

---

## Test Framework Declaration

**Framework:** `testthat` edition 3  
**Declared in:** `DESCRIPTION` lines 72, 80:
```
Suggests:
    testthat (>= 3.0.0)
Config/testthat/edition: 3
```

**Standard R package test runner:**
```r
# Run tests (would work if tests existed):
devtools::test()
# or
Rscript -e "testthat::test_package('evoland')"
```

---

## Test Files Present

**None.** There is no `tests/` directory and no files matching `test_*.r`, `*.test.r`, or `*_spec.r` anywhere in the repository.

---

## CI/CD Configuration

**None.** The repository contains no CI configuration:
- No `.github/` directory
- No `.travis.yml`, `circle.yml`, `Jenkinsfile`, or similar
- No `Makefile` with a `test` target

The only automation infrastructure is SLURM HPC job submission scripts (`scripts/submit_*.sh`) for running the pipeline on a compute cluster — these are pipeline execution scripts, not test runners.

---

## How the Pipeline is Validated Today

In the absence of tests, correctness is validated operationally:

1. **Intermediate file existence checks:** Each pipeline step checks for cached output files before processing. If a step writes a file, the next step will only proceed if that file exists.

2. **`stopifnot()` assertions:** Type and class assertions on critical inputs (e.g., `stopifnot(inherits(r, "SpatRaster"))` in `src/utils.r`).

3. **`tryCatch()` error logging:** Parallel workers catch errors and log them to per-worker log files without crashing the overall run. Errors surface post-hoc via log inspection.

4. **Reconciliation outputs:** `transition_modelling()` writes a reconciliation file recording which transition-region models succeeded or failed, consumed by the next step via `load_unmodelled_transitions()` (`src/simulation_trans_rates_prep.r`).

5. **Profiling output:** Optional profiling via `ALLOCATION_PROFILE=TRUE` env var writes timing and RSS memory metrics to SLURM `.out` files — used for performance validation, not correctness.

---

## Rcpp C++ Code

Two C++ source files exist:
- `src/neighbors.cpp` — spatial hash-based neighbor lookup (`distance_neighbors_cpp`)
- `src/patch_stats.cpp` — landscape patch statistics (aggregation index)

These are compiled via `Rcpp::sourceCpp()` or `devtools::build()`. No C++ unit tests (e.g., Catch2 via `testthat::use_catch()`) are present.

---

## What Tests Should Cover (Gaps)

The following untested areas represent the highest risk given the pipeline's complexity:

| Area | Files | Risk |
|------|-------|------|
| Config loading and path resolution | `src/setup.r` | Silent misconfiguration breaks all downstream steps |
| Raster alignment logic | `src/utils.r` `align_to_ref()` | CRS/extent mismatch can produce silently wrong outputs |
| LULC reclassification | `src/lulc_data_prep.r` | Wrong reclassification map poisons all model inputs |
| Transition identification | `src/transition_identification.r` | Wrong transitions flow into modelling and allocation |
| Simulation transition rate generation | `src/simulation_trans_rates_prep.r` | Optimization errors produce invalid rates |
| Allocation parameter calibration | `src/calibrate_allocation_parameters.r` | Calibration failures are silent if caught by tryCatch |
| Intervention application | `src/implement_spatial_interventions.R`, `src/spatial_interventions_prep.r` | Incorrect spatial masking produces wrong scenario outputs |
| Rcpp neighbor distance computation | `src/neighbors.cpp` | Off-by-one errors in spatial indexing |

---

## Recommended Test Structure (if implemented)

Following standard R package conventions with testthat edition 3:

```
tests/
  testthat/
    helper-fixtures.r       # shared test data factories
    test-utils.r            # tests for src/utils.r functions
    test-setup.r            # tests for config loading (src/setup.r)
    test-lulc-data-prep.r   # tests for reclassification logic
    test-transition-id.r    # tests for transition identification
    test-allocation.r       # tests for allocation helpers
```

**Run command (once tests exist):**
```bash
Rscript -e "devtools::test()"
```

**Coverage (once tests exist):**
```bash
Rscript -e "covr::package_coverage()"
```

---

## Gaps / Unknowns

- Zero test coverage currently. The package has never had tests written for it.
- No CI pipeline — changes are not automatically validated on push.
- No snapshot tests or visual regression tests for raster outputs.
- No mock strategies established for large file I/O (tests would need synthetic small rasters).
- `usethis` is listed in `Suggests` (alongside `testthat`), suggesting test scaffolding was planned but never executed.
