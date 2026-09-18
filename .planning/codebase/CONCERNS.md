# CONCERNS — nascent-lulcc
_Last updated: 2026-04-30_

## Summary

The codebase is an active research pipeline for land use/land cover change (LULCC) modelling in R, targeting the ETH Euler HPC cluster. The core pipeline stages (allocation, transition modelling, feature selection, simulation transition rates) are reasonably well-structured with error handling present in most critical paths. However, several medium-priority issues exist: hardcoded developer-specific file paths in active source files, an incomplete core optimization loop in `simulation_trans_rates_prep.r`, reliance on the legacy `raster` package alongside `terra`, and a complete absence of automated tests despite `testthat` being listed in DESCRIPTION. There are no secrets in source control and no critical blocking bugs in the active pipeline.

---

## High Priority

### Hardcoded local Windows drive path blocks HPC execution of simulation trans rates
- **Issue:** `src/simulation_trans_rates_prep.r` line 181 uses a hardcoded absolute Windows path `"E:/nascent-lulcc-agg/inputs/lulc/future_demand/LULC_demand_results.xlsx"` inside the active (non-commented) xlsx loading block. The CSV alternative is commented out. This path does not exist on the HPC cluster and will cause a hard stop every run.
- **Files:** `src/simulation_trans_rates_prep.r` (line 181), `src/simulation_trans_rates_prep.r` (lines 145–172 commented-out CSV block)
- **Impact:** `simulation_trans_rates_prep` fails immediately on any machine other than the developer's local Windows workstation. The `lulc_demand_path` config key exists and points to a CSV, but the code ignores it.
- **Fix approach:** Uncomment the CSV loading block (lines 145–172), delete or guard the xlsx block, use `config[["lulc_demand_path"]]` which is already defined in both `local_config.yaml` and `hpc_config.yaml`.

### Core optimization loop is a documented placeholder
- **Issue:** The `SIMULATION_TRANS_RATES_REFACTOR_SUMMARY.md` doc explicitly states (lines 63–64, 165–182) that section G of `simulation_trans_rates_prep.r` — the CVXR convex optimization loop — "needs to be integrated" from the old script (`src/old/simulation_transition_rates_estimation.R` lines 220–776). This means the current `simulation_trans_rates_prep` function runs setup but does not actually produce transition rate tables.
- **Files:** `src/simulation_trans_rates_prep.r`, `src/old/simulation_transition_rates_estimation.R`
- **Impact:** Any downstream pipeline stage that reads output from this step (allocation, Dinamica simulations) will fail or use stale outputs from a prior run.
- **Fix approach:** Port the optimization loop from `src/old/simulation_transition_rates_estimation.R` (lines 220–776) into the marked section G of the new script, substituting config-loaded parameters for the previously hardcoded ones.

---

## Medium Priority

### Hardcoded terra temp directory in `calibration_predictor_prep.r`
- **Issue:** `src/calibration_predictor_prep.r` line 17 hardcodes `terra_temp <- "E:/terra_temp"` regardless of environment. The drive `E:` does not exist on the HPC cluster.
- **Files:** `src/calibration_predictor_prep.r` (line 17)
- **Impact:** `calibration_predictor_prep` will fail to set a valid terra temp dir on HPC and may produce terra I/O errors or fall back silently to `C:` (which may have limited space on the cluster login node).
- **Fix approach:** Replace with `Sys.getenv("TERRA_TEMP", unset = tempdir())`, consistent with the pattern already used in `scripts/run_ancillary_data_prep.r` (line 131) and `scripts/hpc_common.sh` (line 89).

### Legacy `raster` package used alongside `terra` in active (non-old) code
- **Issue:** 73 `raster::` calls appear in active source files (excluding `src/old/`). The `raster` package is retired and superseded by `terra`. The two packages have incompatible object types, requiring frequent coercion. `allocation_env.yml` does not include `r-raster`, so the package may not be present in the allocation environment.
- **Files:** `src/lulcc.spatprobmanipulation.r` (~50 calls), `src/landscape_pattern_analysis.r` (~8 calls), `src/spatial_interventions_prep.r` (lines 12–13)
- **Impact:** Silent coercion failures or hard errors when running allocation or spatial interventions in envs that lack `r-raster`. Double memory usage when objects are coerced between types on large rasters.
- **Fix approach:** Port `lulcc.spatprobmanipulation.r` and `spatial_interventions_prep.r` from `raster::stack/mask/overlay/rasterFromXYZ` to `terra::rast/mask/app/rasterize`. `landscape_pattern_analysis.r` is lower-priority as it is not part of the core pipeline.

### HPC shell scripts hardcode username `black` in paths
- **Issue:** `scripts/hpc_common.sh` (lines 13, 89, 114) and `scripts/setup_environments.sh` (lines 20, 47) use paths like `/home/black/`, `/beegfs/black/`. The `.env.template` (lines 5–19) also hardcodes these paths. This prevents other researchers from running the pipeline.
- **Files:** `scripts/hpc_common.sh`, `scripts/setup_environments.sh`, `.env.template`
- **Impact:** Any collaborator or new user must manually find and replace all `black` references before the HPC scripts work. Easy to miss a reference.
- **Fix approach:** Replace hardcoded username segments with `$USER` shell variable (e.g., `/beegfs/$USER/nascent-lulcc`). The `$HOME` variable already handles the home directory case in line 80 of `hpc_common.sh`.

### `spatial_interventions_prep.r` FIXME: hardcoded protected area coverage percentage
- **Issue:** `src/spatial_interventions_prep.r` line 575 contains `cover_poly_raw <- 0.178` immediately after a comment saying `# FIXME this is now 16.9%`, overwriting a computed value with a hardcoded one. This was intentionally inserted to work around a solver infeasibility, but the FIXME acknowledges the root cause is unresolved.
- **Files:** `src/spatial_interventions_prep.r` (lines 571–575)
- **Impact:** `findSumm` solver receives a different target than the data would compute, masking the real coverage discrepancy. Results for conservation-area scenarios (EI_SOC, EI_CUL, EI_NAT) will be based on the hardcoded value rather than the actual data.
- **Fix approach:** Investigate why the raw polygon area calculation returns 16.9% instead of expected ~17.8%, correct the underlying area calculation or adjust the target scenario parameters, then remove the hardcoded override.

### Intervention YAML files reference old relative paths that do not match config schema
- **Issue:** All five `config/SSP*_interventions.yml` files (42 occurrences total) reference `Intervention_mask` paths starting with `Data/Spat_prob_perturb_layers/...`. This path prefix does not match the config key `spat_prob_perturb_dir` which resolves to `inputs/spat_prob_perturb`. These are not config-relative paths — they appear to be legacy paths from a previous project structure.
- **Files:** `config/SSP0_interventions.yml`, `config/SSP1_interventions.yml`, `config/SSP3_interventions.yml`, `config/SSP4_interventions.yml`, `config/SSP5_interventions.yml`
- **Impact:** Spatial intervention masks cannot be found at runtime when `implement_spatial_interventions.R` resolves these paths, causing allocation to silently skip or hard-fail for all scenarios using interventions.
- **Fix approach:** Audit actual raster file locations under `inputs/spat_prob_perturb`, update all `Intervention_mask` paths in the YAML files to match, or introduce a path-resolution step in `implement_spatial_interventions.R` that prepends `config[["spat_prob_perturb_dir"]]`.

### `nhood_predictor_prep.r` non-reproducible random matrices (no seed)
- **Issue:** `src/nhood_predictor_prep.r` lines 80–101 generate random Pythagorean decay matrices without setting a seed, preceded by a FIXME comment (line 68): "this looks like you could simply set the seed to be reproducible?" The matrices are cached to `all_matrices.rds`, so repeated runs reuse the same matrices — but the initial generation is not reproducible if the cache is cleared.
- **Files:** `src/nhood_predictor_prep.r` (lines 67–100)
- **Impact:** Re-running from scratch after cache deletion produces different neighbourhood predictor layers, which flow through to transition model inputs. Results are not reproducible from source.
- **Fix approach:** Add `set.seed(<fixed_value>)` before the `lapply` over `matrix_sizes`, document the seed value in the config or a comment.

### `nhood_predictor_prep.r` type inconsistency: `LULC_years` changes from vector to list mid-function
- **Issue:** `src/nhood_predictor_prep.r` line 116 contains a FIXME: "until here, LULC_years is a vector, from here on out it's a list." The variable is mutated in-place from character vector to named list, which makes the function logic hard to reason about and can cause subtle bugs if any code after line 116 accidentally uses vector-style indexing.
- **Files:** `src/nhood_predictor_prep.r` (lines 116–125)
- **Impact:** Increased fragility; future edits above or below this line may introduce type errors that are hard to debug.
- **Fix approach:** Use a distinct variable name (e.g., `LULC_years_list`) from line 117 onwards, or restructure to build the list from the start.

### `dinamica_utils.r` Dinamica logs written to model working directory, not central log dir
- **Issue:** `src/dinamica_utils.r` line 49 (TODO comment): log files from Dinamica EGO runs are written to `dirname(model_path)` (the region-specific work directory) rather than the shared `logs/` directory used by all other pipeline stages.
- **Files:** `src/dinamica_utils.r` (lines 49–55)
- **Impact:** Dinamica logs are scattered across potentially hundreds of region work directories, making it difficult to grep for errors across an allocation run.
- **Fix approach:** Pass `log_dir` (from config or the caller) into `exec_dinamica` and write the logfile to that directory, following the pattern used in `allocation.r`'s per-region log files.

### `allocation.r` raster writes bypass the project's `write_raster` utility
- **Issue:** `src/allocation.r` line 748 (TODO comment): `terra::writeRaster` is called directly with inline `wopt` settings rather than routing through the project's `write_raster()` helper in `src/utils.r`, which auto-selects datatype and compression. This applies to the anterior raster and subsequent probability/posterior rasters.
- **Files:** `src/allocation.r` (lines 748–754 and surrounding raster writes)
- **Impact:** Inconsistent compression settings across pipeline outputs; if `write_raster()` defaults ever change, only some outputs will benefit.
- **Fix approach:** Replace direct `terra::writeRaster` calls in `allocation.r` with `write_raster()` from `src/utils.r`.

### `allocation.r` inner functions cannot log to the per-region log file
- **Issue:** `src/allocation.r` line 762 (TODO comment): `setup_allocation_inputs` and `run_allocation_dinamica` do not accept a `log_file` parameter and cannot emit messages to the per-region log. Failures inside these functions are invisible in the region log.
- **Files:** `src/allocation.r` (lines 762–785)
- **Impact:** Reduced observability; debugging failures in allocation setup requires manually checking stdout rather than the log file.
- **Fix approach:** Add `log_file = NULL` parameter to `setup_allocation_inputs` and `run_allocation_dinamica`, propagate `log_msg()` calls internally.

---

## Low Priority / Nice to Have

### Commented-out "testing values" block left at top of `allocation.r`
- **Issue:** `src/allocation.r` lines 10–14 contain a block of commented-out interactive testing assignments (`scenario <- ...`, `i <- 1`, etc.). These are developer scratchpad values, not documentation.
- **Files:** `src/allocation.r` (lines 10–14)
- **Fix approach:** Remove the block or move to a dedicated `dev/` scratch script.

### `dist_calc_functions.r` uses `print()` for production logging
- **Issue:** `src/dist_calc_functions.r` lines 83 and 317 call `print(terra::rast(...))` for feedback rather than `log_msg()`. This sends raster summaries to stdout but not to any log file.
- **Files:** `src/dist_calc_functions.r` (lines 83, 317)
- **Fix approach:** Replace with `log_msg(format(terra::rast(...)), log_file)` or suppress if not informative.

### `implement_spatial_interventions.R` includes debug `cat()` left in production path
- **Issue:** `src/implement_spatial_interventions.R` line 425 has a comment "print the percentage difference for debugging purposes" followed by a `cat()` call in a production code path.
- **Files:** `src/implement_spatial_interventions.R` (lines 425–438)
- **Fix approach:** Replace with a `log_msg()` call or remove.

### `landscape_pattern_analysis.r` uses retired packages (`SDMTools`, `rgdal`, `rgeos`)
- **Issue:** `src/landscape_pattern_analysis.r` lines 14, 27–28 list `SDMTools`, `rgdal`, and `rgeos` — all retired from CRAN. This function is not part of the active SLURM pipeline but is committed to `src/` (not `src/old/`).
- **Files:** `src/landscape_pattern_analysis.r` (lines 9–35)
- **Fix approach:** Move to `src/old/` if no longer in use, or replace retired package calls with `terra`/`landscapemetrics` equivalents.

### `simulation_trans_rates_prep.r` has a large commented-out CSV block
- **Issue:** Lines 145–172 of `src/simulation_trans_rates_prep.r` are a fully-formed, working CSV loading alternative that is commented out. The active path uses a hardcoded xlsx file (see High Priority). The CSV block should be the default.
- **Files:** `src/simulation_trans_rates_prep.r` (lines 145–172, 180–200+)
- **Fix approach:** Re-enable the CSV block and delete the xlsx block once the High Priority path fix is applied.

### `src/old/` directory contains large legacy files tracked in git
- **Issue:** The `src/old/` directory contains 12+ R files (including 1300+ line scripts) that are superseded by refactored equivalents. They accumulate git history weight and can confuse new contributors.
- **Files:** `src/old/` (all files)
- **Fix approach:** Move to a separate archive branch or delete with a note in CHANGELOG. The most important logic (CVXR optimization loop) is still needed — see High Priority item above.

### `testthat` listed as Suggests in DESCRIPTION but no tests exist
- **Issue:** `DESCRIPTION` (line 72) lists `testthat (>= 3.0.0)` as a suggested dependency and sets `Config/testthat/edition: 3`, but there are zero test files in the repository (no `tests/` directory, no `*.test.r` files).
- **Files:** `DESCRIPTION`
- **Impact:** No automated regression testing for any pipeline stage. Errors introduced during refactoring (e.g., the ongoing debugging commits visible in git log) are caught only by running full pipeline jobs.
- **Fix approach:** Add at minimum unit tests for the pure-function utilities in `src/utils.r`, `src/setup.r` (config loading), and the optimization helper functions in `src/simulation_trans_rates_prep.r`.

---

## Gaps / Unknowns

- **`src/landscape_pattern_analysis.r` pipeline integration:** It is unclear whether this file is intended to be part of any active submit/run pipeline. No submit or run script sources it. It may be a standalone analysis script that needs its own environment spec.
- **`lulcc.spatprobmanipulation.r` vs `implement_spatial_interventions.R` relationship:** Both files appear to implement spatial probability manipulation for interventions. `run_allocation.r` sources `lulcc.spatprobmanipulation.r` directly. The relationship and intended division of responsibility between the two files is not documented.
- **Intervention config paths resolution:** It is unclear how `src/implement_spatial_interventions.R` resolves the `Data/Spat_prob_perturb_layers/...` paths from the SSP YAML files — whether it prepends `data_basepath`, uses a working directory, or expects absolute paths. This needs tracing through the runtime call chain before the paths can be safely updated.
- **Peru vs. Switzerland data:** Several comments and some config values reference "Peru" (e.g., `src/old/simulation_transition_rates_estimation.R` has Peru-specific paths, intervention_planning.txt references Peru locations). The active configs reference Switzerland (EPSG:2056, Swiss scenarios). The extent to which Peru-specific assumptions remain embedded in active code is not fully audited.
- **`src/parquet_check.r` status:** This file contains a mix of active and commented-out DuckDB queries with hardcoded `E:/` paths. It is unclear if it is a utility script, a one-off diagnostic, or dead code.
- **`DINAMICA_EGO_8_HOME` environment variable:** Required by `src/dinamica_utils.r` (line 39) and not documented in `.env.template` or either config file. If unset, the allocation step fails with a hard stop. Its required value on HPC is not recorded.
