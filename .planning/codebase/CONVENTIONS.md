# CONVENTIONS — nascent-lulcc
_Last updated: 2026-04-30_

## Summary
The codebase is a pure R project (with Rcpp C++ extensions) structured as an R package (`evoland`). All primary source code lives in `src/` as lowercase `.r` files. Conventions are consistent but informal — there is no active linter or formatter configured (no `.lintr`, no `styler` config). Style is tidyverse-adjacent with heavy use of `magrittr` pipes, `purrr`, and `dplyr`.

---

## Language and File Extensions

- Primary: R (`src/*.r`) — lowercase `.r` extension used consistently **except** for two files with uppercase `.R`: `src/implement_spatial_interventions.R` and `src/old/simulation_transition_rates_estimation.R`. New files should use lowercase `.r`.
- C++ extension files: `src/neighbors.cpp`, `src/patch_stats.cpp` — compiled via Rcpp.
- Auto-generated Rcpp bridge: `src/RcppExports.cpp`, `src/RcppExports.r` — do not edit by hand.
- Scripts (entry points for HPC runs): `scripts/run_*.r` and `scripts/submit_*.sh`.

---

## Naming Conventions

### Functions
- **snake_case** throughout. All exported and internal functions use lowercase words joined by underscores.
  - Examples: `transition_modelling()`, `ensure_dir()`, `write_raster()`, `align_to_ref()`, `log_msg()`, `load_predictor_data()`
- **Dot-prefix for private/internal helpers** that are not meant to be called by users:
  - Examples: `.profile_on()`, `.read_proc_status()`, `.reset_vmhwm()`, `.datatable.aware`
- **Legacy dot-separated names** exist in older code — `lulcc.spatprobmanipulation()` (`src/lulcc.spatprobmanipulation.r`) — do not add new functions in this style.
- C++ functions exported via Rcpp follow `snake_case` as well: `distance_neighbors_cpp()`.

### Files (src/)
- Match the primary exported function name: `transition_modelling.r` exports `transition_modelling()`.
- Multi-function utility files: `utils.r` (shared helpers), `utils-pipe.r` (pipe re-export).
- Predictor prep files follow the pattern `<domain>_pred_prep.r`: `climatic_pred_prep.r`, `terrain_pred_prep.r`, `soil_pred_prep.r`, etc.

### Variables
- **snake_case** for local variables, function arguments, and config keys.
  - Examples: `model_dir`, `region_value`, `use_regions`, `periods_to_process`
- **SCREAMING_SNAKE_CASE** used selectively for "important" intermediate data frames/objects (pattern inherited from older code, not enforced):
  - Examples: `Prob_raster_stack`, `Current_interventions`, `Scenario_interventions` in `lulcc.spatprobmanipulation.r` and `implement_spatial_interventions.R`
- New code (post-refactor) uses consistent lowercase snake_case: `lulc_files`, `lulc_years`, `ref_grid`, `reg_vect`.

### Config keys
All configuration is accessed by string key from a flat list: `config[["transition_model_dir"]]`, `config[["data_periods"]]`. Keys use `snake_case`.

---

## Code Style

### Indentation and Spacing
- 2-space indentation (observed consistently across `src/utils.r`, `src/setup.r`, `src/transition_modelling.r`).
- Spaces around operators: `x <- y`, `if (cond)`, not `if(cond)`.
- Opening brace on the same line; closing brace on its own line.

### Line Length
- No enforced limit. In practice lines stay under ~90 characters for code; docstrings and comments can be longer.

### Assignment
- `<-` for assignment universally. `=` is used only for function argument defaults and named list elements.

### Pipes
- `magrittr` `%>%` pipe is re-exported via `src/utils-pipe.r` and used widely in data manipulation chains (`dplyr`, `arrow`, `tidymodels`).
- R 4.1+ native pipe `|>` is used in some newer sections (e.g., `src/transition_identification.r` line 80+). Both styles coexist.

### Function Signatures
- Default argument `config = get_config()` is the standard pattern for pipeline functions:
  ```r
  lulc_data_prep <- function(config = get_config(), refresh_cache = FALSE)
  region_prep <- function(config = get_config())
  transition_modelling <- function(config = get_config(), refresh_cache = FALSE, ...)
  ```
- Purely internal helpers have no default config arg.

### Guard Clauses / Validation
- `stopifnot()` for type assertions: `stopifnot(inherits(r, "SpatRaster"))` (`src/utils.r`).
- `stop()` with `sprintf()` for informative messages: `stop(sprintf("File not found: %s", path))`.
- `tryCatch()` used in data-loading helpers to catch and log errors without crashing the pipeline.

### Messaging / Logging
- Use `message()` for pipeline progress (captured in SLURM `.out` files).
- Use the custom `log_msg()` helper (`src/utils.r`) inside parallel workers — it prepends a timestamp and writes to both a per-worker log file and stdout.
- `cat()` is used in some older/script-level code (entry-point scripts in `scripts/`).
- Emoji characters appear in some `message()` calls (`"✅ Saved '...'"`) — acceptable but not required for new code.

### Sections / Comments
- Long pipeline functions use `###` header blocks:
  ```r
  ### =========================================================================
  ### A- Preparation
  ### =========================================================================
  ```
- Inline comments use `#` with a space after the hash.
- `#todo` (lowercase) appears as an inline annotation for known issues (e.g., `dinamica_utils.r` line 49).

---

## Documentation (Roxygen2)

- All exported functions have Roxygen2 docstrings. Format:
  ```r
  #' Title
  #'
  #' Description
  #'
  #' @param arg_name Type, description
  #' @return Description of return value
  #' @author Name
  #' @export
  ```
- Non-exported helpers often have abbreviated or absent Roxygen blocks.
- `@describeIn` used for grouping related utility functions under a single `@name` namespace in `src/utils.r`.
- `@examples` blocks use `\dontrun{}` for examples that require data files.

---

## Import Style

- R packages used via explicit namespace: `terra::rast()`, `dplyr::filter()`, `jsonlite::fromJSON()` — avoids polluting the global namespace.
- `magrittr::%>%` re-exported via `src/utils-pipe.r`.
- `data.table` NSE enabled via `.datatable.aware <- TRUE` in `src/utils.r`.
- No `library()` calls in `src/` — dependencies declared in `DESCRIPTION` `Imports:` field.
- Entry-point scripts (`scripts/run_*.r`) do load packages with `library()` and `require()`.

---

## Error Handling Patterns

- `tryCatch(expr, error = function(e) { ... })` for recoverable errors in loops.
- `stop()` with descriptive messages for fatal configuration or file-not-found errors.
- `try(expr, silent = TRUE)` used for optional detection (e.g., GDAL info parsing in `src/utils.r`).
- Parallel workers return structured empty tibbles on error rather than crashing the whole run.

---

## Caching Pattern

All major pipeline steps follow a cache-skip pattern:
```r
if (file.exists(output_path) && !refresh_cache) {
  message("Cache hit — skipping...")
  return(invisible(NULL))
}
```
The `refresh_cache` argument is standard on expensive pipeline functions.

---

## Configuration

- Config is loaded via `get_config()` (`src/setup.r`), which auto-detects local vs. HPC environment and reads `config/local_config.yaml` or `config/hpc_config.yaml`.
- All paths are derived from `config[["data_basepath"]]` at runtime — no hardcoded paths in `src/`.
- Environment variables used at runtime: `ALLOCATION_PROFILE`, `ALLOCATION_NUM_WORKERS`, `DINAMICA_EGO_8_HOME`, `SLURM_JOB_ID`.

---

## Gaps / Unknowns

- No `.lintr` config — linting rules are unenforced and may vary between contributors.
- No `styler` config or `.Rprofile` — code formatting is done manually/by editor.
- Mixed use of `|>` and `%>%` with no stated preference for new code.
- Inconsistent capitalization of variable names (PascalCase objects in older files vs. snake_case in newer files) — no convention written down.
- No formal code review checklist or contributing guide found.
