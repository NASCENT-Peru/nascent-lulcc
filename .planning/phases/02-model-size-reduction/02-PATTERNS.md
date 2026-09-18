# Phase 2: Model Size Reduction - Pattern Map

**Mapped:** 2026-05-07
**Files analyzed:** 6 (3 modified, 1 rewritten, 1 new, 1 env file)
**Analogs found:** 6 / 6

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `src/transition_modelling.r` | service (training pipeline) | batch | itself (current tidymodels version) | self-analog — rewrite inner functions, preserve outer shells |
| `src/allocation.r` | service (predict dispatcher) | request-response | `src/allocation.r` lines 540–613 (existing dispatch branches) | self-analog — add branch at top of existing dispatch chain |
| `scripts/retrain_all_models.r` | utility script (orchestrator) | batch | `scripts/run_transition_modelling.r` + `scripts/run_allocation.r` | role-match (entry-point script pattern) |
| `config/local_config.yaml` | config | — | itself (`configuration_settings:` block, lines 59–74) | self-analog — additive key insertion |
| `config/hpc_config.yaml` | config | — | itself (`configuration_settings:` block, lines 63–74) | self-analog — additive key insertion |
| `environments/allocation_env.yml` | config (env spec) | — | itself (MEM-06 block, lines 66–92) | self-analog — additive package insertion |

---

## Pattern Assignments

### `src/transition_modelling.r` (service, batch — full rewrite of inner functions)

**Analog:** itself — the outer function shells stay intact; only inner modelling functions are replaced.

**Outer shells to PRESERVE (do not modify signatures)** (lines 19–100, 455–821, 923–1141):
- `transition_modelling(config, refresh_cache, model_dir, eval_dir, use_regions, model_specs_path, periods_to_process)` — top-level entry; iterates periods
- `perform_transition_modelling(period, use_regions, config, ...)` — per-period orchestration with `furrr::future_map()` over `fs_summary` rows
- `model_single_transition(trans_name, refresh_cache, region, ...)` — per-transition worker; loads data, gets predictors, calls inner modelling

**Config resolution pattern** (lines 28–54 of transition_modelling.r — replicate for any new parameters):
```r
# Pattern: extract from config if caller did not supply
if (is.null(model_dir)) {
  model_dir <- config[["transition_model_dir"]]
  message(sprintf("Model directory set to: %s", model_dir))
}
```
New mlr3 functions must also read `max_training_rows` the same way:
```r
max_rows <- config[["max_training_rows"]] %||% 500000L
```

**`build_transition_model_path()` — current pattern** (lines 103–111):
```r
build_transition_model_path <- function(trans_name, region, model_dir) {
  region_suffix <- ifelse(
    is.null(region) || identical(region, "National extent"),
    "national",
    gsub(" ", "_", tolower(region))
  )
  file.path(model_dir, sprintf("%s_%s.rds", trans_name, region_suffix))
}
```
**Change required:** The `.rds` extension must become `.qs` for new mlr3 models. Either update this function to take an `ext` argument defaulting to `".qs"`, or create a parallel `build_mlr3_model_path()` that wraps it. Old dispatch branches continue loading `.rds` via `readRDS()`; the new mlr3 branch loads `.qs` via `qs::qread()`.

**Cache-skip pattern** (lines 950–977 — `model_single_transition()`):
```r
model_path <- build_transition_model_path(trans_name, region, model_dir)
if (!refresh_cache && file.exists(model_path)) {
  log_msg(sprintf(
    "  Model file already exists: %s (skipping - set refresh_cache=TRUE to overwrite)",
    model_path
  ), log_file)
  return(list(
    transition  = trans_name,
    region      = ifelse(is.null(region), "National extent", region),
    status      = "skipped_existing_model",
    model_path  = model_path,
    skipped     = TRUE,
    ...
  ))
}
```
The mlr3 rewrite must check for the `.qs` path (not `.rds`) in this skip condition.

**Subsampling pattern — EXISTING analog** (lines 2927–3001 of `fit_and_save_best_model()`):
```r
# Existing stratified subsampling: keep ALL minority, downsample majority
# (used in fit_and_save_best_model; move upstream to before Task creation)
response_table <- table(full_data_clean$response)
n_minority <- min(response_table)
n_majority <- max(response_table)
minority_class <- names(which.min(response_table))
majority_class <- names(which.max(response_table))

target_majority <- min(max_final_fit_size - n_minority, n_majority)

final_fit_data <- full_data_clean %>%
  dplyr::group_by(response) %>%
  {
    minority_data <- dplyr::filter(., response == minority_class)
    majority_data <- dplyr::filter(., response == majority_class) %>%
      dplyr::slice_sample(n = target_majority, replace = FALSE)
    dplyr::bind_rows(minority_data, majority_data)
  } %>%
  dplyr::ungroup()
```
For mlr3, apply before Task creation (not inside the fit call). Use `config[["max_training_rows"]]` instead of `max_final_fit_size`. Use base R `rbind()` instead of dplyr if running inside `furrr` workers where dplyr scoping can be fragile (RESEARCH Section 6 recommendation).

**Save pattern — mlr3 replacement for `save_minimal_model()`** (replaces lines 2150–2800):
```r
# mlr3 model object structure (D-05 contract)
model_obj <- list(
  model_type      = "mlr3",
  predictor_names = task$feature_names,  # use Task's view, not colnames()
  response_levels = task$class_names,
  learner         = lrn_fitted          # the inner Learner only, NOT the AutoTuner
)
qs::qsave(model_obj, output_path)       # output_path ends in .qs

# Size gate (D-12): warn, do not stop
size_bytes <- file.size(output_path)
if (size_bytes > 200 * 1024^2) {
  log_msg(sprintf(
    "WARNING: model file exceeds 200MB: %.1f MB — %s",
    size_bytes / 1024^2,
    output_path
  ), log_file)
}
```

**Parallel worker / clone pattern** (from `perform_transition_modelling()` lines 697–806 — each worker is a closure inside `furrr::future_map()`):
```r
# mlr3 Learners are R6 reference objects — always clone before training
# inside a furrr worker to avoid shared state between parallel workers.
lrn_fitted <- learner$clone(deep = TRUE)
lrn_fitted$train(task)
```

**Error return pattern** (lines 761–801 — what `model_single_transition()` returns on failure):
```r
return(list(
  transition    = trans_name,
  region        = region,
  status        = "error",
  error_message = conditionMessage(e),
  cv_metrics    = NULL,
  test_metrics  = NULL
))
```
All new mlr3 training functions must return this same structure on error so `perform_transition_modelling()`'s result aggregation (`results_summary <- purrr::map_dfr(...)` at line 810) continues to work unchanged.

---

### `src/allocation.r` — `predict_saved_transition_prob()` (predict dispatcher, request-response)

**Analog:** itself — lines 501–701 (the existing dispatch function). The mlr3 branch is a new first-check inserted BEFORE the existing `grepl("^tidypredict_", ...)` check at line 541.

**Exact insertion point:** After the `predictor_names` validation block (line 537 closing brace), before the `tidypredict_` branch (line 540). The new block should open with `if (!is.null(model_obj$model_type) && model_obj$model_type == "mlr3")`.

**Existing dispatch branch pattern to copy** (lines 577–613, butchered branch):
```r
if (
  !is.null(model_obj$model_type) &&
    grepl("^butchered_", model_obj$model_type)
) {
  log_msg(
    sprintf(
      "        Path: butchered (model_type='%s'); subsetting + baking predictors",
      model_obj$model_type
    ),
    log_file
  )
  new_data_processed <- subset_saved_transition_data(
    new_data,
    predictor_names
  )
  preprocessed_data <- tryCatch(
    recipes::bake(model_obj$recipe, new_data_processed),
    error = function(e) {
      log_and_stop(sprintf(
        "recipes::bake() failed for butchered model: %s",
        conditionMessage(e)
      ))
    }
  )
  model_obj$model <- restore_ranger_importance_mode(model_obj$model)
  result <- tryCatch(
    predict_saved_butchered_prob(model_obj, preprocessed_data),
    error = function(e) {
      log_and_stop(sprintf(
        "predict_saved_butchered_prob() failed: %s",
        conditionMessage(e)
      ))
    }
  )
  log_msg("        Path: butchered — prediction complete", log_file)
  return(result)
}
```

**mlr3 branch to add (following the same structural template):**
```r
if (!is.null(model_obj$model_type) && model_obj$model_type == "mlr3") {
  log_msg("        Path: mlr3 learner; predict_newdata()", log_file)
  new_data_subset <- subset_saved_transition_data(new_data, predictor_names)
  pred <- tryCatch(
    model_obj$learner$predict_newdata(newdata = new_data_subset),
    error = function(e) log_and_stop(sprintf(
      "mlr3 predict_newdata() failed: %s", conditionMessage(e)
    ))
  )
  # pred$prob is a matrix; columns are named by class label ("0", "1")
  # Always index by name, not position (Risk 6 in RESEARCH)
  prob_1 <- pred$prob[, "1"]
  result <- data.frame(.pred_0 = 1 - prob_1, .pred_1 = prob_1)
  log_msg("        Path: mlr3 — prediction complete", log_file)
  return(result)
}
```

**Helper patterns that the mlr3 branch reuses unchanged:**
- `log_and_stop()` (lines 506–509) — defined inside `predict_saved_transition_prob()`, available to all branches
- `subset_saved_transition_data()` (lines 382–388) — subsets `new_data` to `predictor_names` columns; handles data.table and data.frame
- `get_saved_transition_predictors()` (lines 360–380) — already reads `model_obj$predictor_names` first; mlr3 objects carry this field explicitly (D-05) so no changes needed

**Output convention (lines 425, 443, 490 — how all existing branches format output):**
```r
# All branches return this exact structure:
data.frame(.pred_0 = 1 - prob_1, .pred_1 = prob_1)
# Caller at line 1601 takes pred_result[[2L]] as the transition probability
```

---

### `scripts/retrain_all_models.r` (utility script, batch — new file)

**Analog 1:** `scripts/run_transition_modelling.r` (lines 1–215) — closest match for the script skeleton (shebang, start time, wd setup, src sourcing, get_config(), tryCatch wrapper, summary file write).

**Analog 2:** `scripts/run_allocation.r` (lines 1–294) — shows the modern pattern for argument parsing, no `install.packages()` at runtime, and pre-flight style validation.

**Script skeleton pattern** (from `run_transition_modelling.r` lines 1–140):
```r
#!/usr/bin/env Rscript
# retrain_all_models.r
# Re-train all transition models using the new mlr3 pipeline.
# Usage: Rscript scripts/retrain_all_models.r [--force]

start_time <- Sys.time()

cat("\n========================================\n")
cat("Re-training All Transition Models (mlr3)\n")
cat("========================================\n\n")

# Argument parsing (run_allocation.r lines 30-48 pattern)
.cli_args <- commandArgs(trailingOnly = TRUE)
force_retrain <- "--force" %in% .cli_args

# Working directory setup (run_transition_modelling.r lines 102-113)
script_path <- commandArgs(trailingOnly = FALSE)
script_path <- script_path[grepl("--file=", script_path)]
if (length(script_path) > 0) {
  script_dir <- dirname(sub("--file=", "", script_path))
  project_root <- dirname(script_dir)
} else {
  project_root <- getwd()
  if (basename(project_root) == "scripts") project_root <- dirname(project_root)
}
setwd(project_root)

# Source files (run_transition_modelling.r lines 117-135 pattern)
# NOTE: Do NOT call install.packages() — see run_allocation.r comment lines 82-87
src_files <- c("src/setup.r", "src/utils.r", "src/transition_modelling.r")
for (src_file in src_files) {
  tryCatch(source(src_file), error = function(e) {
    cat(sprintf("ERROR sourcing %s: %s\n", src_file, e$message))
    quit(status = 1)
  })
}

# Config (run_transition_modelling.r lines 140-148)
config <- tryCatch(get_config(), error = function(e) {
  cat(sprintf("ERROR getting config: %s\n", e$message))
  quit(status = 1)
})
```

**Viable transitions loop pattern** (from `perform_transition_modelling()` lines 593–607 — how the existing code reads viable_transitions_lists.csv):
```r
# The existing reconcile_period_transitions() in transition_modelling.r already
# reads viable_transitions_lists.csv and filters it. retrain_all_models.r should
# call transition_modelling() directly (which calls perform_transition_modelling()
# which calls reconcile_period_transitions() internally).
# This matches D-08 recommendation: "call transition_modelling() directly".

transition_modelling(
  config        = config,
  refresh_cache = force_retrain   # TRUE when --force flag passed
)
```

**Summary file pattern** (from `run_transition_modelling.r` lines 186–213):
```r
end_time <- Sys.time()
elapsed <- difftime(end_time, start_time, units = "hours")
cat(sprintf("Total runtime: %.2f hours\n", as.numeric(elapsed)))

summary_file <- file.path(
  config[["transition_model_eval_dir"]],
  sprintf("retrain_summary_%s.txt", Sys.getenv("SLURM_JOB_ID", unset = "local"))
)
if (!dir.exists(dirname(summary_file))) dir.create(dirname(summary_file), recursive = TRUE)
sink(summary_file)
cat(sprintf("Job ID: %s\n", Sys.getenv("SLURM_JOB_ID", unset = "local")))
cat(sprintf("Start: %s\n", start_time))
cat(sprintf("End:   %s\n", end_time))
cat(sprintf("Runtime: %.2f hours\n", as.numeric(elapsed)))
sink()
quit(status = 0)
```

---

### `config/local_config.yaml` (config, additive change)

**Analog:** itself — the `configuration_settings:` block at lines 59–74.

**Exact block structure** (lines 59–74):
```yaml
# Calibration/simulation configurations
configuration_settings:
  ref_grid_target_cellsize: 100  # in meters
  reference_crs: "epsg:2056"
  step_length: 4
  data_periods: ["2018_2022"] #"2010_2014", "2014_2018", 
  regionalization: true
  scenario_names: ["BAU", "NAT", "CUL", "SOC"]
  scenario_to_ssp_mapping: 
    BAU: "ssp245"
    NAT: "ssp126"
    CUL: "ssp126"
    SOC: "ssp126"
  simulation_start_year: 2022
  simulation_end_year: 2060
  selected_scalar: 9.0
```

**Key to insert** (after the last existing key in `configuration_settings`, before the blank line that precedes `simulation_trans_rates_params:`):
```yaml
  max_training_rows: 500000  # Subsampling ceiling for transition model training (Phase 2, D-11)
```
Note: two-space indent matching all other keys in the block. No quotes needed for integers in this project's YAML style. Inline comment follows the `# comment text` convention seen throughout both config files.

---

### `config/hpc_config.yaml` (config, additive change)

**Analog:** itself — the `configuration_settings:` block at lines 63–74.

**Identical block structure** (lines 63–74 — same indentation convention as local_config.yaml):
```yaml
configuration_settings:
  ref_grid_target_cellsize: 100  # in meters
  reference_crs: "epsg:2056"
  step_length: 4
  data_periods: ["2018_2022"]
  regionalization: true
  scenario_names: ["BAU", "NAT", "CUL", "SOC"]
  ...
  selected_scalar: 9.0
```

**Key to insert** (same position as local_config.yaml — after `selected_scalar` line, before blank line):
```yaml
  max_training_rows: 1000000  # Higher threshold on HPC nodes; operator should tune to actual node RAM (Phase 2, D-11)
```

---

### `environments/allocation_env.yml` (env config, additive change)

**Analog:** itself — the MEM-06 comment block and package list at lines 66–92.

**Existing package listing pattern** (lines 66–92):
```yaml
  # ---------------------------------------------------------------------------
  # MEM-06: prediction-time package set
  # ---------------------------------------------------------------------------
  # These packages are required for Stage 7 to load and execute the
  # tidymodels-trained transition probability models without runtime
  # `install.packages()` calls. Order matches REQUIREMENTS.md MEM-06.

  # tidymodels engines used at prediction time
  - r-parsnip
  - r-recipes
  - r-workflows
  - r-ranger
  - r-xgboost=1.7
  - r-tidypredict

  # Model object slimming + (de)serialisation
  - r-butcher
  - r-bundle
  - r-qs

  # Process/memory introspection (cross-platform RSS reporting; OBS-* uses ps)
  - r-ps
  - r-lobstr

  # BLAS thread control inside parallel workers
  - r-rhpcblasctl
```

**Block to insert** (append after the `r-rhpcblasctl` line, before EOF, following the same section comment style):
```yaml
  # ---------------------------------------------------------------------------
  # Phase 2 (MEM-04): mlr3 training and prediction packages
  # ---------------------------------------------------------------------------
  # Required for loading and calling $predict_newdata() on mlr3 Learner objects
  # saved by the rewritten transition_modelling.r. The r-xgboost=1.7 pin above
  # must remain unchanged — mlr3learners 0.14 supports xgboost 1.7 (RESEARCH §5).
  - r-mlr3
  - r-mlr3learners
  - r-mlr3tuning
  - r-paradox
  - r-bbotk
  - r-glmnet          # required by classif.glmnet (regularised GLM, matches current behaviour)
```
Note: `r-xgboost=1.7` and `r-ranger` are already present (lines 78, 77). Do NOT add duplicate entries or change the xgboost pin. The existing tidymodels packages (`r-parsnip`, `r-recipes`, etc.) must stay because old `.rds` predict branches remain active (D-04/D-06).

---

## Shared Patterns

### Logging
**Source:** `src/utils.r` → `log_msg()` (used throughout both `allocation.r` and `transition_modelling.r`)
**Apply to:** All new functions in `transition_modelling.r` and the new branch in `allocation.r`

Pattern (as used at `allocation.r` lines 511–517):
```r
log_msg(
  sprintf("        predict_saved_transition_prob: starting (n_rows=%d)", NROW(new_data)),
  log_file
)
```
Four-space indent for top-level progress; eight-space (eight spaces) for sub-steps inside dispatch branches. `log_file = NULL` is always the default so the function works without a log.

### Null-coalesce operator
**Source:** `allocation.r` line 332 (local to that file); same idiom used in `transition_modelling.r` via `rlang` or defined locally
```r
`%||%` <- function(x, y) if (is.null(x) || (is.atomic(x) && length(x) == 0L)) y else x
```
Used for config key fallbacks: `config[["max_training_rows"]] %||% 500000L`. Define at the top of any new file that needs it if not already defined.

### Config key access pattern
**Source:** `transition_modelling.r` lines 28–54; `allocation.r` (implicitly)
```r
# All config keys are flattened to a single-level list by build_full_config().
# Access pattern (no nested `$` chains):
config[["max_training_rows"]]
config[["transition_model_dir"]]
config[["regionalization"]]
```

### Error handling in workers
**Source:** `perform_transition_modelling()` lines 741–801
```r
tryCatch(
  withCallingHandlers(
    { ... main call ... },
    error = function(e) { captured_trace <<- sys.calls() }
  ),
  error = function(e) {
    log_msg(sprintf("ERROR: %s", conditionMessage(e)), log_file)
    list(transition = trans_name, region = region, status = "error",
         error_message = conditionMessage(e), ...)
  }
)
```
New mlr3 training functions must return an error list (not `stop()`) when running inside `furrr::future_map()` workers.

### `ensure_dir()` for output directories
**Source:** `transition_modelling.r` lines 57–58; `perform_transition_modelling()` line 480
```r
ensure_dir(model_dir)
ensure_dir(eval_dir)
```
Call before any `qs::qsave()` or `saveRDS()` to guarantee the target directory exists.

---

## No Analog Found

All six files have analogs. No files require falling back to RESEARCH.md patterns exclusively — though the mlr3 API patterns (Task creation, AutoTuner, `$predict_newdata()`) have no codebase analog and must be implemented from scratch following RESEARCH.md Section 2 code examples.

---

## Key Landmines for Planner

The following are the highest-risk integration points extracted from RESEARCH.md Section 8 that the planner's action steps must explicitly address:

1. **File extension (.rds → .qs):** `build_transition_model_path()` currently returns `.rds` (line 110). The cache-skip check in `model_single_transition()` (line 953) and the allocation.r loader must use the new `.qs` path for mlr3 models. Old `.rds` files remain loadable via `readRDS()` in the existing branches.

2. **Store `at$learner`, not the AutoTuner:** `qs::qsave(at, path)` would capture the full tuning history (~5× larger). Always extract `at$learner` before saving.

3. **ranger `save.memory = TRUE`:** Without this flag, the OOB predictions matrix bloats the saved file to >1 GB. This is the primary size gate mechanism.

4. **`pred$prob[, "1"]` by name:** Using positional indexing `pred$prob[, 2L]` is fragile if factor level ordering changes. Always use named column.

5. **Clone before training in workers:** mlr3 Learners are R6 reference objects. `learner$clone(deep = TRUE)` is mandatory inside each `furrr::future_map()` worker.

6. **`positive = "1"` in Task creation:** Must be explicit so the probability matrix columns are oriented correctly.

---

## Metadata

**Analog search scope:** `src/`, `scripts/`, `config/`, `environments/`
**Files read:** `src/allocation.r` (lines 330–701), `src/transition_modelling.r` (lines 1–130, 455–850, 923–1141, 2150–3001), `scripts/run_transition_modelling.r` (full), `scripts/run_allocation.r` (full), `scripts/run_feature_selection.r` (full), `config/local_config.yaml` (full), `config/hpc_config.yaml` (full), `environments/allocation_env.yml` (full)
**Pattern extraction date:** 2026-05-07
