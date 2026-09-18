# Phase 2: Model Size Reduction — Research

**Researched:** 2026-05-07
**Domain:** mlr3 migration, R model serialisation, binary classification
**Confidence:** HIGH (standard stack), MEDIUM (XGBoost marshaling details), HIGH (codebase analysis)

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Full replacement of tidymodels in `transition_modelling.r`. mlr3 is used for tuning, final fit, and model serialisation. Tidymodels code is removed (not kept in parallel).
- **D-02:** Use evoland-plus mlr3 branch as a reference only — read its patterns and adapt them to nascent-lulcc's config/path contract. Do not copy verbatim.
- **D-03:** Keep the same algorithm family (GLM, RF/ranger, XGBoost). Structural replacement, not algorithm change.
- **D-04:** Add `model_type = "mlr3"` as a new dispatch branch in `predict_saved_transition_prob()` (src/allocation.r). Existing tidypredict/butchered branches stay.
- **D-05:** mlr3 model objects saved via `qs::qsave()`. Each file must be <200 MB. Saved object must carry `model_type = "mlr3"` and `predictor_names`.
- **D-06:** Old predict branches may be removed in a future cleanup — out of scope for Phase 2.
- **D-07:** Re-train all transitions from scratch. Existing RDS files become obsolete.
- **D-08:** `scripts/retrain_all_models.r` orchestrates re-training of all transitions across all regions, consuming same config/env contract from Phase 1. Replaces `rebutcher_existing_models.r`.
- **D-09:** For transitions whose training dataset exceeds `max_training_rows`, stratified subsampling is applied before tuning and final fit.
- **D-10:** Subsampling is stratified by the binary response variable. Random seed from existing config seed.
- **D-11:** `max_training_rows` is operator-configurable in YAML (suggested default ~500,000).
- **D-12:** Size gate: after saving, assert `file.size(output_path) < 200 * 1024^2`. Log a warning (not stop) if exceeded.
- **D-13:** Predict equality check: for each saved model, run 5-row predict on fixture data, assert probabilities in [0, 1] and non-NA.

### Claude's Discretion
None documented.

### Deferred Ideas (OUT OF SCOPE)
- Removing old tidypredict/butchered predict branches from `allocation.r`
- Block-wise `terra::predict()` for per-transition RAM bounds (Phase 4)
- Full unit/integration test suite (v2)
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| MEM-04 | All model objects saved by `transition_modelling.r` are <200 MB each | mlr3 Learner objects with `save.memory=TRUE` for ranger and `qs::qsave()` compression achieve this; XGBoost serializes without external pointers in mlr3 0.14+ |
</phase_requirements>

---

## Summary

**Key findings:**

1. **mlr3 workflow is Task → Learner → AutoTuner → `$train()` → `$predict_newdata()`**. The AutoTuner wraps the learner, tunes on CV folds, then fits the tuned learner on the full training data in one `$train()` call. No explicit `finalize_workflow()` step is needed — the final fit is automatic.

2. **The evoland-plus reference uses `qs2::qs_serialize()` to store Learners as BLOBs in a database.** For nascent-lulcc we use `qs::qsave()` (already in env) to write the Learner to a `.qs` file. The file extension changes from `.rds` to `.qs`; the `build_transition_model_path()` function must be updated accordingly.

3. **`predict_newdata(newdata)` returns a `PredictionClassif` R6 object.** Probabilities are extracted via `$prob` (a numeric matrix with columns named by class label). The dispatcher in `allocation.r` must extract `pred$prob[, "1"]` (or the positive class column) to get the probability vector, then wrap in a `data.frame(.pred_0 = ..., .pred_1 = ...)` to match the existing caller convention.

4. **XGBoost in mlr3learners 0.14 does NOT have the "marshal" property** — it stores the booster as a wrapped R object and serialises directly via `qs::qsave()` without special marshal/unmarshal calls. This is different from older docs. The `r-xgboost=1.7` pin must match between training and prediction environments.

5. **ranger with `save.memory = TRUE` and `importance = "none"` dramatically shrinks model footprint** by suppressing the OOB predictions matrix and importance scores that dominated the old tidymodels models. The `restore_ranger_importance_mode()` workaround in `allocation.r` is not needed for mlr3-trained models.

6. **Scope: ~140–160 transition-region pairs across 4 regions** (Andes, Costa Peruana, Cuenca del Amazonas, Selva Andina), each modelled for period `2018_2022`. `retrain_all_models.r` loops over all viable transition-region pairs from `viable_transitions_lists.csv` filtered to non-self, non-NA `rate_2018_2022` rows and `region_name != "whole_map"`.

**Primary recommendation:** Replace the full `multi_spec_trans_modelling()` + `fit_and_save_best_model()` + `save_minimal_model()` stack with a clean mlr3 pipeline: TaskClassif → AutoTuner (or direct Learner) → `$train()` on full data → `qs::qsave()` of the trained Learner wrapped in a named list that carries `model_type`, `predictor_names`, and `response_levels`.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Transition model training (tuning + fit) | `src/transition_modelling.r` | `config/model_specs.yaml` | Training logic entirely in modelling script; YAML drives hyperparameter grid |
| Subsampling fallback | `src/transition_modelling.r` | `config/local_config.yaml`, `config/hpc_config.yaml` | Applied before Task creation; threshold from config |
| Model serialisation | `src/transition_modelling.r` | `qs` package | Save at end of each transition's pipeline |
| Prediction dispatch | `src/allocation.r` | `src/transition_modelling.r` (save format contract) | `predict_saved_transition_prob()` dispatcher reads `model_type` field |
| Re-training orchestration | `scripts/retrain_all_models.r` | `src/setup.r` (config), `src/transition_modelling.r` (model logic) | Utility script loops transitions/regions |
| Environment specification | `environments/allocation_env.yml` | — | Adds mlr3 packages; removes or keeps tidymodels packages |

---

## 1. evoland-plus MLR3 Reference Patterns

[CITED: https://github.com/ethzplus/evoland-plus/tree/copilot/integrate-mlr3-library]

The reference codebase (`R/trans_models_t.R`) demonstrates the following patterns that are relevant for adaptation:

### Task Creation
```r
# evoland-plus pattern — adapt target column name to "response"
task <- mlr3::as_task_classif(
  data,           # data.table or data.frame with target + predictors
  target = "did_transition",
  positive = "TRUE"
)
# Set target as stratum for stratified resampling
task$set_col_roles(task$target_names, add_to = "stratum")
```

For nascent-lulcc: target column is `"response"` (factor with levels `"0"` and `"1"`), positive class is `"1"`.

### Two-Mode Final Model Fitting (adapt the "Direct mode")
The reference has two modes. The relevant one: clone the learner and train directly on the full task:
```r
trained_learner <- learner$clone(deep = TRUE)
trained_learner$train(full_task)  # or with row_ids for partial fits
```

### Serialisation
The reference uses `qs2::qs_serialize()` to store learners as raw BLOBs in a SQLite database. **nascent-lulcc uses `qs::qsave()`** (already pinned in `allocation_env.yml`) — equivalent but file-based. The trained Learner is wrapped in a named list before saving:
```r
model_obj <- list(
  model_type      = "mlr3",
  predictor_names = task$feature_names,
  response_levels = task$class_names,
  learner         = trained_learner
)
qs::qsave(model_obj, output_path)
```

### Learner Construction with AutoTuner
```r
learner_at <- mlr3tuning::auto_tuner(
  tuner     = mlr3tuning::tnr("grid_search"),
  learner   = lrn("classif.ranger", predict_type = "prob"),
  resampling = mlr3::rsmp("cv", folds = 3),
  measure   = mlr3::msr("classif.auc"),
  search_space = paradox::ps(
    num.trees = paradox::p_int(lower = 100, upper = 500)
  ),
  term_evals = 9L   # grid size
)
# train on task — AutoTuner handles CV tuning + final fit in one call
learner_at$train(task)
# Access inner trained learner:
final_learner <- learner_at$learner
```

---

## 2. MLR3 API Patterns for This Use Case

[VERIFIED: mlr3 docs, mlr3tuning docs, mlr3learners docs — via WebFetch and WebSearch]

### Task Creation (binary classification)
```r
library(mlr3)
library(mlr3learners)
library(mlr3tuning)
library(paradox)

# transition_data has column "response" (factor "0"/"1") + predictor columns
task <- as_task_classif(transition_data, target = "response", positive = "1")
# Enable stratified resampling on the response
task$col_roles$stratum <- "response"
```

### GLM Learner (logistic regression via stats::glm)
```r
# classif.log_reg: no hyperparameters to tune; no normalization needed
lrn_glm <- lrn("classif.log_reg", predict_type = "prob")
lrn_glm$train(task)
```

Note: mlr3's `classif.log_reg` calls `stats::glm(family = "binomial")` — not glmnet. The current code uses glmnet (`penalty`, `mixture`). If regularised logistic regression is required, use `classif.glmnet` instead:
```r
lrn_glm <- lrn("classif.glmnet",
  predict_type = "prob",
  alpha = to_tune(0, 1),
  s     = to_tune(1e-4, 1, logscale = TRUE)
)
```
[ASSUMED] Whether to use `classif.log_reg` (plain GLM) or `classif.glmnet` (regularised) is a modelling decision. The existing `model_specs.yaml` uses glmnet with `penalty=0.01, mixture=1` (Lasso). The planner should confirm whether to preserve glmnet or switch to plain GLM.

### Ranger Learner (random forest)
```r
lrn_rf <- lrn("classif.ranger",
  predict_type   = "prob",
  num.trees      = to_tune(c(100L, 300L, 500L)),
  mtry           = to_tune(2L, p_int_upper),  # finalized from task
  min.node.size  = to_tune(c(1L, 5L, 10L)),
  importance     = "none",    # suppress importance to save memory
  save.memory    = TRUE,      # suppress OOB predictions matrix
  num.threads    = 1L         # required for parallel workers
)
```

**Memory note:** `save.memory = TRUE` suppresses the OOB predictions matrix — the primary cause of large ranger models in the current tidymodels stack. `importance = "none"` avoids storing the importance vector. These two flags together are the primary mechanism for achieving <200 MB files. [VERIFIED: mlr3learners classif.ranger docs]

### XGBoost Learner
```r
lrn_xgb <- lrn("classif.xgboost",
  predict_type     = "prob",
  nrounds          = to_tune(c(50L, 100L, 200L)),
  eta              = to_tune(c(0.05, 0.1, 0.3)),
  max_depth        = to_tune(c(4L, 6L, 8L)),
  min_child_weight = to_tune(c(1L, 5L)),
  nthread          = 1L
)
```

**XGBoost parameter mapping from current model_specs.yaml:**
| tidymodels param | mlr3 param |
|-----------------|------------|
| `trees` | `nrounds` |
| `tree_depth` | `max_depth` |
| `learn_rate` | `eta` |
| `min_n` | `min_child_weight` |
| `mtry` | `colsample_bytree` (fraction, not integer count) |

[ASSUMED] The `mtry` → `colsample_bytree` mapping requires confirming how the current grid values (e.g., `mtry = 4`) map to fractions for XGBoost.

### AutoTuner Pattern (replaces tune_model + finalize_workflow)
```r
at <- mlr3tuning::auto_tuner(
  tuner      = mlr3tuning::tnr("grid_search"),
  learner    = lrn_rf,
  resampling = mlr3::rsmp("cv", folds = 3L),
  measure    = mlr3::msr("classif.auc"),
  # search_space is read from to_tune() in learner$param_set
  term_evals = as.integer(grid_size)
)

# One call: tunes internally, then fits best params on full task
at$train(task)

# After training:
at$tuning_result            # data.table of best params + score
at$learner                  # the inner Learner fitted on full data
at$learner$model            # the underlying model object (ranger, xgb.Booster, etc.)
```

**Critical difference from tidymodels:** AutoTuner does the tuning CV on the passed task (training data), then immediately fits the best-params learner on ALL rows of the task. There is no separate `parsnip::fit(final_wf, data = full_data)` call. The subsampling fallback (D-09) must be applied BEFORE creating the task.

### Predict Interface
```r
# After loading with qs::qread():
model_obj <- qs::qread(path)
learner   <- model_obj$learner

# predict_newdata() takes a plain data.frame/data.table (no Task needed)
new_dt <- new_data[, model_obj$predictor_names, drop = FALSE]
pred   <- learner$predict_newdata(newdata = new_dt)

# Extract probability of positive class ("1")
prob_1 <- pred$prob[, "1"]
prob_0 <- 1 - prob_1

# Return in the convention expected by predict_saved_transition_prob() callers
data.frame(.pred_0 = prob_0, .pred_1 = prob_1)
```

`predict_newdata()` does NOT require a Task object — it accepts a plain data.frame. [VERIFIED: mlr3 Learner docs]

### Anti-Patterns to Avoid
- **Do not call `learner$predict(task)`** — this requires a Task object and is for evaluation, not deployment. Use `predict_newdata()` for inference.
- **Do not store the AutoTuner itself** — store `at$learner` (the inner fitted Learner) to avoid serialising the full tuning history. The AutoTuner object is ~3-5× larger than just the Learner.
- **Do not set `importance = "impurity"` for ranger** — the importance vector is large and irrelevant at prediction time.

---

## 3. Current transition_modelling.r Structure

[VERIFIED: direct code reading of src/transition_modelling.r (3,569 lines)]

### Entry Points
| Function | Role |
|----------|------|
| `transition_modelling()` | Top-level entry; iterates periods → `perform_transition_modelling()` |
| `perform_transition_modelling()` | Per-period orchestration; parallel over `furrr::future_map()` → `model_single_transition()` |
| `model_single_transition()` | Loads data, gets predictors from FS summary, calls `multi_spec_trans_modelling()` + `fit_and_save_best_model()` |
| `multi_spec_trans_modelling()` | Loops replicates × model types; calls `fit_model_with_tuning()` for each combination |
| `fit_model_with_tuning()` | Creates recipe + parsnip spec + workflow; calls `tune_model()`; fits final on train split |
| `fit_and_save_best_model()` | Selects best model by test metric; creates a new workflow with best params; fits on full data (with sampling if needed); calls `save_minimal_model()` |
| `save_minimal_model()` | Attempts tidypredict, falls back to butcher + manual cleanup; saves via `saveRDS(..., compress = "xz")` |

### Algorithms Trained
Three model types, driven by `config/model_specs.yaml`:
- **GLM:** `logistic_reg(penalty, mixture)` via `glmnet` engine
- **RF:** `rand_forest(mtry, trees, min_n)` via `ranger` engine with `importance = "impurity"`
- **XGBoost:** `boost_tree(mtry, trees, min_n, tree_depth, learn_rate)` via `xgboost` engine

Currently configured with single-value grids (no actual tuning sweep), 2 replicates, 3-fold CV.

### Training Data Structure
- **Response column:** `response` — factor with levels `"0"` (no transition) and `"1"` (transition)
- **Predictor columns:** numeric predictors (selected by feature selection); count varies per transition (from FS output)
- **Size:** ranges from small (Andes mining transitions) to large (Amazon forest transitions potentially millions of rows)
- **Preprocessing in current stack:** `step_normalize()` + `step_zv()` via recipes

**Critical:** mlr3's `classif.ranger` and `classif.xgboost` do NOT require normalisation — these are tree-based models. Only `classif.glmnet` (if used) benefits from normalisation, and mlr3 handles it differently (via `mlr3pipelines` PipeOp). The recipes step can be dropped for RF and XGBoost, but should be kept for GLM if using glmnet.

### Current Save Format
Files are saved as `.rds` (compressed with `xz`) via `saveRDS()`. The saved object is a named list:
```r
list(
  model_type      = "tidypredict_glm" | "tidypredict_rf" | "butchered_glm" | "butchered_rf" | "butchered_xgboost",
  predictor_names = character vector,
  recipe          = trained recipes object,
  model           = parsnip model_fit or serialized xgb booster,
  response_levels = c("0", "1"),
  ...
)
```

The `model_type` field drives dispatch in `predict_saved_transition_prob()`.

### File Path Convention
```r
build_transition_model_path(trans_name, region, model_dir)
# → model_dir/<period>/<trans_name>_<region_suffix>.rds
# e.g.: outputs/transition_models/2018_2022/forested_areas-built_up_and_barren_lands_andes.rds
```

**The `.rds` extension must change to `.qs` for mlr3 models**, or the function must be updated to support both (for backward compatibility with existing `.rds` files loaded by old predict branches).

### Current Subsampling Logic
`fit_and_save_best_model()` already has a `max_final_fit_size` parameter (from `model_specs.yaml: global.max_final_fit_size: 100000`) that performs stratified sampling on the full data before the final fit. This is the pattern D-09 requires, but it needs to move upstream (before Task creation, not after) and read `max_training_rows` from the top-level config.

### Scale: Transitions × Regions
From `viable_transitions_lists.csv`: 4 regions (Andes, Costa Peruana, Cuenca del Amazonas, Selva Andina) × ~42 LULC class transitions = ~168 transition-region model files. Not all will have valid feature selection outputs, so the actual model count is somewhat less.

---

## 4. Predict Dispatcher Analysis

[VERIFIED: direct code reading of src/allocation.r lines 330–700]

### Current Dispatch Logic in `predict_saved_transition_prob()`
The function at line 501 dispatches via `model_obj$model_type` string, following this priority order:

| Priority | Condition | Branch |
|----------|-----------|--------|
| 1 | `grepl("^tidypredict_", model_obj$model_type)` | `predict_saved_tidypredict_prob()` — evaluates R expressions |
| 2 | `grepl("^butchered_", model_obj$model_type)` | `predict_saved_butchered_prob()` — calls `predict(inner_model, ...)` via parsnip |
| 3 | `is_minimal_saved_transition_model(model_obj)` | Legacy minimal format (recipe + model fields) |
| 4 | `is.list(model_obj) && "model" %in% names(model_obj)` | Legacy saved-list format |
| 5 | `inherits(model_obj, "workflow")` | Full workflow (oldest format) |

### Where to Add the mlr3 Branch
The mlr3 branch should be the FIRST check (before existing branches) since it has an unambiguous dispatch key:
```r
# Add at the TOP of the dispatch logic in predict_saved_transition_prob():
if (!is.null(model_obj$model_type) && model_obj$model_type == "mlr3") {
  log_msg("        Path: mlr3 learner; predict_newdata()", log_file)
  new_data_subset <- subset_saved_transition_data(new_data, predictor_names)
  pred <- tryCatch(
    model_obj$learner$predict_newdata(newdata = new_data_subset),
    error = function(e) log_and_stop(sprintf(
      "mlr3 predict_newdata() failed: %s", conditionMessage(e)
    ))
  )
  # pred$prob is a matrix with columns named by class label
  prob_1 <- pred$prob[, "1"]
  result <- data.frame(.pred_0 = 1 - prob_1, .pred_1 = prob_1)
  log_msg("        Path: mlr3 — prediction complete", log_file)
  return(result)
}
```

### Input/Output Contract
- **Input `new_data`:** `data.frame` or `data.table` with at minimum all columns in `predictor_names`. The dispatcher already handles column subsetting via `subset_saved_transition_data()`.
- **Output:** `data.frame` with columns `.pred_0` (prob of class "0") and `.pred_1` (prob of class "1").

The caller (`run_allocation_for_scenario` → worker code) takes `pred_result[[2L]]` (the second column, `.pred_1`) as the transition probability. This convention must be preserved.

### `get_saved_transition_predictors()` Compatibility
The existing helper reads `model_obj$predictor_names` first, then falls back to recipe-based extraction. The mlr3 saved object carries `predictor_names` explicitly (D-05), so this helper works without modification.

---

## 5. Conda Package Requirements

[VERIFIED: anaconda.org/conda-forge — as of 2026-05-07]

### Required New Packages

| Package | conda-forge name | Version (current) | Purpose |
|---------|-----------------|-------------------|---------|
| mlr3 core | `r-mlr3` | 1.6.0 | Task, Learner, resampling |
| mlr3 learners | `r-mlr3learners` | 0.14.0 | classif.log_reg, classif.glmnet, classif.ranger, classif.xgboost |
| mlr3 tuning | `r-mlr3tuning` | 1.6.0 | AutoTuner, grid search, tnr(), msr() |
| paradox | `r-paradox` | 1.0.1 | ParamSet, to_tune(), p_int(), p_dbl() |
| bbotk | `r-bbotk` | 1.10.0 | Auto-dependency of mlr3tuning; Terminator |

### Optional (assess need)
| Package | conda-forge name | Purpose | Decision |
|---------|-----------------|---------|----------|
| mlr3pipelines | `r-mlr3pipelines` | Pre-processing PipeOps (normalisation for GLM) | [ASSUMED] NOT required if normalisation is dropped for tree models and GLM uses plain `classif.log_reg`. Only needed if `classif.glmnet` requires normalisation step. |

### Packages to Remove (or retain for backward compat)
The existing tidymodels packages (`r-parsnip`, `r-recipes`, `r-workflows`, `r-tidypredict`, `r-butcher`, `r-bundle`) are needed for the existing predict branches (D-04: existing branches stay). They must remain in `allocation_env.yml` until all transitions are re-trained and old branches are cleaned up (Phase 2 scope ends at adding the mlr3 branch).

### XGBoost Version Constraint
`r-xgboost=1.7` is already pinned in `allocation_env.yml`. `r-mlr3learners` 0.14.0 supports xgboost 1.7. **Do not change the xgboost pin** — the trained model files must be loadable at prediction time with the same version. [VERIFIED: allocation_env.yml pin, mlr3learners changelog]

### Exact Additions to `allocation_env.yml`
```yaml
  # mlr3 packages (Phase 2 — MEM-04)
  - r-mlr3
  - r-mlr3learners
  - r-mlr3tuning
  - r-paradox
  - r-bbotk
```

Note: `r-mlr3` has a dependency on `r-data.table` (already present) and `r-R6` (pulled automatically).

---

## 6. Subsampling Strategy

[VERIFIED: existing code analysis + D-09/D-10/D-11]

### Current Pattern (from fit_and_save_best_model)
The existing code already implements stratified subsampling by the binary response, keeping ALL minority class rows and downsampling the majority class:
```r
# Existing pattern — keep all minority, sample majority to hit target size
minority_data <- filter(data, response == minority_class)  # keep all
majority_data <- filter(data, response == majority_class) %>%
  slice_sample(n = target_majority, replace = FALSE)
final_fit_data <- bind_rows(minority_data, majority_data)
```

### mlr3 Integration: Before Task Creation
Per D-09, subsampling is applied **before** creating the mlr3 Task, so the Task itself contains only the subsample. This keeps it simple — no need for mlr3 row weights or custom sampling mechanisms.

```r
# In the rewritten transition_modelling.r pipeline, after loading data:
max_rows <- config[["max_training_rows"]] %||% 500000L

if (nrow(transition_data) > max_rows) {
  log_msg(sprintf(
    "  Subsampling: %d rows -> %d (max_training_rows=%d)",
    nrow(transition_data), max_rows, max_rows
  ), log_file)
  
  # Stratified sample preserving class proportions (D-10)
  # Keep all minority, downsample majority
  response_tbl <- table(transition_data$response)
  minority_class <- names(which.min(response_tbl))
  majority_class <- names(which.max(response_tbl))
  n_minority <- response_tbl[[minority_class]]
  n_majority <- min(max_rows - n_minority, response_tbl[[majority_class]])
  
  set.seed(config[["random_seed"]] %||% 123L)  # D-10: seed from config
  
  transition_data <- rbind(
    transition_data[transition_data$response == minority_class, ],
    transition_data[transition_data$response == majority_class, ][
      sample(.Machine$integer.max, n_majority, replace = FALSE), 
    ]
  )
}
```

Base R `rbind` + row indexing is preferred over dplyr here to avoid loading extra packages in the worker context. [ASSUMED] The seed mechanism should use `config[["random_seed"]]` from the existing `model_specs.yaml` `global.random_seed = 123` field, or a dedicated `seed` key if one exists in the top-level config.

### mlr3 Built-in Stratification
mlr3's `rsmp("cv")` supports stratification via `task$col_roles$stratum`. Setting this ensures CV folds are class-balanced during tuning:
```r
task$col_roles$stratum <- "response"
```
This is complementary to (not a replacement for) the pre-task subsampling.

---

## 7. Config Changes

[VERIFIED: reading config/local_config.yaml and config/hpc_config.yaml]

### New Key: `max_training_rows`
Both YAML files need a new top-level configuration key. Following the existing snake_case YAML convention and the `configuration_settings` block pattern:

**In `config/local_config.yaml`** (add under `configuration_settings:`):
```yaml
configuration_settings:
  ...existing keys...
  max_training_rows: 500000  # Subsampling ceiling for final model fit (Phase 2, D-11)
```

**In `config/hpc_config.yaml`** (same location):
```yaml
configuration_settings:
  ...existing keys...
  max_training_rows: 1000000  # Higher on HPC (more RAM available); operator should adjust
```

The HPC value should be higher since HPC nodes have more RAM (D-11 says operator-configurable). [ASSUMED] A reasonable HPC default is 1,000,000 rows — the operator should confirm based on actual node memory specs. The local default of 500,000 is conservative for a Windows development machine.

### Config Key Access Pattern
Following the existing `get_config()` flattening convention (all nested YAML keys are flattened to a single-level list by `build_full_config()`):
```r
max_rows <- config[["max_training_rows"]] %||% 500000L
```
This matches how `config[["transition_model_dir"]]`, `config[["regionalization"]]`, etc. are accessed throughout the codebase.

### No New Environment Variables Needed
`max_training_rows` is pure data config, not a machine-specific path or secret — YAML is the right home for it. No `.env.template` changes are needed.

### model_specs.yaml: `max_final_fit_size` Key
The existing `model_specs.yaml` has `global.max_final_fit_size: 100000`. In the mlr3 rewrite, this key is **superseded by `max_training_rows` in the YAML config**. The `model_specs.yaml` file is still used for algorithm parameters (which algorithms to train, their hyperparameter grids) but the subsampling threshold moves to config. [ASSUMED] Confirm whether `model_specs.yaml` should also be updated to remove the now-unused `max_final_fit_size` key.

---

## 8. Implementation Risks and Landmines

### Risk 1: File Extension Change (.rds → .qs)
**What goes wrong:** `build_transition_model_path()` currently returns a `.rds` path. If changed to `.qs`, the caching check (`file.exists(model_path)`) will not detect existing `.rds` files from previous runs, causing unnecessary re-training. The `predict_saved_transition_prob()` loader uses `qs::qread()` for mlr3 but might receive a `.rds` path for old models.
**Prevention:** Change `build_transition_model_path()` to return `.qs` for new models. Keep old dispatch branches reading via `readRDS()`. The `qs::qread()` call in the new mlr3 branch reads the `.qs` file.
**Caller contract:** `allocation.r` loads models via `qs::qread(path)` or `readRDS(path)` depending on file extension — or use a helper that detects extension. [ASSUMED] Planner should decide: update `build_transition_model_path()` to use `.qs`, or add a new `build_mlr3_model_path()` function.

### Risk 2: AutoTuner vs. Direct Learner for Single-Value Grids
**What goes wrong:** The current `model_specs.yaml` has single-value parameter grids (e.g., `trees: [500]`). Wrapping these in AutoTuner with a 1-combination grid is wasteful overhead. AutoTuner triggers a full CV resample even for single-combo grids.
**Prevention:** Detect whether any parameter has multiple values. If all parameters are single-valued, train the learner directly without AutoTuner. The `multi_spec_trans_modelling()` pattern already does this (`if (nrow(tune_params) > 0)`).

### Risk 3: XGBoost External Pointer Serialisation
**What goes wrong:** XGBoost boosters contain C++ external pointers. On older mlr3learners versions, `qs::qsave()` of an xgb.Booster would lose the model data.
**Mitigation:** mlr3learners 0.14+ stores XGBoost models in a wrapper structure (`structure("wrapper", model = model)`) that handles this correctly. The `r-xgboost=1.7` pin must match between the training env and the prediction env. [MEDIUM confidence — verified that 0.14 does not have "marshal" property, which means standard serialisation works, but the exact mechanism is not documented explicitly]
**Check:** After saving, reload the file in the same R session and run a 5-row predict (D-13) to verify the booster survives round-trip.

### Risk 4: classif.ranger num.trees vs. trees Parameter Name
**What goes wrong:** mlr3learners uses `num.trees` (ranger's native name), NOT `trees` (tidymodels name). Code that tries to set `$param_set$values$trees` will silently fail or error.
**Prevention:** Always use native ranger/xgboost parameter names in mlr3. See mapping table in Section 2.
[VERIFIED: mlr3learners classif.ranger docs]

### Risk 5: predict_newdata Column Order / Missing Columns
**What goes wrong:** `predict_newdata()` requires `newdata` to contain all feature columns the model was trained on. If `predictor_names` in the saved object does not exactly match what was in the training Task (e.g., if feature selection uses semicolon-separated strings that get parsed differently), prediction will fail.
**Prevention:** Store `task$feature_names` (not `colnames(data)[colnames(data) != "response"]`) in `predictor_names` — the Task's view of features is authoritative. These are identical in practice but using the Task's field is explicit.

### Risk 6: response Factor Levels in mlr3 vs. tidymodels
**What goes wrong:** mlr3 labels classes from the factor levels of the target column. If `response` is created as `as.factor(trans_df$response)` where `trans_df$response` is 0/1 integers, the factor levels will be `c("0", "1")` and the positive class must be explicitly set to `"1"` in `as_task_classif(..., positive = "1")`. If `positive` is wrong, the probability columns will be reversed.
**Prevention:** Always pass `positive = "1"` in Task creation. In the mlr3 dispatcher, extract probability via `pred$prob[, "1"]` (by name, not by column index).

### Risk 7: Parallel Execution with mlr3 R6 Objects
**What goes wrong:** mlr3 Learner objects are R6 reference objects. If passed to `furrr::future_map()` workers as globals, they may be copied by value with `future::multisession` but referenced with `future::multicore`. Setting a learner's param values in one worker could affect another worker.
**Prevention:** Always `learner$clone(deep = TRUE)` before training in each parallel worker. The current `model_single_transition()` function already runs inside `furrr::future_map()` — the mlr3 Learner must be cloned inside the worker, not passed as a pre-configured object.
[VERIFIED: evoland-plus reference uses `learner$clone(deep = TRUE)` pattern]

### Risk 8: r-glmnet vs. classif.log_reg Decision
**What goes wrong:** The current model_specs.yaml uses `glmnet` (regularised logistic regression with `penalty` and `mixture` params). mlr3's `classif.log_reg` uses plain `stats::glm()`. These produce different models and require different parameter handling. The GLM pipeline must use `classif.glmnet` if regularisation is desired, which adds a normalisation step (or uses `mlr3pipelines`).
**Prevention:** [ASSUMED] Confirm with the researcher whether to use `classif.log_reg` (simpler, no tunable params, no r-glmnet needed) or `classif.glmnet` (matches current behaviour, requires r-glmnet, requires normalisation). The `r-glmnet` package is NOT currently in `allocation_env.yml` (it was a training-only package) and would need to be added if used.

---

## 9. File-Level Plan Sketch

Suggested decomposition into plans. Each plan is a self-contained unit with a clear verify step.

### Plan 02-01: Environment and Config Foundation
**What:** Add mlr3 packages to `allocation_env.yml`. Add `max_training_rows` to both YAML configs. Update `build_transition_model_path()` to support `.qs` extension.
**Files:** `environments/allocation_env.yml`, `config/local_config.yaml`, `config/hpc_config.yaml`, `src/transition_modelling.r` (path helper only)
**Verify:** `conda-lock` or `conda install --dry-run` shows no conflicts; `config[["max_training_rows"]]` resolves in R.

### Plan 02-02: Rewrite transition_modelling.r Core
**What:** Replace `create_model_spec()`, `tune_model()`, `fit_and_save_best_model()`, `save_minimal_model()` with mlr3 equivalents. New functions: `build_mlr3_learner()`, `train_mlr3_transition()` (handles subsampling + Task + AutoTuner/direct + save). Keep `transition_modelling()`, `perform_transition_modelling()`, `model_single_transition()` outer shells intact but replace the inner modelling call.
**Files:** `src/transition_modelling.r`
**Verify:** Train one small transition locally; `file.size()` < 200 MB; 5-row predict returns [0,1] non-NA probabilities (D-12, D-13).

### Plan 02-03: Extend predict dispatcher in allocation.r
**What:** Add `model_type == "mlr3"` branch as the first check in `predict_saved_transition_prob()`. The branch calls `learner$predict_newdata()` and returns `.pred_0`/`.pred_1` data.frame.
**Files:** `src/allocation.r`
**Verify:** Load a saved mlr3 `.qs` model file; call `predict_saved_transition_prob(model_obj, new_data)` with a 5-row fixture; assert output has columns `.pred_0`, `.pred_1` with values in [0,1].

### Plan 02-04: retrain_all_models.r Utility Script
**What:** Create `scripts/retrain_all_models.r` that reads `viable_transitions_lists.csv`, filters to non-self non-NA `rate_2018_2022` rows for the 4 named regions, loops (or parallel-maps) over each pair, calls the new `transition_modelling()` with `refresh_cache = TRUE`. Logs progress; skips pairs with existing `.qs` files unless `--force` flag passed.
**Files:** `scripts/retrain_all_models.r` (new file)
**Verify:** Dry-run (no actual training) prints the ~140–160 expected transition-region pairs. Full run on one small transition completes without error.

**Execution order:** 02-01 → 02-02 → 02-03 → 02-04. Plan 02-03 can be done in parallel with 02-02 once the saved object contract is defined (from 02-02 design).

---

## Common Pitfalls

### Pitfall 1: Storing AutoTuner Instead of Inner Learner
**What goes wrong:** `qs::qsave(at, path)` saves the full AutoTuner including all CV resample results — this can be 500 MB+. The AutoTuner is not needed at prediction time.
**How to avoid:** Always extract and save `at$learner` (the inner Learner with tuned params fitted on full data), not the AutoTuner object.
**Warning signs:** Saved `.qs` file > 50 MB for a GLM model.

### Pitfall 2: Wrong Column Name for Probability Extraction
**What goes wrong:** `pred$prob[, 2L]` gets the second column, which may not be class "1" if the factor levels are ordered differently. mlr3 sorts class labels lexicographically, so `"0"` is always column 1 and `"1"` is always column 2 for integer-labelled binary outcomes — but using positional indexing is fragile.
**How to avoid:** Always use `pred$prob[, "1"]` (named indexing). Set `positive = "1"` explicitly in `as_task_classif()`.

### Pitfall 3: ranger save.memory Not Set
**What goes wrong:** Default `save.memory = FALSE` causes ranger to store the full OOB prediction matrix (one row per training observation), which was the primary source of >1 GB model files in the current stack.
**How to avoid:** Always set `save.memory = TRUE` in the ranger learner params.

### Pitfall 4: model_specs.yaml Algorithm Names vs. mlr3 Learner IDs
**What goes wrong:** Current YAML uses `"glm"`, `"rf"`, `"xgboost"` as keys. mlr3 uses `"classif.log_reg"` (or `"classif.glmnet"`), `"classif.ranger"`, `"classif.xgboost"`. The `create_model_spec()` switch statement must be updated.
**How to avoid:** Add a name-mapping step or rename the YAML keys. Document the mapping explicitly.

---

## Code Examples

### Complete mlr3 Pipeline for One Transition (reference template)
```r
# Source: research synthesis from mlr3 docs + evoland-plus reference
train_and_save_mlr3_transition <- function(
  transition_data,    # data.frame with "response" (factor) + predictor cols
  predictor_names,    # character vector from feature selection
  output_path,        # .qs file path
  config,
  log_file = NULL
) {
  library(mlr3)
  library(mlr3learners)
  library(mlr3tuning)
  library(paradox)

  max_rows <- config[["max_training_rows"]] %||% 500000L

  # 1. Subsampling (D-09, D-10)
  if (nrow(transition_data) > max_rows) {
    # ... stratified subsample (see Section 6) ...
  }

  # 2. Task creation
  task <- as_task_classif(
    transition_data[, c(predictor_names, "response"), drop = FALSE],
    target   = "response",
    positive = "1"
  )
  task$col_roles$stratum <- "response"

  # 3. Learner construction (example: ranger)
  lrn_rf <- lrn("classif.ranger",
    predict_type  = "prob",
    num.trees     = 500L,
    mtry          = max(1L, floor(sqrt(length(predictor_names)))),
    min.node.size = 5L,
    importance    = "none",
    save.memory   = TRUE,
    num.threads   = 1L
  )

  # 4. Train
  lrn_rf$train(task)

  # 5. Build save object (D-05)
  model_obj <- list(
    model_type      = "mlr3",
    predictor_names = task$feature_names,
    response_levels = task$class_names,
    learner         = lrn_rf
  )

  # 6. Save
  qs::qsave(model_obj, output_path)

  # 7. Size gate (D-12)
  size_bytes <- file.size(output_path)
  if (size_bytes > 200 * 1024^2) {
    log_msg(sprintf(
      "WARNING: model file exceeds 200MB limit: %.1f MB — %s",
      size_bytes / 1024^2, output_path
    ), log_file)
  }
  
  invisible(model_obj)
}
```

### mlr3 Dispatch Branch (for allocation.r)
```r
# Source: research synthesis from mlr3 Learner docs
if (!is.null(model_obj$model_type) && model_obj$model_type == "mlr3") {
  new_data_subset <- subset_saved_transition_data(new_data, predictor_names)
  pred <- tryCatch(
    model_obj$learner$predict_newdata(newdata = new_data_subset),
    error = function(e) log_and_stop(sprintf(
      "mlr3 predict_newdata() failed: %s", conditionMessage(e)
    ))
  )
  prob_1 <- pred$prob[, "1"]
  return(data.frame(.pred_0 = 1 - prob_1, .pred_1 = prob_1))
}
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| tidymodels workflow + butcher | mlr3 Learner directly | Phase 2 | Eliminates recipe/workflow overhead; no `recipes::bake()` at predict time |
| `saveRDS(..., compress = "xz")` | `qs::qsave()` | Phase 2 | 2–5× faster serialise/deserialise; similar compression |
| `tidypredict_fit()` for GLM | `classif.log_reg$predict_newdata()` | Phase 2 | Removes tidypredict dependency at prediction time |
| `restore_ranger_importance_mode()` workaround | Not needed | Phase 2 | mlr3-trained ranger models have consistent structure |
| Manual butcher (predictions=NULL, inbag.counts=NULL) | `save.memory = TRUE` + `importance = "none"` | Phase 2 | Declarative; guaranteed by ranger at training time |

**Deprecated patterns (after Phase 2):**
- `save_minimal_model()` — entire function replaced
- `multi_spec_trans_modelling()` — replaced by mlr3 pipeline
- `fit_model_with_tuning()` — replaced by AutoTuner / direct `$train()`
- `create_model_spec()` / `create_tuning_grid()` — replaced by mlr3 learner param definitions

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `classif.glmnet` (regularised) vs. `classif.log_reg` (plain) — researcher prefers regularised matching current behaviour | Section 2 (GLM), Risk 8 | Wrong algorithm choice; model performance differs from current approach |
| A2 | HPC `max_training_rows` default of 1,000,000 is appropriate for Euler node memory | Section 7 | OOM on HPC if set too high; no subsampling benefit if set too low |
| A3 | `config[["random_seed"]]` is available at the top-level config for subsampling seed (vs. using model_specs.yaml random_seed) | Section 6 | Subsampling not reproducible; minor risk |
| A4 | XGBoost 1.7 + mlr3learners 0.14 serialisation works correctly with `qs::qsave()` without marshal/unmarshal | Risk 3 | XGBoost models may not survive qs round-trip; would require explicit xgb.save.raw workaround |
| A5 | `model_specs.yaml` will be adapted to use mlr3 parameter names (vs. creating a separate mlr3_model_specs.yaml) | Section 3, Plan 02-02 | Plan scope may need to include YAML schema change |
| A6 | Removing preprocessing (step_normalize) for tree models is acceptable (ranger, xgboost do not need it) | Section 3 | No performance impact expected; tree models are scale-invariant |

---

## Open Questions (RESOLVED)

1. **classif.log_reg vs. classif.glmnet for GLM**
   - What we know: Current code uses glmnet (penalty=0.01, mixture=1 = Lasso). mlr3 has both options. `r-glmnet` is not in `allocation_env.yml`.
   - What's unclear: Does the researcher want regularisation preserved? Or is plain logistic regression acceptable?
   - Recommendation: Use `classif.glmnet` (matches current behaviour); add `r-glmnet` to `allocation_env.yml` in Plan 02-01.
   - **RESOLVED:** Plan 02-01 adds `r-glmnet` to `allocation_env.yml`; Plan 02-02 uses `classif.glmnet` with alpha/s parameters matching the current Lasso configuration.

2. **Should model_specs.yaml be restructured for mlr3?**
   - What we know: Current YAML uses tidymodels-style names (`trees`, `learn_rate`, `min_n`). mlr3 uses native names (`nrounds`, `eta`, `min_child_weight`).
   - What's unclear: Scope of YAML change — in-place rename, or new mlr3_model_specs.yaml?
   - Recommendation: In-place rename with comments; keep the same structure. One source of truth.
   - **RESOLVED:** Plan 02-02 renames all keys in-place in `config/model_specs.yaml` (trees→num.trees, learn_rate→eta, etc.) while preserving the single-file structure.

3. **Does `retrain_all_models.r` run models in parallel?**
   - What we know: The existing `perform_transition_modelling()` already uses `furrr::future_map()` over transitions. Parallelism is currently `SLURM_CPUS_PER_TASK`-driven.
   - What's unclear: `retrain_all_models.r` is a new utility script — does it call `transition_modelling()` (which has parallel internals) or does it submit SLURM jobs?
   - Recommendation: Call `transition_modelling()` directly (which handles parallel internally). The utility script is a wrapper that sets up config/paths and calls the existing entry point.
   - **RESOLVED:** Plan 02-04 creates `scripts/retrain_all_models.r` that calls `transition_modelling()` directly; parallelism is handled by `perform_transition_modelling()` internals via `furrr::future_map()`.

---

## Environment Availability

All dependencies are either already in `allocation_env.yml` or available on conda-forge.

| Dependency | Required By | Available | Version | Notes |
|------------|------------|-----------|---------|-------|
| r-mlr3 | Phase 2 training | ✓ conda-forge | 1.6.0 | Not yet in env |
| r-mlr3learners | Phase 2 training | ✓ conda-forge | 0.14.0 | Not yet in env |
| r-mlr3tuning | Phase 2 training | ✓ conda-forge | 1.6.0 | Not yet in env |
| r-paradox | Phase 2 (AutoTuner) | ✓ conda-forge | 1.0.1 | Not yet in env |
| r-bbotk | mlr3tuning dependency | ✓ conda-forge | 1.10.0 | Auto-pulled or explicit |
| r-qs | Model serialisation | ✓ Already in allocation_env.yml | — | Confirmed present |
| r-xgboost=1.7 | XGBoost training | ✓ Already in allocation_env.yml | 1.7.x | Pin must not change |
| r-ranger | Ranger training | ✓ Already in allocation_env.yml | — | Confirmed present |

**No missing dependencies with blocking impact** — all required packages are available on conda-forge for both Windows (local dev) and Linux (Euler HPC). [VERIFIED: anaconda.org/conda-forge lookups]

---

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | None (project uses manual verification / SLURM batch runs) |
| Config file | None |
| Quick run command | `Rscript -e "source('src/setup.r'); source('src/transition_modelling.r'); ..."` with a fixture dataset |
| Full suite command | `scripts/run_transition_modelling.r` on one period |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| MEM-04 | Saved model files <200 MB | smoke | `file.size(path) < 200 * 1024^2` assertion in `train_and_save_mlr3_transition()` | ✅ inline (D-12) |
| MEM-04 | Predict returns [0,1] non-NA | smoke | 5-row predict fixture in verification step | ❌ Wave 0 |
| D-04 | mlr3 dispatch branch works | unit | Call `predict_saved_transition_prob(model_obj, fixture_df)` | ❌ Wave 0 |

### Wave 0 Gaps
- [ ] Fixture dataset (5 rows × N predictors) for predict equality check (D-13)
- [ ] Simple test wrapper: load saved `.qs`, call predict, assert output shape and value range

---

## Security Domain

This phase has no authentication, session management, access control, or cryptographic operations. Phase 2 is a pure data-processing pipeline (train models → save files → load files → predict). No ASVS categories apply.

Input validation note: predictor data columns are numeric (validated by mlr3 Task creation — unknown columns or wrong types will error). No user-controlled input reaches the model training code directly.

---

## Sources

### Primary (HIGH confidence)
- Direct code reading: `src/transition_modelling.r` (3,569 lines), `src/allocation.r` (lines 330–700), `config/model_specs.yaml`, `environments/allocation_env.yml`, `config/local_config.yaml`, `config/hpc_config.yaml`
- `https://mlr3.mlr-org.com/reference/Learner.html` — predict_newdata(), $model, marshaling
- `https://mlr3learners.mlr-org.com/reference/mlr_learners_classif.ranger.html` — save.memory, num.threads
- `https://mlr3tuning.mlr-org.com/reference/AutoTuner.html` — AutoTuner workflow
- `https://mlr3book.mlr-org.com/chapters/chapter4/hyperparameter_optimization.html` — to_tune() patterns
- `https://anaconda.org/conda-forge/r-mlr3` — version 1.6.0 (Apr 2026)
- `https://anaconda.org/conda-forge/r-mlr3learners` — version 0.14.0 (Dec 2025)
- `https://anaconda.org/conda-forge/r-mlr3tuning` — version 1.6.0 (Mar 2026)
- `https://anaconda.org/conda-forge/r-paradox` — version 1.0.1 (Jul 2024)
- `https://anaconda.org/conda-forge/r-bbotk` — version 1.10.0 (Apr 2026)

### Secondary (MEDIUM confidence)
- `https://github.com/ethzplus/evoland-plus/tree/copilot/integrate-mlr3-library` — trans_models_t.R patterns (qs2 serialisation, task creation, clone+train pattern)
- `https://github.com/mlr-org/mlr3learners/blob/main/R/LearnerClassifXgboost.R` — XGBoost marshaling properties (no "marshal" property confirmed)

### Tertiary (LOW confidence)
- Web search results for XGBoost serialization with qs — specific behaviour of qs::qsave on xgb.Booster in mlr3learners 0.14 not explicitly documented; inferred from absence of "marshal" property

---

## Metadata

**Confidence breakdown:**
- Standard stack (mlr3 packages, conda-forge availability): HIGH — verified via official package pages
- Architecture (Task → AutoTuner → save pattern): HIGH — verified via official mlr3 docs
- Predict interface (predict_newdata, $prob matrix): HIGH — verified via official docs
- XGBoost serialisation: MEDIUM — no explicit documentation that qs::qsave works without marshal; inferred from code
- Subsampling R idiom: HIGH — direct code reading of existing pattern
- Transition scope (~168 pairs): HIGH — direct data file reading

**Research date:** 2026-05-07
**Valid until:** 2026-08-07 (stable ecosystem; mlr3 versions may update but API is stable)
