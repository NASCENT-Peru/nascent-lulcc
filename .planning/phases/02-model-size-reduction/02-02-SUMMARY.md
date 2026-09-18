---
phase: 02-model-size-reduction
plan: 02
subsystem: [modelling, training]
tags: [mlr3, qs, ranger, xgboost, glmnet, transition-modelling]

requires:
  - phase: 02-01
    provides: mlr3 packages in allocation_env.yml and transition_model_env.yml, max_training_rows config
provides:
  - train_mlr3_transition() and build_mlr3_learner() functions in transition_modelling.r
  - .qs model save format replacing .rds in build_transition_model_path()
  - mlr3-compatible model_specs.yaml with renamed params (num.trees, nrounds, eta, etc.)
affects: [02-03, 02-04]

tech-stack:
  added: [mlr3, mlr3learners, mlr3tuning, paradox, qs]
  patterns: [AutoTuner-single-call, save-learner-not-autotuner, stratified-subsampling, size-gate-warn-only, sanity-check-5row]

key-files:
  created: [tests/testthat/test-mlr3-training-pipeline.R]
  modified: [src/transition_modelling.r, config/model_specs.yaml]

key-decisions:
  - "Save at$learner (not AutoTuner) to avoid 3-5x size bloat from tuning history"
  - "Single-value grids use direct $train() — no AutoTuner overhead"
  - "save.memory=TRUE and importance='none' hardcoded for ranger — primary size reduction mechanism"
  - "step_normalize NOT replicated — classif.glmnet L1/L2 regularisation is scale-invariant"
  - "num_replicates retained in model_specs.yaml but marked superseded by AutoTuner CV folds"
  - "T-02-03 path injection guard: output_path validated against transition_model_dir at runtime"

patterns-established:
  - "build_mlr3_learner(): constructs typed Learner objects from model_specs params with single/multi-value grid detection"
  - "train_mlr3_transition(): full pipeline — subsample -> task -> train -> save -> size-gate -> sanity-check"

requirements-completed: [MEM-04]

duration: 45min
completed: 2026-05-07
---

# Phase 02 Plan 02: Rewrite Transition Modelling Inner Stack to mlr3 Summary

One-liner: mlr3 training pipeline with stratified subsampling, ranger save.memory+importance flags, qs save format, and 200MB size gate replacing the tidymodels multi_spec_trans_modelling stack.

## What Was Built

### Task 1: TDD RED — Failing tests for mlr3 training pipeline

Created `tests/testthat/test-mlr3-training-pipeline.R` with 4 test_that() blocks:

- **Test 1** (RED): asserts `train_mlr3_transition` exists in `src/transition_modelling.r` — grep-based
- **Test 2** (passes): asserts no `library(tidymodels)`, `library(workflows)`, `library(parsnip)` outside comments
- **Test 3** (RED): asserts `.qs` extension and no `%s_%s.rds` in `build_transition_model_path()`
- **Test 4** (integration): full train-save-load-predict cycle with 200-row synthetic data, guarded by `skip_if_not_installed("mlr3")` and `skip_if_not_installed("mlr3learners")`

**Note:** The test file was already committed in commit `43e2d50` as part of the 02-03 RED state commit (prior agent session had run ahead). Task 1's commit was pre-existing; the file content matched exactly what was required.

### Task 2: GREEN — Rewrite transition_modelling.r inner stack and update model_specs.yaml

**Edit 2a: `build_transition_model_path()` (line 110)**
Changed `sprintf("%s_%s.rds", ...)` to `sprintf("%s_%s.qs", ...)`. Single character change.

**Edit 2b: `model_single_transition()` inner call (lines ~1110-1134)**
Replaced the `multi_spec_trans_modelling()` + `fit_and_save_best_model()` call block with a `tryCatch`/`withCallingHandlers` wrapper calling `train_mlr3_transition()`. Old functions (`multi_spec_trans_modelling`, `fit_model_with_tuning`, `fit_and_save_best_model`, `save_minimal_model`) retained in file but no longer called.

**Edit 2c: New functions inserted after `build_transition_model_path()`, before `read_optional_rds()`**

`build_mlr3_learner(algo, params, predictor_count)`:
- glm branch: `classif.glmnet` with alpha/s; comment documents step_normalize not needed
- rf branch: `classif.ranger` with `save.memory=TRUE`, `importance="none"`, `num.threads=1L` hardcoded
- xgboost branch: `classif.xgboost` with `nthread=1L` hardcoded
- Single-value detection: sets params directly; multi-value uses `to_tune()` for AutoTuner

`train_mlr3_transition(...)`:
- T-02-03 path injection guard (validates output_path within transition_model_dir)
- Stratified subsampling before Task creation (D-09/D-10)
- mlr3 Task with `stratum` role on response column (D-13)
- Per-algorithm training loop: AutoTuner for multi-value grids, direct `$train()` for single-value
- Saves `list(model_type="mlr3", predictor_names, response_levels, learner)` via `qs::qsave()` (D-05)
- 200MB size gate — warn-only, does not stop (D-12)
- 5-row `predict_newdata()` sanity check — asserts probabilities in [0,1], non-NA (D-13)
- Returns result list compatible with `perform_transition_modelling()` aggregation

**Edit 1: `config/model_specs.yaml`**
Full rewrite with mlr3 parameter names:
- `glm`: `penalty` -> `s`, `mixture` -> `alpha`
- `rf`: `trees` -> `num.trees`, `min_n` -> `min.node.size`; removed `importance`, `replace`, `sample.fraction`, `seed` (hardcoded in function)
- `xgboost`: `trees` -> `nrounds`, `tree_depth` -> `max_depth`, `learn_rate` -> `eta`, `min_n` -> `min_child_weight`, `mtry` -> `colsample_bytree` (as fraction)
- `num_replicates` value changed to 1 with comment marking it superseded by AutoTuner CV folds

## Verification Results

| Check | Result |
|-------|--------|
| `grep -c "train_mlr3_transition" src/transition_modelling.r` | 3 (definition + call + tryCatch) |
| `grep -c "save.memory" src/transition_modelling.r` | 2 (comment + code) |
| `grep -c "200 * 1024" src/transition_modelling.r` | 1 (line 343) |
| `grep -c "predict_newdata" src/transition_modelling.r` | 3 |
| `library(tidymodels)` outside comments | 0 |
| `grep -c "num.trees" config/model_specs.yaml` | 1 |
| `grep -c "nrounds" config/model_specs.yaml` | 1 |
| `grep -c "Superseded by AutoTuner" config/model_specs.yaml` | 1 |
| `grep -c "step_normalize" src/transition_modelling.r` | 4 (comments) |
| `.qs` in build_transition_model_path | confirmed (line 110) |
| `%s_%s.rds` in file | 0 (removed) |

## Deviations from Plan

### Pre-existing Task 1 commit

**Found during:** Task 1 execution

**Issue:** The test file `tests/testthat/test-mlr3-training-pipeline.R` was already committed in commit `43e2d50` (`test(02-03): failing tests for mlr3 predict dispatch branch...`) from a prior agent session that executed plan 02-03 ahead of 02-02. The file content matched exactly what the 02-02 plan specified.

**Fix:** Verified the existing file matched all Task 1 acceptance criteria (4 test_that blocks, skip_if_not_installed guards, RED state for Tests 1 and 3). No new commit was needed for Task 1.

**Files modified:** None (pre-existing)

### T-02-03 path injection guard added (Rule 2 — missing critical security mitigation)

**Found during:** Task 2

**Issue:** The plan's `<threat_model>` listed T-02-03 (output_path injection) with disposition `mitigate`, requiring a path validation check before `qs::qsave()`. This was absent from the plan's `<interfaces>` code block.

**Fix:** Added path injection guard at the start of `train_mlr3_transition()`: validates that `normalizePath(output_path)` starts with `normalizePath(config[["transition_model_dir"]])`. Guard is skipped gracefully if `transition_model_dir` is not in config (e.g., in integration tests where a tempfile() path is used).

**Files modified:** `src/transition_modelling.r`

## Known Stubs

None — the plan's goal (mlr3 training pipeline with .qs save format) is fully wired. The selection logic in step 5 of `train_mlr3_transition()` (picking RF if available, else first algorithm) is a documented simplification — full CV-based selection is marked as "In future" in a comment.

## Threat Flags

No new security-relevant surface beyond what was already in the plan's threat model (T-02-03 was mitigated; T-02-04 and T-02-05 were accepted).

## Self-Check: PASSED

- `src/transition_modelling.r` exists: FOUND
- `config/model_specs.yaml` exists: FOUND
- `tests/testthat/test-mlr3-training-pipeline.R` exists: FOUND
- Task 2 commit `39d206b` exists in git log: FOUND
- All acceptance criteria verified against grep outputs above
