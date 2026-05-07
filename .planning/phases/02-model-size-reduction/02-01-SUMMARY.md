---
phase: 02-model-size-reduction
plan: 01
subsystem: [infra, testing]
tags: [mlr3, conda, yaml, testthat]

requires: []
provides:
  - mlr3 package declarations in both conda environments (allocation_env.yml, transition_model_env.yml)
  - max_training_rows subsampling threshold in local and HPC config
  - testthat contract test asserting all conditions
affects: [02-02, 02-03]

tech-stack:
  added: [r-mlr3, r-mlr3learners, r-mlr3tuning, r-paradox, r-bbotk, r-glmnet (in allocation_env), r-mlr3 r-mlr3learners r-mlr3tuning r-paradox r-bbotk (in transition_model_env)]
  patterns: [yaml-contract-test, tdd-red-green]

key-files:
  created: [tests/testthat/test-mlr3-env-contract.R]
  modified: [environments/allocation_env.yml, environments/transition_model_env.yml, config/local_config.yaml, config/hpc_config.yaml]

key-decisions:
  - "r-mlr3pipelines NOT added — classif.glmnet is scale-invariant; step_normalize not required"
  - "transition_model_env.yml gets 5 mlr3 packages (not r-glmnet, already present)"

patterns-established:
  - "Contract test reads YAML as text via readLines — no R parsing; grep-safe"

requirements-completed: [MEM-04]

duration: 5min
completed: 2026-05-07
---

# Phase 02 Plan 01: mlr3 env contract and max_training_rows config — Summary

**One-liner:** Six mlr3 packages added to allocation_env.yml (including r-glmnet for classif.glmnet), five to transition_model_env.yml (r-glmnet already present), max_training_rows threshold added to both configs, green testthat contract tests asserting all conditions.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Write failing contract test (TDD RED) | 7867f18 | tests/testthat/test-mlr3-env-contract.R |
| 2 | Add mlr3 packages and config keys (GREEN) | bed3eae | environments/allocation_env.yml, environments/transition_model_env.yml, config/local_config.yaml, config/hpc_config.yaml |

## Changes Made

### environments/allocation_env.yml
Appended a new `Phase 2 (MEM-04)` block after the `r-rhpcblasctl` line (end of the MEM-06 section) containing:
- `r-mlr3` — core framework
- `r-mlr3learners` — classif.ranger / classif.glmnet / classif.xgboost learners
- `r-mlr3tuning` — AutoTuner for hyperparameter optimisation
- `r-paradox` — search space definitions
- `r-bbotk` — black-box optimisation toolkit (AutoTuner dependency)
- `r-glmnet` — required by classif.glmnet (regularised logistic regression)

The `r-xgboost=1.7` pin is unchanged.

### environments/transition_model_env.yml
Inserted 5 mlr3 packages immediately after the `r-glmnet       # Regularized regression` line:
- `r-mlr3`, `r-mlr3learners`, `r-mlr3tuning`, `r-paradox`, `r-bbotk`

r-glmnet was NOT added again (already present). r-xgboost=1.7 pin unchanged.

### config/local_config.yaml
Added `max_training_rows: 500000` under `configuration_settings` after `selected_scalar: 9.0`.

### config/hpc_config.yaml
Added `max_training_rows: 1000000` under `configuration_settings` after `selected_scalar: 9.0`.

### tests/testthat/test-mlr3-env-contract.R (new)
Five `test_that()` blocks using the `.repo_root` resolver + `.read_text()` pattern from `test-allocation-env-canonical.R`:
1. Asserts all 6 mlr3 packages in `allocation_env.yml`
2. Asserts no duplicate `r-mlr3` entries in `allocation_env.yml`
3. Asserts `max_training_rows:` in `local_config.yaml`
4. Asserts `max_training_rows:` in `hpc_config.yaml`
5. Asserts all 5 mlr3 training packages in `transition_model_env.yml`

## Verification Results

```
grep -E "^\s+-\s+r-mlr3(learners|tuning|pipelines)?\s*$" environments/allocation_env.yml
  - r-mlr3
  - r-mlr3learners
  - r-mlr3tuning

grep -E "^\s+-\s+r-(paradox|bbotk|glmnet)\s*$" environments/allocation_env.yml
  - r-paradox
  - r-bbotk
  - r-glmnet

grep -E "^\s+-\s+r-mlr3(learners|tuning)?\s" environments/transition_model_env.yml
  - r-mlr3         # mlr3 training framework (Phase 2, MEM-04)
  - r-mlr3learners # classif.ranger / classif.glmnet / classif.xgboost learners
  - r-mlr3tuning   # AutoTuner for hyperparameter optimisation

grep -E "^\s+-\s+r-(paradox|bbotk)\s" environments/transition_model_env.yml
  - r-paradox      # Search space definitions for tuning
  - r-bbotk        # Black-box optimisation toolkit (AutoTuner dependency)

grep "max_training_rows" config/local_config.yaml config/hpc_config.yaml
config/local_config.yaml:  max_training_rows: 500000  # Subsampling ceiling ...
config/hpc_config.yaml:  max_training_rows: 1000000  # Higher threshold on HPC nodes ...

grep "r-xgboost=1.7" environments/allocation_env.yml environments/transition_model_env.yml
environments/allocation_env.yml:  - r-xgboost=1.7
environments/transition_model_env.yml:  - r-xgboost=1.7  # Gradient boosting - pin to 1.7.x ...
```

All verification commands returned matches. r-xgboost=1.7 pin unchanged in both files.

## Deviations from Plan

None — plan executed exactly as written.

## TDD Gate Compliance

RED commit (7867f18): `test(02-01): failing contract tests for mlr3 env block and max_training_rows config keys`
GREEN commit (bed3eae): `feat(02-01): add mlr3 packages to allocation_env.yml and transition_model_env.yml, max_training_rows to configs (MEM-04, D-09, D-11)`

Both gates satisfied in correct order.

## Known Stubs

None — all YAML entries are real package declarations; config values are real integers.

## Threat Flags

No new threat surface introduced. All changes are under version control. max_training_rows is a data threshold, not a path or secret. No new network endpoints, auth paths, or file access patterns introduced.

## Self-Check: PASSED

- tests/testthat/test-mlr3-env-contract.R: FOUND
- environments/allocation_env.yml (modified): FOUND
- environments/transition_model_env.yml (modified): FOUND
- config/local_config.yaml (modified): FOUND
- config/hpc_config.yaml (modified): FOUND
- Commit 7867f18: FOUND
- Commit bed3eae: FOUND
