---
phase: 2
reviewers: [claude-inline]
reviewed_at: 2026-05-07T00:00:00Z
plans_reviewed:
  - 02-01-PLAN.md
  - 02-02-PLAN.md
  - 02-03-PLAN.md
  - 02-04-PLAN.md
note: "No external AI CLIs detected (gemini, codex, opencode, qwen, cursor, ollama, lm_studio all unavailable). Inline independent review performed by Claude Code reading plans fresh against codebase."
---

# Cross-AI Plan Review — Phase 2: Model Size Reduction

## Inline Review (Claude Code — independent read)

### Summary

The four plans collectively form a coherent mlr3 migration. The wave structure is correct (env/config first, then parallel code changes, then utility script), the save/load contract is explicit, and the checker already caught and fixed the two most critical gaps (loader missing qs::qread(), --region flag no-op). The plans are unusually detailed for an R project and the acceptance criteria are grep-verifiable throughout.

Two concerns merit attention before execution: the silent drop of the `num_replicates` concept from the current stack (plan 02-02 makes this disappear without documentation), and the missing `r-mlr3pipelines` package which RESEARCH.md §5 flags as needed for glmnet normalisation — though this is likely not required in practice since mlr3's classif.glmnet handles scale-invariance internally.

---

### Plan 02-01: Environment & Config Foundation

**Strengths:**
- Atomic scope — no code changes, only YAML edits and a test file
- TDD RED→GREEN pattern from Phase 1 applied correctly
- Exact package names and YAML values are specified; no ambiguity for the executor
- r-xgboost=1.7 pin preservation explicitly called out in acceptance criteria (prevents accidental unpin)
- Does NOT add r-mlr3pipelines — which is correct (classif.glmnet does not require explicit normalisation pipeline)

**Concerns:**
- LOW: RESEARCH.md §5 lists `r-mlr3pipelines` as a package to add. The plan omits it. This is likely the correct decision (glmnet's internal regularisation is scale-invariant; recipes `step_normalize()` is a tidymodels-ism), but the plan should document the explicit decision to exclude r-mlr3pipelines to avoid confusion if a future executor reads RESEARCH.md and wonders why it's missing.
- LOW: The test file path `tests/testthat/test-mlr3-env-contract.R` is new. The plan correctly models it after `test-allocation-env-canonical.R` but doesn't verify that `tests/testthat/testthat.R` (the runner) will auto-discover the new file. In Phase 1 this was not an issue because testthat auto-discovers files matching `test-*.R`. Confirm or document.

**Suggestions:**
- Add a one-line comment in allocation_env.yml explaining why r-mlr3pipelines is not included (e.g., `# r-mlr3pipelines not required — classif.glmnet handles normalisation internally`)
- The must_haves says "six mlr3 packages" — count in the list is correct (r-mlr3, r-mlr3learners, r-mlr3tuning, r-paradox, r-bbotk, r-glmnet = 6). No issue.

**Risk Assessment: LOW** — Simple YAML edits with TDD safety net.

---

### Plan 02-02: Rewrite transition_modelling.r

**Strengths:**
- Outer function signatures explicitly documented as PRESERVE — executor cannot miss this constraint
- AutoTuner "one $train() call" pattern is correct (no separate finalize step needed)
- `save.memory = TRUE` + `importance = "none"` for ranger are the correct size-reduction flags
- Does NOT save the AutoTuner itself — saves `at$learner` only (avoids 3-5× size bloat from tuning history)
- Size gate (D-12) correctly implemented as warning-only (not hard stop)
- 5-row sanity check (D-13) correctly placed after save

**Concerns:**
- HIGH: The current `multi_spec_trans_modelling()` loops through `model_specs$global$num_replicates` (currently 2 per model type). The mlr3 migration silently drops this concept — `train_mlr3_transition()` trains a single model per algorithm (GLM/RF/XGBoost), selected by AutoTuner's internal CV. This is architecturally correct for mlr3 (AutoTuner's CV folds replace the replicate averaging logic), but the plan does not document this behavioural change. A future reviewer reading the plans and noticing that `num_replicates = 2` in model_specs.yaml no longer has any effect will be confused. The plan should either: (a) add a comment noting replicates are superseded by AutoTuner CV, or (b) remove `num_replicates` from model_specs.yaml to prevent the dead config key from causing confusion.

- MEDIUM: The plan handles "single-value grid → direct `$train()`" vs "multi-value grid → AutoTuner" — this is a good optimization. But the detection logic (how to tell if all values in the search space are single-valued) is not specified in the action. The executor will need to infer this. Suggest adding explicit pseudo-code: `if all param grid values have length == 1, skip AutoTuner and call learner$train(task) directly`.

- MEDIUM: For `classif.glmnet`, the existing tidymodels code uses `step_normalize()` (feature normalisation) in recipes. The mlr3 migration removes this pre-processing step. classif.glmnet's regularisation is scale-invariant for the penalty term, but the features themselves may have very different scales that affect interpretation. The plan does not address whether normalisation should be preserved (via mlr3pipelines PipeOp) or dropped. Given the decision to exclude r-mlr3pipelines from the env, normalisation is implicitly dropped — this should be documented.

- LOW: The `model_specs.yaml` parameter rename table (tidymodels → mlr3 names) is correct in the research. The plan should explicitly list which parameters are renamed for XGBoost: `mtry → colsample_bytree` with fraction conversion (e.g., `mtry = 4` with `p` predictors → `colsample_bytree = 4/p`). This conversion is non-trivial and could produce subtly wrong models if the executor doesn't know about it.

**Suggestions:**
- Add explicit action item: "Remove or comment out `num_replicates` from model_specs.yaml global block, adding a comment: `# Replicates replaced by AutoTuner CV folds in mlr3 pipeline`"
- Specify the single-value detection logic explicitly
- Document the normalisation decision (drop step_normalize, classif.glmnet handles internally)
- Specify the mtry → colsample_bytree fraction conversion for XGBoost

**Risk Assessment: MEDIUM** — The 3,569-line rewrite is the highest-risk plan. The outer function preservation constraint is well-enforced. The replicate drop and glmnet normalisation omission are the primary documentation gaps.

---

### Plan 02-03: Predict Dispatcher + Model Loader

**Strengths:**
- Dispatch branch placed BEFORE the existing tidypredict_ check (line 540) — correct insertion point
- Uses named column extraction `pred$prob[, "1"]` — safe against column reordering
- qs::qread() fix is backward-compatible (.rds files still use readRDS())
- `restore_ranger_importance_mode()` is only called inside specific dispatch branches (lines 601, 633, 653, 679) — NOT called at dispatch entry — so the mlr3 branch at line 540 is unaffected. No action needed.
- Task 3 (loader fix) correctly targets the specific call site (~line 1514)

**Concerns:**
- MEDIUM: The plan says the mlr3 branch calls `subset_saved_transition_data()` to filter predictor columns before `predict_newdata()`. The plan should confirm that `subset_saved_transition_data()` returns a plain `data.frame` (not a `tibble` or `data.table`). mlr3's `predict_newdata()` accepts any data.frame-like object, but if the function returns a tibble with extra attributes, it should still work. Confirm this doesn't need special handling.
- LOW: The test creates a "mock mlr3 model object". For a realistic mock, the test needs a minimal trained Learner. The plan should specify whether to train a real Learner on synthetic data (preferred — tests the actual predict path) or create a fake object with the right structure (cheaper but more fragile). Given the 5-row synthetic dataset in 02-02's test, the same approach is recommended here.

**Suggestions:**
- Confirm `subset_saved_transition_data()` return type in read_first list
- Specify that the mock model in the test is a real Learner trained on synthetic data (not a list with fake structure)

**Risk Assessment: LOW-MEDIUM** — The plan is precise. The loader fix is well-scoped. The primary risk is the dispatch branch interacting with the existing error-handling logic (log_and_stop helper at lines 506-509).

---

### Plan 02-04: retrain_all_models.r Utility

**Strengths:**
- --region filtering is now real (CSV read → filter → temp file → config override)
- --dry-run correctly prints without training
- Follows established script skeleton pattern
- Delegates parallelism to transition_modelling() — no reinventing the wheel

**Concerns:**
- MEDIUM: The temp CSV written by --region filtering uses `tempfile(fileext = ".csv")`. On HPC with multisession parallelism, the R worker processes are spawned from the same parent process and inherit the same temp directory — this is fine. However, if the user runs `retrain_all_models.r` via a SLURM array job (not documented but plausible), the temp file would only exist on the head node. Since this script is designed for single-session use (delegates to transition_modelling() which handles internal parallelism), this is probably fine — but should be documented as "single-session use only, not for SLURM array dispatch".
- LOW: The script doesn't validate that `config[["viable_transitions_lists"]]` is a valid CSV path before reading it. If the config key is missing or the file doesn't exist, the error will be cryptic. Add a `stopifnot(file.exists(...))` or equivalent.
- LOW: The script's `--dry-run` mode prints expected pairs — but it prints them based on the full (unfiltered) viable_transitions list, then applies --region filter. The plan should confirm the dry-run output respects the --region filter (i.e., shows only filtered pairs when --region is set).

**Suggestions:**
- Add documentation comment: "For single-session use. Not designed for SLURM array dispatch — transition_modelling() handles internal parallelism."
- Add file.exists() validation for viable_transitions_lists path
- Confirm --dry-run output is filtered by --region when both flags are provided

**Risk Assessment: LOW** — Thin wrapper over an already-tested function. The --region temp file approach is pragmatic and correct for the expected use case.

---

## Consensus Summary

### Agreed Strengths
- **Wave structure is correct**: env/config (Wave 1) → parallel code changes (Wave 2) → utility script (Wave 3). Dependencies are explicit and correct.
- **Save/load contract is fully specified**: `{model_type="mlr3", predictor_names, response_levels, learner}` list, qs::qsave/qread, .qs extension, extension-detecting loader. No ambiguity between 02-02 and 02-03.
- **TDD pattern from Phase 1 is applied**: RED tests before GREEN implementation.
- **Critical bugs caught before execution**: checker caught --region no-op (blocker) and missing qs::qread() loader (blocker); both fixed in iteration 1.
- **Outer function signatures preserved**: transition_modelling(), perform_transition_modelling(), model_single_transition() are not changed. Safe migration boundary.

### Agreed Concerns (Priority Order)

0. **CRITICAL — transition_model_env.yml missing from Wave 1** (02-01): `environments/transition_model_env.yml` is the conda environment used when training transition models (it contains the tidymodels stack — r-tidymodels, r-ranger, r-xgboost=1.7, etc.). Plan 02-01 only adds mlr3 packages to `allocation_env.yml` (the prediction/main-pipeline environment). The training environment also needs r-mlr3, r-mlr3learners, r-mlr3tuning, r-paradox, and r-bbotk added. r-glmnet is already present in transition_model_env.yml. Without this fix, the rewritten transition_modelling.r will fail with "there is no package called 'mlr3'" when run inside the transition_model_env. This is a runtime blocker.

1. **HIGH — Replicate concept silently dropped** (02-02): `num_replicates: 2` in model_specs.yaml becomes a dead config key. The plan doesn't document this behavioural change. Recommend either removing `num_replicates` from model_specs.yaml (preferred — eliminates dead config) or adding a comment explaining it's superseded by AutoTuner CV folds. **This is a documentation gap, not a correctness issue** — AutoTuner CV is the correct replacement for replicate averaging.

2. **MEDIUM — glmnet normalisation not documented** (02-02): The old tidymodels stack applied `step_normalize()` via recipes. The mlr3 migration drops this. classif.glmnet's regularisation works without normalisation, so predictions will differ numerically from the old models (which is acceptable — re-training from scratch per D-07). But this should be explicitly noted in the plan.

3. **MEDIUM — mtry → colsample_bytree conversion for XGBoost** (02-02): `mtry` in the old stack was an integer count of features; XGBoost's `colsample_bytree` is a fraction [0,1]. The conversion requires dividing by the feature count. If the executor reads the model_specs.yaml value directly without conversion, XGBoost models will train on a different feature fraction than intended.

4. **LOW — r-mlr3pipelines exclusion not documented** (02-01): The research mentions it but the plan correctly excludes it. A one-line comment would prevent future confusion.

### Divergent Views
- None (single reviewer).

---

## Recommended Actions Before Execution

| Priority | Plan | Action |
|----------|------|--------|
| CRITICAL | 02-01 | Add `environments/transition_model_env.yml` to files_modified and task actions — the training environment is missing the mlr3 packages |
| HIGH | 02-02 | Document replicate drop in plan action or model_specs.yaml comment |
| MEDIUM | 02-02 | Specify mtry → colsample_bytree fraction conversion explicitly in action |
| MEDIUM | 02-02 | Note glmnet normalisation drop (step_normalize removed, glmnet handles internally) |
| LOW | 02-01 | Add one-line comment in env.yml: r-mlr3pipelines not required |
| LOW | 02-04 | Add file.exists() validation for viable_transitions_lists path |

The CRITICAL item (transition_model_env.yml missing) is a blocker — without adding r-mlr3, r-mlr3learners, r-mlr3tuning, r-paradox, and r-bbotk to the training environment, the rewritten transition_modelling.r will fail at runtime because these packages won't be available in the conda environment used for model training. Note: r-glmnet is already present in transition_model_env.yml.

The HIGH item (replicates) is the most important documentation gap — it affects model selection logic and should be documented even if the design decision is correct.

---

*Review by: Claude Code (inline, independent read)*
*Phase: 2 — Model Size Reduction*
*Reviewed: 2026-05-07*
