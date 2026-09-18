# Phase 2: Model Size Reduction — Discussion Log

**Phase:** 2 — Model Size Reduction
**Date:** 2026-05-07
**Outcome:** CONTEXT.md written — ready for planning

---

## Pre-Discussion User Notes

User provided upfront context before the discussion began:
- Hyperparameter tuning is relatively fast; the OOM failure occurs in the **final full-data fit** after tuning, not in the tuning step itself.
- Saved model objects have inconsistent structures, requiring defensive loading strategies during allocation prediction.
- Decision to use tidymodels was made to avoid these issues, but it hasn't worked.
- User wants to explore mlr3 as a replacement, pointing to the evoland-plus divergent codebase that has started this integration: `https://github.com/ethzplus/evoland-plus/tree/copilot/integrate-mlr3-library`

---

## Gray Areas Discussed

### 1. MLR3 Migration Strategy

**Question:** How should the mlr3 migration be scoped?
- Options presented: Full replacement (remove tidymodels entirely), Parallel approach (keep tidymodels, add mlr3 path), Incremental (one algorithm at a time)
- **Selected:** Full replacement (Recommended)

**Question:** What learner set should mlr3 use?
- Options presented: Same algorithms (GLM/RF/XGBoost — structural replacement), Expand to new algorithms, Reduce to simplest working set
- **Selected:** Same algorithms (GLM/RF/XGBoost) — structural replacement, not algorithm change

**Question:** How should the evoland-plus reference codebase be used?
- Options presented: Reference only — adapt patterns, Copy verbatim, Ignore it
- **Selected:** Reference only — adapt patterns (do not copy verbatim; adapt to nascent-lulcc's config/path contract)

---

### 2. Predict Interface & Model Serialisation

**Question:** How should the predict interface handle mlr3 models?
- Options presented: Add new mlr3 branch (preserve existing branches), Replace all existing branches immediately, Create separate predict function
- **Selected:** Add new mlr3 branch (Recommended) — preserve tidypredict/butchered branches for backward compatibility with old files

**Question:** How should mlr3 models be saved?
- Options presented: qs::qsave() (already in allocation_env.yml), saveRDS(), Custom format
- **Selected:** qs::qsave() — already established, consistent with existing tooling; must include model_type and predictor_names in saved object

**Question:** Size gate enforcement on save?
- Options presented: Hard stop if >200 MB, Log warning only (continue run), Silent (check manually)
- **Selected:** Log warning only — allows run to continue; operator can investigate; hard stop risks losing a long run over a single model

---

### 3. OOM / Large Transition Handling

**Question:** What strategy should handle transitions whose training data exceeds available memory?
- Options presented: mlr3 + subsampling fallback, mlr3 only (hope it fits), Subsampling only (no mlr3)
- **Selected:** mlr3 + subsampling fallback — mlr3 is the primary path; subsampling activates when row count exceeds threshold

**Question:** How should the subsampling threshold be configured?
- Options presented: Configurable row count in config YAML, Hardcoded constant, Auto-detected from available RAM
- **Selected:** Configurable row count in config YAML — new `max_training_rows` key in both local_config.yaml and hpc_config.yaml

**Question:** What subsampling strategy should be used?
- Options presented: Stratified by response variable (preserves class balance), Simple random, Systematic
- **Selected:** Stratified by binary response (transition present/absent) — ensures class balance is preserved; random seed from existing config seed for reproducibility

---

### 4. Existing Models & Utility Script

**Question:** What should happen to existing tidymodels/butchered RDS model files?
- Options presented: Re-train from scratch (Recommended), Transcode existing files, Keep both systems running indefinitely
- **Selected:** Re-train from scratch — no transcoding utility needed; existing files become obsolete after re-training

**Question:** What utility script should orchestrate re-training?
- Options presented: Replace with retrain_all_models.r (Recommended), Keep rebutcher_existing_models.r and extend it, No utility script (run manually)
- **Selected:** Replace with retrain_all_models.r — loops all transitions/regions, consumes get_config() for paths; replaces rebutcher_existing_models.r from original ROADMAP success criteria

---

## Decisions Captured

| ID | Decision |
|----|----------|
| D-01 | Full replacement of tidymodels in transition_modelling.r with mlr3 (tuning → final fit → save) |
| D-02 | evoland-plus mlr3 branch used as reference only — adapt patterns, do not copy verbatim |
| D-03 | Same algorithm family (GLM/RF/XGBoost) — structural replacement, not algorithm change |
| D-04 | Add model_type="mlr3" as new dispatch branch in predict_saved_transition_prob(); preserve existing branches |
| D-05 | mlr3 models saved via qs::qsave(); must include model_type and predictor_names; size gate <200 MB |
| D-06 | Old predict branches (tidypredict_*, butchered_*) removed in future cleanup — out of scope for Phase 2 |
| D-07 | Re-train all transitions from scratch; no transcoding utility; existing RDS files become obsolete |
| D-08 | scripts/retrain_all_models.r replaces rebutcher_existing_models.r per ROADMAP success criteria |
| D-09 | Subsampling fallback activates when training rows exceed max_training_rows config key |
| D-10 | Subsampling is stratified by binary response variable; random seed from config seed |
| D-11 | max_training_rows is operator-configurable in YAML; reasonable default ~500,000 |
| D-12 | Size gate: log warning (not hard stop) if file.size > 200 MB after saving |
| D-13 | Predict equality check: 5-row predict on fixture; assert probabilities in [0,1] and non-NA |

---

## Deferred Ideas

- Removing old tidypredict/butchered predict branches from allocation.r — deferred to cleanup pass after all transitions are re-trained
- Block-wise terra::predict() for per-transition RAM bounds — Phase 4 (PERF-01)
- Full unit/integration test suite — v2 (TEST-01, TEST-02)

---

*Generated: 2026-05-07*
*Phase: 2-model-size-reduction*
