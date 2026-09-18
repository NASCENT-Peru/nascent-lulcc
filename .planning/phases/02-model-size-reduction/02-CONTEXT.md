# Phase 2: Model Size Reduction - Context

**Gathered:** 2026-05-05
**Status:** Ready for planning

<domain>
## Phase Boundary

Replace the tidymodels-based transition modelling pipeline with mlr3, eliminating the training OOM failures and inconsistent model object structures that block reliable HPC execution. All model artefacts must be <200 MB on disk. A configurable subsampling fallback handles transitions whose training data exceeds available memory. The `predict_saved_transition_prob()` interface in `allocation.r` gains a new mlr3 dispatch branch; existing branches remain for any old files not yet re-trained.

This phase delivers:
- `src/transition_modelling.r` rewritten to use mlr3 (tuning → final fit → save)
- `predict_saved_transition_prob()` extended with `model_type = "mlr3"` branch
- `scripts/retrain_all_models.r` utility to re-train all transitions using the new pipeline
- Configurable `max_training_rows` threshold in config YAML for the subsampling fallback

This phase does NOT include:
- Changing parallelism model (Phase 3)
- Block-wise predict optimisation (Phase 4)
- Migrating `raster` → `terra` or CVXR port (Phase 4)

</domain>

<decisions>
## Implementation Decisions

### MLR3 Migration Strategy
- **D-01:** Full replacement of tidymodels in `transition_modelling.r`. mlr3 is used for tuning, final fit, and model serialisation. Tidymodels code is removed (not kept in parallel).
- **D-02:** Use evoland-plus mlr3 branch (`https://github.com/ethzplus/evoland-plus/tree/copilot/integrate-mlr3-library`) as a reference only — read its patterns and adapt them to nascent-lulcc's config/path contract. Do not copy verbatim.
- **D-03:** Learner set: keep the same algorithm family (GLM, RF/ranger, XGBoost) as the existing tidymodels stack. The goal is a structural replacement, not an algorithm change.

### Model Object and Predict Interface
- **D-04:** Add `model_type = "mlr3"` as a new dispatch branch in `predict_saved_transition_prob()` (src/allocation.r). The new branch handles mlr3 Learner objects. All existing tidypredict/butchered branches stay in place until all transitions are re-trained and old files are obsolete.
- **D-05:** mlr3 model objects are saved via `qs::qsave()` (already in `allocation_env.yml`). Each file must be <200 MB on disk (verified via `file.size()`). The save function must record `model_type = "mlr3"` and `predictor_names` in the saved object for the predict dispatcher.
- **D-06:** After all transitions are re-trained, the old predict branches (tidypredict_*, butchered_*) may be removed in a future cleanup. That cleanup is out of scope for Phase 2.

### Existing Trained Models
- **D-07:** Re-train all transitions from scratch using the new mlr3 pipeline. Existing tidymodels/butchered RDS files become obsolete once re-training is complete. No transcoding utility is needed.
- **D-08:** A `scripts/retrain_all_models.r` utility script orchestrates re-training of all transitions across all regions, consuming the same config/env contract established in Phase 1. This replaces the `rebutcher_existing_models.r` originally proposed in the ROADMAP success criteria.

### OOM / Large Transition Handling
- **D-09:** mlr3 is the primary training path. For transitions whose training dataset exceeds `max_training_rows` (a new config key in both `config/local_config.yaml` and `config/hpc_config.yaml`), stratified subsampling is applied before tuning and final fit. The model is trained on the subsample; prediction uses the full model.
- **D-10:** Subsampling is stratified by the binary response variable (transition present/absent) to preserve class balance. Random seed is set from the existing config seed for reproducibility.
- **D-11:** `max_training_rows` is operator-configurable in YAML. A reasonable starting default (e.g., 500,000) can be set; the researcher/planner should confirm based on HPC node memory specs.

### Verification
- **D-12:** Size gate: after saving each model, assert `file.size(output_path) < 200 * 1024^2`. Log a warning (not a hard stop) if exceeded so the run can continue — the operator can investigate later.
- **D-13:** Predict equality check: for each saved mlr3 model, run a 5-row predict on a small fixture dataset and assert that probabilities are in [0, 1] and non-NA. Full comparison to the pre-migration tidymodels output is not required (re-training produces a new model, not a transcoding).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Core files to be modified
- `src/transition_modelling.r` — 3,569-line tidymodels pipeline to be replaced with mlr3; understand current structure before rewriting
- `src/allocation.r:501` — `predict_saved_transition_prob()` dispatcher; add mlr3 branch here; preserve existing branches

### Configuration contract (from Phase 1)
- `src/setup.r` — `get_stage7_runtime_paths()` and `get_config()` are the authoritative config resolvers; all path/env decisions flow through them
- `config/local_config.yaml` — add `max_training_rows` here
- `config/hpc_config.yaml` — add `max_training_rows` here
- `.env.template` — document any new env vars (unlikely for this phase)

### Requirements
- `.planning/REQUIREMENTS.md` §MEM-04 — model size target (<200 MB), original butcher/bundle approach (superseded by mlr3 migration)
- `.planning/REQUIREMENTS.md` §MLR3-01 — mlr3 evaluation requirement (now active)
- `.planning/ROADMAP.md` §Phase 2 — success criteria (note: rebutcher_existing_models.r criterion is replaced by retrain_all_models.r per D-08)

### Reference codebase
- `https://github.com/ethzplus/evoland-plus/tree/copilot/integrate-mlr3-library` — divergent codebase that has started mlr3 integration; use as a reference for mlr3 patterns (Learner construction, tuning, serialisation), NOT as code to copy verbatim

### Phase 1 summaries (context for what's already in place)
- `.planning/phases/01-repair-visibility/01-01-SUMMARY.md` — path/env contract established
- `.planning/phases/01-repair-visibility/01-02-SUMMARY.md` — allocation_env.yml with qs, ps, lobstr packages

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `predict_saved_transition_prob()` (`src/allocation.r:501`) — dispatcher pattern already handles multiple model types via `model_type` field; adding mlr3 is a new branch, not a structural change
- `qs::qsave()` / `qs::qread()` — already in `allocation_env.yml` (MEM-06); use for mlr3 model serialisation
- `get_config()` / `get_stage7_runtime_paths()` (`src/setup.r`) — config/env resolution; use for `max_training_rows` lookup
- `log_msg()` (`src/utils.r`) — structured logging; use for training progress, size gates, subsampling notices

### Established Patterns
- Model objects carry a `model_type` string field and `predictor_names` vector — mlr3 objects must follow the same convention for the predict dispatcher to work
- `restore_ranger_importance_mode()` (`src/allocation.r:343`) exists because of structural inconsistency in current models — mlr3 should eliminate the need for this workaround; keep it for backward compatibility with old files
- Config keys use snake_case YAML, resolved via `get_config()` — `max_training_rows` should follow the same pattern

### Integration Points
- `transition_modelling.r` → saves model files to configured output directory → `allocation.r` reads them at runtime; the file path convention must stay the same
- `retrain_all_models.r` → loops transitions/regions → calls the rewritten `transition_modelling.r` save logic; must consume `get_config()` for paths

</code_context>

<specifics>
## Specific Ideas

- The user explicitly noted that **hyperparameter tuning is fast** — the OOM failure is in the **final full-data fit**, not the tuning step. mlr3's solution should preserve the fast tuning iteration while fixing the final fit memory ceiling.
- Inconsistent model object structures were the immediate pain point in the current stack — the mlr3 migration should produce a uniform object structure so the predict dispatcher can be simplified over time.
- The evoland-plus mlr3 branch is the closest reference for mlr3 integration in this codebase family — researcher should examine it carefully for learner configuration, resampling strategy, and serialisation format.

</specifics>

<deferred>
## Deferred Ideas

- Removing old tidypredict/butchered predict branches from `allocation.r` — deferred to a cleanup pass after all transitions are re-trained (Phase 2 scope ends at adding the mlr3 branch)
- Block-wise `terra::predict()` for per-transition RAM bounds — Phase 4 (PERF-01)
- Full unit/integration test suite — v2 (TEST-01, TEST-02)

</deferred>

---

*Phase: 2-model-size-reduction*
*Context gathered: 2026-05-05*
