---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: executing
stopped_at: Phase 3.1 planned (2026-05-22) — 03.1-01-PLAN.md written; Task 1 (Fix 4b + commit) ready to execute; waiting for operator HPC submission
last_updated: "2026-05-22T00:00:00Z"
last_activity: 2026-05-22 -- Phase 3.1 plan written; four correctness fixes documented (Fixes 1–4 in src/ already applied; Fix 4b in smoke_test_dinamica.sh pending Task 1)
progress:
  total_phases: 6
  completed_phases: 4
  total_plans: 19
  completed_plans: 15
  percent: 75
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-05)

**Core value:** allocation.r completes reliably for all scenarios × regions × timesteps, producing simulated LULC maps without crashing.
**Current focus:** Phase 3.1 — allocation-correctness-and-dinamica

## Current Position

Phase: 3.1 (allocation-correctness-and-dinamica)
Plan: 1 of 1 — 03.1-01-PLAN.md written; Task 1 ready to execute
Status: Four correctness fixes identified; Fixes 1–4 already applied to src/; Fix 4b (smoke_test_dinamica.sh bind-mount) is Task 1; Task 2 = HPC Dinamica-only smoke
Last activity: 2026-05-22 -- 03.1-01-PLAN.md written; apptainer Read-only FS error root cause confirmed (HPC_SCRATCH_ROOT not bound); all four fixes documented

### Roadmap Evolution

- Phase 3.1 inserted after Phase 3 (2026-05-22, URGENT): Job 364249 confirmed R pipeline; Dinamica never ran (fallback guard fired on HPC); model preload wasteful (38 vs 26 active); phantom TIFs from nomatch=NA; fixes applied, Dinamica-only smoke test ready

Progress: [████████░░] 80%

## Performance Metrics

**Velocity:**

- Total plans completed: 12
- Average duration: ~13 min/plan (across Phases 1, 1.1, 2)
- Total execution time: ~2.6 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 1. Repair & Visibility | 4 | ~0.5h | ~8 min |
| 1.1. Fix Dinamica Launch Contract | 7 (01.1-01–07) | ~45 min combined (+ ~4h operator-side on Euler across Plans 03/06/07) | ~11 min |
| 2. Model Size Reduction | 4 (02-01, 02-02, 02-03, 02-04) | ~71 min combined | ~18 min |
| 3. Parallelism & Memory Architecture | 0 | — | — |
| 4. End-to-End Correctness & Performance | 0 | — | — |

**Recent Trend:**

- Last 5 plans: 01.1-03, 01.1-04, 01.1-05, 01.1-06, 01.1-07
- Trend: Phase 1.1 gap-closure fully executed across Plans 05–07; H8 root cause (circular singleton init in libBase.so) confirmed after 6 diagnostic iterations and fixed via LD_PRELOAD interceptor; live smoke exits 0.

*Updated after each plan completion*

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- Phase 01.1 (01.1-04): Mark INFRA-01 / MEM-06 as NOT YET COMPLETE in REQUIREMENTS traceability despite the Phase 1.1 contract work landing — INFRA-01 SC2 (live `--live` smoke exits 0) and MEM-06 SC5 remain gated on Open Issue 1 (DinamicaConsole std::exception under rocker/r-ver:4.5.3 Ubuntu Noble base). Marking them complete would mislead operators. Phase 01.1 gap-closure / phase 01.2 closes them.
- Phase 01.1 (01.1-04): Cross-language launch-contract mirror test (`tests/testthat/test-dinamica-launch-contract-mirror.R`) added as the standing drift-mitigation safety net for any future divergence between `src/dinamica_utils.r:resolve_dinamica_launch()` and `scripts/smoke_test_dinamica.sh` LAUNCH_CMD; documented in 01.1-PATTERNS.md as a reusable pattern for any future R/shell mirror pair.
- Phase 01.1 (01.1-04): Deprecated `apptainer exec <sif> DinamicaConsole <model>` references retained in both READMEs ONLY inside explicit DEPRECATED markers — helps future operators searching the docs for the old shape find the new contract; complies with the plan's acceptance criterion allowing such references in "Recent Changes" / "previous behavior" notes.
- Phase 01.1 (01.1-03): Phase 1.1 launch-contract mechanics (D-101–D-108, D-112, D-114) all landed and validated mechanically; live `--live` smoke exits 5 (D-107 grep caught DinamicaConsole std::exception) — D-107 detection contract proven live, AppImage/base-library compat fix deferred.
- Phase 2 (02-04): Region filter via temp CSV override — read viable_transitions_lists.csv, filter by region_name, write tempfile, override config[["viable_transitions_lists"]] before calling transition_modelling(); dry-run respects region filter because filter runs first.
- Phase 2 (02-02): Save `at$learner` not AutoTuner to avoid 3-5x size bloat; ranger `save.memory=TRUE` + `importance="none"` hardcoded (primary size reduction); step_normalize not replicated (classif.glmnet is scale-invariant); T-02-03 path injection guard in train_mlr3_transition().
- Phase 2: Full mlr3 replacement of tidymodels in `transition_modelling.r`; `classif.glmnet` (not plain GLM) for logistic regression; `qs::qsave()` with `{model_type="mlr3", predictor_names, response_levels, learner}` list; `max_training_rows` YAML key for subsampling fallback.
- Init: Linux HPC switches to `future::multicore`; Windows local stays on `future::multisession`.
- Init: Pre-compute neighbourhood rasters in parent and pass file paths to workers (not SpatRaster objects).
- Phase 1: `get_stage7_runtime_paths()` is the single resolver for HPC-specific paths; all env overrides flow through it.
- Phase 1: `DINAMICA_EGO_8_HOME` is treated as absolute path to the external `.sif` on Euler (not a wrapper).

### Pending Todos

None yet.

### Blockers/Concerns

- `MultisessionFuture interrupted` (OOM SIGKILL) at ~3 minutes locally — the project's defining failure mode; addressed structurally across Phases 2–3.
- Phase 1 HPC-only verification gates (live Euler smoke test, live env solve, live SIGKILL test) pending operator confirmation — tracked in 01-HUMAN-UAT.md. **Phase 1.1 now closed** — the INFRA-01 live smoke gate is satisfied (exit 0).

## Deferred Items

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| Model framework | MLR3-01 (tidymodels → mlr3 migration) | Conditional on Phase 2 outcome | 2026-05-05 |
| Testing | TEST-01, TEST-02 (unit + integration tests) | v2 | 2026-05-05 |

## Session Continuity

Last session: 2026-05-19
Stopped at: Completed 01.1-07-PLAN.md — Phase 1.1 fully closed; Open Issue 1 RESOLVED; live --live smoke exits 0; INFRA-01 + MEM-06 Complete
Resume file: None
