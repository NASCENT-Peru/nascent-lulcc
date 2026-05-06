# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-05)

**Core value:** allocation.r completes reliably for all scenarios × regions × timesteps, producing simulated LULC maps without crashing.
**Current focus:** Phase 2 — Model Size Reduction

## Current Position

Phase: 2 of 4 (Model Size Reduction)
Plan: 0 of TBD in current phase
Status: Ready to plan
Last activity: 2026-05-05 — Phase 1 complete (user approved); advancing to Phase 2

Progress: [██░░░░░░░░] 25%

## Performance Metrics

**Velocity:**
- Total plans completed: 4
- Average duration: ~8 min/plan
- Total execution time: ~0.5 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 1. Repair & Visibility | 4 | ~0.5h | ~8 min |
| 2. Model Size Reduction | 0 | — | — |
| 3. Parallelism & Memory Architecture | 0 | — | — |
| 4. End-to-End Correctness & Performance | 0 | — | — |

**Recent Trend:**
- Last 5 plans: 01-01, 01-02, 01-03, 01-04
- Trend: Consistent completion, TDD RED→GREEN across all plans

*Updated after each plan completion*

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- Init: Stay on tidymodels (mlr3 deferred to v2 — only triggered if Phase 2 cannot reach <200 MB models).
- Init: Linux HPC switches to `future::multicore`; Windows local stays on `future::multisession`.
- Init: Pre-compute neighbourhood rasters in parent and pass file paths to workers (not SpatRaster objects).
- Phase 1: `get_stage7_runtime_paths()` is the single resolver for HPC-specific paths; all env overrides flow through it.
- Phase 1: `DINAMICA_EGO_8_HOME` is treated as absolute path to the external `.sif` on Euler (not a wrapper).

### Pending Todos

None yet.

### Blockers/Concerns

- `MultisessionFuture interrupted` (OOM SIGKILL) at ~3 minutes locally — the project's defining failure mode; addressed structurally across Phases 2–3.
- Phase 1 HPC-only verification gates (live Euler smoke test, live env solve, live SIGKILL test) pending operator confirmation — tracked in 01-HUMAN-UAT.md.

## Deferred Items

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| Model framework | MLR3-01 (tidymodels → mlr3 migration) | Conditional on Phase 2 outcome | 2026-05-05 |
| Testing | TEST-01, TEST-02 (unit + integration tests) | v2 | 2026-05-05 |

## Session Continuity

Last session: 2026-05-05
Stopped at: Phase 1 approved complete — ready to plan Phase 2
Resume file: None
