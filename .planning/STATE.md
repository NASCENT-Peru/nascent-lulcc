# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-05)

**Core value:** allocation.r completes reliably for all scenarios × regions × timesteps, producing simulated LULC maps without crashing.
**Current focus:** Phase 1 — Repair & Visibility

## Current Position

Phase: 1 of 4 (Repair & Visibility)
Plan: 4 of 4 in current phase
Status: Verifying
Last activity: 2026-05-05 — all 4 plans complete; phase verification starting

Progress: [░░░░░░░░░░] 0%

## Performance Metrics

**Velocity:**
- Total plans completed: 0
- Average duration: —
- Total execution time: 0 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 1. Repair & Visibility | 0 | — | — |
| 2. Model Size Reduction | 0 | — | — |
| 3. Parallelism & Memory Architecture | 0 | — | — |
| 4. End-to-End Correctness & Performance | 0 | — | — |

**Recent Trend:**
- Last 5 plans: —
- Trend: —

*Updated after each plan completion*

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- Init: Stay on tidymodels (mlr3 deferred to v2 — only triggered if Phase 2 cannot reach <200 MB models).
- Init: Linux HPC switches to `future::multicore`; Windows local stays on `future::multisession`.
- Init: Pre-compute neighbourhood rasters in parent and pass file paths to workers (not SpatRaster objects).

### Pending Todos

None yet.

### Blockers/Concerns

- `MultisessionFuture interrupted` (OOM SIGKILL) at ~3 minutes locally — the project's defining failure mode; addressed structurally across Phases 1–3.
- RSS profiling is silently broken (`rss_before=NAMB`); blocks empirical validation of every later memory change. First fix in Phase 1.
- Six prediction-time R packages are missing from `allocation_env.yml`; HPC predict path is latently broken. Closed in Phase 1 via MEM-06.

## Deferred Items

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| Model framework | MLR3-01 (tidymodels → mlr3 migration) | Conditional on Phase 2 outcome | 2026-05-05 |
| Testing | TEST-01, TEST-02 (unit + integration tests) | v2 | 2026-05-05 |

## Session Continuity

Last session: 2026-05-05
Stopped at: Roadmap created — ready to plan Phase 1
Resume file: None
