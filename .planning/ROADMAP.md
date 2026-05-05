# Roadmap: nascent-lulcc

## Overview

Hardening the 7-stage Peruvian LULCC pipeline so that `src/allocation.r` runs reliably end-to-end on the ETH Euler HPC for all scenarios × regions × timesteps. The journey starts by lighting up the diagnostic dashboard (visible RSS, structured logs, fail-fast pre-flight, post-mortem tooling) so every subsequent change is measurable. With visibility in place we shrink the >1GB model objects to a budget that makes parallel forks affordable, then switch to fork-based `multicore` parallelism with shared neighbourhood rasters so per-worker RAM stays bounded. Finally we tackle the dominant 385–472s `predict` cost, lazy I/O, atomic resumability, and clean up the latent correctness gaps (CVXR port, raster→terra migration, intervention paths) that block a clean run.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [ ] **Phase 1: Repair & Visibility** - Fix broken profiling, structured logs, env/path repairs, pre-flight validation, post-mortem tooling, Singularity container for Dinamica EGO 8
- [ ] **Phase 2: Model Size Reduction** - Shrink saved transition models from >1GB to <200MB via butcher/bundle/qs
- [ ] **Phase 3: Parallelism & Memory Architecture** - Switch to fork-based multicore on Linux, share nhood rasters, eliminate OOM
- [ ] **Phase 4: End-to-End Correctness & Performance** - Block-wise predict, lazy parquet, atomic resumability, terra migration, CVXR port

## Phase Details

### Phase 1: Repair & Visibility
**Goal**: Operator can diagnose any allocation failure within minutes by reading the per-region log and a single post-mortem command — RSS values are real, paths work on HPC out of the box, and missing prerequisites fail fast with a single actionable list.
**Depends on**: Nothing (first phase)
**Requirements**: OBS-01, OBS-02, OBS-03, OBS-04, PIPE-01, PIPE-03, PIPE-04, PIPE-07, MEM-06, INFRA-01
**Success Criteria** (what must be TRUE):
  1. Every per-region log shows real numeric `rss_before/after/delta/peak` values (no "NAMB") on both Windows local and Linux HPC.
  2. When an allocation worker is SIGKILLed, `diagnose_alloc_crash.sh` surfaces OOM evidence from `sacct`/`seff`/cgroup memory and a sentinel trace exists in the region log.
  3. Running `allocation.r` with a missing env var, missing R package, missing model file, or missing Dinamica binary aborts before any work with one consolidated list of all gaps.
  4. `simulation_trans_rates_prep.r` and `calibration_predictor_prep.r` execute on a fresh HPC checkout with no manual path edits; HPC shell scripts contain no hardcoded `bblack` references; Dinamica EGO logs land in `logs/`.
  5. Activating `allocation_env.yml` resolves on HPC with all prediction-time packages (`r-parsnip`, `r-recipes`, `r-ranger`, `r-xgboost`, `r-tidypredict`, `r-butcher`, `r-ps`, `r-lobstr`, `r-bundle`, `r-qs`, `r-rhpcblasctl`) loadable via `library()`.
  6. Dinamica EGO 8 executes successfully inside a Singularity container on Euler — a minimal allocation model completes and `exec_dinamica()` can invoke it via `DINAMICA_EGO_8_HOME`; container definition and build instructions are committed to the repository.
**Plans**: 4 plans

Plans:
- [x] 01-01-PLAN.md - Establish the shared R path/env contract and repair the active hardcoded path hotspots.
- [x] 01-02-PLAN.md - Canonicalize `allocation_env` and align HPC shell/bootstrap scripts to the shared contract.
- [x] 01-03-PLAN.md - Add consolidated Stage 7 pre-flight, portable RSS profiling, crash sentinels, and one-command diagnosis.
- [x] 01-04-PLAN.md - Unify Dinamica local/HPC backends, centralize Dinamica logs, and add the Euler smoke-test contract.

### Phase 2: Model Size Reduction
**Goal**: A freshly trained or re-saved transition model loads in well under a second and consumes a small fraction of worker RAM, so the parent process stays small enough that fork-based parallelism becomes viable.
**Depends on**: Phase 1
**Requirements**: MEM-04
**Success Criteria** (what must be TRUE):
  1. Every model artefact written by `transition_modelling.r` is <200 MB on disk (verifiable via `ls -lh` on the model output directory).
  2. A `rebutcher_existing_models.r` utility re-emits all currently trained models below the same 200 MB ceiling without re-training.
  3. After loading a single butchered+bundled model in a fresh R session, `gc_max_vcells` stays under 2 GB (current baseline: ~12 GB).
  4. A predict-equality check on a 5-row sample confirms the reduced model returns identical probabilities to the pre-butcher original.
**Plans**: TBD

### Phase 3: Parallelism & Memory Architecture
**Goal**: A full allocation run on HPC completes for at least one scenario × region × timestep combination with bounded per-worker RAM and no OOM kills, by switching to copy-on-write `multicore` and passing file paths instead of in-memory raster objects to workers.
**Depends on**: Phase 2
**Requirements**: MEM-01, MEM-02, MEM-03, MEM-05
**Success Criteria** (what must be TRUE):
  1. `allocation.r` selects `future::multicore` automatically on Linux HPC and `future::multisession` on Windows local, with no manual config switch.
  2. At least one scenario × region × timestep combination runs to completion on HPC at the planned `--mem`/CPU budget without triggering the cgroup OOM-killer.
  3. RSS profiling (Phase 1) shows per-worker private memory bounded — no worker exceeds a documented per-worker budget — and `models_list` is loaded exactly once in the parent.
  4. Neighbourhood rasters exist as TIF files on scratch before any worker starts; workers receive character paths and call `terra::rast()` themselves; `options(future.globals.onReference = "error")` passes during a dev run.
  5. BLAS, data.table, arrow, and xgboost native thread counts are all pinned to 1 before `future::plan()` is invoked.
**Plans**: TBD

### Phase 4: End-to-End Correctness & Performance
**Goal**: All four scenarios run to completion across all regions and timesteps, with `predict` no longer dominating wall time, restarts skipping completed work atomically, and the latent correctness gaps (raster/terra split, missing CVXR loop, drifted intervention paths) closed.
**Depends on**: Phase 3
**Requirements**: PERF-01, PERF-02, PERF-03, PIPE-02, PIPE-05, PIPE-06
**Success Criteria** (what must be TRUE):
  1. End-to-end allocation completes for all four scenarios (BAU, NAT, CUL, SOC) × all regions (Andes, Amazon, Coast) × all timesteps, producing simulated LULC TIFs.
  2. Per-transition `predict` wall time drops measurably from the 385–472s baseline (target: at least 3× faster) using block-wise `terra::predict()` or row-chunked sparse prediction; per-transition peak RAM is bounded and independent of region size.
  3. Restarting an interrupted allocation skips already-completed (scenario, timestep, region, transition) outputs; no partial `.tif` is ever observed by a downstream step (atomic `.tmp.tif` → `file.rename`).
  4. Predictor reads inside workers use `arrow::open_dataset()` with column projection — no `read_parquet()` of full datasets in the parent.
  5. `simulation_trans_rates_prep.r` section G runs the ported CVXR convex optimisation and emits valid transition rate tables that allocation consumes without manual fixup.
  6. No active source file outside `src/old/` calls `raster::` (verifiable via grep returning zero hits in `lulcc.spatprobmanipulation.r`, `spatial_interventions_prep.r`, `landscape_pattern_analysis.r`); intervention YAMLs reference `inputs/spat_prob_perturb/` paths matching the config schema.
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → 4

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Repair & Visibility | 0/4 | Not started | - |
| 2. Model Size Reduction | 0/TBD | Not started | - |
| 3. Parallelism & Memory Architecture | 0/TBD | Not started | - |
| 4. End-to-End Correctness & Performance | 0/TBD | Not started | - |
