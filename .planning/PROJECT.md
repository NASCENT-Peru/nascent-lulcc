# nascent-lulcc

## What This Is

A 7-stage R pipeline for spatially-explicit Land Use/Land Cover Change (LULCC) modelling in Peru, simulating land transitions from 2022–2060 across four scenarios (BAU, NAT, CUL, SOC). The pipeline runs from raw raster data preparation through statistical transition modelling to spatial allocation via the Dinamica EGO engine, and is designed to execute on either a local Windows workstation or the ETH Euler HPC cluster (SLURM).

## Core Value

allocation.r completes reliably for all scenarios × regions × timesteps, producing simulated LULC maps without crashing.

## Requirements

### Validated

- ✓ Environment-aware configuration (local/HPC auto-detection via SLURM vars) — existing
- ✓ 7-stage sequential pipeline with SLURM dependency chaining — existing (stages 1–6 functional)
- ✓ Multi-region stratification (Andes, Amazon, Coast) via regionalization flag — existing
- ✓ Parallel execution within stages via future/furrr — existing
- ✓ Parquet-based predictor data storage and retrieval — existing
- ✓ tidymodels-based transition modelling (GLM/RF/XGBoost) — existing
- ✓ Dinamica EGO 8 integration via processx subprocess — existing
- ✓ Per-region log files and profiling hooks — existing (but RSS tracking broken)

### Active

- [ ] **ALLOC-01**: allocation.r completes without OOM crash for all scenarios × regions × timesteps on HPC
- [ ] **ALLOC-02**: Memory footprint per worker is bounded — model objects, predictor rasters, and neighbourhood rasters do not duplicate across parallel workers
- [ ] **ALLOC-03**: RAM profiling is accurate — `rss_before/after/delta/peak` values are not NAMB on HPC
- [ ] **ALLOC-04**: Allocation crashes surface a full stack trace and structured error in the region log, not a silent `MultisessionFuture interrupted`
- [ ] **ALLOC-05**: Inner allocation functions (`setup_allocation_inputs`, `run_allocation_dinamica`) emit messages to the per-region log file
- [ ] **PIPE-01**: `simulation_trans_rates_prep.r` uses the config-driven CSV path, not the hardcoded Windows xlsx path — runs on HPC without modification
- [ ] **PIPE-02**: CVXR optimisation loop is ported from `src/old/` into `simulation_trans_rates_prep.r` section G and produces transition rate tables
- [ ] **PIPE-03**: `calibration_predictor_prep.r` reads terra temp dir from env var, not hardcoded `E:/terra_temp`
- [ ] **PIPE-04**: HPC shell scripts use `$USER` instead of hardcoded `bblack` in all paths
- [ ] **PIPE-05**: All active source files (`lulcc.spatprobmanipulation.r`, `spatial_interventions_prep.r`) migrated from legacy `raster` to `terra` — no double memory usage or type coercion at runtime
- [ ] **PIPE-06**: Intervention YAML files (`config/SSP*_interventions.yml`) use paths matching the config schema (`inputs/spat_prob_perturb/`) rather than legacy Swiss-context `Data/Spat_prob_perturb_layers/` paths
- [ ] **PIPE-07**: Dinamica EGO log files are written to the central `logs/` directory, not scattered across region work directories
- [ ] **MLR3-01**: Evaluate mlr3 as replacement for tidymodels in `transition_modelling.r` — target: model objects <100MB (currently >1GB per model)

### Out of Scope

- Generalisation to study areas other than Peru — config and path structure are Peru-specific by design
- New simulation scenarios beyond BAU/NAT/CUL/SOC — scenario framework is fixed for this project
- Automated test suite — no tests exist and building one is not the priority; observability improvements (logging, error surfaces) substitute
- CI/CD pipeline — HPC batch jobs are the deployment mechanism; no web layer to integrate

## Context

**Crash profile (from allocation logs, 2026-04-27/28):**

The allocation step fails with `MultisessionFuture interrupted` (OOM kill of worker process) after ~3 minutes on local runs. Key profiling findings from the last successful-but-slow run (worker_21444_andes.log):

| Stage | Time per transition | Notes |
|-------|--------------------|-|
| `model_load` | 4–16s | gc_max_vcells=12,125MB after one model load — objects are >1GB in memory |
| `predictor_load` | 10–22s | Parquet reads per transition |
| `nhood_extract` | ~78s | Neighbourhood rasters — consistent high cost |
| `predict` | **385–472s** | By far the dominant cost — pixel-wise RF/XGBoost prediction over full region |

RAM profiling is broken: `rss_before=NAMB` in all newer runs, so actual peak memory is unknown.

**Key known blockers (from codebase analysis):**
1. Model objects are >1GB each; `gc_max_vcells=12,125MB` after loading a single model. `r-butcher` and `r-tidypredict` are already in the stack but not fully applied.
2. `future::multisession` workers each hold a full copy of the rasters/models — no shared memory.
3. Neighbourhood rasters are cached per-run but the cache strategy may not survive across parallel workers.
4. `simulation_trans_rates_prep.r` CVXR loop is a placeholder — allocation may be running on stale transition rate tables.
5. `raster` package is used in 73 active call sites alongside `terra`; `allocation_env.yml` does not include `r-raster`, creating a latent hard failure.

**Pipeline topology:**
Stages 1–6 (data prep through spatial interventions) are functional. Stage 7 (allocation → Dinamica) is the current blocker. The missing `submit_scenario_preparation.sh` and `submit_simulation_setup.sh` scripts suggest stages 5–6 have HPC submission gaps.

**Environment:**
- Local dev: Windows 11, E:/ data drive, interactive R sessions
- HPC: ETH Euler cluster, SLURM, micromamba conda, large-memory nodes available
- R 4.3–4.4 across multiple isolated conda environments per stage
- External binary dependency: Dinamica EGO 8 (`DINAMICA_EGO_8_HOME` must be set; not in `.env.template`)

## Constraints

- **Tech stack**: R + Dinamica EGO 8 — pipeline is tightly coupled to Dinamica's model format and CLI interface; no swap-out possible
- **Compatibility**: tidymodels model objects must remain usable for prediction even if training artifacts are stripped — `butcher` and `tidypredict` transformations must preserve `predict()` compatibility
- **Data locality**: All raster and Parquet data lives on the HPC scratch filesystem (`/cluster/scratch/bblack/nascent-lulcc`) or local E:/ — not in the repository; pipeline assumes data is pre-staged
- **Parallelism**: `future::multisession` is the parallelism model for allocation — any memory reduction strategy must be compatible with R's multiprocess/multisession worker model
- **mlr3 migration**: Secondary priority — evaluate feasibility before committing; must produce prediction outputs compatible with the allocation probability map generation step

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Use `future::multisession` for allocation parallelism | Was available, standard in R ecosystem | ⚠️ Revisit — causes full object duplication per worker; may need `multicore` (fork-based, Linux-only) or redesign |
| tidymodels as modelling framework | Good ecosystem, unified interface | ⚠️ Revisit — model objects >1GB even after butchering; mlr3 under evaluation |
| Sequential prediction per transition within each worker | Simple logic, easier to debug | ⚠️ Revisit — `predict` stage is 385–472s per transition; potential for batching or rasterisation |
| Separate conda env per pipeline stage | Avoids package conflicts (xgboost version pinning) | — Pending evaluation |
| Per-region log files in `logs/` | Observability for parallel workers | ✓ Good — essential for diagnosing crashes |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-05-05 after initialization*
