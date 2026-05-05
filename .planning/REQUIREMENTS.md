# Requirements: nascent-lulcc

**Defined:** 2026-05-05
**Core Value:** allocation.r completes reliably for all scenarios × regions × timesteps, producing simulated LULC maps without crashing.

## v1 Requirements

### Observability

- [ ] **OBS-01**: RAM profiling reports real values — `rss_before/after/peak` are valid numbers, not "NAMB", on both local and HPC
- [ ] **OBS-02**: When an allocation worker crashes (including SIGKILL), the region log contains a sentinel trace and `diagnose_alloc_crash.sh` surfaces the OOM evidence from `sacct`/`seff`
- [ ] **OBS-03**: `setup_allocation_inputs` and `run_allocation_dinamica` emit structured messages to the per-region log file (not only to stdout)
- [ ] **OBS-04**: Allocation entry runs pre-flight validation — asserts env vars, all packages loadable, model files present, Dinamica binary executable — and fails fast with a single actionable list of all gaps

### Memory & Parallelism

- [ ] **MEM-01**: allocation.r completes without OOM crash for all scenarios × regions × timesteps on HPC
- [ ] **MEM-02**: Memory footprint per worker is bounded — model objects, predictor rasters, and neighbourhood rasters are not duplicated across parallel workers
- [ ] **MEM-03**: `future::multicore` (fork + COW) is used on Linux HPC; `future::multisession` is used on Windows local; backend is selected automatically based on OS
- [ ] **MEM-04**: All model objects saved by `transition_modelling.r` are <200 MB each — achieved via tightened `butcher` axes, `bundle::bundle()` for XGBoost, and `ranger(save.memory=TRUE)`
- [ ] **MEM-05**: Neighbourhood rasters are pre-computed once per region in the parent process and written to scratch as TIF files; workers receive file paths, not in-memory objects
- [ ] **MEM-06**: `allocation_env.yml` includes all packages required for prediction: `r-parsnip`, `r-recipes`, `r-ranger`, `r-xgboost` (pinned 1.7.x), `r-tidypredict`, `r-butcher`, `r-ps`, `r-lobstr`, `r-bundle`, `r-qs`, `r-rhpcblasctl`

### Performance & Resumability

- [ ] **PERF-01**: Transition probability prediction uses block-wise `terra::predict()` or row-chunked sparse prediction — not full data.frame materialisation — so per-transition RAM is bounded regardless of region size
- [ ] **PERF-02**: Predictor Parquet reads inside workers use lazy `arrow::open_dataset()` with column projection — not `read_parquet()` in the parent
- [ ] **PERF-03**: Completed (scenario, timestep, region, transition) combinations are skipped on restart — outputs are written atomically via `<name>.tmp.tif` → `file.rename()`

### Pipeline Correctness

- [ ] **PIPE-01**: `simulation_trans_rates_prep.r` reads LULC demand from the config-driven CSV path (`config[["lulc_demand_path"]]`) — the hardcoded Windows xlsx path is removed
- [ ] **PIPE-02**: The CVXR convex optimisation loop is ported from `src/old/simulation_transition_rates_estimation.R` into `simulation_trans_rates_prep.r` section G and produces valid transition rate tables
- [ ] **PIPE-03**: `calibration_predictor_prep.r` reads terra temp directory from `Sys.getenv("TERRA_TEMP", unset = tempdir())` — not hardcoded `E:/terra_temp`
- [ ] **PIPE-04**: HPC shell scripts use `$USER` in all paths — no hardcoded `bblack` references in `hpc_common.sh`, `setup_environments.sh`, or `.env.template`
- [ ] **PIPE-05**: All active source files (outside `src/old/`) use `terra` only — 73 `raster::` call sites in `lulcc.spatprobmanipulation.r`, `spatial_interventions_prep.r`, and `landscape_pattern_analysis.r` are migrated or removed
- [ ] **PIPE-06**: Intervention YAML files (`config/SSP*_interventions.yml`) reference `inputs/spat_prob_perturb/` paths matching the config schema — not legacy `Data/Spat_prob_perturb_layers/` paths
- [ ] **PIPE-07**: Dinamica EGO log files are written to the central `logs/` directory — not scattered across region work directories

## v2 Requirements

### Model Framework

- **MLR3-01**: Evaluate `mlr3` as a replacement for `tidymodels` in `transition_modelling.r` — only pursued if Phase 2 (MEM-04) leaves models above 200 MB after full `butcher`/`bundle` application

### Testing

- **TEST-01**: Pure-function unit tests for utilities in `src/utils.r` and `src/setup.r` (config loading, path resolution)
- **TEST-02**: One integration test using a synthetic 100×100 cell raster that exercises the full probability map generation path without requiring real data

## Out of Scope

| Feature | Reason |
|---------|--------|
| Generalisation to other study areas | Config and path structure are Peru-specific by design; no multi-study-area use case |
| New simulation scenarios beyond BAU/NAT/CUL/SOC | Scenario framework is fixed for this project cycle |
| Full automated test suite | HPC batch jobs are the deployment mechanism; observability improvements substitute; not justified by project shape |
| CI/CD pipeline | No web layer; SLURM is the execution environment |
| `targets`/`drake` pipeline orchestration | SLURM dependency chaining + per-region resume covers ~80% of the value at <5% of the effort |
| Real-time progress dashboard | Per-region log files + `diagnose_alloc_crash.sh` are sufficient |

## Traceability

Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| OBS-01 | — | Pending |
| OBS-02 | — | Pending |
| OBS-03 | — | Pending |
| OBS-04 | — | Pending |
| MEM-01 | — | Pending |
| MEM-02 | — | Pending |
| MEM-03 | — | Pending |
| MEM-04 | — | Pending |
| MEM-05 | — | Pending |
| MEM-06 | — | Pending |
| PERF-01 | — | Pending |
| PERF-02 | — | Pending |
| PERF-03 | — | Pending |
| PIPE-01 | — | Pending |
| PIPE-02 | — | Pending |
| PIPE-03 | — | Pending |
| PIPE-04 | — | Pending |
| PIPE-05 | — | Pending |
| PIPE-06 | — | Pending |
| PIPE-07 | — | Pending |

**Coverage:**
- v1 requirements: 20 total
- Mapped to phases: 0 (updated during roadmap creation)
- Unmapped: 20 ⚠️

---
*Requirements defined: 2026-05-05*
*Last updated: 2026-05-05 after initial definition*
