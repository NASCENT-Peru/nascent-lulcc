# Requirements: nascent-lulcc

**Defined:** 2026-05-05
**Core Value:** allocation.r completes reliably for all scenarios × regions × timesteps, producing simulated LULC maps without crashing.

## v1 Requirements

### Observability

- [ ] **OBS-01**: RAM profiling reports real values — `rss_before/after/peak` are valid numbers, not "NAMB", on both local and HPC
- [x] **OBS-02**: When an allocation worker crashes (including SIGKILL), the region log contains a sentinel trace and `diagnose_alloc_crash.sh` surfaces the OOM evidence from `sacct`/`seff` *(closed Phase 1.1: D-107 three-pattern grep in `src/dinamica_utils.r:.check_dinamica_post_run()` + `scripts/smoke_test_dinamica.sh` exit-5 block; validated live on Euler 2026-05-17 — the grep correctly caught the Open Issue 1 `std::exception` with exit 5)*
- [ ] **OBS-03**: `setup_allocation_inputs` and `run_allocation_dinamica` emit structured messages to the per-region log file (not only to stdout)
- [ ] **OBS-04**: Allocation entry runs pre-flight validation — asserts env vars, all packages loadable, model files present, Dinamica binary executable — and fails fast with a single actionable list of all gaps

### Memory & Parallelism

- [ ] **MEM-01**: allocation.r completes without OOM crash for all scenarios × regions × timesteps on HPC
- [ ] **MEM-02**: Memory footprint per worker is bounded — model objects, predictor rasters, and neighbourhood rasters are not duplicated across parallel workers
- [ ] **MEM-03**: `future::multicore` (fork + COW) is used on Linux HPC; `future::multisession` is used on Windows local; backend is selected automatically based on OS
- [ ] **MEM-04**: All model objects saved by `transition_modelling.r` are <200 MB each — achieved via tightened `butcher` axes, `bundle::bundle()` for XGBoost, and `ranger(save.memory=TRUE)`
- [ ] **MEM-05**: Neighbourhood rasters are pre-computed once per region in the parent process and written to scratch as TIF files; workers receive file paths, not in-memory objects
- [x] **MEM-06**: `allocation_env.yml` includes all packages required for prediction: `r-parsnip`, `r-recipes`, `r-ranger`, `r-xgboost` (pinned 1.7.x), `r-tidypredict`, `r-butcher`, `r-ps`, `r-lobstr`, `r-bundle`, `r-qs`, `r-rhpcblasctl` *(closed Phase 1.1 / Plan 01.1-07: live `--live` smoke against `dinamica/dinamica_model/smoketest.ego` exits 0 on rebuilt .sif; Open Issue 1 resolved — see `01.1-07-SUMMARY.md`)*

### Performance & Resumability

- [ ] **PERF-01**: Transition probability prediction uses block-wise `terra::predict()` or row-chunked sparse prediction — not full data.frame materialisation — so per-transition RAM is bounded regardless of region size
- [ ] **PERF-02**: Predictor Parquet reads inside workers use lazy `arrow::open_dataset()` with column projection — not `read_parquet()` in the parent
- [ ] **PERF-03**: Completed (scenario, timestep, region, transition) combinations are skipped on restart — outputs are written atomically via `<name>.tmp.tif` → `file.rename()`

### Infrastructure

- [x] **INFRA-01**: Dinamica EGO 8 runs inside a Singularity container on ETH Euler HPC — container built from the `ethzplus/rocker-geospatial-dinamica` Rocker image (reference: https://github.com/ethzplus/rocker-geospatial-dinamica), tested with a minimal allocation model, and invocable from `exec_dinamica()` via `DINAMICA_EGO_8_HOME` pointing to the container binary *(closed Phase 1.1 / Plan 01.1-07: all SCs verified; live smoke exits 0; Open Issue 1 resolved — see `01.1-07-SUMMARY.md`)*

### Pipeline Correctness

- [x] **PIPE-01**: `simulation_trans_rates_prep.r` reads LULC demand from the config-driven CSV path (`config[["lulc_demand_path"]]`) — the hardcoded Windows xlsx path is removed *(pre-satisfied — verified no xlsx/xls regression in Phase 3.2; grep src/simulation_trans_rates_prep.r src/allocation.r returns zero matches)*
- [x] **PIPE-02**: The CVXR convex optimisation loop is ported from `src/old/simulation_transition_rates_estimation.R` into `simulation_trans_rates_prep.r` section G and produces valid transition rate tables *(pre-satisfied — full CVXR loop `optimize_region_scenario`/`build_mats`/`run_scalar_optimization_loop` confirmed present; Phase 3.2 hardens its inputs only)*
- [ ] **PIPE-03**: `calibration_predictor_prep.r` reads terra temp directory from `Sys.getenv("TERRA_TEMP", unset = tempdir())` — not hardcoded `E:/terra_temp`
- [x] **PIPE-04**: HPC shell scripts use `$USER` in all paths — no hardcoded `black` references in `hpc_common.sh`, `setup_environments.sh`, or `.env.template` *(closed Phase 1.1 / Plan 01.1-02: `setup_environments.sh` rewrite + three-signal HPC-detection refusal D-112; `tests/shell/test-setup-environments-hpc-refusal.sh` PASS:5/FAIL:0)*
- [ ] **PIPE-05**: All active source files (outside `src/old/`) use `terra` only — 73 `raster::` call sites in `lulcc.spatprobmanipulation.r`, `spatial_interventions_prep.r`, and `landscape_pattern_analysis.r` are migrated or removed
- [ ] **PIPE-06**: Intervention YAML files (`config/SSP*_interventions.yml`) reference `inputs/spat_prob_perturb/` paths matching the config schema — not legacy `Data/Spat_prob_perturb_layers/` paths
- [ ] **PIPE-07**: Dinamica EGO log files are written to the central `logs/` directory — not scattered across region work directories
- [ ] **PIPE-08**: Simulation trans_rates CSVs must be regenerated whenever `transition_modelling()` or `transition_feature_selection()` produces new outputs — the pipeline has no automated staleness guard between these stages; a stale trans_rates CSV can include transitions with no model, bypassing the ALLOC-08 hard stop
- [ ] **PIPE-09**: The HPC deployment procedure includes an explicit `git pull` + `git status` check before any SLURM job submission to confirm the working tree matches the intended branch head — stale HPC tree is the root cause of the Phase 3.3 ALLOC-08 regression

### Allocation Throughput

- [ ] **ALLOC-06**: The four work_dir Dinamica artifacts (`trans_rates.csv`, `expansion_table.csv`, `patcher_table.csv`, `probability_map_dir/*.tif`) are written from a single filtered, id_trans-sorted source data frame; persistence and zero-rate rows are dropped before any artifact is written; no `%03d_id_trans_*.tif` prefix gaps exist
- [ ] **ALLOC-07**: Per-transition allocation throughput (placed cells / demanded cells) reaches >=`saturation_threshold` (default 0.90) for every active transition in BAU x all Peruvian regions x 2022->2026, except transitions enumerated in `saturation_exempt`; residual unallocated counts logged per region in `saturation_summary.csv` + `AUDIT stage=saturation ...` line
- [ ] **ALLOC-08**: `alloc_params.csv` row coverage of the active `id_trans` set is enforced as a hard precondition: if any active `id_trans` lacks an `alloc_params` row, allocation `stop()`s with the full missing list
- [ ] **ALLOC-09**: A standalone `scripts/diagnose_allocation_saturation.r` produces a post-hoc per-transition saturation report comparing the demanded change matrix against actual posterior counts, reusing the same metric helpers as the inline summary
- [ ] **ALLOC-10**: Probability-map quality metrics (coverage; P50/P90/P95/P99/max; demand-vs-capacity ratio) are computed and logged for every active transition during each region run

### Run Completeness (Phase 3.6)

- [ ] **RUN-01**: A single scenario (BAU) runs to completion across every region × every timestep in `simulation_year_steps` — for each (region, timestep) a valid GeoTIFF `posterior.tif` exists and the national `posterior_<year>.tif` mosaic exists for every timestep; the run-manifest asserts `count == regions × timesteps` and emits PASS/INCOMPLETE
- [ ] **RUN-02**: The allocation driver derives its timestep pairs from the authoritative `simulation_year_steps` schedule (not the `step_length` `seq()`), reaching 2060 (10 steps); the run only proceeds when a matching `BAU-<region>-trans_rates-<year_ant>.csv` exists for every `year_ant` (blocking pre-flight gate; PIPE-08/09 git-head check enforced before submission)
- [ ] **RUN-03**: Per-region parallel jobs are decoupled — each region chains each timestep on its OWN `region_<suffix>/posterior.tif` (never the shared national mosaic), so concurrent region jobs cannot clobber a shared path; one highmem/fat node per region with adequate `--mem` (never bare sbatch onto the ~93GB compute partition)
- [ ] **RUN-04**: The national `posterior_<year>.tif` series is assembled post-hoc for every timestep from per-region posteriors (sole writer of the national path under parallel jobs), byte-comparable to the inline writer it replaces
- [ ] **RUN-05**: Every (region, timestep) posterior DIFFERS from its anterior (rules out the degenerate "nothing happened" case); saturation is aggregated and reported but does NOT gate completion; trajectory plausibility QA flags hard all-one-class degeneracy (→ INCOMPLETE) and soft demand-mismatch (→ warning)
- [ ] **RUN-06**: Per-timestep posterior/mosaic writes are atomic (`<name>.tmp.tif` → `file.rename()`) so a job killed mid-write cannot leave a half-written posterior; on restart the launcher resumes each region from the next incomplete timestep (timestep-level resume) rather than re-running completed work

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
| OBS-01 | Phase 1 | Pending |
| OBS-02 | Phase 1 + Phase 1.1 | Complete (closed Plan 01.1-04 via 01.1-01 D-107 grep + 01.1-02 exit-5 shell mirror) |
| OBS-03 | Phase 1 | Pending |
| OBS-04 | Phase 1 | Pending |
| MEM-01 | Phase 3 | Pending |
| MEM-02 | Phase 3 | Pending |
| MEM-03 | Phase 3 | Pending |
| MEM-04 | Phase 2 | Pending |
| MEM-05 | Phase 3 | Pending |
| MEM-06 | Phase 1 + Phase 1.1 | Complete (closed Phase 1.1 / Plan 01.1-07; live --live smoke exit 0; CalculateRExpression block executed successfully in smoketest.ego) |
| PERF-01 | Phase 4 | Pending |
| PERF-02 | Phase 4 | Pending |
| PERF-03 | Phase 4 | Pending |
| INFRA-01 | Phase 1 + Phase 1.1 | Complete (closed Phase 1.1 / Plan 01.1-07; all SCs verified; live --live smoke exit 0; Open Issue 1 resolved by LD_PRELOAD fix for H8 circular init bug in libBase.so) |
| PIPE-01 | Phase 3.2 | Complete (pre-satisfied; verified no regression in Phase 3.2 — grep src/simulation_trans_rates_prep.r src/allocation.r returns zero xlsx/xls matches) |
| PIPE-02 | Phase 3.2 | Complete (pre-satisfied; full CVXR loop confirmed present; Phase 3.2 hardens inputs only) |
| PIPE-03 | Phase 1 | Pending |
| PIPE-04 | Phase 1 + Phase 1.1 | Complete (closed Plan 01.1-02 via setup_environments.sh rewrite + D-112 refusal; test-setup-environments-hpc-refusal.sh PASS:5/FAIL:0) |
| PIPE-05 | Phase 4 | Pending |
| PIPE-06 | Phase 4 | Pending |
| PIPE-07 | Phase 1 | Pending |
| PIPE-08 | Phase 3.4 | Pending |
| PIPE-09 | Phase 3.4 | Pending |
| ALLOC-06 | Phase 3.3 | Pending |
| ALLOC-07 | Phase 3.3 | Pending |
| ALLOC-08 | Phase 3.3 | Pending |
| ALLOC-09 | Phase 3.3 | Pending |
| ALLOC-10 | Phase 3.3 | Pending |
| RUN-01 | Phase 3.6 | Pending |
| RUN-02 | Phase 3.6 | Pending |
| RUN-03 | Phase 3.6 | Pending |
| RUN-04 | Phase 3.6 | Pending |
| RUN-05 | Phase 3.6 | Pending |
| RUN-06 | Phase 3.6 | Pending |

**Coverage:**
- v1 requirements: 28 total
- Mapped to phases: 28 ✓
- Unmapped: 0

**Phase totals:**
- Phase 1 (Repair & Visibility): 10 requirements — OBS-01, OBS-02, OBS-03, OBS-04, MEM-06, INFRA-01, PIPE-01, PIPE-03, PIPE-04, PIPE-07
- Phase 2 (Model Size Reduction): 1 requirement — MEM-04
- Phase 3 (Parallelism & Memory Architecture): 4 requirements — MEM-01, MEM-02, MEM-03, MEM-05
- Phase 3.3 (Allocation Throughput): 5 requirements - ALLOC-06, ALLOC-07, ALLOC-08, ALLOC-09, ALLOC-10
- Phase 4 (End-to-End Correctness & Performance): 6 requirements — PERF-01, PERF-02, PERF-03, PIPE-02, PIPE-05, PIPE-06

---
*Requirements defined: 2026-05-05*
*Last updated: 2026-05-17 — Phase 1.1 closed PIPE-04 + OBS-02; INFRA-01 + MEM-06 partial (deferred to gap-closure on 01.1-03 Open Issue 1)*
*Last updated: 2026-05-22 — Phase 3.2 closed PIPE-01 + PIPE-02 (pre-satisfied, verified no regression)*
*Last updated: 2026-05-26 - Phase 3.3 added ALLOC-06..10 (saturation alignment + diagnostic surfaces); status pending operator gate*
