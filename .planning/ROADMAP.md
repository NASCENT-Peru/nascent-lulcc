# Roadmap: nascent-lulcc

## Overview

Hardening the 7-stage Peruvian LULCC pipeline so that `src/allocation.r` runs reliably end-to-end on the ETH Euler HPC for all scenarios × regions × timesteps. The journey starts by lighting up the diagnostic dashboard (visible RSS, structured logs, fail-fast pre-flight, post-mortem tooling) so every subsequent change is measurable. With visibility in place we shrink the >1GB model objects to a budget that makes parallel forks affordable, then switch to fork-based `multicore` parallelism with shared neighbourhood rasters so per-worker RAM stays bounded. Finally we tackle the dominant 385–472s `predict` cost, lazy I/O, atomic resumability, and clean up the latent correctness gaps (CVXR port, raster→terra migration, intervention paths) that block a clean run.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Repair & Visibility** - Fix broken profiling, structured logs, env/path repairs, pre-flight validation, post-mortem tooling, Singularity container for Dinamica EGO 8 *(completed 2005-05-05; INFRA-01 / SC6 reopened by 2005-05-15 live verification — see Phase 1.1)*
- [ ] **Phase 1.1: Fix Dinamica Launch Contract** *(INSERTED 2005-05-15; 4/4 mainline plans landed 2005-05-17; gap-closure plans 01.1-05 + 01.1-06 added 2005-05-17 to close Open Issue 1 — DinamicaConsole std::exception under rebuilt .sif blocking INFRA-01 SC2 + MEM-06 SC5)* - Repaired the seven structural defects in the Dinamica-on-HPC launch path. Launch-contract mechanics (D-101–D-108, D-112, D-114) all landed and the cross-language drift safety net is in place. Live `--live` smoke does not yet exit 0 against the rebuilt `.sif` — DinamicaConsole crashes with `std::exception` regardless of `.ego` content; the 2005-05-17 ldd diagnostic FALSIFIED the library-compat hypothesis. Gap-closure Plan 01.1-05 captures diagnostic evidence on Euler; Plan 01.1-06 applies the resulting targeted fix to the `.def` and re-verifies the live smoke exits 0.
- [x] **Phase 2: Model Size Reduction** - Replace tidymodels with mlr3 in transition_modelling.r; save models as .qs via qs::qsave() with ranger save.memory=TRUE; all artefacts <200 MB *(completed 2005-05-07)*
- [ ] **Phase 3: Parallelism & Memory Architecture** - Switch to fork-based multicore on Linux, share nhood rasters, eliminate OOM
- [x] **Phase 3.1: Allocation Correctness & Dinamica Integration** *(INSERTED 2005-05-22; completed 2005-05-25)* - Fix model over-loading (filter to active transitions), eliminate phantom NA TIFs (nomatch=NULL), fix Dinamica HPC fallback guard, re-export demand CSV from XLSX (`clean_numeric` couldn't parse Excel-introduced thousand-separator format), and verify end-to-end allocation including the Dinamica CA step via dedicated smoke test
- [ ] **Phase 3.2: Transition Pipeline Consistency** *(INSERTED 2005-05-22)* - Harden the end-to-end flow of viable transitions from identification through feature selection, modelling, rate preparation, and allocation so each stage operates on exactly the same transition set with no silent additions, drops, or mismatches.
- [ ] **Phase 3.3: Probability Map Saturation & Allocation Throughput** *(INSERTED 2005-05-26)* - Address Dinamica's low allocation throughput: probability maps don't contain sufficient non-zero values, causing Expander/Patcher to place only a small fraction of the change matrix demand (e.g., 4,477 of hundreds of thousands of requested transitions in Phase 3.1 BAU costa_peruana smoke).
- [ ] **Phase 3.4: Stale Pipeline Artifact Re-run** *(INSERTED 2005-05-29)* - Regenerate simulation trans_rates CSVs and alloc_params on HPC with Phase 3.2-corrected code; unblocks Phase 3.3 operator gate. Root cause: rate CSVs were generated before `forbidden_from_classes` config key and `load_unmodelled_transitions()` were active, so id_trans=34 (mining→high_intensity_agricultural) survived into the active set and triggered the ALLOC-08 hard stop.
- [ ] **Phase 3.5: Reduce Allocation Memory Floor** *(INSERTED 2026-06-22)* - Cut the ~80GB per-region predictor-preload floor via lazy per-transition Parquet reads (target ~10–20GB) and let large-transition ranger prediction use spare cores (`num.threads>1`); flips allocation from memory-bound to core-bound so cheaper/more nodes can each run a region. (Split from the removed Phase 5; multi-scenario node packing merged into Phase 4.)
- [ ] **Phase 4: End-to-End Correctness & Performance** - Block-wise predict, lazy parquet, atomic resumability, terra migration, CVXR port; parallelise the full scenario sweep across Rundeck nodes (S2 multi-scenario packing)

## Phase Details

### Phase 1: Repair & Visibility
**Goal**: Operator can diagnose any allocation failure within minutes by reading the per-region log and a single post-mortem command — RSS values are real, paths work on HPC out of the box, and missing prerequisites fail fast with a single actionable list.
**Depends on**: Nothing (first phase)
**Requirements**: OBS-01, OBS-02, OBS-03, OBS-04, PIPE-01, PIPE-03, PIPE-04, PIPE-07, MEM-06, INFRA-01
**Success Criteria** (what must be TRUE):
  1. Every per-region log shows real numeric `rss_before/after/delta/peak` values (no "NAMB") on both Windows local and Linux HPC.
  2. When an allocation worker is SIGKILLed, `diagnose_alloc_crash.sh` surfaces OOM evidence from `sacct`/`seff`/cgroup memory and a sentinel trace exists in the region log.
  3. Running `allocation.r` with a missing env var, missing R package, missing model file, or missing Dinamica binary aborts before any work with one consolidated list of all gaps.
  4. `simulation_trans_rates_prep.r` and `calibration_predictor_prep.r` execute on a fresh HPC checkout with no manual path edits; HPC shell scripts contain no hardcoded `black` references; Dinamica EGO logs land in `logs/`.
  5. Activating `allocation_env.yml` resolves on HPC with all prediction-time packages (`r-parsnip`, `r-recipes`, `r-ranger`, `r-xgboost`, `r-tidypredict`, `r-butcher`, `r-ps`, `r-lobstr`, `r-bundle`, `r-qs`, `r-rhpcblasctl`) loadable via `library()`.
  6. Dinamica EGO 8 executes successfully inside a Singularity container on Euler — a minimal allocation model completes and `exec_dinamica()` can invoke it via `DINAMICA_EGO_8_HOME`; container definition and build instructions are committed to the repository.
**Plans**: 4 plans

Plans:
- [x] 01-01-PLAN.md - Establish the shared R path/env contract and repair the active hardcoded path hotspots.
- [x] 01-02-PLAN.md - Canonicalize `allocation_env` and align HPC shell/bootstrap scripts to the shared contract.
- [x] 01-03-PLAN.md - Add consolidated Stage 7 pre-flight, portable RSS profiling, crash sentinels, and one-command diagnosis.
- [x] 01-04-PLAN.md - Unify Dinamica local/HPC backends, centralize Dinamica logs, and add the Euler smoke-test contract.

### Phase 1.1: Fix Dinamica Launch Contract *(INSERTED 2005-05-15)*
**Goal**: A fresh operator on Euler can run `bash scripts/setup_environments.sh --env allocation_env --non-interactive` and `bash scripts/smoke_test_dinamica.sh --live ...` to a green light, against a `.sif` rebuildable from this repo with no manual workstation transfer; production allocation workers invoke Dinamica via the supported `bin/DinamicaEGO.sh` launcher and any Dinamica error (including silent `std::exception`) returns a non-zero exit so operators see failures within minutes.
**Depends on**: Phase 1
**Requirements**: INFRA-01 (reopened), OBS-02 (fix detection contract), PIPE-04 (env install root), MEM-06 (smoke-test fixture sanity)
**Success Criteria** (what must be TRUE):
  1. `apptainer build dinamica-ego-8.sif dinamica/container/rocker-geospatial-dinamica.def` succeeds on Euler with no GHCR auth, no workstation transfer step, and no manual edits — bootstraps from a publicly-pullable upstream image.
  2. `scripts/smoke_test_dinamica.sh --live --runtime auto --artifact "$DINAMICA_EGO_8_HOME" --ego dinamica/dinamica_model/<minimal>.ego --require-log-under logs` exits 0 with a non-empty `logs/dinamica-smoke-<ts>.log` containing actual Dinamica model output (not `std::exception`).
  3. When Dinamica prints `Dinamica EGO exited with an error`, `terminate called after throwing`, or `std::exception` to stdout, both `scripts/smoke_test_dinamica.sh` and `src/dinamica_utils.r:exec_dinamica()` return non-zero — regardless of the subprocess exit code.
  4. `src/dinamica_utils.r:resolve_dinamica_launch()` builds the HPC launch command as `apptainer exec --home <staged-home> --env DINAMICA_EGO_8_TEMP_DIR=<staged-tmp> <sif> bash -c 'cd /opt/dinamica/usr && bin/DinamicaEGO.sh <abs-model-path>'` and creates the staged-home + staged-tmp directories with `.dinamica_ego_8.conf` seeded.
  5. The committed smoke-test model in `dinamica/dinamica_model/` is a true minimal `.ego` (binary) that DinamicaConsole accepts; the production allocation flow's encode-then-execute path is unchanged.
  6. `scripts/setup_environments.sh` refuses to fall back to `$PROJECT_ROOT/.envs` when running on HPC and `HPC_SCRATCH_ROOT` is unset; exits non-zero with a single actionable message.
  7. `dinamica/container/README.md` and `docs/README_HPC.md` document the new build flow + launch command shape; the workstation `docker save` workaround is demoted to a fallback note.
**Plans:** 6 plans (4 mainline + 2 gap-closure)

Plans:

**Wave 1** *(parallel — no files_modified overlap)*
- [x] 01.1-01-PLAN.md — R-side launch contract (D-104/D-105/D-106) + exec_dinamica() three-pattern error grep (D-107/D-108) + unit tests.
- [x] 01.1-02-PLAN.md — Shell-side smoke test mirror (LAUNCH_CMD + exit code 5 grep) + setup_environments.sh three-signal HPC-detection refusal (D-112/D-113) + pure-bash test.
- [x] 01.1-03-PLAN.md — Rewrite rocker-geospatial-dinamica.def (D-101/D-102) + add smoketest.ego{-decoded} sibling fixtures (D-109/D-110/DD-2); includes operator gate for live Euler build + smoke verification.

**Wave 2** *(blocked on Wave 1 completion)*
- [x] 01.1-04-PLAN.md — Cross-language mirror assertion test (RESEARCH Target 7) + dinamica/container/README.md and docs/README_HPC.md updates (D-114).

**Gap-closure Wave 1** *(added 2005-05-17 to close Open Issue 1 from 01.1-03-SUMMARY.md; closes INFRA-01 SC2 + MEM-06 SC5)*
- [ ] 01.1-05-PLAN.md — Diagnose Open Issue 1: operator runs the four-step diagnostic ladder inside the rebuilt `.sif` on Euler (ls Data/ tree, strings DINAMICA_EGO_8_* env vars, strace openat() before std::exception, diff fresh AppImage extract vs in-.sif tree); commits six evidence files under `.planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/`; synthesises into FINDINGS.md with a hypothesis ranking + Proposed Fix for Plan 06.

**Gap-closure Wave 2** *(blocked on 01.1-05 completion)*
- [ ] 01.1-06-PLAN.md — Apply the Proposed Fix from diagnostics/FINDINGS.md to `dinamica/container/rocker-geospatial-dinamica.def`; operator rebuilds the .sif on Euler and runs the live `--live` smoke test (exit 0 required); updates 01.1-03-SUMMARY Open Issue 1 → RESOLVED, refreshes runtime-caveat callouts in both READMEs, marks INFRA-01 + MEM-06 Complete in REQUIREMENTS.md.

### Phase 2: Model Size Reduction
**Goal**: A freshly trained or re-saved transition model loads in well under a second and consumes a small fraction of worker RAM, so the parent process stays small enough that fork-based parallelism becomes viable.
**Depends on**: Phase 1
**Requirements**: MEM-04
**Success Criteria** (what must be TRUE — superseded by CONTEXT.md D-01–D-13; the mlr3 migration is the actual plan):
  1. Every model artefact written by `transition_modelling.r` is <200 MB on disk (saved via qs::qsave() with ranger save.memory=TRUE; size gate D-12 logs a warning if exceeded).
  2. `scripts/retrain_all_models.r` re-trains all ~140–160 transition-region pairs using the new mlr3 pipeline; existing tidymodels .rds files become obsolete once re-training completes (D-07, D-08).
  3. `predict_saved_transition_prob()` in `allocation.r` dispatches to the new mlr3 branch when model_type == "mlr3"; existing branches stay for old files (D-04).
  4. A 5-row predict sanity check after saving each mlr3 model asserts probabilities in [0,1] and non-NA (D-13).
**Plans**: 4 plans

Plans:

**Wave 1**
- [x] 02-01-PLAN.md - Add mlr3 packages to allocation_env.yml and max_training_rows to both config YAMLs.

**Wave 2** *(blocked on Wave 1 completion — can run in parallel with each other)*
- [x] 02-02-PLAN.md - Rewrite transition_modelling.r inner stack with mlr3 (train_mlr3_transition, build_mlr3_learner, size gate, sanity check).
- [x] 02-03-PLAN.md - Add mlr3 dispatch branch to predict_saved_transition_prob() in allocation.r; update model loader to qs::qread() for .qs files.

**Wave 3** *(blocked on Wave 2 completion)*
- [x] 02-04-PLAN.md - Create scripts/retrain_all_models.r re-training utility (--force, --dry-run, --region flags).

**Cross-cutting constraints:**
- All plans: model_type = "mlr3" string is the dispatch key; must appear in every saved model list and predict branch
- 02-02 + 02-03: save format contract — {model_type, predictor_names, response_levels, learner} list saved by 02-02, read by 02-03 loader
- 02-02 + 02-03: file extension contract — build_transition_model_path() returns .qs (02-02); loader detects .qs and uses qs::qread() (02-03)

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
**Plans**: 3 plans

Plans:

**Wave 1**
- [ ] 03-01-PLAN.md - Establish the runtime control plane: automatic plan selection, native thread pinning, strict-globals dev mode, cgroup logging, and smoke-run filters.

**Wave 2** *(blocked on Wave 1 completion)*
- [ ] 03-02-PLAN.md - Refactor allocation workers to use parent-preloaded models and path-based neighbourhood rasters with parent baseline logging.

**Wave 3** *(blocked on Wave 2 completion)*
- [ ] 03-03-PLAN.md - Add the reproducible HPC smoke-run wrapper and automated verifier proving no OOM, bounded worker RSS, and readable output.

### Phase 3.1: Allocation Correctness & Dinamica Integration *(INSERTED 2005-05-22)*
**Goal**: The first end-to-end allocation that includes Dinamica running under apptainer produces a real posterior.tif (not an anterior copy), model loading is bounded to only the transitions active in the current scenario/year, and the probability map save writes exactly the TIFs that were predicted (no phantom NA-prob placeholders).
**Depends on**: Phase 3 (smoke run infrastructure and probability map generation confirmed working)
**Requirements**: MEM-01 (model memory), INFRA-01 (Dinamica HPC launch)
**Success Criteria** (what must be TRUE):
  1. `load_allocation_models` log line shows N models loaded where N equals the number of active transitions from trans_rates_df — not the total number of .qs files on disk. For the BAU × costa_peruana × 2022→2026 smoke, this means ≤26 models (not 38).
  2. Probability map save writes exactly the TIFs that had a valid prediction — no 1-cell NA-prob placeholder TIFs for transitions missing a model. `find probability_map_dir/ -name '*.tif' | wc -l` equals the number of active transitions that have both a non-zero rate and a fitted model.
  3. `submit_allocation_dinamica_only.sh` completes: `posterior.tif` exists in the region work_dir, is a valid GeoTIFF, and contains values different from `anterior.tif` (i.e., Dinamica actually ran, not the fallback copy).
  4. The `DINAMICA_START model=<fallback-copy>` breadcrumb does NOT appear in any HPC allocation log. On HPC the breadcrumb is `DINAMICA_START model=<path/to/allocation.ego>`.
**Plans**: 1 plan

Plans:
- [x] 03.1-01-PLAN.md - Apply smoke_test_dinamica.sh bind-mount fix (Fix 4b), verify all four src/ fixes, commit, and run Dinamica-only smoke on HPC to confirm posterior.tif ≠ anterior.tif.

### Phase 3.2: Transition Pipeline Consistency *(INSERTED 2005-05-22)*
**Goal**: Each stage of the LULCC pipeline operates on exactly the same set of viable transitions — `transition_identification.r` deduces theoretical candidates per region from historic maps, `transition_feature_selection.r` confirms statistical viability and writes the definitive viable set, `transition_modelling.r` produces a fitted model for every viable transition, `simulation_trans_rates_prep.r` computes future rates over the viable set with scenario narrative exclusions applied as an explicit logged filter, and `allocation.r` loads only the models whose transitions appear in the active rate table — eliminating information loss and silent mismatches between stages.
**Depends on**: Phase 3.1
**Requirements**: PIPE-01, PIPE-02
**Success Criteria** (what must be TRUE):
  1. The transitions output by `transition_identification.r` for each region exactly match the candidate set fed into `transition_feature_selection.r` — no silent additions or drops between the two files.
  2. The viable transitions written by `transition_feature_selection.r` to `viable_transitions_lists.csv` are the complete input set used by `transition_modelling.r` — every viable transition has a fitted model; no silently skipped transitions.
  3. `simulation_trans_rates_prep.r` computes future rates using only the viable set from `viable_transitions_lists.csv`; scenario narrative exclusions are applied as a documented, explicit filter on top of that set (not a separate hard-coded list) and the excluded transitions are logged.
  4. `allocation.r` loads exactly the transition models whose transitions appear in the active rate table for the current region × scenario × timestep — model count equals active-rate-table row count for the BAU × costa_peruana × 2022→2026 smoke (no over-loading, no missing models).
  5. A cross-stage audit (script or documented manual check) confirms transition sets are consistent across all four pipeline stages for at least one reference region × scenario combination.
**Plans**: 3 plans

Plans:

**Wave 1**
- [ ] 03.2-01-PLAN.md — Fix `final_summary` crash bug in `transition_feature_selection.r`; move hardcoded `year_steps`, `scalars`, and mining prohibition to config; verify PIPE-01/PIPE-02 no-regression.

**Wave 2** *(blocked on Wave 1 completion)*
- [ ] 03.2-02-PLAN.md — Add structured AUDIT log lines at stage 2→3 boundary (`transition_modelling.r`), stage 4 (`simulation_trans_rates_prep.r`), and promote stage 5 warning to stop() (`allocation.r`).

**Wave 3** *(blocked on Wave 2 completion)*
- [ ] 03.2-03-PLAN.md — Create `scripts/audit_transition_pipeline.r` cross-stage consistency checker covering all four pipeline stages (id_trans set-difference via Stages 1, 2, 3, 4 artifacts).

### Phase 3.3: Probability Map Saturation & Allocation Throughput *(INSERTED 2005-05-26)*
**Goal**: Dinamica's Expander + Patcher allocate substantially all of the requested change matrix cells (target: ≥90% of demanded transitions placed) for a reference scenario × region × timestep, rather than the ~1% throughput observed in Phase 3.1 (4,477 of hundreds of thousands of requested cells in BAU × costa_peruana × 2022→2026). The root cause — probability maps not containing enough non-zero high-probability cells to support the demanded volume of transitions — is identified and remediated through one or more of: model calibration adjustments, prediction post-processing (e.g., probability map smoothing or floor), Patcher parameter tuning (`Mean_Patch_Size`, `Patch_Size_Variance`, `Patch_Isometry`), Expander `Perc_expander` rebalancing, or probability map generation improvements.
**Depends on**: Phase 3.2 (clean transition pipeline required so allocation throughput is the only variable being studied)
**Requirements**: ALLOC-06, ALLOC-07, ALLOC-08, ALLOC-09, ALLOC-10
**Success Criteria** (what must be TRUE):
  1. The fraction of requested transition cells actually placed by Dinamica (Expander + Patcher combined) reaches ≥90% for at least one reference scenario × region × timestep (e.g., BAU × costa_peruana × 2022→2026); residual unallocated counts logged in the run summary.
  2. Probability map quality metrics are computed and logged per transition (e.g., fraction of cells with probability > 0, distribution percentiles, spatial autocorrelation) and meet thresholds documented in PROJECT.md.
  3. A diagnostic script (e.g., `scripts/diagnose_allocation_saturation.r`) compares the requested change matrix against the actual posterior transition counts and identifies which transitions are saturation-limited vs successfully allocated.
  4. Root cause (probability map sparseness, Patcher parameters, Expander rebalancing, or other) is documented in the phase summary with quantitative evidence; remediation applied is reproducible.
**Plans**: 5 plans

Plans:

**Wave 1**
- [x] 03.3-01-PLAN.md — Region reconnaissance (regions.json discovery), saturation_threshold + saturation_exempt config keys, src/saturation_diagnostics.r helper module.

**Wave 2** *(blocked on Wave 1 completion)*
- [x] 03.3-02-PLAN.md — Single-source writer refactor in src/allocation.r (filter persistence + zero-rate, alloc_params stop, TIF writer no-skip, inline saturation hook).

**Wave 3** *(blocked on Wave 2 completion — Plan 03 and Plan 04 run in parallel)*
- [x] 03.3-03-PLAN.md — Standalone scripts/diagnose_allocation_saturation.r post-hoc diagnostic with saturation_class classification + Dinamica-log Remaining-Transitions cross-validation.
- [x] 03.3-04-PLAN.md — Test coverage: tests/testthat/test-allocation-single-source-writer.R (ALLOC-06/08) + tests/testthat/test-saturation-diagnostics.R (ALLOC-07/10).

**Wave 4** *(blocked on Wave 2, Wave 3 completion; has operator-gate checkpoint)*
- [ ] 03.3-05-PLAN.md — Add ALLOC-06..10 to REQUIREMENTS.md; update ROADMAP.md; operator-gate live HPC verification (BAU x all Peruvian regions x 2022->2026); write 03.3-SUMMARY.md.

### Phase 3.4: Stale Pipeline Artifact Re-run *(INSERTED 2005-05-29)*
**Goal**: Simulation trans_rates CSVs and alloc_params.csv on Euler are regenerated with Phase 3.2-corrected code so id_trans=34 (and any other no-model transition) is excluded from the active set before allocation runs — removing the ALLOC-08 blocker for Phase 3.3's operator gate.
**Depends on**: Phase 3.2 (code fixes landed), Phase 3.3 Plans 01–04 (ALLOC-08 hard stop in place)
**Requirements**: PIPE-08, PIPE-09
**Success Criteria** (what must be TRUE):
  1. `git log --oneline -1` on Euler shows a commit ≥ `9166fc4` (Phase 3.2 Plan 01) before any SLURM job is submitted.
  2. After `submit_simulation_trans_rates_estimation.sh` completes, `AUDIT stage=4` log lines for every region show `forbidden_excluded > 0`.
  3. `grep ",34," outputs/transition_tables/simulation-lulc-areas-scalar-9.0x/BAU/<region>/BAU-<region>-trans_rates-2022.csv` returns no matches for all 4 regions.
  4. After `submit_calibrate_allocation_parameters.sh` completes, alloc_params.csv for each region covers all active id_trans values with no gaps (verified by spot-check Rscript).
  5. `sbatch scripts/submit_allocation_smoke.sh` completes for all 4 regions with no ALLOC-08 stop.
**Plans**: 1 plan

Plans:
- [ ] 03.4-01-PLAN.md — Operator pre-flight, trans_rates re-run, alloc_params calibration, and allocation smoke for all 4 regions

### Phase 3.5: Reduce Allocation Memory Floor *(INSERTED 2026-06-22)*
**Goal:** Shrink per-region allocation memory and prediction wall time so the sweep scales — two changes to `src/allocation.r`'s prediction path:

1. **Cut the ~80 GB predictor-preload floor (highest leverage).** Make per-transition prediction read predictors lazily from the Parquet dataset per chunk (`arrow::open_dataset()` + column projection on the cell-id keys) instead of preloading the full ~68M × 38 region table into `region_pred_dt`. Target floor ~10–20 GB, flipping the job from memory-bound to core-bound so a highmem node holds 4+ regions and the cheap `40vCPU-40GB` flavors can each run a region.
2. **Threaded prediction.** The run is memory-bound with idle cores; let the large-transition ranger prediction use `num.threads > 1` (currently pinned to 1 via `pin_native_threads_to_one`) when few workers run on a node, bounded by `threads × workers ≤ cores`, to cut the 61M-row prediction time (τ).

Multi-scenario node packing (the original Phase 5 "Goal 1" / S2) is merged into Phase 4 — not in scope here.

**Depends on:** Phase 3
**Requirements**: PERF-01, PERF-02
**Plans:** 2/3 plans executed

Plans:

**Wave 1**
- [x] 03.5-01-PLAN.md — Scoped threaded ranger prediction: get_allocation_predict_num_threads() resolver + num.threads on both predict sites (Goal 2 throughput).

**Wave 2** *(blocked on Wave 1 — shared src/allocation.r ownership)*
- [x] 03.5-02-PLAN.md — Lazy per-from-class predictor reads: load_from_class_predictor_data() + cache + ALLOCATION_PREDICTOR_LAZY escape hatch (Goal 1 memory floor).

**Wave 3** *(blocked on Wave 1+2; operator-gated HPC smoke)*
- [ ] 03.5-03-PLAN.md — selva_andina smoke validation: before/after profile deltas vs job 571309, two-stage isolation, equivalence + residual-floor assessment.

### Phase 4: End-to-End Correctness & Performance
**Goal**: All four scenarios run to completion across all regions and timesteps, with `predict` no longer dominating wall time, restarts skipping completed work atomically, the latent correctness gaps (raster/terra split, missing CVXR loop, drifted intervention paths) closed, and the full sweep parallelised efficiently across Rundeck nodes rather than run as a single serial process.
**Depends on**: Phase 3
**Requirements**: PERF-01, PERF-02, PERF-03, PIPE-02, PIPE-05, PIPE-06
**Success Criteria** (what must be TRUE):
  1. End-to-end allocation completes for all four scenarios (BAU, NAT, CUL, SOC) × all regions (Andes, Amazon, Coast) × all timesteps, producing simulated LULC TIFs.
  2. Per-transition `predict` wall time drops measurably from the 385–472s baseline (target: at least 3× faster) using block-wise `terra::predict()` or row-chunked sparse prediction; per-transition peak RAM is bounded and independent of region size.
  3. Restarting an interrupted allocation skips already-completed (scenario, timestep, region, transition) outputs; no partial `.tif` is ever observed by a downstream step (atomic `.tmp.tif` → `file.rename`).
  4. Predictor reads inside workers use `arrow::open_dataset()` with column projection — no `read_parquet()` of full datasets in the parent.
  5. `simulation_trans_rates_prep.r` section G runs the ported CVXR convex optimisation and emits valid transition rate tables that allocation consumes without manual fixup.
  6. No active source file outside `src/old/` calls `raster::` (verifiable via grep returning zero hits in `lulcc.spatprobmanipulation.r`, `spatial_interventions_prep.r`, `landscape_pattern_analysis.r`); intervention YAMLs reference `inputs/spat_prob_perturb/` paths matching the config schema.
  7. The full sweep can be launched as independent per-scenario jobs (scoped via `ALLOCATION_PROFILE_SCENARIO`) that either pack 2–3 onto one shared node (explicit `--mem`, non-`--exclusive`) or run one-per-node, collapsing the 4× scenario-serial wall time toward a single scenario's chain; a documented launcher / submission pattern exists (S2, merged from the removed Phase 5).
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → 4

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Repair & Visibility | 4/4 | Complete | 2005-05-05 |
| 1.1. Fix Dinamica Launch Contract | 4/6 | Mainline contract complete; gap-closure 01.1-05 + 01.1-06 pending (closes INFRA-01 SC2 / MEM-06 SC5 on Open Issue 1) | partial — mainline 2005-05-17 |
| 2. Model Size Reduction | 4/4 | Complete | 2005-05-07 |
| 3. Parallelism & Memory Architecture | 0/TBD | Not started | - |
| 3.1. Allocation Correctness & Dinamica Integration | 1/1 | Complete — 4,477 cells changed (103→105); demand CSV fix unblocked optimizer | 2005-05-25 |
| 3.2. Transition Pipeline Consistency | 0/3 | Not started (INSERTED 2005-05-22) | - |
| 3.3. Probability Map Saturation & Allocation Throughput | 4/5 | Wave 1-3 plans complete; operator gate pending (03.3-05) | partial — Wave 1-3 2005-05-26 |
| 3.4. Stale Pipeline Artifact Re-run | 0/TBD | Not started (INSERTED 2005-05-29) | - |
| 3.5. Reduce Allocation Memory Floor | 2/3 | In Progress|  |
| 4. End-to-End Correctness & Performance | 0/TBD | Not started | - |
