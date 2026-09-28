---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: executing
stopped_at: "Completed 05-13-PLAN.md (wave 5 of 7 merged; next: 05-14 + 05-15)"
last_updated: "2026-09-28T09:40:00.000Z"
last_activity: 2026-09-28 -- Phase 05 wave 5 complete (05-13)
progress:
  total_phases: 12
  completed_phases: 9
  total_plans: 52
  completed_plans: 45
  percent: 75
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-05)

**Core value:** allocation.r completes reliably for all scenarios × regions × timesteps, producing simulated LULC maps without crashing.
**Current focus:** Phase 05 — integrate-spatial-interventions-branch-and-stage-interventio

## Current Position

Phase: 05 (integrate-spatial-interventions-branch-and-stage-interventio) — EXECUTING
Plan: 13 of 16
Status: Executing Phase 05
Last activity: 2026-09-28 -- Phase 05 wave 5 complete (05-13)

### Roadmap Evolution

- Phase 3.1 inserted after Phase 3 (2026-05-22, URGENT): Job 364249 confirmed R pipeline; Dinamica never ran (fallback guard fired on HPC); model preload wasteful (38 vs 26 active); phantom TIFs from nomatch=NA; fixes applied, Dinamica-only smoke test ready
- Phase 3.2 inserted after Phase 3.1 (2026-05-22, URGENT): Viable transition set found to drift silently across pipeline stages (identification → feature selection → modelling → rate prep → allocation); phase hardens end-to-end consistency so each stage operates on exactly the same transition set
- Phase 3.3 inserted after Phase 3.2 (2026-05-26, URGENT): Dinamica allocation throughput observed at ~1% in Phase 3.1 (4,477 of hundreds of thousands of requested cells placed); root cause likely probability maps with too few non-zero values to support the demanded volume; phase diagnoses and remediates
- Phase 3.5 inserted after Phase 3 (2026-06-22): Reduce the allocation memory floor — (a) lazy per-transition Parquet predictor reads to cut the ~80GB preload floor to ~10–20GB (memory-bound → core-bound), and (b) threaded ranger prediction using spare cores. Multi-scenario node packing (S2) folded into Phase 4's goal + success criteria. (Replaces the briefly-added Phase 5, which was split: Goals 2+3 → Phase 3.5, Goal 1 → Phase 4.)

Progress: [██████████] 98%

- Phase 03.6 inserted after Phase 3: Complete single-scenario end-to-end run (all regions x all timesteps) (URGENT)
- Phase 5 added (2026-09-22): Integrate colleague's `spatial_interventions` branch into the much-advanced `main`; check spatial-intervention masks (`D:\C.3_Modelling\nascent-lulcc-agg\inputs\spat_prob_perturb`) against the intervention config files and plan their HPC file-system placement

## Performance Metrics

**Velocity:**

- Total plans completed: 20
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
| 03.5 | 3 | - | - |
| 03.6 | 5 | - | - |

**Recent Trend:**

- Last 5 plans: 01.1-03, 01.1-04, 01.1-05, 01.1-06, 01.1-07
- Trend: Phase 1.1 gap-closure fully executed across Plans 05–07; H8 root cause (circular singleton init in libBase.so) confirmed after 6 diagnostic iterations and fixed via LD_PRELOAD interceptor; live smoke exits 0.

*Updated after each plan completion*
| Phase 03.6 P01 | 6 min | 3 tasks | 1 files |
| Phase 03.6 P02 | 4 min | 2 tasks | 2 files |
| Phase 03.6 P03 | 7 min | 2 tasks | 2 files |
| Phase 03.6 P04 | ~15 min | 2 tasks | 2 files |
| Phase 05 P01 | 10 min | 2 tasks | 18 files |
| Phase 05 P02 | 25min | 2 tasks | 2 files |
| Phase 05 P03 | 12 min | 3 tasks | 27 files |
| Phase 05 P04 | 20min | 2 tasks | 3 files |
| Phase 05 P05 | 35min | 3 tasks | 6 files |
| Phase 05 P06 | ~2h | 3 tasks | 0 files |
| Phase 05 P10 | ~6 min | 2 tasks | 3 files |
| Phase 05 P11 | ~40 min | 3 tasks | 3 files |
| Phase 05 P12 | ~35 min | 3 tasks | 3 files |
| Phase 05 P13 | ~50 min | 3 tasks | 5 files |

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
- [Phase 03.6]: Phase 03.6 (03.6-01): allocation driver builds timestep pairs from simulation_year_steps (10 steps to 2060), not step_length seq() (9 steps to 2058) — Rate CSVs were generated against simulation_year_steps; step_length seq() requests year_ant tables that do not exist
- [Phase 03.6]: Phase 03.6 (03.6-01): single-region (length(region_inputs)==1) allocation runs chain on their own region posterior and skip the national mosaic write; mosaic moves to post-hoc Plan 03 assembler — Under per-region parallel jobs (D-01) concurrent regions would clobber the shared posterior_<year>.tif and chain on a single-region-extended mosaic
- [Phase 03.6]: Phase 03.6 (03.6-01): Dinamica-written posterior.tif (dinamica_utils.r:861) left unwrapped for atomic writes — written by exec_dinamica subprocess, not R; only R-side anterior + national mosaic writes use write_raster_atomic — Dinamica contract is out of scope per CONTEXT; no R terra::writeRaster exists at that site
- [Phase 03.6]: Phase 03.6 (03.6-02): per-region SLURM fan-out via an explicit bash loop (not --array) so per-region --partition/--mem can differ; region job ids colon-joined into a multi-parent afterok dependency for the national-mosaic-assembly job — An array can't express per-region partition/mem differences (forest-dominated regions need a fat node); master_pipeline.sh only ever chained single-parent afterok, so afterok:<id1>:<id2>:... is new in the repo
- [Phase 03.6]: Phase 03.6 (03.6-02): timestep resume (D-09) scans region_<suffix>/posterior.tif over the posterior years (tail of simulation_year_steps) and exports ALLOCATION_YEAR_POST_FILTER for the first incomplete year; a fully-complete region is skipped — Each timestep writes posterior_<year_end>, so completeness is scanned over the year-ends; resume avoids re-running hours of completed timesteps and Plan 01 D-10 atomic writes guarantee the scan only sees fully-written posteriors
- [Phase ?]: Phase 03.6 (03.6-03): post-hoc assemble_national_mosaic.r is the SOLE writer of national posterior_<year>.tif under per-region parallel jobs — iterates tail(simulation_year_steps,-1), merges per-region region_<suffix>/posterior.tif via terra::merge + write_raster_atomic (INT2U/LZW), byte-comparable to the inline writer it replaces
- [Phase ?]: Phase 03.6 (03.6-03): mosaic full_extent derived once from the initial-LULC raster (aggregated_lulc_dir for simulation_start_year), not a per-year anterior — the inline writer's current_lulc_path was the previous national mosaic carrying the same initial extent, so this is byte-comparable and needs no on-disk per-year national anterior the post-hoc job cannot assume exists
- [Phase ?]: Phase 03.6 (03.6-03): assembler refuses partial mosaics — any missing region posterior for a year emits AUDIT stage=mosaic status=missing and skips that year (no partial national raster); partial completeness is Plan 04's manifest job (T-036-08)
- [Phase 03.6]: Run completion declared by data not logs: run_manifest.r PASS only when the full region x timestep matrix exists + valid GeoTIFFs + differs-from-anterior + national mosaics present + count==regions*timesteps + no hard degeneracy; saturation reported (AUDIT) but never gates (D-06/D-07/D-08)
- [Phase 03.6]: Plausibility (D-08b) via terra::freq: per-cell hard all-one-class degeneracy => INCOMPLETE; national final-year per-class fraction mismatch vs tools/simulation_lulc_areas_2060.csv => soft warning (absent table skipped, never INCOMPLETE)
- [Phase ?]: Phase 5: merged origin/spatial_interventions (a917ba1) into feature branch spatial-interventions-integration via --no-ff merge f22bce8 (D-01); hook fix deferred to Plans 02/04
- [Phase ?]: 05-02: resolve_intervention_masks is base R + yaml:: only; engine never renormalises, only logs cells_sum_gt1
- [Phase ?]: 05-02: allocation.r caller keeps old implement_spatial_interventions signature until 05-04 rewires it
- [Phase 05]: 05-03: Stage 6 spatial-interventions prep retired; interventions run inside allocation (Stage 7), legacy code parked in src/old/
- [Phase 05]: 05-03: Scenario YAML Intervention_mask values are bare filenames resolved under spat_prob_perturb_dir (D-05); intervention prose lives in docs/spatial_interventions/
- [Phase 05]: 05-04: Intervention pre-flight lines (D-14) collected separately and appended after file checks; fixture mode never emits them
- [Phase 05]: 05-04: generate_probability_maps() hook uses year_post (D-07); normalized re-keyed on row_idx after the engine call
- [Phase 05]: 05-05: smoke verifier judges newest worker log with AUDIT summary; mlr3 .__Task__col_info fallback line whitelisted from forbidden markers; masks.sha256 forced eol=lf
- [Phase 05]: 05-06: branch landed via PR #2 (open, not merged) rather than a local --no-ff merge; SC1/D-01 'changes on main' remains PENDING until the operator merges
- [Phase 05]: 05-06: intervention allocation is a highmem-only workload (MaxRSS 95.7 GB on job 838021 vs the 93 GB compute limit)
- [Phase 05]: 05-06: .env must be sourced in the submitting shell before sbatch; --export=ALL alone exports an unconfigured environment (job 838019 died in 2s on the Stage 7 path contract)
- [Phase 05]: 05-06: HPC mask integrity accepted via the validator's compareGeom/value checks on all 14 masks; a separate sha256sum -c result was not reported back

- [Phase 05]: 05-10: pre-flight error lines keep the `intervention mask: ` / `intervention config: ` prefix (with colon) that D-14 established, so they differ deliberately from 05-09's runtime stop() strings; the geometry line also omits the runtime's trailing `; cell-number lookup would be silently wrong` because no lookup has been attempted at pre-flight time
- [Phase 05]: 05-10: WR-04 implemented as two sibling `if`s keyed on interventions_configured / engine_loaded rather than the review's nested `else` — identical behaviour, guard-only diff, no whole-block reflow
- [Phase 05]: 05-11: engine still never renormalises — the two table-wide clamps were replaced by seven per-index-set clamps (1 absolute + 6 relative), so no pass rewrites rows outside its own declared target class and zone
- [Phase 05]: 05-11 CHANGES NUMERICAL BEHAVIOUR: tied-percentile interventions go from rows_changed=0 to positive and exact-zero-difference interventions now apply Prob_adjust_threshold. 05-16's operator smoke re-run MUST be judged against the new semantics, NOT diffed against job 838021. AUDIT field set/order unchanged, so verify_intervention_smoke.r's parser needs no change — only the expected values move.
- [Phase 05]: Wave 3 executors were killed mid-task by a transient API auth error and were RESUMED in place via SendMessage, not re-dispatched — both worktrees held uncommitted work (130 lines in allocation.r, 25 in the engine) that a fresh agent would have discarded

- [Phase 05]: 05-12: AUDIT line extended APPEND-ONLY from 13 to 21 tokens (delta_mean/med/sd/min/max, n_inc, n_dec, sum_abs_delta). Verified: the shipped verify_intervention_smoke.r parser extracts id and rows_changed identically from old and extended lines, so the verifier needs no parser change. Non-finite stats render as literal NA.
- [Phase 05]: 05-12: plan's literal formatC(format="g") left-pads to `digits`, which exploded the AUDIT body to 41 tokens - threat T-05-46 materialising out of its own mitigation; fixed with trimws(formatC(..., width=1L))
- [Phase 05]: 05-13 handoff: res$stats' 19 columns are a superset of the D-18 CSV per-class columns; the writer only needs to prepend scenario, region, year, intervention_id, rank, type, zone, mask

- [Phase 05]: 05-13: telemetry CSV is `intervention_prob_deltas_<scenario>_<region>_<year>.csv` in work_dir, 25 columns; `<region>` is the slug gsub(" ","_",tolower(label)). AUDIT `region=` keeps the label verbatim, the CSV `region` column is the slug - they differ BY DESIGN.
- [Phase 05]: 05-13: rows_target vs sum(n_target) is NOT an equality for Relative interventions - relative_prob_adjust() adds length(sub_idx) to rows_target before its three `next` paths, so a skipped class counts toward AUDIT rows_target while its CSV row correctly reports n_target=0. Any cross-check must scope to Absolute or to n_target>0 rows. sum_abs_delta and rows_changed reconcile unconditionally.
- [Phase 05]: 05-13: plan's sanitiser gsub("[^A-Za-z0-9_.-]","_") keeps `.` so `../` survived as `.._`; a second pass gsub("\.{2,}","_") was added. Verified empirically: 0 traversal survivals across ../, backslash and ....// vectors, SSP2.6 preserved.

### Pending Todos

None yet.

### Blockers/Concerns

- `MultisessionFuture interrupted` (OOM SIGKILL) at ~3 minutes locally — the project's defining failure mode; addressed structurally across Phases 2–3.
- Phase 1 HPC-only verification gates (live Euler smoke test, live env solve, live SIGKILL test) pending operator confirmation — tracked in 01-HUMAN-UAT.md. **Phase 1.1 now closed** — the INFRA-01 live smoke gate is satisfied (exit 0).
- Phase 05 SC1/D-01 PENDING: PR #2 (spatial-interventions-integration -> main) is open but NOT merged; main still has no intervention support

## Deferred Items

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| Model framework | MLR3-01 (tidymodels → mlr3 migration) | Conditional on Phase 2 outcome | 2026-05-05 |
| Testing | TEST-01, TEST-02 (unit + integration tests) | v2 | 2026-05-05 |

## Session Continuity

Last session: 2026-09-23T08:42:13.549Z
Stopped at: Completed 05-06-PLAN.md (main merge pending: PR #2)
Resume file: None
