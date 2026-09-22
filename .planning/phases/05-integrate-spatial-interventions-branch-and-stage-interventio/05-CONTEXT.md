# Phase 5: Integrate spatial_interventions branch and stage intervention masks - Context

**Gathered:** 2026-09-22
**Status:** Ready for planning

<domain>
## Phase Boundary

Bring the colleague-authored `origin/spatial_interventions` branch (4 commits, branch point `f09c064`; `main` is 270 commits ahead) onto `main`: `implement_spatial_interventions()` wired into `generate_probability_maps()`, per-scenario `{BAU,NAT,CUL,SOC}_interventions.yml`, the `interventions_dir` config key, and the DT-native refactor. Check the spatial-intervention masks (local: `D:\C.3_Modelling\nascent-lulcc-agg\inputs\spat_prob_perturb`) against the YAMLs, document their HPC placement, and prove interventions work with one allocation smoke run.

Out of scope: new intervention types or changes to the scenario narratives/intervention design; the multi-scenario sweep (Phase 4).
</domain>

<decisions>
## Implementation Decisions

### Port strategy
- **D-01:** Use a real `git merge` of `origin/spatial_interventions` into a feature branch cut from `main`. The colleague's 4 commits keep their authorship. Resolve conflicts against the current `src/allocation.r`, then add fix-up commits on top (for example the timestep rewiring in D-06). No rebase and no hand-port.
- **D-02:** Delete `config/SSP0–5_interventions.yml` as the branch does. The active set is BAU/NAT/CUL/SOC, matching `scenario_names`, and the SSPx files stay in git history.
- **D-03:** Move the legacy intervention code to `src/old/`: `src/lulcc.spatprobmanipulation.r`, `src/spatial_interventions_prep.r`, and their drivers `scripts/run_spatial_interventions_prep.r` and `scripts/submit_spatial_interventions_prep.sh`. `implement_spatial_interventions.R` becomes the only intervention path. Remove `src/lulcc.spatprobmanipulation.r` from the `src_files` list in `scripts/run_allocation.r`, and grep for any other callers first. This also covers those files' part of Phase 4 SC6 (the `raster::` grep).
- **D-04:** Move the branch's prose docs under `docs/` (for example `docs/spatial_interventions/`) and **convert them to Markdown**: `scenario_narratives.txt` → `.md`, `spatial_masks/README.txt` → `.md`, `urban_settlement_mask_methodology.txt` → `.md`. Update the YAML header comments that cite these files so they point to the new paths. Drop the `spatial_masks/` repo folder.

### Mask path resolution
- **D-05:** YAML `Intervention_mask` entries become **bare filenames** such as `urban_settlement_mask.tif`, and the code resolves them as `file.path(config$spat_prob_perturb_dir, <name>)`. Strip the `spatial_masks/` prefix from all four YAMLs. The existing `spat_prob_perturb_dir` config key (already `inputs/spat_prob_perturb` in both configs) is the one knob that moves between local and HPC. Nothing is resolved relative to the project root.
- **D-06:** `interventions_dir` stays `config/` in the repo, as the branch has it, so the YAMLs stay versioned. The YAML location and the mask location are separate concerns.
- **D-07 (timestep correctness, carried from Phase 3.6):** The branch passes `simulation_time_step = year_ant + config$step_length`. Rewire it so the intervention year is the posterior year of the current timestep pair from `simulation_year_steps`, the same pairs the allocation driver uses. The YAML `Time_steps_implemented` lists (2024…2060) must match those posterior years.

### HPC placement & staging
- **D-08:** HPC target is `${HPC_SCRATCH_ROOT}/inputs/spat_prob_perturb/`, resolved by the existing `data_basepath` + `spat_prob_perturb_dir` in `config/hpc_config.yaml`. No new config is needed.
- **D-09:** Transfer uses an rsync command documented in `docs/README_HPC.md` (local `inputs/spat_prob_perturb` → HPC), plus a verification step that runs the D-12 validator on HPC. No staging script and no git LFS.
- **D-10:** Stage **only the masks the YAMLs reference** (the flat `*_mask*.tif` set). `nascent-pa-prioritization/` (Final_results_*.tif, xlsx, pdf), the local README and the methodology txt stay local.
- **D-11:** The HPC transfer and HPC smoke run are **operator-gated checkpoints**: the plan provides the commands, and the user runs them on Rundeck/Euler and reports back, as in Phases 3.5 and 3.6.

### Validation & smoke proof
- **D-12:** Add a standalone committed validator (for example `scripts/validate_intervention_masks.r`). For every scenario YAML it checks: each referenced mask exists (Static and every Dynamic year entry); orphan masks in the directory that no YAML references; CRS, extent and resolution against the model reference grid; and value domain (0/1/NA). It writes a Markdown report, with the phase-5 run saved into the phase directory. It can be rerun before any future run.
- **D-13:** Grid mismatch is a **hard failure**, fixed by re-exporting the mask offline. No runtime resampling or projection.
- **D-14:** A missing mask fails fast in the **existing consolidated Stage 7 pre-flight** (Phase 1 pattern): resolve every mask needed by the running scenario's active years up front and abort before any work with one list of gaps. The branch's "Dynamic mask has no entry for year → skip" behaviour is only acceptable when the year is intentionally absent from `Time_steps_implemented`. A referenced file that doesn't exist on disk is always an error.
- **D-15:** The smoke run is **NAT × one region × the 2032 step** (`ALLOCATION_YEAR_POST_FILTER` / single-region scoping from Phase 3.6). 2032 exercises the Dynamic mask switch (`phase1_target`) and the mining freeze. Assertions: per-transition probability is 0 inside the PA mask for the targeted to-classes (Absolute interventions); a per-intervention log line reports how many cells changed; the run completes and produces a posterior.

### Claude's Discretion
- How mask values are joined to the long `normalized` DT (`cell_id`/`row_idx` → mask value). Options include caching per region/year, cropping the national mask to the region extent once, or reading it on each call. Pick based on research into the current `generate_probability_maps()` shape (lazy per-from-class reads, threaded prediction).
- Exact location and name of the docs folder, and the validator script name.
- Whether `anterior` / `trans_rates_dt` / `class_name_to_value` (reserved parameters in the branch function) are threaded through as-is or trimmed.
</decisions>

<specifics>
## Specific Ideas

- The colleague's design notes are in the YAML headers (intervention ranking semantics: Absolute conservation first, then mining freeze, indigenous OECM relative decreases, urban densification push). Keep these semantics as they are. This phase ports and wires them without redesigning.
- The branch hook replaces the `#todo integrate more recent approach to spatial intervention from NCCS project` placeholder at `src/allocation.r:2882`, after per-cell normalisation and `setkey(normalized, row_idx)`, before the per-transition TIF writes.
</specifics>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Branch being integrated
- `origin/spatial_interventions` (commits d0a11df, b01bb71, be8121e, a917ba1): `src/implement_spatial_interventions.R`, `config/{BAU,NAT,CUL,SOC}_interventions.yml`, `src/allocation.r` hook diff, `scripts/run_allocation.r` src_files addition, `config/{hpc,local}_config.yaml` `interventions_dir`, `scenario_narratives.txt`, `spatial_masks/README.txt`, `spatial_masks/urban_settlement_mask_methodology.txt`. Also note the `hpc_config.yaml` `scenario_to_ssp_mapping` indentation fix on the branch.

### Current allocation architecture (merge target)
- `src/allocation.r`: `generate_probability_maps()` (~L2470), normalisation plus placeholder (~L2875–2885), `load_allocation_class_map()`, `filter_allocation_timesteps()`, timestep-pair construction from `simulation_year_steps`
- `scripts/run_allocation.r`: src_files list, Stage 7 pre-flight
- `config/hpc_config.yaml`, `config/local_config.yaml`: `data_basepath`, `spat_prob_perturb_dir`
- `config/lulc_schema.json`: class_name → value mapping used by the YAMLs
- `.planning/phases/03.6-complete-single-scenario-end-to-end-run/`: per-region jobs, `ALLOCATION_YEAR_POST_FILTER`, `simulation_year_steps` timestep decision
- `.planning/phases/03.5-reduce-allocation-memory-floor-lazy-per-transition-predictor/`: lazy per-from-class reads and threaded prediction that the hook must coexist with

### Masks and HPC
- Local mask dir: `D:\C.3_Modelling\nascent-lulcc-agg\inputs\spat_prob_perturb\` (flat `*_mask*.tif`, `README.txt`, `urban_settlement_mask_methodology.txt`, `nascent-pa-prioritization/`)
- `docs/README_HPC.md`: where the rsync and staging instructions go
- `.planning/ROADMAP.md` §Phase 5 (success criteria) and §Phase 4 SC6 (overlap: raster:: removal and intervention path schema)
</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `get_stage7_runtime_paths()` and the consolidated Stage 7 pre-flight (Phase 1): extend with mask-existence checks (D-14)
- `log_msg()` / AUDIT log lines: per-intervention change counts
- `write_raster_atomic()`: if any mask-derived raster is written
- `load_allocation_class_map()`: already provides `class_name_to_value` for YAML class names
- Phase 3.6 `ALLOCATION_YEAR_POST_FILTER` + single-region scoping: smoke-run targeting (D-15)

### Established Patterns
- Config paths are relative to `data_basepath`, which on HPC comes from `${HPC_SCRATCH_ROOT}`. No hardcoded user paths (Phase 1 D-13/D-14).
- Fail-fast consolidated prerequisite lists instead of mid-run errors.
- Operator-gated HPC checkpoints with the user reporting job IDs and logs back.

### Integration Points
- Hook site: `generate_probability_maps()` after normalisation, before per-transition TIF writes (the Dinamica `%03d` prefix ordering must stay intact)
- `scripts/run_allocation.r` src_files: add `implement_spatial_interventions.R`, drop `lulcc.spatprobmanipulation.r`
</code_context>

<deferred>
## Deferred Ideas

- Checking that the intervention semantics are right (for example whether Relative-Decrease magnitudes produce plausible land-use outcomes) is scenario science, not integration. Revisit after the Phase 4 full sweep.
- A reusable mask staging script or LFS versioning of masks, if masks start changing often.

</deferred>

---

*Phase: 05-integrate-spatial-interventions-branch-and-stage-interventio*
*Context gathered: 2026-09-22*
