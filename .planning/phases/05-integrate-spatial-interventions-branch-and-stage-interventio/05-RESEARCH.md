# Phase 5: Integrate spatial_interventions branch and stage intervention masks - Research

**Researched:** 2026-09-22
**Domain:** git merge integration + R/terra/data.table allocation hook + raster input staging (local → BeeGFS HPC)
**Confidence:** HIGH (codebase, merge dry run, mask inventory and grid checks all verified with tools in this session)

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

#### Port strategy
- **D-01:** Use a real `git merge` of `origin/spatial_interventions` into a feature branch cut from `main`. The colleague's 4 commits keep their authorship. Resolve conflicts against the current `src/allocation.r`, then add fix-up commits on top (for example the timestep rewiring in D-06). No rebase and no hand-port.
- **D-02:** Delete `config/SSP0–5_interventions.yml` as the branch does. The active set is BAU/NAT/CUL/SOC, matching `scenario_names`, and the SSPx files stay in git history.
- **D-03:** Move the legacy intervention code to `src/old/`: `src/lulcc.spatprobmanipulation.r`, `src/spatial_interventions_prep.r`, and their drivers `scripts/run_spatial_interventions_prep.r` and `scripts/submit_spatial_interventions_prep.sh`. `implement_spatial_interventions.R` becomes the only intervention path. Remove `src/lulcc.spatprobmanipulation.r` from the `src_files` list in `scripts/run_allocation.r`, and grep for any other callers first. This also covers those files' part of Phase 4 SC6 (the `raster::` grep).
- **D-04:** Move the branch's prose docs under `docs/` (for example `docs/spatial_interventions/`) and **convert them to Markdown**: `scenario_narratives.txt` → `.md`, `spatial_masks/README.txt` → `.md`, `urban_settlement_mask_methodology.txt` → `.md`. Update the YAML header comments that cite these files so they point to the new paths. Drop the `spatial_masks/` repo folder.

#### Mask path resolution
- **D-05:** YAML `Intervention_mask` entries become **bare filenames** such as `urban_settlement_mask.tif`, and the code resolves them as `file.path(config$spat_prob_perturb_dir, <name>)`. Strip the `spatial_masks/` prefix from all four YAMLs. The existing `spat_prob_perturb_dir` config key (already `inputs/spat_prob_perturb` in both configs) is the one knob that moves between local and HPC. Nothing is resolved relative to the project root.
- **D-06:** `interventions_dir` stays `config/` in the repo, as the branch has it, so the YAMLs stay versioned. The YAML location and the mask location are separate concerns.
- **D-07 (timestep correctness, carried from Phase 3.6):** The branch passes `simulation_time_step = year_ant + config$step_length`. Rewire it so the intervention year is the posterior year of the current timestep pair from `simulation_year_steps`, the same pairs the allocation driver uses. The YAML `Time_steps_implemented` lists (2024…2060) must match those posterior years.

#### HPC placement & staging
- **D-08:** HPC target is `${HPC_SCRATCH_ROOT}/inputs/spat_prob_perturb/`, resolved by the existing `data_basepath` + `spat_prob_perturb_dir` in `config/hpc_config.yaml`. No new config is needed.
- **D-09:** Transfer uses an rsync command documented in `docs/README_HPC.md` (local `inputs/spat_prob_perturb` → HPC), plus a verification step that runs the D-12 validator on HPC. No staging script and no git LFS.
- **D-10:** Stage **only the masks the YAMLs reference** (the flat `*_mask*.tif` set). `nascent-pa-prioritization/` (Final_results_*.tif, xlsx, pdf), the local README and the methodology txt stay local.
- **D-11:** The HPC transfer and HPC smoke run are **operator-gated checkpoints**: the plan provides the commands, and the user runs them on Rundeck/Euler and reports back, as in Phases 3.5 and 3.6.

#### Validation & smoke proof
- **D-12:** Add a standalone committed validator (for example `scripts/validate_intervention_masks.r`). For every scenario YAML it checks: each referenced mask exists (Static and every Dynamic year entry); orphan masks in the directory that no YAML references; CRS, extent and resolution against the model reference grid; and value domain (0/1/NA). It writes a Markdown report, with the phase-5 run saved into the phase directory. It can be rerun before any future run.
- **D-13:** Grid mismatch is a **hard failure**, fixed by re-exporting the mask offline. No runtime resampling or projection.
- **D-14:** A missing mask fails fast in the **existing consolidated Stage 7 pre-flight** (Phase 1 pattern): resolve every mask needed by the running scenario's active years up front and abort before any work with one list of gaps. The branch's "Dynamic mask has no entry for year → skip" behaviour is only acceptable when the year is intentionally absent from `Time_steps_implemented`. A referenced file that doesn't exist on disk is always an error.
- **D-15:** The smoke run is **NAT × one region × the 2032 step** (`ALLOCATION_YEAR_POST_FILTER` / single-region scoping from Phase 3.6). 2032 exercises the Dynamic mask switch (`phase1_target`) and the mining freeze. Assertions: per-transition probability is 0 inside the PA mask for the targeted to-classes (Absolute interventions); a per-intervention log line reports how many cells changed; the run completes and produces a posterior.

### Claude's Discretion
- How mask values are joined to the long `normalized` DT (`cell_id`/`row_idx` → mask value). Options include caching per region/year, cropping the national mask to the region extent once, or reading it on each call. Pick based on research into the current `generate_probability_maps()` shape (lazy per-from-class reads, threaded prediction).
- Exact location and name of the docs folder, and the validator script name.
- Whether `anterior` / `trans_rates_dt` / `class_name_to_value` (reserved parameters in the branch function) are threaded through as-is or trimmed.

### Deferred Ideas (OUT OF SCOPE)
- Checking that the intervention semantics are right (for example whether Relative-Decrease magnitudes produce plausible land-use outcomes) is scenario science, not integration. Revisit after the Phase 4 full sweep.
- A reusable mask staging script or LFS versioning of masks, if masks start changing often.
</user_constraints>

<phase_requirements>
## Phase Requirements

No REQ-IDs are mapped to this phase (ROADMAP says `Requirements: TBD`). The planner should cover the four success criteria and D-01..D-15:

| ID | Description | Research Support |
|----|-------------|------------------|
| SC1 | Branch changes on `main`, conflicts resolved, existing tests pass | §Merge Dry Run: 3 textual conflicts (all trivial) + 2 **semantic** conflicts that auto-merge but break at runtime (`class_name_to_value` not in scope, `year_post` not a parameter). Test baseline recorded. |
| SC2 | Every referenced mask exists; orphans listed; CRS/extent/res checked | §Mask Inventory: 14/14 referenced masks present, 0 orphan `*_mask*.tif`, all 14 identical to the ref grid (EPSG:4326, 0.000808484°, 24192×16579), values {1, NA} |
| SC3 | HPC placement plan documented | §HPC Placement: `${HPC_SCRATCH_ROOT}/inputs/spat_prob_perturb/` = `/beegfs/black/nascent-lulcc/inputs/spat_prob_perturb/`; **rsync is NOT installed on the workstation** → scp fallback; sha256 manifest given |
| SC4 | Smoke run with interventions completes | §Smoke Run: `NAT × costa_peruana × 2032` on `highmem`; costa has 3.13M NAT-phase1 PA cells, 5.6M concession cells, and rate-table rows to 104/105/106 |
| D-07 | Posterior-year timestep | Branch formula gives 2026 for the first pair (2022→2024) → 2024 interventions would be silently skipped |
| D-12/13 | Validator + hard grid fail | Verified terra calls (`compareGeom`, `freq`, `crs(describe=TRUE)`) in this session |
| D-14 | Pre-flight mask check | `validate_allocation_runtime(config)` is the hook; fixture mode is exact, so existing preflight tests are unaffected |
| D-15 | Assertions | Multiplicative Relative adjustments keep zeros at zero, so the "PA-inside prob == 0" assertion holds even after later-ranked interventions |
</phase_requirements>

## Summary

The branch is small (4 commits by ManuelKurmann, 2026-06-08/09, base `f09c064`, `main` is 272 commits ahead) and the merge is mechanically easy. `git merge-tree --write-tree main origin/spatial_interventions` reports **only three textual conflicts**: `.gitignore` (both sides appended lines), `config/hpc_config.yaml` (whitespace only on `scenario_to_ssp_mapping:`, which `main` already fixed), and `scripts/run_allocation.r` (`src_files` list). `src/allocation.r` **auto-merges cleanly**: the 10-line hook lands exactly on the `#todo integrate more recent approach…` placeholder at current L2882.

That clean auto-merge is the main trap. The merged hook refers to `class_name_to_value`, which **is not defined anywhere in `generate_probability_maps()`** on `main`, and computes the year from `year_ant + config$step_length` because `generate_probability_maps()` has no `year_post` parameter. The first fails at runtime with "object not found" on the first region. The second gives 2026 for the first pair (2022→2024), so the 2024 interventions never fire, which is the D-07 bug. Also note that `scripts/run_allocation.r` wraps every `source()` in a `tryCatch` that only prints `ERROR sourcing …` and continues. A sourcing failure in the new file would therefore show up much later as a "could not find function" error.

The masks are in good shape. All 14 files referenced by the four YAMLs are present flat in `D:\C.3_Modelling\nascent-lulcc-agg\inputs\spat_prob_perturb\`. Every one matches the reference grid exactly (`compareGeom` TRUE), is INT1U, and holds only the value 1 plus NA. There are no orphan `*_mask*.tif` files. The only non-referenced items are the D-10 local-only material (`nascent-pa-prioritization/`, `README.txt`, the methodology txt), an **empty `spatial_masks/` subdirectory**, and a `.DS_Store`. Neither local `README.txt` nor the methodology txt differs from the branch copies (`diff -q` clean). The branch's relative-adjust helper has a real crash path: when either the inside set or the outside set has no positive probabilities, `quantile()` returns NA, `Perc_diff` becomes NaN, and `if (Perc_diff > 0)` stops with "missing value where TRUE/FALSE needed" (reproduced). That case is likely after the rank-1 Absolute=0 zeroes the PA interior. It needs a small defensive guard (skip and log), which does not change the semantics.

**Primary recommendation:** Merge on a feature branch and resolve the three textual conflicts. Then land fix-up commits that (1) add a `year_post` parameter to `generate_probability_maps()` and pass it from `setup_allocation_inputs()`, (2) compute `class_name_to_value <- load_allocation_class_map(config)` in scope, (3) replace per-class `terra::extract(xy)` with a per-mask, per-call cached lookup keyed on region `cell_id` (built once from `anterior_dt$ref_cell_id`), (4) resolve bare mask filenames under `config$spat_prob_perturb_dir` through one shared resolver that the runtime, the Stage 7 pre-flight and the standalone validator all use, and (5) guard the NaN path and emit `AUDIT stage=intervention` log lines.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Intervention definitions (YAML) | Repo config (`config/*_interventions.yml`, resolved from project root via `config_files_paths`) | — | Versioned with the code (D-06) |
| Mask rasters | Data tier (`data_basepath/inputs/spat_prob_perturb`) | — | Large binaries, environment-specific root (D-05/D-08) |
| Mask path resolution + existence | Shared R helper (new, in `src/implement_spatial_interventions.R`) | Stage 7 pre-flight (`validate_allocation_runtime`) + validator script | One resolver keeps runtime, pre-flight and validator consistent |
| Probability perturbation | Region worker, `generate_probability_maps()` after normalisation, before TIF writes | — | The only place the long `normalized` DT exists |
| Grid/value-domain validation | Offline validator script (`scripts/validate_intervention_masks.r`) | — | Hard-fail per D-13, never at runtime |
| Smoke proof | HPC operator (sbatch on `highmem`) | Post-run assertion script | D-11 operator gate |

## Merge Dry Run (VERIFIED: `git merge-tree --write-tree`, git 2.55)

`origin/spatial_interventions` == local `spatial_interventions` == `a917ba1`. Merge base `f09c064`.

| Commit | Author | Content |
|--------|--------|---------|
| d0a11df | ManuelKurmann | `scenario_narratives.txt`, `spatial_masks/README.txt`, `spatial_masks/urban_settlement_mask_methodology.txt` |
| b01bb71 | ManuelKurmann | `config/{BAU,NAT,CUL,SOC}_interventions.yml`, deletes `config/SSP{0,1,3,4,5}_interventions.yml`, `interventions_dir: "config"` in both configs, hpc_config indentation fix, `.gitignore` |
| be8121e | ManuelKurmann | DT-native rewrite of `src/implement_spatial_interventions.R` (476 lines changed) |
| a917ba1 | ManuelKurmann | Hook into `generate_probability_maps()` + `src_files` entry |

**Textual conflicts (3), resolutions:**

| File | Conflict | Resolution |
|------|----------|------------|
| `.gitignore` | main added temp/planning ignores (including 3× duplicated `.planning/HANDOFF.json`/`config.json` lines). The branch added `spatial_interventions_integration_protocol.md`, `spatial_masks_misc/`, `config/old/` | Keep both sides. Optionally de-duplicate main's repeated lines |
| `config/hpc_config.yaml` | `  scenario_to_ssp_mapping: ` (main, trailing space) vs `  scenario_to_ssp_mapping:` (branch). **main already fixed the 4-space indentation bug** | Take branch side (no trailing space). The branch's `interventions_dir` line auto-merges |
| `scripts/run_allocation.r` | main: `"src/saturation_diagnostics.r", "src/allocation.r"`; branch: `"src/allocation.r", "src/implement_spatial_interventions.R", "src/lulcc.spatprobmanipulation.r"` | `"src/setup.r","src/utils.r","src/dinamica_utils.r","src/saturation_diagnostics.r","src/allocation.r","src/implement_spatial_interventions.R"`. main **already** dropped `lulcc.spatprobmanipulation.r`. `test-allocation-single-source-writer.R` asserts saturation_diagnostics sits between utils and allocation, so keep that order |

**Auto-merged but semantically broken (must fix in the fix-up commits):**

1. `class_name_to_value` is used in the hook but not defined in `generate_probability_maps()` (signature: `work_dir, region_label, region_val, scenario, year_ant, calibration_period, anterior_path, trans_rates_df, config, log_file, models_list, nhood_paths`). Fix: `class_name_to_value <- load_allocation_class_map(config)` inside the function (cheap JSON read), or pass it in.
2. `year_post` is not available. `setup_allocation_inputs()` has `year_post` (L2149) and calls `generate_probability_maps()` at L2311 without it. Add a `year_post` param and pass it through, then use `simulation_time_step = year_post` (D-07). Verified pairs: `simulation_year_steps = [2022, 2024, 2028, …, 2060]`, so posterior years = 2024, 2028, …, 2060, which equals every YAML `Time_steps_implemented` list. The branch formula gives 2026, 2028, 2032, … (wrong only for the first pair, which means 2024 interventions are silently skipped).
3. `trans_rates_dt` in the hook refers to the local `trans_rates_dt` (L2640), so that one is fine. `anterior` is in scope (L2557).

`src/implement_spatial_interventions.R` exists on `main` in its old pre-refactor form (signature `interventions_dir, scenario_ID, …, raster_prob_values`) and is untouched since the base, so the branch version replaces it cleanly. **No caller on `main`** uses the old signature (grep verified).

**Other mechanical items for the merge branch:**
- D-02: SSPx deletions come from the branch automatically.
- D-03 callers to update after `git mv` into `src/old/` and `scripts/` (grep verified): `docs/ARCHITECTURE.md` L57/L100/L117 (also says `config/SSP*_interventions.yml`), `docs/DEPLOYMENT.md` L263/L311, `docs/DEVELOPMENT.md` L80/L133, `README.md` L109, `scripts/master_pipeline.sh` L349 (echo text only). No R code sources them. Where do the moved driver scripts go? `src/old/` exists; there is no `scripts/old/`. Recommend `src/old/` for the two `src/` files and a new `scripts/old/` for the two drivers (D-03 wording says `src/old/` for all four; either is fine, just keep them out of active paths).
- D-04: use `git mv` (history preserved), then edit content to Markdown: `scenario_narratives.txt` → `docs/spatial_interventions/scenario_narratives.md`, `spatial_masks/README.txt` → `docs/spatial_interventions/spatial_masks.md`, `spatial_masks/urban_settlement_mask_methodology.txt` → `docs/spatial_interventions/urban_settlement_mask_methodology.md`. YAML headers cite `scenario_narratives.txt` (L12 in all four) and say "Mask paths are relative to the project root" (must change to "bare filenames resolved under `spat_prob_perturb_dir`"). They also cite `spatial_interventions_integration_protocol.md §4a/§3.7`, which is **gitignored and not in the repo** (a dangling reference). Recommend rewording to "(colleague's local design protocol, not versioned)", or asking the colleague for it.
- `spatial_interventions_integration_explainer.md` is **tracked at the repo root on main** (commit 0b98bac). It is the original integration brief. Recommend moving it into `docs/spatial_interventions/` too (discretion).

## Standard Stack

No new packages. Everything already exists in `allocation_env` and locally:

| Library | Version (local, verified) | Purpose |
|---------|---------|---------|
| terra | 1.9.34 | mask read, `extract(x, <cell numbers>)`, `compareGeom`, `freq` |
| data.table | 1.18.4 | in-place prob edits on `normalized` |
| yaml | 2.3.12 | `yaml::yaml.load_file` for intervention YAMLs |
| testthat / withr | 3.3.2 / 3.0.3 | new unit tests with a tempdir fixture |
| jsonlite | 2.0.0 | `load_allocation_class_map()` |

`environments/allocation_env.yml` already lists r-terra, r-yaml and r-data.table.

## Package Legitimacy Audit

Not applicable: this phase installs no external packages. slopcheck was not run.

## Mask Inventory (VERIFIED: terra 1.9.34 on the local files, 2026-09-22)

Reference grid `inputs/spatial_reference_grid/ref_grid_aggregated.tif`: **EPSG:4326**, res 0.000808483755707569°, extent (-81.70618, -68.30232, -18.84495, 0.7138912), 24192 rows × 16579 cols, INT1U, 163,161,178 non-NA cells. `regions.tif` has the identical grid.

> Note: both configs say `reference_crs: "epsg:2056"` (Swiss LV95, a leftover from the Swiss project). The validator MUST compare against the ref-grid raster itself, not `config$reference_crs`.

| YAML reference (after D-05 strip) | Used by | File present | compareGeom(ref) | Values |
|---|---|---|---|---|
| protected_areas_mask_BAU.tif | BAU r1 | ✓ | TRUE | 1 (28.33M), NA |
| protected_areas_mask_NAT_phase0_current.tif | NAT r1 2024/2028 | ✓ | TRUE | 1 (28.33M) |
| protected_areas_mask_NAT_phase1_target.tif | NAT r1 2032 | ✓ | TRUE | 1 (44.60M) |
| protected_areas_mask_NAT_phase2_full.tif | NAT r1 2036–2060 | ✓ | TRUE | 1 (48.69M) |
| protected_areas_mask_CUL_phase{0,1,2}_*.tif | CUL r1 | ✓✓✓ | TRUE | 1 (28.33M / 44.58M / 48.67M) |
| protected_areas_mask_SOC_phase{0,1,2}_*.tif | SOC r1 | ✓✓✓ | TRUE | 1 (28.33M / 43.57M / 48.68M) |
| mining_concessions_mask.tif | NAT r2 (2032+), CUL r2 | ✓ | TRUE | 1 (20.14M) |
| indigenous_lands_mask.tif | NAT r3/r4, CUL r3/r4 | ✓ | TRUE | 1 (42.12M) |
| urban_settlement_mask.tif | NAT r5, CUL r5, SOC r3 | ✓ | TRUE | 1 (1.07M) |
| low_es_value_mask.tif | SOC r2 | ✓ | TRUE | 1 (40.78M) |

- **14 distinct referenced masks, 14 present, 0 gaps.** No two masks are byte-identical (sha256), though BAU/NAT/CUL/SOC phase0 have equal 1-counts.
- **Orphans (not referenced by any YAML):** no `*.tif` at the top level. Non-mask items: `README.txt`, `urban_settlement_mask_methodology.txt`, `nascent-pa-prioritization/` (Final_results_{BaU,NaC,NfN,NfS}.tif, PA_expansion_inputs_final.xlsx, report_scenarios.pdf, i.e. the PA source inputs), an **empty `spatial_masks/` directory** and `.DS_Store`. The validator should scan top-level `*.tif` for the orphan check and list non-tif items as "local-only, not staged" for information.
- Mask cells with value 1 where the ref grid is NA: 48,270 for `mining_concessions_mask.tif`. This is harmless (outside the model domain), so report it as info, not a failure.
- Class names used in the YAMLs (`built_up_and_barren_lands`, `forested_areas`, `high_intensity_agricultural_areas`, `low_intensity_agricultural_areas`, `mining`) all exist in `config/lulc_schema.json` (values 105, 101, 104, 103, 106).
- `Time_steps_implemented` in every intervention ⊆ {2024…2060} = posterior years. NAT `Mining_freeze_post_2030` starts at 2032. Every Dynamic mask has a key for every implemented year.

**sha256 manifest (for the HPC verify step):**
```
e0c9038adce0aa3b94c440e8c2a0a61f3c3190007cc3bf5e7d71a370082b7c37  indigenous_lands_mask.tif
059e65c503eb0b05582a3461d3d49089f904e3f312a163e47546741a13c924b6  low_es_value_mask.tif
8cec343a8d666e3cc8f6e2748cfe75ba8e0e9ce06d43230d5162fd7d7b029a49  mining_concessions_mask.tif
0d5f2b1fe2b05213975bcaf330778d5b32f39703f3dfd093a46305df9cfd489a  protected_areas_mask_BAU.tif
6326ea722b17288136a391610574aa567c2132535e2af913422cb3137659de0f  protected_areas_mask_CUL_phase0_current.tif
356810e4e993d61bb5e9a03f7150e186a932aafb35e0ec718c3ac34e08b656e4  protected_areas_mask_CUL_phase1_target.tif
a2d56ba9aad98f14d267b235baeb1b5b2d1a5aeb31db98f50aa350d7c719fda5  protected_areas_mask_CUL_phase2_full.tif
1e15a19477a8e0ce531f67849fd1449e61400301802d27b783df5fd76eb93ed7  protected_areas_mask_NAT_phase0_current.tif
0cdeffaa72d5e1da4e564af365fa99fcf549eb62920b2e09a1a97cdc37953c4e  protected_areas_mask_NAT_phase1_target.tif
54a30399327bdb7089ef807b7fa163b76e36dedcfc8d2ee3acbe5220a7729f38  protected_areas_mask_NAT_phase2_full.tif
e8ea9f41262f71b2d3a4289563d4b53e9327a76d1a87ca3398d0d10875d6faff  protected_areas_mask_SOC_phase0_current.tif
f09502425208f27b596dd585689db7ca9c6d124df9610fb9d421cb11f079350c  protected_areas_mask_SOC_phase1_target.tif
8c66dbb9d8ae10aebe6936f0a4453206ea8dde5b271e5d48175f7e1c675787b2  protected_areas_mask_SOC_phase2_full.tif
984e4367b5b98874898527089bbf6e675bccd7e32e67db9f6824d693e4790fc8  urban_settlement_mask.tif
```
Total 34 MB. Commit it as `docs/spatial_interventions/masks.sha256`, so that `sha256sum -c` on HPC is the transfer check before the validator runs.

## HPC Placement

- **Resolution (VERIFIED in `src/setup.r::build_full_config`):** `input_dirs` entries → `file.path(data_basepath, path)` (and **auto-`dir.create`d**). `config_files_paths` entries → `file.path(find_project_root(), path)`. So:
  - `config$spat_prob_perturb_dir`: HPC `${HPC_SCRATCH_ROOT}/inputs/spat_prob_perturb` = `/beegfs/black/nascent-lulcc/inputs/spat_prob_perturb`. Local `data_basepath/inputs/spat_prob_perturb` (the local config says `E:/nascent-lulcc-agg`, but **E: does not exist on this machine**. The data lives at `D:/C.3_Modelling/nascent-lulcc-agg`, so local runs need `data_basepath` pointed at D: or the operator's drive).
  - `config$interventions_dir`: `<repo checkout>/config` on both. It ships with `git pull` on login02.
- Because `build_full_config` auto-creates the directory, a missing transfer shows up as an **empty dir**, not a missing dir. The pre-flight must check files, not the directory.
- **Transfer tooling (VERIFIED):** `rsync` is **not installed** in the workstation's Git Bash, and WSL is not installed. `scp`, `ssh` (OpenSSH 10.5) and `sha256sum` are available, and `~/.ssh/config` has `login02.cluster.zalf.de`. D-09 locks "rsync", so document both forms:

```bash
# Preferred (if rsync is available, e.g. from another Linux host or after installing it):
rsync -av --include='*_mask*.tif' --exclude='*' \
  /d/C.3_Modelling/nascent-lulcc-agg/inputs/spat_prob_perturb/ \
  login02.cluster.zalf.de:/beegfs/black/nascent-lulcc/inputs/spat_prob_perturb/
# Fallback from this Windows workstation (Git Bash; rsync absent):
ssh login02.cluster.zalf.de 'mkdir -p /beegfs/black/nascent-lulcc/inputs/spat_prob_perturb'
scp /d/C.3_Modelling/nascent-lulcc-agg/inputs/spat_prob_perturb/*_mask*.tif \
  login02.cluster.zalf.de:/beegfs/black/nascent-lulcc/inputs/spat_prob_perturb/
# Verify on HPC (login02, after git pull):
cd /beegfs/black/nascent-lulcc/inputs/spat_prob_perturb && sha256sum -c <repo>/docs/spatial_interventions/masks.sha256
Rscript scripts/validate_intervention_masks.r   # inside allocation_env
```
The glob `*_mask*.tif` matches exactly the 14 referenced files and excludes `nascent-pa-prioritization/` (D-10).

## Architecture Patterns

### Data flow (per region worker, per timestep)
```
setup_allocation_inputs(year_ant, year_post)
  └─> generate_probability_maps(..., year_post)          [+ year_post param]
        anterior_dt(cell_id, x, y, lulc_class, ref_cell_id)   (already built, L2562-2588)
        per-transition lazy predict → gather → normalized(row_idx, from_val, to_val, cell_id, x, y, prob)
        per-cell normalise (tot_prob>1 → scale); setkey(row_idx)
        ──> implement_spatial_interventions(normalized, cell_index = anterior_dt[, .(cell_id, ref_cell_id)],
                class_name_to_value, interventions_dir, mask_dir = config$spat_prob_perturb_dir,
                scenario, simulation_time_step = year_post, log_file)
              read <scenario>_interventions.yml → filter stage==Allocation & year ∈ Time_steps
              order by ranking → for each: resolve mask (bare name under mask_dir)
                 → cached inside-LUT per mask file (built once per call)
                 → Absolute / Relative adjust on rows (to_val ∈ targets, from_val ∈ filter)
                 → AUDIT stage=intervention log line (rows changed)
        per-trans_rates-row TIF writer (%03d prefix, unchanged)
```

### Pattern 1: Per-call mask lookup cache keyed on region cell_id (discretion item, recommended)
**What:** `anterior_dt` already carries `ref_cell_id` (national cell number). For each distinct mask file used in this call, extract once by national cell number and turn the result into a dense logical LUT indexed by region `cell_id`. Each intervention/class then indexes `lut[normalized$cell_id[sub_idx]]` in O(1).
**Why:** The branch calls `terra::extract(mask, as.matrix(normalized[sub_idx, .(x, y)]))` once **per target class per intervention**. NAT 2032 means 3+1+3+1+1 = 9 extracts, each building a double xy matrix over tens of millions of rows. Measured locally: extract by cell numbers, 2e7 cells = 14.6 s; extract by xy, 1e6 points = 1.8 s (so ~36 s per 2e7 plus a 320 MB matrix). The cache cuts NAT to 4 extracts over the region's valid cells only. Both methods return identical values (verified). Memory is ~4 B × ncell(anterior) per cached mask (≤4 masks per call). That is small next to the measured 110–341 GB region peaks.
**Threading/fork:** prediction threading is finished by the time the hook runs (post-gather). Each forked or sequential worker opens its own `terra::rast()`, so no SpatRaster crosses a fork boundary. Don't create mask SpatRasters in the parent.
```r
# Source: terra::extract docs (y = numeric vector of cell numbers) — verified locally terra 1.9.34
.mask_inside_lut <- function(mask_path, cell_index, cache) {
  key <- normalizePath(mask_path, mustWork = TRUE)
  if (!is.null(cache[[key]])) return(cache[[key]])
  m <- terra::rast(mask_path)
  v <- terra::extract(m, cell_index$ref_cell_id)[[1L]]
  lut <- logical(max(cell_index$cell_id))
  lut[cell_index$cell_id[!is.na(v) & v == 1]] <- TRUE
  cache[[key]] <- lut
  lut
}
# in helpers: inside_flag <- lut[normalized$cell_id[sub_idx]]
```
With this, `x`/`y` in `normalized` are no longer needed by the helpers (keep the columns, since the writer doesn't use them either).

### Pattern 2: One shared resolver for runtime, pre-flight and validator
```r
# returns data.table(scenario, intervention_id, year, mask_name, mask_path, exists)
resolve_intervention_masks <- function(interventions_dir, mask_dir, scenario, years) {
  f <- file.path(interventions_dir, paste0(scenario, "_interventions.yml"))
  if (!file.exists(f)) stop(sprintf("interventions YAML missing: %s", f))
  ivs <- yaml::yaml.load_file(f)
  # for each Allocation intervention and each year in intersect(years, Time_steps_implemented):
  #   Static  -> Intervention_mask (scalar)
  #   Dynamic -> Intervention_mask[[as.character(year)]]; NULL => ERROR (D-14: an implemented year without an entry is a config error)
  #   reject names containing "/" or "\\" (bare filenames only, D-05)
  #   mask_path <- file.path(mask_dir, mask_name); exists <- file.exists(mask_path)
}
```
- **Pre-flight (D-14):** in `validate_allocation_runtime()`'s `config`-supplied (non-fixture) branch, loop over `config$scenario_names` (already narrowed by `ALLOCATION_PROFILE_SCENARIO` in `run_allocation.r`). Get years from `tail(config$simulation_year_steps, -1)`, narrowed by `ALLOCATION_YEAR_POST_FILTER` when set. Append one error line per missing file: `sprintf("intervention mask: missing %s (scenario=%s id=%s year=%d)", ...)`. The fixture mode stays exact, so `test-allocation-preflight.R` is unaffected. Note that the `--preflight-only` path calls with `config = NULL`, so masks are only checked on a real run. That is acceptable and consistent with the other file checks.
- `validate_allocation_runtime` is in `src/allocation.r`, and `implement_spatial_interventions.R` is sourced **after** it. Resolution happens at call time, so that's fine, but put the resolver where both the tests and the validator script can source it (in `implement_spatial_interventions.R`; the validator sources `src/setup.r` and `src/implement_spatial_interventions.R`).

### Pattern 3: Standalone validator (D-12)
`scripts/validate_intervention_masks.r [--out <report.md>]`:
- `setwd(project root)`, `source("src/setup.r")`, `source("src/implement_spatial_interventions.R")`, `config <- get_config()`, `ref <- terra::rast(config$ref_grid_path)`.
- For each scenario in `config$scenario_names`: `resolve_intervention_masks(..., years = all posterior years)`. Missing → FAIL row.
- Per distinct mask: `terra::compareGeom(r, ref, stopOnError = FALSE)` (checks CRS, extent, rows/cols, resolution) → FAIL if FALSE (D-13). Also `nlyr(r) == 1`. Value domain: `terra::freq(r)$value ⊆ {0, 1}`, else FAIL. Report the count of 1-cells outside ref-grid validity as INFO.
- Orphans: top-level `list.files(mask_dir, "[.]tif$")` minus the referenced set → WARN. Non-tif entries and subdirs → INFO ("local-only, not staged").
- Write Markdown (tables). Exit non-zero on any FAIL. The phase-5 run is saved as `.planning/phases/05-…/05-MASK-VALIDATION.md`.
- Runtime: the full national `freq()` over 14 × 401M cells took ~1–2 min locally. Acceptable.
- Pitfall: in heredoc/`Rscript -e`, `"\\.tif$"` got mangled under Git Bash. Use `"[.]tif$"` in regexes.

### Pattern 4: AUDIT log line per intervention (D-15)
Follow the existing `AUDIT stage=<x> key=value` grammar (for example `AUDIT stage=5 region=…`). Replace the helpers' `cat()` (which goes to stdout, not the region log) with `log_msg(..., log_file)`. Pass `log_file` into the helpers.
```
AUDIT stage=intervention region=<suffix> scenario=NAT year=2032 id=Conservation_expansion_and_preservation rank=1 type=Absolute zone=Inside to_val=105 mask=protected_areas_mask_NAT_phase1_target.tif rows_target=<n> rows_changed=<n>
```
`rows_changed` = rows whose `prob` differs before and after (for Absolute that is `length(ix)`, rows with prob>0 that were set).

### Anti-Patterns to Avoid
- **Resolving mask names relative to project root or cwd:** the branch's `terra::rast("spatial_masks/…")` only worked because of the working directory. Always `file.path(mask_dir, name)`.
- **Calling `terra::extract` with an xy matrix per class:** see Pattern 1.
- **Runtime resampling/projection** of masks (D-13).
- **Letting the `run_allocation.r` sourcing `tryCatch` hide errors:** consider making sourcing failures fatal (`quit(status = 1)`), or at least add a post-source `stopifnot(exists("implement_spatial_interventions", mode = "function"))`.
- **Re-normalising after interventions:** don't. Keep the branch semantics (the hook sits after normalisation). See Open Question 1.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Grid equality | manual extent/res float compare | `terra::compareGeom(r, ref, stopOnError=FALSE)` | Handles CRS, extent tolerance, rows/cols, resolution |
| Value domain | reading all values into R | `terra::freq(r)` | Streams blockwise; 401M cells fit fine |
| Cell lookup | xy float matching | `terra::extract(r, <cell numbers>)` using the existing `ref_cell_id` | Exact, faster, no matrix |
| Transfer integrity | size compare | `sha256sum -c masks.sha256` | Available on both ends |
| class name → value | new schema parser | `load_allocation_class_map(config)` | Already used by allocation |

## Runtime State Inventory

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| Stored data | HPC `outputs/simulations/NAT/*/region_*` from the pre-intervention NAT run (a local copy at `D:/…/outputs/simulations/NAT/2024..2060` shows a full NAT chain exists). The smoke run writes `NAT/2032/region_costa_peruana/{probability_map_dir,posterior.tif,…}` with `overwrite=TRUE` | Tell the operator: the smoke overwrites that region/year, and all pre-intervention NAT/CUL/SOC/BAU outputs are obsolete once interventions are on (the Phase 4 sweep must rerun). Optionally `mv` that region dir aside first to keep a before/after comparison |
| Live service config | None. Rundeck/SLURM have no intervention-specific config (verified: submit scripts only pass env vars) | none |
| OS-registered state | None | none |
| Secrets/env vars | `HPC_SCRATCH_ROOT` (existing) drives the mask root. No new env var | none |
| Build artifacts | HPC repo checkout must be pulled on login02 (git doesn't work on compute nodes, per memory note). The local stale branch `spatial_interventions` == origin, so leave it | `git pull` on login02 before the smoke run. Masks go to beegfs via scp/rsync |

## Common Pitfalls

### Pitfall 1: Clean auto-merge hides runtime-broken hook
**What goes wrong:** `object 'class_name_to_value' not found` in every region worker. 2024 interventions skipped (year 2026).
**How to avoid:** Fix-up commit as in §Merge Dry Run items 1–2. Add a static test (in the `test-allocation-single-source-writer.R` style) asserting `generate_probability_maps <- function(` contains `year_post` and the hook uses `simulation_time_step = year_post`.
**Warning signs:** Log line "No interventions found … at time step 2026".

### Pitfall 2: NaN crash in `relative_prob_adjust` (VERIFIED by reproduction)
**What goes wrong:** When `Intervention_vals` (or `Non_Intervention_vals`) has no values > 0, `quantile(x[x>0])` = NA, the means become NaN, `Perc_diff` is NaN, and `if (Perc_diff > 0)` stops with "missing value where TRUE/FALSE needed". This is likely for NAT r3/r4 (Indigenous, Inside) where the lands overlap PAs already zeroed by r1, and for small regions or classes.
**How to avoid:** After computing the positive subsets: `if (!any(Intervention_vals > 0) || !any(Non_Intervention_vals > 0) || !is.finite(Perc_diff)) { log_msg("…skip: no positive probabilities…"); next }`. This is a guard, not a semantic change. Unit-test it.

### Pitfall 3: Dynamic "no entry → skip" masks config errors
**What goes wrong:** A typo in a Dynamic year key silently disables the intervention for that year.
**How to avoid:** D-14: the resolver errors when a year is in `Time_steps_implemented` but has no Dynamic entry. Keep the runtime `next` only as unreachable defence.

### Pitfall 4: Smoke with a year filter uses the 2022 map as anterior
**What goes wrong:** With `ALLOCATION_YEAR_POST_FILTER=2032`, `resume_active` is FALSE and `current_lulc_path` stays the **initial 2022 LULC**. The single pair 2028→2032 runs with the 2022 map as anterior (and 2028 predictors/rates). This is fine for a mechanism smoke but not a scientific 2032 result.
**How to avoid:** Document it in the smoke checkpoint, and don't compare it to the chained NAT 2032 outputs.

### Pitfall 5: The smoke script's default year is invalid
`submit_allocation_smoke.sh` computes `DEFAULT_YEAR_POST = simulation_start_year + step_length` = 2026, which is not a posterior year, so `filter_allocation_timesteps` stops. Always pass `ALLOCATION_YEAR_POST_FILTER=2032` explicitly (D-15 does). Optionally fix the default to `simulation_year_steps[2]`, which would be a Phase 3.6 leftover.

### Pitfall 6: Wrong partition → OOM
Bare `sbatch` lands on `compute` (93 GB). Pass `--partition=highmem` for costa_peruana (measured ~110 GB R-side peak). The others need `fat`.

### Pitfall 7: `reference_crs: "epsg:2056"` in config is wrong
The grid is EPSG:4326. The validator must compare to the ref raster (see §Mask Inventory).

### Pitfall 8: Case-sensitive filename on Linux
`src/implement_spatial_interventions.R` has an uppercase `.R`. The `src_files` entry must match exactly (the branch's does).

### Pitfall 9: Sum of per-cell probabilities can exceed 1 after Relative increases
The hook runs after normalisation. `Increase`/`Increase_inside_decrease_outside` multiply the top-percentile values by (1+p), and each value is capped at 1, but the per-cell sum is not. See Open Question 1. Don't silently change it.

## Code Examples

### Hook after the fix-ups (target shape)
```r
# src/allocation.r, generate_probability_maps(..., year_post, ...) — replaces L2882-2883 placeholder
class_name_to_value <- load_allocation_class_map(config)
normalized <- implement_spatial_interventions(
  normalized           = normalized,
  cell_index           = anterior_dt[, .(cell_id, ref_cell_id)],
  class_name_to_value  = class_name_to_value,
  interventions_dir    = config[["interventions_dir"]],
  mask_dir             = config[["spat_prob_perturb_dir"]],
  scenario             = scenario,
  simulation_time_step = year_post,        # D-07: posterior year of the pair
  log_file             = log_file,
  region_label         = region_label
)
data.table::setkey(normalized, row_idx)    # keep the writer's keyed .(k) lookup valid
```
(`anterior`/`trans_rates_dt` params: trim them. They are unused "reserved" params, and `cell_index` replaces `anterior`'s role. This is a discretion item.) `normalized` is modified by reference via `:=`, and `setkey` is preserved because `:=` on the non-key `prob` doesn't drop the key. Re-asserting `setkey` is cheap insurance.

### Smoke submission (operator, D-11/D-15)
```bash
# on login02 after git pull + mask transfer + validator PASS
sbatch --partition=highmem \
  --export=ALL,ALLOCATION_PROFILE_SCENARIO=NAT,ALLOCATION_REGION_FILTER=costa_peruana,ALLOCATION_YEAR_POST_FILTER=2032 \
  scripts/submit_allocation_smoke.sh
```
Why costa_peruana: it is the smallest/fastest region (fits highmem). NAT phase1 PA has 3.13M cells in region 3, `mining_concessions_mask` 5.62M, `indigenous_lands_mask` 5.83M, `urban_settlement_mask` 0.52M (zonal sums, verified). `NAT-costa_peruana-trans_rates-2028.csv` has rows to 104, 105 and 106 from 101/102/103/105, and 103→104 (verified locally), so all five NAT interventions have targets.

### Post-run assertion script (recommended: `scripts/verify_intervention_smoke.r`)
Inputs: `outputs/simulations/NAT/2032/region_costa_peruana/{probability_map_dir,trans_rates.csv}` + masks. For each row k with `To*` ∈ {104,105,106}: `r <- rast(sprintf("%03d_id_trans_%d.tif", k, id))`, `m <- crop(pa_phase1, r)` (same grid, so exact). Assert `global(r * (m==1), "max", na.rm=TRUE) == 0`. For `To* == 106`: also assert 0 where the concession mask is NA/≠1. Then grep the worker log for `AUDIT stage=intervention` lines (5 expected for NAT 2032), check that `posterior.tif` exists, and that exit status is 0. The zero assertion is robust because every later Relative adjustment is multiplicative (`prob + prob/100*p`), so zeros stay zero. Include `saturation_summary.csv` placed-vs-demanded in the report. Interventions shrink the feasible area, and under-placement of 105/106 is expected information, not a failure.

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `lulcc.spatprobmanipulation.r` + `spatial_interventions_prep.r` (raster::, SSP-indexed, prebuilt perturbation layers) | `implement_spatial_interventions.R` on the long DT, per-scenario YAML, static masks | branch be8121e (2026-06-08) | Legacy moves to `src/old/` (D-03). `raster::` hits drop out of active src (Phase 4 SC6 partial; `landscape_pattern_analysis.r` remains) |
| `config/SSP*_interventions.yml` | `config/{BAU,NAT,CUL,SOC}_interventions.yml` | b01bb71 | `docs/ARCHITECTURE.md` L100 must be updated |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Dinamica's per-transition probability maps tolerate a per-cell sum > 1 (no hard error); each map is consumed independently | Pitfall 9 / Open Q1 | If Dinamica or the saturation logic assumes sum ≤ 1, allocation could behave oddly. The smoke run will reveal a hard error |
| A2 | `globals` detection under `multisession` finds `implement_spatial_interventions` via `generate_probability_maps`'s body (it's a globalenv function). Smoke uses `sequential`, production uses `multicore` (fork), so both are fine regardless | Architecture | Only matters if someone forces multisession |
| A3 | The HPC mask dir `/beegfs/black/nascent-lulcc/inputs/spat_prob_perturb` is currently empty or absent | HPC Placement | If older SSP-era layers exist there, the orphan check will list them. Harmless |
| A4 | The pre-intervention NAT outputs on HPC mirror the local copy | Runtime State | Only affects the backup advice |

## Open Questions

1. **Re-cap the per-cell sum after interventions?**
   - Known: the branch hook sits after normalisation, and the helpers clamp per value to [0,1] only.
   - Unclear: whether the colleague intended a post-intervention renormalisation (the NCCS original is not in the repo).
   - Recommendation: keep it as is (the "no redesign" rule). Log the count of cells with sum > 1 in the AUDIT line so the Phase 4 review can decide.
2. **`spatial_interventions_integration_protocol.md` (cited by all YAML headers) is gitignored and missing.** Recommendation: reword the citation, or ask the colleague for it and add it under `docs/spatial_interventions/`.
3. **rsync is locked in D-09 but absent on the workstation.** Recommendation: document rsync (the canonical command) plus the scp fallback that actually works from this machine, and let the operator choose.
4. **Where the moved driver scripts go** (`src/old/` as D-03 says, or a new `scripts/old/`). Either works. The planner picks one.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Rscript (local) | tests, validator | ✓ (off PATH; prepend `/c/Program Files/R/R-4.5.0/bin`) | 4.5.0 | — |
| terra / data.table / yaml / testthat / withr (local) | tests, validator | ✓ | 1.9.34 / 1.18.4 / 2.3.12 / 3.3.2 / 3.0.3 | — |
| arrow, future (local) | lazy-predictor tests | ✗ | — | Those tests SKIP locally and run on HPC (existing behaviour) |
| git merge-tree | conflict dry run | ✓ | 2.55 | — |
| rsync (workstation) | D-09 transfer | ✗ | — | scp (OpenSSH 10.5) + sha256sum |
| Local mask dir `D:\C.3_Modelling\…\spat_prob_perturb` | SC2 | ✓ readable | — | — |
| Local `data_basepath` `E:/nascent-lulcc-agg` | local runs | ✗ (no E:) | — | data at `D:/C.3_Modelling/nascent-lulcc-agg`. Validator can take `--data-root` or run on HPC |
| HPC highmem partition + allocation_env | smoke | operator-verified in 3.6 | — | fat |

## Validation Architecture

`workflow.nyquist_validation` is `false` in `.planning/config.json`, so this section is skipped. Test notes for the planner:
- **Baseline (VERIFIED 2026-09-22, local, `testthat::test_dir("tests/testthat")` from repo root):** 222 passed, 13 skipped, **4 pre-existing errors** in `test-prep-paths.R` (its `.repo_root` resolves wrong under `test_dir`, so it can't open `src/calibration_predictor_prep.r`; unrelated to this phase). The gate is "no new failures versus this baseline".
- Command: `PATH="/c/Program Files/R/R-4.5.0/bin:$PATH" Rscript -e 'testthat::test_dir("tests/testthat")'`.
- New `tests/testthat/test-spatial-interventions.R`, a tempdir fixture with a tiny national raster, a mask, a YAML and a synthetic `normalized`/`cell_index`. It should cover: Absolute Inside/Outside, the From filter, Dynamic year selection, a year not implemented → unchanged, an implemented year without a Dynamic entry → error, the NaN guard (all-zero inside) → no error and unchanged, Relative Decrease lowers the top percentile inside, zeros stay zero after a Relative Increase, the resolver rejects a path separator, and missing file → pre-flight error line.
- Static assertions (house style): `src_files` contains `"src/implement_spatial_interventions.R"` and not `lulcc.spatprobmanipulation.r`. `generate_probability_maps` gets `year_post`.

## Security Domain

Low surface: repo-controlled YAML and local raster inputs, with no network or auth.

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V5 Input Validation | yes | Reject mask names with path separators or `..` (bare filenames only, D-05). Validate class names against the schema (the branch already `stop()`s on unknown names). Validate `Prob_adjust_type`/`zone`/`valency` enums |
| V2/V3/V4/V6 | no | — |

| Pattern | STRIDE | Mitigation |
|---------|--------|-----------|
| Path traversal via YAML mask name | Tampering | Basename check in the resolver |
| Silent config drift (typo'd year key) | Tampering/Repudiation | D-14 hard error + AUDIT lines |

## Sources

### Primary (HIGH confidence)
- `git merge-tree --write-tree main origin/spatial_interventions`, `git diff f09c064 origin/spatial_interventions`, `git show origin/spatial_interventions:<file>`: conflict set, hook diff, YAML content
- `src/allocation.r` L392-511 (pre-flight), L569-596 (year filter), L683 (class map), L1595-1810 (timestep pairs/resume), L2143-2323 (call site), L2470-2988 (`generate_probability_maps`); `src/setup.r` L260-333 (`build_full_config`); `scripts/run_allocation.r`; `scripts/submit_allocation_smoke.sh`
- terra 1.9.34 run on local masks: CRS/res/extent/compareGeom/freq, zonal sums by region, extract-by-cells vs xy equivalence and timing
- R reproduction of the `quantile(numeric(0))` → NaN → `if` error path
- Local `NAT-costa_peruana-trans_rates-2028.csv`; `sha256sum` of the masks; `command -v rsync` (absent)

### Secondary (MEDIUM confidence)
- User memory notes (Rundeck/sbatch partition routing, per-region peak RSS, git on compute nodes)

### Tertiary (LOW confidence)
- A1 (Dinamica tolerance of per-cell sum > 1): training knowledge only

## Metadata

**Confidence breakdown:**
- Merge/conflicts: HIGH (dry-run merge performed)
- Mask inventory and grid: HIGH (every file inspected with terra)
- Hook design and caching: HIGH (code read + timing measured); performance at big-region scale is MEDIUM (extrapolated)
- HPC staging: HIGH for paths, MEDIUM for operator steps (operator-gated)

**Research date:** 2026-09-22
**Valid until:** ~2026-10-22 (stable; re-run merge-tree if `main` moves in `generate_probability_maps`)
