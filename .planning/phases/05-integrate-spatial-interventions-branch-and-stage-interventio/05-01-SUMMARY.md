---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
plan: 01
subsystem: allocation / spatial-interventions
tags: [git-merge, spatial-interventions, config, D-01, D-02]
requires: []
provides:
  - "Feature branch spatial-interventions-integration with --no-ff merge of origin/spatial_interventions (a917ba1)"
  - "Branch DT-native src/implement_spatial_interventions.R sourced by scripts/run_allocation.r"
  - "interventions_dir config key in hpc_config.yaml and local_config.yaml"
affects:
  - src/allocation.r (auto-merged hook in generate_probability_maps(); known runtime-broken, fixed in Plans 02/04)
tech-stack:
  added: []
  patterns: ["real merge preserving colleague authorship (no squash/rebase)"]
key-files:
  created:
    - config/BAU_interventions.yml
    - config/NAT_interventions.yml
    - config/CUL_interventions.yml
    - config/SOC_interventions.yml
    - scenario_narratives.txt
    - spatial_masks/README.txt
    - spatial_masks/urban_settlement_mask_methodology.txt
  modified:
    - .gitignore
    - config/hpc_config.yaml
    - config/local_config.yaml
    - scripts/run_allocation.r
    - src/allocation.r
    - src/implement_spatial_interventions.R
  deleted:
    - config/SSP0_interventions.yml
    - config/SSP1_interventions.yml
    - config/SSP3_interventions.yml
    - config/SSP4_interventions.yml
    - config/SSP5_interventions.yml
decisions:
  - "Merged origin/spatial_interventions into feature branch spatial-interventions-integration via real --no-ff merge (D-01); colleague authorship preserved"
  - "src_files order: setup, utils, dinamica_utils, saturation_diagnostics, allocation, implement_spatial_interventions.R; lulcc.spatprobmanipulation.r dropped (D-03)"
  - "Auto-merged intervention hook in generate_probability_maps() left untouched (class_name_to_value / year_post fixes owned by Plans 02/04)"
metrics:
  duration: "~10 min"
  completed: 2026-09-22
  tasks: 2
  files: 18
---

# Phase 5 Plan 01: Merge spatial_interventions branch Summary

I merged `origin/spatial_interventions` (a917ba1, 4 commits by ManuelKurmann) into the new feature branch `spatial-interventions-integration` with a real `--no-ff` merge (f22bce8). The three textual conflicts were resolved as RESEARCH §Merge Dry Run prescribes. The SSPx intervention YAMLs are gone and BAU/NAT/CUL/SOC are in place. The test suite matches the baseline exactly.

## Commits

| Step | Commit | Description |
|------|--------|-------------|
| Precondition | ec3c10f | docs(05): planning state before merge (on main, before branching) |
| Task 1 | f22bce8 | Merge commit (parents ec3c10f, a917ba1) |
| Task 2 | — | Verification only, no file changes |

## Conflict Resolution

- **.gitignore:** kept both sides. Main's repeated `.planning/HANDOFF.json` / `.planning/config.json` lines were cut down to one each. Added the branch's `spatial_interventions_integration_protocol.md`, `spatial_masks_misc/` and `config/old/` blocks. CRLF line endings kept.
- **config/hpc_config.yaml:** took the branch side, `  scenario_to_ssp_mapping:` with no trailing space. `interventions_dir: "config"` auto-merged under `config_files_paths`, and `spat_prob_perturb_dir` was left untouched.
- **scripts/run_allocation.r:** `src_files` = setup, utils, dinamica_utils, saturation_diagnostics, allocation, `src/implement_spatial_interventions.R`. `lulcc.spatprobmanipulation.r` is not included.
- **src/allocation.r:** auto-merge accepted as-is. The hook replaced the `#todo integrate more recent approach` placeholder (1 `implement_spatial_interventions(` call, 0 todo lines).

## Verification

- HEAD is on `spatial-interventions-integration` and is a 2-parent merge. `git merge-base --is-ancestor a917ba1 HEAD` passes.
- `git log --format=%an d0a11df^..a917ba1` shows ManuelKurmann for all 4 commits.
- No conflict markers remain (`git grep` is clean outside *.md).
- `config/SSP*_interventions.yml` do not exist. BAU/NAT/CUL/SOC exist.
- `interventions_dir: "config"` is present in both hpc_config.yaml and local_config.yaml.
- Parse check: `src/implement_spatial_interventions.R` and `src/allocation.r` both parse (PARSE_OK).

### Test suite vs baseline

| Metric | Baseline | Post-merge |
|--------|----------|------------|
| Passed | 222 | 222 |
| Skipped | 13 | 13 |
| Failed | 0 | 0 |
| Errors | 4 (test-prep-paths.R) | 4 (test-prep-paths.R) |
| New failing files | — | 0 (NEW_BAD= 0) |

## Deviations from Plan

None. The plan was executed as written.

Side effect worth noting: the branch's b01bb71 also removed a trailing space on the `data_periods` line in hpc_config.yaml. That change auto-merged and is whitespace only.

## Known Stubs / Known Issues

- The intervention hook in `generate_probability_maps()` (src/allocation.r) is known to fail at runtime: `class_name_to_value` is undefined, and without `year_post` the first pair gets 2026, so the 2024 interventions are skipped. This was left in on purpose. Plans 02 and 04 own the fix. The branch must not be run on HPC before then.

## Self-Check: PASSED

- FOUND: src/implement_spatial_interventions.R, scripts/run_allocation.r, config/hpc_config.yaml, config/NAT_interventions.yml
- FOUND commits: ec3c10f, f22bce8
