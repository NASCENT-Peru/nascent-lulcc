---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
plan: 03
subsystem: spatial-interventions
tags: [config, yaml, docs, refactor, housekeeping]
requires:
  - 05-01 (branch merged: scenario YAMLs, spatial_masks/ docs, legacy prep code present)
  - 05-02 (resolve_intervention_masks expects bare filenames under mask_dir)
provides:
  - D-05 compliant scenario YAMLs (bare mask filenames, 14 distinct masks)
  - Single active intervention path (legacy prep parked in src/old/)
  - docs/spatial_interventions/ Markdown doc set
affects:
  - config/{BAU,NAT,CUL,SOC}_interventions.yml
  - docs/ARCHITECTURE.md, docs/DEPLOYMENT.md, docs/DEVELOPMENT.md, README.md, scripts/master_pipeline.sh
tech-stack:
  added: []
  patterns: [git mv for history-preserving moves, pure-rename commit before content-change commit]
key-files:
  created:
    - docs/spatial_interventions/scenario_narratives.md
    - docs/spatial_interventions/spatial_masks.md
    - docs/spatial_interventions/urban_settlement_mask_methodology.md
    - docs/spatial_interventions/integration_explainer.md
    - docs/spatial_interventions/intervention_planning.md
    - src/old/lulcc.spatprobmanipulation.r
    - src/old/spatial_interventions_prep.r
    - src/old/run_spatial_interventions_prep.r
    - src/old/submit_spatial_interventions_prep.sh
  modified:
    - config/BAU_interventions.yml
    - config/NAT_interventions.yml
    - config/CUL_interventions.yml
    - config/SOC_interventions.yml
    - docs/ARCHITECTURE.md
    - docs/DEPLOYMENT.md
    - docs/DEVELOPMENT.md
    - README.md
    - scripts/master_pipeline.sh
decisions:
  - "Stage 6 spatial-interventions prep is retired: interventions run inside allocation (Stage 7); legacy code in src/old/, table rows marked 'retired' where removal would break stage numbering"
  - "The unversioned colleague protocol (spatial_interventions_integration_protocol.md) is cited as 'not versioned in this repo' rather than removed, keeping §-references"
  - "Aligned text blocks in converted docs are rendered as fenced text blocks (not pipe tables) to preserve every number and alignment verbatim"
metrics:
  duration: ~12 min
  completed: 2026-09-22
  tasks: 3
  files: 27
---

# Phase 5 Plan 03: Intervention Housekeeping (YAML prefixes, legacy retirement, docs move) Summary

The four scenario YAMLs now use bare mask filenames that match the Plan 02 resolver contract. The legacy SSP-indexed intervention prep is parked in `src/old/` with its history intact. All intervention prose now lives as Markdown under `docs/spatial_interventions/`.

## Tasks

| # | Task | Commit | Key files |
|---|------|--------|-----------|
| 1 | Strip `spatial_masks/` prefixes and fix YAML headers (D-05, D-06) | 5d5be7e | config/{BAU,NAT,CUL,SOC}_interventions.yml |
| 2 | Retire legacy prep to src/old and update callers (D-03) | d9c2763 | src/old/*, docs/ARCHITECTURE.md, docs/DEPLOYMENT.md, docs/DEVELOPMENT.md, README.md, scripts/master_pipeline.sh |
| 3a | Pure moves into docs/spatial_interventions (D-04) | f56f433 | 5 renames |
| 3b | Convert to Markdown (D-04) | 9f95e1c | docs/spatial_interventions/*.md |

## Verification

- `grep -c spatial_masks/` returns 0 on all four YAMLs. The yaml parse check prints `MASKS_OK 14` (14 distinct bare names, no path separators).
- YAML diff gate is clean: only header comments and `.tif` mask lines changed. Rankings, values, zones, years and class names are untouched.
- No references to the four legacy paths remain outside `.planning/` and `src/old/`. `git log --follow src/old/lulcc.spatprobmanipulation.r` reaches the initial commit.
- `raster::` grep: only `src/landscape_pattern_analysis.r` matches.
- `bash -n scripts/master_pipeline.sh` passes.
- `spatial_masks/`, `scenario_narratives.txt`, `intervention_planning.txt` and the root explainer are gone. Each of the 5 docs has a `# ` heading. `git log --follow docs/spatial_interventions/scenario_narratives.md` reaches d0a11df.
- No `config/SSP*_interventions.yml` exists (D-02).
- testthat: 277 pass / 0 fail / 13 skip / 4 errors. This matches the post-05-02 baseline, and all 4 errors are pre-existing in test-prep-paths.R.

## Deviations from Plan

### Minor adjustments (no rule trigger)

1. **Explainer was already Markdown.** `integration_explainer.md` was only moved. It got a provenance note plus a "historical pre-integration briefing" note, and its relative links (`](src/...`, `](config/...`) were rewritten to `../../` so they still resolve from the new location.
2. **Scenario narrative icon alt-text dropped.** Characteristics lines such as `Climate IconClimate Change: RCP 2.6` were web-scrape artefacts. They became list items without the `<X> Icon` prefix; values are unchanged.
3. **Two in-doc citations rewritten.** In spatial_masks.md, `scenario_narratives.txt L134` became `scenario_narratives.md, BAU section`, because the old line number is meaningless after conversion and the verify gate forbids the `.txt` name. The "This README.txt / methodology.txt" file-list entries now point at their new paths. The number check confirms that every other number in the converted docs is preserved.
4. **DEPLOYMENT.md Stage 7 comment.** "once Stage 6 has finished" became "once Stage 5 has finished", because Stage 6 no longer produces anything.

None of these change intervention semantics.

## Known Stubs

None.

## Self-Check: PASSED

- FOUND: config/*_interventions.yml (bare names), src/old/{lulcc.spatprobmanipulation.r,spatial_interventions_prep.r,run_spatial_interventions_prep.r,submit_spatial_interventions_prep.sh}, docs/spatial_interventions/{scenario_narratives,spatial_masks,urban_settlement_mask_methodology,integration_explainer,intervention_planning}.md
- FOUND commits: 5d5be7e, d9c2763, f56f433, 9f95e1c
