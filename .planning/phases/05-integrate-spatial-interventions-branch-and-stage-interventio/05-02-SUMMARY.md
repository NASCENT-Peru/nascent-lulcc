---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
plan: 02
subsystem: allocation / spatial interventions
tags: [interventions, masks, audit-logging, tdd]
requires: ["05-01"]
provides:
  - resolve_intervention_masks(interventions_dir, mask_dir, scenario, years)
  - .mask_inside_lut(mask_path, cell_index, cache)
  - implement_spatial_interventions(normalized, cell_index, class_name_to_value, interventions_dir, mask_dir, scenario, simulation_time_step, log_file, region_label)
  - absolute_prob_adjust / relative_prob_adjust returning list(normalized, rows_target, rows_changed)
affects: ["05-04 (allocation.r caller rewire)", "05-05 (pre-flight / validator consume resolver)"]
tech-stack:
  added: []
  patterns: [per-call cached cell-number LUT, AUDIT key=value log lines, fail-fast pre-resolution]
key-files:
  created:
    - tests/testthat/test-spatial-interventions.R
  modified:
    - src/implement_spatial_interventions.R
decisions:
  - "Resolver is base R + yaml:: only (no %||%, no data.table) so it can be sourced into baseenv-parented envs and older R"
  - "rows_target = target-zone rows for Absolute, all target-class rows (both zones) for Relative; rows_changed counts selected rows whose prob differs (NA-aware)"
  - "An intervention whose classes are all NaN-skipped still counts as applied and gets its AUDIT line (rows_changed=0)"
  - "No renormalisation after interventions; only cells_sum_gt1 is logged (RESEARCH Open Question 1)"
metrics:
  duration: ~25min
  completed: 2026-09-22
  tasks: 2
  files: 2
---

# Phase 5 Plan 02: Intervention engine hardening Summary

The intervention engine now resolves every mask up front as a bare filename under `mask_dir` via a shared resolver (D-05). It stops before editing anything if a mask is missing or an implemented Dynamic year has no entry (D-14). Mask membership comes from a per-call cached logical LUT built with a single cell-number `terra::extract` per mask. The relative-adjustment NaN path is skipped and logged, and each applied intervention writes one `AUDIT stage=intervention` line plus one summary line per call (D-15).

## Tasks

| Task | Name | Commits |
| ---- | ---- | ------- |
| 1 | Shared mask resolver and cached cell-number LUT | 533a070 (test RED), c2ae21d (feat GREEN) |
| 2 | Rewire engine (LUT, fail-fast, NaN guard, log_msg + AUDIT) | 89887f3 (test RED), 92a2da7 (feat GREEN) |

## Verification

- `test-spatial-interventions.R`: 55 expectations, 0 failures (19 test_that blocks).
- Full suite: 277 pass / 13 skip / 0 fail / 4 errors (the same 4 in test-prep-paths.R as before). Baseline was 222 pass, so this plan adds 55 passing expectations.
- Static greps: `cat(` = 0, `AUDIT stage=intervention region=%s` = 1, `AUDIT stage=intervention_summary` = 1, `is.finite(Perc_diff)` = 1, no `as.matrix(normalized`, no `Intervention_mask,` helper formals, and no `anterior,`/`trans_rates_dt,` in the engine formals.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Robustness] Avoided base `%||%` in the resolver**
- **Found during:** Task 1
- **Issue:** base `%||%` exists only in R >= 4.4. The resolver is meant to be sourced into baseenv-parented envs (Plan 04), where the helper that `allocation.r` defines is not visible.
- **Fix:** Replaced it with explicit `is.null()` checks.
- **Commit:** c2ae21d

**2. [Rule 2 - Validation] Added enum stops for Prob_adjust_zone and Prob_adjust_valency**
- **Found during:** Task 2
- **Issue:** On the branch, an unknown zone left `Target_area_idx` undefined, which gave an obscure error. An unknown valency silently did nothing.
- **Fix:** The helpers now `stop("Unknown Prob_adjust_zone: ...")` / `stop("Unknown Prob_adjust_valency: ...")`. The engine also stops on an empty `Transition_target_classes` translation.
- **Commit:** 92a2da7

**3. [Test design] LUT cache assertion overwrites instead of deleting the mask**
- The plan's key is `normalizePath(mustWork = TRUE)`, so a deleted file cannot be looked up in the cache. The test overwrites the mask with different cells and checks that the cached call still returns the original LUT, while a fresh cache returns the new one.

## Known Interim Break

- `src/allocation.r:2884` still calls `implement_spatial_interventions()` with the old signature (`anterior`, `trans_rates_dt`, no `cell_index`/`mask_dir`). Until Plan 05-04 rewires it (per plan: `files_modified: src/allocation.r`), that call would fail with an unused-argument error. No allocation run should happen between 05-02 and 05-04.

## Known Stubs

None.

## TDD Gate Compliance

Both tasks have a `test(` RED commit before the `feat(` GREEN commit (533a070 -> c2ae21d, 89887f3 -> 92a2da7). RED runs: 10 errors (Task 1) and 9 failing engine tests (Task 2) before implementation.

## Self-Check: PASSED

- FOUND: src/implement_spatial_interventions.R
- FOUND: tests/testthat/test-spatial-interventions.R
- FOUND commits: 533a070, c2ae21d, 89887f3, 92a2da7
