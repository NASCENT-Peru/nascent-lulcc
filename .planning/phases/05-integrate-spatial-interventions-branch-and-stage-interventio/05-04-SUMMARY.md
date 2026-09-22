---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
plan: 04
subsystem: allocation / spatial interventions
tags: [interventions, allocation, preflight, wiring, tdd]
requires: ["05-02", "05-03"]
provides:
  - generate_probability_maps(..., year_ant, year_post, ...) with the interventions hook on the posterior year
  - D-14 intervention mask/config lines in validate_allocation_runtime()
  - fatal sourcing guard in scripts/run_allocation.r
affects: ["05-05 (validator / operator runbook reuse the same resolver and error formats)"]
tech-stack:
  added: []
  patterns: [call-time exists(mode = "function") lookup, consolidated pre-flight error list, static text wiring tests]
key-files:
  created:
    - tests/testthat/test-spatial-interventions-wiring.R
  modified:
    - src/allocation.r
    - scripts/run_allocation.r
decisions:
  - "Intervention pre-flight lines are collected in a separate intervention_errors vector and appended after the file checks (section 3b), so fixture mode can never produce them"
  - "A non-integer ALLOCATION_YEAR_POST_FILTER is reported as an 'intervention config:' line (not a stop); a non-posterior value leaves nothing to check because filter_allocation_timesteps() stops on it later"
  - "Hook re-keys normalized on row_idx after the engine call so the writer's normalized[.(k)] lookup is unaffected by any engine-side reordering"
metrics:
  duration: ~20min
  completed: 2026-09-22
  tasks: 2
  files: 3
---

# Phase 5 Plan 04: Wire interventions into allocation Summary

Allocation now calls the Plan 02 engine with its new signature and the posterior year (D-07). Missing intervention masks and broken intervention YAMLs show up as lines in the single Stage 7 pre-flight list, before any region work starts (D-14). `run_allocation.r` now exits non-zero if any source file fails to load.

## Tasks

| # | Task | Commits |
|---|------|---------|
| 1 | Thread year_post, fix the hook, harden sourcing | 56f0172 (test RED), 8567f64 (fix GREEN) |
| 2 | D-14 pre-flight mask checks + repo YAML/timestep consistency | bbed8bf (test RED), be71313 (feat GREEN) |

## What changed

- `generate_probability_maps()` gains a `year_post` formal (with roxygen), and `setup_allocation_inputs()` passes `year_post = year_post`. The hook now calls `implement_spatial_interventions()` with these arguments: `cell_index = anterior_dt[, .(cell_id, ref_cell_id)]`, `class_name_to_value <- load_allocation_class_map(config)`, `mask_dir = config[["spat_prob_perturb_dir"]]`, `interventions_dir = config[["interventions_dir"]]`, `simulation_time_step = year_post` and `region_label`. After the call, `normalized` is re-keyed on `row_idx`. The `year_ant + step_length` formula is gone, and the writer loop is byte-identical. This fixes the interim break noted in 05-02 (the call at old L2884 would have failed with an unused-argument error).
- `validate_allocation_runtime()` handles non-fixture configs as follows:
  - It runs only when `interventions_dir` and `spat_prob_perturb_dir` are non-empty and `resolve_intervention_masks` exists.
  - It resolves masks for each `scenario_names` entry and each posterior year in `tail(simulation_year_steps, -1)`, narrowed by `ALLOCATION_YEAR_POST_FILTER`.
  - It appends one `intervention mask: missing <path> (scenario=.. id=.. year=..)` line per absent file.
  - Resolver errors become `intervention config: <msg>` lines. It never stops partway through.
- `scripts/run_allocation.r` behaves as follows:
  - The source error handler calls `quit(save = "no", status = 1)`.
  - After the loop, `stopifnot()` requires both `implement_spatial_interventions` and `resolve_intervention_masks` to exist.
  - `src_files` is unchanged.
- ALLOCATION_PROFILE_SCENARIO narrowing: `scripts/run_allocation.r` L180-193 sets `config$scenario_names <- profile_scenario` before `run_preflight_and_print(config = config)` (L226), so the pre-flight only checks the running scenario. No extra narrowing is needed inside the check.

## Verification

- Wiring + single-source-writer filtered run: 0 failures.
- `test-spatial-interventions-wiring.R` covers:
  - 7 static tests.
  - 6 pre-flight tests: missing mask, all present, config error, year filter, skipped check, fixture mode.
  - 8 repo-consistency tests (4 scenarios x local/hpc config). All Time_steps_implemented years are posterior years, and every resolved mask_name is bare.
- Full suite: 378 expectations, 365 pass, 0 fail, 13 skip, 4 errors (all in test-prep-paths.R, pre-existing). `NEW_BAD= 0`.
- Acceptance greps:
  - `simulation_time_step = year_post` = 1
  - `year_ant + config[["step_length"]]` = 0
  - `load_allocation_class_map(config)` = 3
  - aligned `mask_dir = config[["spat_prob_perturb_dir"]]` = 1
  - `quit(save = "no", status = 1)` = 1 and `exists("implement_spatial_interventions", mode = "function")` = 1 in run_allocation.r
  - intervention mask format = 1
  - `"intervention config: %s"` = 1
  - `exists("resolve_intervention_masks", mode = "function")` = 1 in allocation.r

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Validation] Non-integer ALLOCATION_YEAR_POST_FILTER surfaced in pre-flight**
- **Found during:** Task 2
- **Issue:** Mirroring `filter_allocation_timesteps()` parsing exactly would mean calling `stop()` inside the pre-flight, which would break the "one consolidated list" contract.
- **Fix:** A non-integer value adds `intervention config: ALLOCATION_YEAR_POST_FILTER must be an integer posterior year`, which is the same message `filter_allocation_timesteps()` uses, and the mask checks are skipped.
- **Commit:** be71313

### Notes

- On Windows, `Rscript -e` with a `|` inside the argument fails with "The system cannot find the path specified", so the verify commands were run from script files in the scratchpad. This has no effect on the code.

## Known Stubs

None.

## Threat Flags

None. The only new file reads are the mask existence checks under the config-provided `spat_prob_perturb_dir`, which are covered by T-05-15.

## TDD Gate Compliance

Both tasks have a `test(` RED commit before the GREEN commit (56f0172 -> 8567f64, bbed8bf -> be71313). Task 1 RED had 14 failing expectations. Task 2 RED had 5 failing expectations, from the missing-mask and config-error tests.

## Self-Check: PASSED

- FOUND: tests/testthat/test-spatial-interventions-wiring.R, src/allocation.r, scripts/run_allocation.r
- FOUND commits: 56f0172, 8567f64, bbed8bf, be71313
