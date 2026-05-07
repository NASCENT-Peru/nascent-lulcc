---
phase: 02-model-size-reduction
plan: 04
subsystem: [scripts, training]
tags: [mlr3, retrain, rscript, region-filter]

requires:
  - phase: 02-02
    provides: transition_modelling() with mlr3 inner stack
  - phase: 02-03
    provides: qs::qread() loader in allocation.r
provides:
  - scripts/retrain_all_models.r — operator utility for mlr3 model re-training
affects: []

tech-stack:
  added: []
  patterns: [script-skeleton, region-filter-temp-csv, dry-run-mode]

key-files:
  created: [scripts/retrain_all_models.r]
  modified: []

key-decisions:
  - "--region filtering works by overriding config viable_transitions_lists path with a temp filtered CSV"
  - "--dry-run reads the already-filtered viable_transitions (region filter applies first)"
  - "Single-session use only — transition_modelling() handles internal parallelism"
  - "file.exists() validation on viable_transitions_lists path before read.csv()"

patterns-established:
  - "Region filter via temp CSV override: read -> filter -> tempfile -> config override -> call"

requirements-completed: [MEM-04]

duration: 12min
completed: 2026-05-07
---

# Phase 02 Plan 04: Create scripts/retrain_all_models.r Summary

One-liner: Operator utility script for mlr3 re-training of all ~140-160 transition-region model pairs, with --force, --dry-run, and --region flags; region filtering overrides config viable_transitions_lists path with a temp filtered CSV before calling transition_modelling().

## What Was Built

### Task 1: Create scripts/retrain_all_models.r

Created `scripts/retrain_all_models.r` (228 lines) as a new file. Structure follows the established `run_transition_modelling.r` skeleton pattern.

**Sections in order:**

1. **Shebang + comment block** — usage examples for all three flags (`--force`, `--dry-run`, `--region <name>`) and their combinations.

2. **start_time capture** — `start_time <- Sys.time()` before any work.

3. **Banner** — `cat()` block with "Re-training All Transition Models (mlr3)" heading.

4. **CLI arg parsing** — `commandArgs(trailingOnly = TRUE)` with:
   - `force_retrain <- "--force" %in% .cli_args`
   - `dry_run <- "--dry-run" %in% .cli_args`
   - `--region <name>` via positional arg after `--region` flag → `region_filter`

5. **Working directory resolution** — commandArgs `--file=` script_path pattern from `run_transition_modelling.r`, falls back to `getwd()` with basename check.

6. **Source loop** — `src/setup.r`, `src/utils.r`, `src/transition_modelling.r` each wrapped in `tryCatch(..., error = function(e) { cat(...); quit(status=1) })`. No `install.packages()`.

7. **Config load** — `config <- tryCatch(get_config(), error = function(e) { ... quit(status=1) })` — fail-fast.

8. **Display resolved settings** — model dir, eval dir, force, dry_run, region_filter.

9. **Region filter** (when `region_filter` non-NULL):
   - Resolves `vt_path <- config[["viable_transitions_lists"]]`
   - `file.exists(vt_path)` validation BEFORE `read.csv()` — quits with status 1 if missing
   - Reads full CSV, filters to `viable_all$region_name %in% region_filter`
   - If 0 rows: lists available `region_name` values and quits with status 1
   - Writes filtered rows to `tempfile(fileext = ".csv")`
   - Overrides `config[["viable_transitions_lists"]] <- temp_viable`
   - Prints count: "N of M viable transitions will be re-trained"

10. **Dry-run handler** (when `dry_run` TRUE, after region filter step):
    - Reads `config[["viable_transitions_lists"]]` (already region-filtered temp CSV if `--region` was set)
    - `file.exists()` check before read — prints message and quits status 0 if missing
    - Filters out self-transitions, NA/zero `rate_2018_2022`, `whole_map` rows
    - Prints `[DRY RUN] Would re-train N transition-region pairs:` and each pair
    - `quit(status=0)` — no training

11. **Main call** — `tryCatch(transition_modelling(config=config, refresh_cache=force_retrain, use_regions=NULL, model_specs_path=NULL, periods_to_process=NULL), error = function(e) { cat(...); quit(status=1) })`

12. **Summary file** — written to `config[["transition_model_eval_dir"]]` with job ID, start/end timestamps, runtime hours, force/dry_run/region values. Directory created if missing.

13. **Final message and `quit(status=0)`**

**`%||%` operator:** Defined at file scope in `src/transition_modelling.r` (line 2404) — available after sourcing. No local definition needed in the script; used in display of `region_filter %||% "all"` and in summary file write.

## Verification Results

| Check | Command | Result |
|-------|---------|--------|
| transition_modelling( call count | `grep -c "transition_modelling(" scripts/retrain_all_models.r` | 5 (call + comment + source path + 2 references) |
| get_config count | `grep -c "get_config" scripts/retrain_all_models.r` | 1 |
| install.packages count | `grep -c "install.packages" scripts/retrain_all_models.r` | 0 |
| dry_run references | `grep -c "dry_run" scripts/retrain_all_models.r` | 4 |
| region_filter references | `grep -c "region_filter" scripts/retrain_all_models.r` | 7 (definition, display, filter logic, config override, summary) |
| file.exists matches | `grep -n "file.exists" scripts/retrain_all_models.r` | 2 (lines 120, 154) |
| force_retrain references | `grep -c "force_retrain" scripts/retrain_all_models.r` | 4 |
| viable_transitions_lists references | `grep -c "viable_transitions_lists" scripts/retrain_all_models.r` | 11 |
| quit(status calls | `grep -c "quit(status" scripts/retrain_all_models.r` | 8 |
| src/setup.r sourced | `grep -c "src/setup.r" scripts/retrain_all_models.r` | 1 |
| src/transition_modelling.r sourced | `grep -c "src/transition_modelling.r" scripts/retrain_all_models.r` | 2 (source + comment) |
| Shebang | `head -1 scripts/retrain_all_models.r` | `#!/usr/bin/env Rscript` |
| region_filter in filter logic | `grep -n "region_filter"` | `viable_all$region_name %in% region_filter` at line 125 |
| region_filter in config override | `grep -n "region_filter"` | `config[["viable_transitions_lists"]] <- temp_viable` at line 138 |

All acceptance criteria from the plan pass.

## Deviations from Plan

None — plan executed exactly as written. The `%||%` operator check confirmed it is defined at file scope in `src/transition_modelling.r` (line 2404), so no local definition was needed.

## Known Stubs

None — the script fully wires the operator's re-training workflow. The `--region` filter, `--dry-run` preview, and `--force` refresh_cache flag are all implemented end-to-end.

## Threat Flags

No new security-relevant surface beyond what the plan's threat model covered:
- T-02-08: `--region` value used only in row-level string filter (`region_name %in% region_filter`), never in file path construction or code eval — accepted.
- T-02-09: `--force` flag is operator-only, long-running by design — accepted.
- T-02-10: Summary file written to operator-configured eval directory, contains timing/paths/status only — accepted.

## Self-Check: PASSED

- `scripts/retrain_all_models.r` exists: FOUND
- Task 1 commit `6430a47` exists in git log: FOUND
- All acceptance criteria verified against grep outputs above
