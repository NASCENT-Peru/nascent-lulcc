---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
plan: 09
subsystem: spatial-interventions
tags: [validation, fail-fast, geometry, cache-correctness, gap-closure]
requires:
  - resolve_intervention_masks() + attr(., "entries") contract (Phase 5 Plan 07)
  - implement_spatial_interventions() resolver-driven applier (Phase 5 Plan 07)
  - config[["ref_grid_path"]] (already consumed by generate_probability_maps() for cellFromXY)
provides:
  - runtime mask geometry validation in .mask_inside_lut() (CR-02)
  - deterministic ref_cell_id range guard replacing terra::extract()'s stderr warning (CR-02b)
  - cell_index$cell_id input validation before max()/vector allocation (WR-10)
  - "intervention mask missing" marker on the TOCTOU path (IN-07)
  - mtime/size-aware LUT cache key, safe for a session-level cache (IN-03)
  - ref_grid_path as a required formal of implement_spatial_interventions()
affects:
  - src/allocation.r generate_probability_maps() hook (new required argument)
  - "Stage 7 pre-flight plan 05-10: the same nlyr/compareGeom condition must be reported pre-run"
  - "smoke verifier plan 05-14: new stop strings recorded below for the forbidden-marker list"
  - scripts/validate_intervention_masks.r (unchanged; runtime now reports the same condition)
tech-stack:
  added: []
  patterns:
    - validate-the-grid-before-trusting-a-cell-number
    - content-addressed cache key (path + mtime + size + input length)
    - TOCTOU re-raise onto an already-monitored log marker
    - hard failure over silent coercion for out-of-range raster indices
key-files:
  created:
    - .planning/phases/05-integrate-spatial-interventions-branch-and-stage-interventio/05-09-SUMMARY.md
  modified:
    - src/implement_spatial_interventions.R
    - src/allocation.r
    - tests/testthat/test-spatial-interventions.R
    - tests/testthat/test-spatial-interventions-wiring.R
decisions:
  - "ref_grid is a REQUIRED argument of .mask_inside_lut(), not the review's `ref_grid = NULL` default: a default that skips the geometry check would let the exact CR-02 failure mode back in through any future call site that forgets it"
  - "The engine takes ref_grid_path (a path), not a SpatRaster: it owns the read and the terra pointer lifetime stays inside one scope, so the hook cannot hand it a pointer from another session"
  - "terra::nlyr() is checked BEFORE compareGeom(), because compareGeom() ignores layer count by default and a 2-band mask on the correct grid passes it (verified empirically against terra 1.9.34)"
  - "ref_cell_id range is guarded explicitly against terra::ncell(mask) rather than relying on terra::extract()'s R warning, which goes to stderr and never reaches the per-region worker log the smoke verifier scans"
  - "The ref grid is read once, immediately after the D-14 missing-mask fail-fast and before the zero-intervention early return, so an unreadable ref grid is fatal even for a region with no active interventions"
  - "D-13 held: no terra::resample() or terra::project() was introduced; a grid mismatch remains a hard failure"
metrics:
  duration: ~35 min
  completed: 2026-09-23
  tasks: 3
  commits: 3
  red_failures: 23 (21 errors + 2 failures) against the pre-plan code
---

# Phase 5 Plan 09: Runtime Mask Geometry Validation Summary

`.mask_inside_lut()` now refuses to build a lookup table from any mask it has not proven is
single-layer and on the reference grid, and validates both `cell_index` columns before a cell
number is trusted.

## What was built

`.mask_inside_lut()` samples the mask with **raw national cell numbers**, which are only
meaningful on the exact grid that produced them (`config$ref_grid_path`, via
`terra::cellFromXY()` in the allocation hook). It never checked that the mask was on that grid.
The review reproduced two failure modes: a shifted-CRS mask applying an entirely fictional
geography with no warning at all, and a smaller-extent mask degrading to a truncated cell set
behind an `[extract] out of range cell numbers detected` R warning that goes to stderr and never
reaches the per-region worker log.

The only geometry check in the phase lived in `scripts/validate_intervention_masks.r`, a
standalone script wired into nothing. The runtime now makes the same assertion itself.

Order of checks inside `.mask_inside_lut(mask_path, cell_index, cache, ref_grid)`:

1. `cell_index$cell_id` validated non-empty / non-NA / `>= 1` — **first**, so a degenerate call
   fails identically whether or not the file exists (WR-10).
2. `normalizePath(mustWork = TRUE)` wrapped in `tryCatch` and re-raised on the
   `intervention mask missing` marker (IN-07).
3. Cache key composed as `path | mtime | size | length(cell_id)` (IN-03).
4. `terra::nlyr(m) != 1L` — before `compareGeom()`, which ignores layer count (CR-02c).
5. `terra::compareGeom(m, ref_grid, stopOnError = FALSE)` (CR-02a).
6. `ref_cell_id` range-guarded against `terra::ncell(m)` and length-matched to `cell_id`
   (CR-02b).
7. Unchanged return contract: `logical(max(cell_id))`, TRUE where the mask value is 1.

`implement_spatial_interventions()` gained a required `ref_grid_path` formal immediately after
`mask_dir`, reads the grid once after the D-14 missing-mask fail-fast, and passes the SpatRaster
into every LUT build. `generate_probability_maps()` supplies
`ref_grid_path = config[["ref_grid_path"]]` — the same grid `ref_cell_id` was computed from
~300 lines earlier.

## Exact stop messages introduced

Recorded here so the Stage 7 pre-flight plan (05-10) and the smoke verifier plan (05-14) can
match them verbatim.

| Finding | `sprintf()` format string | Fires from |
|---------|---------------------------|------------|
| WR-10 | `cell_index$cell_id must be non-empty, non-NA and >= 1` (literal, no format args) | `.mask_inside_lut()` |
| IN-07 | `intervention mask missing: %s (disappeared after pre-flight)` | `.mask_inside_lut()` |
| CR-02c | `intervention mask %s has %d layers (expected 1)` | `.mask_inside_lut()` |
| CR-02a | `intervention mask %s is not on the reference grid (crs/res/extent mismatch); cell-number lookup would be silently wrong` | `.mask_inside_lut()` |
| CR-02b | `cell_index$ref_cell_id values outside the reference grid 1..%.0f (or not aligned with cell_id) for mask %s` | `.mask_inside_lut()` |
| CR-02 (grid) | `intervention ref grid unreadable: %s (%s)` | `implement_spatial_interventions()` |

The IN-07 message deliberately contains the literal `intervention mask missing`, which
`scripts/verify_intervention_smoke.r:331-336` already treats as a fatal forbidden marker — no
change to that list is required for this path. The five other strings are **not** currently in
the forbidden-marker list; 05-14 should add at least
`is not on the reference grid`, `layers (expected 1)` and `intervention ref grid unreadable`.

`intervention ref grid unreadable` is also emitted to `log_file` via `log_msg()` before the
`stop()`, matching the existing D-14 missing-mask behaviour. The `.mask_inside_lut()` stops are
raised without a `log_msg()` call because the function has no `log_file` in scope; they surface
through the worker's error path, which is how the CR-01/CR-03 resolver stops already behave.

## Tasks completed

| Task | Name | Commit |
|------|------|--------|
| 1 | RED fixtures for mis-gridded masks and degenerate `cell_index` (D-23) | `c83db8f` |
| 2 | Make `.mask_inside_lut()` self-validating | `0980fa5` |
| 3 | Thread `ref_grid_path` from the allocation hook + wiring test | `11c380a` |

## Tests

New fixture helpers in `tests/testthat/test-spatial-interventions.R`: `.ref_grid()`,
`.write_mask_on()` (writes a 1-layer mask onto any template raster), `.write_multilayer_mask()`
and `.write_ref_grid()`. `.write_mask()` is now a thin wrapper over `.write_mask_on()` bound to
`.ref_grid()`, so the aligned and mis-aligned fixtures cannot drift apart.

New named regression blocks, each of which failed against the pre-plan code:

- `CR-02a: a mask on a shifted grid is rejected, not silently applied`
- `CR-02a: the engine aborts on a shifted mask without editing probabilities` (asserts the
  probability table is byte-identical to a pre-call `copy()` and that **no**
  `AUDIT stage=intervention region=` line was written)
- `CR-02b: a mask with a smaller extent is rejected before out-of-range extraction`
  (`expect_no_warning()` around the `expect_error()` — the pre-fix code warned and continued)
- `CR-02c: a multi-layer mask is rejected`
- `CR-02: an unreadable reference grid stops the engine`
- `WR-10: degenerate cell_index is rejected` (zero rows, `NA` `cell_id`, `cell_id == 0`)
- `WR-10b: out-of-range ref_cell_id is rejected`
- `IN-07: a mask removed after resolution reports the forbidden marker`
- `IN-03: the LUT cache key tracks file mtime` — this **replaces** the old
  `.mask_inside_lut maps ... and caches` stale-cache expectation, which asserted that
  overwriting the mask on disk returned the STALE LUT. The block comment now states that the
  previous expectation was the documented IN-03 hazard.

`tests/testthat/test-spatial-interventions-wiring.R` gained
`CR-02: the hook threads ref_grid_path into the interventions engine`, which locates the
`normalized <- implement_spatial_interventions(` call and asserts both that
`ref_grid_path = config[["ref_grid_path"]]` is present and that it sits immediately after
`mask_dir`.

**RED baseline (Task 1, against pre-plan `src/`):** 21 errors + 2 failures / 41 pass.
**After Task 2:** `test-spatial-interventions.R` 104 pass / 0 fail / 0 error.
**Full suite after Task 3:** 459 pass / 0 fail / 13 skip / 4 error.

The 4 errors are the pre-existing `test-prep-paths.R` failures (its `.repo_root` derives from
`sys.frame(1)$ofile`, which is `NULL` under `test_dir()`); the baseline for this worktree was
436 pass / 0 fail / 13 skip / 4 error, so the error count is unchanged and 23 assertions were
added.

## Requirements satisfied

- **CR-02** (runtime half) — mask geometry is validated at runtime; both reproduced failure
  modes now abort. The Stage 7 pre-flight half is 05-10's scope.
- **WR-10** — all three degenerate `cell_index` modes rejected with one named error.
- **IN-03** — cache key carries mtime + size + input length.
- **IN-07** — TOCTOU vanish lands on the monitored marker.
- **D-23** — every fix ships with a named regression test that failed against the pre-fix code.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing critical functionality] Required `ref_grid` instead of the review's `ref_grid = NULL` default**
- **Found during:** Task 2
- **Issue:** The review's suggested body used `ref_grid = NULL` with
  `if (!is.null(ref_grid) && ...)`, so any call site that omitted the argument would skip the
  geometry check entirely and silently reinstate CR-02.
- **Fix:** `ref_grid` is a required formal with no default, as the plan's `<interfaces>` block
  specifies. Every call site passes it by name.
- **Files modified:** `src/implement_spatial_interventions.R`
- **Commit:** `0980fa5`

**2. [Rule 2 - Missing critical functionality] Pre-check `file.exists(ref_grid_path)` before `terra::rast()`**
- **Found during:** Task 2 verification
- **Issue:** `terra::rast()` on a missing path raises the error but *also* emits a bare
  `GDAL error 4` **warning** to stderr first. testthat surfaced it as a `WARN`, and in
  production it would be the same class of stderr-only diagnostic that CR-02b is about.
- **Fix:** `ref_grid_path` is validated as a single non-empty scalar and `file.exists()`-checked
  before `terra::rast()` is called; both report through the same
  `intervention ref grid unreadable:` message and are logged via `log_msg()`.
- **Files modified:** `src/implement_spatial_interventions.R`
- **Commit:** `0980fa5`

**3. [Rule 2 - Missing critical functionality] `length(ref_cell_id) != length(cell_id)` added to the range guard**
- **Found during:** Task 2
- **Issue:** The plan's guard covered NA / `< 1` / `> ncell`. A `cell_index` whose two columns
  have different lengths would still reach `lut[cid[...]]` and recycle silently — the same class
  of defect as the WR-10 `cell_id == 0` mis-indexing.
- **Fix:** Length alignment is part of the same guard and the same message.
- **Files modified:** `src/implement_spatial_interventions.R`
- **Commit:** `0980fa5`

**4. [Rule 3 - Blocking] Moved the engine-level CR-02a test to the end of the file**
- **Found during:** Task 1
- **Issue:** The plan placed the engine twin next to the LUT-level CR-02a block, but
  `test_that()` bodies run at source time and `.engine_fixture()` / `.abs_entry()` / `.norm()` /
  `.run_engine()` are defined further down the file. The block errored with
  `could not find function ".engine_fixture"`.
- **Fix:** Relocated to a new `Phase 5 Plan 09 gap closure, engine half` section at the end of
  the file, alongside the 05-07 engine-half blocks.
- **Files modified:** `tests/testthat/test-spatial-interventions.R`
- **Commit:** `c83db8f`

**5. [Rule 1 - Bug] Wiring assertion uses the file's regex idiom, not a single-space literal**
- **Found during:** Task 3
- **Issue:** The plan's suggested `fixed = TRUE` literal
  `ref_grid_path = config[["ref_grid_path"]]` has one space around `=`, but the hook call site
  aligns its `=` signs (8 spaces). A `fixed` literal would have failed against correct code and
  would break on any future realignment.
- **Fix:** Used the `\\s*=\\s*` regex idiom already established in this file by the
  `mask_dir` / `interventions_dir` assertions, plus a second assertion that `ref_grid_path`
  immediately follows `mask_dir`. The plan's `regexpr(..., fixed = TRUE)` + `substr()` idiom for
  *locating* the call is preserved. The plan's own acceptance grep
  (`ref_grid_path *= *config\[\["ref_grid_path"\]\]`) is space-tolerant and returns 1.
- **Files modified:** `tests/testthat/test-spatial-interventions-wiring.R`
- **Commit:** `11c380a`

### Architectural changes

None. No new dependency, no schema change, no new file under `src/` or `scripts/`.

## Authentication Gates

None.

## Known Stubs

None. No placeholder, hardcoded-empty or TODO value was introduced.

## Threat Flags

None. No new network endpoint, auth path, file-access pattern or schema change at a trust
boundary was introduced. All five mitigations in the plan's register (T-05-31 .. T-05-35) are
implemented and each has a named regression test.

## Out of scope / deferred

- **Stage 7 pre-flight geometry check (CR-02, second half).** `validate_allocation_runtime()`
  still checks mask *existence* only. A mis-gridded mask is now fatal, but at the first
  intervention of the first region rather than before the job starts. This is 05-10's scope; the
  stop strings above are the contract it should mirror.
- **`scripts/verify_intervention_smoke.r` forbidden-marker list.** Only the IN-07 message lands
  on an existing marker. The geometry and ref-grid messages are recorded above for 05-14.
- **`scripts/validate_intervention_masks.r` is still wired into nothing.** The runtime now makes
  the same assertion, so the silent-corruption path is closed either way, but the script remains
  a manual gate.
- **WR-01** (`>` vs `>=` boundary, `Perc_diff == 0` dead zone) — untouched, as flagged by 05-07.
- **IN-06** (duplicated `.repo_root` incantation / `%||%` bootstrap in both test files) — the new
  blocks reuse the existing helper rather than adding a third copy; hoisting it into a
  `helper-*.R` remains open.
- The 4 pre-existing `test-prep-paths.R` errors were left alone per the execution brief.

## Self-Check: PASSED

All claimed files exist on disk and all claimed commits exist in `git log`:
`c83db8f`, `0980fa5`, `11c380a`, `8bf8250`.
