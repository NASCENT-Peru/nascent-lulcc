---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
plan: 10
subsystem: spatial-interventions
tags: [pre-flight, fail-closed, geometry, profile-mode, gap-closure]
requires:
  - resolve_intervention_masks() + its 9-column contract (Phase 5 Plan 07)
  - .mask_inside_lut() runtime nlyr/compareGeom assertions (Phase 5 Plan 09)
  - config[["ref_grid_path"]] (config/hpc_config.yaml:45, config/local_config.yaml:42)
provides:
  - fail-closed D-14 intervention gate in validate_allocation_runtime() (WR-04)
  - profile_timestep_index narrowing in the pre-flight, ordered after ALLOCATION_YEAR_POST_FILTER (WR-05)
  - per-unique-mask nlyr/compareGeom/readability pre-flight geometry pass (CR-02, pre-flight half)
  - "six new pre-flight error lines recorded verbatim below for 05-14 and 05-16"
affects:
  - "smoke verifier plan 05-14: 'cannot verify mask geometry' and 'intervention mask: unreadable' are new strings, not yet in the forbidden-marker list"
  - "operator plan 05-16: the six lines below are greppable in the Stage 7 job log"
  - tests/testthat/test-spatial-interventions-wiring.R (.wiring_config() gained two arguments)
tech-stack:
  added: []
  patterns:
    - fail-closed gate (a missing dependency is an error line, never a skipped check)
    - header-only geometry assertion (terra::rast() reads no cell values)
    - deduplicate-then-verify (each unique mask path opened at most once per pre-flight)
    - pre-flight mirrors the driver's narrowing in the driver's order
key-files:
  created:
    - .planning/phases/05-integrate-spatial-interventions-branch-and-stage-interventio/05-10-SUMMARY.md
  modified:
    - src/allocation.r
    - tests/testthat/test-spatial-interventions-wiring.R
decisions:
  - "The engine check became two sibling `if`s keyed on `interventions_configured`/`engine_loaded` rather than the review's nested `if/else`: it keeps the 45-line resolution body at its original indentation, so the diff is the guard change only and not a whole-block reflow"
  - "The geometry pass runs only when at least one mask actually exists. When every mask is missing, the missing-mask lines are the actionable gap; adding 'cannot verify mask geometry' on top would be noise, and it would have broken the pre-existing `resolver config errors become 'intervention config:' lines` block"
  - "profile_timestep_index is only validated when years is non-empty, so a run whose year filter already emptied the schedule does not also get a confusing `outside the valid range 1..0` line"
  - "nlyr() is checked before compareGeom() and `next`s on failure, mirroring .mask_inside_lut() from 05-09: compareGeom() ignores layer count, so the opposite order lets a 2-band mask on the correct grid through"
  - "terra::rast() calls in the pass are wrapped in suppressWarnings(): GDAL emits a bare stderr warning before the R error on a non-raster file, which is exactly the stderr-only diagnostic class CR-02b was about. The condition is still reported, as an error line"
  - "The pre-flight compareGeom line drops the runtime message's trailing '; cell-number lookup would be silently wrong' clause (it describes a lookup that has not happened yet) but keeps the `is not on the reference grid (crs/res/extent mismatch)` substring byte-identical, which is what 05-14 greps"
  - "The existing pre-flight blocks' zero-byte `file.create()` mask stubs were replaced with real single-layer rasters. Their assertions are unchanged; a zero-byte .tif is now correctly reported as unreadable, which is the point of the feature"
metrics:
  duration: ~45 min
  completed: 2026-09-28
  tasks: 2
  commits: 2
  red_failures: 24 (24 failures + 0 errors) against the pre-plan src/allocation.r
---

# Phase 5 Plan 10: Fail-Closed, Geometry-Aware Stage 7 Intervention Pre-Flight Summary

`validate_allocation_runtime()`'s D-14 gate no longer disappears when the interventions engine is
absent, checks that every staged mask is single-layer and on the reference grid before any region
work starts, and narrows to `profile_timestep_index` exactly the way the driver does.

## What was built

### WR-04 — the gate fails closed

`src/allocation.r:448-449` used to read:

```r
if (!is.null(interventions_dir) && !is.null(mask_dir) &&
    exists("resolve_intervention_masks", mode = "function")) {
```

A missing mask file was a hard failure; a missing *resolver* was a silent PASS. The engine check is
now a separate `engine_loaded` predicate and the two states are two sibling `if`s, so the absent
engine produces an error line and the resolution body keeps its original indentation.

### WR-05 — the gate narrows the way the driver narrows

`config[["profile_timestep_index"]]` is written by `scripts/run_allocation.r:208-219` and consumed
by the driver at `src/allocation.r` (`"Profile mode: restricting scenario ..."`), *after*
`filter_allocation_timesteps()`. The pre-flight mirrored `ALLOCATION_YEAR_POST_FILTER` but not the
index, so a run legitimately pinned to one timestep was still rejected for any mask belonging to
the other nine posterior years — a realistic state on a partially staged HPC deployment. The
pre-flight now applies the same narrowing in the same order, and reports a non-integer or
out-of-range value rather than ignoring it.

### CR-02 (pre-flight half) — geometry is checked while it is cheap

Plan 05-09 made a mis-gridded mask fatal inside `.mask_inside_lut()`, but only at the first
intervention of the first region — hour 3 of a run. The resolution loop now also collects the mask
paths that *do* exist, `unique()`s them across scenarios, opens the reference grid once and each
mask at most once, and asserts readability, `nlyr == 1` and
`compareGeom(mask, ref_grid, stopOnError = FALSE)`.

**D-13 held.** `terra::rast()` reads the header only — no cell values, no `resample()`, no
`project()`. `grep -c "resample(\|project(" src/allocation.r` is **0 before and 0 after** this
plan. A grid mismatch is a hard failure, never a repair.

## Exact pre-flight error lines introduced

Every line is appended to `intervention_errors` and surfaces through the single consolidated gap
list at the `errors <- c(errors, intervention_errors)` join, so they arrive together, before any
work. Recorded verbatim so 05-14 can extend the forbidden-marker list and 05-16 can grep the
Stage 7 job log.

| Finding | `sprintf()` format string | Fires when |
|---------|---------------------------|------------|
| WR-04 | `intervention config: resolve_intervention_masks() not loaded - source src/implement_spatial_interventions.R` (literal, no format args) | interventions are configured but the engine is not in scope |
| WR-05 | `intervention config: profile_timestep_index must be a positive integer` (literal, no format args) | the value is not a length-1 integer >= 1 |
| WR-05 | `intervention config: profile_timestep_index=%d is outside the valid range 1..%d` | the value exceeds the number of remaining posterior years |
| CR-02 | `intervention mask: cannot verify mask geometry, reference grid missing: %s` | `config[["ref_grid_path"]]` is empty, absent or unreadable while masks exist |
| CR-02 | `intervention mask: unreadable %s (%s)` | `terra::rast(mask_path)` errors |
| CR-02 | `intervention mask: %s has %d layers (expected 1)` | `terra::nlyr()` is not 1 |
| CR-02 | `intervention mask: %s is not on the reference grid (crs/res/extent mismatch)` | `terra::compareGeom(..., stopOnError = FALSE)` is not TRUE |

When `ref_grid_path` is unset or not a length-1 non-empty string, the `%s` in the
`cannot verify mask geometry` line renders as the literal `<config[["ref_grid_path"]] unset>`
rather than an empty string, so the line is never truncated to a dangling colon.

### Relationship to the 05-09 runtime strings

05-09's `## Exact stop messages introduced` table is the runtime contract. The two overlapping
pre-flight lines keep the substrings 05-09 nominated for the forbidden-marker list byte-for-byte:

| 05-09 nominated substring | Present in the 05-10 pre-flight line |
|---------------------------|--------------------------------------|
| `is not on the reference grid` | yes — `intervention mask: %s is not on the reference grid (crs/res/extent mismatch)` |
| `layers (expected 1)` | yes — `intervention mask: %s has %d layers (expected 1)` |
| `intervention ref grid unreadable` | **no** — the pre-flight equivalent is `cannot verify mask geometry, reference grid missing:` |

Two deliberate differences from the runtime strings, both specified by the plan:

1. The pre-flight lines carry the `intervention mask: ` prefix (with colon) that the existing
   `intervention mask: missing %s (...)` line established, because `.intervention_lines()` and the
   consolidated gap list key on `^intervention `. The runtime `stop()`s have no colon.
2. The pre-flight `compareGeom` line omits the runtime clause
   `; cell-number lookup would be silently wrong`. At pre-flight time no lookup has been attempted.

**Action for 05-14:** add `cannot verify mask geometry` and `intervention mask: unreadable` to the
forbidden-marker list alongside the three 05-09 strings. Neither is currently in
`scripts/verify_intervention_smoke.r`.

## Tasks completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | RED pre-flight tests for fail-open, profile narrowing and geometry (D-23) | `954a1fc` | `tests/testthat/test-spatial-interventions-wiring.R` |
| 2 | Make the D-14 intervention pre-flight fail closed and narrow like the driver | `7d2c9cc` | `src/allocation.r` |

## Tests

New fixture helpers in `tests/testthat/test-spatial-interventions-wiring.R`, kept local to the file
so it stays independent of `test-spatial-interventions.R` (the two are sourced separately by
`test_dir()`): `.wiring_ref_grid()` (the same 4x5 grid), `.write_wiring_ref_grid()`,
`.write_wiring_mask(dir, name, template)`, `.write_wiring_masks()` and
`.write_wiring_multilayer_mask()`.

`.wiring_config()` gained `ref_grid_path` (defaulting to a real written raster) and
`profile_timestep_index`, plus a `simulation_year_steps` override for the two-scenario dedup case.

A second sourcing environment, `.wenv_noengine`, holds `src/utils.r` + `src/allocation.r` **without**
`src/implement_spatial_interventions.R`. Because it is parented by `baseenv()` there is no path back
to the global env, so `exists("resolve_intervention_masks")` is genuinely FALSE inside the call.

New named regression blocks:

- `WR-04: pre-flight reports a missing engine instead of skipping itself` — asserts the config it is
  handed produces exactly one line, matching `resolve_intervention_masks\\(\\) not loaded`. Not a
  single mask is on disk, so the pre-fix silent PASS could only come from the fail-open guard.
- `WR-04: fixture mode still bypasses the engine check entirely` — guards the
  `fixture` contract against the new line leaking into fixture mode.
- `WR-05: profile_timestep_index narrows the checked years` — index 1 of (2028, 2032) with only
  `nat_mask_2028.tif` staged yields zero missing-mask lines.
- `WR-05: profile_timestep_index still demands the year it does pin` — the converse; index 2 with
  only the 2028 mask still reports 2032.
- `WR-05b: an out-of-range profile_timestep_index is reported, not ignored` — index 9 produces
  `profile_timestep_index=9 is outside the valid range` and `1..2`.
- `WR-05b: a non-integer profile_timestep_index is reported`.
- `WR-05c: profile_timestep_index applies after ALLOCATION_YEAR_POST_FILTER` — with the env filter
  pinned to 2032 and index 1, only `nat_mask_2032.tif` may be demanded. Asserts no line mentions
  `nat_mask_2028.tif`, which is what a wrong order of operations would produce.
- `CR-02: pre-flight rejects a mask that is not on the reference grid` — a shifted-extent mask is
  reported and the aligned one is silent.
- `CR-02: pre-flight rejects a multi-layer mask` — a 2-band raster ON the correct grid, which
  `compareGeom()` alone accepts.
- `CR-02: pre-flight reports an unreadable mask file` — a zero-byte `.tif` that the resolver reports
  as `exists = TRUE`.
- `CR-02b: pre-flight reports an unreadable reference grid rather than skipping geometry`.
- `CR-02b: an empty ref_grid_path is reported, not treated as 'no check needed'`.
- `CR-02: each unique mask path is opened at most once per pre-flight` — two scenarios sharing one
  static mask file produce exactly one geometry line, not one per scenario.

**RED baseline (Task 1, against the pre-plan `src/allocation.r`):** 24 failures / 0 errors / 94 pass
in `test-spatial-interventions-wiring.R`.
**After Task 2:** `test-spatial-interventions-wiring.R` 118 pass / 0 fail / 0 error.
`test-allocation-preflight.R` 12 pass / 0 fail.

Two of the new blocks (`WR-04: fixture mode ...` and `WR-05c`) pass both before and after the fix —
they are ordering/contract guards rather than reproductions, and they are counted in the 94/118.

### Full suite

```
[ FAIL 4 | WARN 6 | SKIP 13 | PASS 490 ]
```

Base-commit baseline was `PASS 459 | FAIL 4 | WARN 6 | SKIP 13`, so +31 passing assertions and no
new failure. The four failures are errors, and their identities are unchanged:

| File | Line | Message |
|------|------|---------|
| `tests/testthat/test-prep-paths.R` | 27 | `Error in file(con, "r"): cannot open the connection` |
| `tests/testthat/test-prep-paths.R` | 35 | `Error in file(con, "r"): cannot open the connection` |
| `tests/testthat/test-prep-paths.R` | 43 | `Error in file(con, "r"): cannot open the connection` |
| `tests/testthat/test-prep-paths.R` | 56 | `Error in file(con, "r"): cannot open the connection` |

That file derives `.repo_root` from `sys.frame(1)$ofile`, which is `NULL` under `test_dir()`. It is
a pre-existing harness defect, out of scope for Phase 5, and was not touched.

## Requirements satisfied

- **WR-04** — the D-14 gate fails closed; the missing engine is reported, not skipped.
- **WR-05** — `profile_timestep_index` narrows the pre-flight in the driver's order; invalid and
  out-of-range values are reported.
- **CR-02 (pre-flight half)** — `nlyr` + `compareGeom` + readability asserted per unique mask, plus
  a reported (never skipped) unverifiable reference grid. The runtime half shipped in 05-09.
- **D-23** — every fix ships with a named regression test that failed against the pre-fix code.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Existing pre-flight fixtures had to write real rasters**

- **Found during:** Task 1
- **Issue:** Four pre-existing blocks staged masks with `file.create()`, producing zero-byte `.tif`
  files. The resolver reports those as `exists = TRUE`, so the new geometry pass correctly reported
  them as `intervention mask: unreadable ...` — which would have broken
  `pre-flight emits no intervention line when every mask is present` and the
  `expect_length(lines, 1L)` in two others. The fixture, not the feature, was wrong.
- **Fix:** Added `.write_wiring_mask()` / `.write_wiring_masks()` and replaced the four
  `file.create()` calls. **Every assertion in those blocks is unchanged.** The zero-byte case is now
  covered deliberately, by `CR-02: pre-flight reports an unreadable mask file`.
- **Files modified:** `tests/testthat/test-spatial-interventions-wiring.R`
- **Commit:** `954a1fc`

**2. [Rule 2 - Missing critical functionality] `suppressWarnings()` around the `terra::rast()` calls**

- **Found during:** Task 2
- **Issue:** `terra::rast()` on a present-but-not-a-raster file emits a bare `GDAL error 4` warning
  to stderr *before* raising the R error. testthat surfaces it as a `WARN`, and on HPC it is exactly
  the stderr-only diagnostic class that CR-02b exists to eliminate. Same for
  `compareGeom(stopOnError = FALSE)`.
- **Fix:** Both reads and the `compareGeom()` call are wrapped in
  `suppressWarnings(tryCatch(...))`. The condition is still reported — as a first-class error line
  in the consolidated gap list, which is where an operator will actually see it.
- **Files modified:** `src/allocation.r`
- **Commit:** `7d2c9cc`

**3. [Rule 2 - Missing critical functionality] A rendered label for an unset `ref_grid_path`**

- **Found during:** Task 2
- **Issue:** The plan's line is
  `intervention mask: cannot verify mask geometry, reference grid missing: <path>`. With
  `ref_grid_path` unset or `""` the `%s` renders empty and the operator gets a line ending in a
  dangling colon, which says nothing about which config key to fix.
- **Fix:** A non-scalar/empty value renders as the literal `<config[["ref_grid_path"]] unset>`.
  Covered by `CR-02b: an empty ref_grid_path is reported, not treated as 'no check needed'`.
- **Files modified:** `src/allocation.r`
- **Commit:** `7d2c9cc`

**4. [Rule 2 - Missing critical functionality] Geometry pass gated on at least one existing mask**

- **Found during:** Task 2
- **Issue:** The plan says to emit `cannot verify mask geometry` whenever "interventions are
  configured" and the reference grid is unusable. Taken literally, a deployment with *no* masks
  staged at all would get that line stacked on top of every missing-mask line, and the pre-existing
  `resolver config errors become 'intervention config:' lines without stopping` block (which
  resolves zero masks) would have gained a second line and failed.
- **Fix:** The pass runs when `length(unique(existing_masks)) > 0`. If nothing was resolved there is
  nothing to verify and the missing-mask lines are the actionable gap. The
  `CR-02b` blocks stage real masks, so the reported-not-skipped guarantee is still under test.
- **Files modified:** `src/allocation.r`
- **Commit:** `7d2c9cc`

**5. [Rule 2 - Missing critical functionality] `profile_timestep_index` validated only when years remain**

- **Found during:** Task 2
- **Issue:** Validating the index against an already-empty `years` would emit
  `outside the valid range 1..0` for *any* index whenever the year filter matched nothing — noise
  layered on top of the real cause.
- **Fix:** The block is guarded on `length(years) > 0L`.
- **Files modified:** `src/allocation.r`
- **Commit:** `7d2c9cc`

### Structural note (not a deviation from behaviour)

The review's WR-04 fix nests the resolution body one level deeper inside an `else`. That reflows
~45 lines and buries the real change. Two sibling `if`s keyed on `interventions_configured` and
`engine_loaded` produce identical behaviour with a guard-only diff. The plan's
`grep -c "resolve_intervention_masks() not loaded" src/allocation.r` acceptance check returns 1
either way.

### Architectural changes

None. No new dependency, no new file, no package-manager invocation, no schema change.

## Authentication Gates

None.

## Known Stubs

None. No placeholder, hardcoded-empty or TODO value was introduced.

## Threat Flags

None. No new network endpoint, auth path or trust-boundary schema change. All three `mitigate`
dispositions in the plan's register are implemented with a named regression test each:

| Threat ID | Status |
|-----------|--------|
| T-05-36 (fail-open engine guard) | mitigated — `WR-04: pre-flight reports a missing engine instead of skipping itself` |
| T-05-37 (mis-gridded mask staged on HPC) | mitigated — `CR-02: pre-flight rejects a mask that is not on the reference grid` + the multi-layer and unreadable blocks |
| T-05-38 (over-demanding masks) | mitigated — the four `WR-05*` blocks |
| T-05-39 (geometry pass cost) | accepted as planned — header reads, each unique path once, asserted by `CR-02: each unique mask path is opened at most once per pre-flight` |

## Out of scope / deferred

- **`scripts/verify_intervention_smoke.r` forbidden-marker list** — `cannot verify mask geometry`
  and `intervention mask: unreadable` are new strings and are not in it. 05-14's scope.
- **`scripts/validate_intervention_masks.r` is still wired into nothing.** The pre-flight now makes
  the same assertion on the masks the run will actually read, so the standalone script is redundant
  for the staged set, but it remains a manual gate.
- **`src/implement_spatial_interventions.R` and `tests/testthat/test-spatial-interventions.R` were
  not touched** — they belong to the parallel plan 05-11.
- **The 4 `test-prep-paths.R` errors** were left alone per the execution brief.
- **STATE.md / ROADMAP.md** were not modified; the orchestrator owns those writes after the wave
  merges.

## Self-Check: PASSED

Files claimed:

- `src/allocation.r` — FOUND
- `tests/testthat/test-spatial-interventions-wiring.R` — FOUND
- `.planning/phases/05-integrate-spatial-interventions-branch-and-stage-interventio/05-10-SUMMARY.md` — FOUND

Commits claimed: `954a1fc` (test, Task 1) and `7d2c9cc` (fix, Task 2) — both present in
`git log` on `worktree-agent-a45f61c31c262095a`.

Acceptance greps on `src/allocation.r`: `resolve_intervention_masks() not loaded` = 1,
`cannot verify mask geometry` = 1, `is not on the reference grid` = 1,
`profile_timestep_index` = 14, `resample(\|project(` = 0 (unchanged).
`Rscript -e 'invisible(parse("src/allocation.r"))'` exits 0.
