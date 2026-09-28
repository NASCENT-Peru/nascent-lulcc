---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
plan: 14
subsystem: testing
tags: [r, terra, testthat, smoke-verifier, telemetry, audit-log, regression-gate]

# Dependency graph
requires:
  - phase: 05-09
    provides: the runtime `stop()` strings (`intervention ref grid unreadable`, `is not on the reference grid`, `layers (expected 1)`, the two `cell_index$` contract stops) that the marker list now treats as fatal
  - phase: 05-10
    provides: the seven Stage 7 pre-flight rejection strings, all carrying the `intervention mask: ` / `intervention config: ` prefixes
  - phase: 05-12
    provides: the extended 21-field `AUDIT stage=intervention` line whose `id`, `type`, `to_vals`, `rows_target`, `rows_changed` and `sum_abs_delta` fields the new telemetry section parses
  - phase: 05-13
    provides: the 25-column `intervention_prob_deltas_<scenario>_<region>_<year>.csv` contract, its `region`-slug/`region_label` asymmetry, and the `WARN intervention telemetry:` / `output directory not writable` markers
provides:
  - "A smoke verifier that FAILS on a mask whose zone contains no cells, instead of reporting a vacuous PASS (WR-07)"
  - "`maps_checked` counts only assertions that had cells to assert on; a non-finite max over a non-empty zone is now a failure, not a silent 0"
  - "A forbidden-marker list covering both length-zero crash signatures plus every runtime stop, pre-flight rejection and telemetry degradation line introduced in plans 05-09/05-10/05-13"
  - "Section (e): the telemetry CSV is asserted against the AUDIT lines it claims to describe (D-22)"
  - "`--interventions-dir`, so the verifier can be pointed at a fixture config without touching `config/`"
  - "`tests/testthat/test-verify-intervention-smoke.R`: an end-to-end fixture that drives the real script as a subprocess"
affects: [05-16, operator HPC re-run, any future scenario or region smoke run]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Subprocess-driven script testing: the test builds a synthetic run-output tree and invokes the shipped script via system2(), so exit codes and stdout/stderr are asserted as the contract they are"
    - "Non-vacuity as a first-class assertion: prove the assertion had something to assert on before recording it as checked"

key-files:
  created:
    - tests/testthat/test-verify-intervention-smoke.R
  modified:
    - scripts/verify_intervention_smoke.r

key-decisions:
  - "`sum(n_target)` vs AUDIT `rows_target` is asserted for Absolute interventions ONLY - relative_prob_adjust() increments rows_target before its three skip paths, so equality would false-alarm on every Relative entry (05-13)"
  - "`sum(n_changed)` vs `rows_changed` and `sum(sum_abs_delta)` vs `sum_abs_delta` are asserted unconditionally, because those two reconcile for all intervention types"
  - "The CSV `region` slug and the AUDIT `region=` label are deliberately NOT compared - they differ by design"
  - "`telemetry_rows=%d` is appended AFTER `maps_checked=%d` so the shipped PASS banner prefix stays byte-identical"
  - "The forbidden-marker list was extended beyond the plan's two literals to the full 05-09/05-10/05-13 marker set, using the prefix forms `intervention mask: ` and `intervention config: ` so one entry covers a whole family"
  - "A non-finite max over a provably non-empty zone is a fail(), not `mx <- 0`"

patterns-established:
  - "Zone assertions report `n_zone=%d` alongside the value asserted, so the per-map line itself proves the check was not vacuous"
  - "Marker families are matched by their error-only prefix rather than one literal per message, so future pre-flight strings in the same family are covered on arrival"

requirements-completed: [WR-07, CR-03, D-22, D-23]

# Metrics
duration: 40min
completed: 2026-09-28
---

# Phase 5 Plan 14: Smoke-verifier gap closure Summary

**`scripts/verify_intervention_smoke.r` can no longer say PASS about nothing: an empty zone is a hard FAIL that does not count toward `maps_checked`, both length-zero crash signatures (plus every 05-09/05-10/05-13 intervention marker) are fatal, and the 05-13 telemetry CSV is now reconciled against the AUDIT lines it claims to describe.**

## Performance

- **Duration:** ~40 min
- **Tasks:** 3 of 3
- **Files modified:** 2 (1 created, 1 modified)
- **Commits:** 3 (+ this docs commit)

## The new PASS banner (verbatim, for the 05-16 operator plan)

Captured from a real fixture run of the post-plan script:

```
PASS verify_intervention_smoke scenario=BAU region=r1 year=2028 interventions=1 maps_checked=2 telemetry_rows=2
```

The shipped prefix through `maps_checked=%d` is unchanged; `telemetry_rows=%d` is appended.
The exit-code contract is unchanged: **0 PASS, 1 FAIL, 2 usage or sourcing error**.

Two other output lines are new and worth grepping for on the HPC re-run:

```
  iv_abs zone=Inside 001_id_trans_11.tif: n_zone=3 max prob in zone = 0 [ok]
telemetry: 2 rows, 1 interventions, 2 classes [ok]
```

and the header block now echoes the resolved policy directory:

```
region_dir: <...>
mask_dir:   <...>
interventions_dir: <...>
```

## Task Commits

1. **Task 1: End-to-end fixture test that a vacuous zone assertion fails (D-23)** - `fcfef3c` (test)
2. **Task 2: Non-vacuous zone assertions, extended markers and `--interventions-dir`** - `ca0b62d` (fix)
3. **Task 3: Assert the telemetry CSV against the AUDIT lines (D-22)** - `86ffe19` (feat)

## Accomplishments

### WR-07 - the vacuous PASS is closed

Reproduced first, against the **unmodified** shipped script, with a mask that is NA on every cell
(so the `Inside` zone selects nothing):

```
  iv_abs zone=Inside 001_id_trans_11.tif: max prob in zone = 0 [ok]
  iv_abs zone=Inside 002_id_trans_12.tif: max prob in zone = 0 [ok]

PASS verify_intervention_smoke scenario=ZZZSMOKE region=r1 year=2028 interventions=1 maps_checked=2
```

Exit 0, two checks reported, zero cells asserted on. The fix computes

```r
n_zone <- terra::global(sel & !is.na(r), "sum", na.rm = TRUE)[[1]][1]
```

**before** the max. A non-finite or zero `n_zone` calls `fail()` with
`"%s: %s has 0 non-NA cells in zone=%s for mask %s (assertion would be vacuous)"` and `next`s
without touching `maps_checked`. The old `if (!is.finite(mx)) mx <- 0` line is gone: on a
provably non-empty zone a non-finite max is now its own `fail()`, never a silent pass.

### CR-03 (verifier half) - the marker list

`argument is of length zero` and `subscript out of bounds` are now fatal markers. Beyond the
plan's two literals, the list was extended to cover the marker families the earlier gap-closure
plans introduced but never wired into the verifier (see Deviations, Rule 2):

| Group | Literals added |
|-------|----------------|
| Generic R crash signatures | `argument is of length zero`, `subscript out of bounds` |
| 05-09 runtime stops | `intervention ref grid unreadable`, `is not on the reference grid`, `layers (expected 1)`, `cell_index$cell_id must be`, `cell_index$ref_cell_id values outside` |
| 05-10 Stage 7 pre-flight | `intervention mask: `, `intervention config: ` |
| 05-13 telemetry | `WARN intervention telemetry:`, `intervention telemetry: output directory not writable` |

The two geometry literals are deliberately prefix-free so one entry covers both the runtime
wording (`intervention mask %s is not on the reference grid (...); cell-number lookup would be
silently wrong`) and the pre-flight wording (`intervention mask: %s is not on the reference grid
(crs/res/extent mismatch)`), which differ only in prefix and trailing clause.

`intervention telemetry: ` is **not** used as a prefix, because
`intervention telemetry: wrote %d rows to %s` is the success line. Only the `WARN ` form and the
pre-flight form are forbidden.

The mlr3 benign whitelist (`.__Task__col_info`, `falling back to direct model prediction`) is
unchanged.

### D-22 - section (e), telemetry CSV

New section between (c) and the forbidden-marker scan, reusing `region_dir`, `active_ids`,
`iv_lines` and `abs0` rather than re-parsing anything. It asserts:

| Assertion | Scope |
|-----------|-------|
| File exists at `<region_dir>/intervention_prob_deltas_<scenario>_<region>_<year>.csv` | when `n_active > 0` |
| `names()` identical to the 25-column contract, in order | always |
| CSV `intervention_id` set equals `active_ids` | always |
| CSV `target_class` set equals the AUDIT `to_vals` set, no duplicate `(id, class)` pair | per intervention |
| `sum(n_changed)` equals AUDIT `rows_changed` | per intervention, **unconditional** |
| `sum(sum_abs_delta)` equals AUDIT `sum_abs_delta` (rel. tol 1e-5, matching `.fmt_num()`'s 6 significant digits) | per intervention, **unconditional** |
| `sum(n_target)` equals AUDIT `rows_target` | **Absolute interventions only** |
| `mean_after` is 0 within 1e-12 on every row with `n_target > 0` | every `abs0` intervention |

When `n_active == 0` the section prints an informational line and asserts nothing.

**The `rows_target` scoping is load-bearing.** `relative_prob_adjust()` does
`rows_target <- rows_target + length(sub_idx)` *before* its three `next` paths, so a skipped
target class still counts toward the AUDIT `rows_target` while its CSV row correctly reports
`n_target = 0` (05-13 observed `iv_rel` with `rows_target=6` / `n_target=0`). An unscoped equality
would fail every Relative intervention. `sum_abs_delta` and `rows_changed` reconcile for all
types, so they carry the unconditional check.

The CSV `region` column (slug, `costa_peruana`) and the AUDIT `region=` field (verbatim label,
`Costa Peruana`) are **not** compared — they differ by design.

### D-23 - the fixture test

`tests/testthat/test-verify-intervention-smoke.R` (371 lines) builds a full synthetic run output
tree in a `withr::local_tempdir()` — `posterior.tif`, `trans_rates.csv` (two target rows),
`probability_map_dir/{001_id_trans_11,002_id_trans_12}.tif`, a `worker_1.log` carrying one
extended 21-field `AUDIT stage=intervention` line and one `intervention_summary` line, a mask, a
`BAU_interventions.yml` with one Absolute-to-0 Inside entry, and the 25-column telemetry CSV —
then drives the **real script** through `system2()` and asserts the exit status and combined
output.

Nine blocks: the static marker-list check (runs even if the subprocess blocks skip), WR-07,
WR-07b, the two CR-03 markers, D-22 / D-22b / D-22c and a happy path that pins
`PASS verify_intervention_smoke`, `maps_checked=2` and `n_zone=3`.

The repo root is resolved with `testthat::test_path("..", "..")` plus an explicit
`file.exists()` probe — no null-coalescing operator at file-source time (IN-06).
`.skip_if_config_unavailable()` skips with
`"verify_intervention_smoke could not load the project config in this environment"` **only** when
exit 2 came from a config/sourcing failure; a usage or unknown-flag rejection is a real result and
is allowed to fail the expectations (this is what made the RED phase meaningful).

## Verification

| Check | Result |
|-------|--------|
| RED baseline for the new test file (pre-Task-2 script) | **23 failed / 2 passed / 0 skipped** |
| After Task 2 | 6 failed / 19 passed — only the three D-22 blocks |
| After Task 3 | **0 failed / 25 passed / 0 skipped** |
| `Rscript -e 'invisible(parse("scripts/verify_intervention_smoke.r"))'` | exit 0 |
| `Rscript -e 'invisible(parse("tests/testthat/test-verify-intervention-smoke.R"))'` | exit 0 |
| `Rscript scripts/verify_intervention_smoke.r --scenario NAT --region costa_peruana` | exit **2**, documented `--year` usage message, usage string now lists `--interventions-dir` |
| Full suite `testthat::test_dir("tests/testthat")` | **PASS 648 / FAIL 0 / ERROR 4 / WARN 4 / SKIP 13** |
| Error identities | exactly `test-prep-paths.R` lines **27, 35, 43, 56** — the documented pre-existing `.repo_root` harness defect, untouched |
| Baseline comparison | base `334ae6a` was PASS 623 / FAIL 4 / SKIP 13; +25 passes are this plan's new block, the same 4 pre-existing errors remain, nothing else regressed |

Acceptance greps on `scripts/verify_intervention_smoke.r`:

| grep -c | Required | Actual |
|---------|----------|--------|
| `n_zone` | >= 4 | 5 |
| `assertion would be vacuous` | 1 | 1 |
| `argument is of length zero` | 1 | 1 |
| `subscript out of bounds` | 1 | 1 |
| `interventions-dir` | >= 3 | 6 |
| `.__Task__col_info` | 1 | 1 |
| `falling back to direct model prediction` | 1 | 1 |
| `intervention_prob_deltas_%s_%s_%d.csv` | 1 | 1 |
| `telemetry_rows=` | >= 1 | 1 |
| `PASS verify_intervention_smoke scenario=%s region=%s year=%d interventions=%d maps_checked=%d` | 1 | 1 |

On `tests/testthat/test-verify-intervention-smoke.R`: `%||%` = 0, `skip(` = 1, 371 lines.

Non-regression on the phase's standing invariants:
`grep -cE 'resample\(|project\(' src/implement_spatial_interventions.R` = 0 (D-13),
`grep -c 'copy(' src/implement_spatial_interventions.R` = 0 (D-20), and neither is introduced in
the verifier.

## Files Created/Modified

- `tests/testthat/test-verify-intervention-smoke.R` *(created)* — nine-block end-to-end fixture
  that drives the shipped verifier as a subprocess; owns `.smoke_fixture()`, `.run_verifier()` and
  the 25-column contract vector.
- `scripts/verify_intervention_smoke.r` *(modified)* — `n_zone` guard, extended forbidden-marker
  list, section (e), `--interventions-dir`, `telemetry_rows` in the PASS banner, header docstring
  paragraphs (c) and (e).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] The forbidden-marker list was extended beyond the plan's two literals**

- **Found during:** Task 2
- **Issue:** The plan's action item named only `argument is of length zero` and
  `subscript out of bounds`. But plan 05-09's summary explicitly hands 05-14 three more strings
  (`is not on the reference grid`, `layers (expected 1)`, `intervention ref grid unreadable`), and
  plans 05-10 and 05-13 introduced eight pre-flight strings and two telemetry-degradation strings
  that no verifier check would have noticed. A verifier that PASSes a run whose log says
  `WARN intervention telemetry: failed to write ...` or
  `intervention mask: ... is not on the reference grid` is exactly the fail-open the phase is
  eliminating.
- **Fix:** Added the full family, using the error-only prefixes `intervention mask: ` and
  `intervention config: ` so one entry covers each group. Confirmed against
  `src/allocation.r` and `src/implement_spatial_interventions.R` that neither prefix ever appears
  on a success line, and deliberately did **not** use `intervention telemetry: ` as a prefix
  because that *is* a success line.
- **Files modified:** `scripts/verify_intervention_smoke.r`
- **Verification:** the two CR-03 blocks pass; the happy-path fixture (whose log contains
  `Applying intervention: iv_abs`) still exits 0, proving no false positive.
- **Committed in:** `ca0b62d`

**2. [Rule 1 - Bug] A non-finite max over a non-empty zone was silently converted to a pass**

- **Found during:** Task 2
- **Issue:** `if (!is.finite(mx)) mx <- 0` turned an all-NA `terra::global()` result into `[ok]`.
  The plan allowed keeping it "only for the now-provably-non-empty case", but on a *non-empty*
  zone a non-finite max means the product yielded nothing usable — which is a defect, not a pass.
- **Fix:** replaced with an explicit `fail()` naming `n_zone`, followed by `next`, so it is
  neither a pass nor counted in `maps_checked`.
- **Files modified:** `scripts/verify_intervention_smoke.r`
- **Verification:** parse clean; happy path and vacuous path both behave as asserted.
- **Committed in:** `ca0b62d`

**3. [Rule 3 - Blocking] `iv_lines` was scoped inside section (b)'s `else` branch**

- **Found during:** Task 3
- **Issue:** section (e) must consume `iv_lines`, but it only exists when a summary line was
  found; on the "no summary line" path section (e) would have raised
  `object 'iv_lines' not found` and crashed the verifier instead of reporting.
- **Fix:** `iv_lines <- character(0)` declared immediately before the branch.
- **Files modified:** `scripts/verify_intervention_smoke.r`
- **Verification:** full suite clean; section (e) degrades to "no rows for AUDIT intervention id"
  reporting rather than an R error.
- **Committed in:** `86ffe19`

### Acceptance-criterion adjustments

**`grep -c ".__Task__col_info"` was 2 at the plan's base commit, not 1.** The literal appeared
both in the `benign` vector and in the explanatory comment two lines above it. Rather than leave
the criterion unsatisfiable, the comment was reworded to "its task column-info attribute is
missing"; the whitelist entry itself is untouched and the count is now 1, which is what the
criterion meant by "whitelist intact". Same treatment for the two CR-03 literals: the new
explanatory comment describes them ("the length-zero message below", "the out-of-bounds one")
instead of repeating them, so each grep counts exactly the one live vector entry.

---

**Total deviations:** 3 auto-fixed (1 missing critical, 1 bug, 1 blocking) + 1 documented
acceptance-criterion adjustment.
**Impact on plan:** No scope creep. All three auto-fixes are inside the two files the plan owns
and serve the plan's own stated purpose — a verifier that cannot report PASS about nothing.

## Issues Encountered

- **The RED phase needed a skip predicate that does not swallow the defect.** The plan's
  `.run_verifier()` spec says to skip on exit status 2. But at RED the script rejects
  `--interventions-dir` as an unknown flag, which *is* exit 2 — a blanket skip would have made the
  entire RED phase vacuous, the same defect class the plan exists to close.
  `.skip_if_config_unavailable()` therefore skips only when the exit-2 output looks like a
  config/sourcing failure and explicitly not when it looks like a usage rejection. This produced a
  real 23-failure RED baseline.
- **Demonstrating the vacuous PASS against the unmodified script** required a config the
  flag-less script could see. A throwaway `config/ZZZSMOKE_interventions.yml` was staged, the run
  captured, and the file deleted immediately (`git status` confirmed clean before the Task 1
  commit). Task 1 left `scripts/` untouched, as required.
- **`WR-11` in `05-REVIEW.md` is not a factual record of the shipped config** and was not used as
  a source; all class names in the fixture were taken from `config/lulc_schema.json`.

## Threat Flags

None. `--interventions-dir` is already registered in the plan's threat model as **T-05-54
(Spoofing, disposition: accept)**; the accepted mitigation — echoing the resolved value in the
verifier's output — is implemented (`interventions_dir: %s` in the header block). T-05-51,
T-05-52 and T-05-53 are all mitigated as specified. No dependency was added; no package-manager
command appears anywhere in this plan (T-05-SC).

## Known Stubs

None.

## Next Phase Readiness

- The verifier is ready for the 05-16 HPC re-run. The operator should expect the banner recorded
  above and should treat `telemetry_rows=0` on a run with active interventions as a defect worth
  chasing, even though the banner alone would be a PASS only if section (e) found nothing to
  assert (`n_active == 0`).
- `--interventions-dir` means the verifier no longer has to be run from a checkout whose
  `config/` holds the scenario under test.
- Plan 05-15 owns `tests/testthat/helper-spatial-interventions.R`,
  `test-spatial-interventions.R` and `test-spatial-interventions-wiring.R`; none of the three was
  touched here, and the new test file is self-contained (it sources no `src/` file and defines all
  its helpers under a `.smoke_` prefix), so the two branches should merge without collision.

## Self-Check: PASSED

- `scripts/verify_intervention_smoke.r` — FOUND
- `tests/testthat/test-verify-intervention-smoke.R` — FOUND
- `.planning/phases/05-integrate-spatial-interventions-branch-and-stage-interventio/05-14-SUMMARY.md` — FOUND
- `fcfef3c`, `ca0b62d`, `86ffe19` — all three present in `git log`, on
  `worktree-agent-aecfad4b1c1b2b6be` at base `334ae6a`
- No modification to `STATE.md`, `ROADMAP.md`, `.planning/HANDOFF.json`, or plan 05-15's three
  test files

---
*Phase: 05-integrate-spatial-interventions-branch-and-stage-interventio*
*Completed: 2026-09-28*
