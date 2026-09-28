---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
plan: 15
subsystem: spatial-interventions
tags: [tests, coverage, valency, zone, vacuous-assertion, test-helper, gap-closure]
requires:
  - "post-05-11 adjuster semantics (`>=` percentile alignment, `Perc_diff >= 0` dispatch, per-index-set clamps)"
  - "per-target-class `res$stats` and the 21-field AUDIT line (Phase 5 Plan 12)"
  - "`ref_grid_path` (Phase 5 Plan 09) and `telemetry_dir` (Phase 5 Plan 13) formals on `implement_spatial_interventions()`"
provides:
  - "`tests/testthat/helper-spatial-interventions.R`: the single `.repo_root` definition for the spatial-interventions test files, resolved without base R's null-default operator and without the sourcing frame's `ofile` entry"
  - "`WR-08` / `WR-08b` / `WR-08c`: coverage for Relative `Increase_inside_decrease_outside` (Inside), Relative `Decrease` with `zone: Outside`, and the guard that rejects the two-zone valency outside `Inside`"
  - "`WR-09`: a zero-preservation block whose Relative pass demonstrably executes"
  - "`.audit_body()` / `.audit_nfields()` / `.audit_field()` relocated next to `.audit_lines()`, so any block in the file can read AUDIT fields"
affects:
  - "the shipped-config valency/zone coverage table in tests/testthat/test-spatial-interventions.R is now the canonical in-repo enumeration; a new combination in config/*.yml should add a row and a block"
  - "any future test file may consume `.repo_root` from the helper instead of re-deriving it (testthat loads helper-*.R under both test_file() and test_dir())"
tech-stack:
  added: []
  patterns:
    - "marker-file-confirmed repo-root resolution: every candidate path is validated by `file.exists(<candidate>/src/implement_spatial_interventions.R)` before it is returned, so a wrong root is never returned silently"
    - "non-vacuity assertions: a behaviour test asserts the absence of the engine's own `intervention skip:` line and a non-zero `rows_changed` on the AUDIT line of the specific intervention under test"
    - "direction-discriminating fixtures: the WR-08b fixture is chosen so the opposite zone setting would move the same rows the OTHER way, making the assertion pin the swap rather than merely observe movement"
key-files:
  created:
    - tests/testthat/helper-spatial-interventions.R
    - .planning/phases/05-integrate-spatial-interventions-branch-and-stage-interventio/05-15-SUMMARY.md
  modified:
    - tests/testthat/test-spatial-interventions.R
    - tests/testthat/test-spatial-interventions-wiring.R
key-decisions:
  - "The helper does NOT read the sourcing frame's `ofile` entry at all. The plan's task text suggested keeping that path behind an explicit `if (is.null(x)) \".\" else x`; the orchestrator's success criteria forbid it outright, and the criteria are right — an `ofile`-derived root is exactly the defect that makes tests/testthat/test-prep-paths.R error four times in every full-suite run. The helper uses `testthat::test_path(\"..\", \"..\")` first and a `getwd()` upward walk as the fallback, and confirms both against a marker file"
  - "The literal spellings of the two removed tokens do not appear anywhere in the three files, not even in comments. The acceptance criterion is a raw `grep -c`, and a comment that quotes the forbidden token trips a grep guard exactly as loudly as code would. The header explains the operator and the frame entry descriptively instead"
  - "DESCRIPTION was NOT modified. It already declares `Depends: R (>= 4.2)` — there was no undeclared floor, there was a floor the removed bootstrap violated (base R's null-default operator arrived in 4.4.0). Removing the bootstrap restores agreement; adding `R (>= 4.4)` would have resolved the disagreement in the wrong direction by raising the project's requirement to match a test-file accident"
  - "The review's suggested WR-09 fixture (rank-2 Relative Increase with `zone = \"Outside\"`) does not work. With rank 1 zeroing the whole inside zone, the inside zone becomes the NON-intervention zone under `zone: Outside` and the NaN guard fires on `!any(Non_Intervention_vals > 0)` — the same skip, in the mirror image. The guard needs positives in BOTH zones, so the zeroed set must be a strict SUBSET of one zone; `From_lulc_filter: forested_areas` on rank 1 achieves that because inside cell 5 carries from_val 102"
  - "`expect_length(a$iv, 2L)` and `expect_false(identical(out$prob, before$prob))` were kept but are documented in-file as necessary-not-sufficient: the engine writes an AUDIT line even for an adjuster that skipped every target class, and rank 1 alone changes the table. The discriminating assertions are `rows_changed` / `n_inc` / `sum_abs_delta` read off the RANK-2 line plus the absence of `intervention skip:`"
  - "`.audit_body()` / `.audit_nfields()` / `.audit_field()` moved up beside `.audit_lines()`. testthat executes test bodies at source time in file order, so the WR-09 block (line ~600) could not call a helper defined in the plan 05-12 section (line ~980). Relocating the three definitions is less invasive than reordering test blocks and makes the utilities available file-wide"
  - "WR-08c uses `expect_error(..., fixed = TRUE)` on `must be 'Inside'` rather than the full sentence, so the block survives rewording of the surrounding prose but not removal of the guard"
requirements-completed: [WR-08, WR-09, IN-06, D-23]
duration: ~35 min
completed: 2026-09-28
---

# Phase 5 Plan 15: Valency/zone coverage, a non-vacuous zero-preservation test, and a shared repo-root helper Summary

**Every valency and zone combination the four shipped production configs use is now exercised by a
test that provably executes the branch it names, and the zero-preservation block that used to pass
because its intervention was skipped now fails if that intervention is skipped.**

## Performance

- **Duration:** ~35 min
- **Tasks:** 3 of 3
- **Files modified:** 3 (1 created, 2 modified)
- **Commits:** 3 (+ this docs commit)

## Suite counts — the new baseline for the operator plan

| Run | PASS | FAIL | SKIP | ERROR |
|-----|------|------|------|-------|
| Base `334ae6a` (before this plan) | 623 | 0 | 13 | 4 |
| After this plan | **659** | **0** | **13** | **4** |

+36 passing expectations, no new failures, no weakened assertions.

The four errors are unchanged and pre-existing, in `tests/testthat/test-prep-paths.R` at lines
**27, 35, 43, 56** — `Error in file(con, "r"): cannot open the connection`, because that file
derives `.repo_root` from the sourcing frame's `ofile` entry, which is NULL under `test_dir()`. Out
of scope for this plan; see "Deferred" below.

Both spatial-interventions files also pass standalone:

| File | `test_file()` before | `test_file()` after |
|------|----------------------|---------------------|
| `test-spatial-interventions.R` | 226 pass / 0 fail / 0 error | 262 pass / 0 fail / 0 error |
| `test-spatial-interventions-wiring.R` | 129 pass / 0 fail / 0 error | 129 pass / 0 fail / 0 error |

## The shipped valency/zone enumeration (source: `config/*.yml`)

Derived by reading the four shipped YAMLs directly and cross-checking
`docs/spatial_interventions/parameter_provenance.md` — **not** from `05-REVIEW.md`, whose WR-11 text
misattributes the `Prob_adjust_zone: Outside` + `Prob_adjust_value: 0` pairing to CUL's
`Mining_outside_restraint` (it belongs to NAT's `Mining_freeze_post_2030` and SOC's
`Mining_in_low_ES_areas`; `Mining_outside_restraint` is Relative/Decrease and carries no
`Prob_adjust_value` key at all). The same table is embedded as a comment in
`tests/testthat/test-spatial-interventions.R` so it stays next to the tests it indexes.

14 shipped Allocation entries, 6 distinct combinations:

| # | type | valency | zone | shipped entries | covered by |
|---|------|---------|------|-----------------|------------|
| 1 | Absolute | n/a (`value: 0`) | Inside | NAT/CUL/SOC `Conservation_expansion_and_preservation` | `engine: Absolute=0 Inside with From filter edits only inside from-class rows` (pre-existing) |
| 2 | Absolute | n/a (`value: 0`) | Outside | NAT `Mining_freeze_post_2030`, SOC `Mining_in_low_ES_areas` | `engine: Absolute=0 Outside only changes outside rows` (pre-existing) |
| 3 | Relative | `Decrease` | Inside | BAU `ineffective_conservation`, NAT `Indigenous_land_OECM_forest`, NAT `Indigenous_land_OECM_ag`, CUL `Indigenous_land_OECM_forest`, CUL `Indigenous_land_OECM_ag` | `engine: Relative Decrease lowers inside values and never raises any` (pre-existing) |
| 4 | Relative | `Increase` | Inside | CUL `Urban_densification` | `WR-01c: an exactly zero percentage difference applies the threshold (Increase)` (pre-existing) + the rewritten `WR-09` block |
| 5 | Relative | `Decrease` | **Outside** | CUL `Mining_outside_restraint` | **`WR-08b` (new)** — was uncovered |
| 6 | Relative | **`Increase_inside_decrease_outside`** | Inside | NAT `Urban_densification`, SOC `Urban_densification` | **`WR-08` (new)** — was uncovered |

Plus the rejection path that keeps the table honest: `Increase_inside_decrease_outside` with
`zone: Outside` is not in any shipped config **because the engine refuses it**, and `WR-08c` now
pins that refusal.

## What each new block proves, and how it would fail

### `WR-08` — `Increase_inside_decrease_outside` moves both zones in one pass

Fixture `.norm()` over target class 105: inside cells 1/3/5 hold 0.2/0.4/0.6, outside cells 2/4/6
hold 0.3/0.5/0.1. Hand-computed against the post-05-11 semantics:

```
Intervention_ptile_val  = quantile(c(.2,.4,.6), .5) = 0.4
Intervention_ptile_mean = mean(.4, .6)              = 0.50
Non_int_ptile_val       = quantile(c(.3,.5,.1), .5) = 0.3
Non_int_ptile_mean      = mean(.3, .5)              = 0.40
Perc_diff = (0.50 - 0.40) / 0.45 * 100 = 22.222   (> threshold 5, so no nudge)
```

Inside rows at/above 0.4 rise (0.4 -> 0.4889, 0.6 -> 0.7333); outside rows at/above 0.3 fall
(0.3 -> 0.2333, 0.5 -> 0.3889). Cells 1 and 6 sit below their percentiles and are asserted
unchanged; the `to_val = 104` rows are asserted unchanged (the WR-02 scope rule).

The AUDIT line the engine actually emitted confirms the arithmetic:

```
... id=iv_rel rank=1 type=Relative zone=Inside to_vals=105 mask=mask_a.tif
rows_target=6 rows_changed=4 ... n_inc=2 n_dec=2 sum_abs_delta=0.4
```

`n_inc=2` **and** `n_dec=2` on a single line is the signature of this valency and of no other — it
is the only branch that writes two index sets in one pass. The block also asserts the log contains
no `intervention skip:` line and does contain the branch's own explanation sentence.

### `WR-08b` — `zone: Outside` swaps the intervention and non-intervention index sets

`.norm_split(inside = 0.2, outside = 0.5)` with `Decrease` / `Outside`:

```
Intervention (OUTSIDE) ptile mean = 0.50
Non-intervention (INSIDE) ptile mean = 0.20
Perc_diff = (0.50 - 0.20) / 0.35 * 100 = 85.71  -> Decrease, Perc_diff >= 0
-> decrease the INTERVENTION rows, i.e. the outside ones: 0.5 -> 0.0714
```

The fixture is deliberately chosen so the assertion is **direction-discriminating**: with
`zone = "Inside"` and this same table, `Perc_diff` would be −85.71, the `Perc_diff < 0` arm would
run, and the outside rows would be *raised* instead. Asserting that the outside rows FELL therefore
pins the swap, not merely "something moved". Inside rows and `to_val = 104` rows are asserted
byte-identical. AUDIT: `zone=Outside rows_target=6 rows_changed=3 ... n_inc=0 n_dec=3`.

### `WR-08c` — the guard

`Increase_inside_decrease_outside` + `zone: Outside` must error with `must be 'Inside'`. The guard
is the first statement in `relative_prob_adjust()`, before any write, so the block also asserts the
probability table is byte-identical to a pre-call `copy()` and that no
`AUDIT stage=intervention region=` line was written.

### `WR-09` — zero preservation, by a pass that ran

**Why the old block passed for the wrong reason.** Its rank-1 entry was an unfiltered
`.abs_entry()`, which zeroes *every* inside row of target class 105. The rank-2 Relative `Increase`
then found no positive probabilities in its intervention zone, the NaN guard fired, and the adjuster
returned via `next` without touching anything. The block proved "a skipped intervention changes
nothing" — the zeros were never at risk.

**Why the review's proposed fix does not work either.** The review suggested giving rank 2
`zone = "Outside"` so it would have positives to work with. It would: but the zeroed inside rows
then become the *non-intervention* zone, and the NaN guard's second clause
(`!any(Non_Intervention_vals > 0)`) fires instead. Same skip, mirror image. The guard requires
positives in **both** zones.

**The fix used.** Zero only part of one zone. `from = list("forested_areas")` restricts rank 1 to
`from_val 101`, and inside cell 5 carries `from_val 102`, so after rank 1 the inside rows are
`0, 0, 0.6` while the outside rows keep `0.3, 0.5, 0.1`. Rank 2 (Relative `Increase`, `Inside`) then
computes `Perc_diff = (0.60 − 0.40) / 0.50 × 100 = 40`, selects cell 5 only, and raises it to 0.84.
The zeros at cells 1 and 3 survive — structurally, because the intervention percentile is taken over
`Intervention_vals[Intervention_vals > 0]` and is therefore always strictly positive, so a zeroed
row can never enter the `>=` selection.

**The counter-check (run by hand, as the plan required).** Reverting rank 1 to the unfiltered
`.abs_entry()` and re-running produced **5 failures** in the block:

| Assertion | Value with the broken fixture |
|-----------|-------------------------------|
| `rows_changed` on the rank-2 AUDIT line == "1" | `"0"` |
| `n_inc` on the rank-2 AUDIT line == "1" | `"0"` |
| `sum_abs_delta` on the rank-2 AUDIT line > 0 | `0` |
| cell 5 prob == 0.84 | `0.00` |
| no `intervention skip:` in the log | `intervention skip:` present |

Critically, `expect_length(a$iv, 2L)` and `expect_false(identical(out$prob, before$prob))` **still
passed** under the broken fixture. Both were kept (the plan and review asked for them) but are
documented in-file as necessary-not-sufficient: the engine writes an AUDIT line even for an adjuster
that skipped every target class, and rank 1 alone changes the table. Anyone strengthening this block
in future should add to the five discriminating assertions, not to those two.

The fixture was restored immediately after the counter-check; the committed state is the passing one
(verified by the post-Task-3 full-suite run above).

## IN-06: the removed bootstrap, and why both halves of it were wrong

Both test files carried a verbatim copy of a `.repo_root` bootstrap that normalised the sourcing
frame's `ofile` entry, null-coalesced it against `"."` with base R's null-default operator, and took
`dirname()` three times. Two independent defects:

1. **The operator.** It entered base R in 4.4.0. `DESCRIPTION` declares `Depends: R (>= 4.2)`. The
   expression ran while the test file was being *sourced*, i.e. **before** `src/utils.r` was loaded,
   so the project's own fallback definition could not have covered it. The files silently required a
   newer R than the package claims — and the engine under test deliberately avoids the operator for
   exactly this reason.
2. **The frame entry.** It is NULL under `testthat::test_dir()`. This is not hypothetical: it is
   precisely why `test-prep-paths.R` errors four times in every full-suite run.

`tests/testthat/helper-spatial-interventions.R` (77 lines) replaces both with:

```r
.repo_root_marker <- file.path("src", "implement_spatial_interventions.R")
# 1. normalizePath(testthat::test_path("..", ".."), mustWork = FALSE)
# 2. fallback: walk up from getwd() until the marker appears
# every candidate confirmed by file.exists(<candidate>/<marker>)
```

testthat sets the working directory to `tests/testthat` for the duration of both `test_file()` and
`test_dir()`, helpers included, so `test_path("..", "..")` is the repo root under either invocation —
verified by running both styles (counts above). Each test file keeps a guarded
`if (!exists(".repo_root", inherits = TRUE)) source(testthat::test_path("helper-..."))` so a bare
`source()` of the file from a console still works without reintroducing the bootstrap.

### `DESCRIPTION`: the R version floor

**Not modified, deliberately.** The plan offered "add `Depends: R (>= 4.1)` or record why not".
`DESCRIPTION` already declares `Depends: R (>= 4.2)` — there was never an undeclared floor. There was
a declared floor that the removed bootstrap violated. Removing the bootstrap restores agreement.
Raising the declaration to `R (>= 4.4)` would have resolved the review's disagreement in the wrong
direction, letting a test-file accident dictate the package's minimum R.

### Grep guards

| Check | Result |
|-------|--------|
| `grep -c '%\|\|%'` across the two test files and the helper | `0, 0, 0` |
| `grep -c 'sys.frame(1)$ofile'` across the same three | `0, 0, 0` |
| `grep -c "Increase_inside_decrease_outside" test-spatial-interventions.R` | `7` (>= 3) |
| `grep -c 'zone = "Outside"' test-spatial-interventions.R` | `3` (>= 2) |
| `grep -c "WR-09" test-spatial-interventions.R` | `3` (>= 1) |
| `grep -c "zeros from Absolute=0 stay zero after a Relative Increase"` | `0` (misleading name gone) |
| `Rscript -e 'invisible(parse("tests/testthat/test-spatial-interventions.R"))'` | exit 0 |

Neither forbidden token appears even in a comment — a raw `grep -c` guard cannot tell prose from
code, so the helper's header describes the operator and the frame entry rather than spelling them.

## Deviations from Plan

### 1. [Rule 3 — Blocking] `.audit_field()` was defined after the block that needed it

- **Found during:** Task 3 (first run of the rewritten `WR-09` block).
- **Issue:** `Error in .audit_field(...): could not find function ".audit_field"`. testthat executes
  `test_that()` bodies at source time in file order; `WR-09` sits at line ~600 and
  `.audit_field()` was defined in the plan 05-12 section at line ~980.
- **Fix:** moved `.audit_body()`, `.audit_nfields()` and `.audit_field()` up beside
  `.audit_lines()`, leaving a pointer comment at the old site. No behaviour change — these are pure
  string-extraction helpers and every existing caller is unaffected (verified: the plan 05-12 and
  05-13 blocks that use them all still pass).
- **Files modified:** `tests/testthat/test-spatial-interventions.R`
- **Commit:** `516ae98`

### 2. The helper does not read the sourcing frame's `ofile` entry at all

- **Found during:** Task 1 (plan text vs orchestrator success criteria).
- **Issue:** the plan's action text said to keep the frame-`ofile` path behind an explicit
  `if (is.null(x) || identical(x, "")) "." else x`; the orchestrator's success criteria say the
  helper must NOT resolve the repo root via that frame entry.
- **Fix:** followed the success criteria. The frame entry is the documented cause of the four
  standing `test-prep-paths.R` errors; keeping it behind a guard would have preserved the defect and
  only hidden its NULL. A marker-confirmed `getwd()` upward walk is the fallback instead.
- **Files modified:** `tests/testthat/helper-spatial-interventions.R`
- **Commit:** `52a6361`

### 3. The `WR-09` fixture differs from the one the review specified

- **Found during:** Task 3 (design).
- **Issue:** the review's `zone = "Outside"` rank-2 entry trips the NaN guard's other clause, so it
  would have reproduced the very skip it was meant to remove — a second vacuous test.
- **Fix:** partial zeroing via `From_lulc_filter` instead, so both zones retain positives.
  Documented in-file and proven by the counter-check table above.
- **Files modified:** `tests/testthat/test-spatial-interventions.R`
- **Commit:** `516ae98`

## Constraints honoured

- **No `src/` modification.** `git diff --stat` against the base touches three files, all under
  `tests/testthat/`. D-13 (no `resample(` / `project(`) and D-20 (no probability-table copy) are
  therefore untouched and remain closed; the `D-20` block that greps the engine source still passes.
- **The 21-field AUDIT `sprintf()` is untouched.** The new blocks only *read* the line, via the
  existing `.audit_field()` extractor, so `scripts/verify_intervention_smoke.r` (plan 05-14) is
  unaffected.
- **No `STATE.md`, `ROADMAP.md` or `.planning/HANDOFF.json` writes**, and none of plan 05-14's files
  (`scripts/verify_intervention_smoke.r`, `tests/testthat/test-verify-intervention-smoke.R`) were
  touched.
- **Fixtures supply the post-05-09/05-13 formals.** `.run_engine()` already passes `ref_grid_path`
  and forwards `telemetry_dir` only when supplied; every new block goes through it unchanged.

## Deferred

`tests/testthat/test-prep-paths.R` carries the *same class* of defect this plan removed: its own
`.repo_root` is derived from the sourcing frame's `ofile` entry (and, unlike the two files fixed
here, without even the directory fix-up), which is NULL under `test_dir()` and resolves to
`.claude/worktrees`. That is what produces the four standing errors at lines 27/35/43/56. It was
explicitly out of scope here. The fix is now a one-line change — delete its bootstrap and let it
consume the helper's `.repo_root`, exactly as the two spatial-interventions files now do — and should
be picked up by whichever plan owns the test-harness cleanup.

## Self-Check: PASSED

- `tests/testthat/helper-spatial-interventions.R` — FOUND (77 lines, defines `.repo_root`)
- `tests/testthat/test-spatial-interventions.R` — FOUND (modified)
- `tests/testthat/test-spatial-interventions-wiring.R` — FOUND (modified)
- Commit `52a6361` — FOUND
- Commit `2e803d3` — FOUND
- Commit `516ae98` — FOUND
- Full suite re-run after the last commit: `PASS 659 | FAIL 0 | SKIP 13 | ERROR 4`, the four errors
  at `test-prep-paths.R:27/35/43/56` only — identities verified, not just the count.
