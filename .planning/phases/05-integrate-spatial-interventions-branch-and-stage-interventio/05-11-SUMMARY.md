---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
plan: 11
subsystem: spatial-interventions
tags: [numerics, boundary-conditions, scoping, audit-honesty, gap-closure]
requires:
  - implement_spatial_interventions() resolver-driven applier (Phase 5 Plan 07)
  - runtime mask geometry validation + ref_grid_path formal (Phase 5 Plan 09)
provides:
  - percentile row selection aligned with the percentile mean (WR-01a)
  - Perc_diff == 0 threshold path, consistent across all three valencies (WR-01b)
  - per-index-set probability clamps scoped to the declared target class and zone (WR-02)
  - signed threshold logging that matches the value assigned (WR-03)
  - self-contained valency explanation log lines prefixed with to_val=<class> (IN-04)
affects:
  - "scripts/verify_intervention_smoke.r: AUDIT field set and order unchanged, but rows_changed VALUES move"
  - "plan 05-16 operator re-run: compare against the new semantics, NOT against job 838021"
  - "plan 05-12: extends the AUDIT line with delta statistics; the format string was left byte-identical for it"
tech-stack:
  added: []
  patterns:
    - mirror the comparison operator between a statistic and the rows it governs
    - scope a by-reference mutation to the index set that was just written
    - log the post-substitution value, never the pre-substitution one
    - every log line carries enough identity to stand alone
key-files:
  created:
    - .planning/phases/05-integrate-spatial-interventions-branch-and-stage-interventio/05-11-SUMMARY.md
  modified:
    - src/implement_spatial_interventions.R
    - tests/testthat/test-spatial-interventions.R
decisions:
  - "All six row selections moved to >=, not just the ones the review reproduced: the mean is defined with >= on both zones, so any remaining strict > would reintroduce the same mean/selection mismatch on a different branch"
  - "The Perc_diff == 0 boundary is handled by widening the existing dispatch to >= 0 rather than adding a fourth branch; at zero the already-present abs(Perc_diff) < threshold test fires and substitutes the threshold, which is exactly what Increase_inside_decrease_outside already did"
  - "Clamping is per-written-index-set, not per-target-class-subset: rows_changed is measured over sub_idx / Target_area_idx, so clamping the written rows only is the narrowest scope that still keeps every mutation accounted for"
  - "pmin/pmax chosen over a conditional update precisely because they propagate NA, preserving the intent of the deleted !is.na(prob) guard without a second pass"
  - "Three stale code comments that quoted the old `if (Perc_diff > 0)` / `Perc_diff > 0 ->` dispatch were corrected; leaving them would have left the NaN guard's own rationale describing code that no longer exists"
  - "D-23 honoured: each of WR-01a, WR-01b, WR-01c, WR-02 (x2), WR-03 (x2) and IN-04 ships with a named regression test that failed against the pre-fix code"
metrics:
  duration: ~40 min (wall, including one mid-run session handover after a transient API auth error)
  completed: 2026-09-28
  tasks: 3
  commits: 3
  red_failures: 14
  green_file: "test-spatial-interventions.R 0 fail / 0 error"
  green_suite: "479 pass / 13 skip / 4 pre-existing error"
---

# Phase 5 Plan 11: Adjustment semantics in the intervention helpers Summary

`relative_prob_adjust()` now adjusts exactly the rows its own percentile mean was computed over,
applies `Prob_adjust_threshold` at an exactly zero percentage difference for all three valencies,
and logs the signed value it actually assigned; both helpers clamp only the rows they wrote, so an
intervention can no longer rewrite a probability row outside its declared target class and zone.

## What was built

### WR-01(a) — the mean and the selection disagreed at the boundary

`Intervention_ptile_mean` and `Non_intervention_ptile_mean` are defined with `>=` against the
percentile value, but all six row selections used a strict `>`. When probabilities tie at the
percentile — exactly what a higher-ranked `Absolute` intervention flattening a zone produces, and
what the upstream `tot_prob` clamp produces — the mean was taken over many rows while **zero** rows
were selected. The AUDIT line still counted the intervention as applied, with `rows_changed=0`.

All six selections are now `>=`, with a comment at the first site citing WR-01 and stating that the
selection must mirror the `>=` that defines the means.

### WR-01(b) — the exact-zero dead zone

`Increase` and `Decrease` dispatched on `if (Perc_diff > 0) ... else if (Perc_diff < 0)`. At exactly
zero neither branch ran, so `Prob_adjust_threshold` — whose entire purpose is to guarantee a minimum
nudge when the two zones are indistinguishable — was silently skipped. `Increase_inside_decrease_outside`
has always applied it at zero, so the three valencies disagreed at the boundary. All four shipped
scenarios rely on `Prob_adjust_threshold: 5` doing that job.

Both dispatches are now `if (Perc_diff >= 0)`. At zero the already-present `abs(Perc_diff) <
Prob_adjust_threshold` test fires, logs through `threshold_msg()` and substitutes the threshold.
The `Perc_diff < 0` branches' direction semantics are untouched.

### WR-02 — table-wide clamps

Both helpers clamped the **entire** probability table inside the per-target-class loop:

```r
normalized[prob > 1, prob := 1]
normalized[!is.na(prob) & prob < 0, prob := 0]
```

An intervention declared for class 105 Inside therefore rewrote rows of every other transition in
both zones, uncounted by `rows_changed`, contradicting the phase's recorded non-renormalisation
decision. Both pairs are deleted. Each written index set is now clamped immediately after its own
`:=` with `normalized[ix, prob := pmin(pmax(prob, 0), 1)]` — one clamp in `absolute_prob_adjust()`
and six in `relative_prob_adjust()` (`Increase_inside_decrease_outside` writes two index sets, so it
clamps twice). `pmin`/`pmax` propagate NA, so NA rows stay NA exactly as the deleted `!is.na(prob)`
guard intended.

`rows_changed` is still computed over `Target_area_idx` (Absolute) and `sub_idx` (Relative), so it is
now a **complete** account of every row the intervention mutated, not a partial one.

### WR-03 — the threshold log reported the wrong sign

Two call sites logged a value different from the one assigned on the next line:

| Site | Assigned | Logged before | Logged now |
|------|----------|---------------|------------|
| `Increase` / `Perc_diff < 0` | `-(Prob_adjust_threshold)` | the pre-threshold `Perc_diff` (e.g. `-3.27868852459017`) | `-5` |
| `Decrease` / `Perc_diff < 0` | `-(Prob_adjust_threshold)` | `+5` | `-5` |

### IN-04 — the orphan log fragment

`because_msg()` emitted a bare sentence fragment (`because the Prob_adjust_valency is Decrease and
the percentage difference is >0 then ...`) with no target class, sitting next to the `to_val=`-prefixed
skip lines and unattributable from the log alone. It now takes `lulc_class` as its first argument and
emits `to_val=<class>: because the Prob_adjust_valency is <valency> <tail>`. All five call sites pass
`lulc_class`.

## Tasks completed

| Task | Name | Commit |
|------|------|--------|
| 1 | RED fixtures for tied percentiles, the zero dead zone, out-of-scope clamping and log honesty (D-23) | `0b55a0d` |
| 2 | Align the percentile comparison and handle an exactly zero difference | `92c7061` |
| 3 | Clamp only the rows the intervention touched | `ab11bc9` |

## Tests

Nine new named blocks at the end of `tests/testthat/test-spatial-interventions.R`, plus two fixture
helpers (`.norm_split(inside, outside)` rewrites only the `to_val = 105` rows so the `to_val = 104`
rows can double as out-of-scope witnesses; `.because_lines()` extracts the valency explanation lines).

| Block | Fixture | Pre-fix behaviour |
|-------|---------|-------------------|
| `WR-01a: a Relative intervention with probabilities tied at the percentile still adjusts rows` | inside 0.4 / outside 0.1, `Decrease` | `quantile(c(0.4,0.4,0.4), 0.5)` **is** 0.4, strict `>` selected nothing; `rows_changed=0` |
| `WR-01b: an exactly zero percentage difference applies the threshold (Decrease)` | inside 0.3 / outside 0.3 | `Perc_diff == 0`, no branch ran, no threshold log, `rows_changed=0` |
| `WR-01c: an exactly zero percentage difference applies the threshold (Increase)` | inside 0.3 / outside 0.3 | same dead zone |
| `WR-02: an Absolute intervention does not rewrite rows outside its target class` | seeded `to_val=104`, cell 2 (outside), `prob = 1.7` | rewritten to `1.00` by the table-wide clamp |
| `WR-02: a Relative intervention does not rewrite rows outside its target class` | same seeded row | rewritten to `1.00` |
| `WR-03: the threshold log reports the signed value actually assigned (Increase)` | inside 0.30 / outside 0.31 → `Perc_diff = -3.279` | logged `-3.27868852459017`, assigned `-5` |
| `WR-03: the threshold log reports the signed value actually assigned (Decrease)` | same | logged `5`, assigned `-5` |
| `IN-04: the valency explanation line is not an orphan fragment` | `.norm()`, `Decrease` (`Perc_diff = 22.2`, so the line existed pre-fix) | no `to_val=` on the line |

**RED baseline (Task 1, against pre-plan `src/`): 14 failing expectations**, with
`git diff --name-only src/` empty at that point as the plan required.

## Verification

| Gate | Result |
|------|--------|
| `parse("src/implement_spatial_interventions.R")` | exits 0 |
| `parse("tests/testthat/test-spatial-interventions.R")` | exits 0 |
| `test_file("tests/testthat/test-spatial-interventions.R")` | 0 fail / 0 error (RED was 14 failing) |
| `test_dir("tests/testthat")` | **PASS 479 \| FAIL 4 \| WARN 6 \| SKIP 13** |
| Baseline at base commit `f7b1741` | PASS 459 \| FAIL 4 \| WARN 6 \| SKIP 13 |
| Failure identities | `test-prep-paths.R` lines **27, 35, 43, 56** — byte-identical to the baseline set, all four the pre-existing `.repo_root`/`sys.frame(1)$ofile` harness defect |

Delta: **+20 passing assertions, no new failure, no new error, no new skip.**

Acceptance greps on `src/implement_spatial_interventions.R`:

| Grep | Required | Actual |
|------|----------|--------|
| `Intervention_vals > Intervention_ptile_val` | 0 | **0** |
| `Non_Intervention_vals > Non_intervention_ptile_val` | 0 | **0** |
| `>= Intervention_ptile_val` | >= 3 | **4** (3 selections + the mean) |
| `if (Perc_diff > 0)` | 0 | **0** |
| `threshold_msg(lulc_class, Perc_diff)` | 0 | **0** |
| `because_msg(lulc_class` | 5 | **5** |
| `normalized[prob > 1, prob := 1]` | 0 | **0** |
| `normalized[!is.na(prob) & prob < 0, prob := 0]` | 0 | **0** |
| `pmin(pmax(prob, 0), 1)` | >= 5 | **7** (1 Absolute + 6 Relative) |
| `cells_sum_gt1` | unchanged | **4** (non-renormalisation diagnostic survives) |

Frozen contracts confirmed intact:

- The AUDIT `sprintf()` format string
  `AUDIT stage=intervention region=%s scenario=%s year=%d id=%s rank=%s type=%s zone=%s to_vals=%s mask=%s rows_target=%d rows_changed=%d`
  occurs **once** and does not appear in `git diff f7b1741 -- src/implement_spatial_interventions.R`
  at all — **byte-identical**, so `scripts/verify_intervention_smoke.r:193-228` keeps parsing it and
  plan 05-12 still owns extending it.
- `list(normalized, rows_target, rows_changed)` return shape of both helpers — unchanged.
- The NaN guard and its `intervention skip: to_val=%s ...` lines — unchanged (only the stale comment
  quoting the old dispatch was corrected).
- `rows_target` semantics (Absolute: zone-restricted target rows; Relative: all rows of the target
  class across both zones) — unchanged.
- Non-renormalisation (05-02 decision) — no rescaling added; `write_summary()` and `cells_sum_gt1`
  untouched.
- **D-13**: `resample(` and `project(` in the intervention engine are still **0 / 0**.
- `ref_grid_path` remains a required formal with no default (05-09).

## The HPC smoke numbers WILL move

**This plan changes numerical behaviour by design.** The `rows_changed` counts produced by the
shipped HPC smoke (job 838021, NAT x costa_peruana x 2032) are pre-fix values and are expected to
change:

- interventions that tied at the percentile previously reported `rows_changed=0` and now report a
  positive count;
- interventions with an exactly zero percentage difference previously changed nothing and now apply
  the `Prob_adjust_threshold: 5` nudge;
- `rows_changed` no longer under-reports, because the mutations it counts are now the only mutations
  the helpers perform.

**The operator re-run in plan 05-16 must be evaluated against the new semantics, not compared
against job 838021.** A diff against the old counts is expected and is not a regression signal. The
AUDIT *field set and order* are unchanged, so the verifier's parser needs no change — only the
expected values do.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Three code comments still described the pre-fix dispatch**

- **Found during:** Task 2 (the acceptance grep `grep -c "if (Perc_diff > 0)" == 0` returned 1)
- **Issue:** The NaN guard's own rationale comment read ``mean chain yields NA/NaN and `if (Perc_diff
  > 0)` would crash with "missing value where TRUE/FALSE needed"``, and the two valency-branch
  headers read `Perc_diff > 0 -> increase/decrease intervention pixels above the percentile`. After
  the `>= 0` change all three described code that no longer exists, and the first made the plan's
  acceptance grep fail on a comment.
- **Fix:** Updated the three comments to `>= 0`. No executable line of the NaN guard was touched —
  its condition, its `intervention skip:` message and its position are unchanged, as
  `<interfaces>` requires.
- **Files modified:** `src/implement_spatial_interventions.R`
- **Commit:** `92c7061`

### Notes on scope

- The plan's WR-01/WR-02/WR-03 fixtures were derived from the engine's own arithmetic and the
  shipped `.rel_entry()` / `.abs_entry()` helpers, **not** from `05-REVIEW.md` prose — per the
  execution brief's anti-pattern warning about WR-11's misattribution. No shipped YAML was read for
  parameter values, and no shipped YAML was modified.
- `config/NAT_interventions.yml` and `config/SOC_interventions.yml` `Urban_densification`
  (`Increase_inside_decrease_outside`) behaviour is unchanged at the boundary: that valency already
  applied the threshold at zero, and its two row selections moved from `>` to `>=` along with every
  other selection, which is the WR-01(a) fix, not a valency-specific change.
- `src/allocation.r` and `tests/testthat/test-spatial-interventions-wiring.R` were not touched (they
  belong to the parallel plan 05-10). `.planning/STATE.md` and `.planning/ROADMAP.md` were not
  touched — the orchestrator owns those.
- `.planning/HANDOFF.json` is modified in the worktree by a session hook, not by this plan, and was
  deliberately left unstaged.
- No authentication gate was hit. No package was installed. No architectural (Rule 4) decision arose.
- Fix-attempt limit never reached: one auto-fix in three tasks.

## Requirements

| ID | Status |
|----|--------|
| WR-01 | Closed — `>=` alignment (a) and the `Perc_diff == 0` threshold path (b), each with a named regression test |
| WR-02 | Closed — clamps scoped to the written index set in both helpers; two regression tests pin a `prob = 1.7` out-of-scope row |
| WR-03 | Closed — both `Perc_diff < 0` threshold sites log the signed assigned value |
| IN-04 | Closed — every `because the Prob_adjust_valency is` line now carries `to_val=<class>:` first |
| D-23 | Satisfied — all seven fixes ship with a test that failed against the pre-fix code |

## Threat Model Outcomes

| Threat ID | Disposition | Outcome |
|-----------|-------------|---------|
| T-05-40 (Tampering — table-wide clamps) | mitigate | Mitigated. Both helpers clamp only their own written index set; two regression tests assert an out-of-scope row is untouched |
| T-05-41 (Repudiation — `rows_changed` as an audit number) | mitigate | Mitigated. With scoped clamps and `>=` selection, `rows_changed` accounts for every mutation the intervention performs |
| T-05-42 (Repudiation — threshold and valency logs) | mitigate | Mitigated. `threshold_msg()` reports the signed assigned value; `because_msg()` emits a complete, class-attributed clause |
| T-05-43 (Tampering — silent no-op at the boundary) | mitigate | Mitigated. The "applied but changed nothing" state the AUDIT line could not distinguish is gone for both the tie and the exact-zero case |
| T-05-SC (package-manager installs) | n/a | Honoured. No dependency added; no `install.packages` / npm / pip / cargo invocation |

## Known Stubs

None. No hardcoded empty value, placeholder, TODO or unwired data path was introduced.

## Threat Flags

None. No network endpoint, auth path, file-access pattern or schema change at a trust boundary was
introduced or altered; this plan only changes arithmetic and logging inside two existing helpers.

## Out of scope / deferred

- **AUDIT delta statistics** — plan 05-12 owns extending the AUDIT line. The format string was left
  byte-identical for it, and the `list(normalized, rows_target, rows_changed)` return shape was not
  pre-emptively widened.
- **Operator HPC re-run** — plan 05-16, against the new semantics (see the section above).
- **`scripts/verify_intervention_smoke.r`** — untouched. Its parser is unaffected; only the values it
  will observe change.
- **The 4 pre-existing `test-prep-paths.R` errors** — left alone per the scope boundary; they are a
  harness defect (`.repo_root` from `sys.frame(1)$ofile`, NULL under `test_dir()`), not a Phase 5
  concern.

## Self-Check

- `src/implement_spatial_interventions.R` — FOUND
- `tests/testthat/test-spatial-interventions.R` — FOUND
- `.planning/phases/05-integrate-spatial-interventions-branch-and-stage-interventio/05-11-SUMMARY.md` — FOUND
- Commit `0b55a0d` — FOUND
- Commit `92c7061` — FOUND
- Commit `ab11bc9` — FOUND
