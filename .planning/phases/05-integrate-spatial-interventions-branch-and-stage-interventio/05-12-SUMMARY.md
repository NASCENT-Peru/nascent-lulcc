---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
plan: 12
subsystem: spatial-interventions
tags: [telemetry, audit-logging, observability, memory-discipline, gap-closure]
requires:
  - resolver-driven applier and Prob_adjust_* schema ownership (Phase 5 Plan 07)
  - runtime mask geometry validation + ref_grid_path formal (Phase 5 Plan 09)
  - per-index-set clamps and `>=` percentile alignment (Phase 5 Plan 11)
provides:
  - "`.delta_stats()` / `.delta_stats_skipped()` / `.bind_delta_stats()`: the 19-column per-target-class probability-change contract"
  - "`res$stats` returned by both `absolute_prob_adjust()` and `relative_prob_adjust()`, one row per Target_classes element in order, skipped classes included"
  - "pooled per-intervention delta vector on `attr(res$stats, \"delta\")`"
  - "eight appended AUDIT fields: delta_mean delta_med delta_sd delta_min delta_max n_inc n_dec sum_abs_delta"
  - "`.fmt_num()`: single-token numeric rendering for AUDIT fields"
affects:
  - "plan 05-13: the per-intervention CSV consumes `res$stats` directly — its 19 columns are a superset of the CSV's per-class columns"
  - "plan 05-14 / plan 05-22: any new AUDIT telemetry appends after sum_abs_delta, never before it"
  - "scripts/verify_intervention_smoke.r: parser unchanged and re-verified against real captured lines; D-22 assertions land in a later plan"
tech-stack:
  added: []
  patterns:
    - "frozen log prefix + additive suffix: new telemetry is appended at the end of an AUDIT line, never interleaved"
    - "seed the skipped-shape result at the top of a loop iteration so every `next` path still contributes its row"
    - "read a mutated index set back ONCE and reuse that vector for every statistic and counter"
    - "lazily-defaulted argument (`n_changed = .count_prob_changes(before, after)`) so a caller that already computed a value never pays for it twice"
    - "render every log field through a formatter that guarantees a single whitespace-delimited token"
key-files:
  created:
    - .planning/phases/05-integrate-spatial-interventions-branch-and-stage-interventio/05-12-SUMMARY.md
  modified:
    - src/implement_spatial_interventions.R
    - tests/testthat/test-spatial-interventions.R
key-decisions:
  - "`.delta_stats()` returns its class delta vector on `attr(<row>, \"delta\")` rather than the adjusters re-reading the table for the roll-up: the pooled vector is a concatenation of work already done, so the intervention-level statistics cost no extra pass over the probability table"
  - "`n_changed` is a lazily-defaulted formal, so `rows_changed` is still computed by exactly one `.count_prob_changes()` call per target class — the telemetry adds no second NA-aware comparison over 7.5M-row index sets"
  - "The skipped-class row is a separate constructor (`.delta_stats_skipped()`) seeded BEFORE any `next` can fire, rather than a post-hoc fill: the three `next` paths in `relative_prob_adjust()` are easy to extend and a future fourth one gets its row for free"
  - "`stats` rides on the helper return list and the pooled delta on an attribute, so `names(res$stats)` stays exactly the 19 contract columns and plan 05-13's CSV writer can bind rows without stripping anything"
  - "The format string was extended in place as one contiguous literal so BOTH the frozen-prefix grep and the appended-fields grep match a single line — the plan's acceptance greps are a structural guard against a future edit splitting the literal and quietly reordering fields"
  - "`.fmt_num()` uses `width = 1L` AND `trimws()`: `formatC(format = \"g\")` defaults `width` to `digits`, and relying on only one of the two guards is how the space-padding defect shipped in the first place"
  - "D-23 honoured: D-18, D-18b, D-18c, D-19, D-19b all failed against the pre-plan engine (47 failing expectations); D-20 is a standing source-level guard that must never start failing"
requirements-completed: [D-18, D-19, D-20, D-23]
duration: ~35 min
completed: 2026-09-28
---

# Phase 5 Plan 12: Probability-change telemetry in the intervention engine Summary

**Every `AUDIT stage=intervention` line now carries the distribution of the probability change that
intervention made — mean, median, sd, min, max, increase/decrease counts and total absolute movement
— computed over the rows it targeted against the values immediately before it ran, with the shipped
field prefix byte-identical and without ever copying the probability table.**

## Performance

- **Duration:** ~35 min
- **Tasks:** 3 of 3
- **Files modified:** 2
- **Commits:** 3

## Accomplishments

### Task 1 — RED tests for the delta contract (commit `3f15494`)

Five new named blocks plus a standing source guard, all in
`tests/testthat/test-spatial-interventions.R`:

| Block | What it pins |
|-------|--------------|
| `D-18: the AUDIT intervention line keeps its shipped prefix and appends delta fields` | the fixed-string prefix the pre-05-12 tests already assert, ONE ordered regex over the whole line, an exact field count of `13 + 8 = 21`, and the shipped verifier's own `sub("^.* id=([^ ]+) .*$", ...)` extraction |
| `D-19: delta statistics describe only this intervention's change` | hand-computed values for inside probs 0.2/0.4/0.6 driven to 0: `delta_min=-0.6`, `delta_max=-0.2`, `delta_mean=delta_med=-0.4`, `delta_sd=0.2`, `n_dec=3`, `n_inc=0`, `sum_abs_delta=1.2` |
| `D-19b: a second intervention's before values are the first intervention's output` | two ranked Absolute-to-0 interventions on the same rows; rank 1 reports `sum_abs_delta=1.2`, rank 2 reports `0` with `n_inc=n_dec=0` |
| `D-18b: a skipped target class still produces a statistics row` | `absolute_prob_adjust(Target_classes = c(105L, 999L))` returns `nrow(res$stats) == 2` with `n_target = 0L` and `NA_real_` mean/sd/percentile/mass for the absent class |
| `D-18c: the helper statistics contract` | `names(res$stats)` is exactly the 19-column vector in order, and the five count columns are integer, for BOTH helpers |
| `D-20: no full-table copy per intervention` | source-level `expect_false` on `copy(normalized`, `data.table::copy(` — and, carried along, on `resample(` / `project(` so D-13 stays closed |

Three new test helpers (`.audit_body()`, `.audit_nfields()`, `.audit_field()`) strip the
`log_msg()` timestamp prefix so field order and field count are asserted over the AUDIT body only.

**RED baseline: 47 failing expectations** in this file (`FAIL 47 | PASS 135`), with `src/`
untouched (`git diff --name-only src/` empty at that commit).

### Task 2 — both helpers return per-target-class statistics (commit `3c36850`)

`.delta_stats(target_class, before, after, n_changed = .count_prob_changes(before, after))` builds
the 19-column row:

```
target_class, n_target, n_changed, mean_before, mean_after, sd_before, sd_after,
p05_delta, p25_delta, p50_delta, p75_delta, p95_delta, min_delta, max_delta,
sum_abs_delta, prob_mass_before, prob_mass_after, n_inc, n_dec
```

`ok <- !is.na(before) & !is.na(after)`, `d <- after[ok] - before[ok]`; percentiles via
`stats::quantile(d, c(.05,.25,.5,.75,.95), names = FALSE, na.rm = TRUE)`; `length(d) == 0L` returns
the `.delta_stats_skipped()` shape.

Both adjusters now loop `for (i in seq_along(Target_classes))` and seed
`stats_rows[[i]] <- .delta_stats_skipped(lulc_class)` at the top of the iteration, so every `next`
path contributes its row — in `absolute_prob_adjust()` the empty-class path, and in
`relative_prob_adjust()` all three (empty class, one empty zone, NaN guard). On the success path the
targeted index set is read back exactly once:

```r
after <- normalized$prob[Target_area_idx]     # or sub_idx, in the Relative helper
n_changed_k <- .count_prob_changes(before, after)
st <- .delta_stats(lulc_class, before, after, n_changed_k)
rm(after)
```

Because `n_changed` is a lazily-defaulted formal, the NA-aware comparison still runs exactly once
per class — the telemetry adds no second pass. `rows_target` / `rows_changed` arithmetic is
unchanged, and the five pre-existing AUDIT-value tests (`rows_target=3 rows_changed=3` etc.) still
pass untouched.

`.bind_delta_stats()` rbinds the rows and pools the per-class deltas with a single `unlist()` onto
`attr(stats, "delta")` (`numeric(0)` when everything was skipped — `unlist()` of a list of empty
numerics returns `NULL`, which is normalised).

### Task 3 — the eight appended AUDIT fields (commit `c841cbe`)

The intervention-level roll-up reads the pooled vector off the attribute, computes
`mean / median / sd / min / max` over it, sums `n_inc`, `n_dec` and `sum_abs_delta` down the stats
columns, writes the line, then `rm()`s the pooled vector. The format string was extended in place:

```
AUDIT stage=intervention region=%s scenario=%s year=%d id=%s rank=%s type=%s zone=%s to_vals=%s mask=%s rows_target=%d rows_changed=%d delta_mean=%s delta_med=%s delta_sd=%s delta_min=%s delta_max=%s n_inc=%d n_dec=%d sum_abs_delta=%s
```

`AUDIT stage=intervention_summary` is untouched. The roxygen for
`implement_spatial_interventions()` gained an `@section Intervention AUDIT line contract:` block
stating that the fields up to and including `rows_changed` are frozen because
`scripts/verify_intervention_smoke.r` parses them by fixed string, and that the eight new fields are
additive.

## Real captured AUDIT lines (for plans 05-13 and 05-14)

From a live engine fixture run (`scenario=BAU`, `year=2028`, region `R1`, mask `mask_a.tif`,
rank-1 Absolute-to-0 on class 105 followed by rank-2 Relative Decrease on class 104), timestamp
prefix stripped:

```
AUDIT stage=intervention region=R1 scenario=BAU year=2028 id=iv_abs rank=1 type=Absolute zone=Inside to_vals=105 mask=mask_a.tif rows_target=3 rows_changed=3 delta_mean=-0.4 delta_med=-0.4 delta_sd=0.2 delta_min=-0.6 delta_max=-0.2 n_inc=0 n_dec=3 sum_abs_delta=1.2
AUDIT stage=intervention region=R1 scenario=BAU year=2028 id=iv_rel rank=2 type=Relative zone=Inside to_vals=104 mask=mask_a.tif rows_target=6 rows_changed=2 delta_mean=-0.0407407 delta_med=0 delta_sd=0.0635053 delta_min=-0.133333 delta_max=0 n_inc=0 n_dec=2 sum_abs_delta=0.244444
AUDIT stage=intervention_summary region=R1 scenario=BAU year=2028 n_interventions=2 cells_sum_gt1=0
```

Token shapes a downstream parser can rely on:

- every field is `name=value` with **no space inside the value** (`delta_*` and `sum_abs_delta` are
  `formatC(format = "g", digits = 6, width = 1L)` then `trimws()`d, so `1.23457e+08` and `1e-12`
  are possible but a space never is);
- a non-finite statistic is the literal token `NA` (e.g. `delta_sd=NA` when only one row moved);
- `n_inc` and `n_dec` are bare integers;
- the AUDIT body is exactly **21** whitespace-delimited tokens (was 13);
- an all-classes-skipped intervention emits `delta_mean=NA delta_med=NA delta_sd=NA delta_min=NA
  delta_max=NA n_inc=0 n_dec=0 sum_abs_delta=0`.

Re-applying the shipped verifier's own expressions to those captured lines:
`is_iv()` matched 2 of 2, `sub("^.* id=([^ ]+) .*$", "\\1", ...)` returned `iv_abs, iv_rel`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `formatC(format = "g")` left-pads to `digits`, splitting one AUDIT field into four tokens**

- **Found during:** Task 3 verification
- **Issue:** the plan specified `formatC(v, format = "g", digits = 6)`. `formatC()` defaults `width`
  to `digits` for the `"g"` format, so `-0.4` rendered as `"   -0.4"` and the line came out with
  **41** whitespace-delimited fields instead of 21:
  `... delta_mean=   -0.4 delta_med=   -0.4 delta_sd=    0.2 ...`. This is exactly threat T-05-46
  (unformattable statistic breaking log parsing) materialising from the mitigation's own
  implementation — and it would have silently broken any splitter-based parser of the new fields,
  including plan 05-14's.
- **Fix:** `.fmt_num()` now uses `trimws(formatC(v, format = "g", digits = 6, width = 1L))` — the
  explicit `width` is the actual fix, `trimws()` is a second guard so the single-token invariant
  survives a future `formatC()` or locale change. The reason is recorded in a comment at the
  helper.
- **Caught by:** the Task 1 test `expect_identical(.audit_nfields(a$iv), .AUDIT_FIELDS_PRE_05_12 + 8L)`,
  which reported `actual: 41 / expected: 21`. No new test was needed — the field-count assertion
  the plan asked for is precisely the regression test for this defect.
- **Files modified:** `src/implement_spatial_interventions.R`
- **Commit:** `c841cbe`

**2. [Rule 3 - Naming] Helper named `.fmt_num()` rather than `fmt_num()`**

- **Found during:** Task 3
- **Issue:** the plan names the formatter `fmt_num(v)`. This file is `source()`d into the global
  environment by the tests and into worker environments by the allocation hook, so a bare
  `fmt_num` would occupy a very collidable global name.
- **Fix:** named `.fmt_num()`, matching the file's existing internal-helper convention
  (`.count_prob_changes()`, `.mask_inside_lut()`). Behaviour is exactly as specified. No acceptance
  criterion greps for the identifier.
- **Files modified:** `src/implement_spatial_interventions.R`
- **Commit:** `c841cbe`

### Deliberate departures from the plan text

**`.delta_stats()` has a fourth, lazily-defaulted formal.** The plan's signature is
`.delta_stats(target_class, before, after)` and separately instructs "reuse `after` for the existing
`.count_prob_changes()` call rather than indexing the table a second time". Taken literally, both
the adjuster and `.delta_stats()` would call `.count_prob_changes()` on the same vectors — two
NA-aware comparisons over a 7.5M-element index set per class. Adding
`n_changed = .count_prob_changes(before, after)` as a defaulted argument satisfies the stated
three-argument call form (the tests call it through the adjusters only), keeps the count to one, and
never forces the promise on the skipped-class path.

## Memory discipline (D-20)

Per intervention, per target class, the additional memory held is three `length(rows_target)`
double vectors — `before`, `after` (dropped via `rm()` the moment the stats row is built) and the
class delta `d`. The class deltas are pooled once for the AUDIT roll-up and dropped as soon as the
line is written. What survives the intervention is the 19-column stats table: one small row per
target class, which plan 05-13 needs for the CSV.

Asserted mechanically by the `D-20` test: zero occurrences of `copy(normalized` and of
`data.table::copy(` in `src/implement_spatial_interventions.R`. D-13 is asserted alongside it —
still zero `resample(` / `project(`. The engine still never renormalises; `cells_sum_gt1` is
logged, not corrected.

## Verification

| Check | Result |
|-------|--------|
| `parse("src/implement_spatial_interventions.R")` | exits 0 |
| `parse("tests/testthat/test-spatial-interventions.R")` | exits 0 |
| `test_file("tests/testthat/test-spatial-interventions.R")` | `FAIL 0 / PASS 182` (was `FAIL 0 / PASS 124` at base; RED peak `FAIL 47`) |
| `test_dir("tests/testthat")` | **PASS 568 / FAIL 0 / ERROR 4 / WARN 4 / SKIP 13** |
| Failing-test identities | only `test-prep-paths.R` lines **27, 35, 43, 56** — the pre-existing `sys.frame(1)$ofile` harness defect, untouched and out of scope |
| `grep -c "AUDIT stage=intervention region=%s ... rows_target=%d rows_changed=%d"` | 1 (frozen prefix intact) |
| `grep -c "delta_mean=%s delta_med=%s ... sum_abs_delta=%s"` | 1 |
| `grep -c "AUDIT stage=intervention_summary region=%s ... cells_sum_gt1=%d"` | 1 (summary untouched) |
| `grep -c "\.delta_stats"` | 9 (>= 4 required) |
| `grep -c "copy(normalized"` / `grep -c "data.table::copy"` | 0 / 0 |
| `grep -c "resample(\|project("` | 0 (D-13 still closed) |
| Shipped verifier re-applied to captured lines | `is_iv()` 2/2, id extraction `iv_abs, iv_rel`, 21 fields, no field contains a space |

Baseline reconciliation: the phase baseline is quoted as `PASS 510 | FAIL 4 | WARN 6 | SKIP 13`,
where the 4 "FAIL" are the `test-prep-paths.R` errors. This run reports them in the `ERROR` column
(`FAIL 0 / ERROR 4`) because the counts were read off `test_dir(reporter = "silent")`'s result
frame rather than the console banner. Same four tests, same four line numbers, no new failures.
The pass count rises 510 -> 568 from this plan's 58 new expectations.

## Known Stubs

None. No placeholder values, empty-literal data sources or TODO markers were introduced.

## TDD Gate Compliance

Plan type is `execute`, not `tdd`, but the plan's own task order is RED/GREEN and was followed:
`test(05-12)` (`3f15494`, 47 failing expectations) precedes both `feat(05-12)` commits
(`3c36850`, `c841cbe`). No refactor commit was needed.

## What's next

- **Plan 05-13** writes `intervention_prob_deltas_<scenario>_<region>_<year>.csv` (D-18 part 2,
  D-21). It consumes `res$stats` directly: the 19 contract columns are a superset of the CSV's
  per-class columns, so the writer only needs to prepend
  `scenario, region, year, intervention_id, rank, type, zone, mask` and drop `n_inc` / `n_dec` if
  the CSV schema in `05-CONTEXT.md` D-18 is taken literally.
- **Plan 05-14 / 05-22** — any further AUDIT telemetry appends after `sum_abs_delta`. The frozen
  prefix now runs through `rows_changed`; the eight new fields are themselves now a published
  contract that plan 05-13's CSV cross-checks and D-22 will assert in the smoke verifier.

## Self-Check: PASSED

- Files claimed created/modified all exist on disk: `05-12-SUMMARY.md`,
  `src/implement_spatial_interventions.R`, `tests/testthat/test-spatial-interventions.R`.
- All four claimed commits exist on `6e71cf4..HEAD`: `3f15494`, `3c36850`, `c841cbe`, plus this
  docs commit.
- `git diff --diff-filter=D --name-only 6e71cf4 HEAD` is empty — no file was deleted.
- No untracked files left behind; `STATE.md`, `ROADMAP.md` and `REQUIREMENTS.md` were not touched
  (the orchestrator owns those writes). `.planning/HANDOFF.json` carries a timestamp-only change
  written by the GSD auto-postool hook, deliberately left uncommitted.
- Diff scope: 3 files, +729 / -14.
