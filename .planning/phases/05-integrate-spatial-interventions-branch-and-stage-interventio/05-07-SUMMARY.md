---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
plan: 07
subsystem: spatial-interventions
tags: [validation, fail-fast, yaml-schema, refactor, gap-closure]
requires:
  - resolve_intervention_masks() (Phase 5 Plan 02)
  - implement_spatial_interventions() (Phase 5 Plan 02)
provides:
  - resolver-owned intervention identity validation (CR-01)
  - resolver-owned Prob_adjust_* schema validation (CR-03)
  - resolver entry_index column + attr(., "entries") provenance contract (WR-06)
  - single YAML parse / single Allocation-year filter in the engine (WR-06)
affects:
  - src/allocation.r validate_allocation_runtime() pre-flight (new stops surface as "intervention config:" lines)
  - scripts/validate_intervention_masks.r (same new stops surface as FAIL config rows)
  - scripts/verify_intervention_smoke.r (AUDIT contract unchanged; consumes the extra column harmlessly)
tech-stack:
  added: []
  patterns:
    - single validated source of truth for a config-derived view
    - validate-before-mutate for by-reference data.table pipelines
    - parallel-list-attribute provenance (attr(df, "entries") + integer index column)
key-files:
  created: []
  modified:
    - src/implement_spatial_interventions.R
    - tests/testthat/test-spatial-interventions.R
decisions:
  - "D-16 honoured: all five findings in scope (CR-01, CR-03, WR-06, IN-01, IN-02) closed in this plan, none deferred"
  - "D-23 honoured: every CR/WR fix ships with a named regression test that failed against the pre-fix code"
  - "Prob_adjust_* schema validation runs for every Allocation entry regardless of the requested years, so the Stage 7 pre-flight and the standalone validator catch a malformed entry even when it is inactive at the year under test"
  - "The resolver's 8 shipped columns keep their names, order and types; entry_index is appended as a 9th column and the parsed entries ride along as an attribute, so every existing consumer is source-compatible"
  - "The removed 'unreachable defence' became a hard 'resolver contract violation:' stop rather than being deleted outright, so any future resolver/applier drift is fatal instead of silent"
metrics:
  duration: ~55 min (wall, including a mid-run session handover)
  completed: 2026-09-23
  tasks: 3
  commits: 3
  red_failures: 26
  green_file: "84 pass / 0 fail / 0 error"
  green_suite: "390 pass / 13 skip / 0 fail / 4 pre-existing error"
---

# Phase 05 Plan 07: Resolver-owned intervention identity and Prob_adjust schema Summary

`resolve_intervention_masks()` is now the single validated source of intervention identity, mask
type and `Prob_adjust_*` schema, and `implement_spatial_interventions()` iterates the resolver's
rows instead of re-parsing the same YAML into a second, independently derived view.

## What was built

**Task 1 — RED regression fixtures (commit `06b06bb`).**
Six named `test_that()` blocks pinning CR-01, CR-03, WR-06 and IN-01, plus engine-half blocks that
assert the probability table is byte-identical to a pre-call `data.table::copy()` after an abort.
The shared `.static_entry()` helper gained a minimal valid `Prob_adjust_*` field set and
`.resolver_cols` gained `entry_index`.

**RED baseline: 26 failing expectations / 58 passing / 0 errors** against the pre-fix engine.
`git diff --name-only src/` was empty at this point, as the plan required.

**Task 2 — resolver hardening (commit `d10a0a1`).**

| Finding | Before | After |
|---------|--------|-------|
| CR-01 | A missing `Intervention_ID` became a lone `NA_character_`; a single NA is not `duplicated()`, so the guard never fired and the row resolved a valid mask | NULL / non-scalar / NA / empty id stops with `Allocation entry N in <yaml> has no usable Intervention_ID` |
| IN-01 | `Static`/`Dynamic` check lived inside the per-year loop, so an entry inactive at the requested year was never checked | Hoisted into a per-entry validation pass that runs regardless of `years`; the `paste(mt, collapse=...)` form no longer crashes on a NULL value |
| CR-03 | Nothing in the `Prob_adjust_*` family was validated anywhere | Per-entry required-key set switched on `Prob_adjust_type`, scalar-ness (`missing/!scalar`), numeric coercion (`non-numeric`), `percentile outside 0-100`, and a non-empty `Transition_target_classes` — all before the year loop, therefore before any probability is mutated |
| WR-06 | Callers had to re-derive the Allocation/year view themselves | Every row carries an integer `entry_index`; every return path (including both early zero-row returns and the no-active-years return) attaches `attr(out, "entries")` |

Unchanged on purpose: the bare-filename `[/\\]` and `..` guards, the duplicate-ID message, the D-14
"no Dynamic Intervention_mask entry" stop, and the 8 existing column names/order/types. The resolver
is still base R + `yaml::` only (verified: no `:=`, no `data.table::` in the function body).

**Task 3 — applier rewrite (commit `bed6da3`).**
Deleted the second `yaml::yaml.load_file()` and the duplicated `Intervention_stage` /
`Time_steps_implemented` filters. The loop now runs `ord <- order(resolved$rank, na.last = TRUE)`
and takes `intervention`, `iv_id`, `mask_path`, `mask_name` and `rank_k` from the resolver row.
The `Intervention_ID` join and its `"No resolved mask ... - skipping intervention."` branch are gone;
a bad `entry_index` is a hard `resolver contract violation:` stop naming index, id, year and scenario.
The dead `Mask_type` check (IN-01) was removed, and `@param normalized` no longer documents the
unused `x`/`y` columns (IN-02).

## Verification

| Gate | Result |
|------|--------|
| `parse("src/implement_spatial_interventions.R")` | exits 0 |
| `parse("tests/testthat/test-spatial-interventions.R")` | exits 0 |
| `test-spatial-interventions.R` | 84 pass / 0 fail / 0 error (RED was 26 failing) |
| `test-spatial-interventions-wiring.R` | 84 pass / 0 fail / 0 error |
| Full `test_dir("tests/testthat")` | **390 pass / 13 skip / 0 fail / 4 error** |
| Baseline for comparison | 361 pass / 13 skip / 0 fail / 4 error |
| Remaining errors | the same 4 pre-existing `test-prep-paths.R` errors, unrelated to this phase |
| Shipped YAMLs over all 10 posterior years | BAU 10 rows / 1 entry, NAT 48 / 5, CUL 50 / 5, SOC 30 / 3 — all resolve, all `entry_index` round-trip back to the right `Intervention_ID` |

Frozen contracts confirmed intact by grep:

- `yaml.load_file` occurrences in the engine: **1** (was 2)
- `Current_interventions`: **0**
- `skipping intervention`: **0**
- `resolver contract violation`: **1**
- The AUDIT `sprintf()` format string `AUDIT stage=intervention region=%s scenario=%s year=%d id=%s rank=%s type=%s zone=%s to_vals=%s mask=%s rows_target=%d rows_changed=%d`: **1**, byte-identical, so `scripts/verify_intervention_smoke.r:193-228` keeps parsing it.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Two inline Dynamic test fixtures lacked the new required schema fields**
- **Found during:** Task 2 (first GREEN run)
- **Issue:** `tests/testthat/test-spatial-interventions.R` builds two Dynamic entries as explicit
  `list(...)` literals rather than via `.static_entry()`, so the plan's instruction to add schema
  defaults to `.static_entry()` did not reach them. Both tests errored with
  `Unknown Prob_adjust_type: <missing>` — the new gate working as designed, on fixtures that predate it.
- **Fix:** Added the same minimal valid field set (`Prob_adjust_type: Absolute`,
  `Prob_adjust_value: 0`, `Prob_adjust_zone: Inside`, `Transition_target_classes`) to both literals.
- **Files modified:** `tests/testthat/test-spatial-interventions.R`
- **Commit:** `d10a0a1`

**2. [Rule 2 - Missing critical functionality] Percentile range check restricted to keys the type actually requires**
- **Found during:** Task 2
- **Issue:** The review's sketch read both percentile keys unconditionally via `x[c(...)]`, which on
  an `Absolute` entry yields a zero-length numeric and silently no-ops — harmless, but it also means
  a stray out-of-range percentile on an `Absolute` entry is neither validated nor an error.
- **Fix:** The percentile check runs over `intersect(req, pct_keys)`, i.e. only the keys the entry's
  `Prob_adjust_type` actually requires, and is skipped entirely for `Absolute`. This keeps the check
  aligned with the required-key set that was just validated as present and numeric, so `any(pct < 0 | pct > 100)`
  can never see an NA.
- **Files modified:** `src/implement_spatial_interventions.R`
- **Commit:** `d10a0a1`

**3. [Rule 2 - Missing critical functionality] `Transition_target_classes` added to the resolver schema**
- **Found during:** Task 2 (specified in the plan's Task 2 action, not in the review's code sketch)
- **Issue:** The applier hard-errors on an absent or unknown target class list well after earlier
  ranks have mutated the surface — the same blast radius as CR-03.
- **Fix:** The resolver requires a present, non-empty, non-NA, non-blank
  `Transition_target_classes` for every Allocation entry. Accepts both the scalar form
  (`Transition_target_classes: mining`, used by NAT/CUL) and the list form.
- **Files modified:** `src/implement_spatial_interventions.R`
- **Commit:** `d10a0a1`

### Notes on scope

- The plan's Task 2 `<files>` lists only `src/implement_spatial_interventions.R`, but commit `d10a0a1`
  also touches the test file (deviation 1 above). Committing them together keeps every commit in this
  plan green, which matters because the RED commit `06b06bb` is deliberately red.
- No authentication gates were hit. No package was installed. No architectural (Rule 4) decision arose.

## Known Stubs

None. No hardcoded empty values, placeholders or unwired data paths were introduced.

## Threat Flags

None. The two trust boundaries in the plan's threat model (scenario YAML to resolver; YAML
`Intervention_mask` to filesystem) are unchanged in shape — this plan only tightens validation on the
first. `T-05-25` and `T-05-26` are now mitigated as planned; `T-05-27`'s existing path-traversal guards
were not touched and remain covered by the "resolver: mask names with separators or '..' are rejected
(D-05)" test.

## Follow-ups for later plans in this phase

Findings deliberately left to their own gap-closure plans (05-08..05-16), not regressions from this work:

- **CR-02** — mask geometry is never validated at runtime (`compareGeom`, `nlyr`, out-of-range cell numbers).
- **WR-01** — `>` vs `>=` boundary and the `Perc_diff == 0` dead zone in `relative_prob_adjust()`.
- **WR-07** — vacuous Absolute-0 raster assertion in `scripts/verify_intervention_smoke.r`.
- **CR-03 tail** — `"argument is of length zero"` and `"subscript out of bounds"` still missing from the
  smoke verifier's forbidden-marker list. This plan makes that message unreachable via the
  `Prob_adjust_*` path, but the marker list is a separate defence and is still short.

## Self-Check: PASSED

- `src/implement_spatial_interventions.R` — FOUND
- `tests/testthat/test-spatial-interventions.R` — FOUND
- `.planning/phases/05-integrate-spatial-interventions-branch-and-stage-interventio/05-07-SUMMARY.md` — FOUND
- Commit `06b06bb` — FOUND
- Commit `d10a0a1` — FOUND
- Commit `bed6da3` — FOUND
