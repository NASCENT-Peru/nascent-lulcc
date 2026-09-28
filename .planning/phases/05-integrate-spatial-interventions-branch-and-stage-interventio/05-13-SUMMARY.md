---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
plan: 13
subsystem: spatial-interventions
tags: [telemetry, csv, atomic-write, path-safety, pre-flight, gap-closure]
requires:
  - "`res$stats` / `.delta_stats()` 19-column per-target-class contract (Phase 5 Plan 12)"
  - "fail-closed, geometry-aware Stage 7 intervention pre-flight (Phase 5 Plan 10)"
  - "resolver-driven applier and Prob_adjust_* schema ownership (Phase 5 Plan 07)"
provides:
  - "`telemetry_dir` formal on `implement_spatial_interventions()` (last argument, NULL = fixture mode)"
  - "`intervention_prob_deltas_<scenario>_<region>_<year>.csv`: the 25-column per-intervention x per-target-class D-18 table"
  - "`.safe_path_token()` / `.region_slug()`: filename sanitisation and the region slug shared with `generate_probability_maps()`"
  - "`WARN intervention telemetry: failed to write <path>: <msg>` — the single non-fatal degradation line (D-21)"
  - "`intervention telemetry: wrote <n> rows to <path>` — the success line"
  - "`intervention telemetry: output directory not writable: <path>` — the eighth Stage 7 intervention pre-flight error line"
affects:
  - "plan 05-14: the CSV filename pattern and the 25-column order are now a published contract to assert against; the AUDIT cross-check has ONE documented asymmetry for Relative interventions (see 'Cross-checking the CSV against the AUDIT line')"
  - "plan 05-16: `intervention telemetry: output directory not writable` is a new greppable Stage 7 job-log line"
  - "tests/testthat/test-spatial-interventions-wiring.R (`.wiring_config()` gained `simulation_output_dir`)"
tech-stack:
  added: []
  patterns:
    - "stage-then-rename for tabular output, mirroring `write_raster_atomic()`"
    - "sanitise, then re-assert the sanitised result — the guard does not trust the sanitiser"
    - "non-fatal telemetry: the whole write lives in one tryCatch whose only effect on failure is a log line"
    - "ancestor walk that stops at an existing non-directory, so a regular file in the path cannot pass by finding a writable grandparent"
    - "optional config key stays absent by default in the test helper, so every pre-existing block keeps exercising the skip path"
key-files:
  created:
    - .planning/phases/05-integrate-spatial-interventions-branch-and-stage-interventio/05-13-SUMMARY.md
  modified:
    - src/implement_spatial_interventions.R
    - src/allocation.r
    - tests/testthat/test-spatial-interventions.R
    - tests/testthat/test-spatial-interventions-wiring.R
key-decisions:
  - "`.safe_path_token()` runs TWO passes, not the one the plan specified. `gsub(\"[^A-Za-z0-9_.-]\", \"_\", x)` keeps `.` in the allowed class, so `../` survives it as `.._` — the traversal the character class was supposed to remove. A second `gsub(\"\\\\.{2,}\", \"_\", x)` collapses any dot run while leaving a single dot alone, so a version-like scenario label stays readable and T-05-47 is actually closed"
  - "The composed basename is re-asserted separator- and `..`-free AFTER sanitisation. The assertion is redundant today by construction; it exists so a future edit to `.safe_path_token()` cannot silently reopen the traversal, and it raises inside the tryCatch so even that failure is non-fatal"
  - "`dir.exists(telemetry_dir)` is checked explicitly and raises a named error rather than letting `fwrite()` fail with a GDAL/libc message: the D-21 WARN line is an operator-facing diagnostic and 'telemetry_dir is not an existing directory' says what to fix. The engine never creates the directory — the run owns its own directory creation"
  - "The pre-flight ancestor walk breaks on an existing NON-directory. Walking blindly up from `<file>/runs` reaches the writable tempdir and wrongly passes, which is exactly the shape the D-21 regression test reproduces"
  - "`rank_str` is hoisted out of the AUDIT `sprintf()` args so the CSV `rank` column and the AUDIT `rank=` field render an absent ranking identically (the literal `NA`). The frozen format string literal itself is untouched"
  - "`telemetry_rows` is `NULL` in fixture mode rather than an empty list, so the per-intervention `cbind` is skipped entirely instead of building rows nobody will read"
  - "D-23 honoured: 5 errored blocks + 1 failed expectation in the engine file and 4 failed expectations in the wiring file, all against the pre-plan source. `D-21b` (telemetry_dir = NULL) and the two converse pre-flight blocks are deliberate standing guards that pass both before and after"
requirements-completed: [D-18, D-21, D-23]
duration: ~50 min
completed: 2026-09-28
---

# Phase 5 Plan 13: Per-intervention probability-delta telemetry CSV Summary

**Every allocation run now drops an `intervention_prob_deltas_<scenario>_<region>_<year>.csv` into
the region work directory — one row per intervention x target class, 25 columns, staged to a `.tmp`
path and renamed — and no failure of that write, nor any filename an operator can put in a YAML,
can cost the run.**

## Performance

- **Duration:** ~50 min
- **Tasks:** 3 of 3
- **Files modified:** 4
- **Commits:** 3 (+ this docs commit)

## The published CSV contract (for plan 05-14)

### Filename

```
intervention_prob_deltas_<scenario>_<region>_<year>.csv
```

Composed by exactly one `sprintf()` in `src/implement_spatial_interventions.R`:

```r
sprintf(
  "intervention_prob_deltas_%s_%s_%d.csv",
  .safe_path_token(scenario),
  .safe_path_token(region_slug),
  year
)
```

- `<region>` is the **slug**: `gsub(" ", "_", tolower(region_label))`. This is byte-identical to the
  `region_suffix` `generate_probability_maps()` builds (`src/allocation.r`) and to the `--region`
  value `scripts/verify_intervention_smoke.r` uses to build
  `<output_root>/<scenario>/<year>/region_<region>`. `region_label = "Costa Peruana"` produces
  `costa_peruana`.
- `<year>` is the **posterior** year (`simulation_time_step`, D-07), rendered with `%d`.
- Both string tokens pass `.safe_path_token()`: `gsub("[^A-Za-z0-9_.-]", "_", x)` followed by
  `gsub("\\.{2,}", "_", x)`.
- The file is written into `telemetry_dir`, which the hook sets to `work_dir` — the region
  directory, the parent of `probability_map_dir`. It therefore sits next to `posterior.tif`,
  `trans_rates.csv` and `probability_map_dir/`.

### Column order (25, exact)

```
scenario, region, year, intervention_id, rank, type, zone, mask, target_class, n_target,
n_changed, mean_before, mean_after, sd_before, sd_after, p05_delta, p25_delta, p50_delta,
p75_delta, p95_delta, min_delta, max_delta, sum_abs_delta, prob_mass_before, prob_mass_after
```

The 05-12 handoff was correct and was verified against the code: the trailing 17 columns are
`res$stats` with `n_inc` and `n_dec` dropped, in `res$stats` order, so the writer only prepends the
eight identifying columns. There is no reordering or renaming.

### Real fixture output

Generated by a live engine run (`scenario=BAU`, `region_label="Costa Peruana"`, `year=2028`,
rank-1 Absolute-to-0 on classes 105 and 104, rank-2 Relative Decrease on class 104), written to
`intervention_prob_deltas_BAU_costa_peruana_2028.csv`:

```csv
scenario,region,year,intervention_id,rank,type,zone,mask,target_class,n_target,n_changed,mean_before,mean_after,sd_before,sd_after,p05_delta,p25_delta,p50_delta,p75_delta,p95_delta,min_delta,max_delta,sum_abs_delta,prob_mass_before,prob_mass_after
BAU,costa_peruana,2028,iv_abs,1,Absolute,Inside,mask_a.tif,105,3,3,0.4,0,0.2,0,-0.58,-0.5,-0.4,-0.3,-0.22,-0.6,-0.2,1.2,1.2,0
BAU,costa_peruana,2028,iv_abs,1,Absolute,Inside,mask_a.tif,104,3,3,0.14,0,0.04,0,-0.176,-0.16,-0.14,-0.12,-0.104,-0.18,-0.1,0.42,0.42,0
BAU,costa_peruana,2028,iv_rel,2,Relative,Inside,mask_a.tif,104,0,0,,,,,,,,,,,,0,,
```

Value shapes a downstream parser can rely on:

- `rank` is the bare ranking (`1`, `2`) or the literal `NA` when the entry has no
  `Intervention_ranking` — the same rendering as the AUDIT `rank=` field.
- `type` is `Prob_adjust_type`, `zone` is `Prob_adjust_zone`, `mask` is the **bare mask filename**
  (`resolved$mask_name`, not the path) — identical to the AUDIT `mask=` field.
- `target_class` is the integer `to_val`, not a class name.
- A **skipped** target class contributes a row with `n_target = 0`, `n_changed = 0`,
  `sum_abs_delta = 0` and every other statistic **empty** (`data.table::fwrite()`'s default
  `na = ""`). `read.csv()` and `fread()` both read those as `NA` on the numeric columns.
- Row order is **rank order** — the order the engine applies interventions — and within an
  intervention, `Target_classes` order.
- `scenario` and `region` carry the sanitised-for-path-safety-free ORIGINAL scenario string and the
  region **slug** respectively; only the filename tokens are sanitised, the `scenario` column is the
  configured value verbatim.

### Cross-checking the CSV against the AUDIT line (read this before writing 05-14's assertions)

The matching AUDIT lines for the run above:

```
AUDIT stage=intervention region=Costa Peruana scenario=BAU year=2028 id=iv_abs rank=1 type=Absolute zone=Inside to_vals=105,104 mask=mask_a.tif rows_target=6 rows_changed=6 delta_mean=-0.27 delta_med=-0.19 delta_sd=0.192146 delta_min=-0.6 delta_max=-0.1 n_inc=0 n_dec=6 sum_abs_delta=1.62
AUDIT stage=intervention region=Costa Peruana scenario=BAU year=2028 id=iv_rel rank=2 type=Relative zone=Inside to_vals=104 mask=mask_a.tif rows_target=6 rows_changed=0 delta_mean=NA delta_med=NA delta_sd=NA delta_min=NA delta_max=NA n_inc=0 n_dec=0 sum_abs_delta=0
```

Three reconciliations hold **unconditionally** and are asserted by `D-18b`:

| AUDIT field | CSV expression |
|-------------|----------------|
| `sum_abs_delta` | `sum(sum_abs_delta)` over that `intervention_id`'s rows |
| `rows_changed` | `sum(n_changed)` over that `intervention_id`'s rows |
| `rows_target` | `sum(n_target)` over that `intervention_id`'s rows — **Absolute interventions only** |

**The `rows_target` asymmetry.** `relative_prob_adjust()` does
`rows_target <- rows_target + length(sub_idx)` **before** its three `next` paths, so a class that is
skipped (empty zone / NaN guard) still contributes to the AUDIT `rows_target`, while its stats row
correctly reports `n_target = 0`. That is exactly the `iv_rel` line above: `rows_target=6`,
CSV `n_target=0`. Both numbers are correct under their own documented definitions —
`rows_target` counts the rows of the target classes, `n_target` counts the rows a statistic was
actually computed over. This is pre-existing behaviour from plan 05-12 and earlier; the AUDIT prefix
through `rows_changed` is frozen, so it was NOT changed here. **Plan 05-14 must scope any
`rows_target == sum(n_target)` assertion to Absolute interventions, or to rows with `n_target > 0`.**

The `region` values also differ by design: the AUDIT `region=` field keeps `region_label` verbatim
(`Costa Peruana`), the CSV `region` column is the slug (`costa_peruana`).

### Log lines introduced

| Line | `sprintf()` format | Fires when |
|------|--------------------|------------|
| success | `intervention telemetry: wrote %d rows to %s` | the rename succeeded |
| failure | `WARN intervention telemetry: failed to write %s: %s` | anything in the write path raised |
| pre-flight | `intervention telemetry: output directory not writable: %s` | `simulation_output_dir` has no writable ancestor directory |

The pre-flight line carries the `intervention ` prefix the consolidated gap list keys on
(`^intervention `), joining the seven lines plan 05-10 recorded. It is line **eight**.

**Action for 05-14/05-16:** `WARN intervention telemetry:` is a new warning marker and
`intervention telemetry: output directory not writable` is a new pre-flight marker; neither is in
`scripts/verify_intervention_smoke.r` today.

## Tasks completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | RED tests for the CSV contract, non-fatal failure and filename safety (D-23) | `7136f0e` | `tests/testthat/test-spatial-interventions.R` |
| 2 | Write the telemetry CSV from the engine, non-fatally | `2d4452c` | `src/implement_spatial_interventions.R` |
| 3 | Wire `telemetry_dir` at the hook and report an unwritable output root at pre-flight | `bede638` | `src/allocation.r`, `tests/testthat/test-spatial-interventions-wiring.R` |

### Task 1 — RED (commit `7136f0e`)

`.run_engine()` gained `telemetry_dir = NULL` and `region_label = "R1"`, and **only forwards
`telemetry_dir` when the caller supplies it** (`do.call()` over a built argument list), so the
~30 pre-05-13 blocks keep calling the engine with its original formals.

| Block | What it pins |
|-------|--------------|
| `D-18: the telemetry CSV is written with the exact 25-column contract` | the file name `intervention_prob_deltas_BAU_r1_2028.csv`, `names()` identical to the 25-column vector in order, no surviving `.tmp`, the constant-column values, and the `wrote 4 rows to ` success line |
| `D-18b: one row per intervention x target class` | `nrow == 4`, `anyDuplicated(intervention_id x target_class) == 0`, and the three AUDIT reconciliations above |
| `D-18c: Absolute-to-0 rows report mean_after = 0` | `mean_after` AND `prob_mass_after` are 0 for every adjusted row; a skipped row is `NA`, not a misleading 0 |
| `D-21: a failing CSV write warns and does not abort the run` | `expect_no_error()`, the returned table still carries the zeroed inside rows and untouched outside rows, exactly one `WARN intervention telemetry:` line, no CSV anywhere under the scratch, and the summary AUDIT line still written |
| `D-21b: telemetry_dir = NULL writes nothing and does not warn` | fixture mode stays silent (standing guard) |
| `D-18d: the filename is sanitised (T-05-47)` | `region_label = "Costa Peruana"` -> `costa_peruana`; a hostile `scenario = "masks/../EVIL"` still runs the engine but the written name is its own `basename()`, with no `/`, `\` or `..` |

The hostile-scenario fixture is the interesting one: `masks/../EVIL` resolves to the same YAML the
fixture wrote (the fixture's `masks/` subdirectory exists), so the engine genuinely runs end to end
rather than failing early on a missing YAML — which is the only way the filename composition is
actually exercised. It lands as
`intervention_prob_deltas_masks___EVIL_r1_2028.csv`.

**RED baseline:** `FAIL 1 | ERROR 5 | PASS 186` in `test-spatial-interventions.R`, with `src/`
untouched (`git diff --name-only src/` empty at that commit). Five of the six new blocks abort on
`unused argument (telemetry_dir = ...)`; `D-21` additionally records a failed expectation because
`expect_no_error()` catches that error before the block dies on the next line.

### Task 2 — the writer (commit `2d4452c`)

`telemetry_dir = NULL` appended at the END of the formals list, documented in roxygen plus a new
`@section Probability-delta telemetry CSV:` block that states the 25 columns, the slug rule, the
atomic-rename rule and the non-fatal rule.

Inside the intervention loop, after the AUDIT line is written and `attr(iv_stats, "delta")` is
dropped:

```r
per_class <- iv_stats[, setdiff(names(iv_stats), c("n_inc", "n_dec")), drop = FALSE]
telemetry_rows[[length(telemetry_rows) + 1L]] <- cbind(
  data.frame(scenario = ..., region = region_slug, year = year,
             intervention_id = ..., rank = rank_str, type = ..., zone = ...,
             mask = ..., stringsAsFactors = FALSE),
  per_class, stringsAsFactors = FALSE
)
```

`cbind()` recycles the one-row identity frame across every class row. Accumulation is in loop order,
which is rank order.

After `write_summary()`, the whole write is one `tryCatch`: compose -> assert the basename ->
assert `dir.exists(telemetry_dir)` -> `data.table::fwrite()` to `<path>.tmp` -> `file.rename()` ->
success line. Any raise inside it `unlink()`s the `.tmp` (when one was named) and logs the single
`WARN intervention telemetry:` line. **`normalized` is returned on every path.**

**D-20 and D-13 still hold.** The telemetry holds one small data.frame per intervention with
`length(Target_classes)` rows; nothing here touches the probability table. `copy(normalized`,
`data.table::copy(`, `resample(`, `project(` and `cat(` are all still 0 occurrences in the file.

### Task 3 — hook and pre-flight (commit `bede638`)

`generate_probability_maps()` passes `telemetry_dir = work_dir` as the last argument of the
`implement_spatial_interventions()` call, and the comment above the call now states where the CSV
lands and that the write is non-fatal. The argument is written with a single space rather than the
call site's `=` alignment because the plan's acceptance grep is the fixed string
`telemetry_dir = work_dir`.

The pre-flight gained a sibling `if (interventions_configured)` block (placed between the WR-04
engine check and the `engine_loaded` resolution body, so neither is re-indented). It walks from
`config[["simulation_output_dir"]]` to the nearest existing ancestor and tests
`file.access(dir, 2) == 0`:

```r
while (!dir.exists(probe) && guard < 64L) {
  if (file.exists(probe)) { blocked <- TRUE; break }   # existing NON-directory
  parent <- dirname(probe); if (identical(parent, probe)) break
  probe <- parent; guard <- guard + 1L
}
writable <- !blocked && dir.exists(probe) && isTRUE(unname(file.access(probe, 2L) == 0L))
```

The `blocked` break is load-bearing: without it, `<regular-file>/runs` walks straight past the file
to the writable tempdir and the check passes. The `guard` counter bounds the walk. **Nothing is
created** — the run makes its own `<scenario>/<year>/region_<region>` directories, so a
not-yet-created root under a writable parent is deliberately NOT a gap. The block is skipped
silently when the key is absent, which is why fixture mode and every pre-existing wiring block still
emit zero intervention lines.

New wiring blocks: `D-18: the hook passes telemetry_dir = work_dir` (static text, plus assertions
that `work_dir` really is the region directory and that `region_suffix` is the slug the filename
uses), `D-21: pre-flight reports an unwritable simulation output root`, `D-21: a writable
simulation output root adds no telemetry line`, and `D-21: an output root that does not exist yet is
judged by its nearest ancestor` (which also asserts the pre-flight created nothing).

**RED baseline for Task 3:** `FAIL 4 | PASS 125` against the pre-fix `src/allocation.r`
(1 in the hook block, 3 in the unwritable-root block). The two converse blocks are standing guards
and pass either way.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing critical security functionality] The specified sanitiser does not remove `..`**

- **Found during:** Task 2, while writing `.safe_path_token()` against the `D-18d` fixture.
- **Issue:** the plan specifies `gsub("[^A-Za-z0-9_.-]", "_", x)` as the whole sanitisation, and the
  threat register (T-05-47) relies on it. But `.` is inside the allowed character class, so
  `"../evil"` becomes `".._evil"` — the `..` traversal component survives untouched. The plan's own
  follow-up assertion ("the basename contains no `/`, `\` or `..`") would then have raised on every
  hostile input, i.e. the mitigation would have degraded to "never write the file" rather than
  "write it safely", and `D-18d`'s requirement that a file IS written could not have been met.
- **Fix:** `.safe_path_token()` applies the specified substitution and then
  `gsub("\\.{2,}", "_", x)`, collapsing any run of two or more dots. A single dot survives, so a
  version-like scenario label (`SSP2.6`) stays readable. `masks/../EVIL` -> `masks_.._EVIL` ->
  `masks___EVIL`. The post-composition assertion is retained as a guard against a future edit to the
  sanitiser, not as the primary defence.
- **Files modified:** `src/implement_spatial_interventions.R`
- **Commit:** `2d4452c`

**2. [Rule 2 - Missing critical functionality] The pre-flight ancestor walk must stop at an existing non-directory**

- **Found during:** Task 3, running the `D-21` unwritable-root block.
- **Issue:** the plan says "find the nearest existing ancestor directory and test it with
  `file.access(<dir>, 2) == 0`". Taken literally, the walk from `<tmp>/blocker/runs` (where
  `<tmp>/blocker` is a regular FILE) skips `<tmp>/blocker` because `dir.exists()` is FALSE, reaches
  the writable `<tmp>`, and reports no gap — while the actual run would fail to create anything
  under a regular file. The check would have passed exactly the deployment shape it exists to catch.
- **Fix:** the walk breaks with `blocked <- TRUE` when `file.exists(probe)` is TRUE but
  `dir.exists(probe)` is FALSE.
- **Files modified:** `src/allocation.r`
- **Commit:** `bede638`

**3. [Rule 2 - Missing critical functionality] `dir.exists(telemetry_dir)` checked explicitly**

- **Found during:** Task 2.
- **Issue:** with a non-existent `telemetry_dir`, `fwrite()` raises a low-level message that names
  the temp path and a libc error, which is a poor operator-facing WARN line for the one diagnostic
  that will be read hours after the fact.
- **Fix:** an explicit `dir.exists()` check raising
  `telemetry_dir is not an existing directory: <path>` inside the same `tryCatch`, so the failure is
  still non-fatal but the WARN line says what to fix. The engine still never creates the directory.
- **Files modified:** `src/implement_spatial_interventions.R`
- **Commit:** `2d4452c`

### Deliberate departures from the plan text

**The acceptance criterion "at least 8 failing expectations" for Task 1 was not reachable as
written, and is reported as 5 errored blocks + 1 failed expectation instead.** Five of the six new
blocks abort at their FIRST engine call with `unused argument (telemetry_dir = ...)`, because the
formal does not exist on the pre-plan engine. testthat records an aborted block as one ERROR and
stops counting its remaining expectations, so the expectation counter cannot reach 8 no matter how
many assertions the blocks contain. The RED signal is nonetheless unambiguous: **6 of the 7 new
blocks are red against the pre-plan source** (`D-21b` is a deliberate standing guard), and
`git diff --name-only src/` was empty at commit `7136f0e`. Block identities at RED:
`D-18` (52), `D-18b` (53), `D-18c` (54), `D-21` (55, 1 failure + error), `D-18d` (57).

**`rank_str` hoisted out of the AUDIT `sprintf()` arguments.** The plan asks for the CSV `rank` to
be "rendered the same way as in the AUDIT line". Duplicating
`if (is.na(rank_k)) "NA" else as.character(rank_k)` in two places is how the two renderings drift.
The expression is computed once next to `rank_k` and used by both. The frozen AUDIT **format string
literal** is byte-identical; only an argument expression changed, so both 05-12 greps still return 1.

### Architectural changes

None. No new dependency (`data.table` is already declared and already used in this file), no new
file, no package-manager invocation, no schema change.

## Threat register status

| Threat ID | Disposition | Status |
|-----------|-------------|--------|
| T-05-47 (traversal via scenario/region in the filename) | mitigate | **mitigated** — two-pass `.safe_path_token()` + post-composition assertion; regression test `D-18d` |
| T-05-48 (unwritable/malformed telemetry path aborting a multi-hour run) | mitigate | **mitigated** — single `tryCatch` degrading to `WARN intervention telemetry:`; pre-flight line for the output root; regression tests `D-21` (engine) and `D-21: pre-flight reports an unwritable simulation output root` (wiring) |
| T-05-49 (partially written CSV read as complete) | mitigate | **mitigated** — `fwrite()` to `<path>.tmp` then `file.rename()`, `.tmp` unlinked on the failure path; `D-18` asserts no `.tmp` survives a success |
| T-05-50 (clobbering an existing run's telemetry) | accept | **accepted as planned** — the name is keyed on scenario x region x year inside the region work directory, so a re-run overwrites its own output exactly as `posterior.tif` does |
| T-05-SC (package-manager installs) | n/a | no install command in any task |

## Verification

| Check | Result |
|-------|--------|
| `parse("src/implement_spatial_interventions.R")` | exits 0 |
| `parse("src/allocation.r")` | exits 0 |
| `parse("tests/testthat/test-spatial-interventions.R")` | exits 0 |
| `test_file("tests/testthat/test-spatial-interventions.R")` | `FAIL 0 / ERROR 0 / PASS 226` (RED peak `FAIL 1 / ERROR 5 / PASS 186`) |
| `test_file("tests/testthat/test-spatial-interventions-wiring.R")` | `FAIL 0 / ERROR 0 / PASS 129` (RED `FAIL 4 / PASS 125`) |
| `test_dir("tests/testthat")` | **PASS 623 / FAIL 0 / ERROR 4 / SKIP 13** |
| Failing-test identities | only `test-prep-paths.R` blocks at lines **26/34/42/55**, raising at **27/35/43/56** — the pre-existing `sys.frame(1)$ofile` harness defect, untouched and out of scope |
| `grep -c "intervention_prob_deltas_%s_%s_%d.csv" src/implement_spatial_interventions.R` | 1 |
| `grep -c "file.rename" src/implement_spatial_interventions.R` | 1 (>= 1 required) |
| `grep -c "WARN intervention telemetry:" src/implement_spatial_interventions.R` | 1 |
| `grep -c "cat(" src/implement_spatial_interventions.R` | 0 |
| `grep -c "copy(normalized" / "data.table::copy"` | 0 / 0 (D-20 holds) |
| `grep -c "resample(\|project("` in both engine and `src/allocation.r` | 0 / 0 (D-13 holds) |
| `grep -c "telemetry_dir = work_dir" src/allocation.r` | 1 |
| `grep -c "intervention telemetry: output directory not writable" src/allocation.r` | 1 |
| `grep -c "simulation_time_step = year_post" src/allocation.r` | 1 (D-07 untouched) |
| `grep -c 'ref_grid_path *= *config\[\["ref_grid_path"\]\]' src/allocation.r` | 1 (CR-02 untouched) |
| `git diff --diff-filter=D --name-only e4a4282 HEAD` | empty — no file deleted |

Baseline reconciliation: the brief's verified baseline at `e4a4282` is
`PASS 568 | FAIL 4 | WARN 6 | SKIP 13`, where the 4 "FAIL" are the `test-prep-paths.R` errors
(`test_dir(reporter = "silent")` reports them as `ERROR 4 / FAIL 0`). This run is
`PASS 623 | FAIL 0 | ERROR 4 | SKIP 13`: +55 passing expectations from this plan's new blocks, same
four errors, same four line numbers, **no new failure of any kind**.

## Known Stubs

None. No placeholder value, hardcoded-empty data source or TODO marker was introduced. The empty
fields in a skipped-class CSV row are `NA` statistics for a class that genuinely had no measured
rows, which is the documented `.delta_stats_skipped()` contract from plan 05-12, not a stub.

## Threat Flags

None. The plan introduces one new file-write path, which is inside the threat register above
(T-05-47 / T-05-49) and inside the region work directory the run already owns. No new network
endpoint, no auth path, no trust-boundary schema change.

## Out of scope / deferred

- **`scripts/verify_intervention_smoke.r` was not touched.** D-22 (the verifier's own assertions
  over the CSV) is plan 05-14's scope. The two new markers it will need are recorded above.
- **The `rows_target` vs `sum(n_target)` asymmetry for Relative interventions** is pre-existing and
  the AUDIT prefix through `rows_changed` is frozen, so it was documented rather than changed. See
  "Cross-checking the CSV against the AUDIT line".
- **The 4 `test-prep-paths.R` errors** were left alone per the execution brief.
- **`STATE.md`, `ROADMAP.md` and `REQUIREMENTS.md` were not modified** — the orchestrator owns those
  writes after the wave merges. `.planning/HANDOFF.json` carries a timestamp-only change written by
  the session hook and was deliberately left uncommitted.

## Self-Check: PASSED

Files claimed:

- `src/implement_spatial_interventions.R` — FOUND
- `src/allocation.r` — FOUND
- `tests/testthat/test-spatial-interventions.R` — FOUND
- `tests/testthat/test-spatial-interventions-wiring.R` — FOUND
- `.planning/phases/05-integrate-spatial-interventions-branch-and-stage-interventio/05-13-SUMMARY.md` — FOUND

Commits claimed: `7136f0e`, `2d4452c`, `bede638` — all present in
`git log e4a4282..HEAD` on `worktree-agent-a01ea4c23ece7d015`.
Diff scope vs base: 4 files, +479 / -7, zero deletions.
No untracked files left behind; all test artefacts are written to `withr::local_tempdir()`.
