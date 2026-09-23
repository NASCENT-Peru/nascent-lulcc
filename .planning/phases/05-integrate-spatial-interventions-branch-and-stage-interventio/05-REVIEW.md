---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
reviewed: 2026-09-23T11:05:00Z
depth: deep
diff_base: main...HEAD (7f8e68b)
files_reviewed: 12
files_reviewed_list:
  - src/implement_spatial_interventions.R
  - src/allocation.r
  - scripts/run_allocation.r
  - scripts/validate_intervention_masks.r
  - scripts/verify_intervention_smoke.r
  - config/BAU_interventions.yml
  - config/NAT_interventions.yml
  - config/CUL_interventions.yml
  - config/SOC_interventions.yml
  - config/hpc_config.yaml
  - tests/testthat/test-spatial-interventions.R
  - tests/testthat/test-spatial-interventions-wiring.R
findings:
  critical: 3
  warning: 11
  info: 7
  total: 21
status: issues_found
---

# Phase 5: Code Review Report

**Reviewed:** 2026-09-23T11:05:00Z
**Depth:** deep (cross-file: engine -> allocation hook -> pre-flight -> scripts -> configs -> tests)
**Files Reviewed:** 12
**Status:** issues_found

## Summary

The Phase 5 intervention engine is structurally sound: the resolver/applier split is a
real improvement over the retired `lulcc.spatprobmanipulation.r`, path traversal is
correctly blocked at the YAML boundary, the NaN guard works, the AUDIT contract is
coherent, and `year_post` is threaded correctly through the single
`generate_probability_maps()` call site. The HPC run proves the happy path.

The defects that matter are all in the *unhappy* paths, and they share one shape:
**the engine fails silently or too late rather than loudly and early.** Three of them
are blockers because each can produce a scientifically wrong scenario output (or a
dead multi-hour Stage 7 worker) while every gate in the phase — resolver,
`validate_allocation_runtime()` pre-flight, and `validate_intervention_masks.r` —
reports success.

All three blockers and the material warnings below were **reproduced empirically**
against the real engine (R 4.5.0, terra, data.table, yaml) using synthetic fixtures;
they are not speculative reads of the code.

Specific structural weaknesses feeding the blockers:

1. `implement_spatial_interventions()` parses the YAML and re-derives the
   Allocation/year filter a **second** time (lines 234-249) instead of consuming the
   resolver's output, then re-joins the two independent views by `Intervention_ID`.
   Any field that makes the two views disagree produces a silent skip.
2. The **only** geometry validation for masks lives in a standalone script that is
   wired into no pipeline, no submit script and no pre-flight. The runtime path
   (`.mask_inside_lut()`) trusts raw cell numbers with no `compareGeom()` and no
   `nlyr()` check.
3. The pre-flight validates mask *existence* only. The `Prob_adjust_*` field set —
   whose absence is an unconditional hard error deep inside the run — is never
   validated anywhere.

## Critical Issues

### CR-01: An intervention with a missing `Intervention_ID` is silently skipped — every gate still reports PASS

**File:** `src/implement_spatial_interventions.R:287`, `src/implement_spatial_interventions.R:306-315`, `src/implement_spatial_interventions.R:48-58`

**Issue:** The resolver and the applier derive the intervention identity independently
and then join on it:

- Resolver (line 50): a missing `Intervention_ID` becomes `NA_character_`. A *single*
  NA is not `duplicated()`, so the duplicate-ID guard at lines 52-58 does not fire.
  The row is emitted with a valid `mask_path` and `exists = TRUE`.
- Applier (line 287): `iv_id <- as.character(intervention[["Intervention_ID"]])` on a
  missing key yields `character(0)`.
- Join (line 306): `resolved$intervention_id == character(0)` yields `logical(0)`, so
  `mask_row` has 0 rows and control falls into the branch at 307-315 — which the
  comment explicitly calls an *"Unreachable defence"*. It is reachable.

Reproduced against the live engine:

```
Found 1 allocation stage interventions for scenario NOID at time step 2028
Applying intervention:
No resolved mask for intervention  year 2028 - skipping intervention.
AUDIT stage=intervention_summary ... n_interventions=0 cells_sum_gt1=0
probs changed? FALSE
```

Blast radius: the Stage 7 pre-flight passes (the mask exists), the standalone
validator passes (it only consumes the resolver), the run completes, posterior rasters
are written, and the scenario's spatial policy is simply absent. Only
`verify_intervention_smoke.r` would notice, and only if someone runs it for that
scenario/region/year. Every field in the `Intervention_ID` chain is user-authored
YAML, and a typo in the key (`Intervention_Id`, `intervention_id`) triggers exactly
this path.

Note the same mechanism also silences the AUDIT line: `sprintf()` with any
zero-length argument returns `character(0)`, so even the per-intervention AUDIT record
would vanish rather than log a placeholder.

**Fix:** Make identity a validated requirement in the resolver and remove the
duplicate derivation in the applier.

```r
# resolve_intervention_masks(), replacing lines 48-58
ids <- vapply(entries, function(x) {
  v <- x[["Intervention_ID"]]
  if (is.null(v) || length(v) != 1L) NA_character_ else as.character(v)
}, character(1))
bad <- which(is.na(ids) | !nzchar(ids))
if (length(bad) > 0L) {
  stop(sprintf(
    "Allocation entry %s in %s has no usable Intervention_ID",
    paste(bad, collapse = ", "), yaml_path
  ))
}
dup <- unique(ids[duplicated(ids)])
if (length(dup) > 0L) stop(...)   # unchanged
```

and in `implement_spatial_interventions()` replace the "unreachable defence" `next`
with a hard stop, so any future drift between the two views is fatal rather than
silent:

```r
mask_row <- resolved[!is.na(resolved$intervention_id) &
                     resolved$intervention_id == iv_id, , drop = FALSE]
if (nrow(mask_row) != 1L) {
  stop(sprintf(
    "resolver/applier disagreement: %d resolved mask rows for id='%s' year=%d scenario=%s",
    nrow(mask_row), paste(iv_id, collapse = ""), year, scenario
  ), call. = FALSE)
}
```

Ideally, drop lines 234-249 entirely and drive the loop from `resolved` joined back to
the parsed entries (see WR-06).

---

### CR-02: Mask geometry is never validated at runtime — a mis-gridded mask silently applies the intervention to the wrong cells

**File:** `src/implement_spatial_interventions.R:130-141` (`.mask_inside_lut`), `src/allocation.r:2952`, `src/allocation.r:443-471` (pre-flight)

**Issue:** `.mask_inside_lut()` samples the mask with **raw national cell numbers**
(`terra::extract(m, cell_index$ref_cell_id)`, line 136). Cell numbers are only
meaningful on the exact grid that produced them — here `config$ref_grid_path` via
`terra::cellFromXY()` at `src/allocation.r:2648-2650`. The engine never calls
`terra::compareGeom()`, never checks CRS/extent/resolution, and never checks
`terra::nlyr(m) == 1` (it blindly takes `[[1L]]`, i.e. layer 1 of a multi-band file).

Two failure modes, both reproduced:

*(a) Same dimensions, different CRS/extent (shifted grid):* the LUT is built from the
wrong geography with **no warning at all**. The intervention reports complete success:

```
AUDIT stage=intervention region=R1 scenario=SHIFT year=2028 id=iv_shift rank=1
  type=Absolute zone=Inside to_vals=105 mask=mask_shift.tif rows_target=3 rows_changed=3
```
The applied cell set was identical to the correctly-aligned mask's cell set — i.e. the
intervention geography was entirely fictional and indistinguishable from a correct run
in the log.

*(b) Smaller extent (cell numbers out of range):* `terra::extract()` returns `NA` and
emits an **R warning, not an error**. Line 138 (`!is.na(v) & v == 1`) coerces those NAs
to "outside", so the intervention applies to a wrong, truncated subset:

```
Warning message: [extract] out of range cell numbers detected
AUDIT ... mask=mask_small.tif rows_target=1 rows_changed=1
```
The warning goes to `stderr`, not to `log_file`, so it is invisible in the per-region
worker log the smoke verifier scans.

The `validate_intervention_masks.r` script *does* implement the right check
(`compareGeom` + `nlyr == 1`, lines 236-249), but `grep -rn validate_intervention_masks
--include=*.sh --include=*.r` shows it is referenced **only by its own usage string** —
it is not called by `master_pipeline.sh`, any `submit_*.sh`, `run_allocation.r`, or the
pre-flight. It is a manual, forgettable gate protecting a silent-corruption path.

**Fix (two parts).** First, make the runtime self-validating — this is cheap
(once per mask per call, behind the existing cache):

```r
.mask_inside_lut <- function(mask_path, cell_index, cache, ref_grid = NULL) {
  key <- normalizePath(mask_path, mustWork = TRUE)
  if (exists(key, envir = cache, inherits = FALSE)) {
    return(get(key, envir = cache, inherits = FALSE))
  }
  m <- terra::rast(mask_path)
  if (terra::nlyr(m) != 1L) {
    stop(sprintf("intervention mask %s has %d layers (expected 1)",
                 mask_path, terra::nlyr(m)), call. = FALSE)
  }
  if (!is.null(ref_grid) && !isTRUE(terra::compareGeom(m, ref_grid, stopOnError = FALSE))) {
    stop(sprintf(
      "intervention mask %s is not on the reference grid (crs/res/extent mismatch); cell-number lookup would be silently wrong",
      mask_path
    ), call. = FALSE)
  }
  v <- terra::extract(m, cell_index$ref_cell_id)[[1L]]
  ...
}
```

Thread `ref_grid <- terra::rast(config[["ref_grid_path"]])` (already loaded at
`src/allocation.r:2647`) into `implement_spatial_interventions()` and down to
`.mask_inside_lut()`.

Second, add the same `compareGeom`/`nlyr` assertion to the D-14 pre-flight block in
`validate_allocation_runtime()` (it already resolves every mask path and the config
already carries `ref_grid_path`), so a mis-staged mask is rejected before any work
starts rather than at hour 3 of a region run — or, at minimum, invoke
`validate_intervention_masks.r` from the Stage 7 submit path.

---

### CR-03: Missing/invalid `Prob_adjust_*` fields are unvalidated — hard crash mid-run after partial mutation

**File:** `src/implement_spatial_interventions.R:672`, `:681`, `:697`, `:707`, `:720` (and `:360`, `:370-376`), `src/allocation.r:443-471`

**Issue:** `relative_prob_adjust()` reads `Prob_adjust_threshold` with
`as.numeric(intervention[["Prob_adjust_threshold"]])`. A missing key yields
`numeric(0)`, and `if (abs(Perc_diff) < numeric(0))` raises
`"argument is of length zero"`. Reproduced end-to-end:

```
Applying intervention: iv
The Percentage difference in average probability above the 0.5 and 0.5 percentiles ... is : 22.22
ERROR: argument is of length zero
```

Three things make this a blocker rather than a nuisance:

1. **It fires after partial mutation.** Interventions are applied in rank order and
   all helpers write to `normalized` **by reference** (`normalized[ix, prob := ...]`).
   A rank-3 intervention with a typo'd threshold key kills the worker *after* ranks 1-2
   have already rewritten the probability surface. There is no transaction boundary and
   no `tryCatch` around the hook at `src/allocation.r:2950-2961`.
2. **No gate catches it.** The D-14 pre-flight (`src/allocation.r:443-471`) checks mask
   *existence* only. The resolver validates `Intervention_stage`, `Mask_type`,
   `Time_steps_implemented` and the mask name, but nothing in the `Prob_adjust_*`
   family. Same exposure for `Prob_adjust_value` (line 360 — a non-numeric YAML value
   becomes `NA` with only a warning and writes `NA` probabilities), for
   `Prob_adjust_intervention_percentile` / `_non_intervention_percentile` (lines
   370-376 — out-of-range values make `quantile()` throw, absent values yield
   `numeric(0)` and get swallowed by the NaN guard as a *silent skip*), and for
   `Prob_adjust_valency`/`_zone` on Absolute entries.
3. **The smoke verifier does not catch it either.** The forbidden-marker list
   (`scripts/verify_intervention_smoke.r:332-336`) contains
   `"missing value where TRUE/FALSE needed"` but **not** its sibling
   `"argument is of length zero"`, which is the exact message this defect produces.

The four shipped YAMLs happen to be complete today (verified: every `Relative` entry
has all five fields, every `Absolute` entry has `Prob_adjust_value` + zone), so this is
latent — but these files are the primary human-edited surface of the whole phase.

**Fix:** Add a schema check to the resolver (it already reads the YAML and is shared by
the pre-flight, the validator and the applier, so one change closes all three gates):

```r
# inside resolve_intervention_masks(), per entry, before the year loop
adj <- as.character(x[["Prob_adjust_type"]])
req <- switch(
  adj,
  Absolute = c("Prob_adjust_value", "Prob_adjust_zone"),
  Relative = c("Prob_adjust_valency", "Prob_adjust_zone", "Prob_adjust_threshold",
               "Prob_adjust_intervention_percentile",
               "Prob_adjust_non_intervention_percentile"),
  stop(sprintf("Unknown Prob_adjust_type: %s (scenario=%s id=%s)", adj, scenario, id))
)
missing_keys <- req[vapply(req, function(k) length(x[[k]]) != 1L, logical(1))]
if (length(missing_keys) > 0L) {
  stop(sprintf("scenario=%s id=%s missing/!scalar: %s",
               scenario, id, paste(missing_keys, collapse = ", ")))
}
num_keys <- intersect(req, c("Prob_adjust_value", "Prob_adjust_threshold",
                             "Prob_adjust_intervention_percentile",
                             "Prob_adjust_non_intervention_percentile"))
bad_num <- num_keys[vapply(num_keys,
  function(k) is.na(suppressWarnings(as.numeric(x[[k]]))), logical(1))]
if (length(bad_num) > 0L) {
  stop(sprintf("scenario=%s id=%s non-numeric: %s",
               scenario, id, paste(bad_num, collapse = ", ")))
}
pct <- suppressWarnings(as.numeric(unlist(x[c("Prob_adjust_intervention_percentile",
                                              "Prob_adjust_non_intervention_percentile")])))
if (length(pct) > 0L && any(pct < 0 | pct > 100)) {
  stop(sprintf("scenario=%s id=%s percentile outside 0-100", scenario, id))
}
```

Also add `"argument is of length zero"` and `"subscript out of bounds"` to
`scripts/verify_intervention_smoke.r:332-336`.

## Warnings

### WR-01: `>` vs `>=` and the `Perc_diff == 0` dead zone let a Relative intervention no-op while AUDIT reports it applied

**File:** `src/implement_spatial_interventions.R:618-627` vs `:678`, `:688`, `:703`, `:713`, `:727`, `:732`; `:671-691`, `:696-716`

**Issue:** Two related off-by-a-comparison defects in `relative_prob_adjust()`.

*(a) Inconsistent boundary.* The percentile **mean** is computed with `>=`
(lines 619, 624) but the **rows to adjust** are selected with strict `>`
(lines 678, 688, 703, 713, 727, 732). When probabilities tie at the percentile —
which is exactly what happens after a higher-ranked `Absolute` intervention flattens a
zone to a constant, and after the upstream `tot_prob` clamp at
`src/allocation.r:2940-2941` — `>` can select **zero** rows while the mean was taken
over many. The intervention then does nothing but still emits
`AUDIT ... rows_target=N rows_changed=0` and counts toward `n_interventions`.

*(b) Exact-zero dead zone.* `Increase` and `Decrease` branch on
`if (Perc_diff > 0) ... else if (Perc_diff < 0)`. At `Perc_diff == 0` neither branch
runs, so the configured `Prob_adjust_threshold` — whose entire purpose is to guarantee
a minimum nudge when the zones are indistinguishable — is silently not applied, and no
"skip" line is logged. Reproduced:

```
The Percentage difference ... for 104 is : 0
AUDIT stage=intervention ... id=iv_eq ... rows_target=6 rows_changed=0
AUDIT stage=intervention_summary ... n_interventions=1 cells_sum_gt1=0
```

Note `Increase_inside_decrease_outside` (lines 717-734) *does* apply the threshold at
zero, so the three valencies behave inconsistently at the boundary. All four shipped
scenarios rely on `Prob_adjust_threshold: 5` doing exactly this job.

**Fix:** Restructure the sign dispatch so zero is handled, and align the comparison:

```r
if (Perc_diff == 0) {
  threshold_msg(lulc_class, Prob_adjust_threshold)
  Perc_diff <- if (Prob_adjust_valency == "Decrease") Prob_adjust_threshold
               else Prob_adjust_threshold          # magnitude; sign set per branch below
}
...
ix <- Intervention_idx[!is.na(Intervention_vals) &
                       Intervention_vals >= Intervention_ptile_val]
```

and mirror `>=` for the non-intervention selections. If strict `>` is intentional,
state the reason in a comment and log a distinct
`intervention no-op: to_val=%s no rows above percentile` line so `rows_changed=0` is
explicable from the log alone.

---

### WR-02: The prob clamps mutate rows outside the intervention's declared scope, uncounted

**File:** `src/implement_spatial_interventions.R:484-486`, `:738-740`

**Issue:** Both helpers clamp the **entire** table inside the per-target-class loop:

```r
normalized[prob > 1, prob := 1]
normalized[!is.na(prob) & prob < 0, prob := 0]
```

These predicates are not restricted to `Target_area_idx` / `sub_idx`, so an
intervention declared for `to_val = 105` rewrites rows of every other transition.
Reproduced: an out-of-scope row (`to_val = 104`, outside the mask, outside the
`Transition_target_classes`) carrying `prob = 1.7` was rewritten to `1.0` by an
intervention that targets only `105` Inside. `rows_changed` is computed only over the
intervention's own index set, so the mutation is invisible in the AUDIT line.

This matters specifically because the phase's recorded design decision is that
probabilities are **not** renormalised and out-of-range sums are only *counted*
(`cells_sum_gt1`). An engine that silently rewrites unrelated transitions contradicts
the spirit of that decision and makes `rows_changed` an unreliable audit number.
Hoisting the clamps out of the class loop would also remove N full-table scans.

**Fix:**

```r
# absolute_prob_adjust(), replacing lines 484-486
normalized[intersect(ix, which(prob > 1)), prob := 1]
normalized[intersect(ix, which(!is.na(prob) & prob < 0)), prob := 0]
```

Simplest correct form: clamp only the rows the intervention touched, e.g.
`normalized[ix, prob := pmin(pmax(prob, 0), 1)]` (NA-safe: `pmin`/`pmax` propagate NA),
placed immediately after the `:=` that introduced the new values, and applied once per
class rather than once per class over the whole table.

---

### WR-03: `threshold_msg()` logs the pre-threshold value in one of four call sites

**File:** `src/implement_spatial_interventions.R:682`

**Issue:** Three call sites (lines 673, 698, 721) log the value `Perc_diff` is being
set *to* (`Prob_adjust_threshold`). Line 682 logs `Perc_diff` — the old,
below-threshold value — while the code assigns `-(Prob_adjust_threshold)`. The log
line reads `"setting to threshold value: -2.3"` when the value actually used is `-5`.
Anyone reconstructing an intervention's effect from the log gets the wrong number.

**Fix:**

```r
threshold_msg(lulc_class, -(Prob_adjust_threshold))
Perc_diff <- -(Prob_adjust_threshold)
```

(and consider passing the signed value at line 708 as well, which currently logs
`Prob_adjust_threshold` while assigning `-(Prob_adjust_threshold)`.)

---

### WR-04: The D-14 pre-flight is fail-open — it silently skips itself when the engine is not loaded

**File:** `src/allocation.r:448-449`

**Issue:**

```r
if (!is.null(interventions_dir) && !is.null(mask_dir) &&
    exists("resolve_intervention_masks", mode = "function")) {
```

If the engine is not in scope, the entire intervention gate evaporates with no error
and no message, and `validate_allocation_runtime()` returns PASS. `run_allocation.r`
happens to `stopifnot()` beforehand, but `validate_allocation_runtime()` is a public
entry point (also driven by `--preflight-only` with a fixture, and by tests via
`sys.source()` into a bare env) and the guard is exactly the fail-open pattern the rest
of the phase is trying to eliminate. Note the asymmetry: a *missing mask file* is a
hard failure, but a *missing resolver* is a silent pass.

**Fix:**

```r
if (!is.null(interventions_dir) && !is.null(mask_dir)) {
  if (!exists("resolve_intervention_masks", mode = "function")) {
    intervention_errors <- c(
      intervention_errors,
      "intervention config: resolve_intervention_masks() not loaded - source src/implement_spatial_interventions.R"
    )
  } else {
    ...
  }
}
```

---

### WR-05: The pre-flight ignores `profile_timestep_index`, so it over-demands masks and can block a legitimate profile run

**File:** `src/allocation.r:450-469`

**Issue:** The pre-flight mirrors `ALLOCATION_YEAR_POST_FILTER` (lines 455-468) but not
`config[["profile_timestep_index"]]`, which `scripts/run_allocation.r:208-219` writes
onto the same config object and which `src/allocation.r:1726-1740` uses to narrow the
schedule to a single timestep. A profile run pinned to one timestep will still be
rejected at the gate if any mask for any of the other nine posterior years is absent.
Given that the `NAT`/`CUL`/`SOC` conservation interventions each reference three
distinct dynamic mask phases across ten years, a partially-staged mask directory is a
realistic state on a fresh HPC deployment.

**Fix:** Apply the same narrowing the driver applies, right after the
`ALLOCATION_YEAR_POST_FILTER` block:

```r
pti <- config[["profile_timestep_index"]]
if (!is.null(pti) && !is.na(pti) && pti >= 1L && pti <= length(years)) {
  years <- years[pti]
}
```

---

### WR-06: The YAML is parsed twice and the Allocation/year filter is duplicated — the root cause of CR-01

**File:** `src/implement_spatial_interventions.R:198-203` vs `:234-249`

**Issue:** `implement_spatial_interventions()` calls `resolve_intervention_masks()`
(which parses the YAML, filters to `Intervention_stage == "Allocation"` at lines 40-44
and intersects `Time_steps_implemented` with the year at lines 70-71), then
**re-parses the same file** at line 234 and **re-implements both filters** at lines
240-249. The two views are then joined by `Intervention_ID`. This is a duplicated
invariant across ~30 lines: any divergence (CR-01) manifests as a silent skip rather
than a mismatch error, and any future change to one filter must be mirrored in the
other with nothing enforcing it.

**Fix:** Have the resolver return the parsed entry alongside each row (e.g. an
`entry_index` column, or a parallel list attribute), and drive the applier loop off
`resolved` directly:

```r
resolved <- resolve_intervention_masks(...)   # gains an entry_index column
if (nrow(resolved) == 0L) { ...; write_summary(0L); return(normalized) }
entries <- attr(resolved, "entries")
ord <- order(resolved$rank, na.last = TRUE)
for (k in ord) {
  intervention <- entries[[resolved$entry_index[k]]]
  mask_path <- resolved$mask_path[k]
  ...
}
```

This deletes lines 234-249 and 306-315 outright and makes the id-join
impossible to get wrong.

---

### WR-07: The Absolute-0 raster assertion passes vacuously when the target zone is empty

**File:** `scripts/verify_intervention_smoke.r:309-320`

**Issue:**

```r
m0  <- terra::ifel(is.na(m), 0, m)
sel <- if (identical(zone, "Inside")) (m0 == 1) else (m0 != 1)
mx  <- terra::global(r * sel, "max", na.rm = TRUE)[[1]][1]
if (!is.finite(mx)) mx <- 0
```

`r * sel` is `0` everywhere `sel` is `FALSE`, so `max` is `0` when the zone contains no
cells at all — and `!is.finite(mx) -> 0` converts an all-NA result to a pass too. The
script then increments `maps_checked` and prints `[ok]`, so the PASS banner reports
`maps_checked=N` even when every one of those N checks was vacuous. That is precisely
the failure mode a smoke verifier exists to rule out: a mask that lands entirely
outside the region would be reported as a clean pass.

**Fix:** Assert the zone is non-empty before asserting the max, and report the count:

```r
n_zone <- terra::global(sel & !is.na(r), "sum", na.rm = TRUE)[[1]][1]
if (!is.finite(n_zone) || n_zone == 0) {
  fail(sprintf("%s: %s has 0 non-NA cells in zone=%s for mask %s (assertion would be vacuous)",
               id, basename(tif), zone, basename(mask_path)))
  next
}
mx <- terra::global(r * sel, "max", na.rm = TRUE)[[1]][1]
...
cat(sprintf("  %s zone=%s %s: n_zone=%d max prob in zone = %g [%s]\n",
            id, zone, basename(tif), as.integer(n_zone), mx, status))
```

---

### WR-08: Two valency/zone combinations shipped in production configs have no test coverage

**File:** `tests/testthat/test-spatial-interventions.R:277-295`, `:364-396`

**Issue:** `.rel_entry()` only ever produces `Decrease`/`Increase` with
`zone = "Inside"`. Untested paths that the shipped YAMLs actually use:

- `Prob_adjust_valency: Increase_inside_decrease_outside`
  (`src/implement_spatial_interventions.R:717-734`) — used by
  `config/NAT_interventions.yml` (`Urban_densification`) and
  `config/SOC_interventions.yml` (`Urban_densification`). This branch is the only one
  that writes to **both** zones in one pass and the only one that applies the threshold
  unconditionally; nothing verifies that the outside decrease actually happens or that
  the `Prob_adjust_zone != "Inside"` guard (lines 532-539) fires.
- `Prob_adjust_type: Relative` with `Prob_adjust_zone: Outside`
  (lines 585-588, where `Intervention_idx`/`Non_intervention_idx` are swapped) — used
  by `config/CUL_interventions.yml` (`Mining_outside_restraint`). Only the *Absolute*
  Outside path is tested (line 323).
- The `nrow(mask_row) == 0L` skip branch (lines 307-315) — untested, and CR-01 shows
  it is reachable.

**Fix:** Extend `.rel_entry()` with `zone`/`valency` already parameterised (it is) and
add:

```r
test_that("engine: Relative Increase_inside_decrease_outside moves both zones (NAT/SOC)", {
  fx  <- .engine_fixture(list(.rel_entry(valency = "Increase_inside_decrease_outside")))
  dt  <- .norm(); before <- copy(dt)
  out <- .run_engine(fx, dt)
  ins <- out$to_val == 105L & out$cell_id %in% c(1L, 3L, 5L)
  outs<- out$to_val == 105L & out$cell_id %in% c(2L, 4L, 6L)
  expect_true(any(out$prob[ins]  > before$prob[ins]))
  expect_true(any(out$prob[outs] < before$prob[outs]))
})

test_that("engine: Relative zone=Outside swaps the intervention zone (CUL)", { ... })

test_that("engine: Increase_inside_decrease_outside with zone=Outside is rejected", {
  fx <- .engine_fixture(list(.rel_entry(
    valency = "Increase_inside_decrease_outside", zone = "Outside")))
  expect_error(.run_engine(fx, .norm()), "must be 'Inside'")
})
```

---

### WR-09: `"zeros from Absolute=0 stay zero after a Relative Increase"` passes for the wrong reason

**File:** `tests/testthat/test-spatial-interventions.R:374-382`

**Issue:** The test asserts that after an `Absolute=0` (rank 1) plus a
`Relative Increase` (rank 2), inside cells are still `0`. But the `Absolute=0` pass
leaves *no positive probabilities inside the mask*, so the NaN guard at
`src/implement_spatial_interventions.R:641-654` skips the Relative intervention in its
entirety. The test therefore proves "a skipped intervention changes nothing" — not
"the `prob > 0` gate at line 480 and the `prob + (prob/100)*x` form preserve zeros",
which is what its name claims. Any future regression in the zero-preservation logic
(e.g. switching `Increase` to an additive offset) would not be caught.

**Fix:** Give the Relative pass a positive value to work with so it actually runs, then
assert the zeros survive:

```r
test_that("engine: zeros from Absolute=0 stay zero after a Relative Increase", {
  fx <- .engine_fixture(list(
    .abs_entry(id = "iv_zero", rank = 1L),                     # zeroes cells 1,3,5
    .rel_entry(id = "iv_inc", valency = "Increase", zone = "Outside", rank = 2L)
  ))
  dt  <- .norm(); before <- copy(dt)
  out <- .run_engine(fx, dt)
  inside <- out$to_val == 105L & out$cell_id %in% c(1L, 3L, 5L)
  expect_true(all(out$prob[inside] == 0))                      # zeros preserved
  expect_false(identical(out$prob, before$prob))               # the Relative pass DID run
  expect_length(.audit_lines(fx$log_file)$iv, 2L)
})
```

---

### WR-10: `.mask_inside_lut()` is unsafe for degenerate `cell_index` input

**File:** `src/implement_spatial_interventions.R:137`, `:468-469`, `:577-578`

**Issue:** `lut <- logical(max(cell_index$cell_id))` has three unguarded failure modes,
all confirmed:

- `cell_index` with zero rows -> `max(integer(0))` returns `-Inf` with a warning ->
  `logical(-Inf)` errors.
- any `NA` in `cell_id` -> `max()` returns `NA` -> `logical(NA)` errors with the opaque
  message `"vector size cannot be NA"`.
- any `cell_id == 0` in `normalized` -> `inside_lut[c(1L, 0L, 3L)]` returns a vector
  **shorter** than the index (`0` is dropped, not preserved as NA), so
  `sub_idx[inside_flag]` at lines 472/475/583/587 recycles and selects the **wrong
  rows silently**. (A negative `cell_id` errors instead, with
  `"only 0's may be mixed with negative subscripts"`.)

In the current wiring `cell_index` comes from `terra::as.data.frame(..., na.rm = TRUE)`
so these are latent, but `.mask_inside_lut()` and the helpers are exported-style
functions with documented contracts and no argument validation.

**Fix:**

```r
cid <- cell_index$cell_id
if (length(cid) == 0L || anyNA(cid) || any(cid < 1L)) {
  stop("cell_index$cell_id must be non-empty, non-NA and >= 1", call. = FALSE)
}
lut <- logical(max(cid))
```

---

### WR-11: The authoritative design source for the four new scenario YAMLs is not in the repository

**File:** `config/BAU_interventions.yml:9-11`, `.gitignore` (`spatial_interventions_integration_protocol.md`)

**Issue:** Every new config header cites
`spatial_interventions_integration_protocol.md ("not versioned in this repo") §4a` /
`§3.7` as the source of the encoded narrative, and that file is explicitly added to
`.gitignore` in this phase. The rationale for every percentile, threshold, valency and
zone choice across `BAU`/`NAT`/`CUL`/`SOC` — including non-obvious ones like
`Prob_adjust_zone: Outside` with `Prob_adjust_value: 0` (a hard national ban on mining
outside the concession mask) — therefore lives outside version control, on one
person's machine. Reviewing or reproducing a parameter choice is impossible from the
repo alone. `docs/spatial_interventions/scenario_narratives.md` and
`.../spatial_masks.md` are also cited; confirm those are tracked.

**Fix:** Either commit the protocol (or a distilled, citable extract of §3.7 and §4a)
under `docs/spatial_interventions/`, or replace the dangling `§` citations with
inline rationale in the YAML headers so each parameter is self-justifying.

## Info

### IN-01: Unreachable defensive `Mask_type` check that would itself error on NULL

**File:** `src/implement_spatial_interventions.R:302-304`

`if (!intervention$Mask_type %in% c("Static", "Dynamic"))` is dead: the resolver
already stops on any other value (line 85) for every intervention that reaches this
loop. Worse, if `Mask_type` were `NULL` the expression is
`if (!logical(0))` -> `"argument is of length zero"`, i.e. the defence crashes rather
than reporting. Delete it, or move the check into the resolver where the value is
actually first seen.

### IN-02: Stale `@param` documentation — `x` and `y` are not used by the engine

**File:** `src/implement_spatial_interventions.R:168-169`

The `normalized` contract documents columns `row_idx, from_val, to_val, cell_id, x, y,
prob`, but the engine never reads `x` or `y` (the whole point of the cell-number LUT
refactor was to stop using xy matrices — see the `.mask_inside_lut` docstring at line
122, "never an xy matrix"). Drop `x, y` from the `@param` line so the contract states
the real minimum.

### IN-03: The mask LUT cache key ignores both file mtime and `cell_index`

**File:** `src/implement_spatial_interventions.R:131-139`

The key is `normalizePath(mask_path)` only. Correct for the current call-scoped
`new.env()` (line 281), but the function signature invites a longer-lived cache, and
`tests/testthat/test-spatial-interventions.R:194-199` explicitly demonstrates that
overwriting the file on disk returns the stale LUT. If the cache is ever hoisted
(e.g. to a session-level cache like `.transition_model_cache` at
`src/allocation.r:786`), this becomes a stale-data bug. Consider keying on
`paste(key, file.mtime(mask_path), length(cell_index$cell_id))`, mirroring the
existing model cache convention, or document the call-scoped-only constraint as a
precondition.

### IN-04: `because_msg()` emits orphan log lines

**File:** `src/implement_spatial_interventions.R:559-564`, called at `:676`, `:685`, `:701`, `:710`, `:724`

Each call writes a standalone timestamped line beginning with
`"because the Prob_adjust_valency is ..."`, with no preceding clause on the same line.
Read in isolation (which is how log greps read) it is a sentence fragment. Prefix it
with the action it explains, e.g.
`sprintf("to_val=%s: applying %s adjustment because ...", lulc_class, ...)`.

### IN-05: Validator orphan detection is extension-case-sensitive and `--scenarios`-dependent

**File:** `scripts/validate_intervention_masks.r:294-295`

`grepl("[.]tif$", top)` misses `.TIF`, `.tiff` and `.TIFF`. A mask named `x.tiff` and
referenced by a YAML would be classified as "local-only, not staged (INFO)" rather than
checked or flagged. Separately, `orphans <- setdiff(tifs, referenced_names)` is computed
against only the scenarios passed via `--scenarios`, so a narrowed run reports every
other scenario's masks as orphans (WARN). Use
`grepl("[.]tiff?$", top, ignore.case = TRUE)` and note the `--scenarios` caveat in the
report header.

### IN-06: Tests bootstrap with `%||%`, which the engine deliberately avoids

**File:** `tests/testthat/test-spatial-interventions.R:18`, `tests/testthat/test-spatial-interventions-wiring.R:10`

`sys.frame(1)$ofile %||% "."` runs at file-source time, **before** `src/utils.r` is
sourced (line 27 / line 108), so it resolves to base R's `%||%` (R >= 4.4). The engine
under test deliberately avoids `%||%` for exactly this reason. Harmless under R 4.5.0,
but the two files now disagree about the project's minimum R. Use an explicit
`if (is.null(x)) "." else x`, or record the R >= 4.4 floor for tests in `DESCRIPTION`.
The six-line `.repo_root` incantation is also duplicated verbatim across both files —
worth hoisting into `tests/testthat/helper-*.R`.

### IN-07: `normalizePath(mustWork = TRUE)` turns a TOCTOU race into a bare R error

**File:** `src/implement_spatial_interventions.R:131`

Between the resolver's `file.exists()` (line 106) and the LUT read, a mask can be
removed or a network mount can drop — a realistic scenario on the beegfs staging path
this phase uses. The result is `"path[1]=...: The system cannot find the file
specified"` with no scenario/intervention/year context, and it is not one of the
markers `verify_intervention_smoke.r` scans for. Wrap it:

```r
key <- tryCatch(normalizePath(mask_path, mustWork = TRUE), error = function(e) {
  stop(sprintf("intervention mask missing: %s (disappeared after pre-flight)", mask_path),
       call. = FALSE)
})
```

so it lands on the already-forbidden `"intervention mask missing"` marker.

---

## Notes on scope

Explicitly excluded per the review brief and confirmed **not** reported above:
non-renormalisation after interventions (recorded design decision; `cells_sum_gt1` is
logged correctly and was verified to fire — the probe observed `cells_sum_gt1=1`),
the deliberate avoidance of `%||%` in the engine, the `.__Task__col_info` benign-marker
exclusion in `verify_intervention_smoke.r`, and the pre-existing
`test-prep-paths.R` errors. `src/old/`, `docs/` and `.planning/` were not reviewed.

Verified clean: no debug artifacts, `TODO`/`FIXME`/`browser()`/stray `print()` in any
new file; all five R sources parse cleanly under R 4.5.0; no injection, eval, or
credential-handling surface; the `[/\\]` and `..` mask-name guards in the resolver are
correct and tested; `year_post` is threaded correctly to the single
`generate_probability_maps()` call site (`src/allocation.r:2370-2376`); the
`%03d_id_trans_%d.tif` index the smoke verifier reconstructs from `trans_rates.csv`
matches the writer's `row_idx` (`src/allocation.r:2708-2709`, `:2988`); and
`config/{BAU,NAT,CUL,SOC}_interventions.yml` were machine-audited — all mask names are
bare filenames, all Dynamic entries cover every implemented year, all
`Time_steps_implemented` are posterior years, no duplicate IDs or ranks within a
scenario, and no missing `Prob_adjust_*` fields *today* (see CR-03 for why that is not
enough).

---

_Reviewed: 2026-09-23T11:05:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: deep_
