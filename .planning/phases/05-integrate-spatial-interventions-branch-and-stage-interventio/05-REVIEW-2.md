---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
reviewed: 2026-09-28T00:00:00Z
depth: deep
cycle: 2
baseline: 05-REVIEW.md (21 findings, treated as closed — NOT re-reported here)
files_reviewed: 4
files_reviewed_list:
  - src/implement_spatial_interventions.R
  - src/allocation.r
  - scripts/verify_intervention_smoke.r
  - scripts/validate_intervention_masks.r
findings:
  critical: 1
  warning: 4
  info: 8
  total: 13
status: issues_found
---

# Phase 5: Code Review Report (cycle 2 — gap-closure fixes)

**Reviewed:** 2026-09-28
**Depth:** deep (cross-file: engine <-> hook <-> pre-flight <-> verifier <-> validator <-> shipped YAML)
**Files Reviewed:** 4
**Status:** issues_found

## Summary

This is a second-cycle review of the four files touched by plans 05-07 .. 05-16. The
original 21 findings in `05-REVIEW.md` were treated as the closed baseline; the question
asked here is whether closing them introduced anything new. Every behavioural claim below
was derived from the code at HEAD, from the shipped `config/*_interventions.yml`, from
`docs/spatial_interventions/spatial_masks.md`, or from an R probe run locally — not from
review prose.

**The mechanical contracts hold.** All the frozen invariants I was asked to check are
intact (see *Invariants verified* below): 0 `resample(`/`project(`, no second copy of the
probability table, 7 writes paired 1:1 with 7 scoped clamps (no index set clamped twice, none
missed), the AUDIT format string is exactly 21 whitespace-delimited tokens with the first 13
byte-identical to `92a2da7`, the PASS banner prefix is byte-identical to `7c9cd96` with
`telemetry_rows=%d` appended, skipped classes are seeded before every one of the four `next`
paths, `attr(res$stats, "delta")` is always set, and `.safe_path_token()` genuinely defeats
traversal (probed: `../../etc/passwd` -> `____etc_passwd`, `..` -> `_`, `x..y` -> `x_y`).

**The new defects are all at the edges of the new guards, not in the arithmetic.** The
dominant theme is that the phase built a *fail-closed runtime* for two of the three
assertions the offline validator makes, and left the third (mask value domain) offline-only
while a code comment claims parity. That gap is silently catastrophic for the two shipped
`Absolute`/`0`/`Outside` interventions and is invisible to the smoke verifier, which is why
it is the single Critical finding. The four Warnings are: a residual pre-flight fail-open
when `interventions_dir`/`spat_prob_perturb_dir` are absent from config; a docstring that
invites a cache hoist the cache key cannot support; `telemetry_dir` carrying a default in
violation of the stated required-formal invariant; and the WR-07 non-vacuity guard turning a
legitimate "this mask does not intersect this region" into a FAIL.

No security issue was found: there is no injection surface, no eval, no secret, and the only
attacker-influenced path component (`scenario`, `region` from operator-edited YAML) is
sanitised and then re-asserted separator-free before any write.

---

## Critical Issues

### CR-01: `.mask_inside_lut()` has no mask value-domain guard — a non-1-coded mask silently turns `Prob_adjust_zone: Outside` into "the entire region", and the smoke verifier PASSes

**File:** `src/implement_spatial_interventions.R:324-328` (the unguarded read), with
`src/implement_spatial_interventions.R:295-308` (the two guards that *were* mirrored) and
`src/allocation.r:617-655` (the pre-flight geometry pass, likewise value-blind).
**Contrast:** `scripts/validate_intervention_masks.r:260-275` — the offline validator *does*
assert the value domain (`bad <- vals[!vals %in% c(0, 1)]` -> hard FAIL).

**Issue.**
`.mask_inside_lut()` decides membership with

```r
v <- terra::extract(m, rcid)[[1L]]
lut <- logical(max(cid))
lut[cid[!is.na(v) & v == 1]] <- TRUE
```

Any value that is not exactly `1` — `255` (the default burn value of `gdal_rasterize` and of
QGIS "Rasterize" on an 8-bit output), `2`, a scaled `0/100` mask, or the character labels a
*categorical* GeoTIFF returns from `terra::extract()` — reads as **outside**, with no error
and no log line. The comment at line 301 asserts *"Same assertion
scripts/validate_intervention_masks.r makes offline, so the runtime and the validator report
the same condition (CR-02)"*, but the validator makes **three** assertions (`nlyr`,
`compareGeom`, value domain within `{0,1,NA}`) and 05-09 mirrored only the first two, into
both the runtime and the Stage 7 pre-flight.

The consequence is not a no-op. For `Prob_adjust_zone: Outside` the zone is the *complement*
of the mask, so an all-FALSE LUT inverts the policy to the whole region:

```r
# absolute_prob_adjust(), src/implement_spatial_interventions.R:1030-1040
} else {
  Target_area_idx <- sub_idx[!inside_flag]   # inside_flag all FALSE => ALL rows
}
...
normalized[ix, prob := Prob_adjust_value]    # Prob_adjust_value == 0
```

Two shipped entries have exactly this shape:

- `config/NAT_interventions.yml:90-107` — `Mining_freeze_post_2030`: `Absolute` / value `0` /
  zone `Outside` / `mining_concessions_mask.tif`, active 2032..2060.
- `config/SOC_interventions.yml:~100-109` — the second `Absolute` / `0` / `Outside` entry.

Concrete failure scenario: `mining_concessions_mask.tif` is rebuilt or re-staged with a burn
value of 255 instead of 1 (or converted through a tool that writes a category table). The run
does not warn. `Mining_freeze_post_2030` then sets **every** `* -> mining` probability in
every region to 0 rather than only outside the concessions, which silently deletes all
permitted mining from 2032 onward — a wrong scientific result presented as a successful run.

**The smoke verifier cannot catch it.** `scripts/verify_intervention_smoke.r:341` builds the
same zone from the same coding (`sel <- if (zone == "Inside") (m0 == 1) else (m0 != 1)`), so
for zone `Outside` it asserts "prob is 0 over the complement of the mask" — which an
all-zero mask satisfies trivially over the whole region, with `n_zone` large and non-vacuous.
The `n_zone` guard (WR-07) only proves the *complement* is non-empty; it never proves the mask
has a single `1` cell. So the shipped evidence for this phase (job 841577,
`interventions=5 maps_checked=16`) genuinely cannot distinguish "the mining mask is correctly
1-coded" from "the mining mask reads as empty" — the only intervention whose assertion would
have proven the mask has 1-cells is the `Inside` one, on a different mask.

**Reachability, stated honestly.** Today's masks satisfy the contract:
`docs/spatial_interventions/spatial_masks.md:11` states *"Each mask is value 1 = inside"* and
`:156` records `terra::rasterize(field = 1L, background = NA)`. So this is a missing guard, not
a present miscomputation. It is Critical because (a) the trigger is a routine re-staging
mistake on a mask the repository does not contain, (b) the outcome is silent and scientifically
wrong rather than a crash, (c) the only defence today is an operator remembering to run
`scripts/validate_intervention_masks.r`, which is wired into nothing — it appears only in
`docs/README_HPC.md:326` — and (d) CR-02's entire rationale was that "the offline validator
covers it" is not an acceptable defence for exactly this class of failure.

**Fix.** The extracted vector is already materialised, so the runtime check is free:

```r
  v <- terra::extract(m, rcid)[[1L]]
  # Mirror the THIRD assertion validate_intervention_masks.r makes: a mask value
  # that is not 0/1/NA is not a mask. Silently reading it as "outside" inverts
  # every Prob_adjust_zone: Outside intervention onto the whole region.
  if (!is.numeric(v)) {
    stop(sprintf(
      "intervention mask %s is categorical/non-numeric; expected numeric {0,1,NA}",
      mask_path
    ), call. = FALSE)
  }
  bad <- unique(v[!is.na(v) & v != 0 & v != 1])
  if (length(bad) > 0L) {
    stop(sprintf(
      "intervention mask %s has value(s) outside {0,1,NA}: %s",
      mask_path, paste(utils::head(bad, 5L), collapse = ", ")
    ), call. = FALSE)
  }
```

and add the cheap header-level half to the Stage 7 pre-flight next to the `nlyr` check
(`src/allocation.r:631`), e.g. `mm <- terra::minmax(m, compute = TRUE)` rejected unless
`mm[1] >= 0 && mm[2] <= 1`. Add `"outside {0,1,NA}"` (or the chosen wording) to the verifier's
`forbidden` list at `scripts/verify_intervention_smoke.r:524-551` so the marker is fatal there
too. Independently, consider making the verifier prove `Outside`-zone masks are non-degenerate
(`global(m0 == 1, "sum") > 0`), which closes the observational blind spot even without the
engine change.

---

## Warnings

### WR-01: the Stage 7 intervention gate is still fail-open when `interventions_dir` or `spat_prob_perturb_dir` is absent from config — the run then dies mid-region with "argument is of length zero"

**File:** `src/allocation.r:446-453`, consumed at `src/implement_spatial_interventions.R:30-50`.

**Issue.**

```r
interventions_dir <- maybe("interventions_dir")      # NULL unless length-1 non-empty chr
mask_dir          <- maybe("spat_prob_perturb_dir")
interventions_configured <- !is.null(interventions_dir) && !is.null(mask_dir)
```

Everything the phase added — the `engine_loaded` check (WR-04), mask existence (D-14), the
geometry pass (CR-02), and the telemetry writability probe (D-21) — hangs off
`interventions_configured`. If either key is missing or empty in a config (neither has a
default in `src/setup.r`; both exist only as literal keys at
`config/local_config.yaml:57` / `config/hpc_config.yaml:61`), the entire intervention gate
skips itself and the pre-flight prints *"Allocation pre-flight passed."* — the exact
fail-open shape the comment at lines 448-452 says the phase removed, just moved one predicate
to the left.

The hook, however, is unconditional (`src/allocation.r:3127-3140` passes
`config[["interventions_dir"]]` whatever it is). I verified the consequence in R:

```r
p <- file.path(NULL, "NAT_interventions.yml")   # -> character(0)
if (!file.exists(p)) ...                        # -> Error: argument is of length zero
```

So a config missing `interventions_dir` gives a cryptic length-zero crash inside
`resolve_intervention_masks()` after hours of Stage 7 work (it is on the verifier's forbidden
marker list, so the smoke *reports* it — but only after the job is burnt). A config missing
`spat_prob_perturb_dir` instead reaches `data.frame(..., mask_path = character(0), ...)` and
dies with "arguments imply differing number of rows: 1, 0", which is **not** on the forbidden
marker list at all.

**Fix.** Treat an unset intervention path as a gate failure, not as "no interventions
configured", and give the engine the same scalar-path guard it already gives `ref_grid_path`:

```r
# src/allocation.r, replacing the silent skip
if (is.null(interventions_dir) || is.null(mask_dir)) {
  intervention_errors <- c(
    intervention_errors,
    "intervention config: interventions_dir / spat_prob_perturb_dir must be single non-empty paths in config"
  )
}
```

```r
# src/implement_spatial_interventions.R, top of resolve_intervention_masks()
for (nm in c("interventions_dir", "mask_dir")) {
  v <- get(nm)
  if (length(v) != 1L || is.na(v) || !nzchar(v)) {
    stop(sprintf("intervention config: %s must be a single non-empty path", nm), call. = FALSE)
  }
}
```

### WR-02: the `.mask_inside_lut()` docstring invites a session-level cache the cache key cannot support — two regions with equal cell counts collide and silently get each other's mask membership

**File:** `src/implement_spatial_interventions.R:248-251` (the claim) and `:281-287` (the key).

**Issue.** The key is `normalizePath | mtime | size | length(cid)`. The only `cell_index`
component is its **length**; nothing in the key identifies *which* region's `cell_id` /
`ref_cell_id` mapping produced the LUT. The docstring nevertheless states:

> An overwritten mask therefore cannot be served stale, so a longer-lived (e.g. session-level)
> cache is safe here, not just the call-scoped `new.env()` the engine currently passes.

That is false. `select_allocation_plan()` can resolve to `future::plan(future::sequential)`
(`scripts/run_allocation.r:256`, and unconditionally at `:283`), and even under
`multisession` each worker handles several elements of `furrr::future_map(region_inputs, ...)`
(`src/allocation.r:2075`). So one R process routinely processes multiple regions. Two regions
whose anterior rasters happen to have the same number of non-NA cells would hit the same key
for the same mask and the second region would receive the **first region's** LUT — indexed by
a different `cell_id` space entirely. The result is a wrong inside/outside partition with no
error, no warning and no telemetry difference; and because `mtime` granularity is 1 s on many
filesystems, the "cannot be served stale" half is also weaker than stated for a
same-size in-place overwrite.

The code as shipped is correct (the cache is call-scoped, `src/implement_spatial_interventions.R:676`).
The defect is the invitation: the docstring is the specification a future editor will act on.

**Fix.** Either delete the safety claim, or make the key actually region-identifying so the
claim becomes true:

```r
  key <- paste(
    key_path,
    format(file.mtime(key_path), "%Y-%m-%d %H:%M:%OS6"),
    file.size(key_path),
    length(cid),
    # Identity of the cell_index, not just its size: a session-level cache is
    # otherwise served the wrong region's LUT whenever two regions have the
    # same non-NA cell count.
    sum(as.numeric(cid)), sum(as.numeric(cell_index$ref_cell_id)),
    sep = "|"
  )
```

(or key on a digest of `ref_cell_id` if a stronger guarantee is wanted).

### WR-03: `telemetry_dir` has a `NULL` default, violating the required-formal contract; a call site that forgets it loses the D-18 CSV with no WARN and no other signal

**File:** `src/implement_spatial_interventions.R:576` (the default), `:682` and `:895` (the
consequences).

**Issue.** The phase contract is that `ref_grid_path` **and** `telemetry_dir` are required
formals with no defaults, precisely so no forgetful call site can skip a check. `ref_grid_path`
complies (line 571). `telemetry_dir = NULL` does not. The asymmetry matters because of how the
NULL path is implemented:

```r
telemetry_rows <- if (is.null(telemetry_dir)) NULL else list()   # :682
...
if (!is.null(telemetry_rows) && length(telemetry_rows) > 0L) {   # :895
```

A *failed* write logs `WARN intervention telemetry: ...` (D-21, and the verifier treats that
marker as fatal). An *unrequested* write logs nothing at all. So a new or refactored call site
that omits `telemetry_dir` — a re-run helper, a second hook, a future per-timestep entry point
— produces a complete, plausible-looking AUDIT trail with no CSV and no diagnostic anywhere.
The only detection is `scripts/verify_intervention_smoke.r:411` ("telemetry CSV missing"),
which is run for one scenario x region x year.

There is exactly one production call site today (`src/allocation.r:3138`) and it passes
`work_dir`, so nothing is broken right now; `tests/testthat/test-spatial-interventions.R:1361`
covers the NULL path deliberately. But "fixture mode" does not need a *default* — the test
helper at `:445-459` already builds its argument list conditionally and can pass
`telemetry_dir = NULL` explicitly.

**Fix.** Drop the default so the formal is required, and keep `NULL` as the documented
explicit fixture value:

```r
  region_label = NA_character_,
  telemetry_dir            # REQUIRED: pass NULL explicitly for fixture mode
) {
  if (missing(telemetry_dir)) {
    stop("telemetry_dir is required; pass NULL explicitly to disable the D-18 CSV", call. = FALSE)
  }
```

### WR-04: the WR-07 non-vacuity guard FAILs a correct run whenever a mask legitimately does not intersect a transition's from-class in this region

**File:** `scripts/verify_intervention_smoke.r:347-354`.

**Issue.**

```r
n_zone <- terra::global(sel & !is.na(r), "sum", na.rm = TRUE)[[1]][1]
if (!is.finite(n_zone) || n_zone == 0) {
  fail(sprintf("%s: %s has 0 non-NA cells in zone=%s ... (assertion would be vacuous)", ...))
  next
}
```

`r` is one *per-transition* probability map, so it is non-NA only on cells of that
transition's **from-class** (`src/allocation.r:3196-3200`: `r <- setValues(anterior, NA_real_)`
then `r[dt_j$cell_id] <- dt_j$prob`). `n_zone` therefore counts "cells inside the selected zone
that also hold the from-class of this one transition". Zero is a completely ordinary outcome:
NAT's `Absolute`/`0`/`Inside` entry targets three to-classes, so every transition whose To is
`built_up_and_barren_lands`, `high_intensity_agricultural_areas` or `mining` is checked,
including rare from-classes. A protected-area mask that contains no
`low_intensity_agricultural_areas` cells in, say, `andes` makes
`low_intensity_ag -> mining` produce `n_zone = 0` — the engine behaved perfectly (nothing to
adjust there), and the verifier reports FAIL and exits 1.

The guard cannot distinguish "the test asserts nothing because the mask landed outside the
region" (the WR-07 defect) from "the test asserts nothing because this transition has no cells
in the zone" (normal). It converted a false PASS into a false FAIL. It happens not to fire for
the one combination the phase gated on (NAT x costa_peruana x 2032, `maps_checked=16`), which
is why it has not been seen yet; it is a live hazard for every other region the operator points
it at.

**Fix.** Report non-overlap as INFO that does not count toward `maps_checked`, and move the
non-vacuity assertion up one level, where it is actually meaningful — per intervention, and per
run:

```r
          if (!is.finite(n_zone) || n_zone == 0) {
            cat(sprintf(
              "  Info: %s %s has no non-NA cells in zone=%s (mask does not intersect this transition's from-class); not counted\n",
              id, basename(tif), zone
            ))
            next
          }
```

plus, after the per-intervention loop, `if (n_checked_for_this_id == 0L) fail(...)` — an
intervention none of whose maps could be asserted on is the real WR-07 condition — and keep a
run-level `maps_checked > 0` requirement whenever `length(abs0) > 0`.

---

## Info

### IN-01: `.delta_stats()` silently discards the caller's `n_target` / `n_changed` when every targeted row is NA

**File:** `src/implement_spatial_interventions.R:389-398`. `if (length(d) == 0L) return(.delta_stats_skipped(target_class))`
returns `n_target = 0L, n_changed = 0L` even when `length(before) > 0` and the caller's
`n_changed_k` was non-zero (a `0.5 -> NA` transition counts as changed in
`.count_prob_changes()`). The AUDIT `rows_target` / `rows_changed` accumulators still count
those rows, so an Absolute intervention would trip the verifier's
`sum(n_target) != rows_target` and `sum(n_changed) != rows_changed` cross-checks
(`scripts/verify_intervention_smoke.r:470-493`) on a *correct* run. Unreachable today —
`prob` is NA-free by construction (`src/allocation.r:3071`: `prob_values[is.na(prob_values)] <- 0`)
and `Prob_adjust_value` is validated non-NA — so this is latent only.
**Fix:** distinguish "no rows" from "no comparable rows": keep `n_target = length(before)` and
the passed `n_changed` in the degenerate branch and NA out only the distribution columns.

### IN-02: the resolver validates `Prob_adjust_type` with `as.character()` but the applier dispatches with `identical()` — validation does not actually guarantee dispatch

**File:** `src/implement_spatial_interventions.R:107-126` (validation) vs `:756`/`:775`/`:793`
(dispatch). A length-1 non-character value (e.g. a YAML *mapping* under
`Prob_adjust_type:`) passes `as.character(adj_raw) %in% c("Absolute","Relative")` and then
fails both `identical()` comparisons, producing `stop("Unknown Prob_adjust_type: Absolute")`
**mid-loop** — after higher-ranked interventions have already rewritten the probability
surface. That is the CR-03 failure shape the validation pass exists to prevent. (I probed the
common case: a one-element YAML *sequence* is simplified by `yaml::` to a length-1 character
vector and is safe, so reachability is low.) **Fix:** require `is.character(adj_raw)` in the
validation pass, or dispatch on `as.character(intervention[["Prob_adjust_type"]])`.

### IN-03: `--preflight-only` asserts nothing about interventions, which the fail-closed comment does not admit

**File:** `scripts/run_allocation.r:158-161` (`config = NULL` by design) vs the comment at
`src/allocation.r:448-452`. The operator-facing gate prints "Allocation pre-flight passed."
without running a single intervention check; the real gate is the in-run call at
`scripts/run_allocation.r:233` / `src/allocation.r:1767`, which does run before
`future::plan()` and before region work. This was consciously accepted
(`05-RESEARCH.md:255`), so the finding is the *comment*, which claims the phase removed "the
exact fail-open shape" — for the `--preflight-only` surface it did not. **Fix:** one sentence
in the comment, or pass `get_config()` in `--preflight-only` mode when it is available.

### IN-04: `.safe_path_token()` is applied to the CSV basename but not to the region directory or to the verifier's reconstruction of the same name

**File:** `src/implement_spatial_interventions.R:902-907` vs
`src/allocation.r:1199` (`region_suffix <- gsub(" ", "_", tolower(region_label))`, unsanitised)
and `scripts/verify_intervention_smoke.r:394-397` (`sprintf(..., scenario, region, year)`, raw).
For any region label containing a character outside `[A-Za-z0-9_.-]` the engine writes
`..._p_ramo_2032.csv` into `region_páramo/` while the verifier looks for
`..._páramo_2032.csv` and reports "telemetry CSV missing" on a healthy run (probed:
`páramo` -> `p_ramo`). Latent for the four shipped regions (`andes`, `costa_peruana`,
`cuenca_del_amazonas`, `selva_andina`), all ASCII. **Fix:** have the verifier apply the same
token function (it already sources the engine), or record the exact basename in the AUDIT
summary line.

### IN-05: the AUDIT/CSV `sum_abs_delta` cross-check has only a 2x margin over the AUDIT field's own rounding

**File:** `src/implement_spatial_interventions.R:446-449` (`formatC(format = "g", digits = 6)`,
i.e. <= 5e-6 relative rounding) vs `scripts/verify_intervention_smoke.r:444`
(`abs(a-b) <= 1e-5 * max(1, |a|, |b|)`). Correct today, but any reduction of `digits`, or a
switch to a wider tolerance basis, silently converts a real disagreement into a pass — or a
rounding artefact into a FAIL. **Fix:** note the coupling in a comment at both sites, or derive
the tolerance from the rendered precision.

### IN-06: the resolver docstring overclaims that every entry is validated regardless of `years`

**File:** `src/implement_spatial_interventions.R:24-28` vs `:186-208`. The identity,
`Mask_type`, `Prob_adjust_*` and `Transition_target_classes` checks do run for every
Allocation entry, but the `Intervention_mask` *shape* is only exercised for years in
`active_years`. A `Dynamic` entry whose `Intervention_mask` is a scalar instead of a
year-keyed mapping surfaces as `"foo.tif"[["2032"]]` -> `subscript out of bounds` (a forbidden
marker) rather than a schema error, and only when that year is in scope. The standalone
validator covers all posterior years, so the exposure is the narrowed pre-flight.
**Fix:** move a `Dynamic` => "mask is a named mapping whose keys are years" assertion into the
per-entry validation pass.

### IN-07: duplicate `Transition_target_classes` are not rejected and would produce duplicate CSV rows plus a double-counted `rows_target`

**File:** `src/implement_spatial_interventions.R:164-170` (validation) and `:1003`/`:1158`
(per-class loops). Two identical class names, or two names mapping to the same
`class_name_to_value`, yield two stats rows for the same `target_class`, double-count
`rows_target`, and trip `anyDuplicated(got)` at
`scripts/verify_intervention_smoke.r:462-466`, i.e. a config typo is reported as a telemetry
contract violation. No shipped YAML has duplicates (checked all four). **Fix:** add
`if (anyDuplicated(tgt) > 0L) stop(...)` to the validation pass.

### IN-08: two residual non-fatality / portability edges in the telemetry path

**File:** `src/implement_spatial_interventions.R:941-955` and `src/allocation.r:485-486`.
(a) The D-21 error handler calls `log_msg()`; if the log write itself fails, that error escapes
the `tryCatch()` and becomes fatal — the one remaining way the "non-fatal" write can abort a
run. Wrapping the handler body in `try(..., silent = TRUE)` closes it. (b) The pre-flight
writability probe uses `file.access(probe, 2L)`, which `?file.access` documents as unreliable
on Windows, so the D-21 pre-flight can pass or fail spuriously on a local run (harmless on the
Linux HPC target). Everything else in that block is sound: I confirmed `tryCatch(expr)`
assigns `csv_path`/`tmp_path` into the function frame so the handler's `unlink()` sees them,
and that `file.rename()` over an existing destination succeeds on Windows R 4.5 (so re-runs do
not leave stale telemetry).

---

## Invariants verified

| Invariant | Result |
|---|---|
| D-13: zero `resample(` / `project(` in the engine and `src/allocation.r` | **0 / 0** — holds |
| D-20: no second copy of the probability table (`copy(normalized`, `data.table::copy`) | **0** in the engine; only per-class `before`/`after`/`d` vectors and one `by = cell_id` aggregate (`:633`) |
| Non-renormalisation | Holds: the only table-wide statement is the read-only `cells_sum_gt1` roll-up (`:632-641`) |
| Scoped clamps | **7 writes / 7 clamps, paired 1:1** (`:1040/1047`, `1285/1288`, `1300/1303`, `1319/1322`, `1334/1337`, `1351/1354`, `1360/1363`). No index set clamped twice; none written without a clamp; `Increase_inside_decrease_outside`'s two sets are disjoint |
| `>=` percentile alignment (WR-01a) | Selection masks are character-identical to the mask used for the percentile means, in all five branches |
| `Perc_diff >= 0` dispatch (WR-01b) | Both `Increase` and `Decrease` take the `>= 0` route; the `< 0` route is `else if (Perc_diff < 0)`, so exactly one branch runs. Positive deltas from a `Decrease` entry are the documented `Perc_diff < 0` route — not reported, per the brief |
| Skipped-class seeding | Seeded before *all four* `next` paths: `absolute:1007/1024`, `relative:1163/1172`, `:1198`, `:1251` |
| `attr(res$stats, "delta")` always set | Yes — `.bind_delta_stats()` (`:428-435`) sets it unconditionally, defaulting to `numeric(0)` |
| 19-column / 25-column contracts | `setdiff(names, c("n_inc","n_dec"))` yields 17 columns; 8 identity + 17 = 25, in the exact order of `tel_cols` (`verify:387-393`) |
| Frozen AUDIT contract | **21 whitespace-delimited tokens**; first 13 byte-identical to `92a2da7`. `.fmt_num()` probed on `-0.4, 1234567, 1e-20, 1/3, 0, NA, Inf, 1e20, -1e-7` — always exactly one token, `Inf`/`NA` -> `"NA"` |
| Frozen PASS banner | Prefix through `maps_checked=%d` byte-identical to `7c9cd96`; `telemetry_rows=%d` appended |
| `ref_grid_path` required formal | Holds (`:571`), with a fail-closed scalar/exists/readable chain at `:610-630` |
| `telemetry_dir` required formal | **Violated** — see WR-03 |
| Telemetry cannot escape the region dir | Holds — `.safe_path_token()` probed against `../../etc/passwd`, `..`, `a/b`, `x..y`; the post-sanitisation re-assertion at `:911` is a genuine second gate |
| `ref_cell_id` grid provenance | Correct: computed at `src/allocation.r:2810-2812` from `config[["ref_grid_path"]]`, the same file the engine validates every mask against |

## Deferred (not findings)

- The four errors in `tests/testthat/test-prep-paths.R` (`.repo_root` NULL under `test_dir()`)
  remain a pre-existing harness defect, out of Phase 5 scope.
- `scripts/validate_intervention_masks.r` calls `terra::freq()` on national masks (~20 M cells
  each); a full value scan per mask is slow but this is an offline operator tool and
  performance is out of v1 review scope. Its logic is sound: `add_result()` accumulates rather
  than stopping, unreadable masks short-circuit before the geometry row is overwritten, the
  case-insensitive orphan comparison is correct, and the `gsub("|", "\\|", ..., fixed = TRUE)`
  markdown escape behaves as intended.

---

_Reviewed: 2026-09-28_
_Reviewer: Claude (gsd-code-reviewer), cycle 2_
_Depth: deep_
_Baseline: `05-REVIEW.md` (21 findings) — treated as closed; not re-reported_
