---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
plan: 08
subsystem: spatial interventions / scenario config provenance / validation tooling
tags: [interventions, provenance, docs, yaml, validator, orphans, wr-11, in-05]
requires: ["05-03", "05-05"]
provides:
  - docs/spatial_interventions/parameter_provenance.md (in-repo rationale for all 14 Allocation entries)
  - tests/testthat/test-intervention-provenance.R (D-23 coverage gate binding YAMLs to the record)
  - YAML headers in config/{BAU,NAT,CUL,SOC}_interventions.yml citing the in-repo record
  - case-insensitive .tif/.tiff classification + scoped-orphan caveat in scripts/validate_intervention_masks.r
affects: ["any future plan adding an Allocation intervention — the coverage gate now requires a rationale row"]
tech-stack:
  added: []
  patterns: [in-repo provenance record substituting for an unversioned external source, would-have-caught-it coverage gate, case-insensitive extension classification with exact-name reference matching]
key-files:
  created:
    - docs/spatial_interventions/parameter_provenance.md
    - tests/testthat/test-intervention-provenance.R
  modified:
    - config/BAU_interventions.yml
    - config/NAT_interventions.yml
    - config/CUL_interventions.yml
    - config/SOC_interventions.yml
    - scripts/validate_intervention_masks.r
decisions:
  - "Per D-16 the external protocol was NOT imported; a distilled in-repo record justifies every parameter from versioned sources and names what it cannot reconstruct"
  - "The provenance record's rationale table cites concrete drivers (narrative clause, mask semantics, engine behaviour) — never a bare protocol citation"
  - "05-REVIEW.md WR-11 misattributes `Prob_adjust_value: 0` to CUL's Mining_outside_restraint; the record documents the shipped config and carries an explicit correction note"
  - "Orphan detection classifies .tif/.tiff case-insensitively but keeps referenced_names exact; the setdiff comparison is lower-cased so x.TIF vs staged x.tif is neither an orphan nor a missing file"
  - "Test repo-root resolution walks up from getwd() looking for config/BAU_interventions.yml — avoids base R's null-coalescing operator at file-source time (IN-06)"
metrics:
  duration: ~50min
  completed: 2026-09-23
  tasks: 3
  files: 7
---

# Phase 5 Plan 08: In-repo parameter provenance and case-insensitive orphan detection Summary

WR-11 and IN-05 are closed. Every percentile, threshold, valency and zone in the four shipped scenario YAMLs is now justified from a file that is in the repository, with the six choices that genuinely still depend on the unversioned protocol named explicitly rather than papered over. The standalone validator no longer misclassifies a `.TIFF` mask as local-only, and a `--scenarios`-narrowed run now says in its own report header that its orphan list is scoped.

## Tasks

| # | Task | Commit | Files |
|---|------|--------|-------|
| 1 | Write the in-repo parameter provenance record (WR-11) | `28a5a55` | `docs/spatial_interventions/parameter_provenance.md` (159 lines) |
| 2 | Point the YAML headers at the record + consistency gate | `c911f8d` | 4 × `config/*_interventions.yml`, `tests/testthat/test-intervention-provenance.R` |
| 3 | Case-insensitive orphan detection + scoped-orphan caveat (IN-05) | `f6b74eb` | `scripts/validate_intervention_masks.r` |

## What was built

### `docs/spatial_interventions/parameter_provenance.md` (159 lines, 6 sections)

- **Why this file exists** — names `spatial_interventions_integration_protocol.md` as the original design authority, `.gitignore` line 50 as what excludes it, and Manuel Kurmann as the holder.
- **Source of record** — table of the six artifacts (one unversioned, five versioned) plus an explicit statement of the engine semantics the rationale depends on: `Absolute`/`value: 0` is a hard block; `Relative` acts only on cells above the percentile and rescales by `prob + (prob/100) * Perc_diff`; `Prob_adjust_threshold` is a floor on `|Perc_diff|`, not a magnitude.
- **Parameter rationale by intervention** — 14 rows, one per Allocation entry (BAU 1, NAT 5, CUL 5, SOC 3), each naming a concrete driver quoted from `scenario_narratives.md`, `spatial_masks.md` or the engine. No row is a bare protocol citation (`grep -c "| per protocol |"` = 0).
- **Non-obvious choices** — three paragraphs: the `Outside`-zone mining geometry (the mask names a *permitted* area and the parameters act on its complement), the `Prob_adjust_threshold: 5` no-op guarantee, and why BAU conservation is a soft Relative-Decrease rather than Absolute=0 (leakage *is* the "ineffective" narrative).
- **Unresolved** — 6 numbered items, quoted verbatim below.
- **Authoring record** — YAMLs first committed `b01bb71` (2026-06-08), last content change `5d5be7e` (2026-09-22); no `Prob_adjust_*` key or value changed since `b01bb71`.

### YAML header rewrites (comment lines only)

Each of the four headers now keeps the statement that the protocol is unversioned and the original authority, and adds that "the rationale for every parameter below is recorded in-repo in docs/spatial_interventions/parameter_provenance.md (WR-11 / D-16)". BAU's second dangling citation (the `§3.7` mountain-to-coast reference on line 24) now also points at `## Unresolved` item 4, which states exactly what the in-repo record cannot reconstruct.

Verified byte-safety of the entries: `git diff -U0 config/ | grep '^[+-]' | grep -v '^[+-][+-]' | grep -vc '^[+-][[:space:]]*#'` returns **0** — every changed line is a comment. `resolve_intervention_masks("config", "config", <scn>, <10 posterior years>)` still resolves for all four scenarios (BAU 10 rows/1 mask, NAT 48/6, CUL 50/6, SOC 30/5, no slashes in any `mask_name`).

### `tests/testthat/test-intervention-provenance.R` (46 assertions, 0 failures)

1. `WR-11: every scenario YAML cites the in-repo provenance record`
2. `WR-11: no dangling protocol citation without the in-repo record` — every line matching `spatial_interventions_integration_protocol` or a section sign must be a comment in a file that also names the provenance path
3. `WR-11: the provenance record exists and covers every Allocation intervention` — parses each YAML with `yaml::yaml.load_file()`, collects every `Intervention_ID` whose `Intervention_stage` is `Allocation`, asserts each appears literally in the record, and guards against vacuous passing with `expect_gt(covered, 0L)`

The third is the D-23 would-have-caught-it gate. Demonstrated non-destructively in a sandbox: it finds 14/14 Allocation interventions covered by the committed record, fails when the record is absent, and fails for a hypothetical new intervention (`Peatland_no_conversion`) with no rationale entry.

### `scripts/validate_intervention_masks.r` (IN-05)

- Both `grepl("[.]tif$", top)` uses replaced with `grepl("[.]tiff?$", top, ignore.case = TRUE)`, so `.TIF`, `.tiff` and `.TIFF` are classified as mask candidates instead of silently falling into the local-only bucket.
- `referenced_names` stays exact (the resolver already rejects anything that is not a bare filename), but the orphan comparison is lower-cased: `orphans <- sort(tifs[!(tolower(tifs) %in% tolower(referenced_names))])`. A YAML referencing `x.TIF` against a staged `x.tif` is now neither an orphan nor a missing file.
- Report header gains, immediately after the scenario list:
  `> Orphan list is scoped to the scenarios checked in this run (--scenarios); masks referenced only by other scenarios will appear here as orphans.`
  The same sentence is mirrored in the usage docstring under `--scenarios`.
- Exercised on a sandbox mask directory: `{urban_settlement_mask.tif, mining_concessions_mask.TIF, stray_upper.TIFF, stray_lower.tiff}` all classified; orphans = `{stray_lower.tiff, stray_upper.TIFF}`; `mining_concessions_mask.TIF` correctly matched against the referenced `mining_concessions_mask.tif`; local-only = `{notes.md, spatial_masks_misc/}`.

## `## Unresolved` (quoted verbatim from the provenance record)

> ## Unresolved
>
> The following could **not** be reconstructed from in-repo sources and therefore still depend on the
> external `spatial_interventions_integration_protocol.md`:
>
> 1. **The magnitude of the percentile cut.** Every Relative intervention uses
>    `Prob_adjust_intervention_percentile: 90` and `Prob_adjust_non_intervention_percentile: 90`. No
>    in-repo source explains why the top decile rather than the top quintile or the top 5%, and no
>    sensitivity analysis over this value exists in the repository. The rationale rows above justify
>    *that* a percentile cut is used (soft rather than hard constraint) but not the number 90.
> 2. **The value of `Prob_adjust_threshold: 5`.** The *role* of the threshold is derivable from
>    `src/implement_spatial_interventions.R`; the choice of 5 rather than 2 or 10 is not recorded
>    anywhere in the repository.
> 3. **Cross-scenario magnitude calibration.** All Relative interventions across all four scenarios
>    share the identical `90 / 90 / 5` triple, so scenarios are differentiated only by mechanism,
>    zone, mask and timing -- never by strength. Whether that uniformity is a deliberate design
>    decision or an unfinished calibration is not recorded in-repo. The `TODO: differentiate from NAT`
>    notes in the CUL and SOC headers (which propose, for CUL, raising `Prob_adjust_threshold` to 10
>    or lowering the percentile for broader bite) suggest the latter, but the intended target values
>    are not stated anywhere that is versioned.
> 4. **BAU's mountain-to-coast migration.** The BAU header states this is encoded via demand-side
>    regional differences and the fitted models rather than a spatial intervention, and cites a
>    protocol section for the rationale. `scenario_narratives.md` supports the *phenomenon*
>    ("Disruption to water availability and quality in mountainous regions driven by substantial
>    further decline of glacial areas forces migration of the population to the coast") but contains
>    nothing about the modelling choice to handle it demand-side. That argument exists only in the
>    protocol.
> 5. **BAU's protected-area share.** `scenario_narratives.md` is internally inconsistent for BAU: its
>    Characteristics block lists "Protected areas (proportion of Peru under protection): 25% by 2030"
>    while its Ecological Restoration and Protection section states "there is no expansion of
>    conservation areas beyond the existing coverage of 17.88%". The BAU YAML header and
>    `protected_areas_mask_BAU.tif` follow the 17.9% / no-expansion reading. Which figure the protocol
>    intended, and therefore whether the BAU mask is the intended one, cannot be settled from in-repo
>    sources.
> 6. **The protocol section mapping itself.** With the protocol outside the repository there is no way
>    to verify that the cited sections are the ones that actually specify these entries, or that the
>    protocol has not since been revised away from what is committed here.

Item 5 is a **new finding surfaced by this plan**: `scenario_narratives.md` contradicts itself on BAU's protected-area share (25% by 2030 in the Characteristics block vs. no expansion beyond 17.88% in the Ecological Restoration section). The shipped BAU mask follows the 17.9% reading. This needs the scenario author to adjudicate; it is not resolvable in-repo and was not raised in `05-REVIEW.md`.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 1 - Bug] The plan's (and `05-REVIEW.md` WR-11's) description of the CUL mining entry does not match the shipped config**

- **Found during:** Task 1
- **Issue:** Task 1 required a paragraph explaining "the CUL `Mining_outside_restraint` combination of `Prob_adjust_zone: Outside` with `Prob_adjust_value: 0` (a national ban on mining outside the concession mask)". In `config/CUL_interventions.yml` that entry is `Prob_adjust_type: Relative` / `Prob_adjust_valency: Decrease` / `Prob_adjust_zone: Outside` and carries **no** `Prob_adjust_value` key at all. The `Outside` + `Prob_adjust_value: 0` combination belongs to NAT's `Mining_freeze_post_2030` and SOC's `Mining_in_low_ES_areas`.
- **Fix:** Wrote the `## Non-obvious choices` paragraph to cover the `Outside`-zone geometry across all three mining entries as they actually are, named `Mining_outside_restraint` explicitly, and added a blockquoted correction note recording the discrepancy with `05-REVIEW.md` so the next reader is not misled by the review text. Writing the plan's version verbatim would have fabricated a parameter that is not in the repository, which Task 1 explicitly forbids.
- **Files modified:** `docs/spatial_interventions/parameter_provenance.md`
- **Commit:** `28a5a55`

**2. [Rule 3 - Blocking] `grep -c "%||%"` acceptance check failed on a comment, not on code**

- **Found during:** Task 2
- **Issue:** The first draft of `test-intervention-provenance.R` mentioned the null-coalescing operator by name twice in its explanatory header comment, so the acceptance check `grep -c "%||%" ... == 0` returned 2 even though the operator is never used.
- **Fix:** Reworded the comment to say "base R's null-coalescing operator (R >= 4.4)". Count is now 0; the test still passes 46/46.
- **Files modified:** `tests/testthat/test-intervention-provenance.R`
- **Commit:** `c911f8d`

### Scope boundary (not fixed)

`tests/testthat/test-prep-paths.R` produces **4 errors** (`cannot open the connection` when reading `src/calibration_predictor_prep.r` and `src/simulation_trans_rates_prep.r`) from a `.repo_root` resolution that does not work in this worktree. These are the 4 errors already in the stated baseline, in files this plan does not touch. Left alone per the scope boundary.

## Verification

| Check | Result |
|---|---|
| `Rscript -e 'l <- readLines("docs/.../parameter_provenance.md"); stopifnot(length(l) >= 60, sum(grepl("^## ", l)) >= 5)'` | `PROVENANCE_OK 159` |
| Rationale table data rows | **14** (matches `grep -c Intervention_ID` sum: BAU 1 + NAT 5 + CUL 5 + SOC 3 = 14) |
| `grep -c "| per protocol |"` in the record | **0** |
| `grep -c "Mining_outside_restraint"` in the record | 4 (paragraph contains both `Outside` and `0`) |
| `grep -c "spatial_interventions_integration_protocol.md"` in the record | 4 |
| `grep -c "docs/.../parameter_provenance.md"` per YAML | BAU 2, NAT 1, CUL 1, SOC 1 (all >= 1) |
| `git diff -U0 config/` non-comment changed lines | **0** |
| `resolve_intervention_masks()` for all 4 scenarios × 10 posterior years | `RESOLVE_OK` (BAU 10/1, NAT 48/6, CUL 50/6, SOC 30/5, no slashes) |
| `testthat::test_file("tests/testthat/test-intervention-provenance.R")` | `FAIL 0 | WARN 0 | SKIP 0 | PASS 46` |
| `grep -c "%||%" tests/testthat/test-intervention-provenance.R` | **0** |
| `Rscript -e 'invisible(parse("scripts/validate_intervention_masks.r"))'` | `PARSE_OK` (exit 0) |
| `grep -c 'grepl("\[.\]tif\$"'` in the validator | **0** |
| `grep -c 'tiff?\$'` / `grep -c "ignore.case = TRUE"` | **2** / **2** |
| `grep -c "Orphan list is scoped to the scenarios checked in this run"` | **2** (report header + usage docstring) |
| `grep -c "compareGeom"` before / after | **10 / 10** (D-12/D-13 contract untouched) |
| `grep -c "resample("` / `grep -c "project("` in the validator | **0** / **0** |
| Sandbox orphan classification (`.tif`/`.TIF`/`.tiff`/`.TIFF` + dir + `.md`) | `ORPHAN_CLASSIFICATION_OK` |
| `testthat::test_dir("tests/testthat")` | **PASS 407, FAIL 0, SKIP 13, ERROR 4** |

Baseline was 361 pass / 13 skip / 0 fail / 4 error. Delta: **+46 passes** (exactly the new test file), **no new failures, no new errors, no new skips**.

## Requirements

| ID | Status |
|---|---|
| WR-11 | Closed — every parameter has an in-repo justification or is named under `## Unresolved`; the coverage gate fails if an Allocation intervention has no rationale entry |
| IN-05 | Closed — extension matching is case-insensitive, orphan comparison is case-normalised, and the scoped-orphan caveat appears in both the usage text and the generated report |
| D-16 | Honoured — provenance recorded in-repo; the external protocol was not imported |
| D-23 | Satisfied — `WR-11: the provenance record exists and covers every Allocation intervention` is the would-have-caught-it gate |

## Threat Model Outcomes

| Threat ID | Disposition | Outcome |
|---|---|---|
| T-05-28 (Repudiation — parameter provenance) | mitigate | Mitigated. All 14 shipped parameters reviewable from the repo; an unjustified new intervention is now a test failure |
| T-05-29 (Tampering — validator orphan classification) | mitigate | Mitigated. An unreferenced mask cannot hide behind an upper-case extension; a narrowed run states its own scope in its report |
| T-05-30 (Information disclosure — record content) | accept | Honoured. The record paraphrases rationale already in `docs/spatial_interventions/` and the YAML comments; nothing was copied from the unpublished protocol and nothing was invented — gaps went to `## Unresolved` |
| T-05-SC (package-manager installs) | n/a | No dependency added; no `install.packages` / npm / pip / cargo invocation |

## Known Stubs

None.

## Self-Check: PASSED

- `docs/spatial_interventions/parameter_provenance.md` — FOUND
- `tests/testthat/test-intervention-provenance.R` — FOUND
- `28a5a55` — FOUND
- `c911f8d` — FOUND
- `f6b74eb` — FOUND
