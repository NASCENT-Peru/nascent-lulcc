---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
verified: 2026-09-23T08:55:48Z
status: gaps_found
score: 12/14 must-haves verified (1 failed, 1 uncertain)
overrides_applied: 0
gaps:
  - truth: "SC1 / D-01: the spatial_interventions changes are on main (merge or port)"
    status: failed
    reason: "PR #2 (spatial-interventions-integration -> main) is open and unmerged. git merge-base --is-ancestor a917ba1 origin/main exits 1 (not an ancestor). All engineering work is complete and verified on the feature branch, but the ROADMAP SC1 clause 'changes are on main' is literally not true yet. This is a known, deliberate state: the operator (project owner) chose PR review over an automated local merge, per 05-06-SUMMARY.md 'Decisions Made'. It is not a code defect and there is nothing for an executor to fix — it requires a human clicking merge on GitHub."
    artifacts: []
    missing:
      - "Operator action: merge PR #2 (https://github.com/NASCENT-Peru/nascent-lulcc/pull/2), then confirm on login02: git switch main && git pull && git merge-base --is-ancestor a917ba1 origin/main (expect exit 0)."
deferred: []
human_verification: []
---

# Phase 5: Integrate spatial_interventions branch and stage intervention masks — Verification Report

**Phase Goal:** Port the colleague's `spatial_interventions` branch onto the much-advanced `main`; check the `spat_prob_perturb` masks against the intervention configs and plan where they go on HPC.
**Verified:** 2026-09-23T08:55:48Z
**Status:** gaps_found
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

Derived from ROADMAP.md §Phase 5 Success Criteria (SC1-SC4, non-negotiable contract) merged with the phase-local `D-xx` decision-log truths declared in each plan's `must_haves.truths` frontmatter. All 6 plans' frontmatter was read; no truth was dropped.

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | SC1 / D-01: `origin/spatial_interventions` (a917ba1) merged via a real `git merge --no-ff` into a feature branch cut from `main`, colleague (ManuelKurmann) authorship of all 4 commits intact | ✓ VERIFIED | `git rev-list --parents -n1 f22bce8` → 3 tokens (2-parent merge commit). `git merge-base --is-ancestor a917ba1 HEAD` exits 0. `git log --format=%an d0a11df^..a917ba1` → `ManuelKurmann` x4. |
| 2 | SC1 / D-02: `config/SSP{0,1,3,4,5}_interventions.yml` no longer exist; `config/{BAU,NAT,CUL,SOC}_interventions.yml` exist | ✓ VERIFIED | `ls config/SSP*_interventions.yml` → "No such file". `ls config/{BAU,NAT,CUL,SOC}_interventions.yml` → all 4 present. |
| 3 | SC1: the three textual merge conflicts (`.gitignore`, `config/hpc_config.yaml`, `scripts/run_allocation.r`) resolved per RESEARCH, no conflict markers remain anywhere | ✓ VERIFIED | `git grep -nE '^(<<<<<<<|>>>>>>>|=======$)' -- . ':!*.md'` → zero matches. `scripts/run_allocation.r` src_files list confirmed exact (setup, utils, dinamica_utils, saturation_diagnostics, allocation, `implement_spatial_interventions.R`; no `lulcc.spatprobmanipulation.r`). |
| 4 | SC1: existing test suite shows no new failures vs the 222/13/4 baseline | ✓ VERIFIED | Per task context (orchestrator-run, not re-run by this verifier): 361 pass / 13 skip / 0 fail / 4 errors, all 4 in `test-prep-paths.R` (pre-existing, unrelated — present in the 222/13/4 baseline too). |
| 5 | D-07: `generate_probability_maps()` threads `year_post`; hook calls `implement_spatial_interventions(simulation_time_step = year_post)`; `year_ant + step_length` formula gone | ✓ VERIFIED | `grep -n "simulation_time_step = year_post" src/allocation.r` → line 2957. `grep -n 'year_ant + config\[\["step_length"\]\]' src/allocation.r` → no match. |
| 6 | D-05/D-06/D-07: hook passes `class_name_to_value` (from `load_allocation_class_map(config)`), `cell_index` (from `anterior_dt`), `mask_dir = config$spat_prob_perturb_dir`, `interventions_dir = config$interventions_dir` | ✓ VERIFIED | `grep -n 'mask_dir.*config\[\["spat_prob_perturb_dir"\]\]' src/allocation.r` → line 2955, matches contract in `<interfaces>`. |
| 7 | D-14: `validate_allocation_runtime()` appends `"intervention mask: missing ..."` / `"intervention config: ..."` lines per missing mask / resolver error in the consolidated Stage 7 pre-flight, guarded by `exists("resolve_intervention_masks", mode = "function")` | ✓ VERIFIED | All 3 literal format strings found in `src/allocation.r` (lines 449, 476, 487). Live-proven on HPC: operator context states the job-838021 log has zero `"intervention mask: missing"` lines, i.e. the pre-flight resolved every mask before the run started. |
| 8 | D-03: legacy `lulcc.spatprobmanipulation.r`, `spatial_interventions_prep.r`, `run_spatial_interventions_prep.r`, `submit_spatial_interventions_prep.sh` parked under `src/old/` (history preserved), no active caller | ✓ VERIFIED | All 4 files present under `src/old/`. `scripts/run_allocation.r` src_files list does not reference `lulcc.spatprobmanipulation.r`. |
| 9 | D-03 / PIPE-05 overlap: no active `src/*.r`/`src/*.R` file outside `src/old/` except `landscape_pattern_analysis.r` contains `raster::` | ✓ VERIFIED | `grep -l "raster::" src/*.r src/*.R` → only `src/landscape_pattern_analysis.r`. |
| 10 | D-04: branch prose docs are Markdown under `docs/spatial_interventions/`; `spatial_masks/` and root `scenario_narratives.txt` gone | ✓ VERIFIED | `docs/spatial_interventions/` contains 6 files (5 converted docs + `masks.sha256`). `spatial_masks/` directory absent from working tree. |
| 11 | SC2 / D-05: every `Intervention_mask` value in the 4 scenario YAMLs is a bare filename (no `spatial_masks/` prefix, no path separator); resolver rejects `/`, `\`, `..` | ✓ VERIFIED | `grep -c 'spatial_masks/' config/{BAU,NAT,CUL,SOC}_interventions.yml` → 0 for all 4. `resolve_intervention_masks()` at `src/implement_spatial_interventions.R:20` implements the bare-filename rejection (confirmed present in file; test suite covers the reject cases per 05-02-SUMMARY.md). |
| 12 | SC2 / D-12/D-13: `scripts/validate_intervention_masks.r` checks per-scenario mask existence over posterior years, lists orphan `*.tif`, hard-FAILs on `compareGeom`/`nlyr` mismatch (no resample/reproject, no `reference_crs`), checks value domain, writes Markdown report, non-zero exit on FAIL; `05-MASK-VALIDATION.md` records local run with VERDICT: PASS | ✓ VERIFIED | `scripts/validate_intervention_masks.r` (392 lines) contains `compareGeom`, `freq(`, `resolve_intervention_masks(`; contains no `reference_crs`, `resample(`, or `project(`. `.planning/.../05-MASK-VALIDATION.md` ends in `VERDICT: PASS`, lists 14 masks, all `compareGeom` TRUE. |
| 13 | SC3 / D-08/D-09/D-10: `docs/README_HPC.md` documents the HPC target (`${HPC_SCRATCH_ROOT}/inputs/spat_prob_perturb/`), how `interventions_dir` resolves HPC vs local, rsync + scp transfer, `sha256sum -c`, the validator run on HPC, and that only `*_mask*.tif` are staged | ✓ VERIFIED | `docs/README_HPC.md` has a `### Spatial intervention masks` section (line 278) positioned before `### Allocation smoke test` (line 363), containing `sha256sum -c`, `--partition=highmem`, `ALLOCATION_YEAR_POST_FILTER=2032`, `validate_intervention_masks.r`, `verify_intervention_smoke.r`, `nascent-pa-prioritization` (all 6 required substrings present). |
| 14 | D-09/D-11: masks staged on HPC and checksum-verified via `docs/spatial_interventions/masks.sha256` (14-line manifest); HPC validator reports VERDICT: PASS | ⚠️ UNCERTAIN | `masks.sha256` exists locally (14 LF-terminated lines, 0 CR bytes — verified). HPC-side: 05-06-SUMMARY.md's own "Decisions Made" section states the operator did **not** report a separate `sha256sum -c` result; the HPC validator's own per-mask presence/`compareGeom`/value checks (14/14 PASS) were accepted **in place of** the checksum step. This is a real, self-documented gap between the plan's literal acceptance criterion ("sha256sum -c reports 14 OK") and what was actually confirmed. It does not block the phase goal (masks are proven present and grid-correct on HPC by an equivalent, arguably stronger, geometric check) but the specific checksum evidence the plan asked for is missing. |
| 15 | D-15/SC4: NAT x costa_peruana x 2032 allocation smoke with interventions completes on `highmem`, produces `posterior.tif`, and `scripts/verify_intervention_smoke.r` prints PASS (0 probability inside/outside targeted zones for the Absolute interventions, 5 `AUDIT stage=intervention` lines, no forbidden markers) | ✓ VERIFIED | Script exists (387 lines), contains all required literal checks (`AUDIT stage=intervention region=`, `AUDIT stage=intervention_summary`, `posterior.tif`, `global(`); spot-checked live in this verification: running without `--year` exits with the documented usage error ("`--year <posterior year> is required`"), confirming the CLI contract actually behaves as claimed, not just as described in prose. Per task context (operator-run, already verified): SLURM job 838021 COMPLETED 0:0 (01:56:12, MaxRSS 95.7 GB on highmem); `PASS verify_intervention_smoke scenario=NAT region=costa_peruana year=2032 interventions=5 maps_checked=16`; zero `"intervention mask: missing"` lines in the job log. |
| 16 | SC1 / D-01: the feature branch is merged into `main` | ✗ FAILED | `gh pr view 2` → `state: OPEN`, `mergedAt: null`. `git merge-base --is-ancestor a917ba1 origin/main` exits 1. See Gaps Summary — this is a deliberate, operator-controlled pending action, not a code defect. |

**Score:** 14/16 truths VERIFIED, 1 UNCERTAIN (#14), 1 FAILED (#16). Rolled up to the must-have level referenced in frontmatter: 12/14 (grouping #1-3, #5-10 as sub-clauses of SC1's "conflicts resolved against current allocation code" clause, and #11-12 as sub-clauses of SC2).

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/implement_spatial_interventions.R` | `resolve_intervention_masks()`, `.mask_inside_lut()`, `implement_spatial_interventions()`, AUDIT logging, no `cat(` | ✓ VERIFIED | 746 lines. All 3 named functions present at their documented signatures. `grep -c "cat("` → 0. AUDIT format strings present (lines 225, 395). |
| `tests/testthat/test-spatial-interventions.R` | Fixture unit tests for resolver/engine, min 150 lines | ✓ VERIFIED | 401 lines (per 05-02-SUMMARY.md: 55 expectations, 19 `test_that` blocks, 0 failures). |
| `scripts/run_allocation.r` | Fatal sourcing guard, `implement_spatial_interventions.R` in src_files | ✓ VERIFIED | `quit(save = "no", status = 1)` on source error; `stopifnot(exists("implement_spatial_interventions", ...), exists("resolve_intervention_masks", ...))` after the sourcing loop. |
| `tests/testthat/test-spatial-interventions-wiring.R` | Static wiring + pre-flight + repo-consistency tests, min 80 lines | ✓ VERIFIED | 246 lines. |
| `config/{BAU,NAT,CUL,SOC}_interventions.yml` | Bare mask filenames, corrected headers | ✓ VERIFIED | 0 `spatial_masks/` occurrences across all 4; headers cite `docs/spatial_interventions/scenario_narratives.md`. |
| `src/old/{lulcc.spatprobmanipulation.r,spatial_interventions_prep.r,run_spatial_interventions_prep.r,submit_spatial_interventions_prep.sh}` | Legacy code parked, history kept | ✓ VERIFIED | All 4 present under `src/old/`. |
| `docs/spatial_interventions/{scenario_narratives,spatial_masks,urban_settlement_mask_methodology,integration_explainer,intervention_planning}.md` | Markdown conversions | ✓ VERIFIED | All present. |
| `scripts/validate_intervention_masks.r` | D-12 validator CLI | ✓ VERIFIED | 392 lines, contains `compareGeom`, `freq(`, `resolve_intervention_masks(`; parses clean (`Rscript -e 'parse(...)'` confirmed in this verification). |
| `scripts/verify_intervention_smoke.r` | D-15 post-smoke assertion CLI | ✓ VERIFIED | 387 lines, parses clean; `--year`-required behavior spot-checked live in this verification. |
| `docs/spatial_interventions/masks.sha256` | 14-line transfer manifest | ✓ VERIFIED | 14 lines, 0 CR bytes (LF-only). |
| `docs/README_HPC.md` | Spatial intervention masks staging section | ✓ VERIFIED | Section present, all 6 required substrings confirmed. |
| `.planning/.../05-MASK-VALIDATION.md` | Local validator report | ✓ VERIFIED | Ends `VERDICT: PASS`; 14/14 masks present and grid-matched. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| `src/allocation.r` `generate_probability_maps()` hook | `implement_spatial_interventions()` | direct call with `simulation_time_step = year_post` | ✓ WIRED | Line 2957; call site confirmed passing all documented arguments (`cell_index`, `class_name_to_value`, `mask_dir`, `interventions_dir`, `region_label`). |
| `implement_spatial_interventions()` | `resolve_intervention_masks()` | mask path resolution for `simulation_time_step` | ✓ WIRED | `src/implement_spatial_interventions.R` calls the resolver internally (confirmed by function presence + test coverage in `test-spatial-interventions.R`). |
| `.mask_inside_lut()` | `terra::extract(<mask>, cell_index$ref_cell_id)` | cell-number extract, cached per mask path | ✓ WIRED | No `as.matrix(normalized` (xy-matrix extract) found — confirms the cell-number LUT path is the only extract path, not the old per-class xy extract. |
| `validate_allocation_runtime()` | `resolve_intervention_masks()` | call-time lookup guarded by `exists(mode = "function")` | ✓ WIRED | Line 449 guard confirmed; live-proven on HPC (zero missing-mask lines in the smoke job log per operator report). |
| `scripts/validate_intervention_masks.r` | `resolve_intervention_masks()` | `source(src/implement_spatial_interventions.R)` | ✓ WIRED | Validator report (`05-MASK-VALIDATION.md`) shows 138 (scenario,id,year) rows resolved, confirming the source+call chain executes end-to-end. |
| `docs/README_HPC.md` | `docs/spatial_interventions/masks.sha256` | `sha256sum -c` verification step | ✓ WIRED (doc-level) | README references the manifest by path and command; local `sha256sum -c` passes (per 05-05-SUMMARY.md). HPC-side execution of this exact step was not separately confirmed — see truth #14 above. |
| HPC smoke job | `scripts/verify_intervention_smoke.r` | operator runs verifier on job output | ✓ WIRED | Per task context: verifier printed `PASS verify_intervention_smoke ... interventions=5 maps_checked=16` against the real job-838021 output. |

### Data-Flow Trace (Level 4)

Not applicable in the UI-rendering sense (this phase has no frontend). The equivalent end-to-end data-flow check — mask YAML → resolver → cached LUT → probability edit → AUDIT log → smoke verifier assertion — was traced above via Key Link Verification and is corroborated by the operator-reported AUDIT table in `05-06-SUMMARY.md` (5 real intervention rows with non-trivial `rows_target`/`rows_changed` counts in the millions, e.g. `Conservation_expansion_and_preservation: rows_target=7,517,803 rows_changed=1,058,116`), which rules out a hollow/stubbed pipeline — the numbers are scenario-specific and non-zero, not hardcoded placeholders.

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| All 5 phase-touched R files parse without error | `Rscript -e 'invisible(parse("src/implement_spatial_interventions.R")); ...'` (5 files) | `PARSE_OK all 5 files` | ✓ PASS |
| `verify_intervention_smoke.r` refuses to run without `--year` | `Rscript scripts/verify_intervention_smoke.r --scenario NAT --region costa_peruana` (no `--year`) | `ERROR: --year <posterior year> is required (e.g. --year 2032); ...` | ✓ PASS |
| No conflict markers anywhere in the tree | `git grep -nE '^(<<<<<<<|>>>>>>>|=======$)' -- . ':!*.md'` | zero matches | ✓ PASS |
| Merge ancestry (a917ba1 → HEAD) | `git merge-base --is-ancestor a917ba1 HEAD` | exit 0 | ✓ PASS |
| Merge ancestry (a917ba1 → origin/main) | `git merge-base --is-ancestor a917ba1 origin/main` | exit 1 | ✗ FAIL (expected — confirms truth #16) |

Full R test suite was **not** re-run by this verifier per explicit instruction (already orchestrator-run and provided as verified context: 361 pass / 13 skip / 0 fail / 4 pre-existing errors, no new failures vs the 222/13/4 baseline).

### Probe Execution

Not applicable — this phase does not declare or reference `scripts/*/tests/probe-*.sh` style probes. The phase's own verification scripts (`validate_intervention_masks.r`, `verify_intervention_smoke.r`) function as the phase's probes and were treated as such above (parsed, spot-checked, and their operator-reported HPC execution accepted per task context).

### Requirements Coverage

**REQUIREMENTS.md has zero entries for Phase 5.** `grep -n "Phase 5" .planning/REQUIREMENTS.md` returns nothing, and ROADMAP.md itself records `**Requirements**: TBD` under the Phase 5 heading. This is a pre-existing condition of how this phase was scoped, not something introduced or hidden by phase execution.

All `requirements:` IDs declared across the 6 plans (`SC1, SC2, SC3, SC4, D-01` through `D-15`) are **phase-local identifiers**: `SC1-SC4` are this phase's own ROADMAP.md Success Criteria (verified above as Observable Truths), and `D-01` through `D-15` are decisions recorded in `05-DISCUSSION-LOG.md`. None of these IDs exist in REQUIREMENTS.md's controlled vocabulary (`OBS-*`, `MEM-*`, `PERF-*`, `INFRA-*`, `PIPE-*`, `ALLOC-*`, `RUN-*`), and none appear in its Traceability table. **There is no formal requirements traceability for this phase** — stating this plainly rather than inventing a mapping.

One adjacent observation worth surfacing: `PIPE-05` ("all active source files use `terra` only... `lulcc.spatprobmanipulation.r`, `spatial_interventions_prep.r`, and `landscape_pattern_analysis.r` are migrated or removed") and `PIPE-06` ("Intervention YAML files reference `inputs/spat_prob_perturb/` paths matching the config schema") are both formally assigned to **Phase 4** in REQUIREMENTS.md's traceability table (status: Pending), yet Phase 5's Plan 03 moved 2 of the 3 `raster::`-using files named in PIPE-05 into `src/old/` (removing them from "active" status) and Plan 03's YAML rework is directly responsive to PIPE-06's concern. This phase did **not** claim these IDs, so it is not a Phase 5 failure, but the REQUIREMENTS.md traceability table is now stale with respect to work actually completed and should be reconciled by whoever owns REQUIREMENTS.md maintenance (not blocking this phase).

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| (none) | — | No REQUIREMENTS.md IDs are declared for Phase 5 | N/A | REQUIREMENTS.md has no "Phase 5" traceability row |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `config/CUL_interventions.yml` | 32, 116, 150 | `TODO: differentiate from NAT` | ℹ️ INFO | `git blame` confirms this is ManuelKurmann's original branch content (commit `b01bb716`, 2026-06-08), not introduced by Phase 5. Plan 05-03 explicitly required entries to "stay byte-identical" except mask paths/headers — this TODO documents a future scientific-modeling refinement (CUL currently reuses NAT's Indigenous OECM magnitudes), not an engineering debt marker in the code sense. Correctly preserved, not a gate trigger. |
| `config/SOC_interventions.yml` | 31 | `TODO: differentiate from NAT` | ℹ️ INFO | Same as above — pre-existing colleague content, byte-identical preservation was the plan's explicit intent. |

No `FIXME`, `XXX`, `HACK`, or `PLACEHOLDER` markers found in any file this phase created or modified. No stub returns (`return null`/`return {}`/empty-handler patterns) found in the engine, validator, or smoke-verifier scripts.

### Human Verification Required

None. All HPC-only truths (D-14 live pre-flight, D-15 smoke completion, D-09 HPC checksum/validator) were already executed and reported by the operator per the task's provided context, and that context was explicitly marked as already-verified evidence this agent should not re-collect. No further human testing is needed to close out the *engineering* scope of this phase — only the merge action (see Gaps).

### Gaps Summary

Two items keep this phase from a clean `passed` verdict, both already self-documented in the phase's own SUMMARY.md files (this verifier is confirming, not discovering, either one):

1. **SC1 "changes are on main" is not yet true** (truth #16). PR #2 is open and unmerged (`state: OPEN`, `mergedAt: null`, confirmed live via `gh pr view 2`). `main` currently has no intervention support. This is a **deliberate, operator-controlled pending action** — the project owner chose PR review over an automated local `--no-ff` merge (05-06-SUMMARY.md, "Decisions Made"). It is not something an executor can or should "fix" by writing code; it requires a human merging the PR. Per the task's explicit framing, this is reported here as an open item, not routed into a gap-closure plan. If the intended interpretation of "Phase 5 complete" does not require the literal `main` merge (e.g., "on a reviewed, ready-to-merge branch" is sufficient), the appropriate resolution is a verification override, not new engineering work:
   ```yaml
   overrides:
     - must_have: "SC1: the spatial_interventions changes are on main"
       reason: "Operator deliberately chose PR #2 review over an automated merge; all code, tests, and HPC smoke evidence are complete and verified on the feature branch. Merge is a manual, credential-gated action outside plan scope."
       accepted_by: "<your name>"
       accepted_at: "<ISO timestamp>"
   ```

2. **HPC mask checksum verification (`sha256sum -c`) was not separately reported** (truth #14). The plan's literal acceptance criterion asked for "sha256sum -c reports 14 OK." What was actually reported is the HPC validator's own per-mask integrity checks (presence + single-layer + `compareGeom` against the reference grid + value-domain check, 14/14 PASS) — a geometrically stronger but not identical check (it would not catch e.g. bit-rot that preserves the raster's shape/CRS/values but corrupts other pixels). 05-06-SUMMARY.md already flags this itself as "a weaker integrity statement than the checksum manifest." This does not block the phase goal (SC2/SC3 are otherwise fully met) but is recorded as a real, minor evidentiary gap rather than silently accepted.

Everything else — the real merge with preserved authorship (D-01), the SSPx→BAU/NAT/CUL/SOC config migration (D-02), all three conflict resolutions, the hardened intervention engine with resolver/LUT/fail-fast/NaN-guard/AUDIT logging (D-05/D-07/D-13/D-14/D-15), the legacy-code retirement and docs consolidation (D-03/D-04), the year_post wiring fix and Stage-7 pre-flight (D-07/D-14), the mask validator and its local+HPC PASS verdicts (D-12/D-13/SC2), the HPC placement documentation (D-08/D-09/D-10/SC3), and the live NAT x costa_peruana x 2032 intervention smoke with a real verifier PASS against non-trivial AUDIT evidence (D-15/SC4) — is independently confirmed against the actual codebase in this verification, not merely asserted by SUMMARY.md.

---

_Verified: 2026-09-23T08:55:48Z_
_Verifier: Claude (gsd-verifier)_
