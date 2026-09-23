---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
plan: 06
subsystem: hpc operations / spatial interventions / verification gate
tags: [interventions, masks, hpc, slurm, highmem, smoke, verification, operator-gate]
requires: ["05-04", "05-05"]
provides:
  - HPC-staged intervention masks in /beegfs/black/nascent-lulcc/inputs/spat_prob_perturb (14 *_mask*.tif)
  - HPC validator report logs/intervention_mask_validation_hpc.md (VERDICT PASS, 14/14)
  - Verified NAT x costa_peruana x 2032 intervention smoke (SLURM job 838021, COMPLETED 0:0)
  - "PASS verify_intervention_smoke scenario=NAT region=costa_peruana year=2032 interventions=5 maps_checked=16"
  - "Operational fact: intervention runs peak at ~95.7 GB RSS, so --partition=highmem is mandatory"
  - "Operational fact: .env must be sourced in the submitting shell; --export=ALL alone is not sufficient"
  - PR #2 (spatial-interventions-integration into main), open, awaiting operator merge
affects: ["any future phase running allocation with interventions on HPC", "post-merge main-branch work"]
tech-stack:
  added: []
  patterns: [operator-gated HPC verification, verdict-by-assertion-script, source-dotenv-before-sbatch]
key-files:
  created:
    - .planning/phases/05-integrate-spatial-interventions-branch-and-stage-interventio/05-06-SUMMARY.md
  modified: []
key-decisions:
  - "Landing on main is done via PR #2 for review rather than a local --no-ff merge; the merge itself is left to the operator, so SC1/D-01 (changes on main) is NOT yet satisfied"
  - "The HPC validator's own grid/value checks on all 14 masks are accepted as the integrity gate for this plan; a separate sha256sum -c result was not reported back"
  - "Intervention allocation runs are classified as highmem-only workloads (95.7 GB observed MaxRSS vs the 93 GB compute partition limit)"
patterns-established:
  - "Pre-sbatch ritual on HPC: source .env, then bash scripts/hpc_common.sh --check-stage7-contract, then sbatch --partition=highmem"
requirements-completed: [SC3, SC4, D-08, D-09, D-11, D-14, D-15]
duration: ~2h (operator-side, dominated by the 01:56:12 smoke job)
completed: 2026-09-23
---

# Phase 5 Plan 06: HPC staging, intervention smoke and hand-off Summary

**14 masks staged and validated on HPC (VERDICT PASS), and the NAT x costa_peruana x 2032 allocation smoke with 5 active interventions completed on highmem (job 838021, 01:56:12, MaxRSS 95.7 GB) with `PASS verify_intervention_smoke ... interventions=5 maps_checked=16`; the branch is on PR #2 but NOT yet merged into main.**

## Performance

- **Duration:** ~2h operator-side (the smoke job alone ran 01:56:12) plus the local pre-hand-off gate
- **Completed:** 2026-09-23
- **Tasks:** 3 (1 autonomous gate, 2 operator checkpoints)
- **Files modified:** 0 source files — this plan produced no repository code changes

## Accomplishments

- **Local gate (Task 1):** branch `spatial-interventions-integration` at `8c95583`, working tree clean outside `.planning/`. Test suite: 361 expectations passed, 13 skipped, 0 failed, 4 errors — all 4 in `test-prep-paths.R` and pre-existing, so no new failures vs the hand-off baseline. `05-MASK-VALIDATION.md` ends in `VERDICT: PASS`. 25 commits ahead of `main` (`d0a11df`..`8c95583`, including the `f22bce8` merge of `a917ba1`).
- **HPC staging (Task 2, D-08/D-09/D-10/D-11):** branch pushed to origin; login02 checkout at `8c95583`, matching local HEAD (closes T-05-21). All 14 `*_mask*.tif` staged into `/beegfs/black/nascent-lulcc/inputs/spat_prob_perturb`.
- **HPC validation:** `Rscript scripts/validate_intervention_masks.r --out logs/intervention_mask_validation_hpc.md` gave **VERDICT: PASS** — all 14 masks present, on the exact reference grid, values in {1, NA}. `interventions_dir` resolved to `/home/black/nascent-lulcc/config`, `mask_dir` to `/beegfs/black/nascent-lulcc/inputs/spat_prob_perturb`. 13 INFO "outside-ref" rows (mask==1 cells where the reference grid is NA, i.e. outside the model domain — harmless): e.g. mining 48,270; protected_areas_mask_BAU 235,814; urban 24,396.
- **Intervention smoke (Task 3, D-15/SC4):** SLURM job **838021**, submitted as `sbatch --partition=highmem --export=ALL,ALLOCATION_PROFILE_SCENARIO=NAT,ALLOCATION_REGION_FILTER=costa_peruana,ALLOCATION_YEAR_POST_FILTER=2032 scripts/submit_allocation_smoke.sh`. `sacct`: `838021 COMPLETED 0:0 Elapsed 01:56:12`; `838021.batch COMPLETED 0:0 MaxRSS 95687088K` (~95.7 GB).
- **D-14 proven live:** `grep "intervention mask: missing"` on the job log returns nothing — the Stage 7 pre-flight resolved every mask on HPC.
- **Verifier PASS (SC4):** `Rscript scripts/verify_intervention_smoke.r --scenario NAT --region costa_peruana --year 2032 --extra-log logs/lulc-allocation-smoke-838021.out` printed
  `PASS verify_intervention_smoke scenario=NAT region=costa_peruana year=2032 interventions=5 maps_checked=16`

## Task Commits

This plan changed no source files. Task 1 was verification-only; Tasks 2 and 3 were operator-executed on HPC.

1. **Task 1: Pre-hand-off gate on the feature branch** — no commit (checks only)
2. **Task 2: HPC mask staging and validation** — operator action, no repository commit
3. **Task 3: Smoke, verification, landing** — operator action; PR #2 opened, merge pending

**Plan metadata:** the `docs(05-06)` commit carrying this SUMMARY, STATE.md and ROADMAP.md.

## Operator-reported evidence

### The 5 AUDIT intervention lines (from `worker_1045557_costa_peruana.log`)

| rank | intervention | mode | zone | to_vals | mask | rows_target | rows_changed |
|---|---|---|---|---|---|---|---|
| 1 | Conservation_expansion_and_preservation | Absolute | Inside | 105,104,106 | protected_areas_mask_NAT_phase1_target.tif | 7,517,803 | 1,058,116 |
| 2 | Mining_freeze_post_2030 | Absolute | Outside | 106 | mining_concessions_mask.tif | 15,195,834 | 612,202 |
| 3 | Indigenous_land_OECM_forest | Relative | Inside | 105,104,106 | indigenous_lands_mask.tif | 12,393,402 | 95,750 |
| 4 | Indigenous_land_OECM_ag | Relative | Inside | 104 | indigenous_lands_mask.tif | 1,925,778 | 45,976 |
| 5 | Urban_densification | Relative | Inside | 105 | urban_settlement_mask.tif | 9,133,978 | 523,705 |

Plus one `AUDIT stage=intervention_summary` line: `n_interventions=5 cells_sum_gt1=31974`.

`cells_sum_gt1=31974` is the D-07 non-renormalisation diagnostic — roughly 32k cells where the post-intervention per-cell transition probabilities sum above 1, expected where the Conservation, Indigenous and Urban zones overlap. The engine logs this and deliberately does not renormalise, per the 05-02 decision.

### Raster assertions

16 probability maps were checked and every one reported `max prob in zone = 0 [ok]` — 12 for the Conservation Inside-Absolute targets and 4 for the Mining Outside-Absolute target. This is the core SC4 claim: the Absolute interventions actually zero the probability inside/outside their zones on real HPC output, not just in fixtures.

`placed vs demanded` was within 1 cell on every transition (integer rounding, not an intervention-induced shortfall). No saturation degeneracy.

## Decisions Made

- **PR over local merge.** The user chose to open PR #2 (https://github.com/NASCENT-Peru/nascent-lulcc/pull/2, base `main`, head `spatial-interventions-integration`) for review rather than merging locally. The merge has not happened. SC1/D-01 ("the feature branch is merged into main") is therefore recorded as **pending**, not satisfied.
- **Checksum gate.** The plan's acceptance criterion asked for `sha256sum -c` reporting 14 OK. The operator did not report a separate `sha256sum -c` line. What was reported is the HPC validator's own per-mask checks — presence, single layer, `compareGeom` against the reference grid, value domain — passing on all 14. That is accepted here as sufficient evidence of intact transfer, but it is a weaker integrity statement than the checksum manifest and is recorded as such.
- **Intervention allocation is a highmem workload.** Observed MaxRSS of 95.7 GB exceeds the 93 GB compute partition. `--partition=highmem` (or fat) is mandatory for any intervention run, not just for costa_peruana.

## Deviations from Plan

### Operator-environment issue resolved during execution

**1. [Rule 3 - Blocking] `.env` not sourced in the submitting shell**
- **Found during:** Task 3, first smoke submission
- **Issue:** Job **838019** failed after 2 seconds with `ERROR: Stage 7 path contract incomplete`. `HPC_TMP_ROOT` and `TERRA_TEMP` were unset because `.env` had not been sourced in the submitting shell. That same stale shell also carried `HPC_SCRATCH_ROOT=/beegfs/black` instead of `/beegfs/black/nascent-lulcc`, which would have pointed the mask resolver at the wrong tree.
- **Fix:** `source .env` before `sbatch`, then confirmed with `bash scripts/hpc_common.sh --check-stage7-contract` giving `Stage 7 path contract OK.` and data base path `/beegfs/black/nascent-lulcc`. Resubmitted as job 838021.
- **Why this matters:** `--export=ALL` exports the *submitting shell's* environment. If that shell never sourced `.env`, `ALL` faithfully exports nothing useful. `--export=ALL` is necessary but not sufficient. The Stage 7 pre-flight contract check is what catches it, and it caught it in 2 seconds rather than 2 hours.
- **Files modified:** none (operator environment, not repository state)

### Noted, not fixed

**2. [Out of scope] PROJ database warnings**
- `PROJ: proj_create_from_name: Open of .../share/proj failed (GDAL error 1)` appears in the job log. GDAL falls back to its built-in database and all CRS handling is correct. Pre-existing, unrelated to this phase, out of scope.

**3. [Informational] 13 outside-ref INFO rows in the HPC validator**
- Mask==1 cells falling where the reference grid is NA. These lie outside the model domain and are ignored by the engine, whose cell-number LUT only covers in-domain cells. No action.

---

**Total deviations:** 1 blocking issue resolved (operator environment), 2 informational observations.
**Impact on plan:** No repository changes were needed. The failed first submission cost 2 seconds and produced a durable operational lesson, recorded here and worth folding into the HPC runbook.

## Issues Encountered

- The 2-second failure of job 838019 (above) was the only execution failure, diagnosed and corrected in one iteration.

## Must-Have Status

| Must-have | Status |
|---|---|
| SC1 — no new test failures vs baseline before hand-off | **SATISFIED** — 361 pass / 13 skip / 0 fail / 4 pre-existing errors in `test-prep-paths.R` |
| D-08/D-09/D-11 — masks staged on HPC, validator PASS | **SATISFIED with a caveat** — 14 masks staged, HPC validator `VERDICT: PASS`; a separate `sha256sum -c` result was not reported back |
| D-15/SC4 — smoke completes on highmem, verifier PASS | **SATISFIED** — job 838021 COMPLETED 0:0; `PASS verify_intervention_smoke ... interventions=5 maps_checked=16`; 16/16 maps zero-in-zone |
| D-14 — Stage 7 pre-flight passed, no missing masks | **SATISFIED** — no `intervention mask: missing` lines in the job log |
| SC1/D-01 — feature branch merged into main | **PENDING** — PR #2 is open, not merged. The spatial_interventions changes are NOT on `main` yet |

## Next Phase Readiness

**One action remains before Phase 5 can be called closed: merge PR #2.**

- PR: https://github.com/NASCENT-Peru/nascent-lulcc/pull/2 (base `main`, head `spatial-interventions-integration`, 25 commits)
- After merging, on login02: `git switch main && git pull`, then confirm `git merge-base --is-ancestor a917ba1 origin/main` exits 0.
- Until then `main` has no intervention support, and any HPC run launched from a `main` checkout will silently run without interventions.

Everything else is ready: masks are on scratch and validated, the engine is proven live, and the verifier is a reusable regression gate for future scenarios and regions.

## Known Stubs

None.

## Threat Flags

None new. Register status: T-05-20 (staged-mask integrity) mitigated via the HPC validator's grid/value checks rather than the intended `sha256sum -c` — see Decisions. T-05-21 (code version drift) mitigated: login02 HEAD `8c95583` equals local HEAD. T-05-22 (job OOM) mitigated: highmem used, and the 95.7 GB peak confirms the mitigation was necessary. T-05-23 (output overwrite) accepted: `NAT/2032/region_costa_peruana` was overwritten with a mechanism-proof run whose anterior is the 2022 initial map. T-05-24 (push to main) accepted: the operator retains sole control and chose PR review.

---
*Phase: 05-integrate-spatial-interventions-branch-and-stage-interventio*
*Completed: 2026-09-23 (except the pending main merge)*

## Self-Check: PASSED

- FOUND: 05-06-SUMMARY.md
- FOUND commits referenced: 8c95583 (branch HEAD), f22bce8 (merge), a917ba1 (upstream tip)
- No source files were created or modified by this plan, so there is nothing further to verify on disk. HPC artefacts (staged masks, validator report, job 838021 outputs) are operator-reported and not verifiable from this workstation.
