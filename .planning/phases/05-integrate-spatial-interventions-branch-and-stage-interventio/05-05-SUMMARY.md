---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
plan: 05
subsystem: spatial interventions / validation tooling / HPC runbook
tags: [interventions, masks, validator, sha256, hpc, smoke, docs]
requires: ["05-02", "05-03"]
provides:
  - scripts/validate_intervention_masks.r (D-12/D-13 validator, rerunnable on HPC)
  - scripts/verify_intervention_smoke.r (D-15 post-smoke assertions)
  - docs/spatial_interventions/masks.sha256 (14-line transfer manifest)
  - README_HPC "Spatial intervention masks" section (D-08/D-09/D-10, SC3)
  - 05-MASK-VALIDATION.md (SC2, local run, VERDICT PASS)
affects: ["05-06 (operator runs the documented staging, validator and smoke commands on HPC)"]
tech-stack:
  added: []
  patterns: [collect-all-then-verdict CLI, newest-log AUDIT selection, crop-not-resample mask alignment]
key-files:
  created:
    - scripts/validate_intervention_masks.r
    - scripts/verify_intervention_smoke.r
    - docs/spatial_interventions/masks.sha256
    - .planning/phases/05-integrate-spatial-interventions-branch-and-stage-interventio/05-MASK-VALIDATION.md
  modified:
    - docs/README_HPC.md
    - .gitattributes
decisions:
  - "Smoke verifier judges the newest worker log carrying an AUDIT intervention_summary line for the scenario/year, so reruns that append new per-PID logs do not double-count AUDIT lines"
  - "The mlr3 '.__Task__col_info' fallback line is whitelisted from the 'could not find function' forbidden-marker check (it is logged on every region)"
  - "masks.sha256 forced to eol=lf via .gitattributes because core.autocrlf=true would check it out with CRLF on Windows"
metrics:
  duration: ~35min
  completed: 2026-09-22
  tasks: 3
  files: 6
---

# Phase 5 Plan 05: Mask validator, smoke verifier and HPC staging docs Summary

Two new CLIs, a checksum manifest and a runbook section. The validator ran locally and gave VERDICT: PASS: 14/14 referenced masks present, 14/14 on the exact reference grid, values {1, NA}, 0 orphans. The smoke verifier correctly FAILs on the existing pre-intervention `NAT/2032/region_costa_peruana` output, which serves as the negative control.

## Tasks

| # | Task | Commit |
|---|------|--------|
| 1 | D-12 validator + local report | ffef325 |
| 2 | D-15 smoke assertion script | 7c9cd96 |
| 3 | sha256 manifest + README_HPC section | de8e23f |

## What was built

- **`scripts/validate_intervention_masks.r`**: uses the same bootstrap as the other scripts. Sourcing errors are fatal (exit 2). Accepts `--out/--mask-dir/--ref-grid/--interventions-dir/--scenarios` as `--flag value` or `--flag=value`. An unknown flag prints usage and exits 2. For each scenario it calls `resolve_intervention_masks()` over the posterior years (`tail(simulation_year_steps, -1)`) and reports resolver errors as FAIL `config` and missing files as FAIL `missing`. For each distinct mask it checks `nlyr == 1` and `compareGeom` against the ref-grid raster (hard FAIL), runs `freq()` to confirm values are within {0,1}, and counts mask==1 cells outside the ref grid (INFO). Top-level `*.tif` files not referenced by any YAML are reported as orphans (WARN). Other entries are listed as local-only. It writes a Markdown report and exits 0 on PASS, 1 on FAIL. It never resamples or projects, and never reads `reference_crs`.
- **`05-MASK-VALIDATION.md`**: 138 (scenario, id, year) references, 14 distinct masks, all present and all compareGeom TRUE (EPSG:4326, 0.000808484°, 24192 x 16579). Local-only entries: `.DS_Store`, `nascent-pa-prioritization/`, `README.txt`, `spatial_masks/`, `urban_settlement_mask_methodology.txt`. The run took about 9 minutes locally, dominated by `freq`/`global` over the 14 national rasters.
- **`scripts/verify_intervention_smoke.r`**: `--year` is required; without it the script exits 2 and names `--year`. It checks (a) `posterior.tif`, and (b) that the newest log with a summary line has exactly N `AUDIT stage=intervention region=` lines (N = active interventions from the resolver, 5 for NAT 2032), the same id set, and one summary line. For (c), each active Absolute/0 intervention has its trans_rates rows (To in targets, From in filter if given) mapped to `%03d_id_trans_%d.tif`. The national mask is cropped to the map, and the script asserts `global(r * zone, "max") == 0` for Inside, or for Outside where the mask is not 1. (d) Forbidden log markers, optionally also checked in `--extra-log` (the SLURM stdout). saturation_summary placed-vs-demanded is printed for information only. The header documents Pitfall 4.
- **`docs/spatial_interventions/masks.sha256`**: computed fresh from the local files and identical to the RESEARCH manifest after sorting. The Git Bash binary marker `*` was normalised to the standard two-space format. The file is LF-only, and `sha256sum -c` passes locally.
- **README_HPC "### Spatial intervention masks"** (placed before "### Allocation smoke test"): covers placement via `data_basepath` + `spat_prob_perturb_dir`, how `interventions_dir` resolves, what to stage (14 `*_mask*.tif`), rsync plus the scp fallback, `sha256sum -c`, the validator on HPC, the `sbatch --partition=highmem ... ALLOCATION_YEAR_POST_FILTER=2032` smoke followed by `verify_intervention_smoke.r`, and notes on highmem/fat, overwrite, mechanism-only output and obsolete prior outputs.

## Verification

- Validator local run: exit 0, `VERDICT: PASS`. Negative test with an empty `--mask-dir`: exit 1, `VERDICT: FAIL (138 failures)`. Unknown flag: exit 2 with usage.
- Smoke verifier: parses. `--help` exits 0. With no `--year` it exits 2 and names `--year`. Against the local pre-intervention `NAT/2032/region_costa_peruana` it exits 1: no summary AUDIT line, PA-inside probabilities up to 0.975 for targets 104/105/106, and mining probabilities outside concessions up to 0.986. That is the expected negative control. It took about 13 minutes locally for 9 maps.
- Test suite (testthat::test_dir, silent reporter): 361 expectations passed, 13 skipped, 0 failed, 4 errors, all in `test-prep-paths.R` (pre-existing). No test or `src/` file was touched in this plan. The 361 vs 365 difference from the 05-04 count is most likely a counting difference (expectations vs tests), because no code on any tested path changed.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] False-positive forbidden marker in the smoke verifier**
- **Found during:** Task 2 (local negative-control run)
- **Issue:** Every allocation worker log contains `mlr3 predict_newdata() failed (could not find function ".__Task__col_info"); falling back to direct model prediction`. This is the known, deterministic ranger fallback. It matched the `could not find function` forbidden marker, so the real HPC smoke would always have FAILed.
- **Fix:** Lines containing `.__Task__col_info` or `falling back to direct model prediction` are excluded before the forbidden-marker scan. Confirmed on the real log: 24 benign lines excluded, 0 remaining hits.
- **Files modified:** scripts/verify_intervention_smoke.r
- **Commit:** 7c9cd96

**2. [Rule 1 - Robustness] AUDIT count judged on the newest log only**
- **Issue:** Worker logs are named per PID and accumulate in `worker_logs/`, so a rerun of the smoke would double the AUDIT count across all logs.
- **Fix:** The script evaluates the newest log that carries a matching `AUDIT stage=intervention_summary` line, and prints a note when more than one log has one.
- **Commit:** 7c9cd96

**3. [Rule 2 - Correctness] `.gitattributes` eol=lf for the manifest**
- **Issue:** `core.autocrlf=true` together with `* text=auto` would check the manifest out with CRLF on Windows.
- **Fix:** Added `docs/spatial_interventions/masks.sha256 text eol=lf`. The committed blob has 0 CR characters.
- **Commit:** de8e23f

**4. [Minor] Additions to the verifier interface:** `--extra-log <path>` (optional, scans the SLURM stdout for forbidden markers), `--help`, and exit 2 for usage errors. Mask/map alignment after crop is also asserted with `compareGeom`.

## Operator notes for 05-06

- The local `config/local_config.yaml` `data_basepath` is `E:/...`, but data lives on `D:`. Local runs therefore pass `--mask-dir`/`--ref-grid`/`--output-root` explicitly. On HPC the defaults from `hpc_config.yaml` apply, so no flags are needed.
- Expect the HPC validator to take several minutes (about 9 minutes locally) and the smoke verifier to take about 10-15 minutes for costa_peruana.
- Pass `--extra-log logs/lulc-allocation-smoke-<job_id>.out` to the verifier so that SLURM stdout is scanned as well.
- Existing `NAT/2032/region_costa_peruana` on HPC is a pre-intervention output. Move it aside before the smoke if you want to keep it.

## Known Stubs

None.

## Threat Flags

None. T-05-16 (sha256 manifest + `sha256sum -c`), T-05-17 (compareGeom hard FAIL, no resampling) and T-05-19 (PASS declared by raster and log assertions) are implemented as planned.

## Self-Check: PASSED

- FOUND: scripts/validate_intervention_masks.r, scripts/verify_intervention_smoke.r, docs/spatial_interventions/masks.sha256, 05-MASK-VALIDATION.md, docs/README_HPC.md section
- FOUND commits: ffef325, 7c9cd96, de8e23f
