---
phase: quick-261014-dex
plan: 01
subsystem: hpc-operations
tags: [allocation, hpc, archive, spatial-interventions, CUL, runbook]
requires: [05.1 intervention smoke + verifier, submit_allocation_smoke.sh, submit_verify_intervention_smoke.sh]
provides:
  - scripts/archive_allocation_outputs.sh (dry-run-by-default rename of $SIM_OUT scenario trees)
  - tests/shell/test-archive-allocation-outputs.sh (pure-bash fixture test, 90 checks)
  - 261014-dex-OPERATOR.md (archive + CUL x costa_peruana x 2024 smoke runbook)
  - docs/README_HPC.md "Archiving outputs before a fresh sweep" subsection
affects: [Phase 4 full sweep (submit_allocation_all_scenarios.sh) start conditions]
tech-stack:
  added: []
  patterns: [library-mode bash script (BASH_SOURCE guard), launcher-identical get_config probe, find/stat/mv only]
key-files:
  created:
    - scripts/archive_allocation_outputs.sh
    - tests/shell/test-archive-allocation-outputs.sh
    - .planning/quick/261014-dex-archive-pre-intervention-allocation-outp/261014-dex-OPERATOR.md
  modified:
    - docs/README_HPC.md
decisions:
  - "Archive = same-filesystem rename of $SIM_OUT/SCEN to $SIM_OUT/_archive_TAG/SCEN; nothing copied or deleted, so every move is reversible with mv -T"
  - "Guarded job list extended beyond the plan's five names to every job that writes $SIM_OUT (lulc-allocation, lulc-allocation-profile, lulc-dinamica-only)"
  - "Runbook rate pre-check applies allocation.r's From*!=To* and Rate!=0 filter, because the verifier ledger reads the filtered region trans_rates.csv"
metrics:
  completed: 2026-10-14
  tasks: 2
  commits: 3
  files: 4
---

# Quick 261014-dex: archive pre-intervention allocation outputs + CUL smoke runbook Summary

A guarded, dry-run-by-default archive script now renames each `$SIM_OUT/BAU|NAT|CUL|SOC` tree to `$SIM_OUT/_archive_TAG/...` using only find, stat and mv (with a manifest), and SIM_OUT is resolved with the launcher's own `get_config()` probe. It comes with a 90-check pure-bash fixture test and a machine-grouped runbook for the archive plus a CUL x costa_peruana x 2024 highmem smoke and its batch verifier, with expected values written before the run.

## Commits

| Task | Commit | Message |
|------|--------|---------|
| 1 (RED) | bf67df9 | test(261014-dex): add failing fixture test for archive_allocation_outputs.sh |
| 1 (GREEN) | 3e4532c | feat(261014-dex): add guarded archive script for allocation output trees |
| 2 | 6ae7651 | docs(261014-dex): add archive + CUL costa_peruana 2024 smoke runbook and README_HPC subsection |

Both shell files are committed with mode 100755 and LF endings in the index (checked with `git ls-files -s` and a CR count on the blobs).

## Inputs inside the archived trees

Confirmed by grepping `src/allocation.r` for reads under `sim_dir` / `timestep_dir` / `region_work_dir`:

- `sim_dir <- file.path(config[["simulation_output_dir"]], scenario)` (L1962). `timestep_dir <- file.path(sim_dir, year_post)` (L2199). `region_work_dir <- file.path(timestep_dir, "region_" + suffix)` (L1339).
- Everything written there is produced by the run itself for that timestep: `anterior.tif` (L1349, written from `current_lulc_path`), `worker_logs/` (L2222), `trans_rates.csv` (L2533, a filtered copy of the Stage 4 CSV in `trans_rate_table_dir`), `probability_map_dir/`, the intervention delta CSV, `posterior.tif`, and the national `posterior_YEAR.tif` (L2473).
- The true inputs live outside SIM_OUT: rate tables (`trans_rate_table_dir`, read at L2540), allocation params (`calibration_param_dir`, L2579), focal matrices (`preds_tools_dir`, L1056), models (`transition_model_dir`), masks (`spat_prob_perturb_dir`), and the initial LULC (`aggregated_lulc_dir`).
- The only read from inside a scenario tree is the resume chain (L2068-2110): `region_posterior_path(prev_year_post)` becomes `current_lulc_path`. That previous-year posterior is the stale chain input this task archives on purpose. Resume is disabled when `ALLOCATION_YEAR_POST_FILTER` is set, so the smoke reads nothing from SIM_OUT.

**Conclusion:** nothing in `$SIM_OUT/SCEN` has to be kept for a fresh run. Everything there is regenerated, and moving the whole tree is safe.

## Verification (run locally on the Windows workstation, Git Bash)

- `bash tests/shell/test-archive-allocation-outputs.sh`: **PASS: 90  FAIL: 0**, exit 0. It printed one `SKIP: symlink case (platform cannot create real symlinks)`: Git Bash on Windows cannot create real symlinks, so the symlinked-scenario refusal was **not** exercised locally. It will run wherever `ln -s` makes a real link.
- The plan's Task 1 `<verify>` command, run verbatim from a scratchpad script: `VERIFY_PASS`.
  - `bash -n` passes on both files.
  - The test exits 0.
  - Non-comment lines of the script have 0 hits for terra / rm -r / rm -f / cp -r / rsync / gdal.
  - There is 1 hit for `cfg[['simulation_output_dir']]`.
  - `git diff --quiet` on the launcher and both src files passes.
- The plan's Task 2 `<verify>` command, run verbatim: `VERIFY_PASS`.
  - 0 `<[a-z_]+>` tokens in the runbook. The only `<` characters are awk `<=` and the `<0` / `<= 0` text in the expected-values section.
  - 8 `**Where:**` lines.
  - The literal CUL smoke and verifier commands are present.
  - README_HPC has 4 hits for `archive_allocation_outputs.sh`.
- All 18 fenced `bash` blocks in the runbook were extracted and passed `bash -n`.
- The runbook's step 5b awk rate pre-check was run on a fixture CSV (quoted header, a folder name with a space, one persistence row, one zero-rate row). It printed `To104=1 To105=2 To106=0`, which is correct: both filtered rows were excluded.
- Manual CLI checks (exit codes asserted in the test as well):
  - With no contract it exits 1, with the hpc_common contract message.
  - With `HPC_SCRATCH_ROOT=/beegfs/black` it exits 1, with the stale-root message.
  - `--bogus`, `--tag ../x`, `--scenarios 'NAT FOO'` and a bare `--tag` exit 2.
  - `--help` exits 0.
- A manual fixture run of the dry run, an apply that hit an existing target (refused, nothing moved) and an apply with a fresh tag (moved=2, manifest with 2 rows, 0 posteriors left) produced the expected output.
- `git diff 14f021f HEAD` shows no change to `scripts/submit_allocation_scenario.sh`, `scripts/submit_allocation_smoke.sh`, `src/allocation.r` or `src/implement_spatial_interventions.R`.

**Not verified locally (needs the HPC):**
- `resolve_sim_out` / `main` past the contract check (they need micromamba, allocation_env and R on login02).
- The real `squeue` filter.
- `stat -c %d` across beegfs.
- Every runbook step.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing critical functionality] Guarded job list extended**
- **Found during:** Task 1
- **Issue:** The plan listed five job names (alloc-region, lulc-allocation-smoke, assemble-mosaic, iv-verify, wr04-probe). `scripts/submit_allocation.sh` (lulc-allocation), `submit_allocation_profile.sh` (lulc-allocation-profile) and `submit_allocation_dinamica_only.sh` (lulc-dinamica-only) also run the allocation driver and write `$SIM_OUT`.
- **Fix:** Added all three to `ARCHIVE_GUARDED_JOB_NAMES` and to runbook step 4a.
- **Files:** scripts/archive_allocation_outputs.sh, 261014-dex-OPERATOR.md

**2. [Rule 2] squeue failure fails closed**
- **Found during:** Task 1
- **Issue:** If `squeue` exists but errors (for example the controller is unreachable), an empty listing would let `--apply` proceed.
- **Fix:** `running_allocation_jobs` prints `UNKNOWN squeue-failed rc=N`, so `--apply` refuses. An absent `squeue` still only warns, as planned.

**3. [Rule 2] Extra path and tree guards**
- **Found during:** Task 1
- **Fix:** The following refusals were added. All are tested except the symlink case, which is skipped on Windows.
  - `validate_sim_out` also rejects `.` / `..` components, a non-absolute HPC_SCRATCH_ROOT, and SIM_OUT equal to the scratch root itself. The prefix match is boundary-safe: a sibling such as `scratch2/` is rejected.
  - `validate_scenarios` rejects duplicates.
  - A scenario dir that is a symlink or not a directory is refused before anything moves.
  - The archive root being a symlink or a non-directory is refused.
  - The same-device check runs against SIM_OUT before `mkdir`, and again against the archive root before each `mv`.
  - After the moves, the script recounts launcher-pattern posteriors and fails if any remain.

**4. [Rule 1 - Bug] Runbook rate pre-check mirrors the run's filter**
- **Found during:** Task 2
- **Issue:** The plan's pre-check counted raw `To*` rows. The verifier ledger reads the region's `trans_rates.csv`, which `allocation.r` (L2542-2545) filters to `From* != To*` and `Rate != 0`. Counting raw rows would overstate the chance that Conservation, Mining or Urban get exercised: a persistence row 104->104 counts as To104.
- **Fix:** The awk one-liner applies the same filter. It was tested on a fixture CSV.

**5. [Rule 3 - Implementation detail] `resolve_sim_out` sets a global**
- **Issue:** `setup_common_env` and `activate_env` print to stdout, and activation must run in the current shell, not in a `$(...)` subshell.
- **Fix:** `resolve_sim_out` sets the global `SIM_OUT` and sends the env-setup chatter to stderr. The probe string, `--vanilla`, `2>/dev/null`, first line and `xargs` trim are identical to the launcher.

**6. README undo example uses literal values**
- **Issue:** The first README draft showed `mv -T "$SIM_OUT/_archive_<tag>/<scenario>" ...`.
- **Fix:** Replaced it with a literal NAT / `pre_interventions_phase5` example, per the no-angle-bracket operator preference. The change was amended into the Task 2 commit before any push.

### TDD Gate Compliance

Task 1 followed RED -> GREEN. `bf67df9` (test) failed with `FAIL: script_exists` (PASS 0, FAIL 1). `3e4532c` (feat) passes 90/90. No refactor commit was needed.

## Assumption Drift (advisory)

- **Found during:** Task 2, rate pre-check
- **Planned:** "row counts for To* = 104, 105, 106" on the raw Stage 4 CSV.
- **Actual:** The rows that reach the verifier ledger are the filtered rows, without persistence or zero-rate rows.
- **Why it matters:** The runbook's "verifier WILL FAIL" prediction now matches what the ledger actually reads.

## Known Stubs

None.

## HPC steps still pending with the operator (all in 261014-dex-OPERATOR.md)

0. Workstation: `git push origin spatial-interventions-integration`, and note the hash.
1. login02: `cd /home/black/nascent-lulcc && git pull`, then `git status --short`.
2. login02: `source .env`. `echo "$HPC_SCRATCH_ROOT"` must print `/beegfs/black/nascent-lulcc`. Then run `bash scripts/hpc_common.sh --check-stage7-contract`.
3. login02: code identity. The hash matches, `STATE archive_allocation_outputs` has 1+ hits, and `has no cell equal to 1` has 2 hits.
4. login02: `squeue -u black` is clear. Then the dry run, then `--apply` with `--tag pre_interventions_phase5`. Afterwards `find ... | wc -l` must print 0.
5. login02: CUL mask `ls -l`, the filtered rate pre-check, and the smoke `sbatch --parsable --partition=highmem --export=ALL,ALLOCATION_PROFILE_SCENARIO=CUL,ALLOCATION_REGION_FILTER=costa_peruana,ALLOCATION_YEAR_POST_FILTER=2024 scripts/submit_allocation_smoke.sh`.
6. login02: `sacct`, the mask-marker grep, then `sbatch --parsable scripts/submit_verify_intervention_smoke.sh --scenario CUL --region costa_peruana --year 2024 --extra-log ...`, then the text-only evidence greps.
7. Paste back the evidence list.

The Phase 4 sweep itself is not launched by this runbook.

## Threat Flags

None. The new surface (renames under `$HPC_SCRATCH_ROOT`) is covered by T-q261014-01..05 and the mitigations are implemented as planned.

## Self-Check: PASSED

- FOUND: scripts/archive_allocation_outputs.sh (100755)
- FOUND: tests/shell/test-archive-allocation-outputs.sh (100755)
- FOUND: .planning/quick/261014-dex-archive-pre-intervention-allocation-outp/261014-dex-OPERATOR.md
- FOUND: docs/README_HPC.md subsection
- FOUND commits: bf67df9, 3e4532c, 6ae7651
