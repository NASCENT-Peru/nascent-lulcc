---
status: partial
phase: 01-repair-visibility
source: [01-VERIFICATION.md]
started: 2026-05-05T00:00:00Z
updated: 2026-05-15T00:00:00Z
---

## Current Test

Verification cycle wrap (2026-05-15):
- Test 1 — **FAILED** with seven structural Phase 1 findings (G1–G7). Dinamica-on-HPC contract is broken end-to-end; smoke test reports SUCCESS while Dinamica is dying.
- Test 2 — **PASSED** (after working around G7).
- Test 3 — **partial**. Dinamica-independent OOM probe submitted to SLURM (job 66581425, queued); operator to run post-mortem block when job completes. SENTINEL+diagnose-script contract was already proven on the workstation via fixture in the original 01-VERIFICATION.md.

Next: route findings into a Phase 1 follow-up plan (suggested name `01-05-fix-dinamica-launch-contract`) covering the six fix-plan rows below.

## Tests

### 1. Live Euler smoke test (INFRA-01 / SC6)
expected: `scripts/smoke_test_dinamica.sh --live` completes with exit 0, `exec_dinamica()` invokes the minimal model via `apptainer exec $DINAMICA_EGO_8_HOME DinamicaConsole`, and a timestamped `logs/dinamica-smoke-*.log` artifact is written.
result: **FAILED** — Six independent issues (G1–G6 below) prevent the contract from holding. The script *reports* `SUCCESS` because Dinamica returns exit 0 even on `std::exception`, which is itself one of the six findings.

  - Verified on Euler 2026-05-15 (`eu-login-04` / `eu-login-18`, apptainer 1.4.5 in /usr/bin).
  - .sif build path: workaround works (build upstream Dockerfile on Docker-capable host → `docker save` → transfer → `apptainer build docker-archive://...`). Image at `/beegfs/$USER/nascent-lulcc/containers/dinamica-ego-8.sif`, 1020M, sha not pinned.
  - Container itself is functional: when invoked through `cd /opt/dinamica/usr && bin/DinamicaEGO.sh <abs-model.ego>`, Dinamica loads, parses, and produces meaningful errors instead of `std::exception`.

### 2. Live allocation_env solve + library() load (MEM-06 / SC5)
expected: `micromamba activate allocation_env` resolves on HPC and all 11 prediction-time packages (`r-parsnip`, `r-recipes`, `r-ranger`, `r-xgboost`, `r-tidypredict`, `r-butcher`, `r-ps`, `r-lobstr`, `r-bundle`, `r-qs`, `r-rhpcblasctl`) load via `library()` without error.
result: **PASS** (Euler 2026-05-15, eu-login-18). Env solved cleanly under `/beegfs/$USER/nascent-lulcc/micromamba/envs/allocation_env` after exporting `HPC_SCRATCH_ROOT`. All 11 packages reported `OK` via `requireNamespace()`; `xgboost` reported version `1.7.6.1` (matches the MEM-06 contract pin `r-xgboost=1.7`). One pre-existing finding G7 surfaced during this test (silent home-fallback when `HPC_SCRATCH_ROOT` is unset) — see Gaps.

### 3. Live SIGKILL + diagnose_alloc_crash.sh (OBS-02 / SC2)
expected: After a real allocation worker is OOM-killed, `bash scripts/diagnose_alloc_crash.sh` surfaces SLURM `sacct`/`seff` OOM evidence, a SENTINEL entry in the relevant region log, and a MaxRSS metric.
result: **partial — Dinamica-independent live SLURM OOM probe submitted (job 66581425, eu-login-18, 2026-05-15 11:15 UTC, --mem-per-cpu=512M, R allocates 2 GB). Job is queued (priority wait); will close out when it completes and the operator runs the post-mortem block below.** The SENTINEL emission + diagnose_alloc_crash.sh parsing contract is already proven via the workstation fixture run (`01-VERIFICATION.md` behavioural spot-check); this probe only validates the live-SLURM `sacct`/`seff` evidence wiring. The "real allocation worker" path is fully blocked by G1–G6 (Dinamica-on-HPC contract is broken end-to-end) and cannot be exercised in this verification cycle.

**Post-mortem block to run when job 66581425 ends** (`squeue -u $USER` shows it gone):

```bash
JID=66581425
echo "=== sacct ==="; sacct -j $JID --format=JobID,State,ExitCode,MaxRSS,ReqMem -P
echo "=== seff ===";  seff $JID 2>&1 | head -20
echo "=== output ==="; cat /beegfs/$USER/nascent-lulcc/test3-logs/oom-$JID.{out,err} 2>/dev/null
echo "=== diagnose_alloc_crash.sh ==="
bash scripts/diagnose_alloc_crash.sh --job-id $JID
```

Pass criteria: `sacct` shows `OUT_OF_MEMORY` (or truncated `OUT_OF_ME+`) and `MaxRSS` near 512M; `diagnose_alloc_crash.sh` extracts those into a one-line crash summary.

## Summary

total: 3
passed: 1
issues: 7
pending: 0
skipped: 0
blocked: 0
failed: 1
deferred: 1

## Gaps

### G1 — Container `.def` bootstraps from a non-existent registry path
- **Phase 1 contract violated:** `dinamica/container/rocker-geospatial-dinamica.def:2` reads `From: ghcr.io/ethzplus/rocker-geospatial-dinamica:latest`. That image is not published — upstream `ethzplus/rocker-geospatial-dinamica` is a Dockerfile-only repo (Dinamica licence forbids redistribution).
- **Evidence:** `apptainer build` on Euler 2026-05-15 returned `FATAL: ... GET https://ghcr.io/token?...rocker-geospatial-dinamica:pull: DENIED`.
- **Anticipated:** Plan flagged this as ASSUMED in `01-RESEARCH.md:299` (A3).
- **Workaround applied:** Operator built upstream Dockerfile on a Docker-capable workstation, `docker save` to tar, transferred to Euler, then `apptainer build dinamica-ego-8.sif docker-archive://dinamica-ego.tar`.
- **Durable fix:** Rewrite `.def` to bootstrap from `docker://rocker/r-ver:4.5.3` (or `rocker/geospatial`) and inline upstream's Dinamica AppImage download + extraction in `%post`. Update README + HPC docs.

### G2 — `DinamicaConsole` exits 0 even on `std::exception` and on hard parse failures
- **Phase 1 contract violated:** Phase 1 SC2 + Phase 1 goal — "operator can diagnose any allocation failure within minutes". The smoke-test script and the production launcher both rely on Dinamica's exit code as the success signal.
- **Evidence:** `apptainer exec ... DinamicaConsole` returned `DINAMICA_EXIT=0` on three separate failure modes: no-args invocation (`std::exception`), `--help` (`std::exception`), and feeding it the wrong file format (`The ".ego-decoded" does not represent a supported model script format` followed by `terminate called after throwing an instance of 'DFF::Exception'`).
- **Impact:** `scripts/smoke_test_dinamica.sh` printed `[live] SUCCESS: Dinamica completed and wrote logs/dinamica-smoke-2026-05-15T08-37-23Z.log` while Dinamica had actually died. The Phase 1 visibility contract is silently broken.
- **Durable fix:** `scripts/smoke_test_dinamica.sh` must grep its own log for `Dinamica EGO exited with an error`, `terminate called after throwing`, and `std::exception` and exit non-zero on any match. `src/dinamica_utils.r:exec_dinamica()` must do the same.

### G3 — Container `$HOME` shadowed by apptainer's default bind-mounts
- **Phase 1 contract violated:** Production-time launcher in `src/dinamica_utils.r:189-190` builds `apptainer exec <sif> DinamicaConsole <args>` with no `--home` flag. Apptainer bind-mounts the host's `$HOME` over the container's `/root`, hiding `/root/.dinamica_ego_8.conf` (the file the Dockerfile writes at build time). Dinamica reads `$HOME/.dinamica_ego_8.conf` which doesn't exist for the operator user → init failure.
- **Evidence:** `ls -la /home/black/.dinamica_ego_8.conf` → no such file; `ls -la /root/.dinamica_ego_8.conf` inside container → present.
- **Workaround applied:** stage `<scratch>/dinamica-home/.dinamica_ego_8.conf` and pass `--home <scratch>/dinamica-home` to `apptainer exec`.
- **Durable fix:** `resolve_dinamica_launch()` must (a) create `<scratch>/dinamica-home/.dinamica_ego_8.conf` if missing and (b) emit `--home <scratch>/dinamica-home` into the container args.

### G4 — Container `/tmp/dinamica` shadowed by host `/tmp` mount
- **Phase 1 contract violated:** same launcher line as G3. Apptainer bind-mounts the host `/tmp` over the container's `/tmp`, so `/tmp/dinamica` (created by the upstream Dockerfile at build time) doesn't exist at runtime.
- **Evidence:** `DINAMICA_EGO_8_TEMP_DIR=/tmp/dinamica` is set inside the container, but `ls /tmp/dinamica` → no such file.
- **Workaround applied:** create `<scratch>/dinamica-tmp` on the host, pass `--env DINAMICA_EGO_8_TEMP_DIR=<scratch>/dinamica-tmp` to `apptainer exec`.
- **Durable fix:** `resolve_dinamica_launch()` must (a) create `<scratch>/dinamica-tmp` and (b) emit `--env DINAMICA_EGO_8_TEMP_DIR=<scratch>/dinamica-tmp` into the container args.

### G5 — Smoke-test model is the production allocation model AND is in the wrong file format
- **Phase 1 contract violated:** Phase 1 SC6 — "smoke test executes a *minimal* Dinamica model on Euler". The committed model `dinamica/dinamica_model/allocation.ego-decoded`:
  1. Is the **production** allocation script (lines 6–34 require a working dir containing `probability_map_dir/`, `anterior.tif`, `posterior.tif`, `expansion_table.csv`, `patcher_table.csv`, `trans_rates.csv`).
  2. Is in the **wrong file format**: `.ego-decoded` is the human-readable text dump and is rejected by `DinamicaConsole` with `The ".ego-decoded" does not represent a supported model script format`. DinamicaConsole only accepts compiled `.ego` binaries.
- **Anticipated:** `01-04-SUMMARY.md:60` documents the planner's choice not to introduce a "minimal-mode" marker, reasoning that "running the existing model verbatim … proves the real launcher path". That reasoning was wrong on both counts (needs unstaged inputs; wrong file format).
- **Why missed:** the smoke test was never end-to-end run against a built `.sif` on Euler — it was deferred to operator-side verification, which is what we are doing now.
- **Durable fix:** ship a true minimal `.ego` (binary) smoke model — e.g., a no-op script generated via the upstream R `dinamica` package or via `process_dinamica_script(mode="encode")` from a hand-written minimal `.ego-decoded`. Or change the smoke contract to validate the launcher only and stop at the model-parse stage.

### G7 — `setup_environments.sh` silently installs envs under `$HOME` when `HPC_SCRATCH_ROOT` is unset
- **Phase 1 contract violated:** D-15 says `HPC_SCRATCH_ROOT` is REQUIRED on HPC and pre-flight should fail fast on missing scratch contract variables. `scripts/setup_environments.sh:87-90` silently falls back to `$PROJECT_ROOT/.envs` instead — burning ~3 GB of conda packages into a home filesystem that typically has hard quotas (Euler default home quota is small; `r-base` + `gdal` + `r-arrow` + `r-xgboost` etc. exceeds it for most users).
- **Evidence:** First run on Euler 2026-05-15 printed `Env install root: /home/black/nascent-lulcc/.envs` despite the verification host's claim that the script is HPC_SCRATCH_ROOT-aware. The transaction proceeded all the way through download/install before being noticed.
- **Anticipated:** No — the verification report at `01-VERIFICATION.md` listed this fallback as VERIFIED behaviour but never reconciled it with D-15 ("fail fast on missing scratch contract variables").
- **Workaround applied:** Source `.env` (or manually export `HPC_SCRATCH_ROOT=/beegfs/$USER/nascent-lulcc`) before running `setup_environments.sh`.
- **Durable fix:** `scripts/setup_environments.sh` should detect the HPC context (e.g. presence of `SLURM_*` env, or `/beegfs` directory, or `--hpc` flag) and refuse to fall back to `$HOME/.envs` when scratch is unset. Match the `scripts/hpc_common.sh --check-stage7-contract` gate. At minimum, the script should print a loud WARNING when the fallback is selected so operators don't fill their home quota.

### G6 — Direct `DinamicaConsole` invocation is not a supported entrypoint
- **Phase 1 contract violated:** Phase 1 SC6 + the entire `src/dinamica_utils.r:resolve_dinamica_launch()` design. The launcher builds `apptainer exec <sif> DinamicaConsole <model>`, but `/opt/dinamica/usr/bin/DinamicaConsole` invoked directly fails with `std::exception` regardless of args, env, or model. The supported entrypoint is `bin/DinamicaEGO.sh` invoked from `cwd=/opt/dinamica/usr`. The wrapper script sets `PROJ_DATA`, `DINAMICA_EGO_8_INSTALLATION_DIRECTORY`, `DINAMICA_EGO_8_GDAL_DATA`, `DINAMICA_EGO_8_LOG_PATH`, computes the right relative `bin/DinamicaConsole` path, and dispatches between Console / Coordinator / Agent / GUI based on argv.
- **Evidence:** Test A (`bash -c 'cd /opt/dinamica/usr && bin/DinamicaEGO.sh <abs-model.ego-decoded>'`) produced a meaningful Dinamica error (`The ".ego-decoded" does not represent a supported model script format.`). Test B and Test C (direct `DinamicaConsole`, even with `PROJ_DATA` + `PYTHONHOME=/opt/dinamica/usr/bin/PyEnvironment` + `PYTHONPATH=/opt/dinamica/usr/bin/PyEnvironment/lib/python3.12` + `--home`) both still failed with `std::exception`. Confirmed at `eu-login-18` 2026-05-15.
- **Durable fix:** `resolve_dinamica_launch()` must build the launch as
  `apptainer exec --home <scratch>/dinamica-home --env DINAMICA_EGO_8_TEMP_DIR=<scratch>/dinamica-tmp <sif> bash -c 'cd /opt/dinamica/usr && bin/DinamicaEGO.sh <abs-model-path>'`
  rather than the direct `DinamicaConsole` invocation. The `<abs-model-path>` must be absolute (the launcher's relative-path branch is fragile across cwd changes).

## Findings → Fix Plan (proposed Phase 1 follow-up)

| # | File | Change | Closes |
|---|---|---|---|
| 1 | `dinamica/container/rocker-geospatial-dinamica.def` | Rewrite to bootstrap from `docker://rocker/r-ver:4.5.3`; inline upstream `%post` to download + extract Dinamica AppImage and run `install_geospatial.sh`. | G1 |
| 2 | `src/dinamica_utils.r:resolve_dinamica_launch()` | Build apptainer args as `exec --home <staged-home> --env DINAMICA_EGO_8_TEMP_DIR=<staged-tmp> <sif> bash -c 'cd /opt/dinamica/usr && bin/DinamicaEGO.sh <abs-model>'`. Create `<staged-home>/.dinamica_ego_8.conf` and `<staged-tmp>` if missing. | G3, G4, G6 |
| 3 | `src/dinamica_utils.r:exec_dinamica()` AND `scripts/smoke_test_dinamica.sh` | After Dinamica returns, grep stdout/log for `Dinamica EGO exited with an error`, `terminate called after throwing`, `std::exception`. Exit non-zero on any match. | G2 |
| 4 | `dinamica/dinamica_model/` | Replace `.ego-decoded` smoke fixture with a true minimal `.ego` (binary) — generated via `process_dinamica_script(mode="encode")` from a hand-written 5-line no-op `.ego-decoded` source. Commit both source and binary. | G5 |
| 5 | `dinamica/container/README.md`, `docs/README_HPC.md` | Document the operator workstation-build → docker-archive transfer flow as the canonical bootstrap until G1's `.def` rewrite lands. Update the live smoke-test command to reflect the new `bin/DinamicaEGO.sh` invocation. | G1 (docs) |
| 6 | `scripts/setup_environments.sh` | Refuse to fall back to `$HOME/.envs` when running on HPC and `HPC_SCRATCH_ROOT` is unset. Mirror the `--check-stage7-contract` gate from `hpc_common.sh`. At minimum, print a loud WARNING. | G7 |

This is a Phase 1 re-execute scope, not a verification-cycle patch. Recommend opening a follow-up plan (e.g., `01-05-fix-dinamica-launch-contract`) to land items 1–5 atomically before declaring INFRA-01 closed.
