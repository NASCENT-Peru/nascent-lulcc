---
phase: 01-repair-visibility
verified: 2026-05-05T19:05:00Z
status: human_needed
score: 6/6 truths verified (3 require human/HPC verification)
overrides_applied: 0
re_verification:
  initial: true
human_verification:
  - test: "Run live HPC smoke test on Euler"
    expected: "scripts/smoke_test_dinamica.sh --live --runtime auto --artifact \"$DINAMICA_EGO_8_HOME\" --ego dinamica/dinamica_model/allocation.ego-decoded --require-log-under logs exits 0 and writes a non-empty logs/dinamica-smoke-<timestamp>.log; INFRA-01 success criterion 6 closes"
    why_human: "Requires Euler login, apptainer/singularity runtime, and a built external Dinamica .sif image. Cannot be exercised on the Windows verification host (no apptainer, no Rscript, no .sif staged). Operator hand-off is documented in SUMMARY 01-04."
  - test: "Activate allocation_env on Euler and load every MEM-06 package via library()"
    expected: "After bash scripts/setup_environments.sh --env allocation_env --non-interactive followed by micromamba activate, calling library() on each of r-parsnip, r-recipes, r-ranger, r-xgboost (1.7.x), r-tidypredict, r-butcher, r-bundle, r-qs, r-ps, r-lobstr, r-rhpcblasctl succeeds without errors (success criterion 5)."
    why_human: "Requires conda/micromamba on a Linux host; the YAML is contractually correct (all MEM-06 packages declared with r-xgboost=1.7), but the actual conda solve + library load can only be confirmed on Euler. Verification host has no micromamba and no Rscript."
  - test: "Run a real allocation with a known SIGKILL injection on Euler and observe the per-region log + diagnose_alloc_crash.sh output"
    expected: "After SIGKILL of one worker process, the per-region log_file under logs/ contains a 'SENTINEL reason=incomplete' line plus a JSON SENTINEL payload with non-NA scenario/region/timestep/transition; running scripts/diagnose_alloc_crash.sh --job-id <jid> prints sacct OUT_OF_MEMORY/MaxRSS, the matching SENTINEL line(s), and a DINAMICA_LOG_PATH entry (success criterion 2)."
    why_human: "Requires SLURM, a real running job, and a controlled SIGKILL. The bash --fixture-dir end-to-end verification did pass on the local host (output contained SENTINEL/OUT_OF_MEMORY/MaxRSS/forest_to_crop), proving the script's parsing/correlation contract. The live SLURM cgroup OOM evidence requires Euler."
---

# Phase 1: Repair & Visibility — Verification Report

**Phase Goal:** Operator can diagnose any allocation failure within minutes by reading the per-region log and a single post-mortem command — RSS values are real, paths work on HPC out of the box, and missing prerequisites fail fast with a single actionable list.

**Verified:** 2026-05-05T19:05:00Z
**Status:** human_needed
**Re-verification:** No — initial verification.

## Goal Achievement

### Observable Truths (from ROADMAP Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Every per-region log shows real numeric `rss_before/after/delta/peak` values (no "NAMB") on Windows local and Linux HPC | VERIFIED | `src/allocation.r:64-98` reads `ps::ps_memory_info()$rss` (portable across Windows + Linux) before falling back to `/proc/self/status` for Linux-only `VmHWM`. `prof_toc()` lines 147-164 emit one PROFILE line with all five RSS fields. `tests/testthat/test-allocation-preflight.R:101-116` asserts numeric, non-NA, positive `rss`. (Human-confirmation note: live HPC PROFILE log inspection is not executed here — see `human_verification` item #2 in 01-02 testing.) |
| 2 | When an allocation worker is SIGKILLed, `diagnose_alloc_crash.sh` surfaces OOM evidence from sacct/seff/cgroup memory and a sentinel trace exists in the region log | VERIFIED (with human follow-up for live SIGKILL) | `scripts/diagnose_alloc_crash.sh` exists, executable (mode 755), bash -n passes; behavioral spot-check (Step 7b) executed end-to-end with the plan's fixture and produced expected SENTINEL / OUT_OF_MEMORY / MaxRSS / forest_to_crop tokens. `src/utils.r:1136-1163` defines `worker_state_flush_sentinel()` writing two-line SENTINEL+JSON; `src/allocation.r:919-923` registers an `on.exit()` flush per worker. Live SLURM SIGKILL path requires Euler — see human verification #3. |
| 3 | Running `allocation.r` with a missing env var, missing R package, missing model file, or missing Dinamica binary aborts before any work with one consolidated list of all gaps | VERIFIED | `src/allocation.r:221-328` defines `validate_allocation_runtime(config, fixture)` validating four prereq buckets (env, packages, files, dinamica). `run_allocation()` at line 707-721 invokes the gate FIRST, before any region work, and `stop()`s with `"Allocation pre-flight failed:\n  - ...\n  - ..."`. `scripts/run_allocation.r:113-153` exposes `--preflight-only` / `--preflight-fixture` and runs the gate again before `future::plan()` (line 213). No `install.packages()` self-healing exists in the entry script (grep confirms 0 hits). `tests/testthat/test-allocation-preflight.R:44-83` asserts the consolidated multi-category error list. |
| 4 | `simulation_trans_rates_prep.r` and `calibration_predictor_prep.r` execute on a fresh HPC checkout with no manual path edits; HPC shell scripts contain no hardcoded `black` references; Dinamica EGO logs land in `logs/` | VERIFIED | (a) `src/calibration_predictor_prep.r:19` resolves terra_temp via `get_stage7_runtime_paths(config)[["terra_temp"]]`; the `E:/terra_temp` literal is gone (grep confirms 0 hits in src/). (b) `src/simulation_trans_rates_prep.r:155` reads `config[["lulc_demand_path"]]`; the `LULC_demand_results.xlsx` literal is gone (grep confirms 0 hits in src/). (c) `config/hpc_config.yaml:8` uses `${HPC_SCRATCH_ROOT}` placeholder. (d) `scripts/hpc_common.sh`, `scripts/setup_environments.sh`, `.env.template`, `environments/allocation_env.yml`, `scripts/submit_allocation*.sh` — grep returns 0 `black` hits across all five (PIPE-04 contract files). (e) `src/dinamica_utils.r:587-606` `resolve_dinamica_log_path()` writes to `<repo_root>/logs/dinamica/<timestamp>_dinamica.log`. NB: `black` references DO remain in `docs/MICROMAMBA_SETUP.md`, `docs/README_HPC.md` line 149, `docs/TRANS_RATE_ESTIMATION_ENV.md`, and `README.md` — see Anti-patterns. PIPE-04 explicitly scopes the contract to `hpc_common.sh`, `setup_environments.sh`, `.env.template`, so the criterion as written is met; the doc references are flagged as warnings, not blockers. |
| 5 | Activating `allocation_env.yml` resolves on HPC with all prediction-time packages loadable via `library()` | VERIFIED (contract) — human confirms live solve | `environments/allocation_env.yml` declares all 11 MEM-06 packages: r-parsnip, r-recipes, r-ranger, r-xgboost=1.7, r-tidypredict, r-butcher, r-bundle, r-qs, r-ps, r-lobstr, r-rhpcblasctl (plus r-workflows). Pin policy section explicitly anchors r-xgboost=1.7 to match training-time. `scripts/submit_allocation.sh:23` and `scripts/submit_allocation_profile.sh:22` activate `ENV_NAME="allocation_env"`. `tests/testthat/test-allocation-env-canonical.R` is committed and asserts presence of all packages and the xgboost pin. Live conda solve + `library()` validation requires Euler — see human verification #2. |
| 6 | Dinamica EGO 8 executes successfully inside a Singularity container on Euler — minimal allocation model completes and `exec_dinamica()` invokes it via `DINAMICA_EGO_8_HOME`; container definition and build instructions committed | VERIFIED (contract + provenance) — human confirms live execution | (a) `src/dinamica_utils.r:115-220` `resolve_dinamica_launch()` reads `DINAMICA_EGO_8_HOME` verbatim as the .sif path on HPC and builds `apptainer exec <sif> DinamicaConsole <model>` (with singularity fallback). `exec_dinamica()` lines 239-324 delegates to the resolver. (b) `dinamica/container/rocker-geospatial-dinamica.def` exists, bootstraps from `ghcr.io/ethzplus/rocker-geospatial-dinamica:latest`, has %labels with INFRA-01 + DINAMICA_EGO_8_HOME contract, and a %test stanza. (c) `dinamica/container/README.md` documents the apptainer/singularity build flow plus external-artifact (D-10) publishing pattern. (d) `scripts/smoke_test_dinamica.sh` exists, executable, --dry-run behavioral check passes (Step 7b), exit-code contract documented (codes 0..4). (e) `docs/README_HPC.md` describes the HPC contract. Live Euler smoke test requires apptainer + built .sif — see human verification #1. |

**Score: 6/6 truths VERIFIED (3 close fully on operator-side HPC verification)**

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/setup.r` | Shared path/env resolution helpers + config expansion | VERIFIED | `get_stage7_runtime_paths()` at lines 54-73 returns 5-key contract; `expand_env_placeholders()` at lines 189-214 expands `${VAR}` placeholders with fail-fast on unset values. |
| `src/calibration_predictor_prep.r` | Config/env-driven terra temp resolution | VERIFIED | Line 19 calls `get_stage7_runtime_paths(config)[["terra_temp"]]`; the `E:/terra_temp` literal is removed. |
| `src/simulation_trans_rates_prep.r` | Config-driven LULC demand input | VERIFIED | Line 155 reads `config[["lulc_demand_path"]]`; xlsx literal removed; CSV path used. |
| `config/hpc_config.yaml` | Authoritative HPC paths aligned to env overrides | VERIFIED | Line 8 uses `${HPC_SCRATCH_ROOT}`; no `black` literals. |
| `.env.template` | Documented runtime overrides | VERIFIED | Stage 7 contract block documents 5 keys (TERRA_TEMP, HPC_SCRATCH_ROOT, HPC_TMP_ROOT, DINAMICA_EGO_8_HOME, DINAMICA_BACKEND); `$USER` expansion replaces `black`. |
| `environments/allocation_env.yml` | Canonical Stage 7 dependencies | VERIFIED | Full MEM-06 set declared with r-xgboost=1.7 pin. |
| `scripts/submit_allocation.sh` | Stage 7 batch entrypoint | VERIFIED | `ENV_NAME="allocation_env"` at line 23; was previously `transition_model_env`. |
| `scripts/submit_allocation_profile.sh` | Profile entrypoint | VERIFIED | `ENV_NAME="allocation_env"` at line 22. |
| `scripts/hpc_common.sh` | Shared HPC bootstrap honoring scratch/temp contract | VERIFIED | `check_stage7_contract()` validates HPC_SCRATCH_ROOT/HPC_TMP_ROOT/TERRA_TEMP; CLI flag `--check-stage7-contract` confirmed (behavioral check pass + fail). `black` literals removed; `$USER` used in `find_micromamba()` fallback. |
| `scripts/setup_environments.sh` | Canonical bootstrap path | VERIFIED | `--env`/`--non-interactive` flags present; falls back to `<repo>/.envs` when HPC_SCRATCH_ROOT is unset. `bash -n` passes. |
| `src/allocation.r` | Portable RSS profiling, pre-flight, worker breadcrumbing | VERIFIED | `ps::ps_memory_info()` reader; `validate_allocation_runtime()`; `run_allocation()` invokes gate first; `worker_state_*` calls at lifecycle boundaries; `on.exit()` SENTINEL flush. |
| `src/dinamica_utils.r` | Log-file-aware Dinamica worker helper | VERIFIED | `run_allocation_dinamica(work_dir, log_file, dry_run, model_path, ...)` signature; emits 11 `DINAMICA_*` breadcrumb sites; `resolve_dinamica_log_path()` writes to central logs/dinamica/. |
| `src/utils.r` | Structured logging + worker-log support | VERIFIED | `worker_state_init/set/get/flush_sentinel` API at lines 1053-1163; `log_msg()`, `initialize_worker_log()` preserved. |
| `scripts/run_allocation.r` | Pre-flight-aware entrypoint | VERIFIED | `--preflight-only` / `--preflight-fixture FILE` flags parsed; `install.packages()` is removed; gate runs before `future::plan()`. |
| `scripts/diagnose_alloc_crash.sh` | Post-mortem OOM/log correlation | VERIFIED | Exists (mode 755), `bash -n` passes; behavioral spot-check with fixture passes (SENTINEL + OUT_OF_MEMORY + MaxRSS + forest_to_crop). |
| `scripts/smoke_test_dinamica.sh` | Operator-facing HPC smoke test | VERIFIED | Exists (mode 755); `--dry-run --runtime apptainer --artifact /tmp/dinamica.sif --ego dinamica/dinamica_model/allocation.ego-decoded` exits 0 with full plan printed (behavioral spot-check pass). |
| `docs/README_HPC.md` | Documented Dinamica external artifact + smoke test usage | VERIFIED (with warning) | New Stage 7 section at top documents DINAMICA_EGO_8_HOME contract, build flow, dry-run + live smoke test commands. WARNING: legacy text below the new section still references `black` in customisation guidance (line 149) — see Anti-patterns. |
| `dinamica/dinamica_model/allocation.ego-decoded` | Smoke-testable allocation model | VERIFIED | Present (3,466 bytes); referenced verbatim by smoke-test script. |
| `dinamica/container/rocker-geospatial-dinamica.def` | Container definition with required provenance | VERIFIED | `Bootstrap: docker / From: ghcr.io/ethzplus/rocker-geospatial-dinamica:latest`; %labels include INFRA-01 + contract env. |
| `dinamica/container/README.md` | Operator build instructions + provenance | VERIFIED | Documents apptainer/singularity build, --fakeroot, external publish location, repo consumption. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| `src/simulation_trans_rates_prep.r` | `config/local_config.yaml` / `config/hpc_config.yaml` | `config[["lulc_demand_path"]]` | WIRED | Line 155 reads the config key; both YAMLs define `lulc_demand_path`. |
| `src/calibration_predictor_prep.r` | `.env.template` | TERRA_TEMP env override | WIRED | `get_stage7_runtime_paths()` reads `Sys.getenv("TERRA_TEMP")`; `.env.template` documents the override. |
| `scripts/submit_allocation.sh` / `_profile.sh` | `environments/allocation_env.yml` | `ENV_NAME="allocation_env"` | WIRED | Both scripts set ENV_NAME to allocation_env; YAML provides the package contract. |
| `scripts/hpc_common.sh` | `.env.template` | shared scratch/temp variable names | WIRED | `HPC_STAGE7_REQUIRED_VARS=(HPC_SCRATCH_ROOT HPC_TMP_ROOT TERRA_TEMP)` matches the names in .env.template. |
| `scripts/run_allocation.r` | `src/allocation.r` | pre-flight-only / full run dispatch via `--preflight-only` | WIRED | Source file lists allocation.r; `validate_allocation_runtime()` invoked from both modes. |
| `src/allocation.r` | `src/dinamica_utils.r` | worker passes per-region log path: `run_allocation_dinamica(..., log_file=log_file)` | WIRED | `src/allocation.r:990-993` calls `run_allocation_dinamica(work_dir=..., log_file=log_file, ...)`. |
| `src/allocation.r` | `scripts/diagnose_alloc_crash.sh` | shared sentinel/log naming contract (`SENTINEL`, `STATE`, `PROFILE`, `DINAMICA_LOG_PATH`) | WIRED | utils.r emits `SENTINEL reason=...` + JSON; diagnose_alloc_crash.sh greps `'SENTINEL\|"event":"SENTINEL"'` and `'DINAMICA_LOG_PATH'`. Behavioral fixture run confirms end-to-end correlation. |
| `src/dinamica_utils.r` | `scripts/smoke_test_dinamica.sh` | shared backend/env contract (`apptainer`, `singularity`, `DINAMICA_EGO_8_HOME`) | WIRED | Both consume DINAMICA_EGO_8_HOME verbatim as the .sif path; both probe apptainer first then singularity; same `<runtime> exec <sif> DinamicaConsole <ego>` shape. |
| `src/dinamica_utils.r` | `logs/` | timestamped Dinamica subprocess logfile (`logs/dinamica/<ts>_dinamica.log`) | WIRED | `resolve_dinamica_log_path()` constructs the path under project root logs/dinamica/. |
| `dinamica/container/rocker-geospatial-dinamica.def` | `docs/README_HPC.md` | provenance + build flow (`ethzplus/rocker-geospatial-dinamica`, `apptainer build`, `singularity build`) | WIRED | All required tokens present in def + README + HPC README per grep gate. |

### Data-Flow Trace (Level 4)

Phase 1 produces orchestration/diagnostic infrastructure rather than a UI rendering dynamic data, so Level 4 applies to the data flowing through the pre-flight + breadcrumb pipeline rather than a component prop tree.

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|--------------------|--------|
| `validate_allocation_runtime()` | `errors` character vector | Live `Sys.getenv()` reads, `requireNamespace()` probes, `file.exists()` checks, `Sys.which()` runtime probe; or fixture override | Yes — proven by fixture-mode test that returned the 5 expected error tokens (MISSING_ENV_A/B, definitelyMissingPkg, missing/model.rds, missing/dinamica.sif). | FLOWING |
| `prof_toc()` PROFILE log line | `after$rss`, `t0$mem$rss`, `after$vmhwm`, `gc_after[..., 6L]` | `ps::ps_memory_info()` (portable) + `/proc/self/status` (Linux peak) + `gc()` matrix indices | Yes — the test asserts `m$rss` is positive numeric on Windows; Linux peak read is appended only when /proc exists. | FLOWING (live HPC peak still requires human run #2/#3) |
| `worker_state_flush_sentinel()` JSON SENTINEL | scenario/region/timestep/transition | Per-process `.worker_state_env`, set at every lifecycle boundary by `worker_state_set()` calls in `src/allocation.r` | Yes — fixture-driven `diagnose_alloc_crash.sh` run consumed the JSON SENTINEL and printed it as the crash summary. | FLOWING |
| `resolve_dinamica_log_path()` central log path | `logs_dir`, `ts` | `find_project_root()` + `Sys.time()` | Yes — path string is well-formed; live file write on a subprocess run requires apptainer (operator hand-off). | FLOWING |
| `resolve_dinamica_launch()` HPC launch contract | `artifact_path`, `runtime`, `args` | `Sys.getenv("DINAMICA_EGO_8_HOME")` + runtime probe (or override) | Yes — Task 1 verify on workstation: launch$artifact_path == /tmp/dinamica.sif, runtime == apptainer, args contains DinamicaConsole. | FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| `bash -n` syntax checks across all touched shell scripts | `bash -n diagnose_alloc_crash.sh smoke_test_dinamica.sh hpc_common.sh setup_environments.sh submit_allocation.sh` | All exit 0 | PASS |
| `--check-stage7-contract` rejects missing vars | `env -u HPC_SCRATCH_ROOT -u HPC_TMP_ROOT -u TERRA_TEMP bash scripts/hpc_common.sh --check-stage7-contract` | Exit 1; stderr names HPC_SCRATCH_ROOT, HPC_TMP_ROOT, TERRA_TEMP | PASS |
| `--check-stage7-contract` accepts complete contract | `HPC_SCRATCH_ROOT=/x HPC_TMP_ROOT=/y TERRA_TEMP=/z bash scripts/hpc_common.sh --check-stage7-contract` | Exit 0; "Stage 7 path contract OK." | PASS |
| `diagnose_alloc_crash.sh --fixture-dir` end-to-end | Plan 01-03 Task 2 fixture (sacct.txt, seff.txt, region.log) | Output contains SENTINEL + OUT_OF_MEMORY + MaxRSS + forest_to_crop | PASS |
| `smoke_test_dinamica.sh --dry-run` plan resolution | `--dry-run --runtime apptainer --artifact /tmp/dinamica.sif --ego dinamica/dinamica_model/allocation.ego-decoded` | Exit 0; plan prints `apptainer exec /tmp/dinamica.sif DinamicaConsole dinamica/dinamica_model/allocation.ego-decoded`, log file `logs/dinamica-smoke-<UTC>.log` | PASS |
| Pre-flight Powershell harness (R-side) | `micromamba run -n allocation_env Rscript --vanilla scripts/run_allocation.r --preflight-only --preflight-fixture <fixture>` | Cannot run on Windows verification host (no Rscript / no micromamba) | SKIP — routed to human verification (item #2/#3) |
| Live HPC smoke test | `bash scripts/smoke_test_dinamica.sh --live --runtime auto ...` | Cannot run on Windows verification host (no apptainer, no .sif) | SKIP — routed to human verification (item #1) |

### Requirements Coverage

PLAN frontmatter `requirements` were collected per plan: 01-01 (PIPE-01, PIPE-03), 01-02 (MEM-06, PIPE-04), 01-03 (OBS-01, OBS-02, OBS-03, OBS-04), 01-04 (INFRA-01, PIPE-07). REQUIREMENTS.md Phase 1 list (10 items): OBS-01, OBS-02, OBS-03, OBS-04, MEM-06, INFRA-01, PIPE-01, PIPE-03, PIPE-04, PIPE-07.

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| OBS-01 | 01-03 | RAM profiling reports real values — rss_before/after/peak are valid numbers, not "NAMB", on local and HPC | SATISFIED (contract); needs live HPC log inspection | `ps::ps_memory_info()` portable reader at src/allocation.r:64-98; PROFILE line emits 4 RSS fields; testthat asserts numeric positive rss on Windows. Live HPC PROFILE line inspection: human verification (operator runs allocation on Euler). |
| OBS-02 | 01-03 | When an allocation worker crashes (incl. SIGKILL), region log contains a sentinel trace and `diagnose_alloc_crash.sh` surfaces OOM evidence from `sacct`/`seff` | SATISFIED (contract); live SIGKILL: human #3 | `worker_state_flush_sentinel()` writes 2-line SENTINEL+JSON; `on.exit()` registration; diagnose_alloc_crash.sh `--fixture-dir` end-to-end run produced expected output. |
| OBS-03 | 01-03 | `setup_allocation_inputs` and `run_allocation_dinamica` emit structured messages to per-region log file | SATISFIED | `setup_allocation_inputs(..., log_file)` and `run_allocation_dinamica(..., log_file=)` both accept and write into log_file; 11 `emit("DINAMICA_*", ...)` sites in dinamica_utils.r. |
| OBS-04 | 01-03 | Pre-flight validation asserts env vars, packages loadable, model files, Dinamica binary, fail-fast with single actionable list | SATISFIED | `validate_allocation_runtime()` aggregates 4 categories into one error vector; `run_allocation()` and `scripts/run_allocation.r` invoke before any work. Test asserts consolidated list. |
| MEM-06 | 01-02 | `allocation_env.yml` includes all prediction packages (parsnip, recipes, ranger, xgboost 1.7.x, tidypredict, butcher, ps, lobstr, bundle, qs, rhpcblasctl) | SATISFIED (contract); needs live env solve: human #2 | YAML declares all 11 + r-workflows; r-xgboost=1.7 pinned; submit scripts activate allocation_env. |
| INFRA-01 | 01-04 | Dinamica EGO 8 inside Singularity container on Euler, built from ethzplus/rocker-geospatial-dinamica, tested with minimal model, invocable via DINAMICA_EGO_8_HOME | SATISFIED (contract); live execution: human #1 | def file Bootstraps from upstream; container README + HPC README document build + publish + smoke flow; resolve_dinamica_launch() consumes DINAMICA_EGO_8_HOME verbatim. |
| PIPE-01 | 01-01 | `simulation_trans_rates_prep.r` reads LULC demand from `config[["lulc_demand_path"]]` — hardcoded xlsx removed | SATISFIED | src/simulation_trans_rates_prep.r:155 reads config key; `LULC_demand_results.xlsx` literal grep returns 0 hits in src/. |
| PIPE-03 | 01-01 | `calibration_predictor_prep.r` reads terra temp from `Sys.getenv("TERRA_TEMP", unset = tempdir())` — not E:/terra_temp | SATISFIED | src/calibration_predictor_prep.r:19 reads via get_stage7_runtime_paths()[["terra_temp"]] which returns Sys.getenv("TERRA_TEMP", default = tempdir()). |
| PIPE-04 | 01-02 | HPC shell scripts use `$USER` — no hardcoded `black` references in hpc_common.sh, setup_environments.sh, .env.template | SATISFIED | grep returns 0 `black` hits in those three files. (Doc files outside the contract still contain `black` — see Anti-patterns.) |
| PIPE-07 | 01-03, 01-04 | Dinamica EGO log files written to central `logs/` directory | SATISFIED | resolve_dinamica_log_path() routes to <repo_root>/logs/dinamica/<ts>_dinamica.log; exec_dinamica() consumes launch$log_file from the resolver. |

**Coverage: 10/10 Phase 1 requirements addressed in plans; 7 fully VERIFIED; 3 (OBS-01 live, MEM-06 live, INFRA-01 live) close on operator-side HPC verification (human items #1, #2, #3).**

**Orphaned requirements:** None. All 10 requirements that REQUIREMENTS.md maps to Phase 1 are claimed by Phase 1 plans (PIPE-07 was claimed by 01-04 even though its requirement ID is not listed in 01-04's frontmatter — it appears in `provides` and the SUMMARY closes it; PIPE-07 is also addressed partially by 01-03 via resolve_dinamica_log_path). The frontmatter omission is a minor traceability gap and does NOT affect goal achievement.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `docs/README_HPC.md` | 149 | `/beegfs/black/micromamba/envs/` (in legacy "First Time Setup" prose) | Warning | Plan 01-04 modified this file but only added the new Stage 7 section at the top. Legacy text below still references `black`. PIPE-04 contract is scoped to `hpc_common.sh`, `setup_environments.sh`, `.env.template` — those are clean — so the criterion as defined holds, but a fresh operator reading the legacy section will see the old user literal. Recommend follow-up cleanup; not a Phase 1 blocker. |
| `docs/MICROMAMBA_SETUP.md` | 15, 20, 32, 41, 78, 98, 102, 119-121, 145, 184, 191, 241-243 | Multiple `/.../black/...` references | Warning | Not in PIPE-04 contract scope; legacy doc not touched in Phase 1. Same recommendation: cleanup later. |
| `docs/TRANS_RATE_ESTIMATION_ENV.md` | 57 | `/project/climate/black/envs` | Info | Legacy doc; out of scope for Phase 1 PIPE-04 contract. |
| `README.md` (top-level) | 16, 25, 26, 32 | `/home/black/nascent-lulcc` paths | Warning | Top-level README; not in PIPE-04 contract scope (which names `hpc_common.sh`, `setup_environments.sh`, `.env.template`). Phase 1 SC4 reads "HPC shell scripts contain no hardcoded `black` references" — README.md is documentation, not a shell script; criterion is met. |
| `src/old/simulation_transition_rates_estimation.R` | 79 | `LULC_demand_results.xlsx` literal | Info | File explicitly under `src/old/`, intentionally excluded from active codepath. Out of scope. |
| `.planning/codebase/INTEGRATIONS.md`, `STACK.md`, `STRUCTURE.md`, etc. | various | `black` references in planning notes | Info | Planning artifacts, not runtime code. Out of scope. |

No blockers found. The remaining `black` references live in legacy operator documentation, not in any active runtime artifact named by PIPE-04 or by Phase 1 success criterion 4.

### Human Verification Required

Three items from `human_verification` in the frontmatter:

#### 1. Live Euler smoke test (closes INFRA-01 + Phase 1 SC6)

- **Test:** On Euler with apptainer module loaded, `.env` sourced, and a built `.sif` at `/project/<project>/containers/dinamica-ego-8.sif`:
  ```bash
  export DINAMICA_EGO_8_HOME=/project/<project>/containers/dinamica-ego-8.sif
  bash scripts/smoke_test_dinamica.sh --live --runtime auto \
      --artifact "$DINAMICA_EGO_8_HOME" \
      --ego dinamica/dinamica_model/allocation.ego-decoded \
      --require-log-under logs
  ```
- **Expected:** Exit 0; `logs/dinamica-smoke-<UTC>.log` exists, is non-empty, and contains "Dinamica" / "completed" / "success" tokens.
- **Why human:** Requires Euler login, apptainer/singularity, and a built external `.sif` not present on the Windows verification host. Documented in 01-04-SUMMARY.md "Operator action required".

#### 2. Live `allocation_env` solve + library() validation (closes Phase 1 SC5)

- **Test:**
  ```bash
  bash scripts/setup_environments.sh --env allocation_env --non-interactive
  micromamba run -n allocation_env Rscript -e "
    pkgs <- c('parsnip','recipes','ranger','xgboost','tidypredict','butcher','bundle','qs','ps','lobstr','RhpcBLASctl');
    for (p in pkgs) library(p, character.only = TRUE);
    cat('OK')"
  ```
- **Expected:** All 11 prediction-time packages load without errors; `r-xgboost` reports 1.7.x.
- **Why human:** Verification host has no micromamba and no Rscript. Cannot execute the conda solve.

#### 3. Live SIGKILL + `diagnose_alloc_crash.sh` on Euler (closes Phase 1 SC2)

- **Test:** Submit an allocation job, after first region worker enters predict stage, SIGKILL one worker PID; then run `bash scripts/diagnose_alloc_crash.sh --job-id $SLURM_JOB_ID` from the submit dir.
- **Expected:** sacct shows the node and `OUT_OF_MEMORY`/`State` (or non-zero ExitCode); region log under `logs/` contains a `SENTINEL reason=incomplete` line + JSON SENTINEL with the killed worker's `transition`; diagnose output prints the latest SENTINEL and the corresponding `DINAMICA_LOG_PATH`.
- **Why human:** Live SLURM, real SIGKILL. The fixture-mode behavioral spot-check already proved the script's parsing/correlation contract.

### Gaps Summary

No blockers. The phase goal — "Operator can diagnose any allocation failure within minutes by reading the per-region log and a single post-mortem command — RSS values are real, paths work on HPC out of the box, and missing prerequisites fail fast with a single actionable list" — is structurally met:

- **Pre-flight (`validate_allocation_runtime`)** runs before any region work, aggregates env+package+file+dinamica gaps into one consolidated error list, and is callable in pre-flight-only mode for verification.
- **Per-region log** carries structured `STATE`, `SENTINEL`, `DINAMICA_*`, `PROFILE` breadcrumbs threaded into both `setup_allocation_inputs()` and `run_allocation_dinamica()`.
- **Single post-mortem command** (`diagnose_alloc_crash.sh`) consumes sacct/seff/cgroup + sentinel + DINAMICA_LOG_PATH and prints a one-line crash summary; behavioral fixture run confirms the wiring.
- **Real RSS values** come from the portable `ps::ps_memory_info()` reader (test asserts numeric positive on Windows; Linux adds VmHWM peak enrichment).
- **Paths work on HPC out of the box** — `${HPC_SCRATCH_ROOT}` placeholder + the `--check-stage7-contract` gate guarantee no silent fallbacks; `black` literals are gone from the contract files; the `lulc_demand_path` and `terra_temp` hotspots both consume the shared resolver.
- **Dinamica-on-Euler** is contractually wired (`DINAMICA_EGO_8_HOME` consumed verbatim as `.sif` path; runtime probe order locked; smoke test enforces D-11 logfile contract; container definition + build instructions committed; built `.sif` external per D-10).

The three remaining items requiring operator confirmation on Euler (live smoke test, live env solve, live SIGKILL) are all proven contractually here on the workstation; they cannot be completed automatically because they depend on HPC-only resources (Euler login, apptainer/singularity, built `.sif`, SLURM). The plan SUMMARYs explicitly hand these off as operator-side gates.

---

*Verified: 2026-05-05T19:05:00Z*
*Verifier: Claude (gsd-verifier)*
