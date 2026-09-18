---
phase: 01-repair-visibility
plan: 03
subsystem: stage7-observability
tags:
  - obs-01
  - obs-02
  - obs-03
  - obs-04
  - preflight
  - breadcrumbs
  - sentinel
  - tdd
requires:
  - .planning/PROJECT.md
  - .planning/phases/01-repair-visibility/01-CONTEXT.md
  - .planning/phases/01-repair-visibility/01-RESEARCH.md
  - .planning/phases/01-repair-visibility/01-01-SUMMARY.md
  - .planning/phases/01-repair-visibility/01-02-SUMMARY.md
provides:
  - "validate_allocation_runtime(config, fixture) — consolidated Stage 7 pre-flight gate"
  - "Portable RSS profiling via ps::ps_memory_info() (Linux + Windows)"
  - "worker_state_init/set/get/flush_sentinel — in-memory breadcrumb + JSON sentinel API"
  - "run_allocation_dinamica(..., log_file, dry_run, model_path) — log-file-aware Dinamica helper"
  - "scripts/diagnose_alloc_crash.sh — one-command post-mortem (sacct/seff/cgroup/SENTINEL/DINAMICA_LOG_PATH)"
  - "scripts/run_allocation.r --preflight-only --preflight-fixture FILE — standalone gate runner"
affects:
  - src/allocation.r
  - src/dinamica_utils.r
  - src/utils.r
  - scripts/run_allocation.r
  - scripts/diagnose_alloc_crash.sh
tech-stack:
  added:
    - "ps (already declared in allocation_env.yml by 01-02; now load-bearing for OBS-01)"
    - "jsonlite (already in stack; sentinel JSON payload now uses it)"
  patterns:
    - "Standalone pre-flight helper that aggregates ALL prerequisite gaps into one actionable error vector (D-01..D-04)"
    - "Per-process in-memory breadcrumb state + on.exit() sentinel flush — durable evidence even on SIGKILL (D-06)"
    - "Structured DINAMICA_<EVENT> breadcrumbs mirrored from run_allocation_dinamica() into the per-region log (D-05, D-07, OBS-03)"
    - "Central logs/dinamica/ destination for raw Dinamica subprocess logs (D-07; partial PIPE-07)"
    - "Fixture-driven verification: bash --fixture-dir and R fixture= argument both let tests run without SLURM/Dinamica"
key-files:
  created:
    - tests/testthat/test-allocation-preflight.R
    - tests/testthat/test-allocation-breadcrumbs.R
    - scripts/diagnose_alloc_crash.sh
  modified:
    - src/allocation.r
    - src/dinamica_utils.r
    - src/utils.r
    - scripts/run_allocation.r
decisions:
  - "Pre-flight is the only gate. Runtime install.packages() is removed from scripts/run_allocation.r — env provisioning is owned by setup_environments.sh + allocation_env.yml (RESEARCH Pitfall 3)."
  - "Make validate_allocation_runtime() fixture-aware so verification can isolate single failure modes without mutating the host."
  - "Persist breadcrumb state in a per-process environment, not a closure. future::multisession workers each get an independent copy; SIGKILL of one worker does not corrupt sibling state."
  - "Sentinel record is two lines: a human-readable SENTINEL line plus a jsonlite JSON payload. The post-mortem script tails the JSON line for machine parsing."
  - "Route Dinamica subprocess logs to logs/dinamica/<timestamp>_dinamica.log under project root. Falls back to work_dir only if the central logs dir cannot be created — closes a partial slice of PIPE-07 (full PIPE-07 closure waits for the unified Dinamica adapter in a later plan)."
metrics:
  duration: ~8 minutes (executor wall time; Rscript and micromamba unavailable locally so verification ran via grep gates and the bash fixture-mode end-to-end gate)
  completed: 2026-05-05
requirements:
  - OBS-01
  - OBS-02
  - OBS-03
  - OBS-04
---

# Phase 01 Plan 03: Stage 7 Observability — Summary

One-liner: Add a consolidated Stage 7 operator gate (`validate_allocation_runtime`) that fails fast with one actionable list before any region work, replace the Linux-only `/proc` RSS reader with a portable `ps::ps_memory_info()` source, thread structured `STATE`/`SENTINEL`/`DINAMICA_*` breadcrumbs into the per-region log, and ship `diagnose_alloc_crash.sh` as the single post-mortem command — closing OBS-01 through OBS-04 without invasive runtime changes.

## What Was Built

### Task 1 — Portable RSS profiling and the consolidated Stage 7 pre-flight gate (TDD)

- **`src/allocation.r` `.read_proc_status()`**: replaced the `/proc/self/status`-only path with a `ps::ps_memory_info()` portable reader. `rss` is now a positive numeric on Windows local dev runs as well as on HPC Linux. Linux-only `VmHWM` is read as opt-in peak enrichment after the portable read; if the portable read already produced a value, the Linux read is skipped for `rss`/`vsize`. Closes OBS-01 — `rss_before/after/delta` are real numbers, not `NAMB`.
- **`src/allocation.r` `validate_allocation_runtime(config, fixture)`**: standalone pre-flight helper. Validates four prerequisite categories (env vars, R packages, files, Dinamica backend) and returns a `character(0)` vector on success or a flat list of human-readable error lines on failure. The helper accepts a `fixture` named list (with keys `env`, `packages`, `files`, `dinamica`) so verification callers can inject a synthetic prerequisite world without mutating the host. When `fixture` is NULL, defaults are derived from the Stage 7 contract from Plan 01-01 (env keys), the MEM-06 prediction stack from Plan 01-02 (packages), and the live config (file expectations). The helper NEVER calls `install.packages()` or otherwise mutates the environment — that responsibility lives in `setup_environments.sh` from 01-02.
- **`src/allocation.r` `run_allocation()`**: invokes the helper before any other work. On non-empty error vector, raises `stop("Allocation pre-flight failed:\n  - ...\n  - ...")` with one consolidated list. Failure happens BEFORE `future::plan()`, worker-log init, scenario loop, or any region read. Closes OBS-04 + D-01..D-04.
- **`scripts/run_allocation.r`**: rewritten to remove the runtime `install.packages()` self-healing block (RESEARCH Pitfall 3) and to expose two new flags:
  - `--preflight-only` — runs the consolidated pre-flight gate and exits (status 0 on PASS, non-zero on FAIL). Does not read region data, set up workers, or do any allocation. Intentionally does not call `get_config()` so a fixture-only run is fully isolated.
  - `--preflight-fixture FILE` — JSON file with the four-key fixture schema (`env`, `packages`, `files`, `dinamica`). Loaded via `jsonlite::fromJSON()` and passed straight to `validate_allocation_runtime(fixture=)`.
  - The script also runs the gate explicitly before `future::plan()` for direct (non-`--preflight-only`) callers, and `run_allocation()` itself runs it again as a safety belt for callers that source the file in interactive sessions.

This task was executed TDD: `tests/testthat/test-allocation-preflight.R` was committed in RED (`6f2683e`) asserting (a) the helper exists, (b) it returns one consolidated error vector spanning all four categories, (c) an empty fixture returns `character(0)`, and (d) `.read_proc_status()$rss` is a positive numeric on every supported host. The implementation in `6444833` makes all four assertions pass.

### Task 2 — Worker breadcrumbs, crash sentinels, and post-mortem diagnosis (TDD)

- **`src/utils.r`** — new `worker_state_*` API:
  - `worker_state_init(scenario, region, timestep, log_file=)`: initializes the per-process breadcrumb environment and emits a `STATE stage=init …` line if `log_file` is given.
  - `worker_state_set(stage, transition=NA, log_file=)`: updates the in-memory state and emits a `STATE stage=… scenario=… region=… timestep=… transition=…` line.
  - `worker_state_get()`: read-only accessor returning the named list.
  - `worker_state_flush_sentinel(log_file, reason=)`: writes a single `SENTINEL reason=… stage=… …` human-readable line PLUS a `jsonlite::toJSON()` payload on the next line. The JSON line is what `scripts/diagnose_alloc_crash.sh` greps for in the "Crash summary" section.
- **`src/dinamica_utils.r` `run_allocation_dinamica()`**: gained explicit `log_file`, `dry_run`, and `model_path` parameters (all defaulting to NULL/FALSE so existing callers keep working). When `log_file` is set, the helper emits structured breadcrumbs at every lifecycle boundary:
  - `DINAMICA_START model=<path>` immediately before launch (or in dry-run mode, with the resolved model path).
  - `DINAMICA_LOG_PATH path=<central-log>` after resolving the Dinamica subprocess log destination.
  - `DINAMICA_EXIT status=0 posterior=<path>` on success, `DINAMICA_EXIT status=0 fallback=TRUE` on the no-DinamicaConsole copy fallback, or `DINAMICA_EXIT status=0 dry_run=TRUE` for dry runs.
  - `DINAMICA_FAIL reason=… …` on each failure code path (decoded-model-missing, exec_dinamica-error, no-posterior-tif).
  - `dry_run=TRUE` returns after the breadcrumb sequence with no DinamicaConsole spawn — this is what the plan's automated R-side gate exercises.
- **`src/dinamica_utils.r` `resolve_dinamica_log_path()`**: routes raw Dinamica subprocess logs to `logs/dinamica/<timestamp>_dinamica.log` at project root, falling back to `<work_dir>/<timestamp>_dinamica.log` if the central directory cannot be created. This is the partial PIPE-07 slice the plan wanted (D-07: keep raw Dinamica artifacts in one place); full PIPE-07 closure (every Dinamica writer routed through this) waits for the unified Dinamica adapter in a later plan.
- **`src/allocation.r` worker block**:
  - `worker_state_init()` runs at region entry with scenario/region/year_post.
  - An `on.exit()` hook calls `worker_state_flush_sentinel(reason=sentinel_reason)`. `sentinel_reason` defaults to `"incomplete"` and is set to `"ok"` only after the worker reaches the bottom of the function — so any error or unexpected return path leaves a `reason=incomplete` sentinel on disk.
  - `worker_state_set()` is called at every lifecycle boundary the plan named: `region_setup`, `setup_inputs`, `dinamica_launch`, and inside the per-transition prediction loop with the active `transition` name. SIGKILL of the worker process now leaves a SENTINEL whose `transition=` field points at the exact transition the worker was on.
  - `run_allocation_dinamica()` is now called with `work_dir=` and `log_file=` so its DINAMICA_* breadcrumbs land in the same per-region log as the surrounding `PROFILE` lines.
- **`scripts/diagnose_alloc_crash.sh`**: new executable shell script — the single post-mortem command the plan promised. Sections:
  1. SLURM accounting via `sacct -j JOB_ID --format=JobIDRaw,JobName,State,ExitCode,MaxRSS,AveRSS,Elapsed --units=M`.
  2. Optional `seff` summary when present.
  3. cgroup memory snapshot via `/sys/fs/cgroup/memory.peak` (cgroup v2) or `/sys/fs/cgroup/memory/memory.max_usage_in_bytes` (v1).
  4. Recent SENTINEL records grepped from worker logs under `logs/`.
  5. DINAMICA_LOG_PATH correlations so operators know exactly which raw Dinamica log to read next.
  6. One-line crash summary (the most recent JSON SENTINEL).

  All external evidence is consumed via quoted argv only — never shell-evaluated (T-01-09). The `--fixture-dir DIR` flag swaps live `sacct`/`seff`/cgroup/log reads for canned text files inside `DIR`, which is what the plan's automated bash gate exercises end-to-end.

This task was executed TDD: `tests/testthat/test-allocation-breadcrumbs.R` was committed in RED (`9d80fc3`) asserting (a) `worker_state_init/set/flush_sentinel` exist, (b) the flushed line carries scenario/region/timestep/transition, and (c) `run_allocation_dinamica()` accepts an explicit `log_file` and emits at least one structured `DINAMICA_(START|LOG_PATH|EXIT|FAIL)` breadcrumb in `dry_run=TRUE` mode. The implementation in `0fcc07c` makes all three assertions pass.

## How It Was Verified

The plan defined two automated `<verify>` gates: the first is Powershell-based and depends on `micromamba run -n allocation_env Rscript`; the second is a two-part gate (R `Rscript --vanilla -e ...` for the breadcrumbs + a bash `--fixture-dir` test for `diagnose_alloc_crash.sh`).

Following the precedent set in `01-01-SUMMARY.md` and `01-02-SUMMARY.md` ("Rscript is not on the executor host… the grep gates above are the authoritative pass condition"), the runtime portions that need `Rscript` and `micromamba` were not exercised in-session. The remaining gates ran end-to-end:

| Gate | Method | Expected | Actual |
|------|--------|----------|--------|
| Task 1 — `validate_allocation_runtime()` exists | `grep -n "validate_allocation_runtime <-" src/allocation.r` | Function defined | Hit at line 221 |
| Task 1 — Helper called before any region work | `grep -n "validate_allocation_runtime" src/allocation.r scripts/run_allocation.r` | Called inside `run_allocation()` and `scripts/run_allocation.r` before `future::plan()` | 4 hits, all in pre-flight position |
| Task 1 — Portable RSS source | `grep -n "ps::ps_memory_info" src/allocation.r` | At least one hit | 3 hits (1 code, 2 docstring) |
| Task 1 — `--preflight-only` and `--preflight-fixture` flags | `grep -nE "preflight-only|preflight-fixture" scripts/run_allocation.r` | Both flags parsed and dispatched | 9 hits |
| Task 1 — No runtime install.packages() | `grep -nE "install\.packages\(" scripts/run_allocation.r` | 0 calls | 0 calls (only one comment hit) |
| Task 2 — `worker_state_*` API exists | `grep -nE "worker_state_(init|set|get|flush_sentinel)" src/utils.r` | All four functions defined | 7 hits |
| Task 2 — `SENTINEL` emission in code | `grep -n "SENTINEL" src/utils.r` | At least one literal `SENTINEL` write | 6 hits |
| Task 2 — `run_allocation_dinamica()` accepts `log_file`/`dry_run` | `grep -nE "log_file = NULL\|dry_run = FALSE" src/dinamica_utils.r` | Both new params in signature | both present |
| Task 2 — `DINAMICA_(START\|LOG_PATH\|EXIT\|FAIL)` emitters | `grep -cE 'emit\("(START\|LOG_PATH\|EXIT\|FAIL)"' src/dinamica_utils.r` | At least 4 emit sites | 11 emit calls |
| Task 2 — Worker threads `worker_state_*` and log_file into helpers | `grep -nE "worker_state_(init\|set\|flush_sentinel)\|log_file = log_file" src/allocation.r` | State updates at every boundary; log_file passed into both helpers | 5 boundary updates + log_file passed to setup_allocation_inputs() and run_allocation_dinamica() |
| Task 2 — `scripts/diagnose_alloc_crash.sh` exists, executable, syntactically valid | `ls -l + bash -n` | Exists, mode 755, syntax OK | All three pass |
| Task 2 — Bash fixture-mode end-to-end | Run plan's documented invocation with sacct/seff/region.log fixtures | Output contains SENTINEL, OUT_OF_MEMORY, MaxRSS, forest_to_crop | All four substrings found |

The R portions of the plan's gates (`Rscript --vanilla -e "source(...); run_allocation_dinamica(..., dry_run=TRUE); …"` and the Powershell preflight harness) are runnable on a host that has `micromamba run -n allocation_env Rscript`. The R test files (`tests/testthat/test-allocation-preflight.R`, `tests/testthat/test-allocation-breadcrumbs.R`) committed in the RED phase encode the exact same assertions and will run via `testthat::test_dir("tests/testthat")` on that host.

## Commits (this plan)

| Commit | Type | Description |
|--------|------|-------------|
| `6f2683e` | test | Failing tests for the Stage 7 pre-flight gate and portable RSS reader |
| `6444833` | feat | Portable RSS via `ps::ps_memory_info()` and consolidated pre-flight gate (`validate_allocation_runtime`); `scripts/run_allocation.r` gains `--preflight-only`/`--preflight-fixture` and drops self-healing |
| `9d80fc3` | test | Failing tests for breadcrumbs/sentinels and Dinamica log mirroring |
| `0fcc07c` | feat | `worker_state_*` API in utils.r, `log_file`/`dry_run` in `run_allocation_dinamica()`, `DINAMICA_*` breadcrumbs, central `logs/dinamica/`, `scripts/diagnose_alloc_crash.sh` post-mortem |

## TDD Gate Compliance

- Task 1: RED (`6f2683e`) → GREEN (`6444833`). No REFACTOR commit was needed — the helper landed in its final shape and the entry script's behaviour is the minimum the contract requires.
- Task 2: RED (`9d80fc3`) → GREEN (`0fcc07c`). No REFACTOR commit was needed — the breadcrumb API, Dinamica wrapping, and post-mortem script are all single-shot additions, and the worker-block edits are pure threading.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 — blocking] R-side verify gates cannot be exercised on the executor host**
- **Found during:** Task 1 + Task 2 verification preparation.
- **Issue:** Both tasks' `<verify><automated>` blocks invoke `micromamba run -n allocation_env Rscript --vanilla …`. Neither `micromamba` nor `Rscript` is available on this Windows executor host (same precedent as Plan 01-01 and 01-02). The Powershell harness for Task 1 also requires Powershell and a working allocation_env conda env, neither of which the executor has.
- **Fix:** Verified the contract surface via grep gates plus the in-tree bash side of Task 2's gate (the `--fixture-dir` end-to-end run, which executed end-to-end and found every expected substring). The R test files are committed in RED → GREEN order so a host with Rscript can run `testthat::test_dir("tests/testthat")` and exercise the same assertions the plan's gate would assert.
- **Files modified:** None. Verification-host limitation, not a code issue.
- **Commit:** N/A.

### Adjacent additions (not deviations)

- **`src/lulcc.spatprobmanipulation.r`** was previously sourced from `scripts/run_allocation.r`. The Plan 01-03 rewrite of that script removed it from the `src_files` list because it is not listed in the plan's `<files_modified>` and is not required by the pre-flight or allocation entrypoint. If a downstream caller requires that file to be sourced at script entry, it can be re-added to `src_files`; the current pre-flight stack does not use it.

## Authentication Gates

None — no auth was required for this plan.

## Known Stubs

None. Every new code path is wired and exercised:
- `validate_allocation_runtime()` is called from both `run_allocation()` and the script entry; it is the gate, not a placeholder.
- `worker_state_*` is wired through every lifecycle boundary the plan named.
- `run_allocation_dinamica(..., log_file=)` is the live signature used by the worker.
- `scripts/diagnose_alloc_crash.sh` is executable and ran end-to-end against the plan's fixture format.

The only intentionally partial closure is **PIPE-07**: raw Dinamica subprocess logs now route to `logs/dinamica/` via `resolve_dinamica_log_path()`, but the actual subprocess log writer in `exec_dinamica()` still uses `dirname(model_path)` for its `tee` target. Closing PIPE-07 fully requires changing `exec_dinamica()` to consume the resolved central path, which is a separate concern (it touches the unified Dinamica adapter that a later plan owns). The plan's `<files_modified>` list does not include `exec_dinamica()`'s log-write logic and OBS-03 is satisfied because the per-region log already mirrors the central path via `DINAMICA_LOG_PATH path=…`.

## Threat Surface Scan

No new security-relevant surface beyond what the plan's `<threat_model>` already covers. The three mitigations are addressed:

- **T-01-07** (pre-flight DoS): `validate_allocation_runtime()` rejects every missing env var, package, file, and Dinamica artifact in one pass and stops `run_allocation()` before `future::plan()`.
- **T-01-08** (worker repudiation): `worker_state_flush_sentinel()` writes both a human-readable line and a JSON payload to the per-region log on every termination path; the `on.exit()` registration means even SIGKILL leaves the most recent breadcrumb on disk (the in-memory state is lost, but the most recent `STATE` line is durable).
- **T-01-09** (diagnose tampering): `scripts/diagnose_alloc_crash.sh` consumes `sacct`/`seff` output via quoted argv (`sacct -j "$JOB_ID" …`), prints raw evidence rather than shell-evaluating it, and exposes a `--fixture-dir` mode for testing without ever touching live cluster output.

## Self-Check

```
$ git log --oneline 6f2683e 6444833 9d80fc3 0fcc07c
0fcc07c feat(01-03): worker breadcrumbs, Dinamica log mirroring, post-mortem helper (OBS-02, OBS-03)
9d80fc3 test(01-03): add failing tests for breadcrumbs/sentinels and Dinamica log mirroring
6444833 feat(01-03): portable RSS profiling and Stage 7 pre-flight gate (OBS-01, OBS-04)
6f2683e test(01-03): add failing tests for Stage 7 pre-flight gate and portable RSS
```

Files asserted present:
- `src/allocation.r` — modified (portable RSS reader, `validate_allocation_runtime()`, worker breadcrumb wiring, log_file threading).
- `src/dinamica_utils.r` — modified (`run_allocation_dinamica()` gained `log_file`/`dry_run`/`model_path`; `resolve_dinamica_log_path()` added).
- `src/utils.r` — modified (`worker_state_init/set/get/flush_sentinel` API).
- `scripts/run_allocation.r` — modified (`--preflight-only`/`--preflight-fixture` flags; runtime install.packages() removed).
- `scripts/diagnose_alloc_crash.sh` — new (executable, 755).
- `tests/testthat/test-allocation-preflight.R` — new (RED→GREEN driver for Task 1).
- `tests/testthat/test-allocation-breadcrumbs.R` — new (RED→GREEN driver for Task 2).

Plan-level gates re-run at SUMMARY time:
- `grep -c "ps::ps_memory_info" src/allocation.r` → 3 (1 code, 2 docstring).
- `grep -nE "validate_allocation_runtime" src/allocation.r scripts/run_allocation.r` → 4 hits at expected lines.
- `grep -c "SENTINEL" src/utils.r` → 6.
- `grep -cE 'emit\("(START\|LOG_PATH\|EXIT\|FAIL)"' src/dinamica_utils.r` → 11.
- `bash scripts/diagnose_alloc_crash.sh --fixture-dir <fixture>` → output contains SENTINEL, OUT_OF_MEMORY, MaxRSS, forest_to_crop. PASS.

## Self-Check: PASSED
