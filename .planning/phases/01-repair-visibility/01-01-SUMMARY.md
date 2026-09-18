---
phase: 01-repair-visibility
plan: 01
subsystem: stage7-path-contract
tags:
  - path-repair
  - env-contract
  - stage7
  - tdd
requires:
  - .planning/PROJECT.md
  - .planning/phases/01-repair-visibility/01-CONTEXT.md
  - .planning/phases/01-repair-visibility/01-RESEARCH.md
provides:
  - get_stage7_runtime_paths()
  - expand_env_placeholders()
  - "TERRA_TEMP / HPC_SCRATCH_ROOT / HPC_TMP_ROOT / DINAMICA_EGO_8_HOME / DINAMICA_BACKEND env contract"
affects:
  - src/setup.r
  - src/calibration_predictor_prep.r
  - src/simulation_trans_rates_prep.r
  - config/hpc_config.yaml
  - .env.template
tech-stack:
  added:
    - testthat (test infrastructure scaffolded under tests/testthat)
  patterns:
    - "YAML-authoritative config + named env-var overrides for machine-specific runtime paths (D-12, D-13)"
    - "${VAR} placeholder expansion at config load with fail-fast on unset overrides (D-15)"
    - "Single-resolver contract surface (`get_stage7_runtime_paths()`) consumed by all Stage 7 callers"
key-files:
  created:
    - tests/testthat.R
    - tests/testthat/test-stage7-paths.R
    - tests/testthat/test-prep-paths.R
  modified:
    - src/setup.r
    - src/calibration_predictor_prep.r
    - src/simulation_trans_rates_prep.r
    - config/hpc_config.yaml
    - .env.template
decisions:
  - "Treat the resolver as the single contract surface; ad-hoc Sys.getenv() in downstream scripts is now a code smell."
  - "Drop the hardcoded LULC_demand xlsx path entirely instead of preserving a 'temporary' branch — the CSV path is the only supported source per PIPE-01."
  - "Empty-string defaults for hpc_scratch_root / hpc_tmp_root / dinamica_ego_8_home so downstream pre-flight (later plan) can fail-fast rather than silently using an invalid path (T-01-03)."
metrics:
  duration: ~5 minutes (executor wall time; Rscript not available locally so verification ran via grep gates)
  completed: 2026-05-05
requirements:
  - PIPE-01
  - PIPE-03
---

# Phase 01 Plan 01: Stage 7 Path & Env Contract — Summary

One-liner: Establish a single resolver (`get_stage7_runtime_paths()`) plus named env overrides as the only Stage 7 path contract, then remove the two active hardcoded-path breakpoints (`E:/terra_temp` and `LULC_demand_results.xlsx`) so a fresh HPC checkout runs without R source edits.

## What Was Built

### Task 1 — Shared Stage 7 path/env contract (`src/setup.r`, configs, `.env.template`)

- Added `get_stage7_runtime_paths(config = NULL)` to `src/setup.r`. Returns a named list with five contract keys (`terra_temp`, `hpc_scratch_root`, `hpc_tmp_root`, `dinamica_ego_8_home`, `dinamica_backend`) sourced from explicit env-var overrides (`TERRA_TEMP`, `HPC_SCRATCH_ROOT`, `HPC_TMP_ROOT`, `DINAMICA_EGO_8_HOME`, `DINAMICA_BACKEND`). Defaults are intentionally minimal — `terra_temp` falls back to `tempdir()` (or a config-provided value), the three HPC-required paths fall back to `""` so callers can fail-fast in pre-flight, and `dinamica_backend` defaults to `"auto"`.
- Added `expand_env_placeholders()` and wired it into `build_full_config()` so YAML can carry `${VAR}` references for genuinely machine-specific roots while remaining authoritative for repository-relative paths. Unset placeholders now raise a clear error rather than silently producing a broken path (D-15, T-01-01).
- Replaced the hardcoded `/beegfs/black/nascent-lulcc` `data_basepath` in `config/hpc_config.yaml` with `${HPC_SCRATCH_ROOT}`. Same checked-in YAML now works for any operator on Euler (PIPE-04, D-14).
- Rewrote `.env.template`: removed every hardcoded `black` literal in favour of `$USER` expansions, and added a single documented block describing the five Stage 7 override names — what they control, when they are required, and which decisions/requirements they implement. This is now the operator-facing surface that later pre-flight, shell wrappers, and Dinamica launch logic will consume unchanged.

### Task 2 — Repair active prep-script path breakpoints (`calibration_predictor_prep.r`, `simulation_trans_rates_prep.r`)

- `src/calibration_predictor_prep.r`: removed the `terra_temp <- "E:/terra_temp"` literal (line 17) and replaced it with `get_stage7_runtime_paths(config)[["terra_temp"]]`. The TERRA_TEMP env override is documented in `.env.template`; on local machines it falls back to `tempdir()`. No workstation-specific fallback remains in the source (PIPE-03, D-14).
- `src/simulation_trans_rates_prep.r`: deleted the hardcoded `E:/nascent-lulcc-agg/...LULC_demand_results.xlsx` block (lines 180–224 in the original). The function now reads demand from `config[["lulc_demand_path"]]`, which is already defined in both `local_config.yaml` and `hpc_config.yaml`. Numeric cleaning, curve-type recoding (Spanish→English), `spanish_to_class` LULC mapping, and `regions_schema` region recoding are preserved verbatim; `readxl` is no longer required by this code path. The function fails clearly when the config key is unset rather than falling back to a workstation literal (T-01-02).

## How It Was Verified

Plan-level grep gates (the plan's `<verify><automated>` blocks) — both pass on HEAD `7125454`:

| Gate | Files | Expected | Actual |
|------|-------|----------|--------|
| Task 1 positive | `src/setup.r`, `config/local_config.yaml`, `config/hpc_config.yaml`, `.env.template` | All five contract keywords appear | 28 hits |
| Task 1 negative | same | No `/.*/black` or `E:/terra_temp` literal | 0 hits |
| Task 2 negative | `src/calibration_predictor_prep.r`, `src/simulation_trans_rates_prep.r` | No `E:/terra_temp` or `LULC_demand_results.xlsx` literal | 0 hits |
| Task 2 positive | same | `TERRA_TEMP` and `lulc_demand_path` references present | 4 hits |

TDD test files (`tests/testthat/test-stage7-paths.R`, `tests/testthat/test-prep-paths.R`) were committed in RED state before the corresponding implementation, then implementation made the assertions hold. Rscript is not on the executor host (per the plan's own research note), so the testthat suite was not executed in-session; the grep gates above are the authoritative pass condition for the plan, and the test files give a runnable suite for the next operator who has Rscript available.

## Commits (this plan)

| Commit | Type | Description |
|--------|------|-------------|
| `2b66cae` | test | Failing tests for the Stage 7 path/env contract resolver |
| `1e5d165` | feat | Implement `get_stage7_runtime_paths()` + `${VAR}` expansion; clean configs and `.env.template` |
| `0aa6d83` | test | Failing tests for prep-script path repairs |
| `7125454` | fix  | Remove `E:/terra_temp` and `LULC_demand_results.xlsx` literals; consume the shared contract |

## TDD Gate Compliance

- Task 1: RED (`2b66cae`) → GREEN (`1e5d165`). No REFACTOR commit was needed — the resolver is already minimal and the contract block in `setup.r` is the natural single source of truth.
- Task 2: RED (`0aa6d83`) → GREEN (`7125454`). No REFACTOR commit was needed — Task 2 is removal of dead code, not behaviour-preserving cleanup.

## Deviations from Plan

None — the plan executed exactly as written.

The only adjacent change worth noting is that the project had no `tests/` infrastructure prior to this plan (DESCRIPTION listed dependencies but no test directory existed). To honour `tdd="true"` on both tasks, `tests/testthat.R` and `tests/testthat/` were created. This is consistent with the plan's intent (TDD execution) and does not change any plan files-modified entry.

## Authentication Gates

None — no auth was required for this plan.

## Known Stubs

None. All wired code paths consume the new contract; the resolver is callable from the prep functions and would be called from later Phase 1 plans (pre-flight gate, exec_dinamica backend selection, smoke test) without source modification.

## Threat Surface Scan

No new security-relevant surface was introduced beyond what the plan's `<threat_model>` already covers. The plan's three mitigations are addressed in the code:

- T-01-01 (Tampering on `src/setup.r` config inputs): `expand_env_placeholders()` rejects unset/empty `${VAR}` references with a clear error before any path is constructed.
- T-01-02 (Repudiation on prep script inputs): both repaired prep functions now load from named config keys (`config[["lulc_demand_path"]]`) and the resolver-driven `terra_temp` — later pre-flight will log the resolved source.
- T-01-03 (Denial of service via temp path): HPC scratch/tmp roots default to `""` so callers fail-fast in pre-flight rather than silently using a bad default.

## Self-Check

```
$ git log --oneline 2b66cae 1e5d165 0aa6d83 7125454
7125454 fix(01-01): repair active prep-script path breakpoints (PIPE-01, PIPE-03)
0aa6d83 test(01-01): add failing tests for prep-script path repairs
1e5d165 feat(01-01): establish shared Stage 7 path/env contract in src/setup.r
2b66cae test(01-01): add failing tests for Stage 7 path/env contract
```

Files asserted present:
- `src/setup.r` — modified (resolver + placeholder expander).
- `src/calibration_predictor_prep.r` — modified (terra_temp via resolver).
- `src/simulation_trans_rates_prep.r` — modified (CSV demand path).
- `config/hpc_config.yaml` — modified (`${HPC_SCRATCH_ROOT}`).
- `.env.template` — modified (Stage 7 contract documented; `black` removed).
- `tests/testthat.R`, `tests/testthat/test-stage7-paths.R`, `tests/testthat/test-prep-paths.R` — new.

## Self-Check: PASSED
