---
status: partial
phase: 01-repair-visibility
source: [01-VERIFICATION.md]
started: 2026-05-05T00:00:00Z
updated: 2026-05-05T00:00:00Z
---

## Current Test

[awaiting human testing on Euler HPC]

## Tests

### 1. Live Euler smoke test (INFRA-01 / SC6)
expected: `scripts/smoke_test_dinamica.sh --live` completes with exit 0, `exec_dinamica()` invokes the minimal model via `apptainer exec $DINAMICA_EGO_8_HOME DinamicaConsole`, and a timestamped `logs/dinamica-smoke-*.log` artifact is written.
result: [pending — requires apptainer + built .sif on Euler]

### 2. Live allocation_env solve + library() load (MEM-06 / SC5)
expected: `micromamba activate allocation_env` resolves on HPC and all 11 prediction-time packages (`r-parsnip`, `r-recipes`, `r-ranger`, `r-xgboost`, `r-tidypredict`, `r-butcher`, `r-ps`, `r-lobstr`, `r-bundle`, `r-qs`, `r-rhpcblasctl`) load via `library()` without error.
result: [pending — requires micromamba + HPC Linux environment]

### 3. Live SIGKILL + diagnose_alloc_crash.sh (OBS-02 / SC2)
expected: After a real allocation worker is OOM-killed, `bash scripts/diagnose_alloc_crash.sh` surfaces SLURM `sacct`/`seff` OOM evidence, a SENTINEL entry in the relevant region log, and a MaxRSS metric.
result: [pending — requires SLURM + cgroup memory evidence from a real HPC run]

## Summary

total: 3
passed: 0
issues: 0
pending: 3
skipped: 0
blocked: 0

## Gaps
