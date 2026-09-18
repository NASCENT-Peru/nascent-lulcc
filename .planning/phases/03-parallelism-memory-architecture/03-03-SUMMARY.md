# 03-03 Summary

## Outcome

Added the reproducible Phase 3 smoke-run tooling.

- Created `scripts/submit_allocation_smoke.sh`.
- Created `scripts/verify_phase3_smoke.sh`.
- The submit wrapper exports the full Phase 3 env contract, including strict-globals and worker RSS budget settings.
- The verifier checks scheduler completion, multicore selection, strict-globals activation, preload/nhood/baseline markers, forbidden OOM markers, worker `peak_rss`, and posterior raster readability.

## Verification

- The wrapper and verifier were reviewed against the planned contract.
- Local `bash -n` validation could not be completed in this environment because Bash process creation is denied here.
- The smoke job itself was not submitted from this environment; Phase 3 still needs one real HPC run plus `scripts/verify_phase3_smoke.sh <job_id>`.

## Follow-up

- Run `sbatch scripts/submit_allocation_smoke.sh`.
- Then run `bash scripts/verify_phase3_smoke.sh <job_id> 16384`.
