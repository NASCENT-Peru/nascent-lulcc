# 03-01 Summary

## Outcome

Implemented the Phase 3 runtime control plane in `src/allocation.r` and `scripts/run_allocation.r`.

- Added `select_allocation_plan()`, `pin_native_threads_to_one()`, and `prof_cgroup_snapshot()`.
- Added smoke-run filters for `ALLOCATION_PROFILE_SCENARIO`, `ALLOCATION_REGION_FILTER`, and `ALLOCATION_YEAR_POST_FILTER`.
- Added the strict-globals dev gate behind `ALLOCATION_DEV_STRICT_GLOBALS`.
- Replaced the fixed `future::multisession` startup with automatic `sequential` / `multicore` / `multisession` selection plus ordered startup logging.
- Added `tests/testthat/test-allocation-runtime-contract.R`.

## Verification

- Text contract checks were added for backend selection, startup ordering, strict-globals, smoke filters, and cgroup logging.
- Local execution of `Rscript`-based tests was not possible in this environment because `Rscript` is not available on `PATH`.

## Notes

- `parallelly` is now part of the Stage 7 pre-flight package contract.
- The smoke-run filters are no-ops unless their env vars are set.
