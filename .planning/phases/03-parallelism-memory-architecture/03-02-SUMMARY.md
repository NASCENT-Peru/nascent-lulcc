# 03-02 Summary

## Outcome

Implemented the parent/worker memory-contract refactor in `src/allocation.r`.

- Added parent-side model preload via `load_allocation_models()`.
- Added parent-side neighbourhood raster materialization via `write_nhood_tif()` and `prepare_region_nhood_paths()`.
- Added parent-side timestep preparation via `prepare_region_worker_inputs()`.
- Refactored workers to consume precomputed `anterior_path`, preloaded `models_list`, and `nhood_paths`.
- Removed the old in-worker neighbourhood raster cache path.
- Added worker RSS budget logging via `ALLOCATION_WORKER_RSS_BUDGET_MB`.
- Added `tests/testthat/test-allocation-memory-contract.R`.

## Verification

- Text contract checks now cover parent preload, path-based neighbourhood rasters, parent baseline markers, and worker-budget hooks.
- Local execution of `Rscript`-based tests was not possible in this environment because `Rscript` is not available on `PATH`.

## Notes

- Parent preload / nhood-precompute / parent-baseline markers are emitted both in parent logs and mirrored into worker logs for smoke verification.
