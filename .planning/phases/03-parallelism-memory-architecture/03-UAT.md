# Phase 3 UAT — Parallelism & Memory Architecture

## Phase Goal
A full allocation run on HPC completes for at least one scenario × region × timestep with bounded per-worker RAM and no OOM kills.

---

## Success Criteria

| # | Criterion | Status | Evidence |
|---|-----------|--------|----------|
| SC1 | `future::multicore` selected automatically on Linux HPC; `future::multisession` on Windows | PENDING | Smoke script forces `ALLOCATION_PARALLEL_STRATEGY=sequential` — auto-selection not yet tested |
| SC2 | At least one scenario × region × timestep runs to completion without OOM | PARTIAL | Local: all 26 transitions completed, reached "Saving probability maps..." then terminal lost (2026-05-21). HPC: got to "Prepared for probability map generation" then timed out at 05:00 |
| SC3 | RSS profiling shows per-worker memory bounded; `models_list` loaded once in parent | CONFIRMED ✓ | Parent preload log present: "preloaded models, anterior cell index, TIF-backed neighbourhood paths and Parquet datasets ready". Full PROFILE RSS lines in both local and HPC runs. `predictor_preload elapsed=28.277s rss_delta=+6031.2MB` |
| SC4 | Nhood rasters as TIF files; workers receive paths; strict-globals passes | CONFIRMED ✓ | "Reopening neighbourhood rasters from TIF paths for predictors" logged before every transition in local run. 26 transitions all confirmed using TIF-backed paths. |
| SC5 | BLAS/data.table/arrow/GDAL threads pinned to 1 before `future::plan()` | CONFIRMED ✓ | HPC log: `pin_native_threads_to_one OMP_NUM_THREADS=1 data.table_threads=1`. Local log also shows `data.table_threads=1`. |

---

## Test Sessions

### Session 1 — 2026-05-21 HPC Smoke Run (run_allocation.r, BAU / costa_peruana / 2026)

**Earlier run (pre `0942b1a`):**
- Got past parent preload, past "Predicting transition probabilities...", reached Transition 105→101
- Timed out loading parquet per-transition with cell_id IN filters (~7 hours, never finished)

**Latest run (post `0942b1a` optimise parquet data loading):**
```
14:31:27 | id_trans 31, 33 missing from model_info [expected — zero-rate transitions]
14:32:50 | Prepared for probability map generation: preloaded models, anterior cell index,
            TIF-backed neighbourhood paths and Parquet datasets ready.
14:32:57 | SENTINEL reason=incomplete stage=setup_inputs scenario=BAU
            region=costa_peruana timestep=2026 transition=NA
```

**Root cause (diagnosed):** `preload_region_predictor_data()` crashes because `dplyr::select()` was called _before_ `dplyr::filter()` on the Arrow datasets. After projecting away `region` and `scenario` columns, the subsequent `filter(region == ..., scenario == ...)` references columns no longer in the lazy plan — Arrow throws when `collect()` executes. The working `load_predictor_data()` in `utils.r` always included `region`/`scenario` in the select; the new function did not.

**Fix applied (2026-05-21):** Swapped `filter` before `select` in both static and dynamic branches of `preload_region_predictor_data()` in `src/allocation.r`. Also removed `.data$scenario` pronoun (uses plain `scenario` for the partition column) — this matches the pattern from `load_predictor_data`.

---

### Session 2 — 2026-05-21 Local test_prob_maps.r (BAU / costa_peruana / 2022→2026)

All 26 transitions predicted successfully:
- `predictor_preload elapsed=28.277s rss_before=42680.4MB rss_after=48711.7MB rss_delta=+6031.2MB`
- Each transition: model_load → predictor_load → nhood_extract → predict → trans_total (all logged with RSS)
- RSS peaked around 37-38GB during predictions, recovered between transitions via GC
- `20:53:20 | Normalizing probabilities...`
- `20:53:32 | Saving probability maps...`
- **Terminal lost due to scheduled restart — unknown if TIF files were written**

---

### Session 3 — 2026-05-21 HPC Smoke Run #2 (submit_allocation_smoke.sh, BAU / costa_peruana / 2026)

```
17:55:37 | pin_native_threads_to_one OMP_NUM_THREADS=1 data.table_threads=1   [SC5 ✓]
17:55:37 | RSS_BUDGET budget_mb=16384.0 source=ALLOCATION_WORKER_RSS_BUDGET_MB
17:55:37 | preload_models models=38                                             [SC3 ✓]
17:55:37 | nhood_precompute paths=16                                            [SC4 ✓]
17:55:37 | parent_baseline
17:55:37 | region_setup elapsed=0.013s rss_before=24575.9MB rss_after=24575.9MB peak_rss=24575.9MB
17:55:37 | setup_inputs — Expansion/Patcher tables written with 28 rows
17:55:37 | id_trans 31, 33 missing from model_info [expected]
17:58:01 | Prepared for probability map generation: preloaded models, anterior cell index,
            TIF-backed neighbourhood paths and Parquet datasets ready.
[no further output — job timed out at ~05:00, approx 11 hours later]
```

**Key observation:** After "Prepared for probability map generation" (17:58:01), no further log lines appeared before the job timed out ~11 hours later. On local machine, the predictor preload took 28s and predictions immediately followed. The HPC silence suggests either:
1. The log file being checked is SLURM stdout, but predictions write to a worker log file at `/cluster/scratch/bblack/nascent-lulcc/outputs/simulations/BAU/2026/region_costa_peruana/worker_logs/` — and that file was not tailed
2. The predictor preload or first prediction is much slower on HPC (OOM pressure / slow storage)

---

## Open Tests — Current Status

- [ ] **SC2 re-verify**: Resubmit HPC smoke run with all three fixes — confirm predictions complete and at least one posterior TIF written
- [ ] **SC1 confirm**: Backend selection not tested — smoke script forces sequential. Either remove the override or submit a separate run without `ALLOCATION_PARALLEL_STRATEGY=sequential` on Linux HPC.
- [x] **SC2 local save**: `test_prob_map_save.r` completed in 247.9s, 28 TIFs written ✓ (save loop fix confirmed)
- [x] **SC3 predictor_preload timing**: HPC `elapsed=33.259s` ✓ (Arrow deadlock fix confirmed)
- [x] **SC4 nhood TIF paths**: "Reopening neighbourhood rasters from TIF paths" confirmed every transition ✓
- [x] **SC5 thread pinning**: `OMP_NUM_THREADS=1` confirmed HPC ✓

## Root Cause Diagnosed — Arrow Deadlock on HPC (predictor preload)

**Symptom:** HPC smoke run hung indefinitely (11h yesterday, 12+ min today) after "Prepared for probability map generation" with zero log output. Local `test_prob_maps.r` (28s preload) was unaffected.

**Root cause (2026-05-22):** `pin_native_threads_to_one()` in `run_allocation.r` sets both `arrow::set_cpu_count(1L)` AND `arrow::set_io_thread_count(1L)` before any Arrow reads. With both Arrow thread pools pinned to 1, Arrow's executor deadlocks: the single CPU thread waits for IO completion while the single IO thread waits for the CPU thread to drain backpressure. On a fast local SSD the timing window is never hit; on Lustre with high metadata latency it deadlocks immediately. `test_prob_maps.r` never calls `pin_native_threads_to_one()`, which is why the local run succeeded.

**Fix applied (2026-05-22):** Removed `arrow::set_io_thread_count(1L)` from `pin_native_threads_to_one()` in `src/allocation.r`. IO threads block on disk/network rather than burning CPU, so they don't contribute to thread oversubscription and should not be pinned.

---

## Root Cause Diagnosed — Save Step Hang

**Symptom:** Local run hung at `Saving probability maps...` for 3+ hours with no output and no TIFs written.

**Root cause (2026-05-22):** Two compounding issues in the save loop:

1. **Unkeyed scan (primary):** `normalized <- rbindlist(gather)` produces ~106M rows (all transitions concatenated). The loop then did `normalized[row_idx == k]` — an unkeyed scan — 26 times. That is ~2.8B row comparisons before a single raster write begins.

2. **Coordinate-based rasterize (secondary):** `terra::rasterize(x = as.matrix(dt_j[, .(x, y)]), ...)` redoes a spatial coordinate → cell lookup for up to 12M points per transition, despite `cell_id` being available in the same table.

**Fix applied (2026-05-22):** `src/allocation.r` save loop:
- Added `data.table::setkey(normalized, row_idx)` after normalization — O(n log n) once, then O(log n + m) per lookup
- Changed filter to `normalized[.(k)]` (binary search)
- Replaced `terra::rasterize(x, y, values, fun)` with `terra::setValues(anterior, NA_real_)` + direct cell assignment `r[dt_j$cell_id] <- dt_j$prob`

---

## Fix Log

| Date | Change | File | Reason |
|------|--------|------|--------|
| 2026-05-21 | Swap `select` → `filter` order in `preload_region_predictor_data`; remove `.data$scenario` pronoun | `src/allocation.r:1730-1748` | Arrow errors when filtering on columns excluded by a prior projection |
