# Phase 3 UAT — Parallelism & Memory Architecture

## Phase Goal
A full allocation run on HPC completes for at least one scenario × region × timestep with bounded per-worker RAM and no OOM kills.

---

## Success Criteria

| # | Criterion | Status | Evidence |
|---|-----------|--------|----------|
| SC1 | `future::multicore` selected automatically on Linux HPC; `future::multisession` on Windows | PENDING | Smoke script forces `ALLOCATION_PARALLEL_STRATEGY=sequential` — auto-selection not yet tested |
| SC2 | At least one scenario × region × timestep runs to completion without OOM | PARTIAL | Job 364249: R pipeline complete (28 prob TIFs, no OOM, peak RSS ~97GB). Dinamica step bypassed — `run_allocation_dinamica()` short-circuits on `Sys.which("DinamicaConsole") == ""` before reaching the apptainer path. Posterior = anterior copy. |
| SC3 | RSS profiling shows per-worker memory bounded; `models_list` loaded once in parent | CONFIRMED ✓ | Full PROFILE RSS lines in both local and HPC runs. `predictor_preload elapsed=33.259s` on HPC |
| SC4 | Nhood rasters as TIF files; workers receive paths; strict-globals passes | CONFIRMED ✓ | "Reopening neighbourhood rasters from TIF paths for predictors" logged before every transition |
| SC5 | BLAS/data.table/arrow/GDAL threads pinned to 1 before `future::plan()` | CONFIRMED ✓ | HPC log: `pin_native_threads_to_one OMP_NUM_THREADS=1 data.table_threads=1` |

---

## Bug Fix Log

All fixes applied to `src/allocation.r` and `scripts/submit_allocation_smoke.sh`.

### Fix 1 — Arrow deadlock on HPC (2026-05-22)
**Symptom:** HPC hung indefinitely after "Prepared for probability map generation" — 11h yesterday, 12min+ today. Local `test_prob_maps.r` unaffected.
**Root cause:** `pin_native_threads_to_one()` sets both `arrow::set_cpu_count(1L)` AND `arrow::set_io_thread_count(1L)`. Arrow's executor deadlocks when both pools are pinned to 1: CPU thread waits for IO completion, IO thread waits for CPU to drain backpressure. On fast local SSD the timing window is never hit; on Lustre with high metadata latency it deadlocks immediately. `test_prob_maps.r` never calls `pin_native_threads_to_one()`, so local was unaffected.
**Fix:** Removed `arrow::set_io_thread_count(1L)` from `pin_native_threads_to_one()`. IO threads block on disk/network — they don't cause CPU oversubscription and should not be pinned.
**Confirmed:** HPC predictor preload now completes in 33s (matching local).

### Fix 2 — Save loop hang (2026-05-22)
**Symptom:** Local `test_prob_maps.r` hung for 3+ hours at "Saving probability maps..." with an empty output directory.
**Root cause:** Two compounding issues:
1. `normalized <- rbindlist(gather)` produces ~106M rows (all transitions concatenated). Save loop did `normalized[row_idx == k]` — an unkeyed full scan — 26 times = ~2.8B row comparisons before any write.
2. `terra::rasterize(x = as.matrix(dt_j[, .(x, y)]), ...)` redid a spatial coordinate→cell lookup for up to 12M points per transition despite `cell_id` being available.
**Fix:** Added `data.table::setkey(normalized, row_idx)` after normalization; changed filter to `normalized[.(k)]` (binary search); replaced `terra::rasterize()` with `terra::setValues(anterior, NA_real_)` + direct cell assignment `r[dt_j$cell_id] <- dt_j$prob`.
**Confirmed:** `scripts/test_prob_map_save.r` (synthetic data, full shape) ran to completion in 247.9s, writing 28 TIFs.

### Fix 3 — mlr3 version mismatch on HPC (2026-05-22)
**Symptom:** `mlr3 predict_newdata() failed: could not find function ".__Task__col_info"` on every transition.
**Root cause:** `.qs` model files were serialized with a different mlr3 version than what is installed in HPC `allocation_env`. The stored R6 train_task has method bindings from the old version; the current mlr3 doesn't have `.__Task__col_info`.
**Fix:** Added fallback in `predict_saved_transition_prob()` — if `predict_newdata()` fails, extracts the underlying fitted model (`learner$model`) and predicts directly: ranger via `predict(model, data, num.threads=1L)`, glmnet via `predict(model, newx, type="response", s="lambda.min")`. Logs "Path: mlr3 direct fallback".
**Status:** Fallback triggers correctly. Next issue: OOM during ranger prediction (Fix 4).

### Fix 4 — OOM during ranger prediction on HPC (2026-05-22)
**Symptom:** Job OOM-killed ~19min into the first transition prediction (105→101, 12M cells).
**Root cause:** Before the first prediction, RSS is already ~47.5GB:
- Base R session + 38 loaded models + anterior: ~41GB
- Per-transition predictor join (+4.5GB) + nhood extraction (+2GB)
- Leaves only ~16.5GB of the 64GB allocation for ranger working memory
- ranger traversing 500 trees × 12M observations × 1 thread exceeds the headroom

Note: fixing the mlr3 version mismatch (Fix 3) would NOT resolve this — the memory footprint is identical whether ranger is called via mlr3 or directly.
**Fix:** Increased `submit_allocation_smoke.sh` from `--mem-per-cpu=16G` to `--mem-per-cpu=32G` (64GB → 128GB total).

---

## Open Tests

- [ ] **SC2 — Fix 5**: `run_allocation_dinamica()` checks `Sys.which("DinamicaConsole") == ""` and falls back before reaching `exec_dinamica()` / apptainer. On HPC, `DinamicaConsole` is never on PATH. Fix: guard the fallback behind `backend == "local"` only, or remove it and let `exec_dinamica()` handle both backends. Also verify `DINAMICA_EGO_8_HOME` (.sif path) is exported in the smoke script.
- [ ] **SC1**: Backend auto-selection not tested — smoke script still forces `ALLOCATION_PARALLEL_STRATEGY=sequential`. Either remove the override or submit a separate HPC run without it to validate `future::multicore` selection on Linux. **Non-blocking for phase goal.**

---

## Test Sessions

### Session 1 — 2026-05-21 HPC run #1 (pre `0942b1a`)
Got past parent preload, reached Transition 105→101 — timed out loading parquet per-transition with cell_id IN filters (~7h).

### Session 2 — 2026-05-21 HPC run #2 (post `0942b1a`)
SENTINEL at `stage=setup_inputs` — Arrow filter/select ordering bug (fixed in commit `0942b1a`).

### Session 3 — 2026-05-21 Local `test_prob_maps.r`
All 26 transitions predicted. Hung at "Saving probability maps..." for 3+ hours (Fix 2). Terminal lost to scheduled restart.

### Session 4 — 2026-05-22 HPC run #3
Arrow deadlock at predictor preload (Fix 1). No further progress.

### Session 5 — 2026-05-22 HPC run #4 (Fix 1 applied)
Predictor preload: 33s ✓. mlr3 version mismatch on first prediction → Fix 3 fallback triggered. OOM killed during ranger prediction of 105→101 (12M cells) → Fix 4.

### Session 6 — 2026-05-22 Local `test_prob_map_save.r`
Synthetic normalized table (106M rows, same shape as real). Save loop: 247.9s, 28 TIFs ✓ (Fix 2 confirmed).

### Session 7 — 2026-05-22 HPC run #5 (Job 364249, 128GB)
Resubmitted with `--mem-per-cpu=32G` (128GB total). All 26 transitions predicted via mlr3 direct fallback. 28 probability TIFs written. Peak RSS ~97GB, within budget. SENTINEL reason=ok. Total runtime 3.61h. Status: **success for R pipeline**. However: `run_allocation_dinamica()` short-circuits on `Sys.which("DinamicaConsole") == ""` — fires on HPC because DinamicaConsole is never on PATH (apptainer is the HPC runtime). Posterior = anterior copy. Dinamica CA model did not execute.

### Session 8 — PENDING
Fix `run_allocation_dinamica()` fallback guard + verify `DINAMICA_EGO_8_HOME` in smoke script, then resubmit to confirm Dinamica runs via apptainer.
