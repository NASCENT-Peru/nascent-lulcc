# Phase 3 UAT — Parallelism & Memory Architecture

## Phase Goal
A full allocation run on HPC completes for at least one scenario × region × timestep with bounded per-worker RAM and no OOM kills.

---

## Success Criteria

| # | Criterion | Status | Evidence |
|---|-----------|--------|----------|
| SC1 | `future::multicore` selected automatically on Linux HPC; `future::multisession` on Windows | PENDING | Run started, parent preload succeeded — backend not confirmed yet in logs |
| SC2 | At least one scenario × region × timestep runs to completion without OOM | **FAILED** | SENTINEL reason=incomplete stage=setup_inputs (2026-05-21 14:32:57) |
| SC3 | RSS profiling shows per-worker memory bounded; `models_list` loaded once in parent | PARTIAL | Parent preload log present: "preloaded models, anterior cell index, TIF-backed neighbourhood paths and Parquet datasets ready" (14:32:50) |
| SC4 | Nhood rasters as TIF files; workers receive paths; strict-globals passes | PARTIAL | "TIF-backed neighbourhood paths" in parent log — not yet verified in worker |
| SC5 | BLAS/data.table/arrow/GDAL threads pinned to 1 before `future::plan()` | PENDING | Not confirmed in shared log snippet |

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

## Open Tests — Requires Next HPC Run

- [ ] **SC2 re-verify**: Submit smoke run with fixed code; confirm no SENTINEL and at least one posterior TIF written
- [ ] **SC1 confirm**: Check log for "Parallel: strategy=multicore workers=N" line
- [ ] **SC2 timing**: Confirm `stage=predictor_preload` completes in reasonable time (bulk load should be <60s vs >7h for per-transition)
- [ ] **SC3 worker RSS**: Confirm `peak_rss` lines appear in worker logs and stay within budget
- [ ] **SC4 nhood TIF paths**: Confirm workers log "Reopening neighbourhood rasters from TIF paths"
- [ ] **SC5 thread pinning**: Confirm `OMP_NUM_THREADS=1` in worker PROFILE log

---

## Fix Log

| Date | Change | File | Reason |
|------|--------|------|--------|
| 2026-05-21 | Swap `select` → `filter` order in `preload_region_predictor_data`; remove `.data$scenario` pronoun | `src/allocation.r:1730-1748` | Arrow errors when filtering on columns excluded by a prior projection |
