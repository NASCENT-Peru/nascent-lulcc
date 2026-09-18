# Research Summary — nascent-lulcc allocation hardening

**Researched:** 2026-05-05
**Goal:** Get `src/allocation.r` running reliably for all scenarios × regions × timesteps on ETH Euler HPC. Current crash: `MultisessionFuture interrupted` (OOM SIGKILL) after ~3 minutes locally.
**Overall confidence:** MEDIUM-HIGH on root-cause diagnosis (codebase reading is conclusive); MEDIUM on quantitative size/speed targets (no live empirical verification possible — web tools unavailable to researchers).

---

## Recommended Stack

| Decision | Pick | Why |
|---|---|---|
| Parallel backend (Linux HPC) | `future::multicore` (fork + COW) | Read-only models stay shared across workers — eliminates per-worker 1GB duplication causing OOM |
| Parallel backend (Windows local) | `future::multisession` with explicit `globals` | No fork on Windows; keeps dev workflow alive |
| Unit of parallelism | Transition within region | Maximises concurrency once workers are cheap; matches independence boundary |
| Model framework | Stay on tidymodels; `butcher` + `bundle` aggressively | mlr3 migration is multi-week; butcher+bundle reaches ~80–90% size win at <10% cost |
| XGBoost serialisation | `bundle::bundle()` | C++ booster handle survives fork/serialisation safely |
| Raster engine | terra only — finish PIPE-05 migration | `r-raster` missing from `allocation_env.yml`; latent hard failure |
| Worker raster handling | Pass file paths, not SpatRaster objects | `SpatRaster@ptr` is `externalptr` — non-serialisable; workers must `terra::rast(path)` themselves |
| Parquet reads | `arrow::open_dataset()` lazy + column projection | Lazy + per-worker reopen is far smaller than materialising row groups |
| RSS profiling | `ps::ps_memory_info()` (cross-platform) | Replaces broken `/proc/self/status` parser → fixes `rss_before=NAMB` |
| Model save format | `qs::qsave()` | 3–10× faster than `saveRDS`, comparable compression |
| Thread pinning (before fork) | `RhpcBLASctl::blas_set_num_threads(1)`, `data.table::setDTthreads(1)`, `arrow::set_cpu_count(1)`, `nthread=1` for xgboost | Prevents N_workers × M_BLAS_threads CPU oversubscription under multicore |

**Add to `allocation_env.yml`:** `r-ps`, `r-lobstr`, `r-rhpcblasctl`, `r-bundle`, `r-qs`, `r-parsnip`, `r-recipes`, `r-ranger`, `r-xgboost` (pinned 1.7.x), `r-tidypredict`, `r-butcher`. The last six are silently missing today — the butchered-model predict path hard-fails on HPC.

---

## Critical Findings

1. **`future::multisession` is the single largest cause of the OOM.** Each worker receives a serialised copy of every captured object — the 1GB+ model becomes N×1GB across N workers. The codebase comment in `calibrate_allocation_parameters.r:796` ("multicore causes OOM") has causality backwards: COW under fork *prevents* the duplication. Switching to `multicore` on Linux is near-one-line and likely yields 60–80% per-worker RAM reduction.

2. **Model objects are >1GB each (`gc_max_vcells=12,125MB` after one model load).** `butcher` and `tidypredict` are in the stack but not consistently applied at *save* time. Adding `bundle::bundle()` for XGBoost + tightening `butcher` axes + `ranger(save.memory=TRUE)` should drop models to <50–200 MB. This must precede the multicore switch — multicore wins are wasted if the parent is still 12GB.

3. **The `predict` stage takes 385–472s per transition** because the code materialises a large data.frame and calls `predict.workflow()` on it. Replacing with `terra::predict(rast, model, fun, ...)` (block-wise) or row-chunking the sparse `from_data` bounds peak RAM and gives natural progress heartbeats. Both a memory and an observability fix.

4. **Neighbourhood rasters are recomputed per worker per transition** (~78s × N transitions × N workers of redundant work). Pre-compute once per region in the parent, write to scratch as TIF, pass paths into workers — eliminates duplicate work entirely.

5. **RSS profiling is silently broken (`rss_before=NAMB`).** The `/proc/self/status` parser returns `NA`, then `sprintf("%.1fMB", NA)` produces "NAMB". Fix via `ps::ps_memory_info()`. Two lines.

6. **`MultisessionFuture interrupted` is uninformative** — OOM-killer SIGKILLs the worker before any R error handler runs. Fix is post-mortem tooling (`sacct`/`seff`/`dmesg`), sentinel-file checkpointing per transition, and `flush(con)` on log writes. `tryCatch` cannot catch SIGKILL.

7. **`terra::SpatRaster` is non-serialisable across `multisession` workers** (external C++ pointer). Audit closures for SpatRaster captures. Set `options(future.globals.onReference = "error")` during dev to surface leaks.

8. **`r-raster` is missing from `allocation_env.yml`** while 73 active call sites still use `raster::`. Latent hard failure on HPC. Either defensively add `r-raster` or finish PIPE-05 migration to terra-only.

9. **SLURM `MaxRSS` ≠ cgroup memory.** `sacct MaxRSS` reports the largest single process, not the sum across workers. The cgroup OOM-killer fires on the sum + page cache. Historical `MaxRSS` numbers have been misleading — calibrate `--mem` against cgroup usage (`/sys/fs/cgroup/memory.current`).

10. **Six prediction-time packages are missing from `allocation_env.yml`** (`r-parsnip`, `r-recipes`, `r-ranger`, `r-xgboost`, `r-tidypredict`, `r-butcher`). The butchered/bundled predict code path cannot run on HPC today.

---

## Table Stakes

**Repairs (Phase 1 — lowest risk, unblocks everything):**
- Fix `prof_toc` via `ps::ps_memory_info()`; add cgroup readout for HPC
- Add missing packages to `allocation_env.yml`
- Propagate `log_msg()` into inner allocation functions (ALLOC-05 TODO in code)
- Centralise Dinamica EGO logs (PIPE-07 TODO in code)
- Fix hardcoded paths (PIPE-01/03/04: xlsx→CSV, E:/terra_temp, black→$USER)
- Document `DINAMICA_EGO_8_HOME` in `.env.template`
- Set `set.seed()` for nhood matrix generation
- Pre-flight env validation at allocation entry
- Sentinel-file checkpoints + `flush(con)` per log write
- `diagnose_alloc_crash.sh` post-mortem script

**Memory reduction (Phase 2):**
- Tighten `butcher` + add `bundle::bundle()` for XGBoost in `transition_modelling.r` save path
- `ranger(save.memory=TRUE, respect.unordered.factors="order")`
- Switch save format to `qs::qsave()`
- One-shot `rebutcher_existing_models.r` script for already-trained models

**Parallelism switch (Phase 3):**
- Switch to `future::multicore` on Linux with Windows fallback guard
- Pin BLAS/data.table/arrow/xgboost threads = 1 before `future::plan()`
- Audit closures for SpatRaster captures

**Predict & I/O refactor (Phase 4):**
- Pre-compute neighbourhood rasters once per region, parent-side → scratch TIFs
- Replace data.frame predict with `terra::predict(rast, model, fun)` or row-chunked sparse predict
- Lazy `arrow::open_dataset()` + per-worker reopen + column projection
- Atomic writes: `<name>.tmp.tif` → `file.rename`
- Skip-already-done at (scenario, timestep, region, transition) granularity

---

## Key Architecture

```
SLURM job (1 node, 1 task, N CPUs, --mem sized to cgroup not MaxRSS)
└── R parent process (Linux HPC)
    ├── Pre-flight validation (env vars, packages, files, binaries)
    ├── Pin BLAS/data.table/arrow/xgboost threads = 1
    ├── Per-region init (sequential, ONCE per region):
    │   ├── Load butchered+bundled models into models_list[]  (read-only)
    │   ├── Pre-compute ALL nhood rasters → scratch TIFs
    │   └── Build nhood_paths named character vector
    └── future::plan(multicore, workers = N)
        └── future_map(transitions_in_region, function(j) {
              # Fork-shared (CoW): models_list, nhood_paths, focal_matrices
              # Worker reopens: terra::rast(nhood_paths[...]), arrow::open_dataset(...)
              # Worker writes: probability_map_<j>.tif (atomic via .tmp + rename)
              # Worker logs: heartbeat per chunk, RESULT line at end
            })
    ├── Normalise probabilities (sequential, rbindlist long-format)
    ├── exec_dinamica()  — ONE DinamicaConsole subprocess per region
    └── Mosaic posteriors → posterior_<region>.tif
```

**Memory budget target (estimated):** parent ~2GB shared via CoW + ~300–600 MB private per worker × 4 workers = ~3.5–5 GB resident on `--mem=16G`. Current observed: crashes at ~12 GB after one model load.

**Three-layer change, in dependency order:**
1. Object-size layer — strip models to <50–200 MB each (butcher + bundle)
2. Parallelism layer — switch to multicore; switch unit of work to transition-within-region
3. Caching/IO layer — pre-compute nhoods once; pass file paths not SpatRasters; lazy arrow datasets

---

## Watch Out For

| # | Pitfall | Prevention |
|---|---------|------------|
| 1 | `MultisessionFuture interrupted` is undiagnosable — SIGKILL fires before any R error handler runs | Sentinel files per transition; `flush(con)` after every `log_msg()`; build `diagnose_alloc_crash.sh` |
| 2 | `terra::SpatRaster` in worker closures — external C++ pointer, non-serialisable | Always pass paths (character) into workers; open with `terra::rast()` inside worker; set `options(future.globals.onReference = "error")` during dev |
| 3 | `multicore` with thread-unsafe native libs (xgboost, BLAS) — CPU oversubscription | Set all native thread counts = 1 BEFORE `future::plan()`; use `bundle::unbundle()` on first predict in worker |
| 4 | `butcher::axe_env()` can break `recipes::bake()` | Test predict-equality on 5-row sample after every butcher step; treat butchered models as predict-only |
| 5 | SLURM `MaxRSS` ≠ OOM-killer's view | Set `--mem` (total) + `--cpus-per-task` explicitly; log `/sys/fs/cgroup/memory.current` from R; use `seff` post-mortem |

---

## Suggested Phase Order

| Phase | Scope | Risk | Impact |
|-------|-------|------|--------|
| 1 — Repair & Visibility | RSS fix, missing pkgs, log propagation, path fixes, pre-flight validation, sentinel files, post-mortem script | LOW | Unblocks all subsequent diagnosis |
| 2 — Model Size Reduction | Tighten butcher, bundle XGBoost, ranger surgery, qs save format, rebutcher existing models | LOW | 70–90% model size reduction before parallelism switch |
| 3 — Parallelism Switch | multicore on Linux, thread pinning, closure audit | LOW–MEDIUM | 60–80% per-worker RAM reduction |
| 4 — Predict & I/O Refactor | Nhood pre-compute, terra::predict block-wise, lazy arrow, atomic writes, skip-already-done | MEDIUM | Removes dominant 385–472s predict cost + last memory peaks |
| 5 — terra Migration (PIPE-05) | Port 73 raster:: calls to terra; intervention YAML paths; CVXR loop port | MEDIUM | Eliminates latent HPC hard failure |
| Deferred: MLR3-01 | Only if Phase 2 leaves models >200 MB | — | — |

**Rationale:** Phase 1 lights up the dashboard. Phase 2 must precede Phase 3 — multicore wins are wasted on a 12GB parent. Phase 3 must precede Phase 4 — need correct profiling to validate predict refactor. Phase 5 can run alongside Phase 4 (no code overlap).

---

## Verification Flags

Before implementation, confirm empirically on Euler:
1. **Euler cgroup version** (`cat /proc/cgroups | grep memory`) — determines correct `/sys/fs/cgroup/memory.*` path
2. **`future::multicore` semantics** on installed R version — confirm against current `future` docs
3. **`butcher` axes for current ranger and xgboost versions** — verify which axes preserve `predict()`
4. **xgboost 1.7 → 2.x serialisation compatibility** — verify pin is consistent across all envs
5. **`terra::SpatRaster` serialisation through qs/saveRDS** — confirm cache design uses TIF files, not serialised SpatRasters
6. **`tidypredict` XGBoost coverage** at current versions — verify before relying as size-reduction path
