# Domain Pitfalls — R Parallel / Spatial / ML Pipeline Robustness

**Domain:** R LULCC pipelines using `future::multisession` + `terra` + `tidymodels` on Windows + SLURM HPC
**Researched:** 2026-05-05
**Researcher confidence overall:** MEDIUM-HIGH (Context7 / live web search were unavailable in this session; findings drawn from the project's own crash logs, CONCERNS.md, allocation.r source, and well-documented behaviour of the R packages in question. Items requiring empirical verification are flagged as LOW.)

---

## Reading guide

Each pitfall has:
- **What goes wrong** — observable symptom
- **Root cause** — why
- **Warning signs** — early-detection cues in this codebase
- **Prevention** — concrete change
- **Phase to address** — which roadmap phase should own the fix

Phases referenced (proposed by roadmap):
- **P1 Stabilise** — make `allocation.r` not OOM (ALLOC-01..05)
- **P2 Observability** — accurate RSS, structured errors (ALLOC-03/04)
- **P3 Memory shrink** — model size, raster sharing, env hygiene (PIPE-05, MLR3-01)
- **P4 HPC hardening** — SLURM accounting, cgroup limits, $USER paths (PIPE-04)
- **P5 Pipeline correctness** — CSV/CVXR/path fixes (PIPE-01..03, 06, 07)

---

## CRITICAL Pitfalls

These cause the current crash or will cause it again on HPC.

### C1. `MultisessionFuture interrupted` is a side-effect of OOM-kill, not a diagnosable error

**What goes wrong:** `furrr::future_map` (line 707 of `allocation.r`) raises `MultisessionFuture interrupted` and the worker disappears. No traceback, no R error object, no message in the per-region log file because the worker process was SIGKILL'd before it could flush its log.

**Root cause:** `future::multisession` workers are independent R processes connected to the parent via socket. When the OS (Linux OOM-killer, Windows job-object limit, or SLURM cgroup `memory.max`) terminates the worker, the parent only observes a broken socket. `future` reports this as `MultisessionFuture interrupted` because it has no way to distinguish OOM-kill from segfault from `kill -9` from a network blip. R's condition system never runs in the killed worker, so `tryCatch`, `withCallingHandlers`, and the per-region `log_file` flush all happen before allocation of the lethal byte and are not visible in the log because the line was buffered.

**Warning signs:**
- Log line "Region: andes (ID=...)" appears, then nothing further from that worker.
- `sacct -j <jobid> --format=JobID,MaxRSS,State,ExitCode` shows `OUT_OF_MEMORY` or `0:9` (SIGKILL) for the worker step.
- Local Windows: Task Manager shows committed memory spiking before the crash; `pageant`-style swap thrash precedes the failure.
- `dmesg | grep -i "killed process"` on Linux shows the OOM kill with PID and RSS.
- `prof_toc` PROFILE lines in the log stop mid-stage (e.g. "stage=predict" started but "elapsed=" line never arrives).

**Prevention:**
1. **Stage-level checkpointing inside the worker.** Wrap each `j` iteration of `generate_probability_maps` (line 1242) in `tryCatch` that writes a sentinel `done_<j>.txt` AND `flush(stdout)` and `close(con)` on the log file before each `gc()`. So when the kill happens, the last completed iteration is on disk.
2. **Run `furrr::future_map` with `.options = furrr_options(seed = TRUE, scheduling = 1L)`** so that R-level errors propagate; OOM kills cannot be made into R errors but at least non-OOM failures will show up.
3. **Wrap the worker body in `withCallingHandlers`** that flushes log on any condition. Currently `prof_toc`, `log_msg`, etc. write but rely on default buffering — set `con <- file(log_file, open = "at", blocking = TRUE)` and explicitly `flush(con)` after every write.
4. **Run a minimal repro on a single region serially first** (`plan(sequential)`). If the same OOM happens serially, the issue is per-region size, not per-worker duplication. If it only happens in parallel, the issue is worker duplication (see C2).
5. **Treat `MultisessionFuture interrupted` as "go look at sacct / dmesg / Resource Monitor" — never as a diagnostic in itself.** Document this in the allocation README so future-you doesn't waste hours trying to grep the R log.

**Phase:** P1 (checkpointing + log flushing) and P2 (post-mortem tooling: `scripts/diagnose_alloc_crash.sh` that runs `sacct`, `dmesg`, and tails per-region logs). Confidence: HIGH on root cause; HIGH on prevention #4 (sequential repro); MEDIUM on #3 (flushing helps but doesn't survive SIGKILL — only sentinel files do).

---

### C2. `terra` SpatRaster objects are non-exportable to `future` workers (silent duplication, then OOM)

**What goes wrong:** When `furrr::future_map` ships a closure to a worker, `future` walks the closure environment to find globals to export. `terra::rast()` returns a `SpatRaster` whose useful payload is a C++ `Rcpp_SpatRaster` external pointer (`externalptr`) into the GDAL/terra C++ heap. External pointers cannot be serialised across processes — when serialised and unserialised in the worker, the pointer either becomes `<pointer: 0x0>` (dangling) or, more commonly, terra's S4 wrapper notices and re-opens the file by path, causing each worker to re-read the raster from disk and allocate its own copy of the C++ data. `future`'s globals scanner flags such objects with the warning `non-exportable object: <name>` and (depending on `future.globals.maxSize` and `future.globals.onMissing`) either errors out or silently exports a broken handle.

In `allocation.r`, the message in your scenario specifically names `region_rast` and `current_lulc`. Look at lines 733 and 736:

```r
region_rast <- terra::rast(region_rast_path)   # line 733
current_lulc <- terra::rast(current_lulc_path) # line 736
```

These are created **inside** the worker's `function(idx)` body (good!), so they should not be exported from the parent. The "non-exportable" warning therefore most likely comes from the `config` object or the `regions` data frame indirectly carrying a SpatRaster reference (e.g. via a closure captured before the parallel section), OR from `nhood_raster_cache` / `focal_matrices` / `anterior` which are created in the worker and then captured by the `get_nhood_raster` closure that is invoked from inside `for (j in ...)` loop.

**Root cause:**
1. `SpatRaster` payload is `externalptr` to GDAL handle + C++ raster cache → can't cross process boundary.
2. `terra` re-opens the file by path on unserialise → each worker holds its own GDAL handle and tile cache.
3. R `serialize`/`unserialize` does not deep-copy the C++ data, so the parent and workers can never *share* the raster — multisession is shared-nothing by design.
4. `future`'s `getGlobalsAndPackages()` is conservative: anything reachable from the closure is shipped; if the closure inadvertently captures `anterior` (created early in `generate_probability_maps`) and that closure is later passed to a nested future or saved to disk, the warning fires.

**Warning signs:**
- Warning text: `non-exportable object found: '<name>' of class 'SpatRaster' / 'PackedSpatRaster' / 'Rcpp_SpatRaster'`
- Per-worker memory in `sacct MaxRSS` ≈ N × single-process baseline rather than ~1× shared.
- `terra::sources(rast)$source` is `""` for an unserialised raster (in-memory — definitely broken cross-process) vs a file path (re-readable but causes per-worker disk I/O and tile cache).
- File handle count (`lsof -p <worker_pid> | wc -l`) explodes as workers re-open the same `.tif`.

**Prevention:**
1. **Always pass raster *paths* (character) into worker closures, not SpatRaster objects.** Open inside the worker. Your code already does this for `region_rast_path` and `current_lulc_path` (good). Audit `setup_allocation_inputs`, `generate_probability_maps`, and `compute_single_nhood_raster` to ensure no SpatRaster is captured by a closure that crosses workers.
2. **For SpatRasters that must cross a process boundary, use `terra::wrap()` / `terra::unwrap()`.** `wrap()` returns a `PackedSpatRaster` containing either the file path metadata or, for in-memory rasters, the values inlined as an R vector — both of which serialise correctly. The `anterior` raster passed to `get_nhood_raster` would be a candidate IF you ever batched that loop with another future.
3. **Set `options(future.globals.onReference = "error")`** during development. This converts the silent "non-exportable" warning into a hard error, so you find the leak source instead of silently rebuilding the raster in every worker.
4. **Set `options(future.globals.maxSize = Inf)`** only after you have confirmed there are no inadvertent SpatRaster exports — otherwise you mask the warning by raising the limit.
5. **For sharing a single read-only raster across workers on Linux, use `plan(multicore)` instead of `multisession`.** `multicore` uses fork() so SpatRaster external pointers are valid in the child *until* the child writes to GDAL state (copy-on-write at the OS page level). This is much more memory-efficient. Caveat: `multicore` does not work on Windows and is not recommended inside RStudio; it is fine in `Rscript` jobs on Euler.
6. **Don't put `anterior` in the closure of `get_nhood_raster`.** Instead, pass `anterior` explicitly as an argument. This makes the data dependency obvious and prevents accidental capture in nested futures later.

**Phase:** P1 (audit closures for SpatRaster captures + `onReference = "error"`); P3 (evaluate `plan(multicore)` for HPC, keep `multisession` for Windows dev). Confidence: HIGH on the wrap/unwrap mechanism; HIGH on multicore vs multisession on HPC; MEDIUM on which exact object triggers the warning in your case (the warning message names it — check the next crash log).

---

### C3. `future::multisession` duplicates the entire R session per worker — including all loaded packages

**What goes wrong:** Each `multisession` worker is a fresh `Rscript` process. On startup it loads every package the parent has loaded (terra, arrow, ranger, xgboost, parsnip, recipes, workflows, tidymodels, ...). Per the project notes, model objects are >1GB and `gc_max_vcells=12,125MB` after one model load. With 3 regions in parallel, the steady-state RAM is ≈ 3 × (R baseline + packages + 1GB model + raster cache + parquet read buffer) plus the parent process. On a local Windows machine with 32GB RAM and ~10GB occupied by Windows + other apps, this overruns and OOMs in ~3 minutes — exactly the crash profile.

**Root cause:** `future::multisession` is shared-nothing. There is no copy-on-write between parent and workers (unlike `multicore` / fork). Every package the worker loads is a full duplicate of the parent's package memory. Every `readRDS(model.rds)` call in each worker materialises a separate copy of the 1GB model.

**Warning signs:**
- 3 regions in parallel × ~4-5GB per worker = 12-15GB just for the workers. If your dev box has 16-32GB physical RAM, OOM is expected.
- `pryr::mem_used()` in the *parent* shows a small number (e.g. 500MB) but `htop` / Task Manager shows total committed memory many times larger.
- On HPC, `sacct -j <jobid> --format=MaxRSS` is much larger than parent's reported memory.

**Prevention:**
1. **Right-size worker count to RAM, not CPU.** `nbrOfWorkers <- min(parallel::detectCores() - 1, floor(available_ram_gb / per_worker_ram_gb))`. With 1GB models + 3GB raster overhead per worker, 3 workers needs ≈12GB; on a 16GB Windows dev machine, 2 is the realistic max.
2. **For Linux HPC runs, use `plan(multicore, workers = N)`.** Forked children share package memory via copy-on-write until they modify it. `xgboost` and `ranger` predict are typically read-only on the model object → memory stays shared. Real-world saving: 30-60% RSS reduction is common.
3. **Reduce model size first (see M1).** Going from 1GB to 100MB models removes the dominant per-worker cost.
4. **Sequential fallback for local dev.** Add `if (Sys.getenv("ALLOCATION_PARALLEL", "TRUE") == "FALSE") plan(sequential)` and document the env var. Faster to debug and uses 1/N the RAM.
5. **Process regions sequentially, parallelise transitions within a region.** The 7 transitions per region are independent; parallelising at the inner loop with a smaller per-worker payload may give better RAM/CPU tradeoff. Caveat: the model is still re-read per worker, so this only wins if you also share the loaded model — which means `multicore`.

**Phase:** P1 (worker-count guard + sequential fallback); P3 (multicore on HPC + model shrink). Confidence: HIGH.

---

### C4. RSS profiling is broken on the platform that needs it most (HPC)

**What goes wrong:** The `prof_tic`/`prof_toc` machinery in `allocation.r` reads `/proc/self/status` (lines 48-68). Per the comments and PROJECT.md ("rss_before=NAMB in all newer runs"), this returns `NA` on Windows (no `/proc`) and is *expected* to work on Linux/HPC. If it's `NA` on HPC too, something is breaking the read.

**Root cause candidates:**
1. The worker's working directory contains characters that trip `file.exists("/proc/self/status")` — unlikely; absolute path.
2. `/proc/self/status` exists but `VmRSS` line is missing — happens inside some restricted containers (Singularity/Apptainer with `--no-mount /proc`); check whether HPC is using a container.
3. `parse_kb` returns NA because the regex `[0-9]+` matched the wrong field; if `kb` is `NA`, `kb / 1024` is `NA`. Defensive but not the bug.
4. The string `"NAMB"` in PROJECT.md suggests the *format string* `%.1fMB` was given an `NA_real_`. R's `sprintf("%.1fMB", NA)` returns `"NAMB"` — that's the smoking gun. So `/proc/self/status` is being read but `parse_kb("VmRSS")` returns NA on HPC.
5. **Most likely:** `readLines("/proc/self/status", warn = FALSE)` raises a permission error inside `tryCatch`-wrapped futures because the worker's seteuid context loses /proc access — verify by `system("cat /proc/self/status | head")` on the HPC interactive node.
6. Alternative: the `regmatches` on a non-matching line returns `character(0)`, `as.numeric(character(0))` returns `numeric(0)`, the `is.na(kb)` check passes (length zero is not NA), then `kb / 1024` returns `numeric(0)`, sprintf prints `NAMB`. Add `length(kb) == 0L` guard (your code already has one — verify it actually triggers).

**Warning signs:** Log lines containing `rss_before=NAMB` or `peak_rss=NAMB`. `gc_max_vcells` will still be a number because that comes from `gc()`, so the discriminator is whether the RSS fields are NA.

**Prevention:**
1. **Add a one-shot diagnostic at worker startup:** `message(readLines("/proc/self/status")[1:5])` to confirm the file is readable and what its contents look like.
2. **Fall back to `ps::ps_memory_info()`** which works across platforms and reads from /proc on Linux and from Win32 API on Windows. Make `prof_toc` cross-platform-correct.
3. **Verify on HPC interactively** by running a 1-line script that reads `/proc/self/status` from an `Rscript` invocation, before assuming the parser is wrong.
4. **Add SLURM cgroup memory readout** as a separate metric: `readLines("/sys/fs/cgroup/memory/memory.usage_in_bytes")` (cgroup v1) or `readLines("/sys/fs/cgroup/memory.current")` (cgroup v2). This is the number SLURM uses to OOM-kill you.

**Phase:** P2 (Observability). Confidence: HIGH on the symptom mechanism (`sprintf("%.1f", NA)` → "NA"); MEDIUM on which of the 4-5 candidate causes is the real one. The fix is the same: switch to `ps::ps_memory_info()` and validate.

---

### C5. `raster` and `terra` coexisting in the same session causes silent type coercion and double-allocation

**What goes wrong:** Per CONCERNS.md, `lulcc.spatprobmanipulation.r` has ~50 `raster::` calls, `spatial_interventions_prep.r` has more, and `allocation_env.yml` does not include `r-raster`. Three failure modes:

1. **Hard ImportError on HPC** if `r-raster` is missing from `allocation_env`. The first `raster::stack(...)` call inside an active code path raises `there is no package called 'raster'` and crashes the worker mid-allocation. This is invisible in the log because workers silently die (see C1).
2. **Silent coercion when both packages are loaded.** `as(spat_raster, "Raster")` and `terra::rast(raster_layer)` both *copy* the underlying data — at large extent (Andes ≈ 42M cells, 4 bytes float = 168MB per layer) every coercion doubles memory.
3. **Method dispatch surprises.** `mask`, `extract`, `extend`, `merge`, `crop` exist in both packages. `library(raster); library(terra)` resolves to terra (loaded last), but a script that does `raster::mask(spat_obj, ...)` will throw "not a valid Raster object". The reverse — `terra::mask(raster_obj)` — works through coercion but allocates a new SpatRaster, doubling memory.

**Root cause:** R's S4 method dispatch and the two packages' overlapping API surface. `raster` is in maintenance mode; `terra` is the supported successor. Mixing them is a transitional state, not a stable pattern.

**Warning signs:**
- Warnings like `'as("RasterLayer", "SpatRaster") is deprecated; use rast() instead'`.
- `tracemem(obj)` shows new addresses after every `mask`/`extend` call — copies are happening.
- `pryr::object_size()` on the same logical raster differs by >2x between the raster and terra representations.
- Worker log ends abruptly on first call to a function that uses raster::.

**Prevention:**
1. **Audit `allocation_env.yml` and `simulation_setup_env.yml`** — add `r-raster` defensively if any sourced file in the env's call graph imports it. This makes the latent failure visible (warning at session start, not crash mid-job).
2. **Migrate `lulcc.spatprobmanipulation.r` and `spatial_interventions_prep.r` to terra-only** (PIPE-05). Direct mapping for most calls:
   - `raster::stack()` → `terra::rast()` (same syntax for file lists)
   - `raster::mask()` → `terra::mask()`
   - `raster::overlay()` → `terra::lapp()` or `terra::app()`
   - `raster::rasterFromXYZ()` → `terra::rast(xyz, type="xyz")`
   - `raster::extract()` → `terra::extract()` (return type differs: terra returns data.frame, not vector — adjust callers)
3. **Add a session-start guard:** `if ("raster" %in% loadedNamespaces() && "terra" %in% loadedNamespaces()) warning("raster and terra both loaded — coercion overhead likely")`.
4. **`landscape_pattern_analysis.r` uses retired `SDMTools`/`rgdal`/`rgeos`** — these are *removed* from CRAN. If allocation ever sources this (it shouldn't, per CONCERNS.md), it will fail to install. Move to `src/old/` or rewrite with `landscapemetrics`.

**Phase:** P3 (full migration in PIPE-05); P1 (defensive `r-raster` add to envs to surface the latent failure). Confidence: HIGH.

---

### C6. SLURM `sacct MaxRSS` does not equal what the OOM killer sees — different memory-accounting modes

**What goes wrong:** SLURM has *three* memory limits and *two* accounting strategies. If you `--mem=16G` your job and the worker reports `MaxRSS=12000M`, you may still get OOM-killed because the cgroup view counts page cache, anonymous mmap, and child-process memory differently from the per-PID `MaxRSS` that `sacct` shows.

**Root cause:** Three concepts:
1. **`MaxRSS` in `sacct`** — peak RSS of the *single largest process* in the job step (per-PID, not summed). With `multisession` workers, each is a separate process; `MaxRSS` shows the biggest one, NOT the sum.
2. **cgroup `memory.max` (or v1 `memory.limit_in_bytes`)** — hard limit enforced by the kernel. When the *sum* of the cgroup's RSS + swap (without page cache, depending on mode) exceeds this, the OOM killer fires inside the cgroup.
3. **`--mem` vs `--mem-per-cpu`** — `--mem=16G --cpus-per-task=8` gives 16GB total; `--mem-per-cpu=2G --cpus-per-task=8` also gives 16GB total but is computed differently. Mixing them causes silent under-allocation.

The lethal interaction: 3 `multisession` workers each at 5GB RSS = 15GB total cgroup RSS; one big single process at 7GB → `sacct MaxRSS=7000M`, but cgroup sees 22GB and kills you. PROJECT.md says ETH Euler has "large-memory nodes available" but no SLURM directives are documented.

**Warning signs:**
- `sacct -j <jobid> --format=JobID,MaxRSS,AveRSS,ReqMem,State` shows `State=OUT_OF_MEMORY` while `MaxRSS` is well under `ReqMem`.
- `seff <jobid>` reports memory utilisation < 100% but state is OOM.
- `dmesg` (only readable by root on HPC; ask cluster admins) shows `Memory cgroup out of memory: Killed process <PID>`.
- Local Windows: similar issue — Job Object memory limits if running under SLURM-on-Windows or under a containerized launcher.

**Prevention:**
1. **Always set both `--mem` (total) AND `--cpus-per-task` explicitly.** Don't rely on `--mem-per-cpu` defaults. For allocation: estimate `total_mem = parent_baseline + N_workers × per_worker_peak × safety_factor(1.3)`. With 3 workers × 5GB × 1.3 = ~20GB; request `--mem=24G`.
2. **Use `seff` and `sacct` post-mortem on every allocation run.** Build it into `scripts/diagnose_alloc_crash.sh`. Specifically: `sacct --format=JobID,JobName,State,ExitCode,ReqMem,MaxRSS,MaxVMSize,AveRSS,Elapsed --units=G -j $JOBID`.
3. **Periodically poll cgroup memory in-process** (separate background thread or just per-stage in `prof_toc`): `readLines("/sys/fs/cgroup/memory.current")`. This is the actual number SLURM uses to decide OOM. Compare to `/proc/self/status:VmRSS` to detect divergence (page cache growth from terra raster I/O is a common cause).
4. **Kill `R_DEFAULT_PACKAGES` bloat:** `Sys.setenv(R_DEFAULT_PACKAGES = "NULL")` in submit script reduces per-worker baseline by ~30-80MB.
5. **DO NOT use `memory.limit()` in R — it is Windows-only and was removed in R 4.2+.** Some StackOverflow answers still recommend it; ignore them. R has no portable memory cap.
6. **Don't trust the SLURM job's `MaxRSS`-vs-`ReqMem` graph** when using multisession. The peak is summed across processes, not maxed.
7. **Test the job at small scale on Euler with `srun --pty bash`** and run the allocation interactively for one region. Watch `top`/`htop` for total RSS across all R processes (parent + workers). If interactive gives 18GB total but `--mem=16G` was requested for the batch, you have your answer.

**Phase:** P4 (HPC hardening). Confidence: HIGH on mechanism; MEDIUM on the specific Euler cgroup version (v1 or v2) — verify with `cat /proc/cgroups | grep memory` on a compute node.

---

## MODERATE Pitfalls

### M1. `butcher` does not preserve `predict()` for all model types — and the order of axes matters

**What goes wrong:** `butcher::axe_env()`, `axe_call()`, `axe_ctrl()`, `axe_data()`, `axe_fitted()` strip different parts of a fitted parsnip/workflow object. Some are safe; others break `predict()`:

- `axe_env(rf_workflow)` is generally safe for `ranger` random forests: the prediction code does not need the training environment.
- `axe_call(workflow)` removes the symbolic call. Safe for `predict()` but breaks `update()`, `tidy()`, and any code that re-prints the model summary.
- `axe_data(workflow)` removes the training data — safe for `predict.workflow()` IF the recipe is already trained (i.e. `bake`able). Your code already handles this (line 289-290 comment).
- `axe_ctrl(workflow)` removes the control object. Safe for prediction but breaks `tune::collect_predictions()` etc.
- For `xgboost`, the workflow stores both a parsnip wrapper and a serialised booster (`xgb.Booster`). `butcher` axes the parsnip wrapper but cannot reach inside the booster. `xgboost`'s booster has a known issue: the underlying C++ object's pointer becomes invalid after R session restart unless re-loaded with `xgb.load.raw`. Your code's `predict_saved_butchered_prob` (lines 256-272) explicitly handles this by re-loading the booster from `model_obj$model$xgb_raw` — good. But this only works if `save_minimal_model()` actually serialises to raw bytes; check that path is hit.
- `recipes::bake()` requires the recipe to be *trained*. `butcher` does not untrain a recipe but `workflows::add_recipe()` refuses trained recipes (line 286-287 comment in your code). So you cannot rebuild a workflow after butchering — must call `predict()` directly on the inner model. Your code does this; just be aware that any future refactor that goes back through `workflows::extract_recipe() %>% workflows::workflow() %>% workflows::add_recipe()` will fail.

**Warning signs:**
- `predict()` after butchering raises `Error: object 'data' not found` (axed too aggressively — usually `axe_call` removed something predict needs to reflect on).
- `Error: argument "x" is missing` from inside `parsnip::predict.model_fit` — the parsnip wrapper lost its `spec` and the dispatcher can't decide which underlying predict to call.
- `Error in xgb.Booster.handle: invalid xgb.Booster.handle` — the C++ pointer is dangling; you forgot to reload from `xgb_raw`.
- Silent: predictions are all NA or all the same value — usually a recipe that lost its `step_*` parameters.

**Prevention:**
1. **Butcher in a known order, then sanity-check `predict()` on a 5-row sample before saving.** Scripted: train → butcher → `predict(butchered, head(training_data, 5)) == predict(original, head(training_data, 5))` within tolerance. If not, escalate to a less aggressive butcher recipe.
2. **For xgboost, always store `xgb.save.raw(booster)` alongside the workflow** and reload with `xgb.load.raw` at predict time. Your code does this; document it as a hard requirement.
3. **For ranger, set `importance.mode = "none"` at training** (your `restore_ranger_importance_mode` is a workaround for old saved models — fix at training time too). Reduces model size by ~10% and avoids the importance.mode-missing error after butchering.
4. **Don't butcher and then expect `update_model()` to work.** Workflow surgery after butcher is a dead-end; treat butchered models as predict-only.
5. **`tidypredict` is the nuclear option:** converts the model to a SQL-style expression tree. Works for GLM, RF (single tree at a time), and limited xgboost. Your code already supports it. But:
   - tidypredict's xgboost support requires xgboost 1.7.x specifically (CONCERNS.md notes this is pinned). xgboost 2.x changed the booster JSON schema and tidypredict has not caught up (LOW confidence — verify with current `tidypredict` release notes).
   - tidypredict expressions are interpreted by `eval()` per row — potentially slower than the original model on large data; benchmark before trusting it for the 385-472s `predict` stage.

**Phase:** P3 (model shrink) and MLR3-01 evaluation. Confidence: HIGH on butcher mechanism; MEDIUM on tidypredict xgboost compatibility (training-data dependent).

---

### M2. Memory leak patterns in `future` that accumulate across iterations

**What goes wrong:** `future::multisession` workers persist across `future_map` calls (they are reused — that's the whole point vs `multisession + cleanup` per call). Several leak sources accumulate over many iterations:

1. **Connection leaks.** Each `arrow::open_dataset(...)` or `file(log_path, "at")` opens an OS file handle. If not explicitly closed, R's GC eventually closes them but only after the corresponding R object becomes unreferenced AND a GC sweep runs. With many transitions per region, file handles can leak. `lsof -p <worker_pid>` will show this growing.
2. **Worker-private state.** Anything assigned with `<<-` (super-assignment) in a worker (your `focal_matrices <<- fm` on line 1216) persists in the worker's globalenv between iterations. If the same worker handles multiple regions, the second region inherits the first region's `focal_matrices` cache. Usually a feature, not a bug — but if `focal_matrices` ever changes between regions, you get stale values silently.
3. **terra GDAL cache.** `terra::gdalCache(size_mb)` defaults to ~25MB but raster operations grow it. Each worker has its own cache; over hundreds of focal/extract calls the cumulative tile cache can balloon. Set `terra::gdalCache(100)` to cap it; call `terra::tmpFiles(remove = TRUE)` to clear the on-disk temp. Also clear in-memory: `terra::sources(rast)` followed by explicit `rm()`.
4. **`gc()` does not free C-allocated memory.** R's `gc()` only manages the R-managed heap. terra's external pointers, xgboost's booster memory, ranger's prediction matrices, and arrow's record batches are all in C/C++ heaps invisible to `gc()`. You can call `gc()` thousands of times and total RSS keeps growing if these C heaps are not explicitly freed.
5. **`furrr_options(seed = TRUE)` is not enough — the worker's `.Random.seed` accumulates state.** Not a memory leak per se but can cause non-reproducibility. Your code uses it (good).
6. **Closure environment retention.** Every `function(...)` created inside the worker closes over its lexical environment. If that environment includes a SpatRaster or 1GB model and the function is stored anywhere (e.g. `nhood_raster_cache` env in line 1200), the model/raster is pinned in memory forever.

**Warning signs:**
- RSS grows monotonically across iterations even though each iteration's stage timing is constant.
- Worker `lsof | wc -l` increases per region.
- After a few regions, `predict` time grows even though data size is constant — usually due to GDAL tile cache thrashing or paging.
- `gc_max_vcells` from your `prof_toc` is stable but actual RSS is not — this is the C-heap leak signature.

**Prevention:**
1. **Don't rely on `gc()` to release native memory.** Add explicit `terra::tmpFiles(remove = TRUE)` and `terra::gdalCache(100)` calls per region. For arrow, close datasets explicitly: there's no `close(ds_static)` API but reassigning to `NULL` and running `gc()` releases the underlying schema/metadata.
2. **Restart workers periodically.** `future::plan(multisession, workers = N, gc = TRUE)` causes futures to call `gc()` after each future resolves. For deeper cleanup, use `future::ClusterRegistry("stop")` and re-create the plan every K regions. Trades startup overhead for memory hygiene.
3. **Profile RSS over iterations, not just within one.** Add a per-region `ps::ps_memory_info()$rss` log line and plot it. If it trends up, you have a leak; if it sawtooths, GC is keeping up.
4. **Use `pryr::mem_change()` to find leaks within an R session** (parent or sequential mode). It reports the RSS delta of an expression. `pryr::mem_used()` reports R-heap only; you need `ps::ps_memory_info()` for C-heap.
5. **Profile native heap with `valgrind --tool=massif Rscript ...` on Linux** for the worst offenders. Heavy but definitive. Or use `tracemem` and `gctorture(TRUE)` to amplify R-side issues.

**Phase:** P2 (per-iteration RSS logging) + P3 (worker restart + native heap clean-up). Confidence: HIGH on the mechanisms; MEDIUM on which specific source dominates in your case (would need a profile run).

---

### M3. arrow Parquet reads with predicate pushdown can silently materialise more than expected

**What goes wrong:** `arrow::open_dataset()` is lazy — no data loaded. But `dplyr::filter()` followed by `dplyr::collect()` runs the predicate on every row group whose statistics overlap the filter. If your hive partitioning is `region=int32, scenario=utf8` (your code), and your filter is `region == 1 & scenario == "BAU"`, arrow correctly narrows to one partition. BUT if the partitioning is wrong, or the parquet files are written without statistics, arrow scans everything.

In your `load_predictor_data` (called from line 1327), you're filtering by `cell_ids` (a ~millions-long vector). Arrow turns `%in% cell_ids` into a bloom-filter or a hash-join and may load every row group of the partition into memory before filtering. For 42M-cell Andes region with ~30 predictors at 8 bytes each = ~10GB potentially in memory.

**Warning signs:**
- `predictor_load` PROFILE line shows `rss_delta=+>5000MB` and `elapsed=>30s` even when `cell_ids` is small.
- arrow warning: `Filter expression is not a recognized scalar function`.
- The Parquet file's row-group statistics are missing (`pyarrow.parquet.read_metadata(file).row_group(0).column(0).statistics is None`) — disables predicate pushdown.

**Prevention:**
1. **Verify pushdown** by running the filter under `arrow::ExecPlan_BuildAndRun()` or by checking timing on a sample query. `system.time(ds %>% filter(region == 1) %>% collect())` should be O(seconds) for a per-region partition.
2. **Sort the parquet files by `cell_id`** at write time. Statistics-based pruning then works on `cell_id %in% ...`. Without sorting, every row group's `[min, max]` covers the full range and pruning is useless.
3. **For `cell_ids %in% small_set`, prefer an explicit `cell_id >= min(cell_ids) & cell_id <= max(cell_ids)`** post-filtered with `dplyr::filter()` after `collect()`. Arrow can push the range filter; the membership test is then in-memory on a much smaller batch.
4. **Use `arrow::write_dataset(..., row_group_size = 100000)`** for predictably-sized row groups (default is 1M which is often too coarse for selective predicates).
5. **Cap arrow's memory pool**: `arrow::set_cpu_count(2); arrow::default_memory_pool()$bytes_allocated()`. If you see arrow holding GBs after a `collect()` returns, you have a leak; force-release with `gc(full=TRUE)` and consider per-call `arrow::mimalloc_memory_pool()` in 12+ to enable `release_unused()`.

**Phase:** P3 (memory shrink — pushdown verification). Confidence: MEDIUM-HIGH on pushdown semantics; MEDIUM on whether your specific parquet layout has statistics (verify with `arrow::read_parquet_metadata()`).

---

### M4. `data.table` copy-on-modify edge cases that break the "no-copy" assumption

**What goes wrong:** `data.table` claims modify-by-reference, but several common patterns silently force copies:
- `dt[, new_col := value]` — modifies in place. Good.
- `dt2 <- dt[lulc_class == from_val]` — your line 1289. Returns a *new* data.table (a subset, not a view). Memory cost = nrow(subset) × ncol × 8 bytes.
- `from_data <- data.table::copy(from_idx)` — your line 1314. Explicit deep copy. Doubles memory.
- `pred_data[from_data, on = "ref_cell_id"]` — your line 1349. Right-join returning a new data.table; allocates the result.
- Shallow vs deep: `data.table::shallow()` creates a new wrapper sharing column vectors. Used internally; rarely needed in user code, but can save memory if you're doing a quick read-only subset.

For the Andes region's anterior_dt at ~42M rows × 5 cols, every `copy()` is ~1.5GB. Three of these in sequence (sparse subset → copy → join → augment with neighborhood) and you're at 4-5GB just for one transition's intermediate tables. Your code does call `rm(fitted_wf, pred_result, from_data, pred_data); gc(verbose=FALSE)` (line 1418-1419) — good.

**Warning signs:**
- RSS spikes during transition predictions and doesn't recover until end of region.
- `tracemem()` on `from_data` shows new addresses after each `:=` chain.
- `pryr::object_size(from_data)` > expected by 2-3x.

**Prevention:**
1. **Avoid `copy()` unless you need to mutate without affecting source.** Your `data.table::copy(from_idx)` (line 1314) — verify you actually need it. If you only `:=` columns that don't exist in `anterior_dt`, you're modifying a subset, not the source; `copy` is unnecessary.
2. **Process transitions in chunks of cells** if the transition's from-class set is too large. E.g. 10M cells × 30 cols = 2.4GB — split into 1M-cell chunks, predict each, append to a sink data.table.
3. **Use `setDT(df)` instead of `as.data.table(df)` where possible** — same effect, no copy.
4. **Drop unused columns aggressively.** After `pred_data[from_data, on="ref_cell_id"]`, do you still need `x` and `y` if neighborhoods are extracted? If yes, keep; if not, `from_data[, c("x","y") := NULL]`.

**Phase:** P3 (memory shrink). Confidence: HIGH.

---

### M5. `terra::extract()` and `terra::focal()` allocation patterns

**What goes wrong:**
- `terra::extract(stack, xy_matrix)` — for N points across K layers, allocates a (N × K) data.frame in R. Your `nhood_extract` stage takes 78s; if N ≈ 10M and K ≈ 5 layers, that's 50M doubles = 400MB just for the result. Plus intermediate buffers.
- `terra::focal(rast, w = matrix)` — applies a moving window. Tile-by-tile internally, but the output is a new SpatRaster of the same extent. Your `compute_single_nhood_raster` returns this raster; cached in `nhood_raster_cache` (line 1200). If you cache 5 nhood layers per region, that's 5 × extent × 4 bytes = e.g. 5 × 168MB = 840MB resident per region — per worker.

**Warning signs:**
- `nhood_extract` PROFILE line shows large `rss_delta`.
- Cache (`nhood_raster_cache`) grows to many entries; `length(ls(nhood_raster_cache))` after a region is large.
- `terra::mem_info()` reports high in-memory raster memory.

**Prevention:**
1. **Limit `nhood_raster_cache` size.** Once you've used a nhood raster for all transitions that need it, evict it. Track usage and drop entries that won't be revisited.
2. **Write nhood rasters to disk and reopen.** `terra::writeRaster(nhood, tempfile())` + `terra::rast(path)` — now the in-memory footprint is just the metadata + tile cache, not the full grid.
3. **Use `terra::extract(rast, xy_matrix, method="simple")` and `as.matrix()` directly** — skip the data.frame wrapper.
4. **Tune `terra::gdalCache(MB)`** — set to a fraction of available worker RAM to avoid runaway tile caching.
5. **Process focal operations on tiled subsets.** `terra::makeTiles()` + per-tile focal + merge.

**Phase:** P3. Confidence: MEDIUM-HIGH (depends on actual extract scale).

---

### M6. `xgboost` 2.x is not drop-in compatible with `tidypredict` / older `parsnip`

**What goes wrong:** xgboost changed its booster JSON serialisation between 1.x and 2.x. Models trained on xgboost 1.7.x and saved as `xgb.save()` may not load on 2.x and vice versa. `tidypredict::tidypredict_fit.xgb.Booster` was written against 1.x's tree format; 2.x's added `learner_model_param.base_score` and other fields that tidypredict's parser may not handle.

CONCERNS.md says xgboost is "pinned for tidypredict compatibility" — good. But conda environment drift is a real risk: a future `micromamba update` could bump xgboost.

**Warning signs:**
- `xgboost::xgb.load.raw(raw_bytes)` raises `[xgboost] booster JSON schema version mismatch`.
- `tidypredict::tidypredict_fit(xgb_model)` raises `Error in tidypredict_to_column: tree_id not found` or similar.
- Predictions from a freshly-trained model differ from the saved-then-loaded version of the same model.

**Prevention:**
1. **Pin xgboost in `allocation_env.yml` and `transition_modelling_env.yml` to the same version.** Currently CONCERNS.md says 1.7.x is pinned — verify it's pinned in *all* envs that touch xgboost models.
2. **Add a smoke test** that trains a tiny xgboost model, saves it, loads it, predicts, and compares — runs as the first thing in allocation if `ALLOCATION_TEST_MODE=TRUE`.
3. **Document the upgrade path** in a comment near `predict_saved_butchered_prob`: "If you bump xgboost, retrain all transition models."

**Phase:** P3 (alongside MLR3-01 evaluation). Confidence: MEDIUM (xgboost compat changes are version-specific; verify against current xgboost release notes).

---

### M7. Dinamica EGO subprocess hangs on Windows after worker process death

**What goes wrong:** `processx::run(DinamicaConsole, ...)` spawns Dinamica as a child of the R worker. If the R worker is OOM-killed (per C1), the Dinamica process becomes orphaned. On Linux, init (PID 1) reaps it. On Windows, Job Objects normally clean up, but if Dinamica is launched detached, it can persist as a zombie consuming CPU and memory.

**Warning signs:**
- `tasklist | findstr Dinamica` (Windows) or `ps -ef | grep -i dinamica` (Linux) shows running Dinamica processes after R has exited.
- Subsequent allocation runs report file lock conflicts on Dinamica logs/outputs in the region work_dir.

**Prevention:**
1. **`processx::process$new(..., cleanup = TRUE, cleanup_tree = TRUE)`** ensures the entire process tree (Dinamica + any of its children) is killed when the R process dies. Verify your `dinamica_utils.r` uses this.
2. **Wrap Dinamica calls in a timeout** (`processx::process$wait(timeout = max_dinamica_secs)`) and explicitly `kill_tree()` if exceeded. Currently per CONCERNS.md the Dinamica logs are scattered; consolidating those (PIPE-07) makes timeout/kill diagnosis easier.
3. **On Windows, use Job Objects** (processx does this by default for `process$new`; verify).

**Phase:** P1 (use cleanup_tree) + P5 (PIPE-07 log consolidation). Confidence: MEDIUM (depends on processx version and exact Windows config).

---

### M8. `furrr_options(seed = TRUE)` does not forward all globals — explicit `globals = c(...)` may be needed

**What goes wrong:** `future` auto-detects globals via `globals::globalsOf()`. It misses:
- Globals referenced inside `do.call()`, `eval()`, `getFromNamespace()`, or other dynamic dispatch.
- Globals introduced by sourced files (`source("...")` inside the worker is fine; the worker re-sources, but if the parent sources something that defines a function that the worker calls indirectly, it may not be auto-shipped).
- Globals only used in error handlers or in branches not taken on the first call.

Your code uses `function(idx)` directly in `furrr::future_map` (line 707), which is the right pattern — but `setup_allocation_inputs`, `run_allocation_dinamica`, `predict_saved_transition_prob`, etc., are referenced by name and globals discovers them by walking the closure. If any of these references a function defined in a sourced file but reached only via `do.call`, the worker may fail with "could not find function 'X'".

**Warning signs:**
- `Error in checkForRemoteErrors: ...could not find function "X"` reported for a function that exists in the parent.
- `Error: GlobalsError: unrecognized symbol`.

**Prevention:**
1. **At the top of the worker function body, explicitly source the project files:** `source(file.path(config$src_dir, "allocation.r"))` etc. Forces the worker to have the same global environment as the parent. Adds a few seconds of startup; eliminates an entire class of bugs.
2. **Use `furrr_options(globals = list(setup_allocation_inputs = setup_allocation_inputs, ...))`** to be explicit. Verbose but bulletproof.
3. **Set `options(future.globals.maxSize = 2 * 1024^3)` (2GB)** to avoid `future` refusing to ship a large global. If a global is *expected* to be large, this is the toggle. If it's unexpectedly large, that itself is the bug.

**Phase:** P1. Confidence: HIGH.

---

## MINOR Pitfalls

### m1. Windows path length 260-char limit (still surfaces in 2026)

`E:/nascent-lulcc-agg/inputs/predictors_prepped/parquet_data/dynamic/2030/scenario=BAU/region=1/part-00001.parquet` — when nested in a `region_<long>/work/<scenario>/<timestep>/` Dinamica work_dir, total path can exceed MAX_PATH (260 chars) on Windows. Causes `Error: cannot open file ...` on Windows but works fine on HPC.

**Prevention:** Enable Long Path support in Windows registry (`LongPathsEnabled = 1`); use `\\?\` prefix in paths if you can't change the registry; keep region work_dir nesting shallow.

### m2. `gc(verbose = FALSE)` after every iteration is expensive

Your code calls `gc()` after every transition (line 1419). Each `gc()` is O(N) in heap size — for a 12GB R heap, this is ~1-3 seconds. Over 7 transitions × 16 timesteps × 3 regions × 4 scenarios = 1344 calls = 22-67 minutes of pure GC overhead.

**Prevention:** Call `gc()` only at region boundaries (you already do, line 797), not after every transition. The transition-level cleanup already happens via `rm()` and the next iteration overwrites the variables.

### m3. `set.seed()` does not affect parallel workers without `furrr_options(seed = TRUE)`

You use `furrr_options(seed = TRUE)` (good). But any code path that calls `set.seed()` *inside* a worker is local to that worker only. Reproducibility across runs requires reproducible worker streams.

**Prevention:** Document the seed strategy in the region log header.

### m4. `arrow::int32()` partition type vs `arrow::int8()` — schema mismatch on partition discovery

Your code uses `region = arrow::int32()` partitioning (line 1185, 1191). If a partition directory was written with int8 in a prior run, schema discovery fails with "incompatible types".

**Prevention:** Stable partition schema declared in a config file; assert at write time and at read time.

### m5. `tibble`/`data.frame` row.names attribute carries large character vector across workers

If `from_data` keeps default row.names, the row.names attribute can be a character vector of length nrow — silently doubles serialisation cost. Your code uses `data.table` (no row.names) — good. Watch for any `as.data.frame()` conversions that re-introduce them.

### m6. R 4.4 vs 4.3 `serialize()` format compatibility

R 4.4 introduced ALTREP changes that *may* affect cross-version `readRDS()` compatibility for some objects. Per STACK.md, you have R 4.3.x in most envs and 4.4.1 in `trans_rate_estimation_env`. If a model is saved in 4.4 and loaded in 4.3, behaviour is undocumented for some object types.

**Prevention:** Pin R version per pipeline stage; consolidate to one R version where feasible.

---

## Phase-Specific Warnings (Roadmap Cross-Reference)

| Phase | Likely Pitfall | Mitigation |
|-------|---------------|------------|
| **P1 Stabilise** (ALLOC-01,02,04,05) | C1 (silent OOM kill); C2 (terra non-exportable); C3 (worker count); C5 (raster missing in env); M7 (Dinamica orphan); M8 (globals not shipped) | Sequential-mode fallback; closure audit; `r-raster` defensive include; `cleanup_tree=TRUE`; explicit source in workers |
| **P2 Observability** (ALLOC-03) | C4 (RSS=NAMB); M2 (per-iteration leak detection) | Switch to `ps::ps_memory_info()`; cgroup readout; per-region RSS log line |
| **P3 Memory shrink** (PIPE-05, MLR3-01) | M1 (butcher breakage); M3 (arrow non-pushdown); M4 (data.table copies); M5 (terra cache); M6 (xgboost compat) | Predict-equality test post-butcher; verify pushdown; eliminate redundant `copy()`; cap GDAL cache; pin xgboost |
| **P4 HPC hardening** (PIPE-04) | C6 (SLURM accounting); m4 (parquet schema drift) | Explicit `--mem` + `--cpus-per-task`; `seff`/`sacct` post-mortem in `diagnose_alloc_crash.sh`; partition schema assertion |
| **P5 Pipeline correctness** (PIPE-01,02,03,06,07) | (mostly path/correctness, lower memory risk) | Already covered in CONCERNS.md fix approaches |

---

## Diagnostic Toolkit (recommended for P2)

A `scripts/diagnose_alloc_crash.sh` (and `.ps1` for Windows) should bundle:

1. **`sacct` extraction:** `sacct -j $JOBID --format=JobID,JobName,State,ExitCode,ReqMem,MaxRSS,MaxVMSize,AveRSS,Elapsed,NodeList --units=G`
2. **`seff` summary:** `seff $JOBID`
3. **Per-worker log tail:** find all `worker_*.log` under the most-recent timestep dir, print last 30 lines of each.
4. **`dmesg` OOM grep** (if user has access; otherwise warn).
5. **R session info from a one-shot R call** that loads the same env: `Rscript -e 'sessionInfo()'`.
6. **Disk space check** on scratch (terra temp dir).
7. **Cgroup memory current** (Linux): `cat /sys/fs/cgroup/memory.current 2>/dev/null || cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null`.

This makes "the allocation crashed" → "here's the post-mortem in 30 seconds" instead of an hour of forensics.

---

## Sources

- `c:/Users/black/switchdrive/git/nascent-lulcc/.planning/PROJECT.md` (crash profile, blockers, decision log)
- `c:/Users/black/switchdrive/git/nascent-lulcc/.planning/codebase/CONCERNS.md` (raster/terra coexistence, hardcoded paths, xgboost pinning)
- `c:/Users/black/switchdrive/git/nascent-lulcc/.planning/codebase/STACK.md` (package versions, R version split, conda env layout)
- `c:/Users/black/switchdrive/git/nascent-lulcc/src/allocation.r` (closure structure, `prof_tic`/`prof_toc`, `nhood_raster_cache`, `predict_saved_*` paths, `furrr::future_map` invocation)

**Live web search and Context7 MCP were unavailable during this research session** (WebSearch permission denied; gsd-sdk Bash blocked). Findings are derived from training-data knowledge of these specific R packages combined with the project-specific evidence above. Items marked LOW or MEDIUM confidence (notably the exact xgboost-tidypredict compatibility version, the precise cgroup version on Euler, and the dominant native-heap leak source) should be verified empirically before being treated as load-bearing.

**Recommended verification before P3:**
- `?future::nbrOfWorkers` and `?future::plan` — confirm `multicore` semantics on the Euler R version
- `terra` NEWS file — confirm `wrap`/`unwrap` API stability for current installed version
- `butcher::axe_*` documentation — confirm which axes preserve `predict()` for ranger and xgboost
- xgboost CHANGELOG — confirm 1.7 → 2.x serialisation compatibility status
- `arrow::open_dataset` predicate-pushdown docs for the installed arrow version
