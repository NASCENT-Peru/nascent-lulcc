# Phase 3: Parallelism & Memory Architecture — Research

**Researched:** 2026-05-11
**Domain:** R parallelism on Linux HPC (SLURM/cgroup) — `future::multicore` fork + COW + native-thread pinning + path-based raster sharing
**Confidence:** HIGH (codebase analysis; future/parallelly/terra docs cross-verified). MEDIUM on exact mlr3 marshal behaviour for xgboost (Phase 2 noted classif.xgboost in mlr3learners 0.14+ does not declare the "marshal" property — accepted from 02-RESEARCH §A4).

---

## Summary

The fix is a short, ordered checklist of small changes inside `src/allocation.r` and `scripts/run_allocation.r`. Nothing structural about the algorithm changes; what changes is the *shape* of the parent/worker contract.

- **Switch to fork-based `multicore` on Linux automatically** via `parallelly::supportsMulticore()` — fall back to `multisession` on Windows. The single line `future::plan(future::multisession, workers = num_workers)` at `scripts/run_allocation.r:220` becomes a `select_allocation_plan()` helper that picks the right strategy with `parallelly::availableCores()`-derived workers (SLURM-aware, reads `SLURM_CPUS_PER_TASK` automatically). [CITED: https://parallelly.futureverse.org/reference/supportsMulticore.html]
- **Move per-region work into the parent's pre-fork phase.** Today the parent loads NOTHING and each worker does the full per-region setup, including `terra::rast()` of region/anterior/current LULC, building `anterior_dt`, opening parquet datasets, and (worst of all) repeatedly loading models inside the per-transition loop. The plan turns the parallel unit inside-out: parent loads `models_list` (~140 mlr3 .qs objects ≈ ~5–15 GB after Phase 2 with `save.memory=TRUE`), pre-computes all neighbourhood rasters to scratch TIFs, builds the `nhood_paths` character vector, and only then forks. Workers receive a per-region work list and character paths.
- **Eliminate all SpatRaster captures in worker closures.** `region_rast`, `current_lulc`, `lulc_region`, `anterior`, `ref_grid`, and the cached SpatRasters in `nhood_raster_cache` all carry `externalptr` payloads and will trip `options(future.globals.onReference = "error")`. The only thing crossing the fork boundary must be character file paths and small R data.frames / lists.
- **Pin every native thread to 1 BEFORE `future::plan()`.** Order: env vars (`OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS`, `MKL_NUM_THREADS`, `GDAL_NUM_THREADS`) → `RhpcBLASctl::blas_set_num_threads(1)` and `omp_set_num_threads(1)` → `data.table::setDTthreads(1)` → `arrow::set_cpu_count(1)`. xgboost and ranger are already pinned per-learner inside `build_mlr3_learner()` (`nthread = 1L`, `num.threads = 1L`). [VERIFIED: codebase, src/transition_modelling.r:146,182]
- **Verify with `options(future.globals.onReference = "error")` in a dev run, then leave the option off in production** (it has measurable scan overhead per future). Add a one-line `ALLOCATION_DEV_STRICT_GLOBALS` env var so the dev gate is reproducible. [CITED: https://future.futureverse.org/articles/future-4-non-exportable-objects.html — "set to 'ignore' by default due to the extra overhead 'error' introduces"]

**Primary recommendation:** Implement an `init_allocation_runtime()` helper in `src/allocation.r` that does the thread-pinning, plan-selection, and pre-flight in one ordered block; call it from `scripts/run_allocation.r` AFTER pre-flight and BEFORE the scenario loop. Pre-load `models_list` and pre-compute nhood TIFs at the region-init boundary inside `run_allocation_one_timestep()`, then refactor the worker body to consume `models_list[[j]]` and `nhood_paths[pred_name]` as inherited globals (COW under multicore, fall-back via PSOCK serialisation on Windows).

---

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| MEM-01 | allocation.r completes without OOM crash for all scenarios × regions × timesteps on HPC | §Smoke-run definition (Q10); §Memory budget; §Plan selector (multicore on Linux) |
| MEM-02 | Memory footprint per worker bounded — models, predictor rasters, nhood rasters not duplicated across workers | §Pre-load `models_list` in parent; §Path-based raster passing pattern; §Fork-safety analysis |
| MEM-03 | `future::multicore` (fork+COW) on Linux HPC; `future::multisession` on Windows local; backend selected automatically based on OS | §future plan selector design (Q1); `select_allocation_plan()` |
| MEM-05 | Neighbourhood rasters pre-computed once per region in parent and written to scratch as TIF files; workers receive file paths, not in-memory objects | §Path-based raster passing pattern; §Current `nhood_raster_cache` is per-worker — must move to parent (Q3) |

---

## Project Constraints (no CLAUDE.md present)

No `./CLAUDE.md` exists at the repository root and no `.claude/skills/` or `.agents/skills/` directories exist. Project-wide guidance comes from:

- `.planning/PROJECT.md` — constraints, key decisions, current failure profile
- `.planning/STATE.md` — current position; the carry-forward decision *"Init: Linux HPC switches to `future::multicore`; Windows local stays on `future::multisession`"* and *"Init: Pre-compute neighbourhood rasters in parent and pass file paths to workers (not SpatRaster objects)"* lock the architectural choices for Phase 3.
- `.planning/REQUIREMENTS.md` §MEM-01..MEM-05 — the four falsifiable success criteria.
- `.planning/phases/01-repair-visibility/01-CONTEXT.md` D-12 — *"Centralize environment/path resolution behind shared helpers and config lookups"*; D-15 — *"Require explicit HPC temp/scratch env vars and fail pre-flight clearly if they are missing."* The Phase 3 plan must extend `validate_allocation_runtime()` for the new pre-flight items (parallelly, RhpcBLASctl already listed; check fork policy at runtime).
- `.planning/phases/02-model-size-reduction/02-CONTEXT.md` D-04, D-05 — model_type="mlr3" with `qs::qsave()` of a `list(model_type, predictor_names, response_levels, learner)`; the predict dispatcher (`src/allocation.r:540`) already handles this. The Phase 2 research §A4 noted that **mlr3learners 0.14+ classif.xgboost does NOT declare the "marshal" property** — so qs round-trip of the saved learner is expected to be predict-safe without an explicit `$marshal()`/`$unmarshal()` step. Phase 3 should sanity-check this on the first scenario × region by including the existing 5-row predict gate or an equivalent before forking.

---

## Architectural Responsibility Map

This is the only multi-tier capability set in scope. The system is a single-node SLURM job; there is no browser/CDN/database tier.

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|--------------|----------------|-----------|
| Plan selection (multicore/multisession/sequential) | R parent process (parallelly + future) | OS (kernel fork support) | `parallelly::supportsMulticore()` is the right contract: it inspects OS, RStudio detection, and `parallelly.fork.enable` together. [CITED: parallelly docs] |
| Native thread pinning (BLAS/OMP/data.table/arrow) | R parent process (env vars + library calls) | Env vars inherited into forks | Set before `future::plan()` so forked children inherit pinned state. [CITED: RhpcBLASctl docs — "parallel library retains BLAS settings of the parent (presumably since it uses fork())"] |
| Read-only objects (`models_list`, focal matrices) | R parent process (heap, fork-shared via COW) | — | Loaded once in parent; children read via COW. Mutation triggers page copy → forbidden in worker. |
| Read-only file-backed rasters (anterior, region, ref_grid, nhood TIFs) | Scratch filesystem (TIF files) | R parent (path strings) / R workers (terra::rast()) | SpatRaster external pointers cannot cross fork or socket boundary safely; workers re-open via path. [CITED: future non-exportable-objects vignette + terra issue #96] |
| Per-transition predict | R worker process (forked or PSOCK) | — | Independent; outputs go to a per-transition TIF in `probability_map_dir`. |
| Dinamica subprocess | R parent process (processx::run, sequential) | OS process tree | Already sequential; out of Phase 3 scope. |
| Worker logs and sentinels | Per-process file handle (Phase 1 worker_state) | Per-region log path | Each forked child has its own pid → log filename is keyed by `Sys.getpid()` (worker `Sys.getpid()` is different from parent under fork). Phase 1 already implemented this at `src/utils.r:1034`. |
| OOM detection | SLURM cgroup + slurmstepd | R parent (post-mortem via `diagnose_alloc_crash.sh`) | cgroup OOM-killer sends SIGKILL (uncatchable); parent observes broken socket / dead PID. Forks receive SIGKILL from the cgroup, not SIGTERM. [CITED: slurm-users discussion] |

---

## Current State of `allocation.r`

This is the concrete situation Phase 3 inherits. File:line citations are exact.

### Parallelism is established by `scripts/run_allocation.r`, not `allocation.r`

`scripts/run_allocation.r:218-220`:

```r
num_workers <- as.integer(Sys.getenv("ALLOCATION_NUM_WORKERS", unset = "4"))
cat(sprintf("Setting up parallel processing with %d workers\n", num_workers))
future::plan(future::multisession, workers = num_workers)
```

`scripts/submit_allocation.sh:48` sets `ALLOCATION_NUM_WORKERS=${SLURM_CPUS_PER_TASK:-4}`. There is **no** `parallelly::availableCores()` call anywhere; SLURM detection is by shell-side env var. There is **no** thread-pinning before `future::plan()`. There is **no** OS check.

The pre-flight (`validate_allocation_runtime()`, `src/allocation.r:221-328`) DOES list `RhpcBLASctl` as a required package (line 247), so the thread-pin call itself is unblocked.

### Parallel unit is REGION (not transition)

`src/allocation.r:907-1039`:

```r
posterior_paths <- furrr::future_map(
  seq_along(region_names),
  function(idx) {
    # ... region_label, region_val ...
    region_rast <- terra::rast(region_rast_path)         # line 952 — INSIDE worker
    current_lulc <- terra::rast(current_lulc_path)       # line 955 — INSIDE worker
    lulc_region <- terra::mask(current_lulc, region_rast, ...)
    terra::writeRaster(lulc_region, anterior_path, ...)  # writes anterior TIF per region
    setup_allocation_inputs(...)                          # calls generate_probability_maps()
    posterior_path <- run_allocation_dinamica(...)
    ...
  },
  .options = furrr::furrr_options(seed = TRUE)
)
```

This is correct given the *current* design (regions are independent; per-region setup is inside the closure). For the Phase 3 multicore switch, this region-level parallelism is the right granularity — Andes/Amazon/Coast are 3 regions → 3 forks, well below SLURM_CPUS_PER_TASK=8.

What's expensive inside one region is `generate_probability_maps()` (transitions are sequential within the region). Phase 3 should NOT rewrite the within-region loop to be parallel; that is Phase 4 (block-wise predict). Phase 3 just needs the region-level parallelism to stop OOM-killing under multicore semantics with COW-shared `models_list`.

### `models_list` is loaded inside `generate_probability_maps()` ONE MODEL AT A TIME inside the loop

`src/allocation.r:1466-1545`:

```r
for (j in seq_len(nrow(model_info))) {
  ...
  t_model_load <- prof_tic()
  fitted_wf <- if (grepl("\\.qs$", mi$file_path, perl = TRUE)) {
    qs::qread(mi$file_path)
  } else {
    readRDS(mi$file_path)
  }
  ...
  rm(fitted_wf, pred_result, from_data, pred_data)
  gc(verbose = FALSE)
  prof_toc(t_trans_total, trans_tag, log_file)
}
```

Critical implication: there is **no `models_list` to pre-load in the parent**. The current design deliberately loads one model at a time and `rm()`s it before the next iteration so peak memory per worker is bounded by one model's size. With Phase 2's mlr3+save.memory=TRUE that's ~50–200 MB resident at any moment — fine for multicore.

So the success-criterion 3 wording *"`models_list` is loaded exactly once in the parent"* is interpretable two ways:

1. **Strict interpretation**: parent eagerly `qs::qread`s every model into a list before forking; workers index into that list. Pros: COW-shared, zero per-worker re-read I/O. Cons: parent RSS becomes ~140 models × ~50–200 MB ≈ 7–28 GB just for models. On Euler with `--cpus-per-task=8 --mem-per-cpu=8G` that's 64 GB total job memory, so parent baseline ≤ 28 GB fits — but it's tight. Also: pre-loading all regions' models when only 3 forks each touch 1 region's slice is wasteful.

2. **Per-region preload**: parent loads models for ONE region into `models_list` just before forking (inside `run_allocation_one_timestep()`), passes the list into the worker closure. With 3 forks at once each fork sees the parent's list (the other 2 regions' models are not yet loaded), but COW only saves memory if all 3 children read the same list — they don't, because each region uses a disjoint subset of models (filenames pattern `_<region_suffix>.rds`). So the COW savings vanish per-region; we'd be back to per-worker copy via fork's eager allocation.

3. **Sequential regions + parallel transitions within region** (the Phase 0 SUMMARY.md preferred design): parent loads region R's models, then forks across transitions, then collects, then moves to region R+1. Pros: COW shares one region's models across N transition forks. Cons: requires inverting the parallel-unit choice; transitions today are sequential at `src/allocation.r:1469`.

**Recommendation for Phase 3**: Keep transitions sequential within a region (don't bleed into Phase 4). For region-level parallelism, the right move is option (1) — pre-load **all** models in the parent before any fork. With Phase 2 size targets (<200 MB per model, typical 50–80 MB) and ~140 models, parent ≈ 7–11 GB. That fits HPC budgets and gives COW savings across the 3 region forks. Document the parent-baseline RSS assumption (see Q5) and let the planner decide whether to defer per-region preload to Phase 4.

### Neighbourhood rasters are cached per-worker

`src/allocation.r:1427-1454`:

```r
nhood_raster_cache <- new.env(parent = emptyenv())
focal_matrices <- NULL
get_nhood_raster <- function(pred_name) {
  if (!exists(pred_name, envir = nhood_raster_cache, inherits = FALSE)) {
    if (is.null(focal_matrices)) {
      fm <- readRDS(file.path(config[["preds_tools_dir"]], "neighbourhood_matrices", "all_matrices.rds"))
      ...
      focal_matrices <<- fm
    }
    rast <- compute_single_nhood_raster(
      anterior = anterior, pred_name = pred_name,
      focal_matrices = focal_matrices, class_name_to_value = class_name_to_value
    )
    assign(pred_name, rast, envir = nhood_raster_cache)
  }
  get(pred_name, envir = nhood_raster_cache)
}
```

This cache is created per-call of `generate_probability_maps()`, i.e. once per region per worker. Under the current multisession design that means 3 workers each compute their own nhoods. Under multicore, the same closure runs in 3 forked children; since the cache env is created inside the worker's closure (not in the parent), there is still no sharing. The right pattern is: parent pre-computes all needed nhood TIFs to scratch, then each worker calls `terra::rast(path)` for whatever predictors it needs.

`compute_single_nhood_raster()` at `src/allocation.r:1759-1798` returns an in-memory SpatRaster (no `filename=` argument to `terra::focal`). Phase 3 must change this so the parent writes the result to a TIF and passes the path; workers receive paths.

### Closures capture multiple SpatRasters today

Audit of the worker function at `src/allocation.r:909` — the closure body uses these objects that are SpatRasters or hold SpatRaster references:

- `region_rast` (line 952) — created inside the worker. SAFE under fork, BUT created on each fork — wastes one terra::rast() call per region.
- `current_lulc` (line 955) — same.
- `lulc_region` (line 958) — derived in worker. SAFE.
- `anterior` (line 1363, inside `generate_probability_maps`) — created in worker. SAFE under fork.
- `ref_grid` (line 1390) — created in worker. SAFE under fork.
- `nhood_raster_cache` env (line 1427) — created in worker. The SpatRasters it holds are all in-memory (no `filename=`). These never cross a fork boundary because the cache is rebuilt per worker.
- `ds_static`, `ds_dynamic` arrow datasets (lines 1409, 1414) — Arrow R6 wrappers around C++ Datasets. These ARE flagged as non-exportable references by `future.globals.onReference="error"` if captured from outside the closure. Today they are created inside the worker → safe.

**The hidden risk:** if anyone refactors and moves any of these creations to the parent ("hoists" the work to avoid recomputation), `future.globals.onReference="error"` will fire. The dev gate in success-criterion 4 is exactly the right protection.

### `config` object — is it fork-safe?

`config` is a plain R list loaded by `get_config()` (`src/setup.r`) from YAML + env-var overrides. It is captured by the worker closure (multiple references at `:986, :1218, etc.`). Plain R lists with strings/numerics/named children serialise fine over both PSOCK and fork. No external pointers expected. Should be safe; will be confirmed by the dev gate. [VERIFIED: no `externalptr` types appear in config keys per grep audit]

### RSS profiling is already worker-aware

`src/allocation.r:42-190` (Phase 1 deliverable). The `prof_toc()` PROFILE line uses `ps::ps_memory_info()` which is per-process. Under multicore each fork has its own PID → each prof_toc emits a separate PROFILE line. The parent's pre-fork RSS is logged via `prof_mem_summary()` at the region boundary. Phase 3 should add a `MEM_LIMIT region=… limit=…MB` log line at worker entry (from `/sys/fs/cgroup/memory.max` or env var) so per-worker budget is verifiable in the success criterion — see §Validation Architecture.

---

## Fork-safety analysis

`options(future.globals.onReference = "error")` walks every reachable object in the closure environment of the future expression and refuses to dispatch if it finds any `externalptr`, `weakref`, or `environment` that references unexportable state. The mechanism is conservative (some flagged objects are actually exportable — data.table is on the false-positive list per the future docs).

[CITED: https://future.futureverse.org/articles/future-4-non-exportable-objects.html — "scan for external pointers before launching the future on a parallel worker, and throw an error if one is detected"]

Below is every potential reference in the Phase 3 worker closures, classified:

| Object | Where created today | Phase 3 plan | Risk |
|--------|---------------------|--------------|------|
| `region_rast`, `current_lulc`, `lulc_region`, `anterior`, `ref_grid` (SpatRaster) | Inside worker (allocation.r:952, 955, 958, 1363, 1390) | **Keep inside worker.** Fork allows it because each child opens fresh; multisession serialises fine because terra now exports the file path via wrap on serialise. Either way, the closure does NOT capture a parent SpatRaster. | LOW. The dev gate catches any future regression that hoists these. |
| `nhood_raster_cache` env + cached SpatRasters | Inside worker (allocation.r:1427) | **Move TIF write to parent.** The env-of-SpatRasters disappears; workers receive `nhood_paths` (named character vector) and call `terra::rast(path)`. | LOW once the change is made. HIGH if planner overlooks the in-memory cache. |
| `models_list` (list of mlr3 model objects from qs::qread) | TODAY: read one-at-a-time inside worker (allocation.r:1531-1535). PHASE 3 OPTION 1: pre-loaded by parent before any fork; passed via closure capture or as explicit argument. | Each element is `list(model_type, predictor_names, response_levels, learner)`. The mlr3 `learner` for ranger may hold a `ranger$forest` R structure (no externalptr after save.memory=TRUE — verified by Phase 2). The xgboost learner contains an xgb.Booster pointer wrapped in mlr3learners 0.14+ as `structure("wrapper", model = model)` which **does serialise across qs without "marshal" property** per Phase 2 RESEARCH §A4. **`future.globals.onReference="error"` may still flag the wrapper as it contains an `externalptr`** — this is the most uncertain risk in Phase 3. | MEDIUM. The mitigation: try a single-region dev run with the option set; if it errors on xgboost wrappers, either (a) call `$marshal()` on the learner before fork and `$unmarshal()` in workers, or (b) keep models loaded per-region per-worker via `qs::qread()` (current design) and rely on disk page-cache sharing across forks for repeated reads. [CITED: mlr3 marshaling docs — `mlr3.mlr-org.com/reference/marshaling.html`] |
| `focal_matrices` (list of integer matrices from RDS) | TODAY: lazy-load inside worker (allocation.r:1432). PHASE 3: pre-load in parent. | Plain R objects — no externalptr. SAFE. | LOW. |
| `class_name_to_value` (named integer vector) | Inside `generate_probability_maps` (allocation.r:1290) | Pre-build in parent before fork. | LOW. Plain R. |
| `anterior_dt` (data.table with sparse cell index) | Inside worker (allocation.r:1367) | **Keep inside worker.** It's built from `anterior` which is per-region — different in every fork. data.table objects have an `.internal.selfref` external pointer; future flags it but it's a documented FALSE POSITIVE for data.table (see future-4-non-exportable-objects vignette). | LOW. May need `future.globals.onReference="ignore"` for data.table specifically, OR rebuild inside worker (current plan does this). |
| `ds_static`, `ds_dynamic` (arrow::Dataset R6 wrappers) | Inside worker (allocation.r:1409, 1414) | **Keep inside worker.** Arrow R6 wrappers around C++ Datasets hold externalptr; if hoisted to parent, future scanner WILL flag and reject. | LOW. Don't hoist. |
| `config` (named list from YAML + env) | Loaded once in `run_allocation()`, captured by every closure | Already shared. Plain R — no externalptr. | LOW. |
| log_file (character path) | Initialised per worker (allocation.r:919) | Already per-pid. | LOW. |
| File connections (e.g. opened by `log_msg`) | `log_msg` uses `cat(..., file = log_file, append = TRUE)` — opens and closes per call (utils.r:1019-1023) | No persistent file handle in the closure. SAFE. | LOW. Phase 1 design avoided long-lived file connections deliberately. |

**Summary of fork-safety conclusion:** With the cache and nhood-write moved to the parent, and `models_list` either pre-loaded or kept per-worker, the only remaining externalptr risk is xgboost wrappers inside the saved mlr3 learner. The dev-mode strict-globals gate (Q9) is the right way to detect this — set the option once on a dev run, observe what fires, address each finding before HPC rollout.

---

## `future` plan selector design (Q1)

### Function shape

```r
# src/allocation.r — new helper, called from scripts/run_allocation.r BEFORE
# future::plan() and AFTER pre-flight.
#
# Returns the named selector and number of workers actually used so the parent
# logs both. Does NOT call future::plan() itself; the caller does, so unit
# tests can dry-run.
#
# Decision tree:
#   1. If R_PARALLELLY_FORK_ENABLE is "false" -> sequential
#   2. Else if parallelly::supportsMulticore() returns TRUE -> multicore
#   3. Else -> multisession (Windows, RStudio, or fork-disabled Linux)
#
# Worker count source priority:
#   1. ALLOCATION_NUM_WORKERS (explicit operator override, today's
#      behaviour — keep for backward compatibility)
#   2. parallelly::availableCores(constraints = NULL)  — SLURM-aware
#   3. parallel::detectCores() - 1 (last resort)
select_allocation_plan <- function() {
  # ALLOCATION_NUM_WORKERS keeps today's contract working
  override <- suppressWarnings(as.integer(
    Sys.getenv("ALLOCATION_NUM_WORKERS", unset = NA_character_)
  ))
  workers <- if (!is.na(override) && override > 0L) {
    override
  } else if (requireNamespace("parallelly", quietly = TRUE)) {
    parallelly::availableCores()  # respects SLURM_CPUS_PER_TASK
  } else {
    max(1L, parallel::detectCores() - 1L)
  }

  # Force-sequential override (operator opt-out for diagnostic runs)
  if (identical(tolower(Sys.getenv("ALLOCATION_PARALLEL_STRATEGY", "")), "sequential")) {
    return(list(strategy = "sequential", workers = 1L))
  }

  # Explicit operator override for either backend (last-resort, dev/test)
  forced <- tolower(Sys.getenv("ALLOCATION_PARALLEL_STRATEGY", ""))
  if (forced %in% c("multicore", "multisession")) {
    return(list(strategy = forced, workers = workers))
  }

  # The automatic decision
  if (requireNamespace("parallelly", quietly = TRUE) &&
        parallelly::supportsMulticore()) {
    list(strategy = "multicore", workers = workers)
  } else {
    list(strategy = "multisession", workers = workers)
  }
}
```

### Why `parallelly::supportsMulticore()` and not `.Platform$OS.type`

- `supportsMulticore()` returns FALSE on Windows AND on RStudio (per parallelly docs) AND when `parallelly.fork.enable=FALSE`. It's the single source of truth the future ecosystem itself uses.
- `.Platform$OS.type == "unix"` returns TRUE inside RStudio on macOS — which is exactly where multicore is unsafe.
- [CITED: https://parallelly.futureverse.org/reference/supportsMulticore.html — "When the parallelly.fork.enable option is NA or not set (the default), a set of best-practices rules decide whether multicore should be supported or not."]

### Worker count source — why `parallelly::availableCores()`

- It reads `SLURM_CPUS_PER_TASK` automatically with no shell-side plumbing. [CITED: parallelly availableCores docs]
- Today `submit_allocation.sh` already exports `ALLOCATION_NUM_WORKERS=${SLURM_CPUS_PER_TASK:-4}`; that env var should remain as the operator override path (priority 1), while `availableCores()` becomes the default when the var is unset. This makes the env-var optional rather than mandatory.

### SLURM-specific gotchas

1. **Single-node only.** multicore forks within one process tree on one node. The submit script (`scripts/submit_allocation.sh:4-5`) uses `--cpus-per-task=8 --mem-per-cpu=8G` — single task, multiple CPUs. Good. Phase 3 must not change this to `--ntasks=N`.
2. **`R_PARALLELLY_FORK_ENABLE` defaults to true on Linux.** Some HPC site startup scripts may export `=false` to mitigate "fork inside MPI ranks" — we should explicitly `unset` it (or set to true) inside `select_allocation_plan()` to be deterministic. The HPC docs are mixed; the safest move is to log the observed value at startup so the operator can see if anything overrode it.
3. **RStudio Server detection.** parallelly disables multicore inside any RStudio. This catches both desktop and Server. If a maintainer ever runs an interactive R session via RStudio Server on Euler login nodes (rare but possible), the selector correctly falls back to multisession. No special handling needed.
4. **Singularity/Apptainer containers don't affect fork.** Whether or not Dinamica EGO runs inside a container is orthogonal — the R parent is on the host, forks happen on the host. (The Dinamica subprocess is sequential and exec'd via processx; it can be in a container without affecting R parallelism.)

### Where to call from

`scripts/run_allocation.r` after line 216 (pre-flight passes), and BEFORE today's line 220 future::plan() call. Replace:

```r
num_workers <- as.integer(Sys.getenv("ALLOCATION_NUM_WORKERS", unset = "4"))
future::plan(future::multisession, workers = num_workers)
```

with:

```r
pin_native_threads_to_one(verbose = TRUE)   # see §thread pinning
plan_choice <- select_allocation_plan()
cat(sprintf("Parallel: strategy=%s workers=%d\n",
            plan_choice$strategy, plan_choice$workers))

if (plan_choice$strategy == "sequential") {
  future::plan(future::sequential)
} else if (plan_choice$strategy == "multicore") {
  options(parallelly.fork.enable = TRUE)  # explicit, never relies on default
  future::plan(future::multicore, workers = plan_choice$workers)
} else {
  future::plan(future::multisession, workers = plan_choice$workers)
}
```

Also update `src/calibrate_allocation_parameters.r:780-820` (the file with the inverted comment "multicore causes OOM") — the same selector should replace that block to avoid future drift. Phase 3 scope decision: rewrite or leave as is? Recommend rewrite to keep behaviour consistent; calibrate runs the same predict path. The planner should decide based on phase-scope discipline.

---

## Path-based raster passing pattern (Q3)

### Where TIFs are materialised

Use `Sys.getenv("TERRA_TEMP", unset = tempdir())` as the canonical scratch root — Phase 1 established this. On HPC it points to `$HPC_SCRATCH_ROOT/terra_temp`; on local it falls back to R's `tempdir()`. A per-region nhood subdirectory keeps things scoped:

```r
nhood_cache_dir <- file.path(
  Sys.getenv("TERRA_TEMP", unset = tempdir()),
  "nhood_cache",
  paste0(scenario, "_", year_post, "_", region_suffix)
)
ensure_dir(nhood_cache_dir)
```

Evict at end of region (after `exec_dinamica()` returns) since nhoods depend only on `anterior` which is region-scoped:

```r
on.exit(unlink(nhood_cache_dir, recursive = TRUE), add = TRUE)
```

### Parent-side write

Discover which nhood predictors are actually needed by ANY model for this region, then pre-compute each one ONCE:

```r
# Parent, inside run_allocation_one_timestep BEFORE future_map
all_nhood_needed <- unique(unlist(lapply(models_list_for_region, function(m) {
  preds <- get_saved_transition_predictors(m)
  grep("_nhood_", preds, value = TRUE)
})))

focal_matrices <- load_focal_matrices(config)  # one read, parent only
nhood_paths <- vapply(all_nhood_needed, function(pred_name) {
  out_path <- file.path(nhood_cache_dir, paste0(pred_name, ".tif"))
  if (!file.exists(out_path)) {
    rast <- compute_single_nhood_raster(
      anterior = terra::rast(anterior_path),  # local re-open; parent doesn't hold it
      pred_name = pred_name,
      focal_matrices = focal_matrices,
      class_name_to_value = class_name_to_value
    )
    terra::writeRaster(
      rast, out_path, overwrite = TRUE,
      datatype = "FLT4S",
      gdal = c("COMPRESS=LZW", "TILED=YES",
               "BLOCKXSIZE=256", "BLOCKYSIZE=256",
               "BIGTIFF=IF_SAFER", "NUM_THREADS=1")
    )
    rm(rast)
  }
  out_path
}, character(1), USE.NAMES = TRUE)
# nhood_paths is a NAMED character vector: pred_name -> tif path
```

GDAL options justified:
- `COMPRESS=LZW` — default in terra; LZW performs well per GDAL docs. [CITED: https://gdal.org/en/stable/drivers/raster/gtiff.html]
- `TILED=YES + BLOCKXSIZE=256 BLOCKYSIZE=256` — enables windowed reads from workers (terra::extract on points reads only relevant tiles).
- `BIGTIFF=IF_SAFER` — guards against >4 GB single files; small overhead when not needed. [CITED: GDAL docs — "The default TIFF format only allows for files 4 GB or smaller; GDAL overcomes this barrier with the BigTIFF creation option."]
- `NUM_THREADS=1` — paired with the global thread-pin policy so terra's GDAL doesn't spawn compression threads. [CITED: GDAL — "NUM_THREADS defaults to 1"]

### Worker-side read

```r
# Inside the future_map worker, after fork:
nhood_paths_needed <- nhood_paths[nhood_needed]   # named char subset
nhood_stack <- terra::rast(nhood_paths_needed)   # opens lazily; ~KB per layer
nhood_vals <- terra::extract(
  nhood_stack,
  as.matrix(from_data[, .(x, y)])
)
```

`terra::rast(<character vector>)` opens each file lazily via GDAL; per-worker resident cost is the GDAL tile cache (a few MB at most when only sparse from-class cells are read). The same TIFs are page-cached by Linux across the 3 forks → effectively shared physical memory.

### What goes wrong if the parent holds a `terra::rast()` already

If the parent constructs a SpatRaster (e.g. holds `anterior` in scope when the fork happens), three failure modes:

1. **Under multisession (PSOCK):** future's globals scanner calls `getGlobalsAndPackages()` on the closure. If `anterior` is reachable in the closure, future serialises it. terra now implements `serialize()` to write the SpatRaster's source path (`terra::sources(rast)$source` — see [terra reference](https://rdrr.io/cran/terra/man/serialize.html)) into the payload, so the worker re-opens via `terra::rast(source)`. This *works* but causes per-worker re-opens (no sharing of the GDAL handle) plus the serialisation step adds latency. With many workers and many futures, this is the documented memory amplifier described in `.planning/research/PITFALLS.md` §C2.
2. **Under multicore (fork):** the parent's SpatRaster's external pointer is inherited valid into the child (COW). BUT — `future` still runs its globals-scanner first, and with `future.globals.onReference="error"` the closure is REJECTED before fork even happens. Without strict mode, the child does inherit a valid pointer and shares the GDAL state with the parent until either mutates → COW page copy. In practice terra's GDAL handles are mostly read-only after open, so this is OK — but the dev gate's purpose is to catch refactors that put a write into the worker.
3. **In-memory SpatRasters (no source file):** even worse. `terra::sources(rast)$source` is `""`, so serialisation produces a broken handle. Workers crash or silently read garbage.

Rule of thumb: every SpatRaster the parent constructs must EITHER be discarded with `rm()` before fork OR written to disk and passed as a path. **Do not hold any SpatRaster in scope when entering `future::plan()`.**

### What about the parent-side preflight that uses terra (e.g. `terra::rast(region_rast_path)` for extent checks)?

Wrap in a `local({...})` block so the SpatRaster goes out of scope before fork. Or be explicit with `rm()` + `gc()` before `future_map()`. This is a discipline the planner should call out in plan acceptance criteria.

---

## One-time `models_list` load (Q4)

### Pre-load pattern for mlr3 `.qs` artefacts

```r
# Parent, before any future::plan() / future_map()
load_all_region_models <- function(region_suffix, calibration_period, config, log_file) {
  model_dir <- file.path(config[["transition_model_dir"]], calibration_period)
  model_files <- list.files(
    model_dir,
    pattern = sprintf(".*_%s\\.qs$", region_suffix),
    full.names = TRUE
  )
  if (length(model_files) == 0L) {
    stop(sprintf("No mlr3 .qs models found for region %s in %s",
                 region_suffix, model_dir))
  }
  log_msg(sprintf("Loading %d mlr3 models for region %s...",
                  length(model_files), region_suffix), log_file)
  t0 <- prof_tic()
  models_list <- setNames(
    lapply(model_files, qs::qread),
    sub("\\.qs$", "", basename(model_files))
  )
  prof_toc(t0, sprintf("region=%s stage=preload_models", region_suffix), log_file)
  models_list
}
```

### Where to put the call

Two viable shapes — the planner should pick one:

**Shape A: Pre-load all regions' models once per scenario × timestep, in `run_allocation_one_timestep()` (src/allocation.r:888) before `future_map`.**

```r
# scenario × timestep boundary, parent only
models_by_region <- setNames(
  lapply(region_suffixes, load_all_region_models, calibration_period, config, log_file_parent),
  region_suffixes
)
# All 3 regions' models in memory in the parent. ~5–15 GB total.
posterior_paths <- furrr::future_map(seq_along(region_names), function(idx) {
  region_suffix <- ...
  models_list <- models_by_region[[region_suffix]]   # COW-inherited
  ...
}, .options = furrr::furrr_options(seed = TRUE))
```

This is the strict reading of success criterion 3 ("loaded exactly once in the parent"). Phase 2 size targets make it feasible.

**Shape B: Each worker `qs::qread`s its own region's models, but the parent ensures `qs::qread` happens once per model file per region via a side-table.** Complicates the design; no COW benefit because forks read disjoint slices. Reject.

**Shape A is the recommended pattern.** Memory cost in parent: 140 models × ~80 MB average = ~11 GB. On Euler with `--cpus-per-task=8 --mem-per-cpu=8G = 64 GB total job memory`, parent baseline at 11 GB leaves ~53 GB for 3 forks → ~17 GB per fork is plenty. Phase 2 success criterion 1 (<200 MB per model) is what makes this affordable; without Phase 2, this would have been 140 × 1 GB = unmanageable.

### Fork-safety of pre-loaded mlr3 learners

Each list element is `list(model_type, predictor_names, response_levels, learner)`. The `learner` is an mlr3 `Learner` R6 object whose `$model` slot contains the trained model:

- **classif.ranger with save.memory=TRUE, importance="none"** (per Phase 2 `build_mlr3_learner` at `src/transition_modelling.r:142-147`): the `$model$forest` is a pure-R nested list (split varIDs, split values, child nodeIDs). No external pointers. Fork-safe and qs-roundtrip-safe. [VERIFIED: ranger docs + Phase 2 02-RESEARCH §A1]
- **classif.glmnet** (`src/transition_modelling.r:117-135`): pure R numeric vectors. Fork-safe. [VERIFIED: standard glmnet behaviour]
- **classif.xgboost** (`src/transition_modelling.r:172-200`): the trained model contains an `xgb.Booster` C++ handle. mlr3learners 0.14+ wraps this as `structure("wrapper", model = model)` which **does NOT declare the "marshal" property** per Phase 2 02-RESEARCH §A4 — implying qs-roundtrip preserves predict capability. **However, `future.globals.onReference="error"` may still flag the externalptr embedded in the wrapper.** The Phase 2 5-row predict-check (`src/transition_modelling.r:367-384`) already validates that `qs::qread` followed by `$predict_newdata()` works for each saved model. **What is NOT yet validated:** whether the globals-scanner accepts a list of such learners as a closure capture.

**Recommendation:** include a dev-run task in the Phase 3 plan that:
1. Loads one mlr3 .qs xgboost model in the parent.
2. Sets `options(future.globals.onReference = "error")`.
3. Calls `future_map(1:2, function(i) lrn$predict_newdata(...))` with the model in the closure.
4. Reports success/failure.

If it fails on xgboost: fall back to `learner$marshal()` in the parent + `learner$unmarshal()` in the worker (cheap — both are R6 methods). Phase 2 RESEARCH already lists this as risk 3 with a clear escape hatch.

### `restore_ranger_importance_mode()` — when to mutate

`src/allocation.r:343-356` mutates the model object's `importance.mode` field. The mlr3 branch at line 540 does NOT call this function (mlr3-trained ranger always has `importance="none"` set explicitly per Phase 2). So no mutation inside the worker for mlr3 models → no COW page copy. Confirmed safe.

For legacy tidymodels/butchered models still in the predict dispatcher, the mutation happens (line 618, 650). With multicore, the parent should call `restore_ranger_importance_mode()` ONCE on each pre-loaded model before fork. Today this is impossible because `models_list` doesn't exist in the parent — Phase 3's Shape A makes it possible.

Add to the parent-side pre-load:

```r
models_list <- lapply(models_list, restore_ranger_importance_mode)
```

This is a no-op for mlr3 models (the function guards on `inherits(model_obj$fit, "ranger")` which mlr3 objects don't match by inheritance, so they pass through unchanged). The planner should verify by reading the function body.

---

## Native thread pinning order (Q6)

Order matters because some library calls read environment variables only at first use, and others have static state set on package load.

### Exact incantation, in order

```r
# src/allocation.r — new helper
pin_native_threads_to_one <- function(verbose = FALSE) {
  # 1. Env vars FIRST. These are inherited into fork children and read by
  #    BLAS/OpenMP/GDAL at first use. Setting them after the libraries are
  #    initialised is too late for some implementations.
  Sys.setenv(
    OMP_NUM_THREADS      = "1",
    OPENBLAS_NUM_THREADS = "1",
    MKL_NUM_THREADS      = "1",
    GOTO_NUM_THREADS     = "1",
    GDAL_NUM_THREADS     = "1",   # already discussed in writeRaster wopt
    R_DATATABLE_NUM_THREADS = "1"
  )

  # 2. Programmatic BLAS + OMP — overrides env if the library was already
  #    initialised. RhpcBLASctl wraps the BLAS-specific API. [CITED: CRAN
  #    RhpcBLASctl docs]
  if (requireNamespace("RhpcBLASctl", quietly = TRUE)) {
    try(RhpcBLASctl::blas_set_num_threads(1L), silent = !verbose)
    try(RhpcBLASctl::omp_set_num_threads(1L), silent = !verbose)
  }

  # 3. data.table — sets process-wide thread count. Must be after package
  #    load (data.table reads R_DATATABLE_NUM_THREADS at load, but
  #    setDTthreads is the authoritative runtime control).
  if (requireNamespace("data.table", quietly = TRUE)) {
    data.table::setDTthreads(1L)
  }

  # 4. arrow — limits the Arrow C++ thread pool. arrow::set_cpu_count() must
  #    be called before any Arrow Dataset operation in this process; once a
  #    Dataset scan is in flight, it ignores changes.
  if (requireNamespace("arrow", quietly = TRUE)) {
    try(arrow::set_cpu_count(1L), silent = !verbose)
    try(arrow::set_io_thread_count(1L), silent = !verbose)
  }

  # 5. xgboost — per-Booster nthread is set at predict time. Phase 2
  #    already pins nthread=1 inside build_mlr3_learner() at
  #    src/transition_modelling.r:182, so the saved xgb.Booster carries the
  #    nthread=1 attribute. No process-wide pin needed; xgboost has no
  #    global thread-count knob. [CITED: xgboost docs; Phase 2 RESEARCH §A1]

  # 6. terra — terra::terraOptions(threads=1) does NOT exist as such; terra
  #    inherits GDAL_NUM_THREADS via the GDAL_NUM_THREADS env (set in step 1).
  #    The wopt$gdal = c("NUM_THREADS=1") creation option per writeRaster
  #    call is the per-file pin.

  if (verbose) {
    cat("Native thread pinning applied:\n")
    cat(sprintf("  OMP_NUM_THREADS=%s\n", Sys.getenv("OMP_NUM_THREADS")))
    if (requireNamespace("data.table", quietly = TRUE)) {
      cat(sprintf("  data.table threads=%d\n", data.table::getDTthreads()))
    }
    if (requireNamespace("arrow", quietly = TRUE)) {
      cat(sprintf("  arrow cpu_count=%d\n", arrow::cpu_count()))
    }
  }
  invisible(NULL)
}
```

### Where to call from

In `scripts/run_allocation.r` AFTER pre-flight passes (line 216) and BEFORE `future::plan()`. Call ONCE in the parent — forks inherit env vars and (per [CITED: RhpcBLASctl docs — "parallel library does retain the BLAS settings of the parent (presumably since it uses fork())"]) the BLAS/OMP state.

For multisession (Windows), the env vars are inherited at child-process spawn, but in-process state like `data.table::setDTthreads()` and `arrow::set_cpu_count()` does NOT cross PSOCK. Each PSOCK worker must call the pin function itself. Use `future`'s `plan(..., workers = N)` with a startup hook:

```r
# After future::plan(multisession, workers = N):
if (plan_choice$strategy == "multisession") {
  future::plan(future::multisession, workers = plan_choice$workers)
  # Run the pin function on each PSOCK worker
  future::value(future::future({
    source("src/allocation.r")
    pin_native_threads_to_one()
  }))
}
```

Or more idiomatically, use `parallelly::makeClusterPSOCK(..., rscript_envs = c("OMP_NUM_THREADS=1", ...))` — but that requires building the cluster manually. Simpler: keep the env vars (step 1) as the durable pin for PSOCK workers; let data.table/arrow defaults follow from env reads at load.

Under multicore (Linux), all of this works via fork inheritance and no per-worker re-pin is needed.

### Order rationale references

- **Env vars before BLAS init:** [CITED: stat.ethz.ch R-sig-Debian thread on OpenBLAS — OpenBLAS reads OMP_NUM_THREADS at first matrix operation; setting after init may not stick on all implementations.]
- **RhpcBLASctl overrides env:** the package's purpose is exactly to provide a runtime override that works even when env was wrong at load time. [CITED: CRAN RhpcBLASctl-package docs]
- **data.table reads R_DATATABLE_NUM_THREADS at load:** confirmed in data.table docs (?data.table::setDTthreads).
- **arrow set_cpu_count must precede Dataset operations:** [CITED: arrow R package docs — "set_cpu_count: Manage Arrow's thread pool. Affects subsequent operations."]

### What about `Sys.setenv(R_DATATABLE_NUM_THREADS = "1")`?

data.table reads this only at package load. Since `data.table` is loaded by the time `pin_native_threads_to_one()` runs, the env var has no effect. Use `data.table::setDTthreads(1L)` (which we already do in step 3). Keep the env var for the sake of being explicit and for any subprocess that's launched later.

---

## Per-worker RAM budget (Q5)

### What success criterion 3 requires

"RSS profiling (Phase 1) shows per-worker private memory bounded — no worker exceeds a documented per-worker budget."

This is two things:
1. A **documented number** for the per-worker budget.
2. A **verifiable assertion** that no worker's RSS exceeded it during the smoke run.

### How to compute the budget

The HPC job has a hard cgroup memory limit set by `--mem-per-cpu × cpus-per-task` (today: 8G × 8 = 64 GB). The parent gets the full memory pool but accounted RSS is per-process. Budget per worker should be:

```
budget_per_worker_MB = (cgroup_limit_MB - parent_baseline_RSS_MB - safety_margin_MB)
                      / num_workers
```

With cgroup_limit = 64000 MB, parent_baseline (after models_list pre-load and nhood pre-compute) ≈ 12000 MB (estimated, see §architectural memory budget), safety_margin = 4000 MB, num_workers = 3 (3 regions parallel):

```
budget_per_worker_MB = (64000 - 12000 - 4000) / 3 ≈ 16000 MB
```

That's per-fork **private** RSS — the additional pages dirty-copied via COW + the worker's own allocations. Under multicore COW the shared parent pages don't count against the worker's private RSS in the kernel's `Pss` (Proportional Set Size) view, but they DO appear in `VmRSS` (`/proc/<pid>/status`).

**Caveat (already documented in research/SUMMARY.md item 9):** SLURM `MaxRSS` aggregates child processes' RSS naively and may double-count shared pages. The cgroup OOM-killer fires on the cgroup's `memory.current` (sum of unique physical pages across the cgroup), not on summed RSS. Therefore:

- **For success-criterion verification**: read `/sys/fs/cgroup/memory.current` from inside R during the run. Phase 1's `prof_mem_summary()` doesn't read cgroup memory yet — Phase 3 should add a `prof_cgroup_summary()` helper.
- **For per-worker assertion**: use `Pss` from `/proc/<pid>/smaps` (sums to the cgroup's view) rather than `VmRSS`. Or accept that VmRSS overcounts and pick a budget that allows for it (the 16 GB number above is generous for that reason).

### Per-worker numbers to log

Add these at strategic points (parent and worker):

| Event | Quantity | Source |
|-------|---------|--------|
| Parent baseline (post-models_list load, post-nhood precompute) | `ps::ps_memory_info()$rss` MB | `prof_mem_summary("parent_baseline", log_file)` |
| Worker entry (first prof_tic in worker) | per-process VmRSS MB + Pss MB | new helper `prof_worker_baseline()` |
| Worker peak (after predict, per transition) | VmHWM MB | existing `prof_toc(..., stage=predict)` already logs `peak_rss` |
| Cgroup snapshot per region | `memory.current` MB | new `prof_cgroup_snapshot()` reads `/sys/fs/cgroup/memory.current` and `/sys/fs/cgroup/memory.max` |

`prof_cgroup_snapshot()` implementation sketch:

```r
prof_cgroup_snapshot <- function(tag, log_file) {
  cur_path <- "/sys/fs/cgroup/memory.current"
  max_path <- "/sys/fs/cgroup/memory.max"
  if (!file.exists(cur_path)) return(invisible(NULL))
  cur <- as.numeric(readLines(cur_path, warn = FALSE)) / 1024^2
  max <- if (file.exists(max_path)) {
    v <- readLines(max_path, warn = FALSE)
    if (identical(v, "max")) NA_real_ else as.numeric(v) / 1024^2
  } else NA_real_
  log_msg(sprintf("CGROUP %s memory.current=%.1fMB memory.max=%sMB",
                  tag, cur, if (is.na(max)) "unlimited" else sprintf("%.1f", max)),
          log_file)
  invisible(list(current_mb = cur, max_mb = max))
}
```

The verifier (the post-mortem script `diagnose_alloc_crash.sh` plus a new grep-based assert in the Phase 3 smoke-run task) can then check:

- `PROFILE region=X stage=predict ... peak_rss=YYY MB` and assert YYY < budget_per_worker_MB.
- `CGROUP ... memory.current=ZZZ MB` per region's worker is below memory.max minus headroom.

### Verifiable budget number

Until empirical numbers exist from a dev run, the planner should treat these as the documented assumptions (mark them in PLAN.md verification criteria):

| Quantity | Assumed value | How to verify |
|----------|---------------|---------------|
| Parent baseline (after `pin_native_threads_to_one()` + `select_allocation_plan()` + before any region work) | ≤ 1500 MB | Dev run; log via `prof_mem_summary("parent_pre_region", log_file)` |
| Parent after `models_list` pre-load (1 region's ~50 models × ~80 MB) | ≤ 6000 MB | Dev run; log post `load_all_region_models()` |
| Parent after all 3 regions' models pre-loaded (Shape A) | ≤ 12000 MB | Dev run; if Shape B is chosen instead, this row doesn't apply |
| Parent after nhood pre-compute (3 regions × ~20 TIFs each = ~60 files on scratch, in-RAM cost ≈ 0) | ≤ 12000 MB | Same |
| Per-worker VmHWM during predict | ≤ 16000 MB | log_msg in `prof_toc(..., stage=predict)` |
| Cgroup memory.current peak | ≤ 56000 MB (8 GB safety) on a `--mem-per-cpu=8G --cpus-per-task=8` job | `prof_cgroup_snapshot("post_region", log_file)` |

The exact numbers will only emerge from the smoke run. The planner's job is to make these assertable as PLAN.md acceptance criteria, not to predict them.

---

## OOM diagnosis hookup (Q7)

### What Phase 1 already provides

- `scripts/diagnose_alloc_crash.sh` — single post-mortem entry point. Reads `sacct -j $JOB_ID --format=...`, `seff $JOB_ID`, cgroup memory snapshots if available, and tails per-region log files for SENTINEL lines.
- `worker_state_init()` / `worker_state_set()` / `worker_state_flush_sentinel()` in `src/utils.r:1060-1163` — durable breadcrumb state, one line of state per lifecycle event, plus an `on.exit()` sentinel.
- `initialize_worker_log()` at `src/utils.r:1030` — per-pid log filename. Under multicore each fork has its own pid → automatically distinct filenames. Confirmed safe.

### What Phase 3 needs to add

Almost nothing. The Phase 1 hooks already work under multicore for normal R-level errors (the on.exit fires before the process exits cleanly).

What does NOT work and cannot be made to work:

- **SIGKILL from cgroup OOM-killer:** uncatchable; the worker process is terminated before `on.exit()` runs. [CITED: SLURM/cgroup docs — "When memory limits are exceeded, cgroups will kill the job with the OOM killer, which sends SIGKILL, which is uncatchable."] The only durable evidence is the most recent STATE line that was flushed to the log file before the kill. This is exactly what `worker_state_set(..., log_file=...)` does — it `log_msg`'s a STATE line on every lifecycle boundary, which `cat(append=TRUE)` writes synchronously (no buffer).
- **slurmstepd's OOM message:** lands in the SLURM .out file, not the per-region log. `diagnose_alloc_crash.sh` reads SLURM accounting and surfaces it. No code change needed.

### One small addition

When a fork receives SIGKILL from cgroup, the parent's `furrr::future_map()` raises a `MulticoreFuture` error (or "lost worker" message — version-dependent). The parent's `tryCatch` at `scripts/run_allocation.r:227-238` catches this and prints `traceback()`. Make sure the catch path also calls `worker_state_flush_sentinel()` in the parent's context to record "child died" — but the child has its own state env, so the parent can only record "I noticed child X died at parent stage Y." Add to the catch:

```r
error = function(e) {
  msg <- conditionMessage(e)
  if (grepl("MulticoreFuture|MultisessionFuture|lost.*worker", msg, perl = TRUE)) {
    # Parent's perspective on a SIGKILLed child. The child's per-region log
    # already has the last-known STATE line.
    log_msg(
      sprintf("PARENT_SENTINEL reason=lost_worker error=%s", msg),
      file.path("logs", sprintf("allocation_summary_%s.txt", Sys.getenv("SLURM_JOB_ID", "local")))
    )
  }
  ...
}
```

This is a 5-line addition to `scripts/run_allocation.r`, not a structural change. Phase 3 should include it.

---

## Terra migration overlap (Q8)

Phase 4 owns the full `raster` → `terra` migration (PIPE-05). For Phase 3, the MINIMUM terra surface area required is:

1. **Parent-side TIF write** of nhood rasters: `terra::writeRaster(rast, path, ...)`. Already used elsewhere in the codebase (allocation.r:968, :1707).
2. **Worker-side `terra::rast(path)`** for nhood, anterior, region, current LULC. Already used (allocation.r:952, 955, 1363, 1390, 1598).
3. **Parent-side computation of nhood rasters** via `compute_single_nhood_raster()` (allocation.r:1759). This already uses `terra::focal()` exclusively.

So Phase 3's terra surface is **already terra**. No new code touches the legacy `raster::` API (which lives in `lulcc.spatprobmanipulation.r`, `spatial_interventions_prep.r`, `landscape_pattern_analysis.r` — all outside the Phase 3 scope).

**Discipline for the planner:** Phase 3 tasks must NOT touch `lulcc.spatprobmanipulation.r` etc. If a task tempts the implementer to "fix raster:: while we're here," that's scope leak into Phase 4. The plan-checker should reject any Phase 3 task that modifies those files.

---

## `future.globals.onReference = "error"` enablement (Q9)

### Where to set the option

In `scripts/run_allocation.r`, BEFORE `future::plan()`:

```r
if (isTRUE(as.logical(Sys.getenv("ALLOCATION_DEV_STRICT_GLOBALS", "FALSE")))) {
  options(future.globals.onReference = "error")
  cat("DEV: future.globals.onReference = 'error' enabled\n")
}
```

Production runs leave `ALLOCATION_DEV_STRICT_GLOBALS` unset → option stays at the default `"ignore"` so there's no scan overhead. The Phase 3 plan should include a dedicated dev-run task that:

1. Sets the env var.
2. Runs one scenario × one region × one timestep (e.g. via `ALLOCATION_PROFILE=TRUE ALLOCATION_PROFILE_SCENARIO=BAU ALLOCATION_PROFILE_TIMESTEP_INDEX=1`).
3. Asserts no `non-exportable` error fires.
4. Logs the run to `logs/dev_strict_globals_<jobid>.out`.

This is the verification path for success criterion 4 ("`options(future.globals.onReference = "error")` passes during a dev run").

### How to surface failures

`future` raises a hard error when an externalptr is found. The error message names the offending object class (e.g., `non-exportable object found: 'anterior' of class 'SpatRaster'`). This is human-readable and points directly at the closure capture. Phase 3 plan task: capture stderr, grep for `non-exportable`, list every flagged object. If the list is non-empty, the dev run FAILS and the fix is either to move the object's construction into the worker or pass it as a path.

### Known false positives the planner should be aware of

- **data.table's `.internal.selfref` is flagged but exportable** [CITED: future-4-non-exportable-objects vignette]. If `anterior_dt` is captured by the closure (it shouldn't be — it's created inside the worker), the error message will fire on data.table. The fix is: keep `anterior_dt` construction inside the worker.

### Should we leave the option on in production?

Per the future docs: "The future.globals.onReference option is set to 'ignore' by default due to the extra overhead 'error' introduces, which can be significant for very large nested objects." With `models_list` being a large nested object, the scan cost matters. **Recommendation:** keep the option `"ignore"` in production; use the env-gated dev mode to catch regressions in CI-like runs.

---

## Scenario × region × timestep "smoke run" definition (Q10)

### Candidate: BAU × Coast × first timestep

**Why Coast:** smallest region of the three (Andes, Amazon, Coast). The codebase has no documented region size differences, but Peru's coastal strip is geographically the narrowest band. If the codebase exposes pixel counts, the planner should verify; if not, the dev run can simply try all three and pick the smallest by wall time.

**Why BAU:** baseline scenario; no special intervention layers. SSP mapping at `src/allocation.r:1270` resolves to `ssp245` for BAU (per config). The parquet partition `dynamic_preds_pq_path` includes scenario-specific data only for `year_ant > 2022`; for the first timestep (2022 → 2025 or similar) the SSP key is `"baseline"` (allocation.r:1281-1283), simplifying the predictor read.

**Why first timestep:** smallest data extent; `current_lulc` and `anterior` are the initial-year rasters, which are the only ones guaranteed to exist before any allocation has run.

### Needed inputs

Already established by Phase 1 pre-flight:
- `HPC_SCRATCH_ROOT`, `HPC_TMP_ROOT`, `TERRA_TEMP`, `DINAMICA_EGO_8_HOME` env vars
- `ref_grid_path`, `lulc_aggregation_path` config keys point at valid files
- All R packages in `allocation_env.yml` loadable
- Dinamica EGO container artifact present (or local DinamicaConsole on PATH)

Phase 3-specific addition:
- `models_list` for Coast region must be available as `.qs` files in `config[["transition_model_dir"]]/<calibration_period>/`. Verify via `ls outputs/transition_models/2018_2022/*_coast.qs`. The Phase 2 deliverable `scripts/retrain_all_models.r --region "Coast"` produces these.

### Expected wall time

From Phase 0 research (PROJECT.md crash profile):
- `model_load` (qs::qread of a mlr3 .qs): expected 0.5–2 s (vs. 4–16 s for old butchered RDS). [VERIFIED: Phase 2 chose qs::qsave for 3–10× speedup.]
- `predictor_load` (arrow parquet read): 10–22 s per transition. Unchanged in Phase 3.
- `nhood_extract` (now backed by pre-computed TIFs): expected to drop from ~78 s to ~10–20 s because the focal computation is done once in the parent. The per-worker cost is just `terra::extract(rast, points)`.
- `predict` (mlr3 `predict_newdata`): 385–472 s per transition is the Phase 4 problem; Phase 3 doesn't move it. With 30–50 transitions for a Coast region, total predict time ≈ 200–400 minutes per timestep per region.

**Realistic smoke-run wall time:** 3–7 hours for one (scenario, region, timestep). The submit script's `--time=48:00:00` is plenty.

### How to detect the cgroup OOM-killer in the SLURM log

Per `diagnose_alloc_crash.sh` (Phase 1 deliverable), check for:
1. `sacct -j $JOB_ID --format=JobID,State,ExitCode` showing `OUT_OF_MEMORY` or `0:9` (SIGKILL).
2. SLURM .out file containing `slurmstepd: error: Detected N oom-kill event(s) in step ... cgroup`.
3. Cgroup memory.current snapshot logged by `prof_cgroup_snapshot()` near `memory.max`.
4. `dmesg | grep -i "killed process"` on the compute node (root-only on Euler; skip).

If `sacct` State=COMPLETED and ExitCode=0:0 → success criterion 2 PASSES.
If sacct shows OUT_OF_MEMORY or any non-zero exit → FAILS; iterate on budget.

### Defining "completes"

For success-criterion 2 the run must:
1. Produce a posterior TIF for the chosen region (file exists, non-zero size, valid raster — `terra::rast(path)` reads without error and `terra::summary()` returns sensible value range).
2. Have `sacct` State=COMPLETED.
3. Have zero `SENTINEL reason=incomplete` or `OOM` lines in the per-region log.
4. Cgroup peak < memory.max.

The Phase 3 plan should include a small shell verification step (or R script) that runs these four checks and exits 0/1 accordingly.

---

## Memory budget — corrected estimate

Revising the Phase 0 estimate (research/SUMMARY.md) with Phase 2 numbers:

| Component | Size | Notes |
|---|---|---|
| R parent baseline (packages loaded, config) | ~700 MB | mlr3 + terra + arrow + xgboost + ranger + data.table |
| Thread pinning + plan select | negligible | Helper calls |
| `models_list` (140 mlr3 .qs models × ~80 MB average) | ~11 GB | Loaded in parent before fork (Shape A) |
| `focal_matrices` (named list of integer matrices) | ~50 MB | One-time read |
| `nhood_paths` (named character vector) | <1 MB | Just paths |
| Pre-computed nhood TIFs on scratch (60 files, ~50 MB each compressed) | ~3 GB | On disk, NOT in parent RAM |
| `class_name_to_value`, model_info data.tables | <10 MB | Per-region metadata |
| **Parent total (shared via COW to children)** | **~12 GB** | |
| **Per-worker private RSS (peak during predict)** | **~3–5 GB** | dominated by `from_data` data.table and predict result for large from-classes |
| **3 workers total private (3 forks)** | **~9–15 GB** | |
| **Cgroup `memory.current` peak (parent + 3 forks, dedup via COW)** | **~16–25 GB** | Well under 64 GB cgroup limit |
| DinamicaConsole subprocess (sequential) | ~2–4 GB | After all 3 region workers finish; not concurrent |

**SLURM request stays at `--cpus-per-task=8 --mem-per-cpu=8G = 64 GB total`.** Plenty of headroom for the smoke run. For production with all 4 scenarios × 38 timesteps × 3 regions, the bottleneck is wall time, not memory.

---

## Common Pitfalls

### Pitfall 1: Hoisting an in-memory SpatRaster to the parent
**What goes wrong:** A future refactor moves `anterior <- terra::rast(anterior_path)` from inside `generate_probability_maps` to the per-region init in the parent, intending to "reuse" it. With `future.globals.onReference="error"` the closure is rejected at fork time. Without it, multisession serialises the path-backed SpatRaster (works but slow) or fails on an in-memory one.
**Prevention:** Treat in-memory SpatRasters as worker-private. Construct rasters in the worker; pass paths from the parent. Enforce via dev-run gate.

### Pitfall 2: Calling `pin_native_threads_to_one()` AFTER `future::plan()`
**What goes wrong:** With multicore, forks have already been spawned with the parent's BLAS/data.table state. With multisession, PSOCK workers were spawned without env vars in their startup environment. Either way, the pin is too late.
**Prevention:** Hard-code the order in `scripts/run_allocation.r`: pre-flight → pin → select → plan. Make the pin function idempotent so it can be safely called twice.

### Pitfall 3: Leaving `parallelly.fork.enable` to the default on RStudio Server
**What goes wrong:** parallelly conservatively returns `supportsMulticore()=FALSE` inside RStudio. If a maintainer runs allocation interactively on Euler login node via RStudio Server, the selector silently falls back to multisession → OOM crash returns.
**Prevention:** Log the chosen strategy AND `parallelly::supportsMulticore()` AND `parallelly.fork.enable` at startup. Operators see in the log what was actually chosen and why.

### Pitfall 4: Pre-loading `models_list` then mutating it inside the worker
**What goes wrong:** `restore_ranger_importance_mode()` is called on each model object inside the worker today (allocation.r:618). Under multicore, this mutation triggers COW page copies on the model's slot, replicating the model into per-worker private memory. The COW benefit is lost.
**Prevention:** Move all `restore_ranger_importance_mode()` calls to the parent's pre-load step. Mark all elements of `models_list` as "read-only after this point" by convention.

### Pitfall 5: Confusing `VmRSS` aggregation with actual physical memory use
**What goes wrong:** SLURM's `sacct MaxRSS` sums VmRSS across the worker tree, double-counting COW-shared pages. With 3 forks each VmRSS=15 GB and a parent at 12 GB, `MaxRSS` reads ~57 GB, but kernel-tracked physical memory is ~25 GB. Operators tune `--mem-per-cpu` upward to "fix" this and waste cluster resources.
**Prevention:** Document explicitly in `diagnose_alloc_crash.sh` output that `MaxRSS` overcounts under multicore; use cgroup `memory.current` (or `seff`'s Memory Efficiency line) as the authoritative measure. Phase 1 already does this.

### Pitfall 6: Worker log files collide under multicore
**What goes wrong:** `initialize_worker_log()` uses `Sys.getpid()` (utils.r:1034). Under multicore, each fork has its own pid (Linux assigns new pids on fork). Under multisession, each PSOCK worker has a distinct pid. So filenames DO differ. But if a future refactor pre-creates the log file in the parent and passes the path into the worker, both children write to the same file, interleaved.
**Prevention:** Always call `initialize_worker_log()` INSIDE the worker. Today's code does (allocation.r:919). Verifier should grep for any future hoisting.

### Pitfall 7: arrow Dataset opened in the parent
**What goes wrong:** `ds_static <- arrow::open_dataset(...)` returns an R6 wrapper holding a C++ Dataset handle (externalptr). If hoisted to the parent (to "reuse across regions"), `future.globals.onReference="error"` rejects the closure. Under fork it might inherit the handle and the child reads from it, but the C++ runtime may not be fork-safe for in-flight scans.
**Prevention:** Open arrow datasets inside the worker. Today's code does (allocation.r:1409). Don't hoist.

---

## State of the Art

Where this differs from what the codebase had (Phase 0 baseline):

| Old approach (Phase 0) | Phase 3 approach | When changed | Impact |
|------------------------|-------------------|--------------|--------|
| `future::plan(multisession, workers=N)` hard-coded | `select_allocation_plan()` returns multicore on Linux, multisession on Windows, sequential on opt-out | Phase 3 | 60–80% per-worker RAM reduction on HPC via COW |
| `ALLOCATION_NUM_WORKERS` env var as sole worker-count source | `parallelly::availableCores()` (SLURM-aware) with env-var override | Phase 3 | Robust to changes in SLURM allocation; explicit override still works |
| Models loaded one at a time inside per-transition loop | Pre-loaded as `models_list` in parent before fork | Phase 3 | Fork COW shares all models across region forks |
| Neighbourhood rasters cached per-worker in-memory | Pre-computed by parent to scratch TIFs; workers read paths | Phase 3 | Eliminates duplicate compute; fork-safe |
| `restore_ranger_importance_mode()` called inside worker | Called once in parent before fork | Phase 3 | Avoids COW page copy of model objects |
| No native thread pinning before `future::plan()` | `pin_native_threads_to_one()` helper invoked first | Phase 3 | Prevents `N × M` thread oversubscription |
| No `future.globals.onReference` dev gate | `ALLOCATION_DEV_STRICT_GLOBALS=TRUE` toggles globals scanner | Phase 3 | Surfaces regressions that re-introduce SpatRaster captures |

Deprecated/outdated:
- The comment in `src/calibrate_allocation_parameters.r:797` claiming "multicore causes OOM" — semantically wrong for read-mostly workloads. Phase 3 plan should remove or correct it when refactoring that file's selector.

---

## Validation Architecture

> Per the Nyquist-validation reference and the planner brief Q11. `nyquist_validation` is `false` in `.planning/config.json`, so the planner is not required to derive automated unit/integration tests — but the surfaces below are what the planner should turn into PLAN.md acceptance criteria.

### Test Framework
| Property | Value |
|----------|-------|
| Framework | None present — repo has no unit/integration test infrastructure (TEST-01, TEST-02 are v2 requirements, deferred to milestone v2 per `.planning/REQUIREMENTS.md`) |
| Config file | None |
| Quick run command | None automated; manual smoke run via `scripts/submit_allocation.sh` + `scripts/diagnose_alloc_crash.sh` |
| Full suite command | None |

### Phase Requirements → Verification Surface Map

Since automated tests don't exist, each requirement maps to a manual or shell-script verification. The planner should embed each row as a falsifiable acceptance criterion in PLAN.md.

| Req ID | Behavior | Verification Type | Manual Check Command / Log Pattern |
|--------|----------|--------------------|-----------------------------------|
| MEM-03 | `select_allocation_plan()` picks multicore on Linux, multisession on Windows | Unit-style: R one-liner | `Rscript -e 'source("src/allocation.r"); cat(select_allocation_plan()$strategy)'` — expect `multicore` on Euler login node, `multisession` on Windows dev box |
| MEM-03 | `parallelly::availableCores()` respects SLURM | Integration: SLURM smoke | `sbatch --cpus-per-task=4 --wrap='Rscript -e "cat(parallelly::availableCores())"'` returns 4 |
| MEM-05 | Pre-computed nhood TIFs exist before any worker starts | Filesystem assertion | `ls $TERRA_TEMP/nhood_cache/<scenario>_<year_post>_<region_suffix>/*.tif` non-empty before first `worker_state STATE stage=predict` line in any per-region log |
| MEM-02, success criterion 4 | `future.globals.onReference="error"` passes during dev run | Integration: dev gate | `ALLOCATION_DEV_STRICT_GLOBALS=TRUE Rscript scripts/run_allocation.r` with a 1-region 1-timestep config; stderr must NOT contain `non-exportable` |
| Success criterion 5 | All native thread counts pinned to 1 before `future::plan()` | Log-asserted | Per-region log contains `PROFILE … pin_native_threads_to_one` line followed by `OMP_NUM_THREADS=1 data.table_threads=1 arrow_cpu_count=1` BEFORE any `STATE stage=region_setup` line |
| Success criterion 2 | One scenario × region × timestep completes without OOM | Integration smoke | (i) `sacct -j $JOB_ID --format=State,ExitCode` = `COMPLETED 0:0`; (ii) posterior TIF exists and `terra::rast(path)` succeeds; (iii) no `SENTINEL reason=incomplete` in per-region log; (iv) no `oom-kill event` in SLURM .out |
| Success criterion 3 | Per-worker private memory bounded; `models_list` loaded once in parent | Log-asserted | (a) exactly one `PROFILE … stage=preload_models region=…` line per (scenario, timestep) appears in the parent's log; (b) per-region log `PROFILE … stage=predict … peak_rss=YYY` has YYY < documented budget (16 GB recommended) for every transition |
| MEM-01 | All scenarios × regions × timesteps run without OOM | Production scale-out — DEFERRED to Phase 4 milestone | Phase 3 only needs the single-instance smoke. Full-matrix is Phase 4 success criterion 1. |

### Sampling Rate
- **Per task commit:** None (no test framework).
- **Per phase milestone:** Smoke run (one scenario × region × timestep) on Euler with `ALLOCATION_DEV_STRICT_GLOBALS=TRUE`.
- **Phase gate:** All seven verification rows above green; `/gsd-verify-work` reviews the smoke run log artefact.

### Wave 0 Gaps
- Add `select_allocation_plan()` helper to `src/allocation.r`.
- Add `pin_native_threads_to_one()` helper to `src/allocation.r`.
- Add `prof_cgroup_snapshot()` helper to `src/allocation.r`.
- Add `load_all_region_models()` helper to `src/allocation.r`.
- Modify `compute_single_nhood_raster()` (or add a wrapper) to write directly to a TIF path provided by the parent.
- Update `scripts/run_allocation.r` to call the helpers in the right order.
- Update `src/calibrate_allocation_parameters.r:780-820` to use the same selector (scope decision — see Q1 above).
- No new test files — testing infrastructure is v2 deferred.

---

## Security Domain

Not applicable in the conventional sense — this is a single-node R compute job with no network endpoints, no auth, no input from untrusted sources. The only relevant security-adjacent properties:

| Aspect | Applies | Standard Control |
|--------|---------|-----------------|
| Input validation | yes | Pre-flight validates env/files/packages (already established in Phase 1) |
| Path injection | yes | `train_mlr3_transition()` already has the T-02-03 guard (transition_modelling.r:243-251). The path-passing pattern in Phase 3 (`nhood_paths`, `models_list` file paths) is constructed by the parent from config + scratch root — no user input. |
| Resource exhaustion (DoS) | yes (self-inflicted via OOM) | cgroup memory limit + worker budget + smoke run before production |
| Secret handling | no | No secrets in this pipeline |
| Subprocess command injection | low | `processx::run` for Dinamica uses argv vector (no shell); paths are config-driven |

No new security surface in Phase 3.

---

## Environment Availability

Already audited in Phase 1's `validate_allocation_runtime()` — every package Phase 3 uses is already listed:

| Dependency | Required By | Available | Version | Fallback |
|------------|-------------|-----------|---------|----------|
| `parallelly` | `select_allocation_plan()` | ✓ (transitive dep of `future` >= 1.20) | latest | `parallel::detectCores() - 1` (already in code) |
| `future` | `future::plan(multicore)` | ✓ (in allocation_env.yml line 54) | r-future | none — required |
| `furrr` | `future_map()` | ✓ (line 55) | r-furrr | none — required |
| `RhpcBLASctl` | `blas_set_num_threads(1)` | ✓ (line 91) | r-rhpcblasctl | env vars only |
| `data.table` | `setDTthreads(1)` | ✓ (line 58) | r-data.table | none |
| `arrow` | `set_cpu_count(1)` | ✓ (line 41) | r-arrow | none |
| `terra` | `writeRaster`, `rast()` | ✓ (line 38) | r-terra | none |
| `qs` | `qread(models)` | ✓ (line 84) | r-qs | `readRDS` fallback exists (allocation.r:1533) |
| `mlr3`, `mlr3learners` | learner `$predict_newdata()` | ✓ (lines 102-103) | r-mlr3 | none |
| `ps` | process RSS | ✓ (line 87) | r-ps | `/proc/self/status` fallback (allocation.r:64-98) |

**Missing dependencies:** none. The Phase 1 pre-flight already lists `parallelly` indirectly via the future package. If we want explicit pre-flight checking of `parallelly`, add it to the `packages_expected` list in `validate_allocation_runtime()` (src/allocation.r:243-248). Recommend the planner add this in a small task.

**Phase 3 has no new external dependencies.**

---

## Open Questions

1. **mlr3 marshal/unmarshal for xgboost under strict-globals scan.** Phase 2 RESEARCH §A4 asserts that mlr3learners 0.14+ classif.xgboost does NOT declare the "marshal" property and `qs::qsave`/`qs::qread` round-trips work for prediction. But the future globals scanner may still reject the embedded externalptr inside the wrapper. **What we know:** Phase 2's 5-row predict-check passes after qs round-trip. **What's unclear:** whether `options(future.globals.onReference="error")` accepts the xgboost learner as a closure capture. **Recommendation for the planner:** include a dev-run task that loads ONE xgboost model in the parent, calls `future_map()` with strict-globals on, and reports. If it fails, fall back to per-region per-worker `qs::qread` (current design — works but loses COW). This decision impacts whether Shape A (parent-preload) or Shape B (per-worker load) is the right pattern.

2. **Pre-load all 3 regions' models at once, or one region at a time?** Shape A (all 3 regions = ~11 GB parent baseline) is simpler and gives best COW sharing, but parent must hold ~11 GB. Shape "A-prime" (load only the current region's models before each region's forks) is 1/3 of that — but with 3 forks each touching a disjoint subset of models, COW shares nothing. **Recommendation:** Shape A is the right default. If the parent baseline becomes a problem on smaller `--mem-per-cpu`, the planner can fall back to A-prime. Either way, the success criterion 3 wording is satisfied because models are loaded once per (scenario, timestep) cycle in the parent — never inside the worker.

3. **Should `src/calibrate_allocation_parameters.r:780-820` be refactored to use the new `select_allocation_plan()`?** It has the inverted "multicore causes OOM" comment and the same multisession-only pattern. Refactoring keeps codebase coherent but creeps Phase 3 scope. **Recommendation:** include in Phase 3 if the planner judges it small (~1 task); otherwise mark as a follow-up. The calibration step runs predict similarly to allocation; same memory issues apply.

4. **What's the actual byte size of a Phase 2 mlr3 .qs file in practice?** All upstream assertions (parent baseline = 11 GB) depend on the average being ~80 MB. Phase 2 had a 200 MB size gate (warning only). If models are at the ceiling, parent baseline could be 28 GB. **Recommendation:** include a one-line `du -sh` task in the smoke run plan; if numbers blow the budget, defer to Phase 4 to either tighten ranger params or revisit Shape A vs A-prime.

5. **Does `parallelly::availableCores()` on Euler's login node return the SLURM allocation correctly?** It should per parallelly docs, but Euler-specific patches or older parallelly versions could behave differently. **Recommendation:** include a dev-run task that prints `parallelly::availableCores()` in a 4-CPU SLURM job and asserts the result is 4.

6. **Cgroup v1 vs v2 path on Euler.** `/sys/fs/cgroup/memory.current` is cgroup v2 syntax. Euler's nodes may still use v1 (`/sys/fs/cgroup/memory/memory.usage_in_bytes`). **Recommendation:** `prof_cgroup_snapshot()` should try both paths and use whichever exists. Phase 1 RESEARCH already flagged this (verification flag 1).

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | mlr3learners 0.14+ classif.xgboost qs-roundtrip preserves predict capability without marshal | §Fork-safety analysis, §Models pre-load | [ASSUMED — inherited from Phase 2 02-RESEARCH §A4]. If wrong, must call `$marshal()` in parent + `$unmarshal()` in worker. Detectable via dev gate. |
| A2 | Average Phase 2 mlr3 .qs file is ~80 MB; max ~200 MB (size gate threshold) | §Memory budget, §Models pre-load | [ASSUMED — based on Phase 2 size targets]. If actual averages are higher, parent baseline blows past 12 GB. Mitigated by `du -sh` task in smoke run. |
| A3 | `parallelly::availableCores()` reads `SLURM_CPUS_PER_TASK` on Euler | §Plan selector | [CITED: parallelly docs]. Low risk on a standard Euler install. Detectable via dev task. |
| A4 | Euler nodes use cgroup v1 OR v2; `/sys/fs/cgroup/memory.current` (v2) is the right path | §Per-worker RAM budget | [ASSUMED]. Mitigated by `prof_cgroup_snapshot()` trying both paths. |
| A5 | Forks inherit `RhpcBLASctl::blas_set_num_threads(1)` state | §Native thread pinning | [CITED: RhpcBLASctl docs — "parallel library retains BLAS settings of the parent"]. Verified in docs; low risk. |
| A6 | `terra::writeRaster(... gdal=c("NUM_THREADS=1", ...))` correctly pins GDAL threads per call | §Path-based raster passing | [CITED: GDAL docs]. Low risk; verified at GDAL level. |
| A7 | Coast is the smallest region of the three; suitable for smoke run | §Smoke run definition | [ASSUMED]. If wrong, smoke run runs longer than estimated but still completes. Cheap to verify by running all 3 and timing. |
| A8 | `restore_ranger_importance_mode()` is a no-op for mlr3-trained models (ranger fit set explicitly to importance="none") | §Models pre-load | [VERIFIED via codebase grep — Phase 2 build_mlr3_learner sets importance="none" explicitly]. Confirmed safe to call as a pass-through. |
| A9 | Dev gate (`future.globals.onReference="error"`) catches every externalptr risk relevant to Phase 3 | §future.globals.onReference enablement | [CITED: future docs explicitly state this is the purpose]. Known false positives (data.table) documented. Low risk. |

---

## Code Examples

Verified patterns from this research session. Each block is ready to drop into the codebase with minor adaptation.

### Plan selector (Q1)
```r
# Source: this research, derived from parallelly + future docs
# (https://parallelly.futureverse.org/reference/supportsMulticore.html)
select_allocation_plan <- function() {
  override <- suppressWarnings(as.integer(
    Sys.getenv("ALLOCATION_NUM_WORKERS", unset = NA_character_)
  ))
  workers <- if (!is.na(override) && override > 0L) override
             else if (requireNamespace("parallelly", quietly = TRUE)) parallelly::availableCores()
             else max(1L, parallel::detectCores() - 1L)

  if (identical(tolower(Sys.getenv("ALLOCATION_PARALLEL_STRATEGY", "")), "sequential")) {
    return(list(strategy = "sequential", workers = 1L))
  }
  forced <- tolower(Sys.getenv("ALLOCATION_PARALLEL_STRATEGY", ""))
  if (forced %in% c("multicore", "multisession")) {
    return(list(strategy = forced, workers = workers))
  }
  if (requireNamespace("parallelly", quietly = TRUE) && parallelly::supportsMulticore()) {
    list(strategy = "multicore", workers = workers)
  } else {
    list(strategy = "multisession", workers = workers)
  }
}
```

### Native thread pinning (Q6)
```r
# Source: this research, derived from RhpcBLASctl + arrow + data.table docs
pin_native_threads_to_one <- function(verbose = FALSE) {
  Sys.setenv(
    OMP_NUM_THREADS         = "1",
    OPENBLAS_NUM_THREADS    = "1",
    MKL_NUM_THREADS         = "1",
    GOTO_NUM_THREADS        = "1",
    GDAL_NUM_THREADS        = "1",
    R_DATATABLE_NUM_THREADS = "1"
  )
  if (requireNamespace("RhpcBLASctl", quietly = TRUE)) {
    try(RhpcBLASctl::blas_set_num_threads(1L), silent = !verbose)
    try(RhpcBLASctl::omp_set_num_threads(1L), silent = !verbose)
  }
  if (requireNamespace("data.table", quietly = TRUE)) data.table::setDTthreads(1L)
  if (requireNamespace("arrow", quietly = TRUE)) {
    try(arrow::set_cpu_count(1L), silent = !verbose)
    try(arrow::set_io_thread_count(1L), silent = !verbose)
  }
  invisible(NULL)
}
```

### Parent-side nhood TIF write (Q3)
```r
# Source: this research, derived from terra writeRaster + GDAL GTiff docs
write_nhood_tif <- function(anterior_path, pred_name, focal_matrices,
                            class_name_to_value, out_path) {
  anterior <- terra::rast(anterior_path)
  rast <- compute_single_nhood_raster(
    anterior = anterior, pred_name = pred_name,
    focal_matrices = focal_matrices, class_name_to_value = class_name_to_value
  )
  terra::writeRaster(
    rast, out_path, overwrite = TRUE,
    datatype = "FLT4S",
    gdal = c("COMPRESS=LZW", "TILED=YES",
             "BLOCKXSIZE=256", "BLOCKYSIZE=256",
             "BIGTIFF=IF_SAFER", "NUM_THREADS=1")
  )
  rm(rast, anterior); gc(verbose = FALSE)
  out_path
}
```

### Dev-gate enablement (Q9)
```r
# Source: this research, derived from future-4-non-exportable-objects vignette
# In scripts/run_allocation.r, before future::plan():
if (isTRUE(as.logical(Sys.getenv("ALLOCATION_DEV_STRICT_GLOBALS", "FALSE")))) {
  options(future.globals.onReference = "error")
  cat("DEV MODE: future.globals.onReference = 'error' enabled\n")
}
```

### Cgroup snapshot (Q5)
```r
# Source: this research, cgroup v1+v2 path probing
prof_cgroup_snapshot <- function(tag, log_file) {
  paths_v2 <- list(current = "/sys/fs/cgroup/memory.current",
                   max     = "/sys/fs/cgroup/memory.max")
  paths_v1 <- list(current = "/sys/fs/cgroup/memory/memory.usage_in_bytes",
                   max     = "/sys/fs/cgroup/memory/memory.limit_in_bytes")
  p <- if (file.exists(paths_v2$current)) paths_v2
       else if (file.exists(paths_v1$current)) paths_v1
       else return(invisible(NULL))
  cur <- as.numeric(readLines(p$current, warn = FALSE)) / 1024^2
  max_raw <- if (file.exists(p$max)) readLines(p$max, warn = FALSE) else "unknown"
  max <- if (identical(max_raw, "max") || identical(max_raw, "unknown")) NA_real_
         else as.numeric(max_raw) / 1024^2
  log_msg(sprintf("CGROUP %s memory.current=%.1fMB memory.max=%sMB",
                  tag, cur, if (is.na(max)) "unlimited" else sprintf("%.1f", max)),
          log_file)
  invisible(list(current_mb = cur, max_mb = max))
}
```

---

## Sources

### Primary (HIGH confidence — verified during this research session)

- **Codebase (HIGH):**
  - `src/allocation.r:42-1798` — current allocation orchestration, profiling helpers, predict dispatch, generate_probability_maps, nhood cache
  - `src/utils.r:1030-1163` — worker_state + initialize_worker_log (Phase 1)
  - `src/transition_modelling.r:113-402` — Phase 2 mlr3 build_mlr3_learner + train_mlr3_transition + qs save format
  - `scripts/run_allocation.r:218-220` — current `future::plan(multisession)` line
  - `scripts/submit_allocation.sh` — current SLURM contract (`--cpus-per-task=8 --mem-per-cpu=8G`, ALLOCATION_NUM_WORKERS)
  - `src/calibrate_allocation_parameters.r:780-820` — inverted "multicore causes OOM" comment
  - `environments/allocation_env.yml` — full package list, no gaps for Phase 3
  - `.planning/STATE.md` — locked Phase 3 direction
  - `.planning/research/SUMMARY.md`, `ARCHITECTURE.md`, `PITFALLS.md` — project-level research
  - `.planning/phases/02-model-size-reduction/02-RESEARCH.md` — Phase 2 risk register, mlr3 marshal-property note

- **Official docs (HIGH):**
  - [parallelly supportsMulticore](https://parallelly.futureverse.org/reference/supportsMulticore.html) — return values by OS, RStudio detection, parallelly.fork.enable mechanism
  - [parallelly availableCores](https://parallelly.futureverse.org/reference/availableCores.html) — SLURM_CPUS_PER_TASK detection
  - [future non-exportable-objects vignette](https://future.futureverse.org/articles/future-4-non-exportable-objects.html) — purpose of `future.globals.onReference="error"`, default value, terra/arrow/raster as flagged classes, data.table false positive
  - [GDAL GTiff driver](https://gdal.org/en/stable/drivers/raster/gtiff.html) — COMPRESS, BLOCKXSIZE, BIGTIFF, NUM_THREADS defaults
  - [SLURM users discussion — OOM-killer signals](https://groups.google.com/g/slurm-users/c/6YOBsfrD6Vs) — cgroup OOM sends SIGKILL (uncatchable)
  - [RhpcBLASctl CRAN package](https://cran.r-project.org/web/packages/RhpcBLASctl/RhpcBLASctl.pdf) — fork inheritance of BLAS thread state

### Secondary (MEDIUM confidence — single source or inference)

- [mlr3 Learner reference](https://mlr3.mlr-org.com/reference/Learner.html) — marshal property exists for some learners; xgboost may or may not declare it (verified absent in 0.14 per Phase 2 RESEARCH but not re-verified this session)
- [mlr3 marshaling reference](https://mlr3.mlr-org.com/reference/marshaling.html) — marshal_model()/unmarshal_model() API; behaviour for specific learners not documented in this page
- [terra serialize reference](https://rdrr.io/cran/terra/man/serialize.html) — SpatRaster serialize writes source path

### Tertiary (LOW confidence — informational only)

- [futureverse discussions on SLURM](https://github.com/HenrikBengtsson/future/discussions/468) — general guidance, no specific multicore-on-SLURM authoritative claims relied upon
- [terra issue #96](https://github.com/rspatial/terra/issues/96) — confirms SpatRaster + clusterR serialization gotcha (background reference)

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — every package is already in `allocation_env.yml`; APIs are stable.
- Architecture: HIGH — fork+COW vs PSOCK is well-understood; parallelly::supportsMulticore() is the right contract.
- Pitfalls: HIGH — every pitfall is grounded in either codebase audit or cited docs.
- mlr3 xgboost marshal under strict-globals: MEDIUM — relies on Phase 2 RESEARCH §A4; verifiable via dev gate.
- Per-worker memory budget: MEDIUM — depends on Phase 2 actual model sizes; verifiable via smoke run.

**Research date:** 2026-05-11
**Valid until:** 2026-06-10 (30 days; future/parallelly/terra are stable; recheck only if mlr3learners or xgboost major-version bump)
