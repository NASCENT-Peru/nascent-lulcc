# Phase 3: Parallelism & Memory Architecture - Pattern Map

**Mapped:** 2026-05-11
**Inputs:** `.planning/ROADMAP.md`, `.planning/REQUIREMENTS.md`, `.planning/STATE.md`, `.planning/phases/03-parallelism-memory-architecture/03-RESEARCH.md`
**Files analyzed:** 3 primary code files, 2 supporting analog files, 1 existing phase-pattern analog
**Analogs found:** 5 / 5

Phase 3 has no `CONTEXT.md` by design. Scope comes from Phase 3 in `ROADMAP.md`, MEM-01/MEM-02/MEM-03/MEM-05 in `REQUIREMENTS.md`, the locked carry-forward decisions in `STATE.md`, and the implementation guidance in `03-RESEARCH.md`.

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `src/allocation.r` - runtime helpers (`select_allocation_plan`, `pin_native_threads_to_one`, `prof_cgroup_snapshot`, model/nhood preload helpers) | service / utility | request-response + file-I/O | `src/allocation.r` helper block at lines 42-356 | exact |
| `src/allocation.r` - region worker orchestration | service | event-driven + file-I/O | `run_allocation_one_timestep()` at lines 887-1045 | exact |
| `src/allocation.r` - probability-map generation refactor | service | transform + file-I/O | `generate_probability_maps()` at lines 1252-1735 | exact |
| `scripts/run_allocation.r` | entry script / orchestrator | request-response | existing pre-flight + `future::plan()` block at lines 188-220 | exact |
| `src/calibrate_allocation_parameters.r` (optional cleanup) | service / orchestrator | request-response | existing parallel selector at lines 786-825 | exact |

## Pattern Assignments

### `src/allocation.r` helper block

**Analog:** `src/allocation.r` lines 42-356

This file already groups cross-cutting helpers near the top:
- `.read_proc_status()` at lines 64-98
- `.reset_vmhwm()` at lines 103-119
- `prof_tic()` / `prof_toc()` / `prof_mem_summary()` at lines 121-190
- `validate_allocation_runtime()` at lines 221-328
- `%||%` and model helpers at lines 332-356

**Pattern to copy:** add new Phase 3 helpers beside these helpers, not in `scripts/run_allocation.r`.

**Imports / dependency style**
Use `requireNamespace(..., quietly = TRUE)` guards instead of unconditional package attachment.

From `validate_allocation_runtime()` lines 280-287:
```r
ok <- suppressWarnings(requireNamespace(pkg, quietly = TRUE))
if (!isTRUE(ok)) {
  errors <- c(errors, sprintf(
    "package: %s not installed (pre-flight does not install - fix the env file)",
    pkg
  ))
}
```

**Env-var lookup convention**
Use `Sys.getenv(..., unset = ...)` with explicit defaults.

From `validate_allocation_runtime()` lines 243-265:
```r
env_expected <- c("HPC_SCRATCH_ROOT", "HPC_TMP_ROOT", "TERRA_TEMP",
                  "DINAMICA_EGO_8_HOME")
dinamica_backend <- Sys.getenv("DINAMICA_BACKEND", unset = "auto")
```

Apply this to:
- `ALLOCATION_NUM_WORKERS`
- `ALLOCATION_PARALLEL_STRATEGY`
- `ALLOCATION_DEV_STRICT_GLOBALS`

**Best-effort host mutation pattern**
Copy the `.reset_vmhwm()` shape for `pin_native_threads_to_one()` and `prof_cgroup_snapshot()`:
```r
if (!file.exists(cf)) {
  return(invisible(FALSE))
}
ok <- tryCatch(
  {
    ...
    TRUE
  },
  error = function(e) FALSE,
  warning = function(w) FALSE
)
```

**Profiling/logging convention**
Use `prof_tic()` / `prof_toc()` and `log_msg()`, not ad hoc `message()`, for parent or worker stages that should land in logs.

From `prof_toc()` lines 133-170:
```r
log_msg(
  sprintf(
    paste0(
      "PROFILE %s elapsed=%.3fs rss_before=%.1fMB rss_after=%.1fMB ",
      "rss_delta=%+.1fMB peak_rss=%.1fMB ..."
    ),
    tag, dt, t0$mem$rss, after$rss, rss_delta, after$vmhwm
  ),
  log_file
)
```

**Where new helpers should live**
- `pin_native_threads_to_one()` next to `.reset_vmhwm()`
- `prof_cgroup_snapshot()` next to `prof_mem_summary()`
- `select_allocation_plan()` next to `validate_allocation_runtime()`
- model preload / nhood preload helpers between `predict_saved_transition_prob()` and `generate_probability_maps()`

### `src/allocation.r` region worker orchestration

**Analog:** `run_allocation_one_timestep()` at lines 887-1045

This is the planner's primary analog for Phase 3 structure. Preserve the outer shell and refactor what gets prepared before workers start.

**Current worker lifecycle pattern**
```r
posterior_paths <- furrr::future_map(
  seq_along(region_names),
  function(idx) {
    ...
    log_file <- initialize_worker_log(
      file.path(region_work_dir, "worker_logs"),
      region_suffix
    )
    worker_state_init(...)
    on.exit({
      worker_state_flush_sentinel(log_file, reason = sentinel_reason)
    }, add = TRUE)
    worker_state_set(stage = "region_setup", log_file = log_file)
    ...
    worker_state_set(stage = "setup_inputs", log_file = log_file)
    ...
    worker_state_set(stage = "dinamica_launch", log_file = log_file)
    ...
  },
  .options = furrr::furrr_options(seed = TRUE)
)
```

**Pattern to preserve**
- Worker log file is created inside the worker with `initialize_worker_log()` from `src/utils.r:1030-1038`
- Breadcrumbs stay inside the worker via `worker_state_init`, `worker_state_set`, `worker_state_flush_sentinel`
- `prof_toc()` tags use `region=%s stage=%s`

**Phase 3 change pattern**
- Parent prepares fork-safe globals first
- Workers receive plain R objects and character paths only
- Workers keep opening `terra::rast()` and `arrow::open_dataset()` inside the worker

**Do not move into parent**
- `initialize_worker_log()` from `src/utils.r:1030-1038`
- `worker_state_*` lifecycle
- `terra::rast(region_rast_path)` / `terra::rast(current_lulc_path)`
- `arrow::open_dataset(...)` in `generate_probability_maps()`

### `src/allocation.r` probability-map generation

**Analog:** `generate_probability_maps()` at lines 1252-1735

This is where the Phase 3 memory shape changes.

**Current model-load pattern to replace**
Lines 1529-1545:
```r
t_model_load <- prof_tic()
fitted_wf <- if (grepl("\\.qs$", mi$file_path, perl = TRUE)) {
  qs::qread(mi$file_path)
} else {
  readRDS(mi$file_path)
}
prof_toc(
  t_model_load,
  sprintf(
    "region=%s stage=model_load from=%d to=%d",
    region_suffix, from_val, to_val
  ),
  log_file
)
```

**Current neighbourhood anti-pattern to replace**
Lines 1427-1454:
```r
nhood_raster_cache <- new.env(parent = emptyenv())
focal_matrices <- NULL
get_nhood_raster <- function(pred_name) {
  ...
  rast <- compute_single_nhood_raster(
    anterior = anterior,
    pred_name = pred_name,
    focal_matrices = focal_matrices,
    class_name_to_value = class_name_to_value
  )
  assign(pred_name, rast, envir = nhood_raster_cache)
}
```

**Worker-side raster extraction pattern to keep, but switch to paths**
Lines 1591-1616:
```r
t_nhood_extract <- prof_tic()
nhood_stack <- terra::rast(lapply(nhood_needed, get_nhood_raster))
nhood_vals <- terra::extract(
  nhood_stack,
  as.matrix(from_data[, .(x, y)])
)
```

Phase 3 should preserve the `terra::extract()` flow but replace `get_nhood_raster` with a named path lookup:
```r
nhood_stack <- terra::rast(nhood_paths[nhood_needed])
```

**Existing helper to reuse**
`compute_single_nhood_raster()` at lines 1759-1798 stays the core computation. Add a wrapper that writes its result to TIF; do not rewrite the focal logic.

**Raster write convention**
Copy the existing `terra::writeRaster()` style from `run_allocation_one_timestep()` lines 965-973:
```r
terra::writeRaster(
  lulc_region,
  anterior_path,
  overwrite = TRUE,
  wopt = list(datatype = "INT2U", gdal = c("COMPRESS=LZW"))
)
```

Use the same style for parent-side neighbourhood TIF materialization, with Phase 3's stronger GDAL options from research.

**Arrow pattern to preserve**
Lines 1409-1421:
```r
ds_static <- arrow::open_dataset(...)
ds_dynamic <- arrow::open_dataset(...)
```
These stay inside the worker. Do not hoist them into parent preload helpers.

### `scripts/run_allocation.r`

**Analog:** `scripts/run_allocation.r` lines 188-220

This is the script-order analog the planner should copy.

**Current sequence**
```r
preflight_exit <- run_preflight_and_print(config = config)
if (preflight_exit != 0L) {
  quit(status = preflight_exit)
}

num_workers <- as.integer(Sys.getenv("ALLOCATION_NUM_WORKERS", unset = "4"))
cat(sprintf("Setting up parallel processing with %d workers\n", num_workers))
future::plan(future::multisession, workers = num_workers)
```

**Phase 3 sequence to preserve**
Keep everything before this block intact, then replace the hard-coded `multisession` block with:
1. pre-flight
2. thread pinning
3. optional strict-globals dev gate
4. plan selection
5. `future::plan(...)`

**Console-output convention**
Use `cat(...)` in the entry script, not `log_msg(...)`, before worker logs exist. This matches the rest of the script's startup diagnostics.

**Cleanup pattern to preserve**
Keep the existing end-of-script reset:
```r
future::plan(future::sequential)
```

### `src/calibrate_allocation_parameters.r` (optional but strong analog)

**Analog:** `src/calibrate_allocation_parameters.r` lines 786-825

Current anti-pattern:
```r
if (n_cores > 1) {
  # ALWAYS use multisession (even on Unix/HPC)
  # multicore uses forking which can cause OOM issues on HPC clusters
  future::plan(future::multisession, workers = n_cores)
  strategy <- "multisession"
  ...
}
```

If the planner includes this file in Phase 3, reuse the same `select_allocation_plan()` helper rather than duplicating OS-selection logic here. If it is out of scope, explicitly note it as follow-up because the current comment contradicts the locked Phase 3 direction in `STATE.md`.

## Shared Patterns

### Logging and breadcrumbs
**Source:** `src/utils.r` lines 1011-1163
**Apply to:** all worker-side Phase 3 changes

Relevant excerpts:
```r
log_msg <- function(msg, log_file = NULL, also_console = TRUE) { ... }
initialize_worker_log <- function(log_dir, trans_name) { ... }
worker_state_init <- function(...) { ... }
worker_state_set <- function(stage, transition = NA_character_, log_file = NULL) { ... }
worker_state_flush_sentinel <- function(log_file, reason = "unknown") { ... }
```

Planner guidance:
- keep worker log creation in worker
- keep `STATE`/`SENTINEL` coverage through new Phase 3 stages
- any new long-running worker step should get `worker_state_set(...)` before it

### Profiling hooks
**Source:** `src/allocation.r` lines 121-190
**Apply to:** parent preload stages and worker memory-sensitive stages

Use:
```r
t0 <- prof_tic()
...
prof_toc(t0, sprintf("region=%s stage=%s", region_suffix, stage_name), log_file)
prof_mem_summary(sprintf("region=%s", region_suffix), log_file)
```

Recommended new stage tags for planner acceptance criteria:
- `stage=pin_threads`
- `stage=preload_models`
- `stage=nhood_precompute`
- `stage=parent_baseline`
- `stage=nhood_extract`

### Phase 2 model contract
**Source:** `src/transition_modelling.r` lines 146-182, 367-392
**Apply to:** parent model preload in Phase 3

Relevant excerpts:
```r
lrn_obj <- mlr3::lrn("classif.ranger",
  predict_type = "prob",
  importance   = "none",
  save.memory  = TRUE,
  num.threads  = 1L
)

lrn_obj <- mlr3::lrn("classif.xgboost",
  predict_type = "prob",
  nthread      = 1L
)

model_check <- qs::qread(output_path)
pred_check <- model_check$learner$predict_newdata(newdata = fixture_rows)
prob_check <- pred_check$prob[, "1"]
```

Planner guidance:
- workers should assume mlr3 `.qs` artifacts are the main case
- use column name `"1"` for probability lookup, never positional indexing
- preserve single-thread native learner settings; Phase 3 adds process-level pinning around them

## Anti-Patterns / Landmines

### Do not replicate worker-local in-memory nhood cache
**Source to remove:** `src/allocation.r` lines 1427-1454

Why:
- duplicates raster work per worker
- preserves in-memory `SpatRaster` objects instead of file paths
- directly conflicts with MEM-05

### Do not hoist `SpatRaster` or Arrow dataset objects into parent globals
Risk areas:
- `region_rast`, `current_lulc`, `anterior`
- `ds_static`, `ds_dynamic`

Why:
- these are the exact objects likely to trip `future.globals.onReference = "error"`
- the research explicitly wants only character paths and plain R structures crossing the boundary

### Do not move `initialize_worker_log()` to the parent
**Source:** `src/utils.r` lines 1030-1038

Why:
- filename uniqueness depends on `Sys.getpid()`
- under `multicore`, the worker PID differs from the parent PID

### Do not duplicate plan-selection logic in multiple files
Add `select_allocation_plan()` once in `src/allocation.r`, then call it from scripts and any optional calibration cleanup. Avoid hand-rolled OS checks in multiple places.

### Do not reintroduce runtime environment healing
`scripts/run_allocation.r` already follows the Phase 1 contract: pre-flight validates, scripts do not install packages. Keep that intact.

## Plan Structure and Verification Analogs

Use these existing artifacts as planner-shape analogs:

### Pattern file shape
**Analog:** `.planning/phases/02-model-size-reduction/02-PATTERNS.md`

Why it matters:
- same single-phase pattern-map format
- uses self-analogs where helpers are inserted into existing files
- records landmines explicitly instead of only listing files

### Verification hook analog
**Analog:** `.planning/phases/01-repair-visibility/01-03-SUMMARY.md`

Copy these planning habits:
- verify by grepable log signatures, not just code diff
- define exact stage tags the operator can grep for
- keep script-level order assertions explicit
- use worker log artifacts and summary logs as acceptance evidence

Concrete verification hooks Phase 3 should plan around:
- `future::plan(...)` choice visible in `scripts/run_allocation.r`
- `options(future.globals.onReference = "error")` dev toggle
- one `PROFILE ... stage=preload_models` line before any worker predict work
- no `qs::qread(...)` inside the per-transition loop once preload is implemented
- worker-side nhood extraction uses `terra::rast(nhood_paths[...])`, not `get_nhood_raster(...)`

## No Analog Found

None. Phase 3 is a refactor phase inside existing R orchestration, not a greenfield addition. Every planned helper has a strong local analog.

## Metadata

**Analog search scope:** `src/`, `scripts/`, `.planning/phases/01-repair-visibility/`, `.planning/phases/02-model-size-reduction/`

**Files scanned:**
- `src/allocation.r`
- `scripts/run_allocation.r`
- `src/utils.r`
- `src/calibrate_allocation_parameters.r`
- `src/transition_modelling.r`
- `.planning/phases/02-model-size-reduction/02-PATTERNS.md`
- `.planning/phases/01-repair-visibility/01-03-SUMMARY.md`

**Pattern extraction date:** 2026-05-11
