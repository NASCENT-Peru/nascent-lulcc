# Phase 3: Parallelism & Memory Architecture — Pattern Map

**Mapped:** 2026-05-11
**Files analyzed:** 7 (3 source files modified, 1 script modified, 0 new files — all helpers land in `src/allocation.r` per RESEARCH §"Wave 0 Gaps")
**Analogs found:** 7 / 7

This phase is a **self-analog** phase: every new helper is inserted into `src/allocation.r` next to its sibling helpers, and the call site changes live in `scripts/run_allocation.r`. There is no new file. The helpers RESEARCH proposed (`select_allocation_plan()`, `pin_native_threads_to_one()`, `prof_cgroup_snapshot()`, `load_all_region_models()`, plus a path-writing wrapper around `compute_single_nhood_raster()`) all follow conventions established in Phase 1 (`.read_proc_status`, `validate_allocation_runtime`, `prof_*` helpers) and Phase 2 (`build_mlr3_learner`, `train_mlr3_transition`).

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `src/allocation.r` — new `select_allocation_plan()` | parallel-control helper | request-response (pure decision tree) | `validate_allocation_runtime()` @ `src/allocation.r:221-328` (env-driven, returns plain data, no side effects on host) | role-match (config-resolver pattern) |
| `src/allocation.r` — new `pin_native_threads_to_one()` | env-mutation helper | request-response (idempotent setter) | `.reset_vmhwm()` @ `src/allocation.r:103-119` (best-effort env/file mutation with `requireNamespace()` guards and silent fallback) | role-match (idempotent setter) |
| `src/allocation.r` — new `prof_cgroup_snapshot()` | observability helper | file-I/O | `.read_proc_status()` @ `src/allocation.r:64-98` + `prof_mem_summary()` @ `src/allocation.r:175-190` | exact (procfs/cgroup reader pattern; per-tag log line) |
| `src/allocation.r` — new `load_all_region_models()` | model-loading helper | batch | `train_mlr3_transition()` @ `src/transition_modelling.r:221-402` (mlr3+qs round-trip; loop over files; structured returns; log_msg) | role-match (file-list → loaded list with progress logging) |
| `src/allocation.r` — new `write_nhood_tif()` wrapper + edit to `run_allocation_one_timestep()` worker body | raster-IO helper + allocation orchestration | file-I/O + request-response | `compute_single_nhood_raster()` @ `src/allocation.r:1759-1798` (existing terra::focal core); `terra::writeRaster` use @ `src/allocation.r:968-973` (anterior write pattern) | exact (parent-side TIF write; worker-side `terra::rast(path)`) |
| `scripts/run_allocation.r` — replace lines 218-220 | entry-point orchestration | request-response | itself; the pre-flight gate insertion @ lines 208-216 is the immediate sibling pattern | self-analog (additive ordered block) |
| `src/calibrate_allocation_parameters.r:780-820` — replace the inverted "multicore causes OOM" selector (scope-optional per RESEARCH §Q1) | parallel-control helper | request-response | new `select_allocation_plan()` (once landed in `src/allocation.r`) | exact (use the same helper) |

---

## Pattern Assignments

### `src/allocation.r` — new helper: `select_allocation_plan()` (parallel-control)

**Analog:** `validate_allocation_runtime()` at `src/allocation.r:221-328` — same shape (pure helper, returns a small data structure, env-var-driven, no side effects on `future::plan()` itself; caller decides what to do with the result).

**Imports / prologue convention** (from the top of the existing helper block at `src/allocation.r:42-44`):

```r
# Short docstring comment block above the function definition.
# Profiling helpers (opt-in via ALLOCATION_PROFILE env var).
# When the env var is unset/FALSE, prof_tic() returns NULL ...
```

The new helper should be inserted **between `validate_allocation_runtime()` (line 328) and the `%||%` definition (line 332)** — that block already groups Stage 7 pre-flight surface area; `select_allocation_plan()` is the natural sibling because it's also called from `scripts/run_allocation.r` after pre-flight passes.

**Env-var override pattern to copy** (lines 262-268, `validate_allocation_runtime` Dinamica backend resolution):

```r
dinamica_backend <- Sys.getenv("DINAMICA_BACKEND", unset = "auto")
dinamica_artifact <- Sys.getenv("DINAMICA_EGO_8_HOME", unset = "")
dinamica_expected <- list(
  backend = dinamica_backend,
  runtime = if (identical(dinamica_backend, "hpc")) "apptainer" else "DinamicaConsole",
  artifact = dinamica_artifact
)
```

Apply the same `Sys.getenv(..., unset = ...)` + `if (identical(tolower(...), "..."))` idiom for the `ALLOCATION_NUM_WORKERS` / `ALLOCATION_PARALLEL_STRATEGY` overrides described in RESEARCH §Q1.

**`requireNamespace()` guard pattern to copy** (lines 280-287):

```r
for (pkg in packages_expected) {
  ok <- suppressWarnings(requireNamespace(pkg, quietly = TRUE))
  if (!isTRUE(ok)) {
    errors <- c(errors, sprintf(
      "package: %s not installed (pre-flight does not install — fix the env file)",
      pkg
    ))
  }
}
```

`select_allocation_plan()` must `requireNamespace("parallelly", quietly = TRUE)` and fall back to `parallel::detectCores() - 1L` if absent — never `stop()` on missing parallelly. That keeps the helper callable from environments where the operator skipped pre-flight (e.g., debugger sessions, the Windows local dev box where parallelly may still be loadable but `supportsMulticore()` returns FALSE anyway).

**Return-shape convention** (lines 327, `validate_allocation_runtime` returns `errors` vector):

The helper returns a named list `list(strategy = "multicore"|"multisession"|"sequential", workers = <int>)` — explicitly NOT calling `future::plan()` itself. The caller (`scripts/run_allocation.r`) does the dispatch. This mirrors how `validate_allocation_runtime()` returns errors and lets the caller decide whether to `stop()`.

**Verbatim source ready to drop** (RESEARCH §"Code Examples — Plan selector" lines 1028-1048):

```r
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

---

### `src/allocation.r` — new helper: `pin_native_threads_to_one()` (env-mutation helper)

**Analog:** `.reset_vmhwm()` at `src/allocation.r:103-119` — best-effort write to a system surface (procfs `clear_refs`), wrapped in `tryCatch()`, silently no-ops on unsupported platforms, returns invisible status.

**Best-effort mutation pattern to copy** (lines 103-119):

```r
.reset_vmhwm <- function() {
  cf <- "/proc/self/clear_refs"
  if (!file.exists(cf)) {
    return(invisible(FALSE))
  }
  ok <- tryCatch(
    {
      con <- file(cf, "w")
      on.exit(close(con), add = TRUE)
      writeLines("5", con)
      TRUE
    },
    error = function(e) FALSE,
    warning = function(w) FALSE
  )
  invisible(ok)
}
```

For `pin_native_threads_to_one()` the analog is the per-package `try(...)` guards — silent on platforms where `RhpcBLASctl`/`arrow`/`data.table` is missing, but the env-var step (`Sys.setenv`) always runs first because it's a pure built-in. Place the helper **immediately after `.reset_vmhwm()` and before `prof_tic()` (line 121)** — both helpers live in the same "host introspection / mutation" block.

**Idempotency contract** (RESEARCH §Pitfall 2): the helper must be safe to call twice. The env-var sets are idempotent; the `RhpcBLASctl::blas_set_num_threads(1L)` / `arrow::set_cpu_count(1L)` / `data.table::setDTthreads(1L)` calls all overwrite a single global — also idempotent. No internal state to track.

**Verbose-flag convention** (lines 554-563 of RESEARCH §Q6 verbose block):

```r
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
```

Uses `cat()` (stdout — captured by SLURM `.out`) rather than `log_msg()` because this runs **before** `future::plan()` and any per-region log file exists. This matches the `cat()` usage in `scripts/run_allocation.r:50-61` where startup diagnostics are emitted before the worker log infrastructure boots.

**Verbatim source ready to drop** (RESEARCH §"Code Examples — Native thread pinning" lines 1053-1073).

---

### `src/allocation.r` — new helper: `prof_cgroup_snapshot()` (observability)

**Analog:** `.read_proc_status()` at `src/allocation.r:64-98` (procfs reader with portable fallback) and `prof_mem_summary()` at `src/allocation.r:175-190` (one-shot summary line emitter).

**Procfs path-existence + portable-read pattern to copy** (lines 67-98):

```r
.read_proc_status <- function() {
  rss <- NA_real_
  vsize <- NA_real_
  if (requireNamespace("ps", quietly = TRUE)) {
    info <- tryCatch(ps::ps_memory_info(), error = function(e) NULL)
    if (!is.null(info)) {
      r <- unname(info[["rss"]])
      ...
    }
  }
  vmhwm <- NA_real_
  if (file.exists("/proc/self/status")) {
    lines <- readLines("/proc/self/status", warn = FALSE)
    parse_kb <- function(prefix) { ... }
    if (is.na(rss)) rss <- parse_kb("VmRSS")
    if (is.na(vsize)) vsize <- parse_kb("VmSize")
    vmhwm <- parse_kb("VmHWM")
  }
  list(rss = rss, vmhwm = vmhwm, vsize = vsize)
}
```

The cgroup helper applies the same `file.exists()`-then-`readLines()` shape, **and** the same try-v2-then-v1 fallback discipline (mirroring how `.read_proc_status` falls back to `/proc` when `ps` is missing).

**Per-tag log-line convention to copy** (`prof_mem_summary()`, lines 175-190):

```r
prof_mem_summary <- function(tag, log_file = NULL) {
  if (!.profile_on()) {
    return(invisible(NULL))
  }
  m <- .read_proc_status()
  log_msg(
    sprintf(
      "PROFILE_MEM %s summary rss=%.1fMB peak_rss=%.1fMB",
      tag, m$rss, m$vmhwm
    ),
    log_file
  )
  invisible(m)
}
```

The cgroup helper follows the **same signature shape**: `(tag, log_file = NULL)` → emits one `CGROUP <tag> memory.current=...MB memory.max=...MB` line, returns `invisible(list(current_mb=, max_mb=))`. Differences from `prof_mem_summary`:

1. **Not gated on `.profile_on()`** — cgroup observability is cheap and load-bearing for success-criterion 3 verification, so it runs always (RESEARCH §Q5).
2. **`log_msg(..., log_file)` lands the line in the per-region log** alongside `PROFILE …` lines so `diagnose_alloc_crash.sh` can grep both. Use the existing `log_msg()` from `src/utils.r:1016-1024` (unchanged).

**Verbatim source ready to drop** (RESEARCH §"Code Examples — Cgroup snapshot" lines 1110-1127). Place this helper **immediately after `prof_mem_summary()` (line 190)** — same block, same shape.

---

### `src/allocation.r` — new helper: `load_all_region_models()` (model-loading)

**Analog 1:** `train_mlr3_transition()` at `src/transition_modelling.r:221-402` — mlr3+qs file I/O, structured returns, `log_msg` for progress.

**Analog 2:** the per-transition inline `qs::qread` block at `src/allocation.r:1530-1545` — current pattern that this helper replaces.

**Existing inline qs::qread pattern to lift into the parent** (lines 1530-1545):

```r
# Load model (we need its predictor names anyway, so load once + use
# immediately + release at end of iteration).
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

The helper consolidates this into a **single parent-side call per region**:

- One `prof_tic()` / `prof_toc()` pair for the whole region's batch (tag: `region=%s stage=preload_models`).
- File-list discovery via `list.files(model_dir, pattern = sprintf(".*_%s\\.qs$", region_suffix), full.names = TRUE)` — analog to how `transition_modelling.r:103-111` builds region-suffixed paths.

**`log_msg` progress pattern to copy** (transition_modelling.r line 305):

```r
log_msg(sprintf("  Training %s learner...", algo), log_file)
```

For the loader: `log_msg(sprintf("Loading %d mlr3 models for region %s...", length(model_files), region_suffix), log_file)` before the load loop, and a `prof_toc()` after.

**Error pattern when no files match** (transition_modelling.r-style `stop()` is acceptable here because this runs in the parent BEFORE any fork — see RESEARCH §"One-time models_list load"):

```r
if (length(model_files) == 0L) {
  stop(sprintf("No mlr3 .qs models found for region %s in %s",
               region_suffix, model_dir))
}
```

This `stop()` propagates up to `run_allocation_one_timestep()`'s caller (`run_allocation()`), which already wraps the whole scenario loop in a `tryCatch` at `scripts/run_allocation.r:227-238`. Inside a worker we'd return an error list (per `perform_transition_modelling()` lines 761-801 convention); inside the parent before fork, `stop()` is correct.

**`restore_ranger_importance_mode()` mutation pre-fork** (RESEARCH §"Models pre-load" + Pitfall 4): after loading the list, the helper must apply `lapply(models_list, restore_ranger_importance_mode)` in the parent so the mutation happens BEFORE fork → no COW page copy in workers. The function at `src/allocation.r:343-356` is a no-op for mlr3-trained models (their `$fit` does not inherit from "ranger" via the legacy slot), so the call is safe.

**Verbatim source skeleton (combining RESEARCH §"Models pre-load" with the existing conventions):**

```r
# Parent-side pre-load of mlr3 .qs models for one region. Called from
# run_allocation_one_timestep() BEFORE future::plan() / future_map().
# Models are loaded once and shared across the region's forked workers via
# copy-on-write (multicore) or serialised once into PSOCK workers (multisession).
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
  # Pre-fork mutation (avoids COW page copy inside workers — RESEARCH Pitfall 4)
  models_list <- lapply(models_list, restore_ranger_importance_mode)
  prof_toc(t0, sprintf("region=%s stage=preload_models", region_suffix), log_file)
  models_list
}
```

Place between `predict_saved_transition_prob()` (ends ~line 700) and `generate_probability_maps()` (around line 1280) — the "service helpers for `run_allocation_one_timestep()`" zone.

---

### `src/allocation.r` — new helper: `write_nhood_tif()` + edit to `generate_probability_maps()` worker

**Analog:** `compute_single_nhood_raster()` at `src/allocation.r:1759-1798` (existing in-memory focal computation) + `terra::writeRaster(...)` use at `src/allocation.r:968-973` (anterior write pattern).

**Existing `compute_single_nhood_raster()` core (lines 1759-1798) — DO NOT change its signature.** The new `write_nhood_tif()` is a thin parent-side wrapper that calls the existing function and writes the result. The worker-side change is to **delete the `nhood_raster_cache` env** (lines 1427-1454) and replace it with `terra::rast(nhood_paths[nhood_needed])`.

**Existing terra::writeRaster pattern to copy** (lines 968-973, anterior write):

```r
terra::writeRaster(
  lulc_region,
  anterior_path,
  overwrite = TRUE,
  wopt = list(datatype = "INT2U", gdal = c("COMPRESS=LZW"))
)
```

The nhood TIF write must use a richer `gdal=` list per RESEARCH §"Path-based raster passing pattern":

```r
terra::writeRaster(
  rast, out_path, overwrite = TRUE,
  datatype = "FLT4S",
  gdal = c("COMPRESS=LZW", "TILED=YES",
           "BLOCKXSIZE=256", "BLOCKYSIZE=256",
           "BIGTIFF=IF_SAFER", "NUM_THREADS=1")
)
```

**Why FLT4S, not INT2U:** nhood values are normalized focal sums (continuous), not integer class IDs. **Why TILED=YES + BLOCKXSIZE=256:** workers `terra::extract()` at sparse points; tiled TIFs enable windowed reads, dramatically lowering per-worker page-cache footprint.

**Worker-side change pattern** — RESEARCH §"Path-based raster passing pattern" (worker-side read):

The existing worker-side nhood extraction at `src/allocation.r:1589-1618`:

```r
if (length(nhood_needed) > 0L) {
  ...
  t_nhood_extract <- prof_tic()
  nhood_stack <- terra::rast(lapply(nhood_needed, get_nhood_raster))   # IN-MEMORY cache
  nhood_vals <- terra::extract(nhood_stack, as.matrix(from_data[, .(x, y)]))
  ...
}
```

becomes:

```r
if (length(nhood_needed) > 0L) {
  ...
  t_nhood_extract <- prof_tic()
  nhood_stack <- terra::rast(nhood_paths[nhood_needed])   # PATH-BASED (parent-prepared)
  nhood_vals <- terra::extract(nhood_stack, as.matrix(from_data[, .(x, y)]))
  ...
}
```

`nhood_paths` is a **named character vector** passed into `generate_probability_maps()` (or `run_allocation_one_timestep` → worker via closure capture — character vectors are always fork-safe).

**Scratch directory pattern** — RESEARCH §"Where TIFs are materialised":

```r
nhood_cache_dir <- file.path(
  Sys.getenv("TERRA_TEMP", unset = tempdir()),
  "nhood_cache",
  paste0(scenario, "_", year_post, "_", region_suffix)
)
ensure_dir(nhood_cache_dir)
```

`Sys.getenv("TERRA_TEMP", ...)` was established by Phase 1 (`get_stage7_runtime_paths()`); the lookup convention matches `src/allocation.r:262-263` (`Sys.getenv("DINAMICA_BACKEND", unset = "auto")`).

**Eviction pattern** — RESEARCH §"Where TIFs are materialised":

```r
on.exit(unlink(nhood_cache_dir, recursive = TRUE), add = TRUE)
```

Use `add = TRUE` to compose with the existing `on.exit()` at `src/allocation.r:936-940` (worker sentinel flush). The current code already uses `add = TRUE` for the same reason — composition rather than replacement.

**Verbatim source ready to drop** (RESEARCH §"Code Examples — Parent-side nhood TIF write" lines 1079-1095) for `write_nhood_tif()`.

---

### `scripts/run_allocation.r` — replace lines 218-220 (entry-point orchestration)

**Analog:** itself — the existing pre-flight gate block at lines 208-216:

```r
# Set up parallel processing — but ONLY after run_allocation()'s pre-flight
# would otherwise pass. We run the gate explicitly here so an early failure
# does not leak into future::plan(). run_allocation() will run it again
# (idempotent) before any region work; the redundant call is intentional so
# that direct callers of run_allocation() (e.g. tests) still get gated.
preflight_exit <- run_preflight_and_print(config = config)
if (preflight_exit != 0L) {
  quit(status = preflight_exit)
}

num_workers <- as.integer(Sys.getenv("ALLOCATION_NUM_WORKERS", unset = "4"))
cat(sprintf("Setting up parallel processing with %d workers\n", num_workers))
future::plan(future::multisession, workers = num_workers)
```

**Insertion contract** — RESEARCH §"Where to call from" + §"Native thread pinning order":

```r
preflight_exit <- run_preflight_and_print(config = config)
if (preflight_exit != 0L) {
  quit(status = preflight_exit)
}

# Phase 3: thread pin + dev-strict-globals gate + auto plan selector.
# ORDER MATTERS — see src/allocation.r §pin_native_threads_to_one and RESEARCH §Q6.
pin_native_threads_to_one(verbose = TRUE)

if (isTRUE(as.logical(Sys.getenv("ALLOCATION_DEV_STRICT_GLOBALS", "FALSE")))) {
  options(future.globals.onReference = "error")
  cat("DEV MODE: future.globals.onReference = 'error' enabled\n")
}

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

**`cat()` startup-diagnostics convention** — already used at `scripts/run_allocation.r:50-61` for R version / `.libPaths()` reporting. The pin/select block follows the same `cat()` style (stdout → SLURM `.out`), not `log_msg()` (no per-region log exists yet).

**Tryback / cleanup convention** — keep the existing `tryCatch` at lines 227-238 and the `future::plan(future::sequential)` cleanup at line 241 unchanged. **One small addition** per RESEARCH §"OOM diagnosis hookup":

```r
error = function(e) {
  msg <- conditionMessage(e)
  if (grepl("MulticoreFuture|MultisessionFuture|lost.*worker", msg, perl = TRUE)) {
    log_msg(
      sprintf("PARENT_SENTINEL reason=lost_worker error=%s", msg),
      file.path("logs", sprintf("allocation_summary_%s.txt",
                                Sys.getenv("SLURM_JOB_ID", "local")))
    )
  }
  cat(sprintf("ERROR in allocation simulations: %s\n", e$message))
  ...
}
```

`log_msg()` is already exposed by sourcing `src/utils.r` (line 90 in the script).

---

### `src/calibrate_allocation_parameters.r:780-820` — replace the inverted "multicore causes OOM" selector (OPTIONAL — RESEARCH §Q1, §Q3)

**Analog:** the new `select_allocation_plan()` itself.

**Existing inverted selector to replace** (lines 794-815):

```r
# Determine parallel strategy
if (n_cores > 1) {
  # ALWAYS use multisession (even on Unix/HPC)
  # multicore uses forking which can cause OOM issues on HPC clusters
  # multisession creates separate R sessions with isolated memory
  future::plan(future::multisession, workers = n_cores)
  strategy <- "multisession"
  message(sprintf(
    "    ✓ Parallel processing ENABLED: %d workers using %s strategy",
    n_cores, strategy
  ))
  ...
} else {
  future::plan(future::sequential)
  ...
}
```

**Replacement (one decision, same shape, no Unicode bullet):**

```r
plan_choice <- select_allocation_plan()
if (plan_choice$strategy == "sequential") {
  future::plan(future::sequential)
} else if (plan_choice$strategy == "multicore") {
  options(parallelly.fork.enable = TRUE)
  future::plan(future::multicore, workers = plan_choice$workers)
} else {
  future::plan(future::multisession, workers = plan_choice$workers)
}
message(sprintf("    Parallel: strategy=%s workers=%d",
                plan_choice$strategy, plan_choice$workers))
```

**Scope decision is the planner's** per RESEARCH §Q1 / §Open Question 3: this refactor is small (~10 lines + the inverted comment), keeps the codebase coherent, and removes a latent landmine. If the planner judges it scope creep, mark as a follow-up; otherwise include as a single-task plan.

---

## Shared Patterns

### Logging
**Source:** `src/utils.r:1011-1024` (`log_msg(msg, log_file = NULL, also_console = TRUE)`)
**Apply to:** Every new helper that emits per-region progress (`load_all_region_models`, `write_nhood_tif`, `prof_cgroup_snapshot`). Use `cat()` only for parent-side startup diagnostics emitted **before** `future::plan()` (where no per-region log exists yet) — i.e., in `scripts/run_allocation.r` and inside `pin_native_threads_to_one(verbose=TRUE)`.

Pattern (as used at `allocation.r:511-517` and ubiquitous throughout `transition_modelling.r`):

```r
log_msg(
  sprintf("        predict_saved_transition_prob: starting (n_rows=%d)", NROW(new_data)),
  log_file
)
```

Indent levels by convention:
- 0 spaces: section headers (`"Loading X for region Y..."`)
- 4 spaces: per-region steps
- 6 spaces: per-transition steps
- 8 spaces: per-step sub-events

### Profile-line format
**Source:** `prof_toc()` @ `src/allocation.r:133-170` — emits `PROFILE <tag> elapsed=... rss_before=... rss_after=... rss_delta=... peak_rss=... gc_max_*=...`
**Apply to:** Every new long-running step in the parent (`load_all_region_models`, `write_nhood_tif`).

Pattern:
```r
t0 <- prof_tic()
# ... work ...
prof_toc(t0, sprintf("region=%s stage=preload_models", region_suffix), log_file)
```

The `stage=` tag must be a **new, unique** stage name so that summariser scripts (`scripts/summarise_allocation_profile.r`) and the verification gates (RESEARCH §Validation Architecture) can find it via grep.

**New stage names introduced by Phase 3** (use these exactly):
- `stage=pin_native_threads_to_one` (parent, once at startup)
- `stage=preload_models` (parent, once per region or once per timestep depending on Shape A vs A-prime)
- `stage=nhood_precompute` (parent, once per region — covers `write_nhood_tif()` loop)
- `stage=parent_baseline` (parent, after model+nhood preload, before fork)

### Worker state breadcrumbs
**Source:** `src/utils.r:1060-1102` (`worker_state_init`, `worker_state_set`, `worker_state_flush_sentinel`)
**Apply to:** Any new worker entry/exit path. The current worker body at `src/allocation.r:929-940` already wires them; Phase 3 edits to the worker body must **preserve** the `worker_state_init` / `on.exit(worker_state_flush_sentinel, add = TRUE)` / `worker_state_set` lifecycle. Add `worker_state_set(stage = "nhood_extract_from_tif", ...)` if a new stage is introduced.

### Null-coalesce
**Source:** `src/allocation.r:332` — `` `%||%` <- function(x, y) if (is.null(x) || (is.atomic(x) && length(x) == 0L)) y else x ``
**Apply to:** Every new config lookup. Example: `Sys.getenv("ALLOCATION_NUM_WORKERS", unset = NA_character_)` is paired with `as.integer()` + `is.na()` check (RESEARCH §Q1) — that idiom is the env-var analog of `%||%`.

### `ensure_dir()` before any file write
**Source:** Pattern used throughout (`src/transition_modelling.r:57-58`; `src/allocation.r:899-900`).
**Apply to:** `write_nhood_tif()` parent-side TIF writes — `ensure_dir(nhood_cache_dir)` before the `terra::writeRaster()` loop.

### `on.exit(..., add = TRUE)` composition
**Source:** `src/allocation.r:936-940` — sentinel flush inside the worker uses `add = TRUE` so additional cleanup hooks compose.
**Apply to:** Any new cleanup in the worker (e.g., the `unlink(nhood_cache_dir, recursive = TRUE)` if you choose to register it inside the worker rather than after `future_map`). **Recommended placement** is in the parent **after** `future_map` returns, not in the worker — because TIFs are parent-written and must outlive every fork. If registered in the parent, it's a simple `on.exit()` not `add = TRUE` since the parent's `run_allocation_one_timestep` has no pre-existing `on.exit`.

### Idempotent `requireNamespace()` guards
**Source:** `src/allocation.r:67`, `:280-287`, `:307` — every dependency probe uses `suppressWarnings(requireNamespace(pkg, quietly = TRUE))` and never `stop()`s on missing packages (pre-flight handles that gate centrally).
**Apply to:** `select_allocation_plan()` (parallelly probe), `pin_native_threads_to_one()` (RhpcBLASctl/data.table/arrow probes), `prof_cgroup_snapshot()` (no package deps — pure filesystem).

---

## Patterns to NOT Replicate (anti-patterns)

These are landmines from the current codebase that the Phase 3 implementation must actively avoid.

### 1. In-memory raster cache inside the worker (`src/allocation.r:1427-1454`)
The existing `nhood_raster_cache <- new.env(parent = emptyenv())` + `get_nhood_raster()` closure pattern is the thing Phase 3 is removing. **Do not preserve it**; do not "improve" it by hoisting the cache to the parent (RESEARCH §Pitfall 1 — that would capture a SpatRaster in a closure that crosses the fork boundary). The replacement is **strictly path-based**: parent writes TIFs, workers `terra::rast(path)`.

### 2. Inverted "multicore causes OOM" comment (`src/calibrate_allocation_parameters.r:796-798`)
```r
# ALWAYS use multisession (even on Unix/HPC)
# multicore uses forking which can cause OOM issues on HPC clusters
# multisession creates separate R sessions with isolated memory
```
This is semantically wrong for read-mostly workloads with parent-loaded shared objects (RESEARCH §"State of the Art"). When refactoring this block, **remove the comment entirely** — do not leave it inverted, do not "soften" it. The new selector's verbose output is the authoritative explanation.

### 3. Runtime `install.packages()`
Already removed in Phase 1 (Plan 01-03). Do not reintroduce. RESEARCH §"Environment Availability" confirms every Phase 3 dependency is already in `allocation_env.yml`.

### 4. Hoisting `anterior <- terra::rast(anterior_path)` to the parent (RESEARCH §Pitfall 1)
The temptation is real because `anterior` is region-scoped and reading it once seems efficient. **Don't.** The SpatRaster external pointer + `future.globals.onReference = "error"` dev gate will reject the closure. Re-open in the worker (current pattern preserves this; verify it stays preserved after the nhood refactor).

### 5. Hoisting `arrow::open_dataset()` to the parent (RESEARCH §Pitfall 7)
Same reason as (4) — Arrow Dataset is an R6 wrapper around a C++ externalptr. Keep `ds_static`/`ds_dynamic` construction inside the worker (current pattern at `src/allocation.r:1409-1421`).

### 6. Pre-creating worker log files in the parent (RESEARCH §Pitfall 6)
`initialize_worker_log()` at `src/utils.r:1030-1038` uses `Sys.getpid()` for filename uniqueness. Each fork has its own PID. **Do not** move `initialize_worker_log()` to the parent or pre-create log paths — workers must call it themselves so the PID is the fork's PID, not the parent's.

### 7. Calling `restore_ranger_importance_mode()` inside the worker (RESEARCH §Pitfall 4)
For mlr3 models this is a no-op anyway, but for legacy `.rds` models still in the dispatcher, mutation inside the worker triggers COW page copy. Apply the mutation **once in the parent** inside `load_all_region_models()` (pattern shown above).

---

## No Analog Found

**None.** Every Phase 3 helper has a strong analog in the existing codebase — this is by design (RESEARCH §Architectural Responsibility Map established that no new external library or new tier is needed; this is a refactor inside the existing R parent/worker structure).

The patterns from external sources (parallelly, future, RhpcBLASctl, GDAL GTiff) are concentrated in **RESEARCH §Code Examples (lines 1020-1128)** and reproduced inline above in each helper's "Verbatim source ready to drop" subsection.

---

## Key Landmines for Planner

The following are the highest-risk integration points extracted from RESEARCH §Common Pitfalls and §Open Questions that the planner's action steps must explicitly address.

1. **Call order in `scripts/run_allocation.r`** — `pre-flight → pin_native_threads_to_one() → strict-globals option → select_allocation_plan() → future::plan()`. Any other order silently misses the contract (RESEARCH §Pitfall 2). The plan's acceptance criteria must include a grep test asserting `pin_native_threads_to_one` line number < `future::plan` line number.

2. **xgboost learner under strict-globals scan** — RESEARCH §"Open Questions Q1" — unverified whether `options(future.globals.onReference = "error")` accepts a closure capture containing the mlr3 xgboost learner's externalptr wrapper. The Phase 3 plan must include a dev-run task that loads ONE xgboost model in the parent, runs `future_map(1:2, ...)` with strict-globals on, and reports. If it fails, the fallback is `learner$marshal()` in the parent + `learner$unmarshal()` in the worker.

3. **Cgroup v1 vs v2 path on Euler** — RESEARCH §"Open Question 6". `prof_cgroup_snapshot()` MUST try both paths; the verbatim source in RESEARCH §Code Examples already does this.

4. **`models_list` parent baseline** — RESEARCH §"Open Question 4" — the ~11 GB parent baseline (140 models × ~80 MB) assumes Phase 2's size targets held in practice. The smoke-run plan must include a `du -sh` task on `outputs/transition_models/2018_2022/*.qs` BEFORE pre-load to verify; if averages exceed ~200 MB, escalate to revisit Shape A vs A-prime.

5. **`config[["preds_tools_dir"]]/neighbourhood_matrices/all_matrices.rds`** lives at `src/allocation.r:1432-1436` — currently lazy-loaded inside the worker. Phase 3 must **pre-load this once in the parent** (it's a plain R list, fork-safe), then pass to `write_nhood_tif()`. Don't leave the parent-side load on a separate `readRDS()` call inside `write_nhood_tif()` itself — that would re-read the file once per predictor.

6. **`class_name_to_value` named integer vector** — currently built inside `generate_probability_maps()` at line 1290. Pre-build in parent (plain R, fork-safe) and pass to both `write_nhood_tif()` (parent) and the worker closure.

7. **`scripts/calibrate_allocation_parameters.r` scope decision** — RESEARCH §"Open Question 3" — refactor or not? The planner picks. PATTERNS.md provides the analog (use the same `select_allocation_plan()`) regardless; the plan-list decides whether a separate task is created.

---

## Metadata

**Analog search scope:** `src/`, `scripts/`, `.planning/phases/01-repair-visibility/`, `.planning/phases/02-model-size-reduction/`

**Files read (with line ranges):**
- `.planning/phases/03-parallelism-memory-architecture/03-RESEARCH.md` (full, 1179 lines, in 4 non-overlapping chunks)
- `.planning/phases/02-model-size-reduction/02-PATTERNS.md` (full — for layout conventions)
- `.planning/phases/01-repair-visibility/01-03-SUMMARY.md` (full — RSS profiling + pre-flight pattern)
- `.planning/STATE.md` (full)
- `.planning/ROADMAP.md` (full)
- `src/allocation.r` lines 1-200, 200-600 (selected), 880-1100, 1400-1620, 1740-1798
- `src/transition_modelling.r` lines 100-320
- `src/utils.r` lines 1000-1163
- `scripts/run_allocation.r` (full, 295 lines)
- `scripts/retrain_all_models.r` (full, 250 lines)
- `src/calibrate_allocation_parameters.r` lines 770-820

**Pattern extraction date:** 2026-05-11
