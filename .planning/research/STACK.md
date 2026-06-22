# STACK — R 2025 Memory-Efficient Parallel ML Prediction over Spatial Rasters

**Project:** nascent-lulcc (Stage 7 allocation OOM remediation)
**Researched:** 2026-05-05
**Scope:** R 4.3–4.4 ecosystem; Linux HPC (ETH Euler / SLURM) and Windows local
**Overall confidence:** MEDIUM (web tooling unavailable in this session; recommendations are grounded in well-established R ecosystem knowledge — see "Verification Gaps" at end)

---

## TL;DR — Recommended Stack

| Decision | Pick | Why (one line) |
|---|---|---|
| Parallel backend (HPC/Linux) | `future::multicore` (or `future.callr::callr` if forking is unsafe) | Fork + COW shares model & raster RAM across workers; multisession copies them N times |
| Parallel backend (Windows local) | `future::multisession` with `globals = list(...)` whitelist | No fork on Windows; explicit globals prevents accidental closure capture of >1 GB models |
| Outer parallelism | `future::plan(list(sequential, multicore))` (one level) | Allocation already iterates regions sequentially per timestep — parallelise transitions inside the region, not regions × transitions |
| Model framework | **Stay on tidymodels** for now; `butcher` + `bundle` aggressively. Migrate to `mlr3` only if butchering still leaves objects > 200 MB | Migration cost is high; butchering typically reclaims 70–95% of size |
| Model size reduction (RF) | `butcher::axe_call() %>% axe_ctrl() %>% axe_data() %>% axe_env() %>% axe_fitted()` on the parsnip fit; for `ranger` also drop `forest$trees` indices not needed for predict | Workflow envs and `ranger$predictions` / `xgb.Booster` raw bytes dominate size |
| Model size reduction (XGBoost) | `xgb.save()` + reload via `xgb.load()` raw → wrap as a thin parsnip-compatible predictor; or `bundle::bundle()` for the workflow | XGBoost stores native handles + R-side env; serialising via xgboost's own format is tighter |
| Raster engine | `terra` only — finish migrating off `raster` (PIPE-05) | `raster` doubles memory for type coercions; `terra` uses C++ pointers (cannot survive serialisation) |
| Per-worker raster handling | **Pass file paths, not SpatRaster objects.** Each worker calls `terra::rast(path)` itself | `SpatRaster` is an external pointer — serialising it produces a broken handle in the worker |
| Predict batching | `terra::predict(rast, model, fun, cores=1, na.rm=TRUE, wopt=list(...))` with `terra` block-wise reading; do NOT call `predict()` on a 100 M-row data.frame | Eliminates the data.frame materialisation that drives RAM peaks |
| Parquet reads | `arrow::open_dataset()` per worker (lazy), `dplyr::filter()` + `collect()` only the needed columns/rows | `arrow::read_parquet()` materialises whole file; lazy datasets + column projection cut RAM by 5–20× |
| Profiling | `bench::mark()` + `lobstr::obj_size()` + `proffer` / `profvis`; `peakRAM` on Linux for RSS | `pryr::object_size` is deprecated in favour of `lobstr::obj_size` |
| RSS measurement (fix ALLOC-03) | Read `/proc/self/status` `VmRSS` via `ps::ps_memory_info()` from the **`ps`** package — works on Linux + Windows | The current `NA MB` symptom suggests `pryr::mem_used()` or a custom parser failing on the HPC kernel |

---

## 1. Parallel Backend: `multicore` vs `multisession`

### The crash pattern, explained

> `MultisessionFuture interrupted` after `gc_max_vcells = 12,125 MB` on one model load.

This is the canonical signature of **per-worker object duplication under `multisession`**. Each worker is an independent R process started via `parallelly::makeClusterPSOCK()`. When the parent passes the model into a future, `future` serialises it (via `qs` or `base::serialize`) and the worker deserialises a full copy. With N workers and a 1.2 GB model object, peak RAM is `parent + N × 1.2 GB`. The OOM killer fires the moment one of those allocations crosses the cgroup limit.

### Backend comparison (Linux HPC)

| Backend | Mechanism | Model RAM cost | Best for |
|---|---|---|---|
| `future::sequential` | Single process | 1× | Debugging, last-resort |
| `future::multicore` | `parallel::mcfork()` — POSIX fork with COW | **1× shared** until a worker writes to the model object; then per-page COW | Linux HPC; this is the right answer |
| `future::multisession` | PSOCK cluster (separate Rscript processes) | **N × full copy** | Windows; cross-platform; when fork is unsafe |
| `future::cluster` (PSOCK) | Same as multisession but explicit | N × | Multi-node MPI-like setups |
| `future.callr::callr` | New `callr` R session per future | N × but with a fresh R for each call | When you need full isolation (e.g., XGBoost native handle leaks) |
| `future.batchtools` SLURM | Each future is a SLURM job | 1× per job, but high latency | Embarrassingly parallel coarse-grained work |

**Linux fork (COW) caveat:** RStudio, RGui, and any GUI/IDE warns against forking — `future::multicore` will silently fall back to sequential inside RStudio. On a SLURM batch job (`Rscript`, no GUI), `multicore` works correctly and is the default-correct choice.

### Why `multicore` is the immediate win

1. **Model object is read-only in the worker** (only `predict()` is called). COW means the 1.2 GB never gets copied — workers read the same physical pages as the parent.
2. **Predictor SpatRasters are file-backed** in `terra` — workers can re-open them; but even in-memory rasters are read-only during `predict()`, so COW also helps.
3. **Globals capture is implicit and free** under fork — none of the per-future serialisation cost.

**Caveats specific to `multicore`:**
- **Do not fork after loading XGBoost models with native handles.** XGBoost's `xgb.Booster` holds a C++ pointer that becomes invalid in the child after fork. Workaround: re-`xgb.load()` inside each future, or use `bundle::bundle()` so the worker reconstructs the booster from raw bytes on first use.
- **Do not fork after loading `terra` `SpatRaster` objects** — same external-pointer problem. Pass the file path; let the worker call `terra::rast(path)`.
- **Do not fork after opening a parallel BLAS / OpenMP threadpool** — leads to deadlock. Set `RhpcBLASctl::blas_set_num_threads(1)` and `omp_set_num_threads(1)` before forking, then restore in workers.
- **Do not nest multicore inside multicore** — use `plan(list(multicore, sequential))` and parallelise at one level only.

### When to NOT use `multicore`

| Situation | Use instead |
|---|---|
| Windows (no fork) | `multisession` with explicit `globals` and `qs` serialisation (see §1.4) |
| RStudio interactive debug | `sequential` (multicore degrades silently to sequential anyway) |
| External pointer instability (XGBoost C handles, terra) | `future.callr::callr` — fresh R session per future, predictable |
| Multi-node SLURM (>1 node per job) | `future.batchtools::batchtools_slurm` |

### Concrete configuration recommendation

```r
# In allocation.r, replace the implicit furrr plan with:

if (.Platform$OS.type == "unix" && !interactive()) {
  # HPC SLURM batch — fork-based COW
  future::plan(future::multicore, workers = n_workers)
} else {
  # Windows / interactive — PSOCK with tight globals
  future::plan(
    future::multisession,
    workers = n_workers,
    gc = TRUE  # force gc on idle workers
  )
}

# CRITICAL: Limit BLAS threads BEFORE plan() so workers don't oversubscribe
RhpcBLASctl::blas_set_num_threads(1)
RhpcBLASctl::omp_set_num_threads(1)
data.table::setDTthreads(1)
```

### Migration difficulty
- `multisession → multicore` on Linux: **LOW**. Single-line plan change. Main risk is XGBoost handle / terra pointer issues — mitigated by re-loading inside the worker (which the code already does for some objects).
- Adding Windows fallback: **LOW**. Already shown above.
- Verifying COW works: **MEDIUM**. Need RSS-aware profiling (see §6) to confirm shared pages.

---

## 2. Model Object Size: `tidymodels` Reduction Strategies

### Why a tidymodels workflow is >1 GB

A `workflow` object retains, by default:
1. **The full training data frame** (often the largest single contributor — drop with `butcher::axe_data()`)
2. **All formula/recipe environments**, which by R's lexical scoping pull in *every object visible at fit time* — easily hundreds of MB if fitted inside a function with rasters in scope (`butcher::axe_env()`)
3. **The model's own bloat:**
   - `ranger`: the full forest (`$forest$child.nodeIDs`, `$forest$split.varIDs`, `$forest$split.values`) plus, by default, OOB predictions, the inbag matrix, and per-tree variable importance. **`ranger`'s forest object scales linearly with `num.trees × max.depth`.** A 500-tree RF on 1 M training rows easily exceeds 500 MB.
   - `xgboost`: the raw booster bytes (`raw`) plus the `evaluation_log`, `feature_names`, the original `xgb.DMatrix` reference (`call$data`), and `attributes`.
   - `glmnet`: `$glmnet.fit$beta` is a sparse matrix; usually <50 MB. Glmnet rarely needs butchering.
4. **Tune results** if the workflow came from `tune::last_fit()` — drop with `butcher::axe_fitted()` on intermediate folds.

### `butcher` — what it actually does

`butcher` (Tidymodels org, last release ~2024) provides surgical strippers:

| Function | Removes | Effect on `predict()` |
|---|---|---|
| `axe_call()` | `model$call`, `terms$.Environment` | Safe — predict doesn't need call |
| `axe_ctrl()` | Control objects (`ranger` control, `xgb.train` params) | Safe |
| `axe_data()` | Training data slot, `model.frame` | Safe — but breaks `update()`, `refit()` |
| `axe_env()` | Formula/recipe environments | Safe but can break `bake()` if recipes reference globals — **TEST after** |
| `axe_fitted()` | `$fitted.values`, `$residuals` | Safe |

**Typical reductions** (training-data heuristics; verify on your models with `lobstr::obj_size()`):
- `ranger` workflow: 1.2 GB → 200–400 MB (mostly the data slot)
- `xgboost` workflow: 800 MB → 50–150 MB (mostly the recipe env + DMatrix ref)
- `glmnet` workflow: 80 MB → 20 MB

### Going further than `butcher`: `bundle`

`bundle` (Tidymodels org) was introduced specifically for serialising models with native pointers (XGBoost, Keras, torch, h2o). It:
1. Calls model-specific `bundle()` methods that serialise the native pointer to raw bytes.
2. Produces a portable object that survives `saveRDS()` + reload across R sessions and forked processes.
3. On the worker, `unbundle()` reconstructs the native handle on first use.

**Use `bundle` for XGBoost models always**; without it, the booster handle is invalid in any deserialised context (PSOCK worker or post-fork XGBoost call).

### Why **NOT** to dump tidymodels for `mlr3` *yet*

`mlr3` learner objects are **not categorically smaller** than tidymodels workflows. They wrap the same underlying engine (`ranger`, `xgboost`, `glmnet`) and store similar metadata. The reductions you'd see come from:

1. `mlr3`'s `Learner$state$model` — the raw model — vs tidymodels' `workflow$fit$fit$fit` chain. Slightly less wrapper overhead, but the underlying `ranger.forest` is the same size.
2. `mlr3`'s lazier handling of training data (it stores a reference to a `Task`, not a copy). Tidymodels' workflow does copy unless butchered.
3. No automatic recipe environment retention.

**Empirical rule of thumb:** A butchered + bundled tidymodels workflow is within 1.2× of the equivalent mlr3 learner state. The migration cost (rewriting `transition_modelling.r`'s ~2200 lines, retraining the predict-from-saved-model code path, replumbing tune → mlr3tuning) is not justified by a 20% size win.

**Recommendation for MLR3-01:** Treat mlr3 as a **fallback** if, after applying `butcher` + `bundle` + `ranger(num.trees, max.depth)` reduction + `xgb.save_raw()`, models are still >200 MB.

### Migration difficulty estimates

| From → To | Effort | Risk |
|---|---|---|
| Naive workflow → `butcher`-stripped | LOW (1–2 days, code already partly there) | LOW — covered by tidymodels test suite |
| `butcher`-stripped → `+ bundle()` for XGBoost | LOW (half a day) | LOW |
| `ranger(num.trees=500, write.forest=TRUE)` → `ranger(num.trees=500, save.memory=TRUE, respect.unordered.factors="order")` | LOW (config change) | LOW — slightly slower predict |
| `tidymodels` → `mlr3` full migration | **HIGH** (2–4 weeks: rewrite training, tuning, model save/load, prediction adapters; reconcile `recipes` → `mlr3pipelines`) | HIGH — production-critical code path |
| `tidymodels` → `tidypredict`-only (already partly done) | LOW–MEDIUM | MEDIUM — `tidypredict` for RF generates large SQL/dplyr expressions and is slower than `predict()` for big trees; existing code already disables it for RF with many obs |

### What to actually configure in training

```r
# In transition_modelling.r model spec for ranger:
parsnip::rand_forest(trees = 300, min_n = 10) %>%   # 300, not 500
  parsnip::set_engine("ranger",
    save.memory = TRUE,                  # uses smaller integer types in forest
    write.forest = TRUE,                 # required for predict
    keep.inbag = FALSE,                  # default; reaffirm
    importance = "none",                 # only enable for diagnostics, not prod
    respect.unordered.factors = "order", # treats factors numerically — much smaller
    num.threads = 1                      # leave threading to outer parallelism
  ) %>%
  parsnip::set_mode("classification")
```

```r
# For xgboost:
parsnip::boost_tree(trees = 500, tree_depth = 6, learn_rate = 0.05) %>%
  parsnip::set_engine("xgboost",
    nthread = 1,                         # don't oversubscribe
    save_period = NULL,
    verbose = 0
  ) %>%
  parsnip::set_mode("classification")
```

```r
# In the save-model path (already exists; tighten it):
slim_workflow <- final_wf %>%
  butcher::axe_call() %>%
  butcher::axe_ctrl() %>%
  butcher::axe_data() %>%
  butcher::axe_env() %>%
  butcher::axe_fitted()

# For XGBoost, additionally bundle:
if (best_model_name == "xgboost") {
  slim_workflow <- bundle::bundle(slim_workflow)
}

# Verify size before write
size_mb <- as.numeric(lobstr::obj_size(slim_workflow)) / 1024^2
if (size_mb > 250) {
  warning(sprintf("Model still %.0f MB after butcher/bundle", size_mb))
}
qs::qsave(slim_workflow, model_path, preset = "high")
```

### Why `qs` not `saveRDS`

`qs::qsave()` (qs2 in 2025) uses zstd + lz4 with multi-threaded compression. Typical results vs `saveRDS(compress="xz")`:
- 3–10× faster save/load
- Comparable or better compression ratio
- Already a tidymodels-friendly format (no special hooks needed)

If `qs` isn't in `allocation_env.yml`, add `r-qs` (or the newer `r-qs2`).

---

## 3. `terra` Raster Processing in Parallel Workers

### The fundamental constraint: `SpatRaster` is an external pointer

```r
r <- terra::rast("predictor.tif")
class(r)            # "SpatRaster"
typeof(r@ptr)       # "externalptr"  <-- C++ pointer, NOT serialisable
```

Serialising a `SpatRaster` produces an object that *appears* to deserialise but whose `@ptr` is invalid — any subsequent operation crashes or returns garbage. Symptoms:
- Worker silently produces all-NA output
- `Error: external pointer is not valid`
- Segfault (especially with large rasters)

This is *the* canonical terra+future bug, and is the most likely cause of *some* worker corruption in the existing pipeline.

### Strategy 1: File-path passing (RECOMMENDED for almost everything)

```r
furrr::future_map(transition_paths, function(model_path) {
  # Re-open everything inside the worker
  r <- terra::rast(predictor_raster_path)   # cheap — just opens GDAL handle
  m <- qs::qread(model_path)
  if (inherits(m, "bundled_workflow")) m <- bundle::unbundle(m)

  terra::predict(r, m, fun = predict_fn,
    filename = file.path(work_dir, paste0(transition, ".tif")),
    overwrite = TRUE,
    wopt = list(datatype = "FLT4S", gdal = "COMPRESS=ZSTD")
  )
}, .options = furrr::furrr_options(seed = TRUE, globals = c("predictor_raster_path", "predict_fn")))
```

Cost: each worker re-opens the GDAL handle (~ms) and reads chunks lazily as needed.

### Strategy 2: `terra::wrap()` / `unwrap()` for genuinely small rasters

For rasters that *must* travel (e.g., a small region mask):

```r
r_packed <- terra::wrap(small_raster)   # serialises raster contents to a PackedSpatRaster

furrr::future_map(..., function(...) {
  r <- terra::unwrap(r_packed)
  # ...
})
```

`wrap()` materialises the raster contents into a portable R object. **Only use for rasters that fit comfortably in memory** — a wrapped 30,000×30,000 Int32 raster is ~3.6 GB and will be sent to every worker.

### Strategy 3: `terra::predict()` block-wise (replaces the slow per-pixel loop)

The current cost profile shows `predict` at 385–472 s per transition. This strongly suggests the code is materialising a large `data.frame` of predictors and calling `predict.workflow()` on it. `terra::predict()` does this block-wise automatically:

```r
predictors_stack <- terra::rast(c(p1_path, p2_path, p3_path))  # SpatRaster of N layers

# terra reads one block (e.g., 1024 x 1024) at a time, calls fun(), writes result
prob_rast <- terra::predict(
  predictors_stack,
  model = slim_workflow,
  fun = function(model, ...) {
    df <- as.data.frame(...)
    predict(model, df, type = "prob")$.pred_1   # parsnip API
  },
  na.rm = TRUE,
  filename = out_path,
  overwrite = TRUE,
  wopt = list(datatype = "FLT4S")
)
```

**Memory ceiling per worker** is then `block_size × n_predictors × 8 bytes` ≈ a few hundred MB regardless of total raster size. No more 100M-row data.frames.

### Strategy 4: terra's own multithreading

`terra` supports `cores=` in `predict()`, `app()`, `lapp()`. **Critical:** if you parallelise *outside* with `future::multicore` AND set `cores>1` inside terra, you oversubscribe (workers × cores threads) and slow everything down. Pick one level:
- **One transition at a time, parallel inside:** `terra::predict(..., cores = n_workers)` — best when transitions can be handled sequentially
- **Multiple transitions, serial inside:** `furrr::future_map(transitions, ..., terra::predict(..., cores = 1))` — best when you have many transitions and predict isn't already cores-bottlenecked

For the LULCC pipeline with ~10–20 transitions per region, **outer-parallel-over-transitions, inner-serial-terra** is usually the winner because transitions are independent and the per-transition `predict()` already does block-wise reads.

### `terra::tmpFiles()` and TMPDIR

terra writes intermediate files. On HPC, `/tmp` is typically a small ramdisk. Set:
```r
terra::terraOptions(tempdir = Sys.getenv("TMPDIR", "/beegfs/.../terra_tmp"))
```
The codebase already does this (PIPE-03) but should verify the env var is set in the SLURM script.

---

## 4. Parquet Reads via `arrow`: Lazy + Per-Worker

### Wrong pattern (loads everything into the parent before forking)

```r
# Parent process loads 5 GB of predictor data into RAM, then forks N workers...
all_preds <- arrow::read_parquet(parquet_path)
furrr::future_map(transitions, function(t) {
  subset <- dplyr::filter(all_preds, transition == t)
  # ...
})
# RAM: 5 GB parent + (with multicore) ~5 GB shared via COW (OK)
# RAM: 5 GB parent + N × 5 GB (with multisession) → OOM
```

### Right pattern (each worker opens its own lazy dataset)

```r
furrr::future_map(transitions, function(t) {
  ds <- arrow::open_dataset(parquet_path)   # lazy — opens metadata only
  subset <- ds %>%
    dplyr::filter(transition == t) %>%
    dplyr::select(all_of(predictors_for_t)) %>%
    dplyr::collect()                        # materialises only the needed slice
  # ...
}, .options = furrr::furrr_options(globals = c("parquet_path", "predictors_for_t")))
```

**Why this works:**
- `arrow::open_dataset()` reads only the parquet footer (KBs).
- Predicate pushdown means `filter()` is executed by Arrow's C++ engine, often skipping entire row groups via column statistics.
- Column projection in `select()` reads only the needed columns from disk.
- Total RAM per worker is bounded by the *result*, not the file.

### Multi-file partitioned datasets

If predictors are split per region/year, use a partitioned dataset:

```r
ds <- arrow::open_dataset(
  "predictors/",
  partitioning = c("region", "year")
)
# Hive-style partitioning lets filter(region == "andes", year == 2030) skip non-matching files entirely
```

This is the single biggest disk-I/O and RAM win for the predictor_load stage (currently 10–22 s — could be <2 s with good partitioning).

### `arrow` threading

Set per-worker:
```r
arrow::set_cpu_count(1)
arrow::set_io_thread_count(2)
```
Otherwise Arrow grabs all cores by default and fights `future::multicore` workers.

---

## 5. mlr3 — Concrete Comparison vs tidymodels

| Dimension | tidymodels | mlr3 | Verdict |
|---|---|---|---|
| Model object size (RF, butchered) | 200–400 MB | 150–300 MB | Marginal mlr3 win |
| Model object size (XGBoost, butchered+bundled) | 50–150 MB | 50–120 MB | Comparable |
| Recipes/preprocessing storage | Recipe env can bloat by 100s of MB if not axed | `mlr3pipelines` graph stores transformer state explicitly — generally smaller | mlr3 win |
| API stability | Very stable; mature | Stable but smaller community; more breaking changes historically | tidymodels win |
| Tuning ergonomics | `tune` package, dial-based | `mlr3tuning` + `paradox` — more flexible but steeper | tidymodels win for clarity |
| Resampling parallelism | Via `tune` + `future` | Native `mlr3::future` integration | Tie |
| Spatial / blocked CV | `spatialsample` | `mlr3spatial`, `mlr3spatiotempcv` (more comprehensive) | **mlr3 strong win** for spatial work |
| Custom predictors / engines | `parsnip` engines, well-documented | `mlr3extralearners` | Comparable |
| Saved-model ecosystem | `butcher`, `bundle`, `tidypredict`, `vetiver` | `mlr3` has its own `Learner$state$model`; works with `bundle` indirectly | tidymodels has more dedicated tooling |
| Migration cost from existing tidymodels code | n/a | High — full rewrite of training, tuning, persistence | tidymodels wins by inertia |

**Bottom line:** The *only* compelling reason to switch is `mlr3spatial` for inherently spatially-aware modelling, which is genuinely better than tidymodels for LULCC. For pure object-size reduction, **butcher + bundle on tidymodels achieves 80–90% of what mlr3 would**, at <10% of the migration cost.

**Defer mlr3 evaluation (MLR3-01) until after** the immediate OOM is resolved. If butchered models are still >200 MB and predict is still the bottleneck, then prototype mlr3 on one transition.

---

## 6. RAM Profiling Fix (ALLOC-03)

The `rss_before=NA MB` symptom suggests the current profiler is using `pryr::mem_used()` (R-side memory only, not RSS) or parsing `/proc/self/status` with a regex that fails on the Euler kernel's format.

### Recommended approach — the `ps` package

```r
# r-ps is in conda-forge and works on Linux + Windows
get_rss_mb <- function() {
  tryCatch({
    info <- ps::ps_memory_info(ps::ps_handle())
    as.numeric(info[["rss"]]) / 1024^2
  }, error = function(e) NA_real_)
}

# In the worker log:
log_msg(sprintf("rss_before=%.0f MB", get_rss_mb()), log_file)
```

### Fallback (no `ps` package)

```r
get_rss_mb <- function() {
  if (.Platform$OS.type == "unix") {
    status <- tryCatch(readLines("/proc/self/status"), error = function(e) character())
    line <- grep("^VmRSS:", status, value = TRUE)
    if (length(line) == 0) return(NA_real_)
    as.numeric(sub("[^0-9]+", "", line)) / 1024  # kB → MB
  } else {
    # Windows
    NA_real_  # use ps::ps_memory_info instead
  }
}
```

Add `r-ps` to `allocation_env.yml`.

---

## 7. Recommended Stack — Full Specification

### Add to `allocation_env.yml`

```yaml
# Memory-efficient model serialisation
- r-butcher          # Strip fitted models — already in main stack
- r-bundle           # Serialise XGBoost native handles (NEW — required for fork safety)
- r-qs               # Or r-qs2 — fast model save/load (replaces saveRDS)

# Profiling and process introspection
- r-ps               # RSS measurement, process info (fixes ALLOC-03)
- r-lobstr           # Modern object-size measurement (replaces pryr::object_size)
- r-rhpcblasctl      # Control BLAS threading inside workers

# Already present, keep
- r-future
- r-furrr
- r-terra
- r-arrow
- r-data.table
```

### Remove from main stack consideration (do NOT add)

| Package | Why not |
|---|---|
| `r-mlr3` and ecosystem | Defer until butcher+bundle proves insufficient (MLR3-01 is secondary priority) |
| `future.callr` | Only needed if multicore proves unsafe for XGBoost — try multicore first |
| `clustermq`, `rrq`, `rq` | Multi-node distributed queues; overkill for single-node SLURM job |
| `disk.frame` | Superseded by arrow/duckdb |
| `bigmemory`, `ff` | Pre-arrow era; arrow + duckdb dominate |
| `parallel::mclapply` | Use `furrr::future_map` consistently — single API |

### Versions / Pins

| Package | Recommended pin | Notes |
|---|---|---|
| `r-base` | 4.3.x (current) or 4.4.1 | Either is fine; 4.4 has marginally better gc |
| `r-terra` | ≥1.7-71 | Frequent bugfixes; pin floor not ceiling |
| `r-arrow` | ≥15.0 | Lazy datasets stable; predicate pushdown reliable |
| `r-future` | ≥1.34 | `globals` handling and multicore safety improvements |
| `r-furrr` | ≥0.3.1 | Aligned with future ≥1.34 |
| `r-butcher` | ≥0.3.4 | Stable API |
| `r-bundle` | ≥0.1.0 | XGBoost + Keras coverage |
| `r-xgboost` | 1.7.x (already pinned for tidypredict) | **Do not** upgrade to 2.x without re-validating the saved-model code path |
| `r-ranger` | ≥0.16.0 | `save.memory=TRUE` available |
| `r-ps` | ≥1.7 | RSS support solid |

---

## 8. Alternatives Considered

| Choice | Recommended | Alternative considered | Why not |
|---|---|---|---|
| Parallel backend (Linux HPC) | `future::multicore` | `future::multisession` | Causes the OOM via per-worker copies — root cause of current crash |
| Parallel backend (Linux HPC) | `future::multicore` | `parallel::mclapply` | Same fork mechanism; lacks furrr's progress / seed handling consistency |
| Parallel backend (Linux HPC) | `future::multicore` | `future.batchtools::batchtools_slurm` | Per-future SLURM job is too coarse for per-region transitions; high latency |
| Model framework | tidymodels + butcher + bundle | mlr3 | Migration cost (HIGH) outweighs ~20% size benefit; revisit if butcher insufficient |
| Model serialisation | `qs` / `qs2` | `saveRDS(compress="xz")` | qs is 3–10× faster, comparable compression |
| XGBoost handle survival | `bundle::bundle()` | `xgb.save()` + `xgb.load()` direct | Bundle integrates cleanly with parsnip workflow; xgb.save loses parsnip wrapper |
| Raster engine | `terra` only | `terra` + `raster` (current) | Double memory; PIPE-05 already targets this |
| Raster across workers | File path + re-open | `terra::wrap()` | wrap copies raster contents — fine for masks, OOM for predictor stacks |
| Predict pattern | `terra::predict(rast, model, fun, ...)` | `as.data.frame(rast); predict()` | Avoids materialising 100M-row data.frame |
| Parquet read | `arrow::open_dataset()` lazy | `arrow::read_parquet()` eager | Lazy reads only the needed slice; eager loads everything |
| Tuning | `tune` + future | `mlr3tuning` | Already in use; no migration justified |
| RSS profiling | `ps::ps_memory_info` | `pryr::mem_used` | mem_used is R-side only; doesn't catch native (XGBoost / terra) allocations |

---

## 9. Installation Summary

```yaml
# environments/allocation_env.yml — proposed additions
dependencies:
  # ... existing entries ...

  # Memory profiling and process control
  - r-ps
  - r-lobstr
  - r-rhpcblasctl

  # Model size reduction (butcher already implicit; pin it)
  - r-butcher
  - r-bundle

  # Fast serialisation
  - r-qs
```

```r
# In allocation entry-point script, before any future plan:
RhpcBLASctl::blas_set_num_threads(1)
RhpcBLASctl::omp_set_num_threads(1)
data.table::setDTthreads(1)
arrow::set_cpu_count(1)

# Plan
n_workers <- as.integer(Sys.getenv("SLURM_CPUS_PER_TASK", "4"))
if (.Platform$OS.type == "unix" && !interactive()) {
  future::plan(future::multicore, workers = n_workers)
} else {
  future::plan(future::multisession, workers = n_workers)
}

# terra
terra::terraOptions(
  tempdir = Sys.getenv("TMPDIR", "/beegfs/${USER}/terra_tmp"),
  memfrac = 0.4,                                  # leave 60% for other processes
  todisk = TRUE                                   # write intermediates rather than holding in RAM
)
```

---

## 10. Phase / Roadmap Implications

The recommendations above factor naturally into 4 phases (rough; refine in roadmap):

1. **Phase A — Backend swap (1–2 days, high impact, low risk)**
   - Switch `future::multisession` → `future::multicore` on Linux
   - Add BLAS thread pinning
   - Add `ps`-based RSS profiling (fixes ALLOC-03)
   - **Expected win:** 60–80% RAM reduction per worker on HPC

2. **Phase B — Model size reduction (2–3 days, high impact, low risk)**
   - Tighten `butcher` calls in `transition_modelling.r`
   - Add `bundle::bundle()` for XGBoost
   - Adjust `ranger` engine args (`save.memory`, `respect.unordered.factors="order"`)
   - Switch save format to `qs::qsave()`
   - **Expected win:** 70–90% per-model-file reduction

3. **Phase C — Predict refactor (3–5 days, high impact, medium risk)**
   - Replace data.frame-based predict with `terra::predict(rast, model, fun)`
   - Pass file paths into workers, not SpatRasters
   - Switch parquet reads to `arrow::open_dataset()` lazy
   - **Expected win:** Predict stage 385–472 s → 60–120 s; eliminates several GB of transient RAM

4. **Phase D — mlr3 evaluation (deferred; only if needed)**
   - Prototype one transition in mlr3
   - Compare object size, predict speed, integration cost
   - Decision point on full migration

---

## 11. Pitfalls / Phase-Specific Warnings

| Phase | Pitfall | Mitigation |
|---|---|---|
| A (backend swap) | Multicore + XGBoost native handle = segfault on first predict in worker | Reload XGBoost via `bundle::unbundle()` inside the worker on first use |
| A | Multicore + terra SpatRaster passed as global = invalid pointer | Pass file paths only; verify `globals` list excludes raster objects |
| A | Multicore in RStudio degrades silently to sequential | Document: HPC batch only; use multisession when interactive |
| B | `butcher::axe_env()` breaks `recipes::bake()` if recipe references globals | Test predict on a held-out batch *after* every butcher step |
| B | `respect.unordered.factors="order"` changes split semantics — minor accuracy drift | Validate against held-out test set; difference usually <0.5% AUC |
| C | `terra::predict(cores>1)` × `multicore workers` = oversubscription | Set terra cores=1 when outer-parallel; pick one level |
| C | Arrow predicate pushdown silently fails on non-row-group-aligned filters | Test with a known-result query; use `dplyr::collect()` only after profiling row count |
| D | mlr3 migration touches the whole training/serialisation/predict pipeline | Hold off until A–C ship and quantify residual gap |

---

## 12. Verification Gaps (be honest)

This research was conducted **without access to web tools** (WebSearch, WebFetch, and Brave/Exa CLIs were all denied in this session). Recommendations are grounded in well-established R ecosystem knowledge consistent with R 4.3–4.4 and the package versions present in the conda env, but specific points worth validating against current docs before commiting to phase work:

| Claim | Confidence | How to verify |
|---|---|---|
| `future::multicore` + COW shares model RAM | HIGH | Empirical: run with multicore + 4 workers; compare RSS sum vs single-worker RSS via `ps` |
| `butcher` reduces ranger workflows by 70–90% | MEDIUM | Empirical: measure with `lobstr::obj_size()` before/after on one of the existing models |
| `bundle::bundle()` is required for fork-safe XGBoost | HIGH | Documented in `bundle` README; reproducible test: load model in parent, fork, predict in child without bundle → segfault |
| `mlr3` model objects are within 1.2× of butchered tidymodels | MEDIUM-LOW | Validate by porting one model and measuring; my estimate is from 2024-era benchmarks |
| `terra::predict(cores>1)` oversubscribes with future workers | HIGH | Documented in terra; reproducible by running both and comparing wall time |
| `arrow::open_dataset()` predicate pushdown uses row-group statistics | HIGH | Documented in arrow R package |
| `ps` package works on Euler's RHEL kernel | MEDIUM | Test once on the cluster — package is pure C/cross-platform but cgroup quirks possible |
| `qs2` is the current recommended replacement for `qs` | MEDIUM | Verify with `available.packages()` on conda-forge — `qs` may still be preferred for stability |

**If web tooling becomes available**, validate first:
1. Current `butcher` and `bundle` README for any API changes since 0.3 / 0.1
2. `future` 1.34+ release notes for multicore safety changes
3. Latest `terra` predict() docs for `cores=` and `wopt=` interactions
4. mlr3 v0.20+ benchmark posts (mlr-org blog) for current memory characteristics

---

## Sources

(Web research blocked in this session — see Verification Gaps above.)

Internal sources read:
- `c:/Users/black/switchdrive/git/nascent-lulcc/.planning/PROJECT.md`
- `c:/Users/black/switchdrive/git/nascent-lulcc/.planning/codebase/STACK.md`
- `c:/Users/black/switchdrive/git/nascent-lulcc/environments/allocation_env.yml`
- `c:/Users/black/switchdrive/git/nascent-lulcc/src/allocation.r` (lines 680–740 + greps)
- `c:/Users/black/switchdrive/git/nascent-lulcc/src/transition_modelling.r` (greps for butcher/tidypredict/workflow)

External knowledge baseline (R 4.3–4.4 ecosystem, package authors and maintained docs as of training cutoff):
- `future` (Henrik Bengtsson) — multicore vs multisession semantics; COW behaviour
- `butcher`, `bundle`, `parsnip`, `workflows`, `recipes` (Tidymodels) — model object stripping & native-handle serialisation
- `ranger` (Marvin N. Wright) — `save.memory`, `respect.unordered.factors`
- `xgboost` R package — native handle / `xgb.save_raw` semantics
- `terra` (Robert J. Hijmans) — external pointer non-serialisability, `wrap`/`unwrap`, `predict`
- `arrow` R package — lazy datasets, predicate pushdown
- `mlr3` (mlr-org) — Learner state structure, mlr3spatial features
- `ps` (Gábor Csárdi) — cross-platform process introspection
