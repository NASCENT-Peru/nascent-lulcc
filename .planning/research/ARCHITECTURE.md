# Architecture Patterns — Memory-Efficient Parallel Spatial Prediction in R

**Domain:** LULCC pipeline allocation step (R + terra + tidymodels + Dinamica EGO)
**Researched:** 2026-05-05
**Mode:** Brownfield architecture redesign for `src/allocation.r` to eliminate OOM crashes on the ETH Euler HPC under `future::multisession`.

> **Source-availability caveat (read first).** External lookup tools (WebSearch, WebFetch, Bash/CLI) were not available in this research session, so claims that would normally be verified directly against `future`, `butcher`, `terra`, or `parsnip` documentation are marked **MEDIUM confidence (training data + codebase evidence)**. Findings derived from reading `src/allocation.r`, `src/transition_modelling.r`, `src/calibrate_allocation_parameters.r`, `scripts/run_allocation.r`, `environments/allocation_env.yml`, and `.planning/PROJECT.md` are **HIGH confidence**. Any decision below should be cross-checked against current `future`/`butcher`/`terra` docs before commit.

---

## TL;DR — Recommended Architecture

```
SLURM job (1 node, 1 task, N CPUs, ~M GB)
└── R parent process
    ├── Load: focal_matrices, ref_grid metadata, model_info, anterior_dt indices  (sequential, once per region)
    ├── Pre-compute: ALL neighbourhood rasters for region, write to scratch    (sequential, ~78s × N nhood preds)
    ├── Pre-load: butchered+tidypredict model objects into a list (lazy refs)  (sequential)
    └── future::plan(multicore, workers = N)        # Linux HPC: fork-based, copy-on-write
        └── future_map(transitions, ...)            # PARALLELISE OVER TRANSITIONS, not regions
            ├── Worker reads model from parent memory (CoW: not duplicated until written)
            ├── Worker reads nhood rasters from disk via terra (file-backed, lazy)
            ├── Predict on the sparse "from-class" cell subset (already the design)
            └── Write per-transition probability TIF to work_dir/probability_map_dir
        # After all transitions complete, parent thread:
    ├── Normalise probabilities (long-format rbindlist, already the design)
    ├── exec_dinamica()  (sequential, one DinamicaConsole subprocess per region)
    └── Mosaic posteriors (sequential, terra::merge)
```

**Three-layer change** (build in this order; each unlocks the next):

1. **Object-size layer** — Strip models to <50 MB each (butcher + ranger surgery + xgboost raw bytes). Switch save path so RF models always go through "ranger-surgery" not tidypredict (tidypredict-RF is known-bad for >10k rows; codebase already disables it).
2. **Parallelism layer** — Switch `future::multisession` → `future::multicore` on Linux HPC; switch unit of work from scenario-region-timestep to transition-within-region. Multicore + forking + read-only model list = effective "shared memory" via copy-on-write.
3. **Caching/IO layer** — Pre-compute neighbourhood rasters once per region to disk (file-backed terra), eliminating per-transition recomputation; invoke `DinamicaConsole` sequentially (one external binary per node) using `processx::run`'s blocking semantics.

**Memory budget (estimated, MEDIUM confidence):**

| Component | Current | Target |
|---|---|---|
| Per-model size in RAM | >1 GB | <50 MB |
| Resident set size per worker | unbounded (>12 GB observed) | ~1.5–3 GB |
| Total for 4 workers on 32 GB node | crashes | ~15 GB peak (fits with headroom) |

---

## Recommended Architecture (in detail)

### Component diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│ SLURM batch job  (cgroup memory limit = sbatch --mem)                │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ R parent process                                                │ │
│  │                                                                 │ │
│  │  Per-region init (sequential):                                  │ │
│  │   • models_list = lapply(model_files, read_butchered_model)    │ │
│  │     → kept in parent address space, READ-ONLY after this point │ │
│  │   • anterior, region_rast, ref_grid loaded as terra SpatRaster │ │
│  │     (already file-backed by terra; very small in-RAM footprint)│ │
│  │   • nhood_dir = compute_all_nhoods(anterior, focal_matrices)   │ │
│  │     → writes ~10–20 .tif to scratch; in-RAM map = dict of paths│ │
│  │                                                                 │ │
│  │  future::plan(multicore, workers = N)  (Linux ONLY)             │ │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐          │ │
│  │  │ fork  W1 │ │ fork  W2 │ │ fork  W3 │ │ fork  W4 │          │ │
│  │  │  shares  │ │  shares  │ │  shares  │ │  shares  │          │ │
│  │  │ models[] │ │ models[] │ │ models[] │ │ models[] │          │ │
│  │  │ via CoW  │ │ via CoW  │ │ via CoW  │ │ via CoW  │          │ │
│  │  │  – reads │ │  – reads │ │  – reads │ │  – reads │          │ │
│  │  │    nhood │ │    nhood │ │    nhood │ │    nhood │          │ │
│  │  │    .tif  │ │    .tif  │ │    .tif  │ │    .tif  │          │ │
│  │  │  – writes│ │  – writes│ │  – writes│ │  – writes│          │ │
│  │  │    prob  │ │    prob  │ │    prob  │ │    prob  │          │ │
│  │  │    .tif  │ │    .tif  │ │    .tif  │ │    .tif  │          │ │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘          │ │
│  │                                                                 │ │
│  │  Sequential:                                                    │ │
│  │   • Normalize probabilities                                     │ │
│  │   • exec_dinamica()  ── only ONE DinamicaConsole at a time     │ │
│  │   • mosaic posteriors                                           │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                      │
│  Scratch FS (/beegfs):                                      │
│    inputs/predictors/parquet_data/...     (read-only, lazy via arrow)│
│    outputs/transition_models/.rds         (read-only, butchered)     │
│    outputs/simulations/<scen>/<yr>/region_<r>/                       │
│      ├── nhood_cache/<class>_nhood_<m>.tif  (per-region, computed 1x)│
│      ├── probability_map_dir/NNN_id_trans_*.tif                      │
│      └── posterior.tif                                                │
└─────────────────────────────────────────────────────────────────────┘
```

### Data flow rules

| Object | Where it lives | Loaded by | Shared across workers? |
|---|---|---|---|
| `terra::SpatRaster` (anterior, ref_grid, region) | File on scratch + small in-RAM proxy | Parent (once) | Yes — file path is shared; each worker can `terra::rast()` it independently with negligible RAM |
| `focal_matrices` (rds, ~MB) | Parent R session | Parent (once) | Yes via fork CoW |
| `models_list` (butchered, <50 MB each × N transitions) | Parent R session | Parent (once, sequentially) | **Yes via fork CoW** — this is the key win |
| Parquet predictor partitions | Scratch FS | Worker, lazily via `arrow::open_dataset()` | No duplication — arrow handles its own memory map per process |
| Neighbourhood rasters | Scratch FS (pre-computed) | Worker, via `terra::rast(nhood_path)` | Shared via filesystem cache; each worker's resident copy is small (lazy/windowed reads) |
| Per-transition probability TIFs | Scratch FS | Each worker writes one | One file per transition; no contention |
| DinamicaConsole subprocess | OS process tree | Parent only (sequential) | Not parallelised — one at a time per region |

---

## Sub-question answers

### Q1. What is the right unit of parallelism?

**Recommendation: parallelise over TRANSITIONS within one (scenario, region, timestep), not over regions.** Keep scenarios and timesteps strictly sequential at the R level (use SLURM array jobs to fan out scenarios across nodes if wall-clock pressure remains).

| Unit | Pro | Con | Verdict |
|---|---|---|---|
| Scenario × region × timestep (current) | Maximum throughput in principle | Each worker re-does **all** N transitions → loads all models, all nhoods, all parquet → memory blows up; redundant work | **Reject.** This is what is causing OOM. |
| Region (within one scenario+timestep) | Simple to keep | Only 3 regions (Andes/Amazon/Coast) → at most 3-way parallelism; per-region work is still 30–60+ transitions sequentially → 385–472s × 30 = 3–4 hours per region | Suboptimal. |
| **Transition (within one region)** | Each worker holds 1 model + 1 from-class subset + nhood reads from disk; with multicore CoW the whole `models_list` is shared; high concurrency (N transitions) | Need to serialise the normalisation step at the end | **Recommended.** |
| Tiled raster chunks (within one transition) | Bounds peak RAM independent of region size | Most predictions already operate on the sparse "from-class" cell subset, not full raster — chunk overhead would exceed savings unless region cells > ~10M | Defer; revisit only if transition-level fails. |

**Why this works memory-wise:** the dominant cost per transition is the model load (~1 GB current, target ~50 MB after butchering) and the prediction matrix for the sparse from-class subset (typically 10⁵–10⁶ cells × ~30 predictors × 8 bytes ≈ 30–250 MB). Predictions for different transitions are independent, and the model list can be loaded once in the parent and shared via fork CoW. **MEDIUM confidence** — depends on actual butchered model size and from-class density.

**Why "transition × region" is preferred over "transition × region × scenario":** scenarios differ only via per-scenario probability adjustments (`lulcc.spatprobmanipulation`) and trans-rates tables; the *predicted probability rasters* are scenario-independent for the same `year_ant` because the dynamic predictors are SSP-keyed (see `ssp_name <- config[["scenario_to_ssp_mapping"]][[scenario]]` at allocation.r:1043). If you cache predicted probability rasters keyed by (region, year_ant, ssp_name), you can re-use them across scenarios that share an SSP. **HIGH confidence** from code reading.

### Q2. `future::multicore` vs `future::multisession` on SLURM HPC

**Recommendation: switch to `future::multicore` on Linux HPC for the allocation stage.** Keep `multisession` only for Windows local development.

The codebase (`src/calibrate_allocation_parameters.r:796–798`) currently asserts:

> `# multicore uses forking which can cause OOM issues on HPC clusters`
> `# multisession creates separate R sessions with isolated memory`

**This comment is the wrong way round for read-mostly workloads** and is the conceptual root of the OOM crash. Forked processes share read-only pages with the parent via copy-on-write: a 1 GB model loaded in the parent costs **0 additional RAM** until a worker writes to it. Multisession workers, by contrast, **always** receive a serialised copy of every closure capture and explicit export — so a 1 GB model becomes 4 GB across 4 workers.

| Property | `multisession` | `multicore` |
|---|---|---|
| Mechanism | Fresh R subprocess via `parallelly::makeClusterPSOCK`; communicates over sockets; each worker is a clean R session | `parallel::mcparallel` (POSIX `fork()`); workers inherit parent address space |
| Linux | Works | Works (recommended for read-mostly large data) |
| Windows | Works | **Disabled** by `future` even if requested |
| RStudio | Works | Disabled by default (RStudio prints a warning); set `options(parallelly.fork.enable = TRUE)` to override (only when you understand the risks) |
| Object transfer cost | Serialise → ship → deserialise once per worker per `future_map` call | Zero (memory is shared via CoW until mutation) |
| Memory at idle (4 workers, 1 GB read-only object in parent) | ~4 GB extra | ~0 extra |
| Memory if workers mutate the object | ~4 GB (was already paid up front) | up to ~4 GB extra (CoW page faults) |
| Risk: child holds open file handles, locks, threads | Low — clean R session | Medium — children inherit everything from parent (e.g. open ports) |
| Risk: thread-unsafe native libraries (BLAS, GDAL) | Low | Real but manageable; see mitigations below |

**MEDIUM confidence** on the specific behaviours; this matches the standard description of `future`'s strategies but should be re-verified against current `future` docs (https://future.futureverse.org/) before committing.

**SLURM-specific mitigations for `multicore`:**

1. **Single node, single task, multiple CPUs.** Use `--ntasks=1 --cpus-per-task=N --mem=M` (NOT `--ntasks=N`). Multicore forks within one process tree on one node — multi-node MPI-style parallelism would require `future.batchtools`.
2. **Set `parallelly.fork.enable = TRUE`** explicitly in `run_allocation.r` to guard against environments where it is off by default.
3. **Constrain BLAS threading.** Set `RhpcBLASctl::blas_set_num_threads(1)` or `OMP_NUM_THREADS=1`/`OPENBLAS_NUM_THREADS=1` before forking. Without this, each forked child can spawn its own multi-thread BLAS pool, causing CPU oversubscription. Critical if any prediction path uses matrix algebra (it doesn't here significantly, but xgboost respects `nthread`).
4. **Constrain xgboost threading.** When loading a butchered xgboost via `xgb.load.raw`, set `nthread = 1` on prediction (or rebuild Booster with `nthread = 1`) — xgboost's internal OpenMP pool plus N forked workers = oversubscription.
5. **Constrain `data.table` threading.** `data.table::setDTthreads(1)` before forking, for the same reason.
6. **Make the "shared" objects truly read-only inside workers.** Don't `[<-` or `setattr()` them. The `restore_ranger_importance_mode` helper currently mutates `model_obj$model$importance.mode`; do that **once in the parent** before the parallel section, not inside each worker — otherwise CoW triggers a page copy.
7. **Keep ALSO an env-var override.** Provide `ALLOCATION_PARALLEL_STRATEGY=multicore|multisession|sequential` so Windows dev still works and you can fall back to multisession on demand.

**Why the existing codebase comment ("multicore causes OOM") was wrong but understandable.** Multicore can blow up memory in two scenarios that look like "fork OOM":
  - Workers mutate large shared objects → CoW pages get copied → memory grows. The fix is to keep workers read-only.
  - SLURM accounting tools count RSS naively and double-count CoW-shared pages across processes → `MaxRSS` looks huge even though the kernel is sharing pages. The fix is to use cgroup memory accounting (which the kernel reports correctly) or trust `/proc/<pid>/smaps` `Pss` (proportional set size) instead of `VmRSS`. The existing `prof_toc()` reads `VmRSS` from `/proc/self/status` which is per-process resident memory and **does** count shared pages multiple times when summed across workers. This is a measurement artefact, not an actual memory cost.

### Q3. Pre-computing and sharing neighbourhood rasters

**Recommendation: shared disk cache (per-region, computed once at the start of the region run by the parent, written to scratch as compressed COG-style TIFs) — NOT per-worker recomputation, NOT shared in-memory raster.**

The current implementation (`allocation.r:1200–1227`) caches nhood rasters in an `nhood_raster_cache` env *within one transition loop on one worker*. Under multisession this means each worker recomputes the same nhood rasters → 4× the ~78s/transition cost across 4 workers, on top of the memory.

**Why disk cache wins over in-memory cache:**

1. **terra::SpatRaster is a thin pointer to a file or in-memory matrix.** A SpatRaster backed by a TIF on scratch costs ~KB in RAM; pixels are paged in lazily on `extract()`/`values()`. Forked workers inherit the file path "for free" via CoW; multisession workers can re-open the file with negligible cost.
2. **In-memory shared rasters via shared memory segments (`bigmemory`, `mmap`, `arrow::Table`) are an option** but add complexity for marginal benefit — terra already gives you memory-mapped behaviour via GDAL.
3. **Computation is deterministic per (anterior_raster, focal_matrix) pair**, so a content-addressed cache survives reruns within a region's lifetime.

**Cache design:**

```r
# Parent process, once per (region, timestep):
nhood_cache_dir <- file.path(region_work_dir, "nhood_cache")
ensure_dir(nhood_cache_dir)

# Discover ALL nhood predictors needed across ALL transitions in this region
all_nhood_needed <- unique(unlist(lapply(models_list, function(m) {
  preds <- get_saved_transition_predictors(m)
  grep("_nhood_", preds, value = TRUE)
})))

# Compute each one ONCE, write to disk
for (pred_name in all_nhood_needed) {
  out_path <- file.path(nhood_cache_dir, paste0(pred_name, ".tif"))
  if (!file.exists(out_path)) {
    r <- compute_single_nhood_raster(anterior, pred_name, focal_matrices, class_name_to_value)
    terra::writeRaster(r, out_path, datatype = "FLT4S",
                       gdal = c("COMPRESS=LZW", "TILED=YES", "BLOCKXSIZE=256", "BLOCKYSIZE=256"),
                       overwrite = TRUE)
  }
}

# Workers receive a NAMED CHARACTER VECTOR mapping pred_name -> path
nhood_paths <- setNames(file.path(nhood_cache_dir, paste0(all_nhood_needed, ".tif")),
                        all_nhood_needed)
# → cheap to fork-share
```

Inside the worker:
```r
nhood_stack <- terra::rast(nhood_paths[nhood_needed])
nhood_vals <- terra::extract(nhood_stack, as.matrix(from_data[, .(x, y)]))
```

**HIGH confidence** on the architectural pattern; **MEDIUM confidence** on the COG/tiled write parameters being optimal — verify with `terra::writeRaster` docs.

**Eviction:** delete `nhood_cache/` at end of region, before next region starts (each region has a different `anterior` raster, so the cache is region-scoped).

**Scratch-disk budget:** ~10–20 nhood rasters × ~size of one anterior raster (compressed, ~10–100 MB each) = ~1–2 GB per region. Trivial for HPC scratch.

### Q4. Minimising model object memory footprint

**Status: The codebase already has the right framework (butcher + tidypredict + custom ranger surgery). What's missing is (a) consistent application, (b) correct order of operations, and (c) some specific further trims.**

#### What `butcher` removes (MEDIUM confidence — based on standard butcher behaviour, verify against current docs)

| Axe function | Removes | Risk to predict() |
|---|---|---|
| `axe_call()` | The original function call captured in `$call` slots | None for predict; some print methods may regress |
| `axe_ctrl()` | Tuning control objects | None |
| `axe_data()` | Cached training data (e.g. `lm$model`, recipe `$template`) | None for predict on new data |
| `axe_env()` | Captured environments hanging off formulas/closures | Watch out for recipes that use `imp_vars(all_predictors())` etc — env may be needed at bake time. Test thoroughly. |
| `axe_fitted()` | Cached `$fitted.values` and similar | None for predict |

**Predict-compat invariant:** after butchering a `parsnip::model_fit`, you must still be able to call `predict(model_fit, new_data, type = "prob")`. The codebase's `predict_saved_butchered_prob` already exercises this path.

#### Specific further reductions needed

1. **ranger `$forest`** — already retained (necessary). The `predictions`, `inbag.counts`, `variable.importance`, `confusion.matrix` are correctly stripped (allocation.r:2462–2506). Verify `forest$child.nodeIDs`, `forest$split.varIDs`, `forest$split.values` are kept (these are the actual decision tree structure). **If RF model is still >50 MB after this, the `$forest` itself is large** — only knob left is `num.trees` (reduce at training time) and `min.node.size` (raise at training time to prune).

2. **xgboost** — current `xgb.save.raw` approach is correct and is the standard memory-minimising serialisation. Confirm `xgb.load.raw` followed by `predict` does not re-instantiate the training matrix. **MEDIUM confidence.**

3. **GLM** — should be tiny after butcher (just coefficients + family + link). If still large, the `$qr`, `$model`, `$data`, `$y`, `$prior.weights`, `$fitted.values`, `$linear.predictors`, `$weights`, `$residuals`, `$effects` slots can all be NULLed manually.

4. **recipes (`$template`)** — the codebase already does `trained_recipe$template <- NULL`. Good. Also worth inspecting `trained_recipe$tr_info`, `$last_term_info` and checking `trained_recipe$steps[[i]]` for cached internals (e.g. `step_dummy` keeps level lists which can be large for high-cardinality predictors).

5. **Drop `formula` environment.** `recipes::recipe(formula, data = head(data, 0))` creates a recipe with a tiny `template`. If the formula or any step holds onto the full training data via its captured environment, butcher's `axe_env()` should remove it but this should be verified.

6. **Top-level `final_workflow` vs. extracted parts.** The codebase already extracts `recipe` + `model_fit` into a minimal list — good. Don't save the whole `workflow` object: it carries blueprint, fit_objects, pre/post hooks, all of which are unnecessary for predict.

**Target: <50 MB per model file on disk, <100 MB resident.** **MEDIUM confidence** that this is achievable; if RF gets stuck above 100 MB, the model has too many trees (reduce `num.trees` from the typical default 500 to 100–200 — check generalisation impact first).

#### `predict_saved_butchered_prob` requires `parsnip` and `recipes` at predict time

`environments/allocation_env.yml` does NOT currently list `r-parsnip`, `r-recipes`, `r-ranger`, `r-xgboost`, `r-tidypredict`, or `r-butcher`. The script imports `workflows`, but `predict.model_fit` lives in `parsnip`, and `recipes::bake()` is called explicitly. **This is a latent runtime failure** for the butchered code path. Add to the env:

```yaml
# environments/allocation_env.yml additions:
- r-parsnip>=1.2.1
- r-recipes
- r-ranger
- r-xgboost=1.7
- r-tidypredict
- r-butcher    # only if you re-butcher at allocation time; otherwise not needed
```

**HIGH confidence** — this is a code-reading gap, not a docs claim.

### Q5. Tiled/chunked prediction with terra

**Recommendation: NOT NEEDED for this pipeline at current resolution. Defer; add only if a region's from-class subset exceeds ~10M cells.**

The current design (`generate_probability_maps`) already does the most important memory optimisation: it predicts only on the **sparse from-class cell subset**, not the full raster. For typical Peruvian regions at 100 m resolution:

- Andes/Amazon region full extent: ~10–40 M cells
- Cells in any one "from" class (e.g. forest, cropland): ~10⁵–10⁷
- Predictor matrix per transition: cells × ~30 predictors × 8 bytes ≈ 30 MB – 2.4 GB

Most transitions will have from-class subsets in the 10⁵–10⁶ range, giving a comfortable ~30–250 MB matrix. This fits in a worker.

**When tiled prediction would be needed:** if `from_data` exceeds ~10 M rows for any transition (e.g. a bare-soil class covering most of a region), the matrix could exceed 5 GB. In that case, the right pattern is:

```r
# Sketch — chunk the SPARSE from-class cells, not the raster grid
chunk_size <- 1e6L
n_chunks <- ceiling(nrow(from_data) / chunk_size)
pred_chunks <- lapply(seq_len(n_chunks), function(i) {
  rows <- ((i - 1L) * chunk_size + 1L):min(i * chunk_size, nrow(from_data))
  predict_saved_transition_prob(model, from_data[rows], log_file = log_file)
})
prob_values <- do.call(rbind, pred_chunks)[[2L]]
rm(pred_chunks); gc()
```

This is "row-chunk on the data.table" not "spatial-tile on the raster" — much simpler than `terra`'s blockwise machinery (`terra::blocks`, `terra::writeStart/writeValues/writeStop`).

**`terra::predict` has a built-in tiled mode** (`predict(rast, model, fun = my_predict_fn, na.rm = TRUE)`) that handles raster tiling internally. **MEDIUM confidence** — useful only if you can express your model as a function `(matrix-of-predictor-values) -> vector-of-probabilities`, which the butchered model wrappers can. But it requires *all* predictors be available as raster layers; the parquet-backed predictors would have to be rasterised first, defeating the sparse-cell optimisation. **Don't go this route** unless you abandon the sparse-cell design.

**Verdict:** keep the current sparse-cell design; add row-chunk fallback only if/when `from_data` ever exceeds ~5 M rows in practice (instrument `prof_toc` to log `nrow(from_data)` per transition to find out).

### Q6. Integrating Dinamica EGO in a parallel R job

**Recommendation: keep DinamicaConsole calls strictly sequential (one external binary running at a time per node). Use the parent process to invoke `processx::run`; do NOT call it from inside `future_map` workers.**

Reasoning:

1. **DinamicaConsole is a heavy native binary** that has its own threading, GDAL dependencies, and per-region working directory. Two concurrent invocations on one node would compete for CPU and (possibly) for GDAL temp files in `DINAMICA_HOME`. The codebase already passes `-disable-parallel-steps` (dinamica_utils.r:31), suggesting this is a known interaction concern.

2. **`processx::run` is blocking** — it doesn't fork the R process, so concurrent calls *within* `future_map` workers would each spawn a new DinamicaConsole, leading to N concurrent native binaries. **Avoid.**

3. **The natural sequence is:**
   ```
   Per region:
     parallel:   generate_probability_maps()    # CPU/RAM-bound; parallelise transitions
     sequential: setup_allocation_inputs()      # mostly CSV writes; cheap
     sequential: exec_dinamica()                # one DinamicaConsole at a time
   ```

4. **If you want parallelism across regions during the Dinamica step**, do it via SLURM array jobs rather than within-node parallelism: each array task = one (scenario, timestep) pair, runs all 3 regions through Dinamica sequentially on its own node. The natural parallelism in this pipeline is across SLURM array tasks, not across R workers.

5. **Logging.** Move Dinamica log files from `dirname(model_path)` to the project `logs/` dir (already a flagged TODO in the codebase at dinamica_utils.r:49). When sequential per region this is unambiguous; if you ever did want concurrent regions per node you would need per-region log file names.

**HIGH confidence** on (1)–(4) from code reading and OS reasoning; **MEDIUM confidence** on Dinamica's specific concurrency behaviour (Dinamica EGO documentation should be consulted).

---

## Patterns to Follow

### Pattern 1: Read-only shared objects via fork
**What:** Load all read-only state (models, focal matrices, schema) in the parent before `future::plan(multicore)`. Inside workers, never assign to these objects.
**When:** Every parallel section under multicore on Linux.
**Example:**
```r
# Parent
models_list <- lapply(model_files, readRDS)
models_list <- lapply(models_list, restore_ranger_importance_mode)  # mutate ONCE here
nhood_paths <- precompute_nhoods(anterior, all_nhood_preds, cache_dir)

future::plan(future::multicore, workers = N)
results <- furrr::future_map(seq_along(models_list), function(j) {
  m <- models_list[[j]]                # read-only ref into parent
  preds <- get_saved_transition_predictors(m)
  # ... predict, write TIF, return path
})
```

### Pattern 2: Separate the parallel and sequential phases explicitly
**What:** Don't interleave parallel R work with external-binary calls. Phase 1 = parallel R prediction; phase 2 = sequential native subprocess.
**Why:** Native binaries with their own threading defeat the point of fork-CoW and may leak file descriptors back into forked workers.

### Pattern 3: Cache by content hash, evict by scope
**What:** Cache neighbourhood rasters keyed by `(anterior_path, focal_matrix_id)`; evict at end of region.
**Why:** Avoids per-transition recomputation without growing unbounded across regions.

### Pattern 4: Lazy file-backed rasters everywhere
**What:** Keep rasters as `terra::rast(path)` references, not `terra::values()`-materialised matrices.
**Why:** A SpatRaster pointer is ~KB; the materialised matrix is GB. Use `terra::extract(rast, points)` at the latest possible moment.

### Pattern 5: Worker-scoped resource limits
**What:** Inside the parent, before forking:
```r
RhpcBLASctl::blas_set_num_threads(1)
data.table::setDTthreads(1)
Sys.setenv(OMP_NUM_THREADS = "1", OPENBLAS_NUM_THREADS = "1", MKL_NUM_THREADS = "1")
```
**Why:** Forked workers inherit these limits. Without them, N workers × M BLAS threads = NM threads all fighting for CPU.

---

## Anti-Patterns to Avoid

### Anti-Pattern 1: `future::multisession` for read-mostly workloads with large objects
**What:** Current pipeline.
**Why bad:** Each worker receives a serialised copy of every captured large object. Memory scales linearly with worker count.
**Instead:** Use `multicore` on Linux (this whole document).

### Anti-Pattern 2: Nested parallelism (futures + BLAS threads + xgboost nthread)
**What:** Default xgboost uses all available threads; default OpenBLAS uses all available threads. With 4 future workers each doing this, you get 4 × ncores threads.
**Why bad:** CPU oversubscription, context switches, performance worse than sequential.
**Instead:** Set thread count = 1 for all native libs before forking.

### Anti-Pattern 3: Mutating "shared" objects inside multicore workers
**What:** `model_obj$fit$importance.mode <- "none"` inside a forked worker.
**Why bad:** Triggers CoW; that 1 GB model now uses 5 GB across 4 workers + parent.
**Instead:** Mutate once in the parent before forking.

### Anti-Pattern 4: `terra::values()` of a full-region SpatRaster
**What:** Materialising the whole anterior or nhood raster as an R matrix.
**Why bad:** Defeats terra's lazy/file-backed design; pulls the whole grid into RAM.
**Instead:** `terra::extract(rast, points_or_cells)` at the sparse cells you actually need.

### Anti-Pattern 5: Trusting summed `VmRSS` across workers as "total memory used"
**What:** SLURM's `MaxRSS` reported per-task aggregates child processes' RSS, double-counting CoW-shared pages.
**Why bad:** Looks like 16 GB used when actual physical memory is 4 GB shared + 4 × 1 GB private = 8 GB.
**Instead:** For accurate accounting on Linux, sum `Pss` from `/proc/<pid>/smaps` across the worker tree, OR rely on cgroup memory accounting (`memory.current` in cgroup v2). SLURM's cgroup memory limit (set via `--mem`) is the real OOM trigger and is accurate.

---

## Suggested Build Order

This ordering is dependency-driven — each phase unlocks the next.

| Order | Phase | Why this order | Unblocks |
|---|---|---|---|
| 1 | **Re-butcher all existing model files** offline (one-shot script) — strip ranger surgery, ensure xgboost is xgb.save.raw, GLM minimal | Cannot reduce worker memory until per-model size is bounded. Also: validates the predict-compat path. | All subsequent memory work |
| 2 | **Add `parsnip`, `recipes`, `ranger`, `xgboost` to `allocation_env.yml`** | Without these, the butchered predict path fails on HPC | Phase 3 |
| 3 | **Pre-compute nhood rasters once per region (sequential, parent process)** before any parallelism | Removes 78s × N transitions of duplicate work; makes nhood data file-backed | Phase 4 |
| 4 | **Switch `future::multisession` → `future::multicore` on Linux** with thread-pinning + read-only model list pre-loaded in parent | Now memory benefit is realisable; this is the OOM fix | Phase 5 |
| 5 | **Switch unit of parallelism from region to transition-within-region** | Maximises concurrency now that workers are cheap | Phase 6 |
| 6 | **Cache predicted probability rasters by (region, year_ant, ssp_name)** for re-use across scenarios sharing an SSP | Big wall-clock saving across BAU+CUL etc. that share ssp245 | Phase 7 |
| 7 | **Move DinamicaConsole logs to project `logs/` dir; ensure sequential invocation** | Quality-of-life cleanup; not a correctness issue but flagged TODO | — |
| 8 (optional) | **Investigate row-chunked prediction** if any single transition's `from_data` exceeds 5M rows | Only worth it if instrumented data shows the need | — |

---

## Memory Budget Estimate (MEDIUM confidence)

Assumes Linux HPC node with `--mem=32G`, `--cpus-per-task=4`, `--ntasks=1`.

| Component | Size | Notes |
|---|---|---|
| R parent baseline (loaded packages, config) | ~500 MB | Standard R + tidymodels + terra + arrow |
| `models_list` (~30 transitions × <50 MB butchered) | ~1.5 GB | Loaded once in parent |
| `focal_matrices`, `class_name_to_value`, `anterior_dt` index | ~200 MB | Sparse cell index for region |
| Open arrow datasets (lazy) | ~10 MB | Pointers + schema only |
| nhood SpatRaster pointers (file-backed) | ~10 MB | Just paths + metadata |
| **Parent total (shared via CoW)** | **~2.2 GB** | |
| Per-worker private overhead | ~50–100 MB | R runtime + active prediction buffers |
| Per-worker peak (1 transition's from_data + pred result) | ~250–500 MB | Sparse subset, varies by from-class |
| **Per-worker peak (private + CoW-shared writes)** | **~300–600 MB** | |
| **4 workers total private** | **~1.2–2.4 GB** | |
| **Total resident (parent + 4 workers, accurate Pss)** | **~3.5–5 GB** | Fits in 32 GB with massive headroom |
| DinamicaConsole subprocess (sequential) | ~2–4 GB | Native binary, separate from R memory |

**Conservative SLURM request:** `--mem=16G --cpus-per-task=4 --ntasks=1` should fit comfortably. Current observed crash at `gc_max_vcells=12,125 MB` after a single model load suggests the un-butchered models alone are blowing past this — confirming the per-model-size problem is the root cause, not a fundamental impossibility.

---

## Scalability Considerations

| Concern | Per region (~5 M cells) | Per region (~50 M cells) | Per region (~500 M cells) |
|---|---|---|---|
| `anterior_dt` sparse index | ~150 MB | ~1.5 GB | ~15 GB — needs chunking |
| Per-transition `from_data` (typical 10% from-class) | ~30 MB | ~300 MB | ~3 GB |
| Per-transition `from_data` (90% from-class) | ~270 MB | ~2.7 GB | ~27 GB — definitely needs row chunking |
| nhood raster cache on disk | ~1 GB | ~10 GB | ~100 GB — still fine on scratch |
| Probability map TIFs on disk | ~1 GB | ~10 GB | ~100 GB |

For Peru at 100 m, regions are well within the "50 M cell" column; the design comfortably accommodates this. Only if resolution dropped to 30 m globally would the 500 M column become relevant.

---

## Confidence Summary

| Claim | Confidence | Basis |
|---|---|---|
| `multisession` duplicates objects per worker | HIGH | Standard `future` documentation; consistent across versions |
| `multicore` uses fork + CoW on Linux | HIGH | Same as above |
| `multicore` is disabled on Windows by `future` | HIGH | Same as above |
| Read-only forked memory does not duplicate | HIGH | POSIX fork semantics; Linux CoW is a kernel guarantee |
| Sparse from-class cell prediction is already correct in codebase | HIGH | Direct code reading of `generate_probability_maps` |
| Pre-computing nhoods once per region eliminates ~78s × N transitions of duplicate work | HIGH | Direct code reading; current cache is per-worker per-transition-loop |
| `butcher::axe_*` preserves predict() compatibility | MEDIUM | Standard butcher behaviour; needs verification on this specific tidymodels version |
| ranger surgery (drop `predictions`, `inbag.counts`, importance) preserves predict() | HIGH | Codebase already does it and works |
| xgboost `xgb.save.raw` + `xgb.load.raw` preserves predict() | MEDIUM | Codebase already does it and works (allocation.r:266); standard xgboost pattern |
| `tidypredict` is unsuitable for RF with >10k rows | HIGH | Codebase already has this guardrail |
| `r-parsnip`/`r-recipes`/`r-ranger` are missing from `allocation_env.yml` | HIGH | Direct file reading |
| Dinamica EGO should be invoked sequentially | HIGH | Code already sets `-disable-parallel-steps`; OS-level reasoning |
| Memory budget estimates | MEDIUM | Order-of-magnitude estimates; depends on actual butchered sizes |
| The codebase comment "multicore causes OOM" is wrong for read-mostly workloads | HIGH | OS-level fact about CoW; the comment conflates measurement artefact (summed RSS) with actual physical memory use |

---

## Sources

- **Codebase (HIGH confidence):**
  - `c:/Users/black/switchdrive/git/nascent-lulcc/.planning/PROJECT.md` — crash profile, known blockers, decisions log
  - `c:/Users/black/switchdrive/git/nascent-lulcc/.planning/codebase/ARCHITECTURE.md` — pipeline topology, parallelism model statement
  - `c:/Users/black/switchdrive/git/nascent-lulcc/src/allocation.r` — current allocation orchestration, `generate_probability_maps`, profiling helpers, butchered/tidypredict prediction paths
  - `c:/Users/black/switchdrive/git/nascent-lulcc/src/transition_modelling.r:2138–2546` — `save_minimal_model`, ranger surgery, xgboost raw serialisation, butcher invocation
  - `c:/Users/black/switchdrive/git/nascent-lulcc/src/calibrate_allocation_parameters.r:780–820` — current (incorrect) rationale for choosing multisession on HPC
  - `c:/Users/black/switchdrive/git/nascent-lulcc/src/dinamica_utils.r` — `exec_dinamica`, `processx::run` blocking semantics, `-disable-parallel-steps` flag
  - `c:/Users/black/switchdrive/git/nascent-lulcc/scripts/run_allocation.r:165–168` — current `future::plan(future::multisession, workers = num_workers)`
  - `c:/Users/black/switchdrive/git/nascent-lulcc/environments/allocation_env.yml` — current allocation env (missing parsnip/recipes/ranger/xgboost)

- **Authoritative external references to verify before implementation (NOT consulted in this session due to tool unavailability):**
  - https://future.futureverse.org/ — `future::multicore` vs `multisession` semantics
  - https://future.futureverse.org/articles/future-1-overview.html — overview of strategies
  - https://parallelly.futureverse.org/ — `availableCores()` SLURM-aware behaviour
  - https://butcher.tidymodels.org/articles/butcher.html — what each `axe_*` removes; predict-compat guarantees
  - https://rspatial.github.io/terra/reference/predict.html — terra's tiled predict pattern
  - https://rspatial.github.io/terra/reference/blocks.html — chunked raster I/O
  - https://parsnip.tidymodels.org/reference/predict.model_fit.html — confirm predict path on butchered model_fit
  - Dinamica EGO 8 user guide — concurrency/threading guarantees of DinamicaConsole

**Recommendation: before merging the implementation that follows from this research, spot-check at minimum the `future::multicore` semantics under SLURM and the current `butcher` axe behaviour against the latest docs.**
