# Feature Landscape — Allocation Stage Hardening

**Domain:** R-based spatial machine-learning pipeline (LULCC simulation, `future`/`furrr` parallelism, `terra` rasters, `tidymodels`/RF/XGBoost prediction, Dinamica EGO 8 subprocess)
**Researched:** 2026-05-05
**Scope:** Features needed to make Stage 7 (allocation) production-grade on the ETH Euler HPC cluster
**Confidence:** HIGH for codebase-derived claims; MEDIUM for ecosystem patterns (built on established R parallel/spatial knowledge — external verification via Context7/WebSearch/Brave/Exa was unavailable in this session and several "specific package version" claims should be re-validated against current docs before adopting).

---

## Orientation

The pipeline already has *partial* implementations of most table-stakes observability features (per-region logs, opt-in profiling hooks, `log_msg()`). The work is mostly **completion, repair, and propagation** — not greenfield invention. This document is organized around that reality:

- **Table stakes** = features the pipeline must have for the allocation stage to be diagnosable and finishable on HPC. Several are partially implemented.
- **Differentiators** = features that would make the pipeline robust enough for re-use across study areas / contributors and resilient to single-worker failures.
- **Anti-features** = capabilities that sound good for a "production" pipeline in general but would add complexity without payoff for *this* project (single-user research pipeline, fixed scenarios, HPC batch deployment, no web layer).

For each feature, the **Status in codebase** column tells you whether the work is greenfield, completion of an existing skeleton, or a repair.

---

## Table Stakes

These must exist for the allocation stage to be usable. Without them, every crash is a black box and every long run is a gamble.

### Observability — Error Capture

| Feature | Why Expected | Complexity | Status in codebase | Notes |
|---|---|---|---|---|
| **Structured error capture inside `furrr::future_map`** with full stack trace surfaced to per-region log | Currently a worker OOM kill produces only `MultisessionFuture interrupted` with no stack trace. Without this, every crash requires re-running with extra instrumentation to reproduce. | Medium | Partial (per-region log exists; no `tryCatch` wrapper inside the worker body) | Wrap the body of `run_allocation_one_timestep`'s inner `future_map` in `withCallingHandlers` + `tryCatch` that captures `sys.calls()` / `rlang::trace_back()` and writes a structured failure record (`status=fail`, `error=<msg>`, `traceback=<lines>`) to the region log *before* re-throwing. `future` does forward conditions, but a worker killed by the OS (OOM-killer) never gets to throw — see "Worker death detection" below. |
| **Worker death detection (signal vs. error distinction)** | `MultisessionFuture interrupted` covers both R-level errors and SIGKILL from cgroup OOM-killer. These need different remediation (code bug vs. memory bound). | Medium | Missing | After `future_map` returns, check each result: a worker killed externally produces a specific `FutureError` class. Log `cause=oom_kill` vs `cause=r_error` so the region log says *why*. Cross-reference with SLURM `sacct -j $JOBID --format=MaxRSS,ExitCode` for ground truth on memory cap hits. |
| **Pre-flight environment validation** at allocation entry | The pipeline currently fails late on missing env vars (`DINAMICA_EGO_8_HOME`), missing packages (`r-raster` not in `allocation_env.yml`), missing files (intervention masks at wrong path). Each is a 30+ minute lost run on HPC. | Low | Missing | At top of `allocation.r`, call a `validate_allocation_preconditions(config)` that asserts: env vars present, terra temp dir writable, all referenced model `.rds` files exist, all referenced intervention masks exist, all required R packages loadable, Dinamica binary executable. Fail fast with a single clear error listing all gaps. |
| **Structured exit summary per region** (success / fail / skipped + counts) | Currently you have to grep through long logs to know what completed. A single end-of-run summary makes it trivial to know "Andes done, Amazon failed at transition 3, Coast skipped". | Low | Missing | One line at end of each region: `RESULT region=andes status=ok transitions_done=12 transitions_failed=0 elapsed=<s> peak_rss=<MB>`. Aggregate across regions in the orchestrating script. |

### Observability — Memory Reporting

| Feature | Why Expected | Complexity | Status in codebase | Notes |
|---|---|---|---|---|
| **Working RSS / peak-RSS sampling on the platform that actually runs the workload** | `prof_tic`/`prof_toc` already exist and read `/proc/self/status` (VmRSS/VmHWM), but the codebase reports `rss_before=NAMB` in newer runs — the field is broken end-to-end. PROJECT.md confirms this is unknown root cause. | Medium | **Repair** — infrastructure exists in `src/allocation.r` lines 42–160 | Diagnose why the field is `NAMB` (most likely: the profile env var isn't propagated into `multisession` workers, so `prof_tic()` returns NULL and the formatter prints `NA` joined with "MB"). Test: ensure `Sys.setenv(ALLOCATION_PROFILE = "TRUE")` is set inside the worker (not just in the parent), or use `furrr_options(globals = TRUE)` to capture it. |
| **External-process memory accounting** (Dinamica subprocess RSS) | `prof_toc` only reads R's own `/proc/self/status` — Dinamica EGO is a child process, so its RSS is invisible. The OOM kills may originate in Dinamica, not R. | Low | Missing | At `run_allocation_dinamica` entry/exit, capture child PID and read `/proc/<pid>/status` peak. Or: rely on SLURM cgroup accounting — `sacct -j $JOBID.batch --format=MaxRSS` after the job, parsed back into a summary. The cgroup view is authoritative on HPC and includes all children. |
| **Per-worker memory ceiling logged at startup** | Without knowing "this worker was given 32GB," every memory log line is uncalibrated. | Low | Missing | At worker start: read `/sys/fs/cgroup/memory.max` (cgroup v2) or `SLURM_MEM_PER_CPU * SLURM_CPUS_PER_TASK`, log `MEM_LIMIT region=<x> limit=<MB>`. |

### Observability — Progress Tracking

| Feature | Why Expected | Complexity | Status in codebase | Notes |
|---|---|---|---|---|
| **Per-transition progress lines with ETA hint** | Allocation runs 4 scenarios × 3 regions × 8 timesteps × N transitions, hours-to-days. A user has to know "at 60% in 4h" to plan. | Low | Partial — `log_msg` calls exist for major stage entries; no count-based progress | Inside the transition loop, log `PROGRESS region=x scenario=y timestep=z transition=i/N elapsed_so_far=<s>`. With `furrr` use `progressr::with_progress` + `progressor()` to surface progress out of workers (works across multisession). |
| **Heartbeat from inside `predict()`** | The predict stage takes 385–472s per transition — currently a 7-minute silence per transition. If a worker is stuck, you can't tell vs. just slow. | Medium | Missing | Either chunk the prediction (see "Chunked prediction" below — gets you natural heartbeats per chunk) or, if prediction stays monolithic, fork a watcher thread that touches a heartbeat file every 30s. Chunking is the right answer here because it solves both observability and memory. |
| **Stage-level timing roll-up at region end** | `prof_toc` already logs per-stage timings; add a roll-up that says "of the X seconds this region took, predict=Y%, nhood_extract=Z%, dinamica=W%". | Low | Partial — line-level timings exist; no aggregation | Pure post-processing of the existing `PROFILE` lines; can be done in a small R script that scans the per-region log. |

### Observability — Logging Hygiene

| Feature | Why Expected | Complexity | Status in codebase | Notes |
|---|---|---|---|---|
| **`log_msg()` propagated into all inner allocation functions** | TODO in `allocation.r` line 762 explicitly calls this out. Today, `setup_allocation_inputs` and `run_allocation_dinamica` write to stdout (captured by SLURM `.out`) but not to the per-region log file, so multi-region debugging requires correlating across two log streams. | Low | **Repair** — TODO present | Add `log_file = NULL` parameter to the inner functions; default to `NULL` (caller decides), pass through from `run_allocation_one_timestep`. Same pattern for `compute_single_nhood_raster`, `dist_calc_functions.r` `print()` calls. |
| **Centralized Dinamica EGO log directory** | TODO in `dinamica_utils.r` line 49. Currently logs are scattered across hundreds of region work directories — `grep` across all of them takes minutes. | Low | **Repair** — TODO present | `exec_dinamica` accepts `log_dir`; default to `config$log_dir`. |
| **Replace `print()` and `cat()` debug calls with `log_msg()`** | `dist_calc_functions.r:83,317`, `implement_spatial_interventions.R:425` use `print`/`cat` which bypass the log file. | Low | **Repair** — listed in CONCERNS.md | Mechanical replacement. |

### Memory Bound Per Worker

| Feature | Why Expected | Complexity | Status in codebase | Notes |
|---|---|---|---|---|
| **Bounded model-object size** (target: <100MB per loaded model, currently >1GB) | This is the primary OOM driver: each `multisession` worker holds an independent copy of every model loaded; 12+ transitions × 1GB × N workers exceeds any reasonable node. | High | Partial — `butcher` and `tidypredict` are in the stack but not consistently applied; mlr3 evaluation is `MLR3-01` in PROJECT.md | Apply `butcher::butcher()` aggressively at *save* time in `transition_modelling.r` (so cached models are already small) — not at load time, which would still pay the disk I/O cost. Audit which model components survive butchering and break `predict()`; allocation already has fallback paths for this in `allocation.r` lines 463–522. |
| **Lazy / on-demand model loading inside the worker, with explicit free** | A single worker handling N transitions today loads model 1, predicts, leaves it in memory, loads model 2, etc. Only the rolling working set should live at any moment. | Low | Partial — model load is per-iteration; no explicit `rm` + `gc` between transitions in the predict loop | After each transition's `predict()`, `rm(fitted_wf, processed_data, predictions); gc(full = TRUE)`. Already done at region end (line 796–797). |
| **Bounded prediction working set** via chunked / tiled prediction | `predict()` over the full region raster currently materializes the entire predictor data frame — at high resolution this is the main RAM peak. | Medium | Missing | See "Chunked prediction over rasters" below — this is both a memory and observability fix. |

### Checkpoint / Skip-Already-Done

| Feature | Why Expected | Complexity | Status in codebase | Notes |
|---|---|---|---|---|
| **Resume-from-where-you-left-off at the (scenario × timestep × region) granularity** | A 12-hour HPC job that crashes at hour 10 should not redo hours 0–9. The natural unit is the region's posterior raster: if `posterior_<region>.tif` exists and is non-empty, skip. | Low | Missing | At entry of `run_allocation_one_timestep`'s inner `future_map`, check whether the expected output exists (`file.exists(posterior_path)` plus a sanity check on dimensions / non-zero size). If yes, log `SKIP region=x reason=already_done` and return the path. |
| **Atomic write of completion markers** | A partial output file from a crashed worker would be mistaken for "done" by the resume logic above. | Low | Missing | Write outputs to `<name>.tmp.tif`, then `file.rename` to `<name>.tif` on success. `terra::writeRaster` doesn't do this natively — wrap in `write_raster()` (already in `utils.r`) and add the rename step there. |

### Path & Environment Portability

| Feature | Why Expected | Complexity | Status in codebase | Notes |
|---|---|---|---|---|
| **No hardcoded absolute paths in active source** | `simulation_trans_rates_prep.r` line 181 (`E:/...xlsx`), `calibration_predictor_prep.r` line 17 (`E:/terra_temp`), `hpc_common.sh` lines 13/89/114 (`/cluster/.../bblack/...`). Each is a hard-stop on HPC or for any other user. | Low | **Repair** — listed as PIPE-01/03/04 in PROJECT.md | Replace with `config[[...]]` lookups, `Sys.getenv("TERRA_TEMP", unset = tempdir())`, `$USER`. |
| **`raster` → `terra` migration in active source** | 73 `raster::` calls in non-`old/` source. `allocation_env.yml` doesn't include `r-raster`, so allocation will hard-fail anywhere a `raster::` call is reached. Already a latent bomb. | Medium | **Repair** — listed as PIPE-05 | Mechanical port: `raster::raster` → `terra::rast`, `raster::stack` → `terra::rast` (multilayer), `raster::overlay` → `terra::app`/`terra::lapp`, etc. |
| **`DINAMICA_EGO_8_HOME` documented in `.env.template`** | Listed as a Gap in CONCERNS.md. Required by `dinamica_utils.r:39` but not documented anywhere. | Trivial | **Repair** | Add to `.env.template` with comment + example HPC path. |

---

## Differentiators

These would make the pipeline robust beyond just "finishes once on HPC" — re-runnable, debuggable by another person, and resilient to single-failure modes. Worth doing once the table stakes are in.

### Chunked Prediction Over Rasters

| Feature | Value Proposition | Complexity | Notes |
|---|---|---|---|
| **Chunked / tiled prediction with `terra::predict()` or manual `terra::blocks()` loop** | Predict step is currently 385–472s per transition over the full region — the dominant cost AND a memory peak (all predictor cells materialized at once). Chunking solves both: bounded memory per chunk and natural progress heartbeats. | Medium | The conceptual options for spatial ML prediction over large rasters in R are: (a) `terra::predict(raster, model, fun = ..., na.rm = TRUE)` which iterates internally in blocks if `terra` decides the raster is "too big to fit" — but this depends on `terraOptions(memfrac=...)` and is only as good as the `fun` you pass; (b) explicit `terra::blocks(r)` loop where you control chunk size; (c) `gdalcubes` or `stars`-based windowed iteration. For RF/XGBoost where the model object is the limiting factor, (b) gives you the most control. The current `allocation.r` flow extracts predictor values into a `data.table` first (lines 1310–1370) — this is what materializes everything at once. Refactor to: iterate cell blocks → for each block extract predictors → predict → write block to output → release. Confidence: MEDIUM (general pattern is well-established in `terra` ecosystem; specific block-size sweet spot for RF/XGBoost on this region's pixel count needs empirical tuning). |
| **Optionally: `tidypredict`/SQL-translated prediction for RF/GLM** | Bypasses R model objects entirely — translate model to SQL/dplyr code, run via DuckDB/Arrow over Parquet, get predictions back. Eliminates the per-worker model duplication problem at the source. | High | Already in stack (`tidypredict`) and partially used (`allocation.r:401`). Coverage is limited (works well for GLM/some RF; XGBoost translation is incomplete). Confidence: MEDIUM — `tidypredict` is established but its XGBoost coverage as of training-data cutoff is partial; verify before committing this as a strategy. |

### Checkpoint / Restart

| Feature | Value Proposition | Complexity | Notes |
|---|---|---|---|
| **Per-transition output caching** (probability map per `region × timestep × transition`) | Today even a successful region re-runs every transition's `predict()` (10+ minutes each) when called again. With per-transition cache: re-run after a single transition's bug fix touches only that transition. | Low | Same pattern as the region-level resume above, one level finer. Cache key = `(scenario, timestep, region, transition_id, model_hash, predictor_hash)`. The `model_hash` is critical: stale cache after model retraining is a silent correctness bug. Use `digest::digest(file = model_path)` or just `file.mtime`. |
| **`furrr`-aware retry on transient failures** | Some failures are transient (filesystem hiccup, transient network on `/cluster/scratch`). A per-region retry-once-with-fresh-worker policy avoids losing 4 hours of work to a 1-second blip. | Medium | `furrr::future_map` does not retry natively. Wrap each region call in a `purrr::insistently(rate = rate_backoff(max_times = 2))` *outside* the future, so retry spins up a fresh worker (escapes any worker-corrupt state). Don't retry on `cause=oom_kill` (will just re-fail). |
| **`drake`-style or `targets`-style dependency graph** for the pipeline | Reproducible re-runs that automatically detect "input X changed → invalidate downstream Y, Z". The natural fit for a 7-stage pipeline. | High | The R ecosystem standard here is **`targets`** (successor to `drake`). For *this* project: the pipeline is already structured as 7 SLURM-chained scripts, and PROJECT.md "Out of Scope" is silent on `targets` adoption. **Recommend deferring** — adopting `targets` is a meaningful refactor and the SLURM dependency chain plus per-region resume covers ~80% of the value. Revisit only if the pipeline grows beyond one researcher. Confidence on this recommendation: MEDIUM — based on project scope signals, not benchmark data. |

### Caching Expensive Intermediates Across Workers

| Feature | Value Proposition | Complexity | Notes |
|---|---|---|---|
| **On-disk cache for neighbourhood rasters** (cross-worker, cross-run) | Nhood extract is ~78s per transition × ~12 transitions × 3 regions × 8 timesteps × 4 scenarios. The matrices themselves don't change between scenarios — they're a function of (LULC year, class, kernel matrix), all of which are stable. | Low | The current cache is an in-process `new.env()` (`allocation.r:1200`) — does not survive worker restart and is duplicated across workers. Replace with a disk cache keyed on `digest::digest(list(lulc_year, class_name, matrix_id))`, stored as `qs::qsave` (faster than `saveRDS` for SpatRaster proxies — but verify SpatRaster serialization roundtrip works through `qs`; `terra` objects are notoriously non-portable across sessions because they hold C++ pointers — the safer pattern is to cache as a `.tif` via `terra::writeRaster` and re-`rast()` it). Confidence: HIGH on the "terra objects don't serialize cleanly" gotcha; this is a well-known terra limitation. |
| **Shared read-only data via memory-mapped Parquet / on-disk Arrow** | Predictor data loaded per-transition by each worker. With a memory-mapped Arrow dataset, OS page cache deduplicates across workers automatically (Linux only). | Medium | Already partially in place — Parquet datasets are read by `arrow::open_dataset()` per worker. The win is letting the OS share pages: each worker calls `arrow::open_dataset()` (cheap, lazy), the actual read happens once and the OS keeps the pages hot for subsequent worker reads. No code change needed beyond confirming `arrow::open_dataset` is used (not `read_parquet` which materializes). |
| **Switch `future::multisession` → `future::multicore` on Linux/HPC** | `multicore` is fork-based: read-only objects (loaded models, large rasters) are shared via copy-on-write, eliminating the per-worker model duplication. This is *the* lever for the OOM problem if mlr3 doesn't pan out. | Low | Already noted in PROJECT.md "Key Decisions" as `⚠️ Revisit`. Caveats: `multicore` is Linux-only (fine — that's where the problem is); `multicore` does not work inside RStudio (fine — HPC is non-interactive); `multicore` interacts badly with some C++ libraries that hold thread state across fork (XGBoost in particular has had fork-safety issues — verify before committing). Use `future::plan(future::multicore, workers = N)` on HPC and keep `multisession` as the local-Windows fallback via `if (.Platform$OS.type == "unix") multicore else multisession`. Confidence: MEDIUM — fork-based parallelism with XGBoost has historically had issues; needs an empirical smoke test on Euler before committing. |

### Testing for Spatial R Pipelines

| Feature | Value Proposition | Complexity | Notes |
|---|---|---|---|
| **`testthat` unit tests for pure helpers** | `src/utils.r` (write_raster type detection, log_msg formatting), `src/setup.r` (config loading), the CVXR optimization helpers. Not slow, runs on any machine, catches regressions during the active refactor. | Low | `testthat` is in DESCRIPTION but `tests/` does not exist. PROJECT.md says "Out of Scope: Automated test suite — observability improvements substitute." This is defensible for the *full pipeline* but **not** for pure-function helpers — those are essentially free to test and the absence of tests is making the active refactor risky. **Recommend a minimal `tests/testthat/` covering only pure utilities**, not the parallel/spatial machinery. |
| **Tiny-raster integration test for allocation** | A 100×100 cell synthetic LULC raster, a synthetic model, a synthetic config — runs the allocation pipeline end-to-end in <30s. Catches structural bugs (path resolution, raster CRS mismatch, model save/load roundtrip) without needing HPC. | Medium | This is the highest-leverage test investment. Spatial pipeline gotchas (CRS/extent/resolution mismatch, NA propagation, `terra` C++ pointer staleness) cannot be caught by unit tests on pure R code, and waiting for a 12-hour HPC run to reveal them is the current cost. Pattern: synthesize fixtures with `terra::rast(nrows=100, ncols=100, ext=..., vals=sample(...))`; use `withr::with_dir(tempdir(), ...)`. |
| **Snapshot tests for prediction outputs** | When refactoring (e.g., chunked prediction), confirm bit-equivalent (or numerically-equivalent within tolerance) outputs vs. baseline. | Low | `testthat::expect_snapshot_value` for small numerical outputs; for raster outputs, hash with `digest::digest` after rounding to a fixed precision, or compare with `terra::all.equal`. |
| **Property tests for path/config resolution** | After the path-portability refactor, want confidence that "config X resolves to Y" on Linux *and* Windows. | Low | Parameterized `testthat` cases that mock `Sys.getenv()` and check resolution. Skips the actual filesystem. |

### Reproducibility

| Feature | Value Proposition | Complexity | Notes |
|---|---|---|---|
| **Fixed `set.seed` for nhood matrix generation** | `nhood_predictor_prep.r` lines 80–101 generate random matrices with no seed (FIXME in code). The `.rds` cache hides this most of the time but cache-cold runs produce different predictors → different models → different allocations. | Trivial | **Repair** — explicit FIXME in code. Add `set.seed(<config_value>)`. |
| **Run manifest** at the start of each allocation run | Single JSON written to the run dir capturing: git SHA, R version, package versions (`sessionInfo()`), config snapshot, env vars, SLURM job ID, hostname, start time. | Low | Pure observability win, ~50 LOC. Makes "this run was different from that run" answerable retroactively. Can be appended to from `prof_mem_summary` at end. |

---

## Anti-Features

Things that look like best practices in a generic "production pipeline" template but would be wasted complexity for *this* project (one researcher, fixed scenarios, HPC batch deployment, no web layer, no compliance regime).

| Anti-Feature | Why Avoid (in this project's context) | What to Do Instead |
|---|---|---|
| **Full `targets` / `drake` pipeline rewrite** | The pipeline already has SLURM dependency chaining (one stage feeds the next). The marginal value of a `targets` graph is automatic invalidation — but at this project's iteration cadence (active research, frequent intentional re-runs of single stages), explicit `Rscript run_allocation.r` from the user is fine. The cost is a multi-week refactor that touches every script. | Per-stage skip-if-output-exists logic (the resume feature in Table Stakes). 95% of the value at 5% of the effort. |
| **`logger` / `lgr` / `futile.logger` / structured JSON logging stack** | The project already has a working `log_msg()` helper writing timestamped per-region files. Real win is *propagating* that helper into inner functions and adding structure to the message strings (key=value), not swapping in a new logging framework that adds a dependency and a learning curve. | Define a small set of message conventions (`PROFILE region=x stage=y elapsed=z`, `RESULT region=x status=ok`, `PROGRESS scenario=x ...`) in `log_msg`'s docstring; keep using `log_msg`. |
| **Distributed task queue (Redis/RabbitMQ/Celery-equivalent)** | SLURM *is* the task queue here. Allocation is one job array; per-region work is a `furrr` parallel inside one node. Adding a separate queue is solving a problem the pipeline doesn't have. | Use SLURM job arrays + `furrr` within-node parallelism. Already the design. |
| **Real-time monitoring dashboard (Grafana / Prometheus / Shiny live view)** | One-shot batch jobs on HPC, results written to disk. The user reads the per-region log when the job completes (or by `tail -f` while it runs). A dashboard would require deploying a service the HPC environment doesn't host. | Post-hoc log summarization script (`scripts/summarize_allocation_run.r`) that reads all region logs and produces a single Markdown/HTML summary on demand. Cheap and serves the same purpose. |
| **CI/CD with auto-deploy** | "Deployment" is `git pull && sbatch submit_allocation.sh` on the HPC login node. There's nothing to CD into. PROJECT.md "Out of Scope" already excludes this. | A pre-commit hook running `lintr` + `styler` if anything at all. Even that is optional. |
| **Containerization (Docker/Singularity) of the entire pipeline** | The pipeline already uses `micromamba` conda envs per stage on HPC. Singularity would re-solve the same problem with more friction (HPC Singularity workflows are painful for iterative dev). | Keep the per-stage conda envs, document them in `setup_environments.sh`, ensure `r-raster` is added to `allocation_env.yml`. |
| **Distributed model serving** (a model server the workers call over HTTP) | Adds a network hop and a service to keep alive for what is fundamentally a per-job batch problem. The model duplication problem is better solved by `multicore` fork-sharing or model object reduction. | `multicore` plan + `butcher`/`tidypredict` model size reduction. |
| **Generic LULCC framework for arbitrary study areas** | PROJECT.md "Out of Scope": "Generalisation to study areas other than Peru — config and path structure are Peru-specific by design." | Don't build for hypothetical future Switzerland/Ecuador/etc. runs. Configurability exists where it pays off (paths, scenarios within the fixed BAU/NAT/CUL/SOC frame); no further. |
| **Property-based / fuzzing tests on raster math** | Fuzzing `terra` operations is wildly disproportionate to the failure modes seen here — which are infrastructure bugs (paths, env vars, parallel duplication), not subtle numerical bugs in raster math. | Snapshot tests on small fixed fixtures (Differentiator above). |
| **`mlr3` adoption purely for ecosystem reasons** | PROJECT.md flags this as `MLR3-01` — *evaluate* feasibility before committing. Switching frameworks is a multi-week effort with prediction-output-compatibility risk. | Evaluate only on the criterion that matters: does mlr3 produce models <100MB while preserving prediction parity with the existing tidymodels pipeline? If `butcher` + `tidypredict` can hit that bar, **don't switch**. |

---

## Feature Dependencies

```
Pre-flight env validation ─┐
                           ├─→ All other allocation features (fail-fast saves debug time)
Path/raster portability ───┘

Working RSS reporting (repair) ─→ Memory bound diagnosis ─→ multicore vs. multisession decision
                                                         ─→ Model size reduction strategy choice
                                                         ─→ Chunked prediction sizing

log_msg propagation (repair) ─→ Structured error capture in workers
                              ─→ Per-region exit summary

Atomic write of completion markers ─→ Skip-already-done resume

On-disk nhood cache ─→ (independent — pure speedup)

Tiny-raster integration test ─→ Safety net for all subsequent refactors
```

**Order to do them:**

1. **Repairs first** (lowest risk, unblocks everything else): RSS profiling, `log_msg` propagation, hardcoded paths, `r-raster`/`terra` migration, env-var documentation, nhood seed, `DINAMICA_EGO_8_HOME` documented.
2. **Pre-flight validation + structured error capture** — turns silent failures into actionable failures.
3. **Memory bound** — `multicore` plan on Linux + butcher/tidypredict audit + chunked prediction.
4. **Resume / skip-already-done** — the cheapest insurance against losing 12-hour runs.
5. **Differentiators** (on-disk nhood cache, tiny-raster integration test) once the pipeline is reliably finishing.

---

## MVP Recommendation (the smallest set that addresses ALLOC-01..05 + PIPE-01..07)

Prioritize:

1. **Repair RSS profiling** (ALLOC-03) — without this, every other memory diagnosis is a guess.
2. **Pre-flight validation + structured error capture in workers** (ALLOC-04) — converts `MultisessionFuture interrupted` into actionable info.
3. **Propagate `log_msg` into inner allocation functions** (ALLOC-05) — TODO already in code.
4. **Switch to `multicore` plan on Linux + audit `butcher`/`tidypredict` coverage** (ALLOC-02) — addresses the model duplication root cause without committing to mlr3.
5. **Per-region skip-already-done + atomic writes** — converts a fragile 12h job into a resumable one.
6. **Path / package portability repairs** (PIPE-01..07) — mechanical, blocks HPC otherwise.

**Defer:**

- mlr3 migration (MLR3-01) until #4 has been measured — `butcher`/`tidypredict` may close the gap.
- `targets` adoption — explicit out-of-scope by project shape.
- Full automated test suite — the *minimum* (pure-function unit tests + one tiny-raster integration test) is worth it; full coverage is not.

---

## Sources

- `c:/Users/bblack/switchdrive/git/nascent-lulcc/.planning/PROJECT.md` — requirements and crash profile (HIGH confidence, primary source)
- `c:/Users/bblack/switchdrive/git/nascent-lulcc/.planning/codebase/CONCERNS.md` — known issues catalogue (HIGH)
- `c:/Users/bblack/switchdrive/git/nascent-lulcc/TODO.md` — explicit allocation TODOs (HIGH)
- `c:/Users/bblack/switchdrive/git/nascent-lulcc/src/allocation.r` lines 42–160 (profiling helpers), 700–800 (region loop), 1196–1230 (nhood cache), 1240–1370 (prediction loop) — current implementation state (HIGH)
- `c:/Users/bblack/switchdrive/git/nascent-lulcc/src/utils.r` lines 18–92 (write_raster), 1010–1038 (log_msg, initialize_worker_log) — existing helpers to build on (HIGH)
- General R ecosystem knowledge for `future`/`furrr`/`terra`/`testthat`/`butcher`/`tidypredict`/`progressr`/`qs`/`digest` patterns (MEDIUM — external verification was unavailable in this session; before committing to any specific package version or claim about XGBoost fork-safety, `multicore` interactions, or `tidypredict` XGBoost coverage, verify against current Context7 / official package docs)

**Verification flags for downstream phases:**

- Verify `tidypredict` XGBoost coverage at current versions before committing to it as the size-reduction strategy.
- Verify `future::multicore` + XGBoost fork safety with a smoke test on Euler before switching the parallel plan.
- Verify `terra` SpatRaster serialization through `qs`/`saveRDS` round-trips (likely fails — known C++ pointer issue) before designing the on-disk nhood cache around it; the safer design caches `.tif` files.
