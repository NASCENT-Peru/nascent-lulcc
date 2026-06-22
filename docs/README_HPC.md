# HPC Pipeline Setup for LULCC modelling

This directory contains scripts and environments for running the Land Use Land Cover Change (LULCC) modelling pipeline on HPC systems using Slurm.

## Recent Changes (Phase 1.1)

The Dinamica-on-Euler launch contract was rewritten in Phase 1.1 to the
apptainer-`exec`-`--home`-`--env`-`bash -c`-`bin/DinamicaEGO.sh` shape
(**D-104**). The previous `apptainer exec <sif> DinamicaConsole <model>` form
is **DEPRECATED** because it produced silent `std::exception` failures on the
upstream image. The `setup_environments.sh` silent fallback to
`$PROJECT_ROOT/.envs` on HPC was also replaced with a three-signal HPC
detector that refuses to install conda envs under `$HOME` when
`HPC_SCRATCH_ROOT` is unset (**D-112**). The workstation `docker save` →
`docker-archive://` workaround is no longer canonical; see
`dinamica/container/README.md` for the build-flow contract.

> **Phase 1.1 gap-closure — resolved 2026-05-19.** The `--live` smoke test now exits **0**
> against `dinamica/dinamica_model/smoketest.ego`. Open Issue 1 (`DinamicaConsole`
> `std::exception`) was resolved by Plan 01.1-07: root cause was a circular singleton init
> bug in `libBase.so` (H8 — all prior H1–H7 hypotheses FALSIFIED over 6 diagnostic
> iterations in Plans 05–06), fixed by an LD_PRELOAD interceptor compiled in `%post` Stage 6.
> See `.planning/phases/01.1-fix-dinamica-launch-contract/01.1-07-SUMMARY.md` and
> `diagnostics/FINDINGS.md` H8 for the full record.

## Stage 7 Dinamica-on-Euler contract (INFRA-01)

Stage 7 (allocation) calls Dinamica EGO 8 through a single R entrypoint
(`exec_dinamica()` in `src/dinamica_utils.r`). On Euler the Dinamica binary is
**not** installed natively; instead, an external Apptainer/Singularity image
is consumed verbatim by `apptainer exec` / `singularity exec`. The repository
ships only the container definition (`dinamica/container/rocker-geospatial-dinamica.def`)
and the build instructions (`dinamica/container/README.md`); the built `.sif`
artifact stays outside the repo (locked decision D-10).

### `DINAMICA_EGO_8_HOME` contract

| Backend | What `DINAMICA_EGO_8_HOME` must be                                   |
| ------- | -------------------------------------------------------------------- |
| HPC     | **Absolute path to the external Dinamica `.sif` image** on Euler.    |
| Local   | Absolute path to the local Dinamica EGO 8 install directory.         |

On Euler, set it to wherever the built `.sif` lives, e.g.:

```bash
export DINAMICA_EGO_8_HOME=/project/<project>/containers/dinamica-ego-8.sif
```

`exec_dinamica()` consumes that exact path as the image argument and builds
the **D-104** launch shape:

```bash
apptainer exec \
    --home   "$HPC_SCRATCH_ROOT/dinamica-home" \
    --bind   "$HPC_SCRATCH_ROOT/dinamica-tmp:$HPC_SCRATCH_ROOT/dinamica-tmp" \
    --env    DINAMICA_EGO_8_TEMP_DIR="$HPC_SCRATCH_ROOT/dinamica-tmp" \
    "$DINAMICA_EGO_8_HOME" \
    bash -c 'cd /opt/dinamica/usr && bin/DinamicaEGO.sh /abs/path/to/model.ego'
```

`singularity exec` is the only fallback runtime spelling. The runtime is
probed in the order `apptainer` first, `singularity` second.

The `--home`/`--env` flags stage Dinamica's mutable state under
`$HPC_SCRATCH_ROOT/dinamica-{home,tmp}` (**D-105**) so the binary does not
touch Euler `$HOME` (home quota) and so its temp files are isolated per job.
The model path passed to `bin/DinamicaEGO.sh` must be absolute (**D-106**).
`resolve_dinamica_launch()` seeds the minimal `.dinamica_ego_8.conf` under
the staged home idempotently.

### Required env vars (Stage 7)

In addition to the path contract from Plan 01-01 (`HPC_SCRATCH_ROOT`,
`HPC_TMP_ROOT`, `TERRA_TEMP`, validated by
`scripts/hpc_common.sh --check-stage7-contract`), Stage 7 requires:

| Variable               | Required on HPC | Required locally | Notes                                   |
| ---------------------- | --------------- | ---------------- | --------------------------------------- |
| `DINAMICA_EGO_8_HOME`  | yes             | yes              | `.sif` path on HPC, install dir locally |
| `DINAMICA_BACKEND`     | optional        | optional         | `auto` (default), `local`, or `hpc`     |

`.env.template` exposes both variables with the right defaults.

### Container definition + build instructions

The committed container definition lives at:

- `dinamica/container/rocker-geospatial-dinamica.def` — Apptainer/Singularity
  build definition rooted in the upstream
  [`ethzplus/rocker-geospatial-dinamica`](https://github.com/ethzplus/rocker-geospatial-dinamica)
  image.
- `dinamica/container/README.md` — operator-facing build flow and where to
  publish the built `.sif` as an external artifact (D-10).

To (re)build the image on a node that supports container builds:

> **Quota warning:** The built `.sif` is ~1 GB. Build **directly to
> `$DINAMICA_EGO_8_HOME`** (project or scratch filesystem), not to a relative
> path inside `$REPO_ROOT`. A relative path resolves under `$HOME` and Euler
> home quotas will be exhausted at the final `Creating SIF file…` step,
> producing `disk quota exceeded` after an otherwise successful build.

```bash
# Route build temp/cache to scratch (avoid intermediate-layer home quota):
export APPTAINER_TMPDIR="$HPC_SCRATCH_ROOT/apptainer-tmp"
export APPTAINER_CACHEDIR="$HPC_SCRATCH_ROOT/apptainer-cache"
mkdir -p "$APPTAINER_TMPDIR" "$APPTAINER_CACHEDIR"

# Build directly to the external artifact path:
apptainer build "$DINAMICA_EGO_8_HOME" \
    dinamica/container/rocker-geospatial-dinamica.def

# Fallback spelling:
singularity build "$DINAMICA_EGO_8_HOME" \
    dinamica/container/rocker-geospatial-dinamica.def
```

`$DINAMICA_EGO_8_HOME` is already the stable external-artifact path (D-10) —
no separate publish step needed when building directly to it.

### Smoke-test commands (D-11)

`scripts/smoke_test_dinamica.sh` is the operator-facing wrapper that proves
the Dinamica wiring **before** any real Stage 7 batch job. The same script
covers two modes: a workstation dry-run (no runtime probe, no real `.sif`
needed) and the live Euler smoke test that requires Dinamica to actually
complete and write a timestamped `dinamica-smoke-*.log` under `logs/`.

#### Dry-run command (workstation, no apptainer/singularity required)

```bash
bash scripts/smoke_test_dinamica.sh \
    --dry-run \
    --runtime apptainer \
    --artifact /tmp/dinamica.sif \
    --ego dinamica/dinamica_model/smoketest.ego
```

This prints the resolved launch plan (runtime, `DINAMICA_EGO_8_HOME` artifact
path, ego model, log file destination, staged home/tmp, absolute model path,
and the full resolved `apptainer exec --home … --env … bash -c 'cd … && bin/DinamicaEGO.sh …'`
command) and exits 0 without spawning Dinamica. The dry-run uses the
operator-supplied runtime name verbatim so the contract can be validated on
a host that has neither `apptainer` nor `singularity` installed.

The smoke fixture is `dinamica/dinamica_model/smoketest.ego` — a no-op `.ego`
(D-109 / DD-2). The production `allocation.ego-decoded` is unchanged and
continues to be loaded by `run_allocation_dinamica()` for real Stage 7 runs.

#### Live Euler smoke-test command

```bash
# Source the .env file first so DINAMICA_EGO_8_HOME and the Stage 7 path
# contract are populated (HPC_SCRATCH_ROOT must be set; --live exits 1
# otherwise per D-105).
source .env

bash scripts/smoke_test_dinamica.sh \
    --live \
    --runtime auto \
    --artifact "$DINAMICA_EGO_8_HOME" \
    --ego dinamica/dinamica_model/smoketest.ego \
    --require-log-under logs
```

The live mode:

1. Fails fast (exit 1) if `HPC_SCRATCH_ROOT` is unset (D-105).
2. Probes `apptainer` first, then `singularity`, on `PATH`.
3. Checks that `$DINAMICA_EGO_8_HOME` actually points to a readable `.sif`.
4. Creates `$HPC_SCRATCH_ROOT/dinamica-{home,tmp}` if missing and seeds
   `.dinamica_ego_8.conf` idempotently.
5. Runs the D-104 launch command
   (`apptainer exec --home … --bind <tmp>:<tmp> --env DINAMICA_EGO_8_TEMP_DIR=… <sif> bash -c 'cd /opt/dinamica/usr && bin/DinamicaEGO.sh <abs-model>'`)
   and tees combined stdout/stderr into `logs/dinamica-smoke-<timestamp>.log`.
6. After the subprocess returns, greps the log for the **D-107** error
   patterns (`Dinamica EGO exited with an error`,
   `terminate called after throwing`, `std::exception`) and exits **5** on
   any match — regardless of the subprocess exit code (Dinamica returns 0
   even on `std::exception`).
7. Exits **non-zero** unless Dinamica completes successfully **and** the
   timestamped logfile lands under `logs/` **and** no D-107 error pattern
   matched.

Exit-code contract (documented at the top of the script):

| Exit | Meaning                                                                                  |
| ---- | ---------------------------------------------------------------------------------------- |
| 0    | success                                                                                  |
| 1    | usage / argument validation error (incl. `--live` without `HPC_SCRATCH_ROOT`)            |
| 2    | dry-run resolution failed (artifact missing, runtime not on PATH, etc.)                  |
| 3    | live Dinamica subprocess returned a non-zero exit code                                   |
| 4    | live Dinamica succeeded but no `dinamica-smoke-*.log` was written                        |
| 5    | live Dinamica returned 0 BUT printed a D-107 error string in the log                     |

> Per the Phase 1.1 gap-closure (Plan 01.1-07, 2026-05-19), the `--live` command
> exits **0**. Open Issue 1 is RESOLVED — see `01.1-07-SUMMARY.md`.

### Phase 1.1 — HPC env install root contract (D-112)

`scripts/setup_environments.sh` detects HPC context via three OR'd signals:

1. `SLURM_JOB_ID` or `SLURM_CLUSTER_NAME` environment variable is set
2. `/beegfs` directory exists on the host
3. `--hpc` CLI flag or `FORCE_HPC=true` environment variable

When HPC is detected AND `HPC_SCRATCH_ROOT` is unset, the script **refuses**
to fall back to `$PROJECT_ROOT/.envs` and exits non-zero with a named-signal
message:

```text
ERROR: HPC context detected (<signal>) but HPC_SCRATCH_ROOT is unset.
       Refusing to install conda envs under $HOME (home filesystem quota).
       Source the project .env or run: export HPC_SCRATCH_ROOT=/beegfs/$USER/nascent-lulcc
```

Source `.env` (which sets `HPC_SCRATCH_ROOT`) before running:

```bash
bash scripts/setup_environments.sh --env allocation_env --non-interactive
```

If you need to force HPC behaviour from a workstation (e.g. for a smoke run
that does not have `/beegfs` or SLURM env vars), pass the `--hpc`
flag explicitly — the script will still refuse to proceed without
`HPC_SCRATCH_ROOT`. On a true workstation with no HPC signals and no `--hpc`
flag, the script keeps the `$PROJECT_ROOT/.envs` path and emits a one-line
confirmation:

```text
Env install root (local fallback, no HPC signals): /path/to/repo/.envs
```

The detection signal that fired is named in the message so operators can
disambiguate stale env-var pollution from real HPC context.

## Running the allocation stage (Stage 7)

There are two ways to run the allocation: the **smoke test** (one region, one
scenario, one timestep — proves the wiring) and the **real step** (all regions ×
all scenarios × all timesteps). Both activate `allocation_env` and call
`scripts/run_allocation.r`.

On the current ZALF HPC, reserve an exclusive node via the Rundeck `allocate_node`
job, SSH in, and **submit with `sbatch`** — SLURM is available on the node, and
`sbatch` is what writes the log files and applies the resource directives. Both
scripts request the **whole reserved node** (`#SBATCH --exclusive --mem=0`) and carry
**no `--partition`**, so the Rundeck node size you pick is the memory limit. Pass the
per-run knobs with `sbatch --export=ALL,VAR=value,...`. (Running directly with `bash`
still works as a fallback and tees output to `logs/`, but `sbatch` is preferred so the
standard logs are written.)

Note: because each job is `--exclusive`, only one runs on the reserved node at a time
— per-region smoke jobs queue and run sequentially. For all regions in parallel, use
the real step (multicore within a single job) on a fat node.

### Resources / node selection

Allocation is **single-threaded per region** and its peak memory is dominated by a
per-region predictor-preload floor (~80 GB for the largest region) plus a >128 GB
spike on the large forest→* transitions in `cuenca_del_amazonas` and `selva_andina`.
See the memory profile and full node table in
[HPC_PIPELINE_README.md](HPC_PIPELINE_README.md#allocation-stage-memory-profile).

| Workload | Node | Notes |
|----------|------|-------|
| Smoke, `cuenca_del_amazonas` / `selva_andina` | `fat-exclusive` (1.5 TB), or `highmem-exclusive` (188 GB) + `ALLOCATION_PREDICT_BATCH_ROWS` | >128 GB peak on the big forest transition |
| Smoke, `andes` / `costa_peruana` | `highmem-exclusive` (188 GB), `compute-exclusive` (93 GB) usually fits | lower peak |
| Real step (all regions in parallel) | `fat-exclusive` (1.5 TB) | concurrent region workers × per-region peak |
| Real step (one region at a time) | `highmem-exclusive` (188 GB) | force sequential; ~130 GB peak |

`compute-exclusive` (93 GB) cannot hold the two big regions — the ~80 GB floor
alone leaves no headroom.

> Prerequisite: confirm the Dinamica wiring with the dry-run / `--live`
> `scripts/smoke_test_dinamica.sh` (above) before the first real Stage 7 job.

### Allocation smoke test

`scripts/submit_allocation_smoke.sh` runs the allocation for a **single region**,
the **BAU** scenario, and the **first** simulation timestep (profile mode,
`ALLOCATION_PARALLEL_STRATEGY=sequential`). Use it to verify a region completes
without the `ALLOC-08` hard stop and writes a `posterior.tif`.

```bash
# From the repo root on the reserved node, one region per job:
sbatch --export=ALL,ALLOCATION_REGION_FILTER=selva_andina,ALLOCATION_PROFILE_SCENARIO=BAU \
  scripts/submit_allocation_smoke.sh
```

For the two big regions, bound prediction-time memory:

```bash
sbatch --export=ALL,ALLOCATION_REGION_FILTER=cuenca_del_amazonas,\
ALLOCATION_PROFILE_SCENARIO=BAU,ALLOCATION_PREDICT_BATCH_ROWS=5000000 \
  scripts/submit_allocation_smoke.sh
```

Relevant environment variables (see the script header for the full list):

| Variable | Effect |
|----------|--------|
| `ALLOCATION_REGION_FILTER` | restrict to one region (`andes`, `cuenca_del_amazonas`, `costa_peruana`, `selva_andina`) |
| `ALLOCATION_PROFILE_SCENARIO` | scenario to run (default `BAU`) |
| `ALLOCATION_YEAR_POST_FILTER` | posterior year; auto-computed from config if unset |
| `ALLOCATION_PREDICT_BATCH_ROWS` | batch large-transition prediction to cap peak RSS (unset = single-shot) |
| `ALLOCATION_WORKER_RSS_BUDGET_MB` | no-op (logged only) |

**Pass criteria:** exit code 0, no `ALLOC-08` line in the log, and a `posterior.tif`
under `outputs/simulations/<scenario>/<year>/region_<region>/`. `sbatch` writes
`logs/lulc-allocation-smoke-<jobid>.{out,err}`. To cover all four regions, submit one
job per region (they run one at a time under `--exclusive`), or run the real step.

### Real allocation step

`scripts/submit_allocation.sh` runs the **full** allocation: every scenario in
`config[["scenario_names"]]`, every region, every timestep. Scenarios run
sequentially; within a scenario, regions are processed in parallel via
`furrr::future_map` (multicore), with the worker count taken from
`ALLOCATION_NUM_WORKERS` (defaults to `SLURM_CPUS_PER_TASK`, else 4).

**Memory implication:** peak ≈ (number of concurrent region workers) × (per-region
peak). With the big regions exceeding 128 GB each, running several in parallel only
fits on `fat-exclusive`. To run on `highmem-exclusive`, serialise the regions with
`ALLOCATION_PARALLEL_STRATEGY=sequential`.

```bash
# All regions in parallel (default multicore) — reserve fat-exclusive (1.5 TB):
sbatch --export=ALL,ALLOCATION_PREDICT_BATCH_ROWS=5000000 scripts/submit_allocation.sh

# One region at a time — reserve highmem-exclusive (188 GB):
sbatch --export=ALL,ALLOCATION_PARALLEL_STRATEGY=sequential,ALLOCATION_PREDICT_BATCH_ROWS=5000000 \
  scripts/submit_allocation.sh
```

(The worker count comes from `ALLOCATION_NUM_WORKERS`, which the script sets from
`SLURM_CPUS_PER_TASK`; with only four regions the default already runs them all in
parallel. Use `ALLOCATION_PARALLEL_STRATEGY=sequential` to force one at a time.)

`sbatch` writes `logs/lulc-allocation-<jobid>.{out,err}` and requests 48 h; the real
wall time depends on region count, scenario count, and concurrency.

**Monitoring:** watch resident memory with `top`/`htop` on the reserved node. Under
SLURM, `sacct -j JOBID --format=JobID,State,ExitCode,MaxRSS` gives the authoritative
peak (it captures the Dinamica child process that R-side RSS logging does not).

## Files Overview

### Environment files (`environments/`)

Conda/micromamba specs; on HPC they install under `$HPC_SCRATCH_ROOT/micromamba/envs`.

- `data_prep_env.yml` — data preparation stages
- `dist_calc_env.yml` — distance calculations
- `clim_data_env.yml` — climate data processing
- `feat_select_env.yaml` — feature selection (RRF, arrow, etc.)
- `transition_model_env.yml` — transition modelling (tidymodels, ranger, xgboost, etc.)
- `trans_rate_estimation_env.yml` — simulation transition-rate estimation
- `allocation_params_env.yml` — allocation parameter calibration
- `allocation_env.yml` — allocation / Dinamica simulations (Stage 7)

### Pipeline scripts (`scripts/`)

Each stage has a `submit_<stage>.sh` SLURM wrapper that activates its environment and
runs the matching `run_<stage>.r`. Submit them with `sbatch` on a Rundeck-reserved
node (they can also run directly with `bash` as a fallback).

- `setup_environments.sh` — create/update conda environments
- `hpc_common.sh` — shared helpers + the Stage 7 path-contract check
- `master_pipeline.sh` — submit the **full** pipeline as a SLURM `sbatch` dependency chain
- `submit_allocation_smoke.sh` / `submit_allocation.sh` — allocation smoke / real step (see above)
- `submit_<stage>.sh`, `run_<stage>.r` — individual pipeline stages

For the full ordered stage list see
[HPC_PIPELINE_README.md](HPC_PIPELINE_README.md#pipeline-stages).

## Usage

> On the current ZALF HPC, reserve an exclusive node via Rundeck, SSH in, and submit
> stage scripts with `sbatch` (so SLURM writes the logs and applies the whole-node
> `--exclusive`/`--mem=0` request). `bash` works as a fallback. See
> [HPC_PIPELINE_README.md](HPC_PIPELINE_README.md#execution-models).

### 1. First-time setup — build environments

Source the project `.env` (sets `HPC_SCRATCH_ROOT` etc.), then build the env(s) you
need. They install under `$HPC_SCRATCH_ROOT/micromamba/envs` on HPC (or `<repo>/.envs`
locally when no HPC signal is present):

```bash
source .env
bash scripts/setup_environments.sh --env allocation_env --non-interactive
# omit --env to build all environments
```

### 2. Running the full pipeline (SLURM only)

`master_pipeline.sh` submits **every** stage as an `sbatch --dependency` chain and
monitors with `squeue`/`sacct`. It needs a multi-node SLURM controller, so it does
**not** apply to the single reserved-node model — there, submit stages one at a time.

```bash
bash scripts/master_pipeline.sh   # the orchestrator itself calls sbatch per stage
```

### 3. Running individual stages

```bash
# sbatch on the reserved node (writes logs/<stage>-<jobid>.{out,err}):
sbatch scripts/submit_feature_selection.sh
sbatch scripts/submit_transition_modelling.sh
```

For the allocation stage (smoke + real step) see
[Running the allocation stage](#running-the-allocation-stage-stage-7) above.

### 4. Monitoring

- **SLURM:** `squeue -u $USER`, `sacct -j JOBID --format=JobID,State,ExitCode,MaxRSS`,
  `scancel JOBID`. Logs land in `logs/<stage>-<jobid>.{out,err}`.
- **Live memory:** `top`/`htop` (resident size `RES`) on the reserved node.

## Resource allocation

The submit scripts request the **whole reserved node** (`#SBATCH --exclusive --mem=0`,
no `--partition`), so the Rundeck node type you reserve sets the real RAM/core limit.
The `--cpus-per-task` value still controls each stage's worker pool (`SLURM_CPUS_PER_TASK`).
Reserve a node sized for the stage:

| Stage | `--cpus-per-task` | Time | Suggested node | Environment |
|-------|-------------------|------|----------------|-------------|
| Feature selection | 4 | 72 h | `highmem-exclusive` (188 GB) | `feat_select_env` |
| Transition modelling | 3 | 72 h | `highmem-exclusive` (188 GB) | `transition_model_env` |
| Distance calc | 48 | 24 h | `highmem-exclusive` (188 GB) | `dist_calc_env` |

For the allocation stage's (larger) memory profile and node mapping see
[Running the allocation stage](#running-the-allocation-stage-stage-7) above and
[HPC_PIPELINE_README.md](HPC_PIPELINE_README.md#allocation-stage-memory-profile).

## Customization

### Adjusting Resource Requirements

Edit the `#SBATCH` directives in the submission scripts:
- `--cpus-per-task`: Number of CPU cores
- `--mem-per-cpu`: Memory per CPU core
- `--time`: Maximum runtime (HH:MM:SS)

### Adjusting environment paths

Environment paths derive from the contract in `scripts/hpc_common.sh`:
- `MAMBA_EXE_CUSTOM`: override the micromamba executable location (else auto-probed)
- `ENV_BASE_PATH`: env install root, derived as `$HPC_SCRATCH_ROOT/micromamba/envs`

### Adding dependencies

Add packages to the appropriate spec in `environments/`, then recreate the env:
```bash
micromamba env remove -p "$HPC_SCRATCH_ROOT/micromamba/envs/<name>"
micromamba env create -f environments/<name>.yml -p "$HPC_SCRATCH_ROOT/micromamba/envs/<name>"
# or simply re-run: bash scripts/setup_environments.sh --env <name> --non-interactive
```

## Troubleshooting

### Common Issues

1. **Environment not found**: Ensure `setup_environments.sh` completed successfully
2. **Package missing**: Check if all required packages are in the environment files
3. **Memory issues**: Increase `--mem-per-cpu` or reduce data batch sizes
4. **Time limit exceeded**: Increase `--time` or optimize processing

### Debug Information

The pipeline scripts include extensive logging and diagnostics:
- R version and library paths
- Package installation status
- Configuration loading
- Runtime statistics

### Log Files

All output is captured in timestamped log files:
- `logs/feat-select-JOBID.{out,err}`
- `logs/trans-model-JOBID.{out,err}`
- `logs/pipeline_summary_TIMESTAMP.txt`

## Notes

- The scripts assume a specific HPC setup with micromamba. Adjust paths as needed for your system.
- The transition modelling job depends on feature selection completing successfully.
- All intermediate results are saved to disk to allow for resuming if needed.
- The pipeline uses parallel processing within jobs (controlled by `--cpus-per-task`).