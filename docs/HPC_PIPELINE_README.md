# LULCC Modelling Pipeline — HPC Environment

This document describes how to run the LULCC (Land Use Land Cover Change) modelling
pipeline on HPC. It covers the two execution models the repository supports, the
pipeline stages and their environments, and the memory characteristics of the
allocation stage (the most memory-intensive part of the pipeline).

For the Dinamica-EGO-on-HPC container contract (Apptainer/Singularity launch,
`DINAMICA_EGO_8_HOME`, smoke tests) see [README_HPC.md](README_HPC.md). For
building the conda/micromamba environments see [MICROMAMBA_SETUP.md](MICROMAMBA_SETUP.md).

## Execution models

The standard workflow on the current ZALF HPC:

1. In the web-based **Rundeck** system, run the `allocate_node` job and choose the
   node type sized for the stage (see the node table below).
2. SSH into the reserved node.
3. **Submit the stage with `sbatch`** — e.g. `sbatch scripts/submit_allocation_smoke.sh`.

**Always use `sbatch`, not `bash`.** SLURM is available on the reserved node, and
submitting with `sbatch` is what writes the per-job log files (`#SBATCH --output` /
`--error`) and applies the resource directives. The submit scripts request the
**whole reserved node** via `#SBATCH --exclusive` + `#SBATCH --mem=0` (all cores,
all RAM) and carry **no `--partition`** — the Rundeck reservation already places the
job on the node you picked. So the node type you reserve in Rundeck *is* the memory
limit. (Running a script directly with `bash` still works as a fallback — it derives
paths from its own location and tees output to `logs/` — but the standard log files
are only written under `sbatch`.)

**Available Rundeck node types:**

| Node type | RAM | Cores | Notes |
|-----------|-----|-------|-------|
| `compute-exclusive` | ~93 GB | 80 vCores | General compute |
| `highmem-exclusive` | ~188 GB | 80 vCores | Memory-heavy stages |
| `fat-exclusive` | ~1.5 TB | 160 vCores | Largest allocation regions |
| `gpu-Nvidia-Tesla-V100` | ~93 GB | 48 vCores | 2× V100 |
| `gpu-Nvidia-Tensor-Core-H100` | ~756 GB | 128 vCores | 4× H100 |
| `2vCPU-2GB-Ram` / `4vCPU-16GB-Ram` / `16vCPU-32GB-Ram` / `40vCPU-40GB-Ram` | small | shared | non-exclusive |

> `master_pipeline.sh` chains the full pipeline with `sbatch --dependency=afterok`,
> which needs a multi-node SLURM controller. Under the single reserved-node model you
> run **one stage at a time** on the node instead.

## Pipeline stages

The full ordered dependency graph is defined authoritatively in
[`scripts/master_pipeline.sh`](../scripts/master_pipeline.sh). Each stage has a
`scripts/submit_<stage>.sh` launcher (SLURM wrapper) that activates the right
environment and runs the corresponding `scripts/run_<stage>.r` entrypoint.

The stages, in dependency order:

1. **Reference grid prep** → **LULC data prep** → **region prep** → **ancillary
   data prep** — prepare the spatial base layers and study regions.
2. **Calibration predictor prep** → **predictor parquets** — build the suitability/
   accessibility predictors and write them as partitioned Parquet datasets.
3. **Transition identification** → **transition dataset prep** — identify observed
   LULC transitions and assemble per-transition modelling datasets.
4. **Feature selection** — collinearity filtering + Guided Regularized Random
   Forest (GRRF) per transition.
5. **Transition modelling** — train per-transition classifiers (mlr3 / ranger,
   glmnet, xgboost) with cross-validation.
6. **Allocation parameterisation** — calibrate allocation parameters
   (`calibrate_allocation_parameters()`).
7. **Scenario preparation** → **simulation setup** — build simulation transition
   rate tables (`simulation_trans_rates_prep()`) and per-scenario inputs.
8. **Dinamica simulations / allocation** — run the spatially-explicit allocation
   through Dinamica EGO 8 (`run_allocation.r`; see [README_HPC.md](README_HPC.md)).

> Note: the old `submit_data_preparation.sh`, `submit_model_finalization.sh`, and
> `partial_pipeline.sh` referenced in earlier versions of this document **no longer
> exist**. Use the per-stage `submit_*.sh` scripts and `master_pipeline.sh`.

## Conda / micromamba environments

Environment specs live in `environments/` (not `envs/`). On HPC they install under
`$HPC_SCRATCH_ROOT/micromamba/envs` (see `scripts/hpc_common.sh`), built by
`scripts/setup_environments.sh`. See [MICROMAMBA_SETUP.md](MICROMAMBA_SETUP.md) for
the full setup flow.

| Env file | Used by |
|----------|---------|
| `data_prep_env.yml` | data prep stages |
| `dist_calc_env.yml` | distance calculations |
| `clim_data_env.yml` | climate data processing |
| `feat_select_env.yaml` | feature selection |
| `transition_model_env.yml` | transition modelling |
| `trans_rate_estimation_env.yml` | simulation transition-rate estimation |
| `allocation_params_env.yml` | allocation parameter calibration |
| `allocation_env.yml` | allocation / Dinamica simulations |

Build one (HPC context requires `HPC_SCRATCH_ROOT` to be set first):

```bash
source .env   # sets HPC_SCRATCH_ROOT, HPC_TMP_ROOT, TERRA_TEMP, etc.
bash scripts/setup_environments.sh --env allocation_env --non-interactive
```

## Required environment variables (Stage 7 path contract)

`scripts/hpc_common.sh` enforces a path contract — jobs refuse to run with any of
these unset rather than constructing hidden defaults:

| Variable | Purpose |
|----------|---------|
| `HPC_SCRATCH_ROOT` | data + env install root on scratch (e.g. `/beegfs/$USER/nascent-lulcc`) |
| `HPC_TMP_ROOT` | per-job tmp root (backs `$TMPDIR`) |
| `TERRA_TEMP` | `terra` tempdir; defaults to `$HPC_SCRATCH_ROOT/terra_temp` if unset |

Plus, for the allocation/Dinamica stage: `DINAMICA_EGO_8_HOME` (and optional
`DINAMICA_BACKEND`) — see [README_HPC.md](README_HPC.md). Source `.env` (from
`.env.template`) to populate all of these.

Validate the contract any time:

```bash
bash scripts/hpc_common.sh --check-stage7-contract
```

## Allocation stage memory profile

The allocation stage (`scripts/run_allocation.r` via `src/allocation.r`) is the
most memory-intensive part of the pipeline, and it is **single-threaded per region**
(default `ALLOCATION_PARALLEL_STRATEGY=sequential`, with native BLAS/data.table
threads pinned to 1). Its peak memory is dominated by two things:

- **Predictor preload floor.** Each region loads its full predictor table into
  memory once. For the largest region (cuenca_del_amazonas: ~68M cells × ~38 cols)
  this is **~80 GB resident** and stays resident for the whole region run.
- **Large "from"-class transitions.** Forest-dominated regions
  (`cuenca_del_amazonas`, `selva_andina`) have transitions *from* forest that are
  viable across tens of millions of cells. Predicting one of those (e.g. ~62M rows)
  pushes peak RSS **above 128 GB** and OOM-kills the job on smaller nodes.

### Node mapping for allocation

| Region(s) | Recommended node | Why |
|-----------|------------------|-----|
| `cuenca_del_amazonas`, `selva_andina` | `fat-exclusive` (1.5 TB), or `highmem-exclusive` (188 GB) with batching on | ~80 GB floor + a >128 GB peak on the big forest transition |
| `andes`, `costa_peruana` | `highmem-exclusive` (188 GB); `compute-exclusive` (93 GB) usually fits | far fewer forest cells, lower peak |

`compute-exclusive` (93 GB) is **not** viable for the two big regions — the ~80 GB
preload floor alone leaves no headroom. Because each region run is single-threaded,
on the fat node you can run **all four regions concurrently** in separate SSH shells
(scope each with `ALLOCATION_REGION_FILTER`) to use the cores.

### Allocation tuning knobs (environment variables)

Read by `src/allocation.r` / `scripts/submit_allocation_smoke.sh`:

| Variable | Effect |
|----------|--------|
| `ALLOCATION_REGION_FILTER` | restrict the run to one region (e.g. `selva_andina`) |
| `ALLOCATION_PROFILE_SCENARIO` | restrict to one scenario (e.g. `BAU`) |
| `ALLOCATION_PARALLEL_STRATEGY` | `sequential` (default for smoke), `multicore`, or `multisession` |
| `ALLOCATION_NUM_WORKERS` | worker count for non-sequential strategies |
| `ALLOCATION_PREDICT_BATCH_ROWS` | **memory fix**: predict large transitions in row-batches to bound prediction-time peak RSS. Unset = original single-shot. Try `5000000` for the big regions. Note: this caps prediction *transients*, not the ~80 GB preload floor. |
| `ALLOCATION_WORKER_RSS_BUDGET_MB` | **no-op** — only logged as a breadcrumb; it does not bound or chunk anything. |

Example (big region on a reserved fat/highmem node — pass env vars through to the
job with `sbatch --export`):

```bash
sbatch --export=ALL,ALLOCATION_REGION_FILTER=cuenca_del_amazonas,\
ALLOCATION_PROFILE_SCENARIO=BAU,ALLOCATION_PREDICT_BATCH_ROWS=5000000 \
  scripts/submit_allocation_smoke.sh
```

## Monitoring and logs

Jobs submitted with `sbatch` write `logs/<stage>-<jobid>.{out,err}` (via the
`#SBATCH --output`/`--error` directives), and the allocation worker also writes a
per-region log. Standard SLURM commands:

```bash
squeue -u $USER                 # queued/running jobs
sacct -j JOBID --format=JobID,State,ExitCode,MaxRSS   # exit code + peak memory
scancel JOBID                   # cancel
```

`sacct … MaxRSS` is the authoritative per-job peak — it captures child processes
like Dinamica that R-side RSS logging does not. You can also watch live memory with
`top`/`htop` (resident size `RES`) on the reserved node.

## Troubleshooting

- **OOM on `cuenca_del_amazonas` / `selva_andina`** — move to `fat-exclusive` (or
  `highmem-exclusive` with `ALLOCATION_PREDICT_BATCH_ROWS=5000000`). See the
  allocation memory section above.
- **`HPC context detected … but HPC_SCRATCH_ROOT is unset`** — source `.env` before
  running env setup or any stage; the path contract is mandatory.
- **Environment not found** — confirm the env exists at
  `$HPC_SCRATCH_ROOT/micromamba/envs/<name>` and rebuild with
  `scripts/setup_environments.sh` if missing.
- **Dinamica `std::exception` / silent failure** — see the Dinamica launch contract
  and smoke tests in [README_HPC.md](README_HPC.md).
- **Stage script can't find `run_*.r`** — submit with `sbatch scripts/submit_<stage>.sh`
  from the repo root. (When run directly with `bash`, scripts derive the project root
  from their own location, so that works too.)
