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
export DINAMICA_EGO_8_HOME=/cluster/project/<project>/containers/dinamica-ego-8.sif
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
2. `/cluster/scratch` directory exists on the host
3. `--hpc` CLI flag or `FORCE_HPC=true` environment variable

When HPC is detected AND `HPC_SCRATCH_ROOT` is unset, the script **refuses**
to fall back to `$PROJECT_ROOT/.envs` and exits non-zero with a named-signal
message:

```text
ERROR: HPC context detected (<signal>) but HPC_SCRATCH_ROOT is unset.
       Refusing to install conda envs under $HOME (home filesystem quota).
       Source the project .env or run: export HPC_SCRATCH_ROOT=/cluster/scratch/$USER/nascent-lulcc
```

Source `.env` (which sets `HPC_SCRATCH_ROOT`) before running:

```bash
bash scripts/setup_environments.sh --env allocation_env --non-interactive
```

If you need to force HPC behaviour from a workstation (e.g. for a smoke run
that does not have `/cluster/scratch` or SLURM env vars), pass the `--hpc`
flag explicitly — the script will still refuse to proceed without
`HPC_SCRATCH_ROOT`. On a true workstation with no HPC signals and no `--hpc`
flag, the script keeps the `$PROJECT_ROOT/.envs` path and emits a one-line
confirmation:

```text
Env install root (local fallback, no HPC signals): /path/to/repo/.envs
```

The detection signal that fired is named in the message so operators can
disambiguate stale env-var pollution from real HPC context.

## Files Overview

### Environment Files (`../envs/`)
- `feat_select_env.yaml` - Environment for feature selection (includes RRF, arrow, etc.)
- `transition_model_env.yml` - Environment for transition modelling (includes tidymodels, ranger, xgboost, etc.)
- `dist_calc_env.yml` - Environment for distance calculations (if needed)
- `clim_data_env.yml` - Environment for climate data processing (if needed)

### Pipeline Scripts
- `setup_environments.sh` - Creates all conda environments
- `submit_feature_selection.sh` - Slurm job for feature selection
- `submit_transition_modelling.sh` - Slurm job for transition modelling  
- `master_pipeline.sh` - Runs the complete pipeline sequentially
- `run_feature_selection.r` - R script for feature selection pipeline
- `run_transition_modelling.r` - R script for transition modelling pipeline

## Usage

### 1. First Time Setup

Create the conda environments:
```bash
cd inst/
./setup_environments.sh
```

This will create environments at `/cluster/scratch/bblack/micromamba/envs/` (adjust paths as needed).

### 2. Running the Complete Pipeline

To run both feature selection and transition modelling sequentially:
```bash
cd inst/
./master_pipeline.sh
```

This will:
- Submit feature selection job
- Wait for it to complete
- Submit transition modelling job (depends on feature selection)
- Wait for it to complete
- Generate a summary report

### 3. Running Individual Steps

#### Feature Selection Only
```bash
cd inst/
sbatch submit_feature_selection.sh
```

#### Transition modelling Only
```bash
cd inst/
sbatch submit_transition_modelling.sh
```

### 4. Monitoring Jobs

Check job status:
```bash
squeue -u $USER
```

Check job details:
```bash
scontrol show job JOBID
```

View logs:
```bash
tail -f logs/feat-select-JOBID.out
tail -f logs/trans-model-JOBID.out
```

## Resource Allocation

### Feature Selection
- **CPUs**: 4 cores
- **Memory**: 32GB per CPU (128GB total)
- **Time**: 72 hours
- **Environment**: `feat_select_env`

### Transition modelling  
- **CPUs**: 8 cores
- **Memory**: 16GB per CPU (128GB total)
- **Time**: 72 hours
- **Environment**: `transition_model_env`

## Customization

### Adjusting Resource Requirements

Edit the `#SBATCH` directives in the submission scripts:
- `--cpus-per-task`: Number of CPU cores
- `--mem-per-cpu`: Memory per CPU core
- `--time`: Maximum runtime (HH:MM:SS)

### Adjusting Environment Paths

Update these variables in the scripts:
- `MAMBA_EXE`: Path to micromamba executable
- `ENV_PATH`: Base path for conda environments

### Adding Dependencies

Add packages to the appropriate `.yml` file in `envs/` directory, then recreate the environment:
```bash
micromamba env remove -p /path/to/env
micromamba env create -f envs/environment_file.yml -p /path/to/env
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