<!-- generated-by: gsd-doc-writer -->
# Deployment

This document covers how to deploy and run the nascent-lulcc pipeline on the ETH Euler HPC cluster
(SLURM). It includes the deployment target, pre-flight environment setup, the Dinamica EGO container
build, per-stage SLURM resource allocations, job monitoring, rollback procedures, and known
operational issues.

---

## Deployment Targets

The pipeline has one intended execution target: the **ETH Euler HPC cluster** (SLURM). A local
Windows workstation mode is also supported for interactive development but is not a production
deployment target.

| Target | Config file | Notes |
|---|---|---|
| ETH Euler HPC (SLURM) | `config/hpc_config.yaml` | Primary production target. Jobs submitted via `sbatch`. |
| Local workstation | `config/local_config.yaml` | Development only. Functions called interactively in R. |

The HPC deployment requires:
- Access to the ETH Euler cluster <!-- VERIFY: cluster access is managed via ETH IT -->
- A home directory under `/home/$USER/`
- A scratch directory under `/beegfs/$USER/` (wiped every 60–90 days)
- SLURM module system with `apptainer` (or `singularity`) available

There are no Docker files, Vercel configs, Netlify configs, or other cloud platform manifests in the
repository. The only deployment artifact is the Apptainer/Singularity container image for Dinamica
EGO 8, described in the [Dinamica Container Build](#dinamica-ego-8-container-build) section below.

---

## Build Pipeline

There is no automated CI/CD pipeline. No `.github/workflows/` files are present. All deployment
steps are run manually by the operator on the Euler login node. The deployment sequence is:

1. Upload the repository to the cluster login node.
2. Source `.env` to export all required environment variables.
3. Install micromamba and create conda environments.
4. Build the Dinamica EGO 8 Apptainer image (once per image version or `.def` change).
5. Validate the Stage 7 path contract.
6. Smoke-test the Dinamica launch.
7. Submit pipeline jobs via `bash scripts/master_pipeline.sh` or individual `sbatch` calls.

---

## Environment Setup

### 1. Upload the Repository

```bash
# Target path on ETH Euler
/home/$USER/nascent-lulcc
```

Copy or clone the repository to the above path. All SLURM scripts derive the project root
from `$NASCENT_LULCC_HOME` set in `.env`.

### 2. Configure Environment Variables

```bash
cp .env.template .env
# Edit .env — set DINAMICA_EGO_8_HOME and verify scratch paths
source .env
```

The `.env` file is the only operator-facing surface for machine-specific values. Required variables
for any HPC job are listed in the [Required vs Optional Summary](#required-vs-optional-summary)
below. The full variable reference is in `docs/CONFIGURATION.md`.

Key variables to set before proceeding:

| Variable | What to set |
|---|---|
| `HPC_SCRATCH_ROOT` | `/beegfs/$USER/nascent-lulcc` |
| `HPC_TMP_ROOT` | `$HPC_SCRATCH_ROOT/temp` |
| `TERRA_TEMP` | `$HPC_SCRATCH_ROOT/terra_temp` |
| `DINAMICA_EGO_8_HOME` | Absolute path to the built `.sif` image (see container build below) |
| `DINAMICA_BACKEND` | `hpc` (or leave as `auto`) |

### 3. Install Micromamba

```bash
bash scripts/install_micromamba_simple.sh
```

The script downloads the latest `micromamba-linux-64` binary from GitHub Releases and installs it
to `$HOME/.local/bin/micromamba`. It supports Linux x86_64 only (the Euler node architecture).

### 4. Create Conda Environments

```bash
# Create all pipeline environments
bash scripts/setup_environments.sh

# Or create a single environment
bash scripts/setup_environments.sh --env allocation_env --non-interactive
```

On HPC, environments are installed under `$HPC_SCRATCH_ROOT/micromamba/envs/` (scratch — recreate
if wiped). The script detects HPC context via SLURM environment variables or `/beegfs`
presence and refuses to install under `$HOME` if `HPC_SCRATCH_ROOT` is unset.

Environments provisioned:

| Environment | Pipeline stage |
|---|---|
| `data_prep_env` | Stage 1: Data preparation |
| `feat_select_env` | Stage 2: Feature selection |
| `transition_model_env` | Stage 3: Transition modelling; Stage 6: Spatial interventions |
| `allocation_params_env` | Stage 4: Allocation parameter calibration |
| `trans_rate_estimation_env` | Stage 5: Scenario transition rate preparation |
| `allocation_env` | Stage 7: Allocation / Dinamica simulations |
| `dist_calc_env` | Distance/accessibility predictor calculation (ancillary) |
| `clim_data_env` | Climate data download (Python, ancillary) |

### 5. Validate the Stage 7 Path Contract

Before submitting any Stage 7 job:

```bash
bash scripts/hpc_common.sh --check-stage7-contract
```

This validates that `HPC_SCRATCH_ROOT`, `HPC_TMP_ROOT`, and `TERRA_TEMP` are all set and
non-empty. All Stage 7 submit scripts source `hpc_common.sh` and call `setup_common_env()`, which
runs the same check and exits non-zero if any contract variable is missing.

---

## Dinamica EGO 8 Container Build

Stage 7 requires Dinamica EGO 8, which runs via an Apptainer/Singularity container on Euler. The
built `.sif` image is an **external artifact** — it is not committed to the repository and must be
built once by the operator.

The container definition is at `dinamica/container/rocker-geospatial-dinamica.def`. It is a port
of the upstream [`ethzplus/rocker-geospatial-dinamica`](https://github.com/ethzplus/rocker-geospatial-dinamica)
Dockerfile, pinned to `rocker/r-ver:4.5.3`. The built image weighs approximately 1 GB.

### Build Command (Apptainer — preferred on Euler)

> **Quota warning:** Always build directly to `$DINAMICA_EGO_8_HOME` on the project or scratch
> filesystem — never to a relative path inside the repo directory. A relative path resolves under
> `$HOME`, which will exhaust Euler's home quota during the `Creating SIF file…` step.

```bash
# Route build temp/cache to scratch to avoid intermediate-layer quota exhaustion
export APPTAINER_TMPDIR="$HPC_SCRATCH_ROOT/apptainer-tmp"
export APPTAINER_CACHEDIR="$HPC_SCRATCH_ROOT/apptainer-cache"
mkdir -p "$APPTAINER_TMPDIR" "$APPTAINER_CACHEDIR"

# Build directly to the external artifact path
apptainer build "$DINAMICA_EGO_8_HOME" \
    dinamica/container/rocker-geospatial-dinamica.def
```

### Build Command (Singularity — fallback)

```bash
singularity build "$DINAMICA_EGO_8_HOME" \
    dinamica/container/rocker-geospatial-dinamica.def
```

### Recommended Image Locations

The `.sif` must live outside the repository clone. Recommended paths on Euler:

```text
# Shared across cluster project users (preferred)
/project/<project>/containers/dinamica-ego-8.sif   <!-- VERIFY: project filesystem path -->

# Per-user staging (acceptable if no project filesystem)
/beegfs/$USER/nascent-lulcc/containers/dinamica-ego-8.sif
```

Set `DINAMICA_EGO_8_HOME` in `.env` to the absolute path of the built image.

### Rebuild Triggers

Rebuild and re-verify the `.sif` when any of these change:

- Dinamica EGO version bump (update `DINAMICA_EGO_DOWNLOAD_URL` in the `.def` file)
- Base image bump (`rocker/r-ver:4.5.3` → later version)
- Changes to the `%post` or `%test` block in `rocker-geospatial-dinamica.def`
- Euler runtime version bump (apptainer/singularity format change)

### Smoke-Test the Container

After building, verify the Dinamica launch contract before running any real Stage 7 job:

```bash
# Source .env first so DINAMICA_EGO_8_HOME and the Stage 7 path contract are set
source .env

bash scripts/smoke_test_dinamica.sh \
    --live \
    --runtime auto \
    --artifact "$DINAMICA_EGO_8_HOME" \
    --ego dinamica/dinamica_model/smoketest.ego \
    --require-log-under logs
```

The script exits 0 only if Dinamica completes successfully, writes a timestamped
`logs/dinamica-smoke-*.log`, and the log contains no error patterns
(`Dinamica EGO exited with an error`, `terminate called after throwing`, `std::exception`).

A dry-run mode (no Apptainer/Singularity required) is also available for workstation validation:

```bash
bash scripts/smoke_test_dinamica.sh \
    --dry-run \
    --runtime apptainer \
    --artifact /tmp/dinamica.sif \
    --ego dinamica/dinamica_model/smoketest.ego
```

---

## Running the Pipeline

### Complete Pipeline (recommended)

```bash
cd /home/$USER/nascent-lulcc/scripts
bash master_pipeline.sh
```

`master_pipeline.sh` submits all stages as a chained SLURM sequence using `--dependency=afterok`,
polls `squeue`/`sacct` for completion after each stage, and exits non-zero at the first failure.
A summary report is written to `logs/complete_pipeline_summary_<timestamp>.txt`.

### Individual Stage Submission

Each stage has a paired `run_*.r` driver and `submit_*.sh` SLURM wrapper. Submit individually:

```bash
# Stage 1: Data preparation (multiple sub-jobs; submit in dependency order)
sbatch scripts/submit_ref_grid_prep.sh
sbatch --dependency=afterok:<ref_job_id> scripts/submit_lulc_data_prep.sh
sbatch --dependency=afterok:<lulc_job_id> scripts/submit_region_prep.sh
sbatch --dependency=afterok:<region_job_id> scripts/submit_ancillary_data_prep.sh
sbatch --dependency=afterok:<ancillary_job_id> scripts/submit_calibration_predictor_prep.sh
sbatch --dependency=afterok:<cal_pred_job_id> scripts/submit_predictor_parquets.sh
sbatch --dependency=afterok:<parquet_job_id> scripts/submit_transition_identification.sh
sbatch --dependency=afterok:<trans_id_job_id> scripts/submit_transition_dataset_prep.sh

# Stage 2: Feature selection
sbatch --dependency=afterok:<data_prep_final_job_id> scripts/submit_feature_selection.sh

# Stage 3: Transition modelling
sbatch --dependency=afterok:<feat_select_job_id> scripts/submit_transition_modelling.sh

# Stage 4: Allocation parameter calibration
sbatch --dependency=afterok:<model_job_id> scripts/submit_calibrate_allocation_parameters.sh

# Stage 5: Scenario transition rate preparation
sbatch --dependency=afterok:<alloc_param_job_id> scripts/submit_simulation_trans_rates_estimation.sh

# Stage 6: Spatial interventions preparation
sbatch --dependency=afterok:<trans_rates_job_id> scripts/submit_spatial_interventions_prep.sh

# Stage 7: Allocation / Dinamica simulations — run the launcher with bash once
# Stage 6 has finished (it is not an SBATCH job itself; it probes the region list
# and calls sbatch per region, then queues the national-mosaic job afterok):
ALLOC_SCENARIO=NAT bash scripts/submit_allocation_scenario.sh   # one scenario
bash scripts/submit_allocation_all_scenarios.sh                 # all four scenarios
```

### Allocation Smoke Run (Stage 7 validation)

Before committing a full Stage 7 run, submit the smoke job to validate the allocation–Dinamica
wiring on a single scenario and region:

```bash
sbatch scripts/submit_allocation_smoke.sh
```

The smoke job runs sequentially (no parallel fork), targets one scenario (`BAU` by default) and
one region (`costa_peruana` by default), and limits the simulation to the first timestep. Verify
the result with:

```bash
bash scripts/verify_phase3_smoke.sh <JOB_ID> <WORKER_RSS_BUDGET_MB>
```

---

## SLURM Resource Allocations

Resource directives are defined per stage in each `submit_*.sh` script. Adjust
`--mem-per-cpu` and `--time` directly in the script for your data volume.

| Stage | Submit script | CPUs | Mem/CPU | Wall time |
|---|---|---|---|---|
| Ref grid prep | `submit_ref_grid_prep.sh` | 4 | 16 G | 1 h |
| Region prep | `submit_region_prep.sh` | 4 | 16 G | 2 h |
| LULC data prep | `submit_lulc_data_prep.sh` | 4 | 16 G | 6 h |
| Ancillary data prep | `submit_ancillary_data_prep.sh` | 4 | 16 G | 12 h |
| Calibration predictor prep | `submit_calibration_predictor_prep.sh` | 4 | 16 G | 6 h |
| Predictor parquets | `submit_predictor_parquets.sh` | 4 | 16 G | 4 h |
| Transition identification | `submit_transition_identification.sh` | 4 | 16 G | 2 h |
| Transition dataset prep | `submit_transition_dataset_prep.sh` | 4 | 16 G | 4 h |
| Distance calculation | `submit_dist_calc.sh` | 48 | 2.7 G | 24 h |
| Feature selection | `submit_feature_selection.sh` | 4 | 32 G | 72 h |
| Transition modelling | `submit_transition_modelling.sh` | 3 | 42 G | 72 h |
| Alloc. param. calibration | `submit_calibrate_allocation_parameters.sh` | 4 | 28 G | 6 h |
| Sim. transition rates prep | `submit_simulation_trans_rates_estimation.sh` | 6 | 16 G | 4 h |
| Spatial interventions prep | `submit_spatial_interventions_prep.sh` | 4 | 16 G | 4 h |
| Allocation region job (per region) | `submit_allocation_region.sh` (via `submit_allocation_scenario.sh`) | 160 (fat) / 80 (highmem) | whole node (`--exclusive --mem=0`) | 24 h |
| National mosaic assembly | `submit_assemble_mosaic.sh` (queued `afterok` by the launcher) | 4 | node default | 4 h |
| Allocation smoke | `submit_allocation_smoke.sh` | 4 | 16 G | 12 h |
| Allocation monolithic (legacy) | `submit_allocation.sh` | 8 | 8 G | 48 h |
| Dinamica simulations | `submit_dinamica_simulations.sh` | 8 | 8 G | 48 h |

Log files for each job are written to `logs/<job-name>-<JOBID>.{out,err}`.

---

## Required vs Optional Summary

| Variable | Required for HPC | Fails if absent |
|---|---|---|
| `HPC_SCRATCH_ROOT` | Yes | Yes — pre-flight gate exits 1 |
| `HPC_TMP_ROOT` | Yes | Yes — pre-flight gate exits 1 |
| `TERRA_TEMP` | Yes | Yes — pre-flight gate exits 1 |
| `DINAMICA_EGO_8_HOME` | Yes (Stage 7) | Yes — `exec_dinamica()` errors clearly |
| `DINAMICA_BACKEND` | No | No — defaults to `auto` |
| `ALLOCATION_*` tuning vars | No | No — safe defaults apply |
| `GDAL_CACHEMAX` / `OMP_NUM_THREADS` | No | No — defaults apply |

See `docs/CONFIGURATION.md` for the full variable reference.

---

## Monitoring

```bash
squeue -u $USER                    # Check job queue status
sacct -j <JOB_ID>                  # Check job accounting details
scancel <JOB_ID>                   # Cancel a running or pending job
tail -f logs/<stage>-<JOBID>.out   # Stream job stdout
tail -f logs/<stage>-<JOBID>.err   # Stream job stderr
```

SLURM task profiling (`--profile=task`) is enabled in all submit scripts. Profile data can be
queried via `sacct` with energy and I/O fields.

When `ALLOCATION_PROFILE=TRUE` is set (enabled automatically in the smoke job), the allocation
stage emits `PROFILE … elapsed=…s rss_before=…MB rss_after=…MB` lines to the job log.

---

## Rollback Procedure

There is no automated rollback mechanism. The pipeline produces outputs incrementally to the scratch
filesystem under `$HPC_SCRATCH_ROOT/outputs/`. All outputs are regenerable from source inputs by
resubmitting the relevant stage(s).

To rerun a failed or incorrect stage:

1. Identify which stage failed from `sacct` or the job log.
2. Delete or rename the affected output directory under `$HPC_SCRATCH_ROOT/outputs/<stage>/` if
   you need to force recomputation (most functions respect a `refresh_cache` flag to skip existing
   outputs).
3. Resubmit the stage individually using `sbatch scripts/submit_<stage>.sh`.
4. If downstream stages have already consumed corrupt outputs, delete those outputs too and
   resubmit the full chain from the failed stage onwards.

For Stage 7 specifically: delete the corrupt `posterior.tif` files and re-run
`bash scripts/submit_allocation_scenario.sh` — resume is driver-side and automatic. The
launcher skips regions whose posteriors are all present; for any region with a gap, the
driver resumes at the first missing posterior and re-runs the remaining timesteps
(overwriting stale downstream posteriors). Do **not** use `ALLOCATION_YEAR_POST_FILTER`
for resume — it is the single-timestep smoke filter and would run exactly one step.

---

## Operational Issues

### Conda environments wiped (scratch purge)

Environments live in `$HPC_SCRATCH_ROOT/micromamba/envs/` which is on scratch (purged every
60–90 days on Euler). Recreate with:

```bash
source .env
bash scripts/setup_environments.sh
```

To recreate a single environment:

```bash
bash scripts/setup_environments.sh --env allocation_env --non-interactive
```

### Dinamica EGO not found

`DINAMICA_EGO_8_HOME` must point to the built `.sif` image before any Stage 7 job runs. Set it
in `.env` and re-source before submitting:

```bash
export DINAMICA_EGO_8_HOME=/path/to/dinamica-ego-8.sif
source .env
bash scripts/submit_allocation_scenario.sh
```

### HPC_SCRATCH_ROOT unset on HPC

If `setup_environments.sh` or `hpc_common.sh` exits with:

```text
ERROR: HPC context detected (...) but HPC_SCRATCH_ROOT is unset.
```

Source the `.env` file first:

```bash
source .env
bash scripts/setup_environments.sh
```

### Home quota exceeded during container build

The built `.sif` is approximately 1 GB. Always build directly to a path on scratch or the project
filesystem. Never use a relative path from inside the repository clone. See the
[Dinamica Container Build](#dinamica-ego-8-container-build) section for the correct build
command.

### Memory or time limit exceeded

Adjust `#SBATCH --mem-per-cpu` and `#SBATCH --time` in the relevant `submit_*.sh` script. The
table in [SLURM Resource Allocations](#slurm-resource-allocations) shows current defaults.

### Missing scratch directories

Run `source .env` to trigger directory creation (`.env` contains `mkdir -p` calls), or create
manually:

```bash
mkdir -p "$HPC_SCRATCH_ROOT"/{data,results,logs,terra_temp,temp,micromamba/envs}
```

### Micromamba not found at job start time

`hpc_common.sh:find_micromamba()` searches `$MAMBA_EXE_CUSTOM`, `$HOME/.local/bin/micromamba`,
and `/home/$USER/.local/bin/micromamba` in order. If none are found, reinstall:

```bash
bash scripts/install_micromamba_simple.sh
```

Or set `MAMBA_EXE_CUSTOM` to the actual binary path before submitting jobs.

---

## Storage Layout

| Location | Persistence | Contents |
|---|---|---|
| `/home/$USER/nascent-lulcc` | Permanent | Repository code, configs, scripts |
| `$HOME/.local/bin/micromamba` | Permanent | Micromamba binary (~20 MB) |
| `$HPC_SCRATCH_ROOT/micromamba/envs/` | Temporary* | All conda environments (~2–5 GB) |
| `$HPC_SCRATCH_ROOT/data/` | Temporary* | Input rasters and prepared predictor data |
| `$HPC_SCRATCH_ROOT/outputs/` | Temporary* | All pipeline outputs |
| `$HPC_SCRATCH_ROOT/terra_temp/` | Temporary* | terra raster processing intermediates |
| `$DINAMICA_EGO_8_HOME` | Permanent† | Dinamica EGO 8 `.sif` image (~1 GB) |

*Scratch is wiped periodically (approximately every 60–90 days on ETH Euler). Outputs can be
regenerated from source by rerunning the pipeline.

†Recommended location is the project filesystem; scratch is acceptable as a staging location.
<!-- VERIFY: exact scratch purge cycle and home quota size for ETH Euler -->
