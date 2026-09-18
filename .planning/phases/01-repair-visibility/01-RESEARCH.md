# Phase 1: Repair & Visibility - Research

**Researched:** 2026-05-05
**Domain:** Stage 7 allocation runtime hardening, observability, HPC path contracts, and Dinamica container execution
**Confidence:** MEDIUM

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Add a full operator gate before any allocation work starts.
- **D-02:** Validate allocation-only runtime prerequisites rather than auditing the entire upstream pipeline.
- **D-03:** Report all missing prerequisites in one actionable failure list instead of failing category-by-category.
- **D-04:** Centralize pre-flight checks in a standalone helper called by `run_allocation()`.

### Log Surface And Crash Breadcrumbing
- **D-05:** Pass the per-region log path into key inner helpers, especially `setup_allocation_inputs()` and `run_allocation_dinamica()`.
- **D-06:** When a worker dies unexpectedly, emit a sentinel record with the last-known stage, scenario, region, timestep, and transition context.
- **D-07:** Keep a timestamped Dinamica subprocess log under `logs/` and mirror important Dinamica lifecycle events into the per-region log.
- **D-08:** Standardize critical lifecycle events as structured messages, but do not normalize every log line in Phase 1.

### Dinamica-on-Euler Operating Model
- **D-09:** Use one Dinamica entrypoint in code, with backend selection by environment: wrapper/container behavior on HPC and direct `DinamicaConsole` locally.
- **D-10:** Treat the Singularity image as an external artifact rather than a repo-owned build output.
- **D-11:** Add a smoke-test contract so Phase 1 can verify the HPC Dinamica wiring before a real allocation run.

### Path Repair Strategy
- **D-12:** Centralize environment/path resolution behind shared helpers and config lookups instead of ad hoc fixes in each script.
- **D-13:** Treat YAML config as authoritative, with a small set of env vars for machine-specific overrides such as temp dirs and Dinamica location.
- **D-14:** Remove hardcoded user-specific HPC paths from active code, touched docs, and operational helpers modified in this phase.
- **D-15:** Require explicit HPC temp/beegfs env vars and fail pre-flight clearly if they are missing.

### the agent's Discretion
No discretionary areas were delegated to the agent during discussion.

### Deferred Ideas (OUT OF SCOPE)
None - discussion stayed within phase scope.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| OBS-01 | RAM profiling reports real values — `rss_before/after/peak` are valid numbers, not "NAMB", on both local and HPC | Use `ps::ps_memory_info()` as the portable RSS source and keep Linux-specific extras optional. [VERIFIED: repo grep src/allocation.r] [CITED: https://ps.r-lib.org/reference/ps_memory_info.html] |
| OBS-02 | When an allocation worker crashes (including SIGKILL), the region log contains a sentinel trace and `diagnose_alloc_crash.sh` surfaces the OOM evidence from `sacct`/`seff` | Add last-stage breadcrumbing in worker scope and use `sacct`/`seff` as the standard post-mortem source. [VERIFIED: repo grep scripts/submit_allocation_profile.sh] [CITED: https://slurm.schedmd.com/sacct.html] |
| OBS-03 | `setup_allocation_inputs` and `run_allocation_dinamica` emit structured messages to the per-region log file | `setup_allocation_inputs()` already accepts `log_file`; `run_allocation_dinamica()` does not yet. [VERIFIED: repo grep src/allocation.r src/dinamica_utils.r] |
| OBS-04 | Allocation entry runs pre-flight validation and fails fast with one actionable list of all gaps | `run_allocation()` is the correct top-level insertion point for a consolidated operator gate. [VERIFIED: repo grep src/allocation.r] |
| PIPE-01 | `simulation_trans_rates_prep.r` reads LULC demand from config-driven CSV path | The active code still reads a hardcoded Windows XLSX path while the CSV path remains commented out. [VERIFIED: repo grep src/simulation_trans_rates_prep.r] |
| PIPE-03 | `calibration_predictor_prep.r` reads terra temp directory from env var | The active code still hardcodes `E:/terra_temp`. [VERIFIED: repo grep src/calibration_predictor_prep.r] |
| PIPE-04 | HPC shell scripts use `$USER` in all paths | `.env.template`, `hpc_common.sh`, `setup_environments.sh`, and `config/hpc_config.yaml` still contain `black` paths. [VERIFIED: repo grep scripts .env.template config/hpc_config.yaml] |
| PIPE-07 | Dinamica EGO log files are written to the central `logs/` directory | `exec_dinamica()` still writes logs beside the `.ego` model path. [VERIFIED: repo grep src/dinamica_utils.r] |
| MEM-06 | `allocation_env.yml` includes all prediction-time packages | `allocation_env.yml` currently contains only a minimal subset and omits the required prediction stack. [VERIFIED: repo grep environments/allocation_env.yml] |
| INFRA-01 | Dinamica EGO 8 runs inside a Singularity container on Euler and is invocable from `exec_dinamica()` | Apptainer/Singularity `exec` with bind mounts is the standard HPC container invocation pattern; the referenced ETH image repo could not be verified in-session. [CITED: https://apptainer.org/docs/user/main/cli/apptainer_exec.html] [CITED: https://apptainer.org/docs/user/1.3/bind_paths_and_mounts.html] [ASSUMED] |
</phase_requirements>

## Summary

Phase 1 should be planned as a runtime-contract repair, not as a broad refactor. The code already has the right structural chokepoints: `run_allocation()` is the gatekeeper, `run_allocation_one_timestep()` is the worker lifecycle boundary, `setup_allocation_inputs()` is the file-prep seam, and `exec_dinamica()` is the single Dinamica execution adapter. [VERIFIED: repo grep src/allocation.r src/dinamica_utils.r]

The biggest planning insight is that three failures are coupled and should be solved together: broken RSS reporting, missing Stage 7 pre-flight validation, and drift between environment files and submit scripts. Current profiling reads `/proc/self/status`, which is Linux-only and therefore cannot satisfy the requirement for valid local Windows RSS values; current run scripts still attempt opportunistic package installation at runtime; and the submit scripts activate `transition_model_env` while the requirement explicitly targets `allocation_env.yml`. [VERIFIED: repo grep src/allocation.r scripts/run_allocation.r scripts/submit_allocation.sh environments/allocation_env.yml environments/transition_model_env.yml] [CITED: https://ps.r-lib.org/reference/ps_memory_info.html]

The Dinamica-on-Euler work should be planned around a wrapper contract, not around scattering container logic through R code. Keep one R entrypoint, but let the HPC backend call `apptainer exec` or `singularity exec` with explicit binds, explicit scratch/temp env, a central log destination, and a smoke-test model. This aligns with the locked decisions and with Apptainer’s standard execution and bind model. [CITED: https://apptainer.org/docs/user/main/cli/apptainer_exec.html] [CITED: https://apptainer.org/docs/user/1.3/bind_paths_and_mounts.html]

**Primary recommendation:** Plan Phase 1 around three deliverables: one consolidated Stage 7 pre-flight helper, one unified Dinamica launcher with local/HPC backends, and one shared path/env contract reused by R and shell entrypoints. [VERIFIED: repo grep src/allocation.r src/dinamica_utils.r scripts/hpc_common.sh scripts/setup_environments.sh]

## Project Constraints (from AGENTS.md)

- No `AGENTS.md` file exists at repo root, so there are no project-local directives beyond the planning artifacts already loaded. [VERIFIED: repo root listing]

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Stage 7 pre-flight validation | API / Backend | Frontend Server (SSR) — | The checks are pure runtime/business validation performed before allocation work begins in R entry code. [VERIFIED: repo grep src/allocation.r] |
| Per-region structured logging | API / Backend | Database / Storage | The log events are emitted by R workers and persisted to the filesystem under `logs/` / worker log files. [VERIFIED: repo grep src/allocation.r src/utils.r src/dinamica_utils.r] |
| Dinamica subprocess/container execution | API / Backend | CDN / Static — | Process spawning, env wiring, and backend switching are runtime orchestration responsibilities, not data-layer or client concerns. [VERIFIED: repo grep src/dinamica_utils.r] |
| HPC path/env bootstrap | API / Backend | Database / Storage | The shell and R layers resolve filesystem paths and env vars before reading or writing data. [VERIFIED: repo grep scripts/hpc_common.sh scripts/setup_environments.sh src/setup.r] |
| Dinamica log artifact storage | Database / Storage | API / Backend | The owning concern is stable filesystem placement and retention, while the backend decides when to write. [VERIFIED: repo grep src/dinamica_utils.r] |
| Post-mortem OOM diagnosis | API / Backend | Database / Storage | The diagnostic script queries SLURM accounting and correlates it with log artifacts. [VERIFIED: repo grep scripts/submit_allocation_profile.sh] [CITED: https://slurm.schedmd.com/sacct.html] |
| Allocation env completeness | API / Backend | — | Dependency resolution for Stage 7 is part of backend runtime provisioning. [VERIFIED: repo grep environments/allocation_env.yml scripts/submit_allocation.sh] |

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `r-base` | `4.3` | Stage 7 runtime for allocation and Dinamica orchestration. [VERIFIED: repo grep environments/allocation_env.yml environments/transition_model_env.yml] | Already pinned across stage env files and is the active project runtime. [VERIFIED: repo grep environments/*.yml] |
| `ps` | Add to `allocation_env.yml` (repo currently missing pin) | Portable process RSS measurement on Windows and Linux. [VERIFIED: requirements MEM-06] [CITED: https://ps.r-lib.org/reference/ps_memory_info.html] | Its `rss` field is documented as portable and maps to real process resident memory on both UNIX and Windows. [CITED: https://ps.r-lib.org/reference/ps_memory_info.html] |
| `processx` | Existing, unpinned in repo envs | Subprocess execution for Dinamica and wrapper commands. [VERIFIED: repo grep src/dinamica_utils.r environments/allocation_env.yml] | The code already uses it at the Dinamica chokepoint, so Phase 1 should extend rather than replace it. [VERIFIED: repo grep src/dinamica_utils.r] |
| `Apptainer` / `Singularity` | Site-provided runtime | Execute Dinamica inside a SIF image on Euler. [CITED: https://apptainer.org/docs/user/main/cli/apptainer_exec.html] | `exec` against SIF images plus bind mounts is the standard HPC container pattern. [CITED: https://apptainer.org/docs/user/main/cli/apptainer_exec.html] [CITED: https://apptainer.org/docs/user/1.3/bind_paths_and_mounts.html] |
| `SLURM sacct` | Site-provided | Standard source for job memory/accounting evidence. [CITED: https://slurm.schedmd.com/sacct.html] | `MaxRSS`, job state, and `OUT_OF_MEMORY` are first-class accounting fields/states. [CITED: https://slurm.schedmd.com/sacct.html] [CITED: https://slurm.schedmd.com/job_state_codes.html] |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `r-terra` | Existing, unpinned in repo envs | Raster IO and tempdir-sensitive processing during prep/allocation. [VERIFIED: repo grep environments/allocation_env.yml scripts/run_allocation.r] | Use wherever Phase 1 touches temp path repair or raster writes. [VERIFIED: repo grep src/calibration_predictor_prep.r src/allocation.r] |
| `r-arrow` | Existing, unpinned in repo envs | Predictor parquet access in allocation. [VERIFIED: repo grep environments/allocation_env.yml src/allocation.r] | Required because pre-flight should validate prediction-time parquet loading. [VERIFIED: repo grep src/allocation.r] |
| `r-workflows`, `r-parsnip`, `r-recipes`, `r-ranger`, `r-xgboost`, `r-tidypredict`, `r-butcher`, `r-bundle`, `r-qs`, `r-lobstr`, `r-rhpcblasctl` | Mixed: partially present, mostly absent from `allocation_env.yml` | Prediction-time model loading and compatibility. [VERIFIED: repo grep environments/allocation_env.yml environments/transition_model_env.yml .planning/REQUIREMENTS.md] | Use to satisfy MEM-06 and to make pre-flight validate the real Stage 7 prediction stack instead of a minimal subset. [VERIFIED: requirements MEM-06] |
| `seff` | Site-provided helper | Human-readable memory efficiency report for completed jobs. [VERIFIED: requirements OBS-02] | Use as a best-effort supplement to `sacct`, not as the only evidence source. [CITED: https://docs.hpc.shef.ac.uk/en/latest/referenceinfo/scheduler/SLURM/Common-commands/seff.html] |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `ps::ps_memory_info()` | Continue parsing `/proc/self/status` | `/proc` gives Linux-specific extras like `VmHWM`, but it cannot satisfy Windows-local RSS correctness on its own. [VERIFIED: repo grep src/allocation.r] [CITED: https://ps.r-lib.org/reference/ps_memory_info.html] |
| One Dinamica adapter with backend selection | Separate local and HPC codepaths in different scripts | Separate entrypoints would duplicate logging, validation, and path logic and would violate D-09. [VERIFIED: CONTEXT D-09] |
| `sacct` + optional `seff` | `seff` only | `seff` is convenient but less authoritative and less predictable across clusters than direct SLURM accounting fields. [CITED: https://slurm.schedmd.com/sacct.html] [CITED: https://docs.hpc.shef.ac.uk/en/latest/referenceinfo/scheduler/SLURM/Common-commands/seff.html] |

**Installation:**  
Use `allocation_env.yml` as the canonical Phase 1 prediction/runtime environment, then make the submit scripts activate it. [VERIFIED: requirements MEM-06] [VERIFIED: repo grep scripts/submit_allocation.sh]

**Version verification:**  
Repo-pinned versions verified in-session: `r-base=4.3` in both allocation and transition-model envs, and `r-xgboost=1.7` in `transition_model_env.yml`. [VERIFIED: repo grep environments/allocation_env.yml environments/transition_model_env.yml]  
Non-pinned additions required by MEM-06 should be added explicitly to `allocation_env.yml`; current exact package versions were not registry-verified in-session. [VERIFIED: requirements MEM-06] [ASSUMED]

## Architecture Patterns

### System Architecture Diagram

```text
scripts/run_allocation.r
  -> load config via src/setup.r
  -> run Stage 7 pre-flight helper
      -> validate env vars
      -> validate package loadability
      -> validate file/model inputs
      -> validate Dinamica backend availability
  -> future workers by region
      -> initialize per-region worker log
      -> emit lifecycle breadcrumb state
      -> setup_allocation_inputs(..., log_file)
          -> read transition tables / allocation params
          -> write Dinamica CSV inputs
          -> generate probability maps
      -> run_allocation_dinamica(..., log_file)
          -> local: direct DinamicaConsole
          -> HPC: apptainer/singularity wrapper
          -> write timestamped Dinamica subprocess log under logs/
      -> on failure/exit
          -> sentinel record with last known stage/context
  -> if SLURM failure suspected
      -> diagnose_alloc_crash.sh
          -> sacct / seff / cgroup evidence
          -> correlate with region log + Dinamica log
```

### Recommended Project Structure

```text
src/
├── allocation.r                # Stage 7 orchestration, pre-flight call, worker breadcrumbs
├── dinamica_utils.r            # Single Dinamica adapter + backend selection
├── setup.r                     # Shared config/env/path resolution
└── utils.r                     # Logging and reusable validation helpers

scripts/
├── run_allocation.r            # Thin entrypoint, no runtime package installs
├── submit_allocation.sh        # SLURM wrapper activating canonical allocation env
├── hpc_common.sh               # Shared HPC env bootstrap using $USER and explicit scratch vars
└── diagnose_alloc_crash.sh     # Post-mortem helper for sacct/seff/log correlation

environments/
└── allocation_env.yml          # Canonical prediction-time dependency definition
```

### Pattern 1: Consolidated Stage 7 Pre-flight Gate
**What:** One helper called by `run_allocation()` that aggregates all missing prerequisites before any region work starts. [VERIFIED: CONTEXT D-01 D-02 D-03 D-04]  
**When to use:** Before `future::plan()` and before any raster/model/parquet reads. [VERIFIED: repo grep scripts/run_allocation.r src/allocation.r]  
**Example:**
```r
# Source: repo pattern + locked decision D-04
preflight <- validate_allocation_runtime(config)
if (!preflight$ok) {
  stop(paste(c("Allocation pre-flight failed:", preflight$errors), collapse = "\n"))
}
```

### Pattern 2: Single Dinamica Backend Adapter
**What:** Preserve one code entrypoint (`exec_dinamica()`), but branch inside it between local direct `DinamicaConsole` and HPC container/wrapper execution. [VERIFIED: CONTEXT D-09] [VERIFIED: repo grep src/dinamica_utils.r]  
**When to use:** For every Dinamica invocation, including smoke tests. [VERIFIED: CONTEXT D-11]  
**Example:**
```r
# Source: repo pattern + Apptainer exec docs
if (detect_environment() == "hpc") {
  processx::run("apptainer", c("exec", "--bind", bind_spec, sif_path, "DinamicaConsole", model_path))
} else {
  processx::run("DinamicaConsole", c(model_path))
}
```

### Pattern 3: Worker Breadcrumb + Sentinel Logging
**What:** Track last-known worker state in-memory and flush a sentinel record on handled failure or deferred cleanup. [VERIFIED: CONTEXT D-06]  
**When to use:** Around region setup, input generation, per-transition prediction, Dinamica launch, and post-run validation. [VERIFIED: repo grep src/allocation.r]  
**Example:**
```r
# Source: repo logging pattern
state <- list(stage = "region_setup", scenario = scenario, region = region_label, timestep = year_post, transition = NA)
log_msg(sprintf("STATE stage=%s scenario=%s region=%s timestep=%s", state$stage, state$scenario, state$region, state$timestep), log_file)
```

### Anti-Patterns to Avoid

- **Runtime package self-healing:** `scripts/run_allocation.r` and sibling entrypoints currently attempt `install.packages()` at runtime; Phase 1 should remove or bypass that behavior for Stage 7 because it hides broken envs instead of failing fast. [VERIFIED: repo grep scripts/run_allocation.r scripts/run_simulation_trans_rates_prep.r scripts/run_calibration_predictor_prep.r]
- **Dual source of truth for allocation runtime env:** Planning around `transition_model_env` while separately “fixing” `allocation_env.yml` would leave MEM-06 unsatisfied. [VERIFIED: repo grep scripts/submit_allocation.sh requirements MEM-06]
- **Worker-local Dinamica logs inside model/work directories:** This scatters artifacts and makes crash triage slower. [VERIFIED: repo grep src/dinamica_utils.r]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Cross-platform RSS metrics | Custom `/proc` and Windows-branch memory parsers | `ps::ps_memory_info()` | The package already documents portable `rss` and `vmem` fields across UNIX and Windows. [CITED: https://ps.r-lib.org/reference/ps_memory_info.html] |
| Container filesystem wiring | Ad hoc shell glue that guesses mounts | `apptainer exec --bind` or `APPTAINER_BINDPATH` | Bind mounts are first-class container runtime features and map exactly to the scratch/data contract needed here. [CITED: https://apptainer.org/docs/user/1.3/bind_paths_and_mounts.html] |
| OOM post-mortem parsing | Grepping arbitrary stdout for memory clues only | `sacct` fields plus optional `seff` summary | SLURM already exposes job state and `MaxRSS`, including `OUT_OF_MEMORY`. [CITED: https://slurm.schedmd.com/sacct.html] [CITED: https://slurm.schedmd.com/job_state_codes.html] |
| Multi-file path repair | Script-by-script hardcoded path fixes | Shared helpers in `src/setup.r` and shared shell env bootstrap | The codebase already centralizes environment detection and config expansion there. [VERIFIED: repo grep src/setup.r scripts/hpc_common.sh] |

**Key insight:** Phase 1 succeeds by reducing hidden runtime state, not by adding more fallback logic. The correct bias is “validate early, fail loudly, log once in the right place.” [VERIFIED: CONTEXT D-01 D-03 D-07 D-12 D-15]

## Common Pitfalls

### Pitfall 1: Linux-only profiling masquerading as portable profiling
**What goes wrong:** `rss_before/after/peak` become `NA` on Windows-local runs because the current implementation depends on `/proc/self/status`. [VERIFIED: repo grep src/allocation.r]  
**Why it happens:** The code reads Linux kernel procfs directly and returns `NA` when `/proc` is unavailable. [VERIFIED: repo grep src/allocation.r]  
**How to avoid:** Use `ps::ps_memory_info()` for the portable RSS path and keep `/proc`-specific peak/HWM reads as Linux-only enrichment if desired. [CITED: https://ps.r-lib.org/reference/ps_memory_info.html]  
**Warning signs:** `NAMB` or `NA` RSS fields in profile logs, especially on local Windows. [VERIFIED: requirements OBS-01] [VERIFIED: project docs PROJECT.md]

### Pitfall 2: Fixing `allocation_env.yml` without changing the submit script
**What goes wrong:** MEM-06 appears fixed on paper, but HPC jobs still run under `transition_model_env`. [VERIFIED: repo grep scripts/submit_allocation.sh environments/allocation_env.yml environments/transition_model_env.yml]  
**Why it happens:** The operational entrypoint currently points at the wrong environment for Stage 7. [VERIFIED: repo grep scripts/submit_allocation.sh]  
**How to avoid:** Treat “canonical Stage 7 env file” and “submit script activation target” as one atomic plan item. [VERIFIED: requirements MEM-06]  
**Warning signs:** Pre-flight passes in one interactive env but batch jobs still fail to load packages. [ASSUMED]

### Pitfall 3: Runtime package installation hiding broken infrastructure
**What goes wrong:** Scripts attempt to install missing packages at startup, which can silently mutate user libraries, fail under restricted HPC nodes, or mask env drift. [VERIFIED: repo grep scripts/run_allocation.r scripts/run_simulation_trans_rates_prep.r scripts/run_calibration_predictor_prep.r]  
**Why it happens:** Entry scripts are doing environment repair instead of environment validation. [VERIFIED: repo grep scripts/run_allocation.r]  
**How to avoid:** Pre-flight should test `requireNamespace()` and stop with one actionable list; environment creation belongs in `setup_environments.sh` or manual provisioning. [VERIFIED: CONTEXT D-01 D-03]  
**Warning signs:** Batch logs showing `Attempting to install missing packages into R_LIBS_USER...`. [VERIFIED: repo grep scripts/run_allocation.r]

### Pitfall 4: Path repairs done only in R or only in shell
**What goes wrong:** HPC checkouts still require manual edits because shell helpers, `.env.template`, and YAML disagree about scratch/temp/base paths. [VERIFIED: repo grep scripts/hpc_common.sh scripts/setup_environments.sh .env.template config/hpc_config.yaml]  
**Why it happens:** The project currently has user-specific paths in multiple layers. [VERIFIED: repo grep scripts/hpc_common.sh scripts/setup_environments.sh .env.template config/hpc_config.yaml]  
**How to avoid:** Define one shared contract: YAML is authoritative, env vars are only machine-specific overrides, and shell helpers derive from `$USER` plus explicit scratch vars. [VERIFIED: CONTEXT D-12 D-13 D-15]  
**Warning signs:** Any touched Phase 1 file still containing `/.../black/...` after the repair. [VERIFIED: requirements PIPE-04]

### Pitfall 5: Treating Dinamica container support as “just a path”
**What goes wrong:** `DINAMICA_EGO_8_HOME` semantics become ambiguous between local install directories and HPC container images/wrappers. [VERIFIED: repo grep src/dinamica_utils.r requirements INFRA-01]  
**Why it happens:** Current code assumes a direct local installation layout with `usr/lib` beneath `DINAMICA_EGO_8_HOME`. [VERIFIED: repo grep src/dinamica_utils.r]  
**How to avoid:** Plan an explicit backend contract, e.g. local `DINAMICA_EGO_8_HOME=<install dir>` and HPC `DINAMICA_EGO_8_HOME=<wrapper or image path>` with backend-specific resolution inside `exec_dinamica()`. [VERIFIED: CONTEXT D-09 D-13] [ASSUMED]  
**Warning signs:** Code outside `exec_dinamica()` starts branching on container details. [VERIFIED: CONTEXT D-09]

## Code Examples

Verified patterns from official sources and current repo structure:

### Portable RSS lookup
```r
# Source: https://ps.r-lib.org/reference/ps_memory_info.html
rss_mb <- unname(ps::ps_memory_info()[["rss"]]) / (1024 * 1024)
```

### Standard Apptainer execution with bind mounts
```bash
# Source: https://apptainer.org/docs/user/main/cli/apptainer_exec.html
# Source: https://apptainer.org/docs/user/1.3/bind_paths_and_mounts.html
apptainer exec \
  --bind /beegfs/$USER/nascent-lulcc:/workspace \
  /path/to/dinamica.sif \
  DinamicaConsole /workspace/allocation.ego
```

### Standard SLURM accounting query for memory and job state
```bash
# Source: https://slurm.schedmd.com/sacct.html
sacct -j "$SLURM_JOB_ID" \
  --format=JobID,JobName,State,ExitCode,MaxRSS,Elapsed \
  --units=M
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `Singularity` naming only | `Apptainer` is the current project/runtime name, while SIF remains the image format | Apptainer `v1.0.0+` per current docs | Phase 1 should support site reality by probing `apptainer` first and `singularity` second if needed. [CITED: https://apptainer.org/docs/user/main/cli/apptainer_exec.html] |
| Direct local binary invocation assumptions | HPC container execution via `exec` + bind mounts | Current Apptainer user guide | Container wiring should stay behind the Dinamica adapter instead of leaking into callers. [CITED: https://apptainer.org/docs/user/main/cli/apptainer_exec.html] [CITED: https://apptainer.org/docs/user/1.3/bind_paths_and_mounts.html] |
| Stdout-only crash clues | Per-worker structured logs plus SLURM accounting | Current project decision + existing profile submit script | Phase 1 can deliver useful observability without building a full test harness. [VERIFIED: CONTEXT D-06 D-07] [VERIFIED: repo grep scripts/submit_allocation_profile.sh] |

**Deprecated/outdated:**
- Parsing `/proc/self/status` as the only RSS source is outdated for this phase because the requirement explicitly spans local Windows and HPC Linux. [VERIFIED: repo grep src/allocation.r] [CITED: https://ps.r-lib.org/reference/ps_memory_info.html]
- Hardcoded `E:/...` and `/.../black/...` paths are outdated relative to the config-driven environment model already present in `src/setup.r`. [VERIFIED: repo grep src/calibration_predictor_prep.r src/simulation_trans_rates_prep.r scripts/hpc_common.sh scripts/setup_environments.sh .env.template config/hpc_config.yaml src/setup.r]

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Euler will provide either `apptainer` or `singularity` in batch environments and allow the necessary bind mounts. | Standard Stack / Environment Availability | The HPC Dinamica wrapper design could need site-specific changes or a different launcher. |
| A2 | The intended HPC contract for `DINAMICA_EGO_8_HOME` can safely shift from “install directory” to “wrapper or image path resolved by `exec_dinamica()`”. | Common Pitfalls / Architecture Patterns | If another phase or external operator tooling depends on the old semantics, Phase 1 could break local execution. |
| A3 | The referenced `ethzplus/rocker-geospatial-dinamica` image source remains the intended base even though it could not be fetched in-session. | Standard Stack / Open Questions | The planner may target the wrong container base or miss repo-specific build conventions. |
| A4 | Batch jobs should ultimately run from `allocation_env.yml` rather than `transition_model_env.yml`. | Common Pitfalls / Open Questions | If the user intentionally chose the transition-model env for allocation, changing submit scripts would be a regression. |

## Open Questions (RESOLVED)

1. **Euler runtime name (`apptainer` vs `singularity`)**
   - Resolution: Phase 1 will not hardcode a single site runtime name up front. The Dinamica adapter and smoke test will probe `apptainer` first, then `singularity`, print the selected runtime in dry-run output, and fail clearly if neither exists. This resolves the planning gap without adding a human checkpoint. [ASSUMED] [VERIFIED: requirements INFRA-01]

2. **Canonical Stage 7 execution environment**
   - Resolution: `allocation_env.yml` is the canonical Stage 7 runtime environment for Phase 1 because MEM-06 and the roadmap name it explicitly; the submit scripts must activate it rather than `transition_model_env`. Any duplicate-package drift is treated as an implementation repair, not an open planning decision. [VERIFIED: requirements MEM-06] [VERIFIED: repo grep scripts/submit_allocation.sh] [VERIFIED: ROADMAP Phase 1 success criteria 5]

3. **Dinamica binary path inside the target image**
   - Resolution: Phase 1 will not assume a fixed container-internal filesystem path in advance. The unified adapter will resolve the external artifact/wrapper path from config, derive the executable command through a dry-run/resolution helper, and the smoke test will print the resolved command, artifact path, and logfile path before any real run. If the image does not expose `DinamicaConsole` as expected, the smoke test becomes the failing proof point rather than a hidden runtime surprise. [ASSUMED] [VERIFIED: CONTEXT D-09 D-11]

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| `micromamba` | Environment provisioning scripts | ✓ | `2.4.0` [VERIFIED: shell tool audit] | — |
| `bash` | HPC shell helpers and submit scripts | ✓ | Version probe failed on Windows, but command exists. [VERIFIED: shell tool audit] | PowerShell cannot replace HPC batch shell behavior cleanly. [ASSUMED] |
| `Rscript` | Local execution and any local smoke test | ✗ | — [VERIFIED: shell tool audit] | No local fallback detected. |
| `apptainer` | Euler container execution contract | ✗ on current workstation | — [VERIFIED: shell tool audit] | Probe on Euler; optionally support `singularity`. [ASSUMED] |
| `singularity` | Alternate Euler container execution contract | ✗ on current workstation | — [VERIFIED: shell tool audit] | Probe on Euler; prefer `apptainer` if available. [ASSUMED] |
| `docker` | Local container image inspection/build experiments | ✗ | — [VERIFIED: shell tool audit] | Use external artifact + HPC runtime smoke test instead. [VERIFIED: CONTEXT D-10] |
| `git` | Repo-based smoke-test helpers and docs | ✓ | `2.32.0.windows.2` [VERIFIED: shell tool audit] | — |
| `rg` | Fast code/file auditing during implementation | ✓ | `15.1.0` [VERIFIED: shell tool audit] | — |

**Missing dependencies with no fallback:**
- `Rscript` on the current workstation blocks any local execution-based verification in this workspace. [VERIFIED: shell tool audit]

**Missing dependencies with fallback:**
- `apptainer` / `singularity` are absent locally, but Phase 1 can still plan an HPC-only smoke-test contract because the requirement is Euler-specific. [VERIFIED: requirements INFRA-01] [ASSUMED]

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | No auth surface identified in this phase. [VERIFIED: project docs INTEGRATIONS.md] |
| V3 Session Management | no | No session surface identified in this phase. [VERIFIED: project docs INTEGRATIONS.md] |
| V4 Access Control | no | This phase is local/HPC batch execution rather than an application authorization surface. [VERIFIED: project docs INTEGRATIONS.md] |
| V5 Input Validation | yes | Validate env vars, file paths, executable presence, and config-derived inputs before work starts. [VERIFIED: CONTEXT D-01 D-02 D-03 D-15] |
| V6 Cryptography | no | No cryptographic behavior is in scope. [VERIFIED: project docs INTEGRATIONS.md] |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Executing the wrong Dinamica binary or wrapper from `PATH` | Elevation of Privilege / Tampering | Resolve executable path explicitly during pre-flight and log the resolved backend before launch. [VERIFIED: repo grep src/dinamica_utils.r] [ASSUMED] |
| Shell/path injection through unvalidated bind or scratch env vars | Tampering | Restrict pre-flight to expected absolute paths and quote subprocess arguments through `processx` instead of shell string concatenation where possible. [VERIFIED: repo grep src/dinamica_utils.r scripts/hpc_common.sh] [ASSUMED] |
| Writing runtime logs into versioned model directories | Tampering / Repudiation | Move Dinamica logs to central `logs/` and record their paths in region logs. [VERIFIED: repo grep src/dinamica_utils.r] [VERIFIED: CONTEXT D-07] |
| Silent dependency drift from runtime package installation | Repudiation / Tampering | Remove self-healing installs from Stage 7 execution path and make env completeness a pre-flight failure. [VERIFIED: repo grep scripts/run_allocation.r] |

## Sources

### Primary (HIGH confidence)
- Repository code and planning artifacts - `src/allocation.r`, `src/dinamica_utils.r`, `src/setup.r`, `src/utils.r`, `src/calibration_predictor_prep.r`, `src/simulation_trans_rates_prep.r`, `scripts/hpc_common.sh`, `scripts/setup_environments.sh`, `scripts/run_allocation.r`, `scripts/submit_allocation.sh`, `scripts/submit_allocation_profile.sh`, `environments/allocation_env.yml`, `environments/transition_model_env.yml`, `.planning/REQUIREMENTS.md`, `.planning/PROJECT.md`, `.planning/ROADMAP.md`, `.planning/phases/01-repair-visibility/01-CONTEXT.md`. [VERIFIED: repo grep]
- `ps` official docs - portable RSS and memory field semantics. https://ps.r-lib.org/reference/ps_memory_info.html
- Apptainer official docs - `exec` contract and bind-mount behavior. https://apptainer.org/docs/user/main/cli/apptainer_exec.html ; https://apptainer.org/docs/user/1.3/bind_paths_and_mounts.html
- Slurm official docs - `sacct` fields and `OUT_OF_MEMORY` job state. https://slurm.schedmd.com/sacct.html ; https://slurm.schedmd.com/job_state_codes.html

### Secondary (MEDIUM confidence)
- Sheffield HPC `seff` usage reference - practical output shape and role of `seff` as a summary helper. https://docs.hpc.shef.ac.uk/en/latest/referenceinfo/scheduler/SLURM/Common-commands/seff.html

### Tertiary (LOW confidence)
- Referenced ETH container image source `ethzplus/rocker-geospatial-dinamica` could not be fetched in-session and remains unverified. [ASSUMED]

## Metadata

**Confidence breakdown:**
- Standard stack: MEDIUM - The repo chokepoints and official `ps` / Apptainer / Slurm docs are solid, but the exact Euler container image/runtime details were not directly verified. 
- Architecture: HIGH - The implementation seams are explicit in the current codebase and align cleanly with the locked decisions.
- Pitfalls: HIGH - The major path/env/logging/profile drifts are directly observable in repo code and scripts.

**Research date:** 2026-05-05  
**Valid until:** 2026-05-12
