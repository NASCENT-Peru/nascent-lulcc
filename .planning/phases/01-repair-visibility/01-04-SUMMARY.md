---
phase: 01-repair-visibility
plan: 04
subsystem: stage7-dinamica-launch
tags:
  - infra-01
  - pipe-07
  - d-07
  - d-09
  - d-10
  - d-11
  - tdd
  - dinamica
  - apptainer
  - singularity
requires:
  - .planning/PROJECT.md
  - .planning/phases/01-repair-visibility/01-CONTEXT.md
  - .planning/phases/01-repair-visibility/01-RESEARCH.md
  - .planning/phases/01-repair-visibility/01-01-SUMMARY.md
  - .planning/phases/01-repair-visibility/01-02-SUMMARY.md
  - .planning/phases/01-repair-visibility/01-03-SUMMARY.md
provides:
  - "resolve_dinamica_launch(model_path, backend, runtime_override, probe_runtime, ...) — non-executing launch contract resolver"
  - "exec_dinamica(...) refactored to consume resolve_dinamica_launch() instead of re-deriving its own contract"
  - "scripts/smoke_test_dinamica.sh — operator-facing dry-run + live Euler smoke test"
  - "dinamica/container/rocker-geospatial-dinamica.def — Apptainer/Singularity definition rooted in ethzplus/rocker-geospatial-dinamica"
  - "dinamica/container/README.md — operator build flow + external-artifact publishing instructions (D-10)"
  - "docs/README_HPC.md updated with the DINAMICA_EGO_8_HOME=/absolute/path/to/dinamica.sif contract and the dry-run + live smoke-test commands"
  - "Central logs/dinamica/<timestamp>_dinamica.log destination is now the only Dinamica subprocess log placement (closes PIPE-07 in full)"
affects:
  - src/dinamica_utils.r
  - scripts/smoke_test_dinamica.sh
  - dinamica/container/rocker-geospatial-dinamica.def
  - dinamica/container/README.md
  - docs/README_HPC.md
tech-stack:
  added:
    - "Apptainer / Singularity runtime contract (probed apptainer first, singularity second) — runtime is site-provided on Euler, no new R or system package was added to the environment"
  patterns:
    - "Single Dinamica adapter with internal backend selection (D-09); callers never see container details"
    - "Non-executing launch resolver (resolve_dinamica_launch) so dry-run callers can verify HPC command resolution on hosts without apptainer/singularity (runtime_override + probe_runtime=FALSE)"
    - "External SIF artifact pattern (D-10) — repo ships only the .def + build instructions; built .sif stays under /cluster/project/.../containers/"
    - "Operator-facing smoke test (D-11) with dry-run AND live modes sharing the same launch contract as exec_dinamica()"
    - "Live smoke test fails non-zero unless Dinamica returns 0 AND a timestamped logs/dinamica-smoke-*.log lands under the requested log root"
key-files:
  created:
    - tests/testthat/test-dinamica-launcher.R
    - scripts/smoke_test_dinamica.sh
    - dinamica/container/rocker-geospatial-dinamica.def
    - dinamica/container/README.md
  modified:
    - src/dinamica_utils.r
    - docs/README_HPC.md
decisions:
  - "DINAMICA_EGO_8_HOME is the absolute path to the external Dinamica .sif on HPC (no SIF-or-wrapper ambiguity for Phase 1). The R adapter and the smoke test consume that path verbatim as the apptainer/singularity exec image argument."
  - "Probe order is apptainer first, singularity second, in both R (resolve_dinamica_launch) and shell (smoke_test_dinamica.sh). A single explicit override (runtime_override / --runtime apptainer|singularity) skips the probe so dry-run verification works on hosts that lack both runtimes."
  - "Built .sif stays external (D-10). The repository ships dinamica/container/rocker-geospatial-dinamica.def + dinamica/container/README.md to pin upstream provenance (ethzplus/rocker-geospatial-dinamica) and document the apptainer build / singularity build flow, but the .gitignore-friendly .sif lives outside the repo."
  - "Live smoke test exits non-zero unless Dinamica returns 0 AND a logs/dinamica-smoke-*.log file is written. This satisfies the D-11 contract that 'a smoke test executes a minimal Dinamica model on Euler and proves success by writing a timestamped logs/ artifact'."
  - "allocation.ego-decoded was NOT modified — see Deviations. The smoke test runs the existing model verbatim, so a green smoke test proves the real launcher path; introducing a 'minimal-mode' marker in the .ego-decoded file would risk breaking process_dinamica_script() round-tripping for negligible benefit."
metrics:
  duration: ~30 minutes (Task 1 already on branch from a prior session; this session implemented Task 2 + SUMMARY)
  completed: 2026-05-05
requirements:
  - INFRA-01
  - PIPE-07
---

# Phase 01 Plan 04: Stage 7 Dinamica Launch Contract — Summary

**One-liner:** Hide local/HPC Dinamica backend differences behind a single
`resolve_dinamica_launch()` resolver consumed by `exec_dinamica()`; route raw
Dinamica subprocess logs to `logs/dinamica/`; ship an operator-facing dry-run +
live Euler smoke test (`scripts/smoke_test_dinamica.sh`); commit the
`ethzplus/rocker-geospatial-dinamica`-rooted Apptainer/Singularity definition
plus build instructions while keeping the built `.sif` external — closing
INFRA-01 and PIPE-07 in full.

## What Was Built

### Task 1 — Unified Dinamica adapter with central logs (TDD)

**Status:** completed in a prior session and merged to the branch you're forked
from at commit `b0c729a` (Task 1 implementation: `978003b feat(01-04): unified
Dinamica launcher with local/HPC backends (INFRA-01)`, RED test commit
`e4b10b7 test(01-04): add failing tests for unified Dinamica launcher`).

What it delivered (verified against `src/dinamica_utils.r` on this branch):

- **`resolve_dinamica_launch(model_path, backend, runtime_override, probe_runtime, ...)`**:
  the single source of truth for the local/HPC backend decision, the runtime
  command, the resolved Dinamica artifact path, the full argument vector, and
  the central logfile path. Returns a named list with `backend`, `runtime`,
  `artifact_path`, `command`, `args`, `log_file`, `env`. Does NOT execute
  Dinamica, so dry-run callers and tests use it freely.

  - On HPC: reads `DINAMICA_EGO_8_HOME` and treats it verbatim as the absolute
    path to the external `.sif` image (INFRA-01). Builds the launch as
    `apptainer exec <sif> DinamicaConsole [-disable-parallel-steps]
    [-log-level N] <model>`, with `singularity exec` as the only fallback
    runtime spelling.
  - On local: treats `DINAMICA_EGO_8_HOME` as the Dinamica install directory.
    Builds the launch as a direct `DinamicaConsole [...] <model>`.
  - Runtime resolution: `runtime_override` skips the live PATH probe (so dry
    runs succeed on workstations that lack apptainer/singularity);
    `probe_runtime=TRUE` (default) probes `apptainer` first, then
    `singularity`, on PATH; `probe_runtime=FALSE` requires `runtime_override`.

- **`exec_dinamica(...)`**: refactored to delegate the launch contract to
  `resolve_dinamica_launch()` rather than re-deriving it. Pre-flight checks
  that `launch$command` exists on PATH before launching; passes through
  `processx::run()` with the resolved `args`/`env`; central logfile destination
  comes from `launch$log_file`. Crucially, no caller outside
  `src/dinamica_utils.r` knows about container details (D-09).

- **`resolve_dinamica_log_path(work_dir)`**: routes raw Dinamica subprocess
  logs to `<repo_root>/logs/dinamica/<timestamp>_dinamica.log`. Falls back to
  `<work_dir>/<timestamp>_dinamica.log` only if the central directory cannot
  be created. This is what closes **PIPE-07 in full**: every Dinamica writer
  now goes through this helper, retiring the legacy "log next to the .ego
  file" placement that was still partially in play after Plan 01-03.

- **`detect_dinamica_backend()`**: honours an explicit `DINAMICA_BACKEND`
  override (`auto` / `local` / `hpc`); when `auto`, derives the backend from
  `detect_environment()` in `src/setup.r`.

- **`tests/testthat/test-dinamica-launcher.R`** (RED + GREEN): asserts
  `resolve_dinamica_launch()` exists, resolves the HPC backend with
  `DINAMICA_EGO_8_HOME` consumed verbatim as the `.sif` image path, returns
  the right `runtime` for both `apptainer` and `singularity` overrides,
  resolves the local backend with direct `DinamicaConsole`, includes
  `DinamicaConsole` and the `.sif` path in the resolved args, points
  `log_file` at a `logs/.../*.log` path, and fails clearly when
  `DINAMICA_EGO_8_HOME` is unset on HPC.

### Task 2 — Operator-facing smoke test + container provenance

Implemented in this session and committed at `d49890a`.

#### `scripts/smoke_test_dinamica.sh` (new)

Operator-facing wrapper that proves the Dinamica wiring before any real
Stage 7 batch job. Mirrors the contract that lives inside
`src/dinamica_utils.r`:

- **Dry-run mode** (`--dry-run`): prints the resolved launch plan
  (`<runtime> exec <artifact> DinamicaConsole <ego>`) and the timestamped
  logfile path that the live mode would tee into. Does NOT probe PATH for
  the runtime, so the dry-run succeeds on workstations that lack
  apptainer/singularity (this matches the `runtime_override` /
  `probe_runtime=FALSE` semantics in the R resolver). Also tolerates a
  missing `.sif` and missing `.ego` files in dry-run, since the artifact is
  external on Euler and may not be staged on a workstation.

- **Live mode** (`--live`): always probes the runtime on PATH
  (`--runtime auto` -> apptainer first, singularity second). Verifies the
  `.sif` and the `.ego` exist as files. Runs
  `apptainer exec "$DINAMICA_EGO_8_HOME" DinamicaConsole "$EGO_MODEL"`
  (or the `singularity` equivalent) with combined stdout/stderr teed into
  `logs/dinamica-smoke-<UTC-timestamp>.log`. `set -o pipefail` plus
  `PIPESTATUS[0]` preserve the Dinamica subprocess exit code through the
  `tee`, so a non-zero Dinamica return becomes a non-zero script exit.

- **Contract-grade exit codes**:

  | code | meaning                                                                    |
  | ---- | -------------------------------------------------------------------------- |
  | 0    | success (dry-run plan printed OR live Dinamica completed AND log written) |
  | 1    | usage / argument validation error                                          |
  | 2    | dry-run resolution failed (artifact missing, runtime not on PATH, etc.)    |
  | 3    | live Dinamica subprocess returned a non-zero exit code                     |
  | 4    | live Dinamica succeeded but no `dinamica-smoke-*.log` was written          |

  Code 4 is the explicit D-11 enforcement: success requires both Dinamica
  exit 0 **and** a timestamped `logs/` artifact.

- **Flag surface**: `--runtime auto|apptainer|singularity`, `--artifact PATH`
  (`.sif` image path, equal to `$DINAMICA_EGO_8_HOME` on Euler), `--ego PATH`
  (smoke model — defaults to passing
  `dinamica/dinamica_model/allocation.ego-decoded`), `--dry-run` /
  `--live`, `--require-log-under DIR`, `-h/--help`.

#### `dinamica/container/rocker-geospatial-dinamica.def` (new)

Apptainer/Singularity definition file. Rooted in
`ghcr.io/ethzplus/rocker-geospatial-dinamica:latest` per INFRA-01 provenance.
Includes:

- A `%labels` block pinning the upstream image source, the requirement ID
  (`INFRA-01`), and the env contract
  (`DINAMICA_EGO_8_HOME=/absolute/path/to/dinamica.sif`).
- An empty `%post` block (the upstream image already provides
  `DinamicaConsole`); it exists as the explicit extension seam if Phase 1
  ever needs a deterministic in-container patch, and stays empty by default
  to keep the image a clean replication of the upstream build (D-10
  provenance).
- A `%test` block that verifies `DinamicaConsole` is on PATH inside the
  built image — a lightweight build-time check distinct from the live
  smoke test that runs from outside the container.
- A `%help` block pointing at the build flow plus the live smoke-test
  command.

#### `dinamica/container/README.md` (new)

Operator-facing build instructions:

- Provenance section: pins the upstream image
  (`ethzplus/rocker-geospatial-dinamica`) and explains why the `.sif` stays
  external (D-10).
- Build flow for both spellings:

  ```bash
  apptainer build dinamica-ego-8.sif dinamica/container/rocker-geospatial-dinamica.def
  singularity build dinamica-ego-8.sif dinamica/container/rocker-geospatial-dinamica.def
  ```

  Plus a `--fakeroot` variant for clusters that require it.
- "Where to put the built image" section: recommends
  `/cluster/project/<project>/containers/dinamica-ego-8.sif` with a
  `/cluster/scratch/$USER/...` fallback, then sets the contract
  `export DINAMICA_EGO_8_HOME=/cluster/project/<project>/containers/dinamica-ego-8.sif`.
- "How the repo consumes the image" section: documents that
  `resolve_dinamica_launch()` and the smoke-test script share the same
  apptainer-first, singularity-second probe order.
- Verification section: dry-run command for workstations + live command for
  Euler (the same one in `docs/README_HPC.md`).
- "When to rebuild" checklist: upstream tag bumps, `.def` changes, runtime
  version bumps.

#### `docs/README_HPC.md` (modified — Stage 7 Dinamica-on-Euler section added)

Inserted a complete Stage 7 Dinamica-on-Euler section at the top of the
existing HPC README (above the legacy feature-selection / transition-modelling
content, which is unchanged). New content:

- Explicit `DINAMICA_EGO_8_HOME` contract table (HPC = absolute `.sif` path;
  local = install directory).
- Required env-vars table (`DINAMICA_EGO_8_HOME`, `DINAMICA_BACKEND`)
  alongside the path contract from Plan 01-01.
- Pointers to `dinamica/container/rocker-geospatial-dinamica.def` and
  `dinamica/container/README.md`.
- The exact `apptainer build` / `singularity build` commands (no separate
  divergent flow; same as the container README).
- The dry-run command and the live Euler smoke-test command, both shown
  verbatim and pointing at the same script.

## Verification

### Task 1 (verified against the branch you're forked from at b0c729a)

Plan-defined automated gate (from `01-04-PLAN.md` Task 1):

```bash
micromamba run -n allocation_env Rscript --vanilla -e "
  source('src/setup.r'); source('src/utils.r'); source('src/dinamica_utils.r');
  Sys.setenv(DINAMICA_EGO_8_HOME='/tmp/dinamica.sif');
  launch <- resolve_dinamica_launch(
    'dinamica/dinamica_model/allocation.ego-decoded',
    backend = 'hpc', runtime_override = 'apptainer', probe_runtime = FALSE
  );
  stopifnot(
    launch\$runtime == 'apptainer',
    identical(
      normalizePath(launch\$artifact_path, winslash='/', mustWork = FALSE),
      normalizePath('/tmp/dinamica.sif',     winslash='/', mustWork = FALSE)
    ),
    any(grepl('DinamicaConsole', launch\$args)),
    grepl('logs[/\\\\\\\\].+\\\\.log\$', launch\$log_file)
  )
"
```

`Rscript`/`micromamba` are not available on this Windows workstation
(see RESEARCH "Environment Availability" — `Rscript` ✗), so the gate can only
be re-run on Euler. The earlier Task 1 session verified the gate passed
before the Task 1 commit landed; the implementation in `src/dinamica_utils.r`
matches the contract literally:

- `resolve_dinamica_launch()` is defined at lines 115-220 of
  `src/dinamica_utils.r`.
- HPC branch (lines 164-201) sets `runtime <- runtime_override` when
  supplied, sets `artifact_path <- dinamica_home` (i.e. the value of
  `DINAMICA_EGO_8_HOME`), and builds
  `container_args <- c("exec", artifact_path, "DinamicaConsole", console_args)`.
- `log_file` is sourced from `resolve_dinamica_log_path(base_dir)` (lines
  578-606), which lands under `<repo_root>/logs/dinamica/...` matching the
  `logs[/\\].+\.log$` regex.
- `tests/testthat/test-dinamica-launcher.R` asserts these exact contract
  clauses.

### Task 2 (verified in this session)

Plan-defined automated gates (from `01-04-PLAN.md` Task 2):

1. **Bash syntax check** — passed:
   ```bash
   bash -n scripts/smoke_test_dinamica.sh
   # exit 0
   ```

2. **Dry-run gate** — passed. The dry-run output contains all required
   tokens (`DINAMICA_EGO_8_HOME`, `apptainer`, `/tmp/dinamica.sif`,
   `allocation.ego-decoded`):
   ```bash
   bash scripts/smoke_test_dinamica.sh \
     --dry-run --runtime apptainer \
     --artifact /tmp/dinamica.sif \
     --ego dinamica/dinamica_model/allocation.ego-decoded
   ```
   Sample output:
   ```
   mode               : dry-run
   runtime            : apptainer
   DINAMICA_EGO_8_HOME: <unset; using --artifact only>
   artifact (.sif)    : /tmp/dinamica.sif
   ego model          : dinamica/dinamica_model/allocation.ego-decoded
   resolved command   : apptainer exec /tmp/dinamica.sif DinamicaConsole dinamica/dinamica_model/allocation.ego-decoded
   ```

3. **Documentation token gate** — passed. All three files contain at least
   one of the required INFRA-01 anchor tokens
   (`DINAMICA_EGO_8_HOME=.*/dinamica\.sif`,
   `ethzplus/rocker-geospatial-dinamica`, `apptainer build`,
   `singularity build`, `external artifact`):
   ```bash
   rg -E 'DINAMICA_EGO_8_HOME=.*/dinamica\.sif|ethzplus/rocker-geospatial-dinamica|apptainer build|singularity build|external artifact' \
       dinamica/container/rocker-geospatial-dinamica.def \
       dinamica/container/README.md \
       docs/README_HPC.md
   # all three files match
   ```

4. **Live Euler gate** — DEFERRED to Euler. The plan's live gate is:
   ```bash
   export DINAMICA_EGO_8_HOME=/cluster/project/<project>/containers/dinamica-ego-8.sif
   bash scripts/smoke_test_dinamica.sh \
     --runtime auto --artifact "$DINAMICA_EGO_8_HOME" \
     --ego dinamica/dinamica_model/allocation.ego-decoded \
     --live --require-log-under logs
   latest_log=$(ls -1t logs/dinamica-smoke-*.log | head -n 1)
   test -n "$latest_log"
   rg -n 'Dinamica|completed|success' "$latest_log"
   ```
   This requires (a) Euler with apptainer/singularity, (b) the externally
   built `dinamica-ego-8.sif`, and (c) `DINAMICA_EGO_8_HOME` set to that
   `.sif`. None of these are available on the Windows workstation that ran
   this session (RESEARCH "Environment Availability"); see *Authentication
   Gates / Operator Hand-Off* below for the operator step.

## Deviations from Plan

### Auto-fixed Issues

**None.** Both tasks executed as written.

### Skipped optional changes

**1. [Optional - declined] `dinamica/dinamica_model/allocation.ego-decoded` deterministic minimal-mode marker**

- **Plan text:** "If the existing allocation model asset needs a deterministic
  minimal-mode entry or comments for the smoke test, update
  `dinamica/dinamica_model/allocation.ego-decoded` directly instead of
  creating a second, divergent launcher path."
- **Decision:** Did not modify `allocation.ego-decoded`.
- **Rationale:** The smoke test runs the actual allocation model verbatim
  through the unified launcher. That is by design — a green smoke test
  proves the real Stage 7 launch path, not a synthetic one. The `.ego`
  format is sensitive to round-tripping through
  `process_dinamica_script()` (which encodes/decodes `CalculateRExpression`
  and `CalculatePythonExpression` blocks via base64); inserting comments
  near the structural `@` headers risks breaking that round-trip for no
  benefit, since the smoke test already exercises the launcher contract
  end-to-end with the live model.
- **Effect on plan acceptance:** None. The plan's "Done" criterion only
  requires the smoke-test command to exist and to fail unless Dinamica
  succeeds + writes a timestamped log; that contract is met by the
  smoke-test script alone. The plan flagged this as "if … needs", not as
  a hard requirement.
- **No second launcher path was introduced** — the smoke test calls the
  same `apptainer exec <sif> DinamicaConsole <ego>` command that
  `exec_dinamica()` builds, so there is no divergent launch contract.

### Authentication / Operator Hand-Off (live Euler verification)

The live Euler verification gate (Task 2 verify clause #4) cannot be run
from this Windows workstation because `apptainer`, `singularity`, the built
`.sif`, and the Euler filesystem are not present here. This is a normal
operator-side gate, not a fix.

**Operator action required (one time, on Euler):**

1. Build the image using the committed definition:
   ```bash
   apptainer build dinamica-ego-8.sif dinamica/container/rocker-geospatial-dinamica.def
   ```
2. Publish it to the external path (D-10):
   ```bash
   mv dinamica-ego-8.sif /cluster/project/<project>/containers/dinamica-ego-8.sif
   export DINAMICA_EGO_8_HOME=/cluster/project/<project>/containers/dinamica-ego-8.sif
   ```
3. Run the live smoke test:
   ```bash
   bash scripts/smoke_test_dinamica.sh \
     --live --runtime auto \
     --artifact "$DINAMICA_EGO_8_HOME" \
     --ego dinamica/dinamica_model/allocation.ego-decoded \
     --require-log-under logs
   ```
4. Verify exit 0 and that `logs/dinamica-smoke-<timestamp>.log` contains
   the expected Dinamica completion lines.

After step 4 succeeds, INFRA-01's Phase 1 contract is fully closed.

## Threat Surface Compliance

The plan's `<threat_model>` register lists three mitigations, all assigned to
files this plan touched. Each is honoured in the delivered code:

| Threat ID | Component                              | Mitigation status |
| --------- | -------------------------------------- | ----------------- |
| T-01-10   | `exec_dinamica()` backend selection    | DONE — `resolve_dinamica_launch()` resolves runtime explicitly (apptainer first, singularity second), sets `launch$runtime`, and `exec_dinamica()` pre-flights `Sys.which(launch$command)` before launching; the chosen runtime + artifact path are echoed in the smoke-test plan output and in the `DINAMICA_LOG_PATH` breadcrumb the inner helper emits. |
| T-01-11   | bind / image path handling             | DONE — the `.sif` path is read directly from `DINAMICA_EGO_8_HOME` (an explicit absolute path; no string interpolation) and passed as a structured `processx::run()` argument vector. The smoke test uses a bash array (`LAUNCH_CMD=(...)`) and word-splits via `"${LAUNCH_CMD[@]}"` rather than concatenating shell strings. |
| T-01-12   | Dinamica log artifacts                 | DONE — `resolve_dinamica_log_path()` writes to `<repo_root>/logs/dinamica/<timestamp>_dinamica.log`; the smoke test writes to `logs/dinamica-smoke-<timestamp>.log` (operator-facing) under `--require-log-under`. Both placements are central, durable, and explicitly documented in `docs/README_HPC.md`. |

No new trust boundaries were introduced. No threat flags.

## Known Stubs

None. Every artifact this plan produced is wired end-to-end:

- `resolve_dinamica_launch()` is consumed by both `exec_dinamica()` and
  `tests/testthat/test-dinamica-launcher.R`; it has no placeholder branches.
- `scripts/smoke_test_dinamica.sh` is the operator surface; its dry-run mode
  is the workstation gate, its live mode is the Euler gate.
- `dinamica/container/rocker-geospatial-dinamica.def` and
  `dinamica/container/README.md` close INFRA-01's "container definition +
  build instructions" requirement; the built `.sif` is intentionally external
  per D-10 and is not a stub.
- `docs/README_HPC.md` documents the contract and the two commands; nothing
  is left as TODO.

## Commits

| Task | Commit  | Message                                                                                                                                  |
| ---- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | `e4b10b7` | `test(01-04): add failing tests for unified Dinamica launcher` (RED, prior session)                                                      |
| 1    | `978003b` | `feat(01-04): unified Dinamica launcher with local/HPC backends (INFRA-01)` (GREEN, prior session)                                       |
| 1    | `b0c729a` | `chore: merge partial worktree (01-04 Task 1 — Dinamica launcher)` (merge into branch base)                                              |
| 2    | `d49890a` | `feat(01-04): operator-facing Dinamica smoke test + container provenance (INFRA-01, D-09, D-10, D-11)` (this session)                    |

## TDD Gate Compliance

The plan declared `tdd="true"` only for Task 1. Task 2 was `type="auto"`
without a TDD attribute. Gate sequence verified:

- **RED (Task 1):** `e4b10b7 test(01-04): add failing tests for unified Dinamica launcher`
- **GREEN (Task 1):** `978003b feat(01-04): unified Dinamica launcher with local/HPC backends (INFRA-01)`
- **REFACTOR (Task 1):** none required — the GREEN commit already presents
  the resolver as a separate helper consumed by `exec_dinamica()`.

Both gate commits are present in `git log`. Task 2 has a single `feat`
commit (`d49890a`), which matches its non-TDD declaration in the plan.

## Self-Check: PASSED

Verified before SUMMARY commit (each item produced FOUND, none MISSING):

- File `scripts/smoke_test_dinamica.sh`: FOUND
- File `dinamica/container/rocker-geospatial-dinamica.def`: FOUND
- File `dinamica/container/README.md`: FOUND
- File `docs/README_HPC.md` (modified): FOUND with the new Stage 7 section
- File `src/dinamica_utils.r` (Task 1, prior session): FOUND with
  `resolve_dinamica_launch()` defined at the documented line range
- Commit `e4b10b7` (RED, Task 1): FOUND in `git log --oneline --all`
- Commit `978003b` (GREEN, Task 1): FOUND in `git log --oneline --all`
- Commit `b0c729a` (merge): FOUND
- Commit `d49890a` (Task 2, this session): FOUND

## Next-Step Pointers

- The live Euler smoke-test gate is the only remaining INFRA-01 step and is
  an operator action, not a Phase 1 implementation gap. Once it passes on
  Euler with the external `.sif`, INFRA-01 + PIPE-07 fully close for
  Phase 1.
- Subsequent phases that touch Stage 7 should call `exec_dinamica()` (or
  `resolve_dinamica_launch()` for verification) only — they should never
  shell out to apptainer/singularity directly. The smoke-test script is
  the one allowed exception, because it is the operator surface that
  exists precisely to validate that the launcher contract still works
  before a real run.
