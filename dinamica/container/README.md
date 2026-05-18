# Dinamica EGO 8 Container — Build Instructions

**Phase 1 Plan 04 Task 2 — INFRA-01, D-09, D-10**
**Phase 1.1 — D-101, D-102, D-103, D-104, D-105, D-114** (build flow + launch shape rewrite)

This directory holds the committed Apptainer/Singularity definition file for the
external Dinamica EGO 8 image used by Stage 7 on Euler. The repository **does
not** ship the built `.sif` artifact (D-10): the image stays external and is
consumed by `exec_dinamica()` and `scripts/smoke_test_dinamica.sh` through the
`DINAMICA_EGO_8_HOME` environment variable.

> **Runtime caveat (Phase 1.1 open issue).** The build flow and launch shape
> below are mechanically validated end-to-end (the `.sif` builds cleanly from
> this `.def` on Euler, and the D-104 launch command is honoured by both the
> R-side `resolve_dinamica_launch()` and the shell-side smoke test). However,
> the live `--live` smoke test does **not** yet exit 0 against the rebuilt
> image: `DinamicaConsole` crashes with `std::exception` on bare invocation
> inside the `.sif`, independent of the `.ego` fixture. The D-107 post-hoc
> error grep catches this (exit 5) — i.e. the *detection* contract works.
> The remaining gap is a runtime/library-compatibility issue between the
> Dinamica EGO 8 AppImage and the Ubuntu Noble base layer, and is tracked
> for resolution in Phase 1.1 gap-closure / phase 01.2. See
> `.planning/phases/01.1-fix-dinamica-launch-contract/01.1-03-SUMMARY.md`
> "Open Issue 1" for the full diagnostic.

## Provenance

The definition (`rocker-geospatial-dinamica.def`) is a **verbatim port** of the
upstream Dockerfile at
[`ethzplus/rocker-geospatial-dinamica`](https://github.com/ethzplus/rocker-geospatial-dinamica),
inlined directly in `%post` per **D-101**. The upstream project is a
Dockerfile-only repository (Dinamica's licence forbids redistribution of the
binary), so no published image exists to bootstrap from — the only sustainable
contract is to inline the upstream `%post` steps and bootstrap from a public
base.

**Pinned base image: `rocker/r-ver:4.4.3`** (D-102). The Phase 1.1 DD-1
discretion was resolved in favour of `rocker/r-ver` over `rocker/geospatial`
because the upstream Dockerfile already inlines the geospatial install steps
from `install_geospatial.sh`; using `rocker/geospatial:4.4.3` would duplicate
that work and roughly triple the base-layer pull size (~1.27 GB vs ~348 MB
compressed). The built `.sif` weighs in around 1018 MB after `%post`.

> **Base image choice note (Phase 1.1 iteration 3):** The image was originally
> pinned at `rocker/r-ver:4.5.3` (Ubuntu Noble 24.04), but three successive
> rebuilds all produced `std::exception` crashes in `DinamicaConsole` despite
> clean library loading (confirmed via LD_DEBUG=files). The rollback to
> `rocker/r-ver:4.4.3` (Ubuntu Jammy 22.04) targets a suspected C++ ABI
> runtime incompatibility between the Dinamica AppImage (compiled on Jammy)
> and Noble's libstdc++14. See
> `.planning/phases/01.1-fix-dinamica-launch-contract/diagnostics/FINDINGS.md`
> H5 for the full diagnostic chain. Once confirmed working on Jammy, pin to
> `4.4.3` until Dinamica publishes Noble-compatible binaries.

## Files

- `rocker-geospatial-dinamica.def` — committed Apptainer/Singularity definition
  file. Single source of truth for the container that runs Dinamica on Euler.
- `README.md` — this file.

The built `dinamica-ego-8.sif` is **NOT** committed and **MUST NOT** be added
to the repository. `.gitignore` already excludes large binary artifacts; the
`.sif` is treated as an external Euler-side artifact per D-10.

## Build flow

The build runs once per upstream image bump or per `.def` change. The build is
self-contained: the `%post` block pulls `rocker/r-ver:4.5.3` from Docker Hub
(no GHCR or other private registry auth required), installs the geospatial R
stack, downloads the Dinamica EGO 8 AppImage from `dinamicaego.com`, extracts
it under `/opt/dinamica`, and seeds the minimal `.dinamica_ego_8.conf`. Both
runtime spellings are supported and produce the same artifact.

### Apptainer (preferred on Euler)

> **Quota warning:** The built `.sif` is ~1 GB. **Always build directly to
> `$DINAMICA_EGO_8_HOME`** (project or scratch filesystem) — never use a
> relative path like `dinamica-ego-8.sif` from inside `$REPO_ROOT`. A relative
> path resolves to `$REPO_ROOT` which is under `$HOME` on Euler, and Euler
> home quotas (~15–50 GB) will be exhausted during the `Creating SIF file…`
> step, causing a fatal `disk quota exceeded` error after an otherwise
> successful build.

Route the build temp/cache to scratch to avoid intermediate-layer quota
exhaustion, and build directly to `$DINAMICA_EGO_8_HOME`:

```bash
export APPTAINER_TMPDIR="$HPC_SCRATCH_ROOT/apptainer-tmp"
export APPTAINER_CACHEDIR="$HPC_SCRATCH_ROOT/apptainer-cache"
mkdir -p "$APPTAINER_TMPDIR" "$APPTAINER_CACHEDIR"

apptainer build "$DINAMICA_EGO_8_HOME" \
    dinamica/container/rocker-geospatial-dinamica.def
```

### Singularity (fallback spelling)

```bash
singularity build "$DINAMICA_EGO_8_HOME" \
    dinamica/container/rocker-geospatial-dinamica.def
```

If `--fakeroot` is required by the cluster build node, pass it through:

```bash
apptainer build --fakeroot "$DINAMICA_EGO_8_HOME" \
    dinamica/container/rocker-geospatial-dinamica.def
```

### When to rebuild (D-102 trigger conditions)

Rebuild and re-publish the `.sif` when any of these change:

1. **Dinamica EGO version bump** (e.g. upstream releases 8.8+). Update
   `DINAMICA_EGO_DOWNLOAD_URL` in `rocker-geospatial-dinamica.def` to the new
   AppImage URL number (currently `nui_download/1960/`), AND update the
   `@version` line in `dinamica/dinamica_model/smoketest.ego-decoded` to
   match the new Dinamica version string.
2. **Base-image bump** (e.g. `rocker/r-ver` moves to 4.5+ once Dinamica ships
   Noble-compatible binaries, or `rocker/r-ver:4.4.x` bumps to a newer patch).
   Update the `From:` tag in `rocker-geospatial-dinamica.def`. **Note:** do NOT
   bump from `rocker/r-ver:4.4.x` (Jammy) to `rocker/r-ver:4.5.x` (Noble) until
   the Dinamica AppImage is recompiled for Noble — see the base image note in the
   Provenance section above. Verify that the upstream `install_geospatial.sh`
   apt-package list still resolves cleanly on the new Ubuntu base.
3. **`%post` or `%test` body changes** in `rocker-geospatial-dinamica.def`.
4. **Euler runtime version bump** (`apptainer`/`singularity`) in a way that
   invalidates the image format.

In all cases, re-run the live smoke test before allowing a Stage 7 batch job
to consume the new image (see "Verifying a built image" below).

## Fallback: workstation `docker save` → `docker-archive://`

> **Use only if** the Euler build host has no outbound internet access OR
> the upstream Dinamica AppImage URL is unreachable from the cluster. The
> canonical path is `apptainer build … rocker-geospatial-dinamica.def`
> above; the workstation-transfer dance is a one-time workaround and is
> **not** part of the build contract.

If you cannot build on Euler directly, build the image on a workstation that
has Docker installed (e.g. via the upstream Dockerfile at
`ethzplus/rocker-geospatial-dinamica`), `docker save` it to a tarball, copy
the tarball to Euler, and convert it to `.sif` via
`apptainer build dinamica-ego-8.sif docker-archive://<tar>`. Track the
provenance of the resulting `.sif` carefully — the rebuild-trigger
conditions above no longer apply mechanically (no `.def` ↔ `.sif`
correspondence), so any workstation-transferred image must be manually
re-verified each time the upstream Dockerfile changes.

## Where to put the built image (external artifact, D-10)

The built `.sif` is an external artifact and must live outside the repository
clone. The recommended location on Euler is the project filesystem so it is
shared across users without re-building per operator:

```text
/cluster/project/<project>/containers/dinamica-ego-8.sif
```

If you do not yet have access to a project filesystem, a per-user staging path
under scratch is acceptable:

```text
/cluster/scratch/$USER/nascent-lulcc/containers/dinamica-ego-8.sif
```

After publishing the `.sif`, export `DINAMICA_EGO_8_HOME` to the absolute path
of that external artifact:

```bash
export DINAMICA_EGO_8_HOME=/cluster/project/<project>/containers/dinamica-ego-8.sif
```

This contract is also documented in `.env.template` and in
`docs/README_HPC.md`.

## How the repo consumes the image

`src/dinamica_utils.r:resolve_dinamica_launch()` reads `DINAMICA_EGO_8_HOME`
as the absolute `.sif` path on the HPC backend and constructs the **D-104
launch shape**:

```bash
apptainer exec \
    --home   "$HPC_SCRATCH_ROOT/dinamica-home" \
    --env    DINAMICA_EGO_8_TEMP_DIR="$HPC_SCRATCH_ROOT/dinamica-tmp" \
    "$DINAMICA_EGO_8_HOME" \
    bash -c 'cd /opt/dinamica/usr && bin/DinamicaEGO.sh /abs/path/to/model.ego'
```

The runtime is probed in the order `apptainer` first, `singularity` second.
The R adapter (`src/dinamica_utils.r:resolve_dinamica_launch()`) and
`scripts/smoke_test_dinamica.sh` share both the probe order **and** the
launch shape verbatim — drift is caught at test time by
`tests/testthat/test-dinamica-launch-contract-mirror.R`, which re-resolves
the launch in both languages and asserts matching substrings.

Why this shape (D-104):
- `bin/DinamicaEGO.sh` is the only entrypoint that sets the env vars
  (`PROJ_DATA`, `DINAMICA_EGO_8_INSTALLATION_DIRECTORY`,
  `DINAMICA_EGO_8_GDAL_DATA`, `DINAMICA_EGO_8_LOG_PATH`) and relative paths
  the binary needs. **DEPRECATED (pre-Phase 1.1):** the previous shape
  `apptainer exec <sif> DinamicaConsole <model>` produced silent
  `std::exception` failures on the upstream image and must not be used.
- `--home <staged-home>` and `--env DINAMICA_EGO_8_TEMP_DIR=<staged-tmp>`
  (D-105) keep Dinamica's mutable state out of `$HOME` (Euler home quota)
  and into `$HPC_SCRATCH_ROOT`. The staged dirs are seeded with the minimal
  `.dinamica_ego_8.conf` idempotently by `resolve_dinamica_launch()`.
- The model path passed to `bin/DinamicaEGO.sh` MUST be absolute (D-106) —
  the launcher's relative-path branch is fragile under `cd`.

## Verifying a built image

Run the live smoke test once after each rebuild to prove the image and the
launch contract still line up:

```bash
# On Euler, after `module load apptainer` (or singularity) and `source .env`:
scripts/smoke_test_dinamica.sh \
    --live \
    --runtime auto \
    --artifact "$DINAMICA_EGO_8_HOME" \
    --ego dinamica/dinamica_model/smoketest.ego \
    --require-log-under logs
```

The smoke fixture is `dinamica/dinamica_model/smoketest.ego` — a no-op
`.ego` (D-109 / DD-2) that exercises the launch contract only. The
production `allocation.ego-decoded` is unchanged and continues to be loaded
by `run_allocation_dinamica()` for real Stage 7 runs.

The script exits 0 only if Dinamica completes successfully **and** writes a
timestamped `dinamica-smoke-*.log` file under `logs/` **and** the log does
not contain any of the D-107 error patterns
(`Dinamica EGO exited with an error`, `terminate called after throwing`,
`std::exception`). On any other outcome it exits non-zero (1=usage,
2=resolution, 3=non-zero exit, 4=missing log, 5=D-107 error pattern matched
despite exit 0) and prints which contract clause was violated.

> Per the Phase 1.1 runtime caveat at the top of this file, the live smoke
> test currently exits **5** against the rebuilt `.sif` because
> `DinamicaConsole` crashes with `std::exception`. The D-107 detection
> contract is validated; the underlying AppImage/base-library compatibility
> fix is tracked in `01.1-03-SUMMARY.md` Open Issue 1.

A workstation dry-run (no apptainer/singularity required, no real `.sif`
needed) is also available and is what `tests/`-style verification gates run:

```bash
scripts/smoke_test_dinamica.sh \
    --dry-run \
    --runtime apptainer \
    --artifact /tmp/dinamica.sif \
    --ego dinamica/dinamica_model/smoketest.ego
```
