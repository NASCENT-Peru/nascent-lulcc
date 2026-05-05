# Dinamica EGO 8 Container — Build Instructions

**Phase 1 Plan 04 Task 2 — INFRA-01, D-09, D-10**

This directory holds the committed Apptainer/Singularity definition file for the
external Dinamica EGO 8 image used by Stage 7 on Euler. The repository **does
not** ship the built `.sif` artifact (D-10): the image stays external and is
consumed by `exec_dinamica()` and `scripts/smoke_test_dinamica.sh` through the
`DINAMICA_EGO_8_HOME` environment variable.

## Provenance

The definition (`rocker-geospatial-dinamica.def`) is rooted in the upstream
image
[`ethzplus/rocker-geospatial-dinamica`](https://github.com/ethzplus/rocker-geospatial-dinamica),
which packages Dinamica EGO 8 on top of the
[`rocker-geospatial`](https://rocker-project.org/) R toolchain. The bootstrap
points at the canonical GHCR tag so the `.sif` is byte-reproducible from the
upstream image without vendoring binaries inside this repository.

## Files

- `rocker-geospatial-dinamica.def` — committed Apptainer/Singularity definition
  file. Single source of truth for the container that runs Dinamica on Euler.
- `README.md` — this file.

The built `dinamica-ego-8.sif` is **NOT** committed and **MUST NOT** be added
to the repository. `.gitignore` already excludes large binary artifacts; the
`.sif` is treated as an external Euler-side artifact per D-10.

## Build flow

The build runs once per upstream image bump. Both spellings are supported and
produce the same artifact.

### Apptainer (preferred on Euler)

```bash
apptainer build dinamica-ego-8.sif \
    dinamica/container/rocker-geospatial-dinamica.def
```

### Singularity (fallback spelling)

```bash
singularity build dinamica-ego-8.sif \
    dinamica/container/rocker-geospatial-dinamica.def
```

If `--fakeroot` is required by the cluster build node, pass it through:

```bash
apptainer build --fakeroot dinamica-ego-8.sif \
    dinamica/container/rocker-geospatial-dinamica.def
```

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

`src/dinamica_utils.r:resolve_dinamica_launch()` reads `DINAMICA_EGO_8_HOME` as
the absolute `.sif` path on the HPC backend and constructs the launch command
verbatim:

```bash
apptainer  exec "$DINAMICA_EGO_8_HOME" DinamicaConsole "$EGO_MODEL"
singularity exec "$DINAMICA_EGO_8_HOME" DinamicaConsole "$EGO_MODEL"
```

The runtime is probed in the order `apptainer` first, `singularity` second.
The R adapter and `scripts/smoke_test_dinamica.sh` share that probe order so
the contract is identical from R and from the operator-facing script.

## Verifying a built image

Run the live smoke test once after each rebuild to prove the image and the
launch contract still line up:

```bash
# On Euler, after `module load apptainer` (or singularity) and `source .env`:
scripts/smoke_test_dinamica.sh \
    --live \
    --runtime auto \
    --artifact "$DINAMICA_EGO_8_HOME" \
    --ego dinamica/dinamica_model/allocation.ego-decoded \
    --require-log-under logs
```

The script exits 0 only if Dinamica completes successfully **and** writes a
timestamped `dinamica-smoke-*.log` file under `logs/`. On any other outcome it
exits non-zero and prints which contract clause was violated.

A workstation dry-run (no apptainer/singularity required, no real `.sif`
needed) is also available and is what `tests/`-style verification gates run:

```bash
scripts/smoke_test_dinamica.sh \
    --dry-run \
    --runtime apptainer \
    --artifact /tmp/dinamica.sif \
    --ego dinamica/dinamica_model/allocation.ego-decoded
```

## When to rebuild

Rebuild and re-publish the `.sif` when:

1. The upstream `ethzplus/rocker-geospatial-dinamica` tag we bootstrap from
   moves (Dinamica version bump, R/geospatial stack bump, security patches).
2. The `%post` or `%test` blocks in `rocker-geospatial-dinamica.def` change.
3. The Euler runtime versions (`apptainer`, `singularity`) bump in a way that
   invalidates the image format.

In all three cases, run the live smoke test before allowing a Stage 7 batch
job to consume the new image.
