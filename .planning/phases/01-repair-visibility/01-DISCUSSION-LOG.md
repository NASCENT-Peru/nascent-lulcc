# Phase 1: Repair & Visibility - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md - this log preserves the alternatives considered.

**Date:** 2026-05-05
**Phase:** 1-Repair & Visibility
**Areas discussed:** Pre-flight failure contract, Log surface and crash breadcrumbing, Dinamica-on-Euler operating model, Path repair strategy

---

## Pre-flight failure contract

| Option | Description | Selected |
|--------|-------------|----------|
| Minimal runtime gate | Only check what the current run immediately needs. | |
| Full operator gate | Check env vars, required packages, model files, Dinamica availability, and key runtime paths before work starts. | x |
| Staged gate | Run a full check but structure failures into buckets. | |

**User's choice:** Full operator gate
**Notes:** User wanted a complete upfront operator-facing validation pass.

| Option | Description | Selected |
|--------|-------------|----------|
| Allocation-only artifacts | Validate just Stage 7 runtime artifacts. | x |
| Full upstream readiness | Validate prerequisite outputs from earlier stages too. | |
| Hybrid | Hard-fail on allocation-critical artifacts and warn on upstream gaps. | |

**User's choice:** Allocation-only artifacts
**Notes:** Keep pre-flight scoped to the failing allocation stage.

| Option | Description | Selected |
|--------|-------------|----------|
| Combined actionable list | One failure summary with every missing item and a suggested fix. | x |
| Grouped summary | One summary grouped by category. | |
| Fail fast | Stop on the first broken category. | |

**User's choice:** Combined actionable list
**Notes:** Matches the Phase 1 success criterion for a single actionable list.

| Option | Description | Selected |
|--------|-------------|----------|
| Upfront gate in `run_allocation()` | Keep checks inline at the top-level entrypoint. | |
| Split global plus local checks | Divide validation between global and per-target checks. | |
| Standalone helper | Centralize checks in a helper called by `run_allocation()`. | x |

**User's choice:** Standalone helper
**Notes:** Chosen to keep validation logic centralized and extensible.

---

## Log surface and crash breadcrumbing

| Option | Description | Selected |
|--------|-------------|----------|
| Top-level only | Keep detailed logging only in the main loop. | |
| Key inner helpers | Pass `log_file` into high-value helpers like setup and Dinamica execution. | x |
| Full logging contract | Standardize logging across nearly all helpers. | |

**User's choice:** Key inner helpers
**Notes:** Focus inner visibility where failures are most likely to disappear today.

| Option | Description | Selected |
|--------|-------------|----------|
| Single sentinel | Emit one unexpected-termination marker near the worker boundary. | |
| Context-rich sentinel | Include last-known stage, scenario, region, timestep, and transition. | x |
| Heartbeats | Add periodic breadcrumbs throughout execution. | |

**User's choice:** Context-rich sentinel
**Notes:** User preferred richer post-mortem context over minimal termination markers.

| Option | Description | Selected |
|--------|-------------|----------|
| Separate Dinamica log | Keep raw subprocess output in its own log under `logs/`. | |
| Region log only | Stream Dinamica output only into the per-region log. | |
| Both | Keep the raw log and mirror key lifecycle lines into the region log. | x |

**User's choice:** Both
**Notes:** Preserve raw Dinamica output while making operator triage fast.

| Option | Description | Selected |
|--------|-------------|----------|
| Mostly freeform | Add only a few extra messages. | |
| Critical lifecycle structured | Standardize start/finish/failure events. | x |
| All new logs structured | Make all new Phase 1 logs key-value lines. | |

**User's choice:** Critical lifecycle structured
**Notes:** Good balance between operator tooling and implementation effort.

---

## Dinamica-on-Euler operating model

| Option | Description | Selected |
|--------|-------------|----------|
| Wrapper-first | Always go through a repo-owned Singularity wrapper. | |
| Environment-first | Make `DinamicaConsole` available on `PATH` and keep R unchanged. | |
| Hybrid | Use wrapper/container behavior on HPC and direct execution locally. | x |

**User's choice:** Hybrid
**Notes:** Preserve local ergonomics while making Euler reliable.

| Option | Description | Selected |
|--------|-------------|----------|
| Repo-owned definition | Keep the build contract in-repo and store the `.sif` outside the repo. | |
| Definition plus helper | Add repo automation around image creation or validation. | |
| External image only | Treat the image as external and document how to point to it. | x |

**User's choice:** External image only
**Notes:** The repo should not become the owner of the heavy runtime artifact.

| Option | Description | Selected |
|--------|-------------|----------|
| Presence check | Verify the entrypoint exists and is executable. | |
| Smoke-test contract | Run a lightweight validation command to prove wiring works. | x |
| Manual verification | Document the test but do not automate it. | |

**User's choice:** Smoke-test contract
**Notes:** User wanted a stronger runtime proof before real allocation runs.

| Option | Description | Selected |
|--------|-------------|----------|
| Same interface, different backend | One code entrypoint chooses the backend by environment. | x |
| HPC-only support | Focus only on Euler in Phase 1. | |
| Explicit mode switch | Require manual mode declaration. | |

**User's choice:** Same interface, different backend
**Notes:** Keeps operators and downstream code on one stable interface.

---

## Path repair strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Targeted patching | Fix only the known offenders in-place. | |
| Centralized path contract | Route path resolution through shared helpers/config lookups. | x |
| Hybrid | Centralize high-risk paths and patch some lower-risk ones locally. | |

**User's choice:** Centralized path contract
**Notes:** User preferred a durable path model over scattered hotfixes.

| Option | Description | Selected |
|--------|-------------|----------|
| Config-first | YAML config is authoritative, with a few env overrides. | x |
| Env-first | Favor shell/env configuration over YAML. | |
| Script-local detection | Let each script detect and assemble paths itself. | |

**User's choice:** Config-first
**Notes:** Aligns with the existing setup system instead of adding parallel path logic.

| Option | Description | Selected |
|--------|-------------|----------|
| Strict Phase 1 scope | Remove hardcoded user paths only from directly affected active scripts. | |
| Broader sweep | Remove them from active code, touched docs, and touched operational helpers. | x |
| Full repo audit | Eliminate them everywhere except history. | |

**User's choice:** Broader sweep
**Notes:** Clean up the practical operator surface without expanding to a full archival audit.

| Option | Description | Selected |
|--------|-------------|----------|
| Explicit env vars | Require HPC temp/beegfs env vars and fail clearly if missing. | x |
| Defaults plus overrides | Derive standard scratch/temp locations automatically. | |
| Config-only | Force all temp/beegfs paths through YAML. | |

**User's choice:** Explicit env vars
**Notes:** User preferred explicit HPC runtime requirements over inferred defaults.

---

## the agent's Discretion

None.

## Deferred Ideas

None.
