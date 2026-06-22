# Phase 1: Repair & Visibility - Context

**Gathered:** 2026-05-05
**Status:** Ready for planning

<domain>
## Phase Boundary

This phase hardens the Stage 7 allocation runtime so operators can diagnose failures quickly, prerequisite problems fail before any real work starts, HPC checkout works without manual path edits, and Dinamica EGO 8 is runnable on Euler with a clear operating contract.

</domain>

<decisions>
## Implementation Decisions

### Pre-flight Failure Contract
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

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase Scope And Requirements
- `.planning/ROADMAP.md` - Defines the Phase 1 goal, requirements, and success criteria.
- `.planning/PROJECT.md` - Captures project-level constraints, active decisions, and current failure profile.
- `.planning/REQUIREMENTS.md` - Maps the concrete observability, infrastructure, and path-fix requirements for Phase 1.
- `.planning/STATE.md` - Records current project position and carry-forward decisions.

### Codebase Maps
- `.planning/codebase/STACK.md` - Defines the runtime stack, environment layout, and Dinamica dependency shape.
- `.planning/codebase/ARCHITECTURE.md` - Explains pipeline stages, Stage 7 responsibilities, and current integration boundaries.
- `.planning/codebase/INTEGRATIONS.md` - Documents HPC, Dinamica, logging, and external runtime expectations.

### Allocation And Logging Runtime
- `src/allocation.r` - Main Stage 7 orchestration, profiling hooks, region log flow, and integration points for pre-flight and breadcrumbing.
- `src/dinamica_utils.r` - Dinamica execution path, current log placement behavior, and the single runtime entrypoint to preserve.

### Path And Environment Hotspots
- `src/calibration_predictor_prep.r` - Contains the hardcoded `E:/terra_temp` path that must move behind the shared path contract.
- `src/simulation_trans_rates_prep.r` - Contains the hardcoded Windows demand workbook path that must be repaired for HPC checkout.
- `scripts/hpc_common.sh` - Central HPC helper with user-specific scratch/temp assumptions and env bootstrap behavior.
- `scripts/setup_environments.sh` - Environment setup flow with hardcoded HPC user-path assumptions.
- `.env.template` - User-facing environment contract that must align with the new centralized path rules.
- `environments/allocation_env.yml` - Prediction-time package environment that Phase 1 must complete for pre-flight validation.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/allocation.r` profiling helpers - Existing `prof_tic()`, `prof_toc()`, and `prof_mem_summary()` already provide the backbone for structured visibility improvements.
- `src/utils.r` logging and path helpers - Existing helper patterns can host shared validation and path-resolution logic instead of duplicating checks in entry scripts.
- `scripts/hpc_common.sh` - Best place to consolidate shell-side HPC environment setup once user-specific paths are removed.

### Established Patterns
- Config-driven path resolution already exists through `src/setup.r` and environment-aware YAML selection, so Phase 1 should extend that pattern rather than add new script-local detection.
- Stage entrypoints are file-oriented and fail best when they validate artifacts upfront, which matches the chosen operator-gate approach.
- Dinamica execution is already funneled through `src/dinamica_utils.r`, so backend branching should stay behind that single interface.

### Integration Points
- `run_allocation()` is the correct top-level caller for the new pre-flight helper.
- `setup_allocation_inputs()` and `run_allocation_dinamica()` are the key logging expansion points for inner visibility.
- HPC wrapper/container behavior should plug into `exec_dinamica()` without changing the rest of the allocation flow.

</code_context>

<specifics>
## Specific Ideas

- The Phase 1 pre-flight should stay tightly focused on Stage 7 runtime readiness, not become a full pipeline audit.
- Operators should get one consolidated, human-readable prerequisite failure list with suggested fixes.
- Dinamica logging should remain a preserved raw artifact while still surfacing key events in the per-region log for quick triage.

</specifics>

<deferred>
## Deferred Ideas

None - discussion stayed within phase scope.

</deferred>

---

*Phase: 1-Repair & Visibility*
*Context gathered: 2026-05-05*
