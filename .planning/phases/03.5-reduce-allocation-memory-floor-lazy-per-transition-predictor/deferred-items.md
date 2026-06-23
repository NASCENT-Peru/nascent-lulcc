# Phase 3.5 — Deferred / Out-of-Scope Items

Discovered during execution. NOT fixed (outside the touched task's scope).

## Pre-existing test failures (unrelated to Plan 03.5-01)

Discovered while running `testthat::test_dir('tests/testthat')` for Plan 01 Task 2
regression verification. All six failures are in files this plan never touched and
reproduce on the plan's base commit (fb2ea69). They are NOT caused by the
threading change and are left untouched per the executor SCOPE BOUNDARY rule.

| Test | Line(s) | Symptom | Why out of scope |
|------|---------|---------|------------------|
| `test-prep-paths.R` | 27, 35, 43, 56 | `cannot open file '.../.claude/worktrees/src/simulation_trans_rates_prep.r'` — the test's `.repo_root` helper does `dirname()` three times assuming a fixed `tests/testthat/` depth, which mis-resolves under `test_dir` for some files. Reads `src/simulation_trans_rates_prep.r`. | Plan 01 touches only `src/allocation.r`, the resolver test, and the smoke script. `simulation_trans_rates_prep.r` is unrelated (PIPE-01/PIPE-02 territory). |
| `test-allocation-runtime-contract.R` | 31 | `pin_pos (8070) >= plan_pos (7721)` — expects the first `pin_native_threads_to_one` literal to precede the first `future::plan(` literal in `scripts/run_allocation.r`. | `scripts/run_allocation.r` is byte-for-byte unmodified by this plan (empty `git diff` vs base). The ordering condition predates Plan 01. |
| `test-dinamica-launcher.R` | 196 | Dinamica `conf_lines` config mismatch (extra `GdalToolsData` line). | Dinamica launcher config — unrelated to the allocation prediction path. |

**Note on the local test environment:** `mlr3` / `mlr3learners` are not installed in
the local Windows R 4.5.0 used for these runs, so the mlr3 *integration* tests in
`test-mlr3-predict-dispatch.R` SKIP locally (7 PASS / 2 SKIP). They run on HPC where
the allocation conda env provides mlr3. The grep-based contract assertions in that
file (which DO run locally) pass with the threading change in place.

**Recommendation:** address the `.repo_root` depth assumption and the
`run_allocation.r` pin/plan ordering in a separate test-hardening pass — they are
not blockers for Plan 03.5-01.
