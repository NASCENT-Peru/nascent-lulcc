---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
verified: 2026-09-28T17:37:24Z
status: passed
score: 24/24 must-haves verified
overrides_applied: 0
re_verification:
  previous_status: gaps_found
  previous_score: 12/14
  gaps_closed:
    - "SC1 / D-01 / D-17: the spatial_interventions changes are on main (PR #2 merged, merge commit f54ccc2)"
    - "D-09 / D-11 (prior truth #14): sha256sum -c docs/spatial_interventions/masks.sha256 reports 14 OK on HPC"
  gaps_remaining: []
  regressions: []
gaps: []
deferred:
  - truth: "tests/testthat/test-prep-paths.R runs cleanly under test_dir()"
    addressed_in: "Out of Phase 5 scope (test-harness defect, pre-existing baseline)"
    evidence: "4 errors at lines 27/35/43/56 (Error in file(con, \"r\"): cannot open the connection) caused by .repo_root being derived from sys.frame(1)$ofile, which is NULL under test_dir(). Product code is unaffected; identities reproduced independently by this verifier. 05-15 removed this defect class from the intervention test files; the one-line fix for test-prep-paths.R was deliberately not applied in Phase 5."
human_verification: []
---

# Phase 5: Integrate spatial_interventions branch and stage intervention masks — Verification Report

**Phase Goal:** Merge the colleague-authored `spatial_interventions` branch into `main`, fitting the port to the current allocation architecture (lazy per-from-class predictor reads, threaded prediction, int32 Dinamica cube, transition-pipeline consistency checks); check the masks against the intervention config files and write down a target HPC layout.
**Verified:** 2026-09-28T17:37:24Z
**Status:** passed
**Re-verification:** Yes — after the 05-16 operator gate closed the two gaps recorded by the 2026-09-23 cycle.

## Prior cycle: both gaps CLOSED

The previous `05-VERIFICATION.md` (2026-09-23, `gaps_found`, 12/14) recorded two open items. Both are
now closed, re-checked directly by this verifier rather than accepted from `05-16-SUMMARY.md`:

| Prior gap | Status | Evidence collected in this cycle |
|---|---|---|
| **SC1 / D-01 truth #16** — PR #2 open, `git merge-base --is-ancestor a917ba1 origin/main` exited **1**; "changes are on `main`" was literally not true | ✓ **CLOSED** | `git merge-base --is-ancestor a917ba1 origin/main` now exits **0**. `f54ccc2` is a 2-parent merge commit (`git log -1 --format='%H %P'` → 3 tokens), subject `Merge pull request #2 from NASCENT-Peru/spatial-interventions-integration`, authored 2026-09-28. `git merge-base --is-ancestor 3f06dc3 origin/main` exits **0**, so the merge carried all six gap-closure plans (05-10..05-15) rather than preceding them. `git diff --stat origin/main HEAD -- src scripts config tests docs` is **empty**: the working tree this report verifies is byte-identical to `origin/main` for all code, config, test and doc paths. Colleague authorship intact: `git log --format=%an d0a11df^..a917ba1` → `ManuelKurmann` ×4. |
| **D-09 / D-11 truth #14** — the HPC `sha256sum -c` step was never separately reported; the validator's geometry checks were substituted for it | ✓ **CLOSED** | Operator-attested on HPC: 14 OK / 0 FAILED in `/beegfs/black/nascent-lulcc/inputs/spat_prob_perturb`. **Independently strengthened here:** this verifier ran `sha256sum -c docs/spatial_interventions/masks.sha256` against the source masks in `D:/C.3_Modelling/nascent-lulcc-agg/inputs/spat_prob_perturb` — **14/14 OK**. The manifest is therefore proven to be a correct description of the real mask bytes, not an unverifiable artifact, so the HPC-side 14-OK result is a meaningful transfer proof. |

## Goal Achievement

### Observable Truths

Must-haves merged from the ROADMAP §Phase 5 Success Criteria (SC1–SC4, the non-negotiable contract)
with the `must_haves.truths` frontmatter of **all 16 plans** (05-01..05-16). No truth was dropped; the
review-finding truths (CR/WR/IN) added by the wave-5..11 gap-closure plans are grouped by concern
below and each individual finding is traced in *Requirements Coverage*.

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | **SC1 / D-01 / D-17:** the `spatial_interventions` changes are on `main` via a real merge, colleague authorship intact | ✓ VERIFIED | See *Prior cycle* table above. `is-ancestor` exit 0 for both `a917ba1` and `3f06dc3`; `f54ccc2` has 2 parents; `origin/main` ≡ HEAD for `src/ scripts/ config/ tests/ docs/`. |
| 2 | **SC1:** conflicts resolved against current allocation code; no conflict markers; `scripts/run_allocation.r` sources the merged file list and aborts fatally on a sourcing failure | ✓ VERIFIED | `git grep -nE '^(<<<<<<<\|>>>>>>>\|=======$)' -- . ':!*.md'` → **0 matches**. `src_files` (lines 88–95) is exactly `setup.r, utils.r, dinamica_utils.r, saturation_diagnostics.r, allocation.r, implement_spatial_interventions.R` — no `lulcc.spatprobmanipulation.r`. Sourcing loop has `quit(save="no", status=1)` in its `error=` handler (line 107) followed by `stopifnot(exists("implement_spatial_interventions", mode="function"), exists("resolve_intervention_masks", mode="function"))` (lines 111–113). |
| 3 | **SC1:** existing tests still pass | ✓ VERIFIED | **Re-run by this verifier, not accepted from SUMMARY:** `test_dir("tests/testthat")` → **PASS 684 / FAIL 0 / ERROR 4 / WARN 4 / SKIP 13**. The 4 errors are exactly the pre-existing `test-prep-paths.R` harness defect (4 named tests listed in *Deferred*), reproduced identically. Matches the recorded baseline; no new failures. |
| 4 | **D-02:** `config/SSP{0,1,3,4,5}_interventions.yml` gone; `config/{BAU,NAT,CUL,SOC}_interventions.yml` present | ✓ VERIFIED | `ls config/SSP*_interventions.yml` → "No such file or directory". All 4 scenario files present (81/201/210/143 lines). |
| 5 | **D-03:** legacy intervention code parked under `src/old/` with no active caller; no active `src/*.{r,R}` outside `src/old/` uses `raster::` except `landscape_pattern_analysis.r` | ✓ VERIFIED | All 4 legacy files present under `src/old/`. Repo-wide grep for the 4 old paths outside `src/old/` and `.planning/` finds only descriptive prose in `docs/ARCHITECTURE.md` (which states they are "retired to `src/old/`"), a historical-briefing sentence, and a negative test assertion in `test-spatial-interventions-wiring.R:104` — **no active caller**. `grep -l "raster::" src/*.r src/*.R` → only `src/landscape_pattern_analysis.r`. |
| 6 | **D-04:** branch prose docs are Markdown under `docs/spatial_interventions/`; `spatial_masks/` and the root `.txt`/`.md` originals are gone | ✓ VERIFIED | `docs/spatial_interventions/` holds 7 files (5 converted docs + `masks.sha256` + `parameter_provenance.md`). `spatial_masks/`, `scenario_narratives.txt`, `intervention_planning.txt`, `spatial_interventions_integration_explainer.md` all absent from the tree. |
| 7 | **D-05 / D-06:** every `Intervention_mask` is a bare filename resolved under `spat_prob_perturb_dir`; the resolver rejects `/`, `\`, `..`; dirs come from config | ✓ VERIFIED | `grep -c 'spatial_masks/'` → **0** in all 4 YAMLs. `resolve_intervention_masks()` (`src/implement_spatial_interventions.R:205-207`) stops on `grepl("[/\\\\]", name)` or `grepl("..", name, fixed=TRUE)` before composing `file.path(mask_dir, name)`. `config/{hpc,local}_config.yaml` both define `interventions_dir: "config"` and `spat_prob_perturb_dir: "inputs/spat_prob_perturb"`. |
| 8 | **D-07:** `generate_probability_maps()` threads `year_post`; the hook passes `simulation_time_step = year_post`; it runs after per-cell normalisation and before the per-transition TIF writer; every `Time_steps_implemented` year is a posterior year | ✓ VERIFIED | Signature at `allocation.r:2694-2708` includes `year_post`; call site `2533` passes `year_post = year_post` (line 2539). Normalisation at 3103–3105 (`tot_prob` divide) → hook at **3127** → TIF writer at 3146; `setkey(normalized, row_idx)` re-applied at 3140. **Independently parsed the 4 YAMLs with `yaml::yaml.load_file`:** 14 Allocation entries, years `2024,2028,…,2060`, `all(years %in% seq(2024,2060,4))` → **TRUE**. The branch's `year_ant + step_length` formula is absent. |
| 9 | **D-14 / WR-04 / WR-05 / CR-02(pre-flight) / D-21(pre-flight):** Stage 7 pre-flight is fail-closed — missing masks, resolver errors, a mis-gridded/multi-layer mask, an unloaded engine, a bad `profile_timestep_index` and an unwritable output root all surface before any region work | ✓ VERIFIED | `validate_allocation_runtime()` `intervention_errors` block, `allocation.r:402-697`: `"intervention config: resolve_intervention_masks() not loaded - source src/implement_spatial_interventions.R"` (456–458, WR-04 fail-closed); telemetry-dir writability (488–492, D-21); `ALLOCATION_YEAR_POST_FILTER` + `profile_timestep_index` validation and narrowing (509–537, WR-05); `"intervention mask: missing %s (scenario=%s id=%s year=%d)"` (567–569); per-unique-mask `nlyr != 1` and `compareGeom` assertions (633–650, CR-02); all appended to the single consolidated list at 697. Fixture-mode output unchanged — `test-allocation-preflight.R` is green in the full-suite run (truth 3). |
| 10 | **SC2:** every mask path referenced by the per-scenario YAMLs maps to a file in `spat_prob_perturb`; orphans listed; CRS/extent/resolution checked against the reference grid | ✓ VERIFIED | **Re-derived from source, then re-run as a probe.** Independent YAML parse → 14 distinct referenced mask filenames; `ls D:/C.3_Modelling/nascent-lulcc-agg/inputs/spat_prob_perturb/*_mask*.tif` → **14 files**, names an exact set match, and an exact set match with the 14 lines of `masks.sha256`. This verifier then ran `Rscript scripts/validate_intervention_masks.r --mask-dir <real dir> --ref-grid <real ref grid>` → **exit 0, VERDICT: PASS**, 138 (scenario,id,year) entries, 14/14 present, `compareGeom` **14/14**, **0 orphan** top-level `.tif`, 13 INFO `outside-ref` rows — reproducing `05-MASK-VALIDATION.md` line for line. Additionally verified that no *semantic* YAML change has occurred since that report's commit (`git diff 12f247f..HEAD -- config/*_interventions.yml` touches only header/comment lines; zero `mask`/`Time_steps`/`Prob_adjust`/`Intervention_ID`/`target_classes` lines), so the report is still factually current. |
| 11 | **D-12 / D-13:** the validator checks existence over all posterior years, lists orphans, hard-FAILs on `compareGeom`/`nlyr` mismatch with **no** resample/reproject and no stale `reference_crs`, checks the value domain, writes Markdown and exits non-zero on FAIL | ✓ VERIFIED | `scripts/validate_intervention_masks.r` (408 lines): `compareGeom` ×10, `freq(` ×1, `nlyr` ×4, `resolve_intervention_masks(` ×1; `reference_crs` **0**, `resample(` **0**, `project(` **0**. Non-zero exit and fail-closed behaviour **proven live**: the un-overridden run on this workstation exited **1** with `VERDICT: FAIL (140 failures)` and `mask directory does not exist` (see *Observations*), while the corrected run exited 0. D-13 also holds in product code: `resample(`/`project(` count **0** in both `src/implement_spatial_interventions.R` and `src/allocation.r`. |
| 12 | **SC3 / D-08 / D-10:** a documented HPC placement plan exists — target directory, how `interventions_dir` resolves HPC vs local, transfer/staging steps, and that only `*_mask*.tif` are staged | ✓ VERIFIED | `docs/README_HPC.md` §**Spatial intervention masks** (line 278, before §Allocation smoke test at 363) documents: placement `${HPC_SCRATCH_ROOT}/inputs/spat_prob_perturb/` = `data_basepath` + `spat_prob_perturb_dir`; `interventions_dir` is the repo `config/` on both HPC and local so YAMLs arrive by `git pull` and masks never enter git; what to stage (only the flat `*_mask*.tif`, 14 files ≈34 MB, `nascent-pa-prioritization/` and the `.txt` files stay local); `rsync --include='*_mask*.tif'` with an `scp` fallback for Git Bash; `sha256sum -c` against the manifest; the validator run requiring `VERDICT: PASS` with "any FAIL is fixed by an offline re-export, never by resampling on HPC"; and the smoke recipe with `--partition=highmem` + `ALLOCATION_YEAR_POST_FILTER`. |
| 13 | **D-09 / D-11:** masks staged on HPC and checksum-verified against the 14-line manifest | ✓ VERIFIED (operator-attested + locally strengthened) | `masks.sha256`: 14 lines, LF-only (**0** CR bytes). Operator-reported on beegfs: 14 OK / 0 FAILED. This verifier independently ran `sha256sum -c` against the source masks: **14/14 OK** (see *Prior cycle* table). Job 841577 resolved every mask on HPC with zero `intervention mask: missing` pre-flight lines, which is independent corroboration that the files are actually present there. |
| 14 | **SC4 / D-15:** an allocation smoke run with interventions enabled completes for at least one scenario × region × timestep, and the hardened verifier prints PASS | ✓ VERIFIED (orchestrator-verified operator run) | SLURM job **841577** (NAT × costa_peruana × 2032, `highmem`): `COMPLETED 0:0`, MaxRSS 88.5 GB, 02:11:20. Verifier banner `PASS verify_intervention_smoke scenario=NAT region=costa_peruana year=2032 interventions=5 maps_checked=16 telemetry_rows=9`. 5 extended `AUDIT stage=intervention` lines + 1 summary, all 8 delta fields populated with scenario-specific non-trivial values (e.g. `rows_target=7517803 rows_changed=1058116 sum_abs_delta=62441`) — not placeholders. Telemetry CSV header matches the shipped 25-column contract exactly, and its row count (9) equals the arithmetic sum of interventions × target classes (3+1+3+1+1). Banner format is byte-compatible with the shipped `sprintf()` at `verify_intervention_smoke.r:598-601`, which this verifier read. |
| 15 | **CR-01 / WR-06 / IN-01:** identity and configuration are validated by the resolver, which owns the Allocation/year filter; a missing/empty/non-scalar `Intervention_ID` or a duplicate aborts; an unknown `Mask_type` is caught even for an inactive year | ✓ VERIFIED | `resolve_intervention_masks()`: `Intervention_stage == "Allocation"` filter (55–60, single place); id extraction with `length(v) != 1L → NA` then `stop("... has no usable Intervention_ID")` (66–76) and duplicate detection (78–84); per-entry validation loop over **all** Allocation entries regardless of `years` (92–160) enforcing `Mask_type ∈ {Static,Dynamic}` (IN-01). Every return path carries `attr(out, "entries")` via `with_entries()` (43–47, WR-06) and rows carry `entry_index`, so the applier is driven by resolver rows. YAML is parsed once per call (`yaml.load_file` appears once). |
| 16 | **CR-02 / IN-03 / IN-07 / WR-10:** a mask that is not on the reference grid, is multi-layer, has out-of-range cell numbers, has been deleted since pre-flight, or is queried with a degenerate `cell_index` aborts with a named error instead of silently mis-indexing | ✓ VERIFIED | `.mask_inside_lut(mask_path, cell_index, cache, ref_grid)` (260–332): `cell_id` non-empty/non-NA/`>= 1` check **first** (WR-10, 263–266); `normalizePath(mustWork=TRUE)` re-raised as `"intervention mask missing: … (disappeared after pre-flight)"` — the exact marker the smoke verifier treats as fatal (IN-07, 270–277); cache key = normalised path + sub-second mtime + size + `length(cid)` (IN-03, 280–286); `nlyr != 1` checked **before** `compareGeom` (298–303); `compareGeom(..., stopOnError=FALSE)` mismatch aborts (306–311); `ref_cell_id` range/alignment against `terra::ncell(m)` rejected deterministically rather than relying on terra's warning (CR-02b, 315–326). `ref_grid` is a required argument with **no fallback**. Wired: `allocation.r:3133` passes `ref_grid_path = config[["ref_grid_path"]]`. |
| 17 | **CR-03:** an entry missing a `Prob_adjust_*` field required by its `Prob_adjust_type` aborts **before** any probability is mutated | ✓ VERIFIED | The `req`/`missing_keys`/`bad_num`/`pct_keys`/`Transition_target_classes` validation pass (`implement_spatial_interventions.R:105-160`) runs inside the resolver, i.e. at pre-flight and validator time and before the applier's first write; unknown `Prob_adjust_type` also stops. `Absolute` requires `{Prob_adjust_value, Prob_adjust_zone}`, `Relative` requires the 5 relative keys, percentiles are range-checked 0–100. |
| 18 | **WR-01 / WR-02 / WR-03 / IN-04:** tied percentiles still adjust rows; a zero percentage difference applies the threshold for **all three** valencies; no intervention rewrites a row outside its declared target class and zone; threshold logs report the signed value assigned; threshold/explanation lines carry the target class | ✓ VERIFIED | `relative_prob_adjust()` selects with `>=` on both sides (`ix <- Intervention_idx[Intervention_vals >= Intervention_ptile_val]`, mirroring the `>=` used for the percentile means) — WR-01(a). Boundary is `if (Perc_diff >= 0)` in both `Increase` (1267) and `Decrease` (1309) branches — WR-01(b). All **7** `pmin(pmax(prob, 0), 1)` clamps are applied as `normalized[ix, ...]`, i.e. only to rows just written — WR-02. All 5 `threshold_msg()` calls pass the value actually assigned to `Perc_diff` (`+threshold` / `-(threshold)`) — WR-03. `because_msg()` prefixes `to_val=<class>:` — IN-04. NaN guard (`!is.finite(Perc_diff)` plus both-zone positivity checks) skips and logs instead of crashing. |
| 19 | **WR-07 / D-22:** a mask landing entirely outside the region FAILS instead of a vacuous PASS; `maps_checked` counts only assertions with cells; an `argument is of length zero` anywhere in the scanned logs FAILS; the verifier asserts the telemetry CSV exists, matches the AUDIT lines and shows `mean_after = 0` for Absolute-to-0 targets | ✓ VERIFIED | `scripts/verify_intervention_smoke.r` (602 lines): `n_zone <- terra::global(sel & !is.na(r), "sum", ...)`; `if (!is.finite(n_zone) || n_zone == 0) fail(... "assertion would be vacuous")` **before** `maps_checked <- maps_checked + 1L`, and a non-finite max over a provably non-empty zone also fails rather than passing (344–372). `"argument is of length zero"` is in the forbidden-marker list; so is `intervention mask missing`. 25-column `tel_cols` contract (387–393); CSV path composed identically to the writer; `mean_after` asserted `abs(ma) <= 1e-12` for Absolute-0 rows with `n_target > 0` (500–506). `--interventions-dir` flag present and documented. |
| 20 | **WR-08 / WR-09 / IN-06:** every valency × zone combination shipped in the four production configs is exercised by a test; the zero-preservation test proves the Relative pass actually ran; neither intervention test file depends on base R's `%||%` at source time | ✓ VERIFIED | Independently enumerated the shipped combos from the YAMLs: **6** distinct `type/valency/zone` pairs — `Absolute/Inside`, `Absolute/Outside`, `Relative/Decrease/Inside`, `Relative/Decrease/Outside`, `Relative/Increase/Inside`, `Relative/Increase_inside_decrease_outside/Inside`. All 6 appear in `test-spatial-interventions.R`: `.abs_entry(zone="Outside")` (549), `.rel_entry(valency="Increase_inside_decrease_outside")` (1436), `.rel_entry(valency="Decrease", zone="Outside")` (1489), `Increase`/`Inside` (625, 953, 993), plus the `Inside` defaults — and a negative test that `Increase_inside_decrease_outside` + `zone="Outside"` errors (1527). The `ZEROSURVIVE` test now asserts on the **rank-2 line specifically** (`rows_changed=1`, `n_inc=1`, `sum_abs_delta > 0`, exact `0.84`) and `expect_false(any(grepl("intervention skip:", ...)))` — so it can no longer pass because the Relative pass was skipped (WR-09). `grep -cF '%||%'` → **0** in both test files and in the 77-line `helper-spatial-interventions.R` (IN-06). |
| 21 | **WR-11 / IN-05:** every parameter choice is justified from a file that is in the repository; no YAML header cites the unversioned protocol without naming the in-repo record; `.TIF/.tiff/.TIFF` masks are classified rather than reported as local-only; a `--scenarios`-narrowed validator run says so in its report header | ✓ VERIFIED | `docs/spatial_interventions/parameter_provenance.md` (159 lines) with `## Source of record`, `## Parameter rationale by intervention`, `## Non-obvious choices`, `## Unresolved`, `## Authoring record`; all 8 distinct shipped intervention IDs covering the 14 entries appear in it. All 4 YAMLs cite it (`grep -l` → 4/4) alongside the protocol reference, and `tests/testthat/test-intervention-provenance.R` (138 lines, 3 `test_that` blocks) is a repo-consistency gate that fails if an intervention is added without a rationale row. Validator: `tifs <- top[grepl("[.]tiff?$", top, ignore.case=TRUE)]` with case-insensitive orphan comparison (302–310, IN-05), and the scoped-orphan sentence is emitted into the report header (340). |
| 22 | **D-18 / D-19:** every `AUDIT stage=intervention` line reports the distribution of the probability change that intervention made, with the pre-existing fields and order unchanged; each allocation run writes one `intervention_prob_deltas` CSV per scenario × region × year with one row per intervention × target class; deltas are computed over the targeted rows against the values immediately before that intervention ran | ✓ VERIFIED | Single `sprintf()` at `implement_spatial_interventions.R:831` — the prefix through `rows_changed=%d` is intact and the 8 `delta_*`/`n_inc`/`n_dec`/`sum_abs_delta` fields are **appended at the end**, so the shipped parser (which extracts `id` and `rows_changed` by fixed string) still works; the frozen-field contract is documented in the roxygen block (496–521). `before <- normalized$prob[Target_area_idx]` / `[sub_idx]` is taken immediately before each adjuster's write and `after` immediately after (D-19); `.delta_stats()` / `.bind_delta_stats()` guarantee exactly one stats row per target class, seeded with `.delta_stats_skipped()` before any `next` path. CSV: `sprintf("intervention_prob_deltas_%s_%s_%d.csv", ...)` (903), 25 columns documented at 546–555 and mirrored by the verifier's `tel_cols`. **Live proof from job 841577:** 9 CSV rows, header identical to the contract, and AUDIT `sum_abs_delta` consistent with the per-class CSV rows. |
| 23 | **D-20 / D-21:** no intervention materialises a second copy of the probability table; a telemetry write failure warns and lets the run complete; the filename cannot escape the region work directory | ✓ VERIFIED | `grep -nE 'data\.table::copy\|(^\|[^.[:alnum:]_])copy\('` in the engine → **0 matches**; the only snapshots are the two subset vectors `normalized$prob[Target_area_idx]` / `[sub_idx]`. Telemetry write is wrapped in `tryCatch` emitting `"WARN intervention telemetry: failed to write %s: %s"` and returning the adjusted table (945); it stages to `.tmp` then `file.rename` (927–933). Filename is sanitised by `.safe_path_token()` **and** re-asserted separator-free and `..`-free after composition (911–913). MaxRSS on the post-telemetry smoke (88.5 GB) is *below* the pre-telemetry job 838021 (95.7 GB) — no memory regression. |
| 24 | **D-16 / D-23:** gap closure covers every `05-REVIEW.md` finding with none deferred on severity grounds, and each CR/WR fix ships a regression test | ✓ VERIFIED | All **21** findings (CR-01..03, WR-01..11, IN-01..07) individually traced to shipped code in truths 15–21 and in *Requirements Coverage* below — none unaddressed. **Tests re-run by this verifier:** the 4 intervention test files (`test-spatial-interventions.R` 1541 lines / 60 `test_that`, `test-spatial-interventions-wiring.R` 606 / 32, `test-verify-intervention-smoke.R` 371 / 9, `test-intervention-provenance.R` 138 / 3) → **462 passing expectations, 0 failures, 0 errors, 0 skips**. |

**Score:** **24/24 truths VERIFIED.** 0 FAILED, 0 UNCERTAIN.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `src/implement_spatial_interventions.R` | Resolver + LUT + engine + both adjusters + telemetry; no `cat(`, no `resample(`/`project(` | ✓ VERIFIED | 1388 lines (vs the branch's 585 — a strict superset: 12 top-level functions vs 3). `cat(` **0**, `resample(` **0**, `project(` **0**, `TODO/FIXME/XXX/TBD/HACK` **0**. All named functions present at the documented signatures. |
| `src/allocation.r` | `year_post`-threaded hook with `ref_grid_path` + `telemetry_dir`; fail-closed D-14 pre-flight | ✓ VERIFIED | 3287 lines. Hook at 3127–3139 passing all 10 documented arguments. Pre-flight block 402–697. `resample(`/`project(` **0**. |
| `scripts/run_allocation.r` | Merged `src_files`, fatal sourcing guard | ✓ VERIFIED | 336 lines; see truth 2. |
| `scripts/validate_intervention_masks.r` | D-12/D-13/IN-05 validator CLI | ✓ VERIFIED | 408 lines. **Executed by this verifier:** exit 1 + `VERDICT: FAIL` on a missing mask dir, exit 0 + `VERDICT: PASS` against the real masks. |
| `scripts/verify_intervention_smoke.r` | D-15/D-22/WR-07/CR-03 post-smoke assertion CLI | ✓ VERIFIED | 602 lines; non-vacuous zone assertions, extended forbidden markers, telemetry assertions, `--interventions-dir`. |
| `tests/testthat/test-spatial-interventions.R` | Engine/resolver/adjuster regression fixtures (min 150) | ✓ VERIFIED | 1541 lines, 60 `test_that`. |
| `tests/testthat/test-spatial-interventions-wiring.R` | Static wiring + pre-flight + repo-consistency (min 80) | ✓ VERIFIED | 606 lines, 32 `test_that`. |
| `tests/testthat/test-verify-intervention-smoke.R` | End-to-end fixture proving a vacuous zone fails (min 80) | ✓ VERIFIED | 371 lines, 9 `test_that`. |
| `tests/testthat/test-intervention-provenance.R` | WR-11 repo-consistency gate (min 30) | ✓ VERIFIED | 138 lines, 3 `test_that`. |
| `tests/testthat/helper-spatial-interventions.R` | Shared repo-root resolution without `%||%` (min 10) | ✓ VERIFIED | 77 lines, `%||%` count 0. |
| `docs/spatial_interventions/parameter_provenance.md` | In-repo provenance record for all 14 Allocation entries (min 60) | ✓ VERIFIED | 159 lines; all 8 distinct IDs covered; `## Unresolved` records the open `TODO: differentiate from NAT` magnitudes. |
| `docs/spatial_interventions/masks.sha256` | 14-line transfer manifest (min 14) | ✓ VERIFIED | 14 lines, LF-only; **`sha256sum -c` → 14/14 OK** against the real masks. |
| `docs/README_HPC.md` | §Spatial intervention masks staging + smoke | ✓ VERIFIED | 606 lines; section at 278; all required substrings present. |
| `config/{BAU,NAT,CUL,SOC}_interventions.yml` | Bare filenames, corrected headers, provenance citation | ✓ VERIFIED | 14 Allocation entries; 0 `spatial_masks/`; all 4 cite both `scenario_narratives.md` and `parameter_provenance.md`. |
| `config/{hpc,local}_config.yaml` | `interventions_dir`, `spat_prob_perturb_dir`, `ref_grid_path` | ✓ VERIFIED | All three keys present in both. |
| `src/old/{lulcc.spatprobmanipulation.r, spatial_interventions_prep.r, run_spatial_interventions_prep.r, submit_spatial_interventions_prep.sh}` | Legacy code parked, history kept | ✓ VERIFIED | All 4 present; no active caller. |
| `docs/spatial_interventions/*.md` (5 converted docs) | Markdown conversions | ✓ VERIFIED | All 5 present. |
| `.planning/.../05-MASK-VALIDATION.md` | Local validator report, VERDICT: PASS | ✓ VERIFIED | Reproduced independently (truth 10). |

No artifact is a stub: every file that is claimed to implement behaviour was opened and read, and the
behaviour asserted in the must-haves was located at specific line numbers.

### Key Link Verification

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| `setup_allocation_inputs()` | `generate_probability_maps(year_post = year_post)` | call-site argument | ✓ WIRED | `allocation.r:2533` call, `2539` argument. |
| `generate_probability_maps()` | `implement_spatial_interventions()` | direct call, 10 named args | ✓ WIRED | `3127-3139`; after normalisation (3103–3105), before the TIF writer (3146). |
| hook | engine geometry gate | `ref_grid_path = config[["ref_grid_path"]]` | ✓ WIRED | `3133` (matches the plan's regex; a literal-space grep misses it only because of column alignment). |
| hook | telemetry CSV | `telemetry_dir = work_dir` | ✓ WIRED | `3138`; writer composes the filename at `implement_spatial_interventions.R:903`. |
| `implement_spatial_interventions()` | `resolve_intervention_masks()` | `attr(resolved, "entries")` + `resolved$entry_index` | ✓ WIRED | `584-589`, then `missing_rows` gate at `590-596` **before** any mutation. |
| `implement_spatial_interventions()` | `.mask_inside_lut(..., ref_grid)` | required `ref_grid` SpatRaster | ✓ WIRED | No fallback path exists; `ref_grid` is a positional requirement. |
| `.mask_inside_lut()` | `terra::extract(m, cell_index$ref_cell_id)` | cell-number extract, cached | ✓ WIRED | Engine reads only `normalized$cell_id / $from_val / $to_val / $prob` — **no `$x`/`$y`, no `as.matrix`, no `xyFromCell`** (IN-02). |
| `validate_allocation_runtime()` | `resolve_intervention_masks()` + `compareGeom` | guarded call-time lookup, fail-closed | ✓ WIRED | `454-456`, `552`, `633-650`. |
| `scripts/validate_intervention_masks.r` | `resolve_intervention_masks()` | `source(src/implement_spatial_interventions.R)` | ✓ WIRED | **Proven by execution:** 138 resolved rows in this verifier's own run. |
| `config/*_interventions.yml` | `docs/spatial_interventions/parameter_provenance.md` | header citation | ✓ WIRED | 4/4 files; enforced by `test-intervention-provenance.R`. |
| `docs/README_HPC.md` | `docs/spatial_interventions/masks.sha256` | `sha256sum -c` step | ✓ WIRED | Documented **and executed** (14/14 OK locally; 14 OK / 0 FAILED on HPC). |
| HPC smoke job | `scripts/verify_intervention_smoke.r` | verifier run on job output | ✓ WIRED | `PASS verify_intervention_smoke … telemetry_rows=9` from job 841577; banner format matches the shipped `sprintf()`. |
| smoke verifier | telemetry CSV | `read.csv` + per-intervention cross-check vs AUDIT | ✓ WIRED | `tel_path` composed identically to the writer; 25-column contract asserted. |

### Data-Flow Trace (Level 4)

No frontend; the equivalent trace is mask config → resolver → cached LUT → probability mutation →
AUDIT line → telemetry CSV → smoke assertion. Traced end to end and proven non-hollow:

| Stage | Data | Source | Produces real data | Status |
|---|---|---|---|---|
| YAML → resolver | 14 Allocation entries, 138 (scenario,id,year) rows | `yaml.load_file` on the shipped `config/*.yml` | Yes — independently parsed by this verifier; 6 distinct valency/zone combos, 14 mask names | ✓ FLOWING |
| resolver → filesystem | `mask_path`, `exists` | real `spat_prob_perturb` directory | Yes — 14/14 files present, 14/14 sha256 OK, 14/14 `compareGeom` TRUE | ✓ FLOWING |
| mask → LUT | logical vector indexed by region `cell_id` | `terra::extract(m, ref_cell_id)` after grid proof | Yes — job 841577 mutated millions of rows per intervention | ✓ FLOWING |
| engine → AUDIT | 21-field line per intervention | `sprintf` over `res$rows_target`/`res$stats` | Yes — 5 scenario-specific lines with distinct non-zero deltas | ✓ FLOWING |
| engine → CSV | 9 rows × 25 columns | `fwrite` of `do.call(rbind, telemetry_rows)` | Yes — header matches contract; row count equals interventions × target classes | ✓ FLOWING |
| CSV + logs → verifier | PASS/FAIL | `read.csv` + log scan + `terra::global` | Yes — PASS with `maps_checked=16`, `telemetry_rows=9`; vacuous checks provably cannot inflate `maps_checked` | ✓ FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|---|---|---|---|
| Full R test suite (SC1 "existing tests still pass") | `Rscript -e 'test_dir("tests/testthat")'` | `PASS 684 FAIL 0 ERROR 4 WARN 4 SKIP 13`; all 4 errors in `test-prep-paths.R` | ✓ PASS |
| Intervention test files only (D-23) | `test_dir(filter="spatial-interventions\|intervention-provenance\|verify-intervention-smoke")` | 462 passed, 0 failed, 0 errors, 0 skips | ✓ PASS |
| Mask manifest integrity (D-09/D-11) | `sha256sum -c docs/spatial_interventions/masks.sha256` in the real mask dir | 14 × `OK`, 0 FAILED | ✓ PASS |
| Mask validator against real masks (SC2/D-12) | `Rscript scripts/validate_intervention_masks.r --mask-dir <real> --ref-grid <real>` | exit 0, `VERDICT: PASS`, 138 entries, 14/14, 0 orphans | ✓ PASS |
| Validator is fail-closed (D-12 non-zero exit) | same script with the config's default (non-existent) mask dir | exit 1, `VERDICT: FAIL (140 failures)`, `mask directory does not exist` | ✓ PASS |
| Independent YAML contract parse (D-07/WR-08) | `yaml::yaml.load_file` over the 4 configs | 14 Allocation entries; 6 valency/zone combos; all `Time_steps_implemented` ∈ posterior years | ✓ PASS |
| Merge is real and complete (SC1/D-01/D-17) | `git merge-base --is-ancestor` ×2, `git log -1 --format=%P`, `git diff origin/main HEAD` | exit 0, exit 0, 2 parents, empty diff | ✓ PASS |
| No conflict markers | `git grep -nE '^(<<<<<<<\|>>>>>>>\|=======$)' -- . ':!*.md'` | 0 matches | ✓ PASS |
| HPC smoke (SC4) | `sbatch`/`sacct` on `login02` | job 841577 `COMPLETED 0:0` | ? SKIP → satisfied by orchestrator-verified operator evidence (not re-runnable from the workstation) |

### Probe Execution

This project has no `scripts/*/tests/probe-*.sh` probes; the phase's own CLIs are its probes and were
executed rather than read:

| Probe | Command | Result | Status |
|---|---|---|---|
| `scripts/validate_intervention_masks.r` | `bash`-invoked `Rscript … --mask-dir <real> --ref-grid <real>` | exit 0, `VERDICT: PASS` | PASS |
| `scripts/validate_intervention_masks.r` (negative) | `Rscript …` with the stale config default | exit 1, `VERDICT: FAIL` | PASS (fail-closed proven) |
| `tests/testthat` (full) | `Rscript -e 'test_dir(...)'` | 684 pass / 0 fail / 4 pre-existing errors | PASS |
| `scripts/verify_intervention_smoke.r` | executed on HPC against job 841577 output | `PASS verify_intervention_smoke … telemetry_rows=9` | PASS (operator-run, orchestrator-verified) |

### Requirements Coverage

**REQUIREMENTS.md has no Phase 5 rows.** `grep -i 'phase 5' .planning/REQUIREMENTS.md` → nothing; its
controlled vocabulary is `OBS-*, MEM-*, PERF-*, INFRA-*, PIPE-*, ALLOC-*, RUN-*, TEST-*`. All 48 IDs
declared across the 16 plans are **phase-local**: `SC1–SC4` are the ROADMAP Success Criteria,
`D-01..D-23` are decisions in `05-CONTEXT.md`, and `CR-01..03 / WR-01..11 / IN-01..07` are the 21
`05-REVIEW.md` findings. Stated plainly rather than mapped onto unrelated IDs. **Zero orphaned IDs:**
every one of the 48 is claimed by at least one plan and accounted for below.

| Requirement | Source plan(s) | Description | Status | Evidence |
|---|---|---|---|---|
| SC1 | 01,02,03,04,06 | Changes on `main`, conflicts resolved, tests pass | ✓ SATISFIED | Truths 1–3 |
| SC2 | 03,05,06 | Masks map to files; orphans; grid check | ✓ SATISFIED | Truth 10 (re-run) |
| SC3 | 05,06 | HPC placement plan documented | ✓ SATISFIED | Truth 12 |
| SC4 | 02,04,05,06 | Smoke run completes | ✓ SATISFIED | Truth 14 |
| D-01 | 01,06 | Real merge, authorship intact | ✓ SATISFIED | Truth 1 |
| D-02 | 01,03 | SSP configs removed | ✓ SATISFIED | Truth 4 |
| D-03 | 03 | Legacy code parked | ✓ SATISFIED | Truth 5 |
| D-04 | 03 | Docs to Markdown | ✓ SATISFIED | Truth 6 |
| D-05 | 02,03,04 | Bare filenames + traversal rejection | ✓ SATISFIED | Truth 7 |
| D-06 | 03,04 | Config-driven dirs, corrected headers | ✓ SATISFIED | Truths 7, 21 |
| D-07 | 02,04 | `year_post` threading, posterior years | ✓ SATISFIED | Truth 8 |
| D-08 | 05,06 | HPC target directory | ✓ SATISFIED | Truth 12 |
| D-09 | 05,06 | Staging + checksum | ✓ SATISFIED | Truth 13 |
| D-10 | 05 | Only `*_mask*.tif` staged | ✓ SATISFIED | Truth 12 |
| D-11 | 06 | Masks staged on HPC | ✓ SATISFIED | Truth 13 |
| D-12 | 05 | Validator semantics | ✓ SATISFIED | Truth 11 (executed) |
| D-13 | 02,05 | No resample/reproject/`reference_crs` | ✓ SATISFIED | Truth 11 |
| D-14 | 02,04,06 | Pre-flight mask/config gate | ✓ SATISFIED | Truth 9 |
| D-15 | 02,05,06 | Smoke + verifier assertions | ✓ SATISFIED | Truths 14, 19 |
| D-16 | 07,08 | All 21 review findings closed | ✓ SATISFIED | Truth 24 |
| D-17 | 16 | Merge + HPC checksum closed | ✓ SATISFIED | Prior-cycle table |
| D-18 | 12,13 | Delta telemetry (AUDIT + CSV) | ✓ SATISFIED | Truth 22 |
| D-19 | 12 | Per-intervention sequential deltas | ✓ SATISFIED | Truth 22 |
| D-20 | 12 | No second prob-table copy | ✓ SATISFIED | Truth 23 |
| D-21 | 13 | Telemetry failure non-fatal | ✓ SATISFIED | Truth 23 |
| D-22 | 14 | Verifier asserts telemetry | ✓ SATISFIED | Truth 19 |
| D-23 | 07–15 | Regression test per fix | ✓ SATISFIED | Truth 24 (462 expectations re-run) |
| CR-01 | 07 | Missing `Intervention_ID` aborts | ✓ SATISFIED | Truth 15 |
| CR-02 | 09,10 | Off-grid/multi-layer/out-of-range mask aborts | ✓ SATISFIED | Truths 16, 9 |
| CR-03 | 07,14 | `Prob_adjust_*` schema validated pre-mutation | ✓ SATISFIED | Truth 17 |
| WR-01 | 11 | Aligned percentile + `>= 0` boundary | ✓ SATISFIED | Truth 18 |
| WR-02 | 11 | Clamps scoped to written rows | ✓ SATISFIED | Truth 18 |
| WR-03 | 11 | Signed threshold logs | ✓ SATISFIED | Truth 18 |
| WR-04 | 10 | Pre-flight errors when engine unloaded | ✓ SATISFIED | Truth 9 |
| WR-05 | 10 | `profile_timestep_index` narrowing | ✓ SATISFIED | Truth 9 |
| WR-06 | 07 | Resolver owns the entry list | ✓ SATISFIED | Truth 15 |
| WR-07 | 14 | Vacuous zone FAILs | ✓ SATISFIED | Truth 19 |
| WR-08 | 15 | All shipped combos tested | ✓ SATISFIED | Truth 20 (combos re-derived) |
| WR-09 | 15 | Zero-preservation test is honest | ✓ SATISFIED | Truth 20 |
| WR-10 | 09 | Degenerate `cell_index` rejected | ✓ SATISFIED | Truth 16 |
| WR-11 | 08 | In-repo parameter provenance | ✓ SATISFIED | Truth 21 |
| IN-01 | 07 | Unknown `Mask_type` always caught | ✓ SATISFIED | Truth 15 |
| IN-02 | 09 | Cell-number lookup, never x/y | ✓ SATISFIED | Key-link table |
| IN-03 | 09 | mtime/size-aware cache key | ✓ SATISFIED | Truth 16 |
| IN-04 | 11 | Logs attributable to a target class | ✓ SATISFIED | Truth 18 |
| IN-05 | 08 | `.TIF/.tiff` classified; scoped orphan header | ✓ SATISFIED | Truth 21 |
| IN-06 | 15 | No `%||%` at source time | ✓ SATISFIED | Truth 20 |
| IN-07 | 09 | Vanished mask uses the fatal marker | ✓ SATISFIED | Truth 16 |

Adjacent note (not a Phase 5 obligation): `PIPE-05` and `PIPE-06` are assigned to **Phase 4** in
REQUIREMENTS.md yet Phase 5 work satisfies much of their text (two of the three `raster::` files named
in PIPE-05 now sit in `src/old/`; PIPE-06's YAML-path concern is closed by D-05). The traceability
table is stale with respect to completed work and should be reconciled by its owner.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|---|---|---|---|---|
| `config/CUL_interventions.yml` | 36, 120, 154 | `TODO: differentiate from NAT` | ℹ️ INFO | `git log -L` confirms commit `b01bb71` (ManuelKurmann, 2026-06-08) — pre-existing colleague content, preserved deliberately (Plan 03 required byte-identical entries apart from mask paths/headers). Now referenced to in-repo follow-up: `parameter_provenance.md` §`## Unresolved` names the intended CUL-specific magnitudes. Documents a scientific-modelling refinement, not engineering debt. |
| `config/SOC_interventions.yml` | 35 | `TODO: differentiate from NAT` | ℹ️ INFO | Same origin and same in-repo follow-up record. |
| `docs/spatial_interventions/parameter_provenance.md` | 60, 61, 131 | `TODO` (quoted) | ℹ️ INFO | Quotations of the two YAML TODOs inside the record that resolves them. |

**Debt-marker gate: PASS.** `TBD`, `FIXME` and `XXX` counts are **0** across every file this phase
created or modified. The only `TODO`s are pre-existing colleague content with a named in-repo
follow-up record, so the gate's "unreferenced marker" condition is not met. No stub returns, no
empty handlers, no `cat()` in the engine, no hardcoded-empty data paths.

Two candidate anti-patterns were investigated and **dismissed on evidence**:

- `docs/spatial_interventions/integration_explainer.md:85` says "CRS / extent alignment is assumed,
  not checked", which contradicts the shipped `compareGeom` gates. **Not a defect:** the file's third
  line labels it a "Historical pre-integration briefing … The current behaviour is documented in
  `docs/ARCHITECTURE.md` and `src/implement_spatial_interventions.R`."
- The legacy `Agri_maintenance`/`Agri_abandonment` marginality special-case that the explainer said
  the integration would preserve is absent from the shipped engine. **Not a regression:**
  `git show a917ba1:src/implement_spatial_interventions.R` has **0** occurrences of it either (it
  lives only in `src/old/lulcc.spatprobmanipulation.r`), and no shipped YAML entry uses those IDs —
  all 14 entries are `Intervention_stage: Allocation` with the 8 IDs listed in the provenance record.
  The port is a strict superset of the branch file (1388 lines / 12 functions vs 585 / 3).

### Observations (non-blocking)

1. **`config/local_config.yaml:5` has `data_basepath: "E:/nascent-lulcc-agg"`, and drive `E:` does not
   exist on this workstation** (the data is at `D:/C.3_Modelling/nascent-lulcc-agg`). A bare
   `Rscript scripts/validate_intervention_masks.r` therefore exits 1 with `mask directory does not
   exist`; `--mask-dir`/`--ref-grid` overrides are required locally, which is how `05-MASK-VALIDATION.md`
   (header: `D:/…`) must have been produced, though it does not record the flags used.
   **Out of Phase 5 scope and not a gap:** `git log -L 5,5:config/local_config.yaml` shows the `E:`
   value was last set on 2026-01-27 by `blenback` ("update aggregation code"), long before this phase;
   Phase 5's only change to that file was adding the `interventions_dir` line. The validator's
   behaviour here is correct fail-closed behaviour, and it is HPC runs that matter for the phase goal.
   Worth a one-line fix by the workstation owner, plus recording validator flags in future reports.
2. **`05-MASK-VALIDATION.md` predates Plan 08's validator changes** (IN-05 case-insensitivity, the
   scoped-orphan header sentence). Its findings remain factually current because no semantic YAML
   change has occurred since (verified by diff), and this verifier re-ran the *current* validator to
   the same result — so no re-run is required.
3. **`docs/README_HPC.md`'s prose summary of `verify_intervention_smoke.r` predates Plan 14**: it lists
   the AUDIT/zone/`posterior.tif` checks but not the telemetry-CSV assertions or `--interventions-dir`
   (both documented in the script's own usage header). Cosmetic doc-freshness only; SC3's required
   content is all present.

### Human Verification Required

**None.** Every HPC-only item this phase depends on has already been executed and reported: the
`sha256sum -c` (operator-attested, and independently corroborated here by verifying the manifest
against the source masks), the HPC validator `VERDICT: PASS`, the Stage 7 pre-flight resolving every
mask, and the job-841577 smoke with a hardened-verifier PASS. Nothing remains for a human to test in
order to close this phase, and no truth was left UNCERTAIN.

### Gaps Summary

**No gaps.** This is a `passed` verdict reached by re-deriving the phase's claims from the codebase
rather than from its SUMMARY files. Specifically, the following were re-established from primary
sources rather than accepted: the merge ancestry and the HEAD ≡ `origin/main` equivalence; the full
test suite (684/0/4/13, run by this verifier); the 462 intervention-test expectations; the 14 Allocation
entries, 6 valency/zone combinations and posterior-year timesteps (parsed from the shipped YAML, which
also confirmed that `05-REVIEW.md`'s WR-11 text misattributes the `Outside` + `Prob_adjust_value: 0`
pairing to CUL's `Mining_outside_restraint` — that entry is `Relative`/`Decrease`/`Outside` with no
`Prob_adjust_value`, and the `Outside`+`0` pairing belongs to NAT's `Mining_freeze_post_2030` and SOC's
`Mining_in_low_ES_areas`); the presence of all 14 masks and the correctness of the sha256 manifest; and
a fresh `VERDICT: PASS` from the shipped mask validator plus a `VERDICT: FAIL`/exit-1 proof that it is
fail-closed.

One item is recorded as **deferred, not a gap**: the 4 `test-prep-paths.R` errors are a pre-existing
test-harness defect (`.repo_root` from `sys.frame(1)$ofile`, NULL under `test_dir()`), out of Phase 5
scope, with product code unaffected. Plan 05-15 removed the same defect class from the intervention
test files; applying the equivalent one-line fix to `test-prep-paths.R` is a clean candidate for a
future phase.

---

_Verified: 2026-09-28T17:37:24Z_
_Verifier: Claude (gsd-verifier) — goal-backward, FORCE stance_
