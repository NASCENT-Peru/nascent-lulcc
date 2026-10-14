---
phase: quick-261014-dex
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - scripts/archive_allocation_outputs.sh
  - tests/shell/test-archive-allocation-outputs.sh
  - docs/README_HPC.md
  - .planning/quick/261014-dex-archive-pre-intervention-allocation-outp/261014-dex-OPERATOR.md
autonomous: true
requirements: [QUICK-261014-dex]

must_haves:
  truths:
    - "Run on login02 with no flags, the archive script moves nothing and prints, for each of BAU NAT CUL SOC, the region-posterior count, the national-mosaic count, a per-region breakdown and the exact source -> destination it would use"
    - "Run with --apply, each existing $SIM_OUT/<scenario> tree is renamed (mv, same filesystem) to $SIM_OUT/_archive_<tag>/<scenario>, so the launcher pattern $SIM_OUT/$SCENARIO/$YEAR/region_<suffix>/posterior.tif no longer finds any pre-intervention posterior"
    - "The archive script refuses to run when the Stage 7 contract fails, when SIM_OUT is empty, '/', relative, not a directory, or not under $HPC_SCRATCH_ROOT/, when an archive target already exists, when source and archive are on different filesystems, and (for --apply) when allocation/mosaic/verifier jobs of $USER are queued or running"
    - "The archive script resolves SIM_OUT with the same get_config() probe as submit_allocation_scenario.sh and never loads terra, reads raster contents, copies or deletes"
    - "A manifest of every move (time, user, host, code HEAD, scenario, source, destination, counts) is written inside the archive dir"
    - "The operator can submit a CUL x costa_peruana x 2024 intervention smoke on highmem and its verifier as batch jobs by copying literal commands from 261014-dex-OPERATOR.md, with no <placeholder> tokens"
    - "The runbook states in advance what CUL costa_peruana 2024 evidence should look like for the two never-run combinations (Relative/Decrease/Outside, Relative/Increase/Inside) and which values would be wrong, and states up front whether the Absolute-0 verifier ledger can FAIL for CUL in costa_peruana"
  artifacts:
    - path: "scripts/archive_allocation_outputs.sh"
      provides: "Guarded dry-run-by-default archive of $SIM_OUT/<scenario> trees"
      contains: "cfg[['simulation_output_dir']]"
      min_lines: 150
    - path: "tests/shell/test-archive-allocation-outputs.sh"
      provides: "Pure-bash fixture test of the archive script in library mode"
      min_lines: 80
    - path: ".planning/quick/261014-dex-archive-pre-intervention-allocation-outp/261014-dex-OPERATOR.md"
      provides: "Machine-grouped operator runbook: push, pull, contract, code identity, archive, CUL smoke, sacct, verifier, evidence"
      contains: "Where:"
    - path: "docs/README_HPC.md"
      provides: "Short operator subsection documenting archive_allocation_outputs.sh"
      contains: "archive_allocation_outputs.sh"
  key_links:
    - from: "scripts/archive_allocation_outputs.sh"
      to: "scripts/hpc_common.sh"
      via: "source + check_stage7_contract / setup_common_env / activate_env / verify_rscript"
      pattern: "check_stage7_contract"
    - from: "scripts/archive_allocation_outputs.sh"
      to: "src/setup.r get_config()"
      via: "launcher-identical Rscript --vanilla probe of simulation_output_dir"
      pattern: "simulation_output_dir"
    - from: "261014-dex-OPERATOR.md"
      to: "scripts/submit_allocation_smoke.sh"
      via: "sbatch --parsable --partition=highmem --export=ALL,ALLOCATION_PROFILE_SCENARIO=CUL,ALLOCATION_REGION_FILTER=costa_peruana,ALLOCATION_YEAR_POST_FILTER=2024"
      pattern: "ALLOCATION_PROFILE_SCENARIO=CUL"
    - from: "261014-dex-OPERATOR.md"
      to: "scripts/submit_verify_intervention_smoke.sh"
      via: "sbatch batch wrapper (existing) forwarding --scenario CUL --region costa_peruana --year 2024 --extra-log"
      pattern: "submit_verify_intervention_smoke.sh"
---

<objective>
Close the two blockers in front of the Phase 4 full sweep (`bash scripts/submit_allocation_all_scenarios.sh`):

1. Pre-intervention posteriors in the HPC output tree would be treated as finished by the launcher's file-presence skip check (`submit_allocation_scenario.sh` L174-180) and as chain anteriors by the driver's resume. Deliver a guarded, dry-run-by-default archive script that renames each `$SIM_OUT/<scenario>` tree out of the launcher's path pattern.
2. CUL has no live evidence. Two valency/zone combinations exist only in CUL: Relative/Decrease/Outside (Mining_outside_restraint, mining_concessions_mask.tif) and Relative/Increase/Inside (Urban_densification, urban_settlement_mask.tif). Deliver a runbook for a CUL x costa_peruana x 2024 smoke plus verifier, using the existing `submit_allocation_smoke.sh` and `submit_verify_intervention_smoke.sh`, with expected-value guidance written before the run.

Purpose: the sweep must start from intervention-aware anteriors only, and the two CUL-only combinations must have run once live before 160 region-timesteps depend on them.
Output: `scripts/archive_allocation_outputs.sh`, `tests/shell/test-archive-allocation-outputs.sh`, a short `docs/README_HPC.md` subsection, and `261014-dex-OPERATOR.md`. The executor runs on the Windows workstation and cannot reach the HPC; all HPC actions are runbook steps for the user.
</objective>

<execution_context>
@C:/Users/black/.claude/plugins/cache/gsd-plugin/gsd/4.9.1/workflows/execute-plan.md
@C:/Users/black/.claude/plugins/cache/gsd-plugin/gsd/4.9.1/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@scripts/submit_allocation_scenario.sh
@scripts/hpc_common.sh
@scripts/submit_allocation_smoke.sh
@scripts/submit_verify_intervention_smoke.sh
@config/CUL_interventions.yml
@tests/shell/test-setup-environments-hpc-refusal.sh

<interfaces>
<!-- Extracted by the planner. Use directly; do not re-explore. -->

SIM_OUT resolution in scripts/submit_allocation_scenario.sh (L98-142), the logic to mirror exactly:
- `source "$SCRIPT_DIR/hpc_common.sh"`; `ENV_PATH="$ENV_BASE_PATH/allocation_env"`
- `setup_common_env || exit 1`; `activate_env "$ENV_PATH" || exit 1`; `RSCRIPT_BIN=$(verify_rscript "$ENV_PATH")`, exit on non-zero
- probe: `"$RSCRIPT_BIN" --vanilla -e "setwd(Sys.getenv('PROJECT_ROOT')); source('src/setup.r'); cfg <- get_config(); cat(cfg[['simulation_output_dir']], '\n')" 2>/dev/null`, take line 1, trim with `xargs`; PROJECT_ROOT must be exported first.
- On HPC this resolves to `$HPC_SCRATCH_ROOT/outputs/simulations` (config/hpc_config.yaml: `data_basepath: "${HPC_SCRATCH_ROOT}"`, `output_dirs.simulation_output_dir: "outputs/simulations"`). Real value: `/beegfs/black/nascent-lulcc/outputs/simulations`.
- src/setup.r loads only the `yaml` namespace (no terra, no raster I/O). Side effect: `build_full_config()` `dir.create`s any missing configured input/output dir (same as the launcher already does on login02).

Launcher skip pattern (L176): `$SIM_OUT/$SCENARIO/$YEAR/region_${REGION_SUFFIX}/posterior.tif` for every posterior year 2024..2060 (10 years). National mosaic (assemble_national_mosaic.r / allocation.r L2473): `$SIM_OUT/$SCENARIO/$YEAR/posterior_$YEAR.tif`. Run manifest: `$SIM_OUT/$SCENARIO/run_manifest.csv`.

What lives under `$SIM_OUT/<scenario>/<year>/region_<suffix>/` (all written BY the run, regenerated every timestep): `anterior.tif` (allocation.r L1349, written from the chained current_lulc_path), `probability_map_dir/` (L2848), `worker_logs/` (L2222), `trans_rates.csv` (L2533, a COPY of `trans_rate_table_dir/.../<scen>-<label>-trans_rates-<year_ant>.csv`), `intervention_prob_deltas_<scen>_<region>_<year>.csv`, `posterior.tif`. True inputs live outside SIM_OUT: initial LULC in `aggregated_lulc_dir` (inputs/lulc/aggregated), rate tables in `trans_rate_table_dir` (outputs/transition_tables/simulation-lulc-areas-scalar-9.0x/<SCEN>/<label>/), masks in `spat_prob_perturb_dir`, models in `transition_model_dir`, params in `simulation_param_dir` (outputs/allocation_parameters/simulation). The only cross-step dependency inside a scenario tree is the previous posterior (the chain), which is exactly what must be archived.

Smoke anterior: with ALLOCATION_YEAR_POST_FILTER set, `filter_allocation_timesteps()` (allocation.r L930) keeps one pair and resume is disabled (L2068-2072), so `current_lulc_path` stays the initial 2022 LULC. For 2024 that is the correct anterior; the smoke reads nothing from any archived tree.

Smoke env vars (scripts/submit_allocation_smoke.sh L71-80; read by scripts/run_allocation.r L180-201 and src/allocation.r): `ALLOCATION_PROFILE_SCENARIO` (default BAU), `ALLOCATION_REGION_FILTER` (default costa_peruana), `ALLOCATION_YEAR_POST_FILTER` (default start+step = 2026, NOT a posterior year — must be set). `#SBATCH --exclusive --mem=0 --cpus-per-task=4 --time=12:00:00`, stdout `logs/lulc-allocation-smoke-%j.out`. Its header comment (L3-7, L16-25) about Rundeck reservation and "No --partition" is OUTDATED.

Verifier batch wrapper (EXISTS, use it — do not write a new one and do not use `sbatch --wrap`): `scripts/submit_verify_intervention_smoke.sh` (job-name iv-verify, 48G, 2 cpus, compute partition is fine, stdout `logs/iv-verify-%j.out`), forwards all args to `scripts/verify_intervention_smoke.r`. Verifier flags: `--year` (required), `--scenario`, `--region`, `--output-root`, `--mask-dir`, `--interventions-dir`, `--extra-log`. Exit 0 PASS / 1 FAIL / 2 usage. PASS banner: `PASS verify_intervention_smoke scenario=%s region=%s year=%d interventions=%d maps_checked=%d telemetry_rows=%d`.

Job names that touch SIM_OUT: `alloc-region` (submit_allocation_region.sh), `lulc-allocation-smoke`, `assemble-mosaic`, `iv-verify`, `wr04-probe`.

AUDIT line format (src/implement_spatial_interventions.R L945): `AUDIT stage=intervention region=<label> scenario=<s> year=<y> id=<id> rank=<n> type=<t> zone=<z> to_vals=<v,..> mask=<f> rows_target=<n> rows_changed=<n> delta_mean=.. delta_med=.. delta_sd=.. delta_min=.. delta_max=.. n_inc=<n> n_dec=<n> sum_abs_delta=<x>`; summary line `AUDIT stage=intervention_summary ... n_interventions=<n> cells_sum_gt1=<n>`.

relative_prob_adjust() (L1216-1460) branch log lines, per target class, format `to_val=<v>: because the Prob_adjust_valency is <V> and the percentage difference is ...`:
- Increase, Perc_diff >= 0: "...>0 then increasing the probability of the intervention pixels" (n_inc > 0)
- Increase, Perc_diff < 0: "...<0 then decreasing the probability of the non-intervention pixels" (n_dec > 0)
- Decrease, Perc_diff >= 0: "...>0 then decreasing the probability of the intervention pixels" (n_dec > 0)
- Decrease, Perc_diff < 0: "...<0 then increasing the probability of the non-intervention pixels" (n_inc > 0)
- skips: "intervention skip: to_val=<v> one zone has no rows", "intervention skip: to_val=<v> no positive probabilities in one zone"
- threshold: "The Percentage difference is below the threshold for <v>, setting to threshold value: <x>"
- Only rows >= the zone's 90th percentile of positive probs are adjusted, by Perc_diff percent (|Perc_diff| >= threshold 5). rows_target counts target rows in BOTH zones.

Telemetry CSV columns (verifier L570-576, 25): scenario, region, year, intervention_id, rank, type, zone, mask, target_class, n_target, n_changed, mean_before, mean_after, sd_before, sd_after, p05_delta..p95_delta, min_delta, max_delta, sum_abs_delta, prob_mass_before, prob_mass_after. One row per (intervention, target class), skipped classes included.

LULC values (config/lulc_schema.json): 101 forested_areas, 102 natural_grasslands_and_shrublands, 103 low_intensity_agricultural_areas, 104 high_intensity_agricultural_areas, 105 built_up_and_barren_lands, 106 mining, 107 water_body.

CUL interventions at 2024 (config/CUL_interventions.yml), all 5 active:
1. Conservation_expansion_and_preservation: Absolute 0, Inside, mask protected_areas_mask_CUL_phase0_current.tif, targets 105,104,106 (3 telemetry rows) — the ONLY Absolute-0 intervention in CUL.
2. Mining_outside_restraint: Relative Decrease Outside, mining_concessions_mask.tif, target 106 (1 row) — NEW combination.
3. Indigenous_land_OECM_forest: Relative Decrease Inside, indigenous_lands_mask.tif, targets 105,104,106, From 101 (3 rows) — same combo as NAT, already live.
4. Indigenous_land_OECM_ag: Relative Decrease Inside, indigenous_lands_mask.tif, target 104, From 103 (1 row) — already live.
5. Urban_densification: Relative Increase Inside, urban_settlement_mask.tif, target 105 (1 row) — NEW combination.
Expected: interventions=5, 5 AUDIT intervention lines + 1 summary, telemetry_rows=9.

Verifier Absolute-0 ledger (verify_intervention_smoke.r L297-560): applies to Absolute-0 interventions only, i.e. only Conservation_expansion_and_preservation for CUL. It FAILs if the region's trans_rates.csv has no To* row in {104,105,106} (GAP-1 ledger), or if rows exist but no map holds a non-NA cell inside the CUL phase0 PA mask (per-intervention zero maps), or overall maps_checked == 0. The Outside non-degeneracy check (CR-01) only runs for Absolute-0 Outside interventions, so it does NOT run for CUL (Mining_outside_restraint is Relative); mining_concessions_mask.tif was already proven 1-coded over costa_peruana in 05.1 (NAT job 841577 tree). Reference point: NAT x costa_peruana x 2032 passed with 12 Conservation Inside maps + 4 Mining Outside maps = maps_checked=16 (different scenario rate table, different PA mask), so CUL Conservation target rows in costa_peruana are expected but not guaranteed; the runbook adds a login02 text pre-check of the CUL rate CSV.

Rate CSV path: `/beegfs/black/nascent-lulcc/outputs/transition_tables/simulation-lulc-areas-scalar-9.0x/CUL/<region label>/CUL-<region label>-trans_rates-2022.csv`, header columns include `From*` and `To*` (allocation.r L1279-1290). The region label spelling is not known from the repo (regions.json lives on scratch), so commands must glob `CUL/*/CUL-*-trans_rates-2022.csv`.

Prior operator evidence (05.1-03-SUMMARY): code tree `/home/black/nascent-lulcc`, data root `/beegfs/black/nascent-lulcc` (not a repo), branch `spatial-interventions-integration`; NAT costa smoke job 841577 on highmem, sacct MaxRSS 88.5 GB, 2h11m; andes needs fat (192.8 GiB). Code-identity guard string from 05.1: `has no cell equal to 1` (2 hits in src/implement_spatial_interventions.R, 1 in src/allocation.r).

Shell test pattern: tests/shell/test-setup-environments-hpc-refusal.sh (pure bash, `set -euo pipefail`, PASS/FAIL counters, mktemp fixtures).
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Guarded archive script plus fixture test</name>
  <files>scripts/archive_allocation_outputs.sh, tests/shell/test-archive-allocation-outputs.sh</files>
  <behavior>
    - Library mode: sourcing the script defines functions and does not run main (guard `[ "${BASH_SOURCE[0]}" = "$0" ]`, same pattern as hpc_common.sh L242).
    - validate_sim_out rejects: empty string, "/", a relative path, a path that is not an existing directory, a path not under "$HPC_SCRATCH_ROOT/", and an empty or "/" HPC_SCRATCH_ROOT. It accepts "$HPC_SCRATCH_ROOT/outputs/simulations" when that dir exists.
    - Dry run over a fixture tree with NAT (2 years x 2 regions posteriors + 2 national mosaics + run_manifest.csv), CUL (1 posterior), BAU absent, SOC absent, plus an unrelated top-level entry: prints per-scenario counts (NAT region posteriors=4, mosaics=2; CUL 1/0; BAU and SOC "absent"), lists the unrelated entry as left in place, and changes nothing on disk (a before/after `find | sort` listing is identical).
    - Apply over the same fixture: NAT and CUL trees end up at "$SIM_OUT/_archive_<tag>/NAT" and ".../CUL" with identical file listings to before; "$SIM_OUT/NAT" and "$SIM_OUT/CUL" no longer exist; no path matching "$SIM_OUT/{BAU,NAT,CUL,SOC}/*/region_*/posterior.tif" exists; the unrelated entry is untouched; ARCHIVE_MANIFEST.tsv exists in the archive dir with one header and one row per moved scenario.
    - Apply refuses (non-zero, nothing moved) when "$SIM_OUT/_archive_<tag>/<scenario>" already exists.
    - Apply refuses when the running-jobs check reports a matching job (the test overrides the job-listing function after sourcing).
    - Invalid --tag (e.g. containing "/" or "..") and an unknown scenario name are rejected by the argument validators.
  </behavior>
  <action>
Create `scripts/archive_allocation_outputs.sh` (bash, `set -uo pipefail` like the launcher, executable bit via `git update-index --chmod=+x` since the workstation is Windows). Header comment must state: run on login02 from the code tree `/home/black/nascent-lulcc` after `source .env`; it is NOT an sbatch job; dry run is the default; why it exists (launcher file-presence skip at submit_allocation_scenario.sh L174-180 plus driver resume would treat pre-intervention posteriors as finished or chain off them); what it never does (no terra, no raster reads, no copy, no delete — rename only); and that the only R process is the launcher-identical, yaml-only `get_config()` probe, which `submit_allocation_scenario.sh` already runs on login02 on every sweep.

Structure as functions so the test can call them in library mode:
- `resolve_sim_out`: mirror submit_allocation_scenario.sh L39-42, L101-107, L125-134 exactly (export PROJECT_ROOT; setup_common_env, activate_env "$ENV_BASE_PATH/allocation_env", verify_rscript, each failing closed; the probe string `setwd(Sys.getenv('PROJECT_ROOT')); source('src/setup.r'); cfg <- get_config(); cat(cfg[['simulation_output_dir']], '\n')` with `--vanilla` and `2>/dev/null`; first line; trim via xargs). Comment that it is a deliberate copy of the launcher probe (do not modify the launcher in this task — its skip logic and probe stay untouched per the task constraints).
- `validate_sim_out <path>`: the refusals listed in the behavior block, each with a specific ERROR message on stderr and return 1. Use string prefix matching against "${HPC_SCRATCH_ROOT%/}/".
- `validate_tag <tag>`: must match ^[A-Za-z0-9._-]+$ and must not contain "..". Default tag `pre_interventions_$(date +%Y%m%d)`; archive root is "$SIM_OUT/_archive_$TAG" (leading underscore + `_archive_` prefix cannot equal any of BAU/NAT/CUL/SOC, so the launcher pattern `$SIM_OUT/$SCENARIO/...` cannot match it).
- Scenario list: default "BAU NAT CUL SOC", overridable with `--scenarios "NAT CUL"`; each must be one of those four (reject anything else, since the name becomes a path component).
- `running_allocation_jobs`: if `squeue` exists, print `squeue -h -u "$USER" -o '%i %j %T'` lines whose job name is exactly one of alloc-region, lulc-allocation-smoke, assemble-mosaic, iv-verify, wr04-probe; if squeue is absent print nothing and emit a WARNING to stderr.
- `report_scenario <sim_out> <scenario> <archive_root>`: if "$sim_out/$scenario" is absent print "[<s>] absent - nothing to move"; if it is a symlink refuse; otherwise count region posteriors with `find "$d" -mindepth 3 -maxdepth 3 -path '*/region_*/posterior.tif'` and national mosaics with `find "$d" -mindepth 2 -maxdepth 2 -name 'posterior_*.tif'`, print both, a per-region-dir breakdown (`awk -F/` on the parent dir name, `sort | uniq -c`), the list of posterior years present, and "would move: SRC -> DST". Counting is metadata only (find), never du or file reads.
- `run_archive <sim_out> <mode dry-run|apply> <tag> <scenarios...>`: print SIM_OUT, archive root, mode; list top-level entries of SIM_OUT that are not in the scenario list as "left in place"; call report_scenario for each; in dry-run also print any running_allocation_jobs lines as a WARNING. In apply mode: refuse (return 1, nothing moved) if running_allocation_jobs prints anything; pre-check every present scenario's target "$ARCHIVE_ROOT/<s>" does not exist BEFORE moving any (`[ -e ]` or `[ -L ]`), refuse otherwise; `mkdir -p` the archive root; for each present scenario check `stat -c %d` of source equals that of the archive root (refuse on mismatch — guarantees rename, never copy); `mv -T -- "$src" "$dst"`; on mv failure stop immediately with an ERROR naming what has and has not moved. Append to "$ARCHIVE_ROOT/ARCHIVE_MANIFEST.tsv" (header written only when the file is new): ISO timestamp, $USER, hostname, `git -C "$PROJECT_ROOT" rev-parse --short HEAD` (or "unknown"), scenario, src, dst, region_posteriors, national_mosaics. End with a greppable line `STATE archive_allocation_outputs mode=<mode> sim_out=<path> archive=<path> moved=<n>`.
- `main`: parse `--apply`, `--tag X`, `--scenarios "..."`, `-h/--help` (unknown flag -> exit 2); source hpc_common.sh; run `check_stage7_contract` and exit 1 if it fails; also refuse if "$HPC_SCRATCH_ROOT" does not end in `/nascent-lulcc` with a message that a stale root like /beegfs/black resolves the wrong tree (warn-and-refuse; allow override only via `ARCHIVE_ALLOW_ANY_SCRATCH_ROOT=1` documented in --help); resolve_sim_out; validate_sim_out; validate_tag; run_archive.

Then create `tests/shell/test-archive-allocation-outputs.sh` following tests/shell/test-setup-environments-hpc-refusal.sh style: pure bash, mktemp fixture root used as HPC_SCRATCH_ROOT with `outputs/simulations` beneath it, empty files touched as fake .tif (no real rasters), source the script in library mode, override `running_allocation_jobs` with a stub function per case, and assert every behavior bullet above with PASS/FAIL counters; exit non-zero on any FAIL; clean up the fixture with a trap. Do not call main or resolve_sim_out in the test (they need micromamba/R).

Before writing the script, confirm the "no input lives inside SIM_OUT/<scenario>" finding by grepping src/allocation.r for reads under sim_dir / timestep_dir / region_work_dir (the interfaces block lists what was found); record the confirmation in the SUMMARY under a heading "Inputs inside the archived trees" — expected conclusion: nothing needs preserving, everything there is regenerated, and the previous-year posterior is the stale chain input being archived on purpose.
  </action>
  <verify>
    <automated>bash -n scripts/archive_allocation_outputs.sh && bash -n tests/shell/test-archive-allocation-outputs.sh && bash tests/shell/test-archive-allocation-outputs.sh && test "$(grep -v '^[[:space:]]*#' scripts/archive_allocation_outputs.sh | grep -cE 'terra|rm -r|rm -f|cp -r|rsync|gdal')" -eq 0 && test "$(grep -v '^[[:space:]]*#' scripts/archive_allocation_outputs.sh | grep -c "cfg\[\['simulation_output_dir'\]\]")" -ge 1 && git diff --quiet -- scripts/submit_allocation_scenario.sh src/allocation.r src/implement_spatial_interventions.R</automated>
  </verify>
  <done>Both files pass `bash -n`; the fixture test passes every behavior case; the script contains no terra/rm/cp/rsync/gdal in non-comment lines; the launcher and both src files are unmodified; the script is committed with the executable bit; the SUMMARY records the inputs-inside-trees finding.</done>
</task>

<task type="auto">
  <name>Task 2: Operator runbook for archive plus CUL x costa_peruana x 2024 smoke, and README_HPC subsection</name>
  <files>.planning/quick/261014-dex-archive-pre-intervention-allocation-outp/261014-dex-OPERATOR.md, docs/README_HPC.md</files>
  <action>
Write `261014-dex-OPERATOR.md`. Rules (standing user preferences): group steps by machine, each group opening with an explicit "Where:" line (workstation Git Bash / login02 shell / compute node via sbatch); literal commands with real values (`/home/black/nascent-lulcc`, `/beegfs/black/nascent-lulcc`, `/beegfs/black/nascent-lulcc/outputs/simulations`, branch `spatial-interventions-integration`, tag `pre_interventions_phase5`); NO angle-bracket placeholders anywhere — runtime values are captured into shell variables and also saved to a file so a new shell can recover them (e.g. `JOB=$(sbatch --parsable ...)` then `echo "$JOB" > logs/cul-smoke-2024.jobid`, later `JOB=$(cat logs/cul-smoke-2024.jobid)`); one line explaining what each non-obvious flag prevents. Commands go in fenced bash blocks in the runbook (the no-fence rule applies to this plan's action text, not to the runbook).

Open with an "Outdated guidance - read first" box: (a) Rundeck node reservation is NOT needed for sbatch; the header comment of scripts/submit_allocation_smoke.sh (L3-7, L16-25: "No --partition: the Rundeck reservation places the job") is outdated — sbatch placement is cluster-wide; (b) bare sbatch lands on the 93 GB compute partition and OOMs — always `--partition=highmem` for costa_peruana (NAT costa smoke 841577 peaked at sacct MaxRSS 88.5 GB, 2h11m); (c) docs/README_HPC.md shows the verifier as a bare interactive `Rscript` call — outdated; use `sbatch scripts/submit_verify_intervention_smoke.sh`; never run Rscript/terra interactively on login02 (the HPC manager emails warnings); (d) `/beegfs/black/nascent-lulcc` is the data root and not a git repo — never `git pull` there.

Then the ordered steps:
0. Where: workstation. `git push origin spatial-interventions-integration`, plus `git log --oneline -1` to note the hash to compare in step 3.
1. Where: login02. `cd /home/black/nascent-lulcc && git pull` and `git status --short` (sbatch runs the SUBMIT DIR's code via SLURM_SUBMIT_DIR).
2. Where: login02. `source .env`, `echo "$HPC_SCRATCH_ROOT"` must print exactly `/beegfs/black/nascent-lulcc` (a stale `/beegfs/black` passes the contract yet resolves the wrong tree — worse than unset), then `bash scripts/hpc_common.sh --check-stage7-contract` expecting `Stage 7 path contract OK.`.
3. Where: login02. Prove code identity BEFORE submitting anything: `git log --oneline -1` must equal the workstation hash from step 0; `grep -c "STATE archive_allocation_outputs" scripts/archive_allocation_outputs.sh` must print 1 or more (string introduced by this task); `grep -c "has no cell equal to 1" src/implement_spatial_interventions.R` must print 2 (05.1 guard). Any mismatch: stop.
4. Where: login02. `squeue -u black` must show no alloc-region / lulc-allocation-smoke / assemble-mosaic / iv-verify / wr04-probe jobs. Dry run: `bash scripts/archive_allocation_outputs.sh --tag pre_interventions_phase5 2>&1 | tee logs/archive-dryrun-pre_interventions_phase5.log`. Say what to read: SIM_OUT line must be `/beegfs/black/nascent-lulcc/outputs/simulations`; NAT should show at least 40 region posteriors (the complete Phase 3.6 run, 4 regions x 10 years; costa_peruana and andes may carry the 2032 intervention-smoke posteriors which overwrote the 3.6 ones in place); CUL and SOC partial counts across posterior years for costa_peruana and andes (from the 05.1 WR-04 probe sweep); if NAT shows 0 the root is wrong — stop. Note the script runs one yaml-only get_config probe (the same one the sweep launcher runs on login02) and only `find`/`mv`. Then apply: same command with `--apply`, tee to `logs/archive-apply-pre_interventions_phase5.log`; confirm with `ls /beegfs/black/nascent-lulcc/outputs/simulations/_archive_pre_interventions_phase5` and `cat .../ARCHIVE_MANIFEST.tsv`, and `find /beegfs/black/nascent-lulcc/outputs/simulations -mindepth 4 -maxdepth 4 -path '*/region_*/posterior.tif' -not -path '*/_archive_*' | wc -l` must print 0. State that archived trees (including the NAT 2032 smoke outputs that backed 05.1 evidence) are renamed, not deleted, and can be renamed back with `mv -T`.
5. Where: login02 (text-only pre-checks), then submit. Pre-checks: `ls -l` the four CUL masks needed at 2024 under `/beegfs/black/nascent-lulcc/inputs/spat_prob_perturb/` (protected_areas_mask_CUL_phase0_current.tif, mining_concessions_mask.tif, indigenous_lands_mask.tif, urban_settlement_mask.tif); and an awk one-liner over `/beegfs/black/nascent-lulcc/outputs/transition_tables/simulation-lulc-areas-scalar-9.0x/CUL/*/CUL-*-trans_rates-2022.csv` that finds the `To*` column from the header (strip quotes) and prints, per file, the row counts for To* = 104, 105, 106 (text read of a small CSV; acceptable on login02). Interpretation table: costa_peruana file with 0 rows for all of 104/105/106 => the verifier WILL FAIL the Conservation_expansion_and_preservation ledger (GAP-1, "this run is not evidence for them") — that is a data outcome, not an engine defect, decide before submitting whether to proceed; 0 rows for 106 => Mining_outside_restraint cannot be exercised (rows_target=0) and the Relative/Decrease/Outside combo stays without live evidence; 0 rows for 105 => same for Urban_densification (Relative/Increase/Inside). Submit: `JOB=$(sbatch --parsable --partition=highmem --export=ALL,ALLOCATION_PROFILE_SCENARIO=CUL,ALLOCATION_REGION_FILTER=costa_peruana,ALLOCATION_YEAR_POST_FILTER=2024 scripts/submit_allocation_smoke.sh)`, save to `logs/cul-smoke-2024.jobid`. Flag notes: `--parsable` prints only the id; `--partition=highmem` prevents the 93 GB compute OOM (the script's `--exclusive --mem=0` then takes the whole ~188 GB node); `--export=ALL,...` keeps the sourced .env contract vars while adding the three filters; ALLOCATION_YEAR_POST_FILTER must be set because the script default 2026 is not a posterior year; 2024 is the first posterior year so the anterior is the initial 2022 LULC and nothing archived is read. Monitor with `squeue -j "$JOB"` and `tail -n 40 "logs/lulc-allocation-smoke-$JOB.out"`.
6. Where: login02 after the job leaves the queue. `sacct -j "$JOB" --format=JobID,State,Elapsed,MaxRSS,Partition` (expect COMPLETED, highmem, MaxRSS in the same range as 88.5 GB; use sacct MaxRSS, not the R PROFILE_MEM line). `grep -c "intervention mask: " "logs/lulc-allocation-smoke-$JOB.out"` must be 0. Then the verifier as a batch job on compute: `VJOB=$(sbatch --parsable scripts/submit_verify_intervention_smoke.sh --scenario CUL --region costa_peruana --year 2024 --extra-log "logs/lulc-allocation-smoke-$JOB.out")`, save to `logs/cul-verify-2024.jobid`; after it finishes `tail -n 60 "logs/iv-verify-$VJOB.out"` and `sacct -j "$VJOB" --format=JobID,State,ExitCode`. Text-only evidence extraction on login02: `grep -rh "AUDIT stage=intervention" /beegfs/black/nascent-lulcc/outputs/simulations/CUL/2024/region_costa_peruana --include='*.log'`; `grep -rhE "to_val=10[56]: because|intervention skip: to_val=10[56]|below the threshold for 10[56]|Percentage difference in average probability" ... --include='*.log'`; `column -s, -t` (or `cut -d, -f4,7,9-14,21-23`) on `/beegfs/black/nascent-lulcc/outputs/simulations/CUL/2024/region_costa_peruana/intervention_prob_deltas_CUL_costa_peruana_2024.csv`; `grep -c "Info: .* not counted" "logs/iv-verify-$VJOB.out"`.

Add an "Expected values and what would look wrong" section, written before the run:
- Verifier: PASS banner with `scenario=CUL region=costa_peruana year=2024 interventions=5`, `telemetry_rows=9` (3+1+3+1+1 target-class rows), `maps_checked` > 0 counting only Conservation_expansion_and_preservation zone=Inside maps (the only Absolute-0 CUL intervention; NAT's 12 Conservation maps at 2032 are a loose reference only — different rate table and PA mask); `Info: ... not counted` lines are acceptable. What the verifier does NOT assert for CUL: section (c) checks Absolute-0 zones only and the CR-01 Outside non-degeneracy check runs only for Absolute-0 Outside interventions, so the two new Relative combinations are judged only by sections (b) count/ids and (e) telemetry reconciliation (target classes, sum(n_changed)=rows_changed, sum(sum_abs_delta)=AUDIT). Direction and magnitude for Relative interventions are the operator's judgement from the AUDIT lines, branch log lines and CSV, per the next bullets.
- Mining_outside_restraint (Decrease, Outside; intervention zone = OUTSIDE concessions; to_vals=106): rows_target > 0 when the step-5 pre-check showed To*=106 rows; 0 < rows_changed < rows_target, expected roughly a tenth of one zone's rows (only rows at or above that zone's 90th percentile move); exactly one of n_inc / n_dec non-zero; the branch log line decides which: "...>0 then decreasing the probability of the intervention pixels" => n_dec > 0, n_inc = 0, delta_max <= 0; "...<0 then increasing the probability of the non-intervention pixels" (inside concessions) => n_inc > 0, n_dec = 0. So n_inc > 0 on a Decrease intervention is NOT wrong by itself. Adjusted cells move by at least about 5 percent of their own probability (threshold 5).
- Urban_densification (Increase, Inside; to_vals=105): same structure; ">0 then increasing the probability of the intervention pixels" => n_inc > 0, n_dec = 0, delta_min >= 0 inside; "<0 then decreasing the probability of the non-intervention pixels" => n_dec > 0, n_inc = 0. Negative deltas are wrong only if the log says the intervention (inside) pixels were being increased.
- Would look wrong (report, do not explain away): rows_changed = 0 while rows_target > 0 with no `intervention skip: to_val=...` line for that class; both n_inc > 0 and n_dec > 0 on a single-target-class Relative intervention; a delta sign contradicting its branch log line; rows_changed equal to rows_target (percentile selection did not bite); telemetry_rows not 9; interventions not 5; any `intervention mask: ` line in the smoke log; a `posterior.tif` missing. Legitimate non-evidence (record, not a failure of the engine): `intervention skip: to_val=106 one zone has no rows` or `... no positive probabilities in one zone` — the combination ran but was not exercised.
- Classify before touching code (05.1 rule): a guard firing on genuinely bad data is fixed offline in the data, never by weakening the guard.

Add a short "After the smoke" note: the CUL costa_peruana 2024 posterior stays in `$SIM_OUT/CUL`; the sweep launcher will submit that region (9 of 10 posteriors missing) and the driver will resume at 2028 chaining off it. That is valid (same code, anterior = initial 2022 LULC, exactly what step 1 of the sweep does). If every posterior should come from the sweep job itself, re-run the archive with `--scenarios CUL --tag post_cul_smoke_phase5 --apply` before the sweep. Do not launch the sweep in this runbook.

Close with "7. Paste back as evidence": step-3 hash and both grep counts; the dry-run per-scenario count block and the apply STATE line plus the manifest; the step-5 rate pre-check output for costa_peruana; the smoke sacct line; the `intervention mask:` grep count; the verifier PASS/FAIL banner with its Conservation `[ok]` lines; the five AUDIT intervention lines plus the summary line; the branch log lines for to_val=105 and 106; the CSV rows for Mining_outside_restraint and Urban_densification; the Info-line count.

Then edit `docs/README_HPC.md`: add a short subsection (around 10-20 lines) directly before the existing "**Intervention smoke.**" paragraph titled "Archiving outputs before a fresh sweep" that documents `scripts/archive_allocation_outputs.sh` (why: the launcher skips a region whose posteriors all exist and the driver resumes off existing posteriors, so outputs from older code must be moved out of `$SIM_OUT/<scenario>`; dry run default, `--apply`, `--tag`, `--scenarios`; rename-only to `$SIM_OUT/_archive_<tag>/<scenario>`, manifest; run on login02 from the code tree after `source .env`). Do not rewrite the outdated Rundeck/--partition or interactive-Rscript passages in this task (recorded 05.1 open items); the runbook calls them out instead.
  </action>
  <verify>
    <automated>F=.planning/quick/261014-dex-archive-pre-intervention-allocation-outp/261014-dex-OPERATOR.md; test -f "$F" && test "$(grep -cE '<[a-z_]+>' "$F")" -eq 0 && test "$(grep -c '^Where:\|^\*\*Where:' "$F")" -ge 5 && grep -q 'ALLOCATION_PROFILE_SCENARIO=CUL,ALLOCATION_REGION_FILTER=costa_peruana,ALLOCATION_YEAR_POST_FILTER=2024' "$F" && grep -q -- '--partition=highmem' "$F" && grep -q 'submit_verify_intervention_smoke.sh --scenario CUL --region costa_peruana --year 2024' "$F" && grep -q 'telemetry_rows=9' "$F" && grep -q 'has no cell equal to 1' "$F" && grep -q 'archive_allocation_outputs.sh' docs/README_HPC.md</automated>
  </verify>
  <done>The runbook exists with Where: lines for every machine group, no angle-bracket placeholders, the outdated-guidance box first, steps 0-7 in order with literal values, the CUL expected-values section (including the up-front Absolute-0 ledger statement and the Relative direction rules), and the evidence list; README_HPC has the archive subsection; both are committed.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| operator shell -> shared scratch (/beegfs) | the archive script renames multi-GB output trees on a shared parallel filesystem |
| config/env -> path construction | SIM_OUT comes from .env (HPC_SCRATCH_ROOT) + YAML via get_config(); a stale or empty value redirects every path |
| login02 -> HPC usage policy | compute on the login node triggers manager warnings |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-q261014-01 | Tampering | archive_allocation_outputs.sh path handling | mitigate | validate_sim_out refuses empty, "/", relative, non-directory and not-under-$HPC_SCRATCH_ROOT/ paths; HPC_SCRATCH_ROOT must end in /nascent-lulcc; scenario names restricted to BAU/NAT/CUL/SOC and tag to ^[A-Za-z0-9._-]+$ without ".." |
| T-q261014-02 | Tampering / data loss | mv of scenario trees | mitigate | dry run by default; --apply pre-checks every target is absent before moving any; mv -T rename only, no rm/cp (grep-gated in verify); same-device stat check guarantees rename not copy; manifest records every move for reversal |
| T-q261014-03 | Tampering (race) | trees in use by running jobs | mitigate | --apply refuses when squeue lists alloc-region / lulc-allocation-smoke / assemble-mosaic / iv-verify / wr04-probe jobs for $USER; runbook step 4 also checks squeue |
| T-q261014-04 | Repudiation | provenance of which code produced evidence | mitigate | runbook step 3 compares HEAD to the workstation hash and greps one string from this task and the 05.1 guard before any submission; manifest records code HEAD |
| T-q261014-05 | Denial of service (policy) | login02 compute | mitigate | script uses only find/mv/stat plus the yaml-only get_config probe the launcher already runs; verifier runs via the existing sbatch wrapper; smoke on --partition=highmem |
| T-q261014-06 | Information disclosure | runbook / manifest contents | accept | contains only cluster paths and the user's own username already present in repo docs; no secrets |
</threat_model>

<verification>
- `bash -n` on both new shell files; `bash tests/shell/test-archive-allocation-outputs.sh` exits 0.
- Non-comment lines of the archive script contain no terra/rm/cp/rsync/gdal; they contain the launcher-identical `cfg[['simulation_output_dir']]` probe.
- `git diff` shows no change to scripts/submit_allocation_scenario.sh, scripts/submit_allocation_smoke.sh, src/allocation.r, src/implement_spatial_interventions.R.
- Runbook: no `<...>` placeholders, Where: lines present, literal CUL smoke and verifier commands present.
- Commits use the trailer `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`; `.planning/HANDOFF.json` and the stray `lulc-allocation-smoke-841987.out` are NOT staged.
</verification>

<success_criteria>
- The operator can, from the runbook alone, archive all pre-intervention scenario trees on login02 with a reviewed dry run, then submit and verify a CUL x costa_peruana x 2024 intervention smoke as batch jobs.
- After the archive, no `$SIM_OUT/{BAU,NAT,CUL,SOC}/*/region_*/posterior.tif` from older code remains for the sweep launcher to skip on or the driver to chain from.
- Before the smoke runs, the operator knows whether the Absolute-0 ledger can FAIL for CUL in costa_peruana (via the rate-CSV pre-check) and what correct vs wrong evidence looks like for Mining_outside_restraint and Urban_densification.
</success_criteria>

<output>
Create `.planning/quick/261014-dex-archive-pre-intervention-allocation-outp/261014-dex-SUMMARY.md` when done. It must include the "Inputs inside the archived trees" finding, the test output, and a list of the HPC steps still pending with the operator.
</output>
