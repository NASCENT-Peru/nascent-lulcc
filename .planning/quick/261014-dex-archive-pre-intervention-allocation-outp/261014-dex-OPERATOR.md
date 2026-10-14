# 261014-dex operator runbook: archive pre-intervention outputs, then CUL x costa_peruana x 2024 smoke

Two things have to happen before the Phase 4 full sweep
(`bash scripts/submit_allocation_all_scenarios.sh`):

1. Move every existing `/beegfs/black/nascent-lulcc/outputs/simulations/BAU|NAT|CUL|SOC` tree out of
   the way. The sweep launcher skips any region whose posteriors all exist, and the driver resumes a
   region from its existing posteriors. Posteriors from older code would be treated as finished work
   or used as the starting map for the next timestep.
2. Run CUL once in costa_peruana for 2024 and verify it. Two intervention combinations exist only in
   CUL and have never run live: Relative/Decrease/Outside (Mining_outside_restraint) and
   Relative/Increase/Inside (Urban_densification).

This runbook does not launch the sweep.

---

## Outdated guidance (read this first)

> **(a) Rundeck reservation is not needed.** The header comment of `scripts/submit_allocation_smoke.sh`
> (lines 3-7 and 16-25: "No --partition: the Rundeck reservation places the job") is out of date.
> `sbatch` places jobs cluster-wide, so you do not reserve a node first.
>
> **(b) Always pass `--partition=highmem` for costa_peruana.** A bare `sbatch` lands on the 93 GB
> `compute` partition and runs out of memory. The NAT costa_peruana smoke (job 841577) peaked at
> 88.5 GB (sacct MaxRSS) and took 2h11m on highmem.
>
> **(c) Do not run the verifier with a bare `Rscript` on login02.** `docs/README_HPC.md` still shows
> `Rscript scripts/verify_intervention_smoke.r ...` as an interactive command. That is out of date.
> Submit it with `sbatch scripts/submit_verify_intervention_smoke.sh ...` (step 6). Never run
> Rscript or terra work interactively on login02: the HPC manager sends warning emails.
>
> **(d) `/beegfs/black/nascent-lulcc` is the data root, not a git repository.** Never run `git pull`
> there. The code lives in `/home/black/nascent-lulcc`, and `sbatch` runs the code of the directory
> you submit from.

---

## Step 0: push the code

**Where:** Windows workstation, Git Bash, in the repo checkout.

```bash
git push origin spatial-interventions-integration
git log --oneline -1
```

Write down the 7-character hash from `git log`. Step 3 compares it with the HPC checkout.

---

## Step 1: update the HPC code tree

**Where:** login02 shell.

```bash
cd /home/black/nascent-lulcc
git pull
git status --short
```

`git status --short` should print nothing, or only files you know about. Every later command runs
from `/home/black/nascent-lulcc`, because a job runs the code in the directory it was submitted from
(SLURM_SUBMIT_DIR).

---

## Step 2: load and check the path contract

**Where:** login02 shell, in `/home/black/nascent-lulcc`.

```bash
source .env
echo "$HPC_SCRATCH_ROOT"
bash scripts/hpc_common.sh --check-stage7-contract
```

- `echo` must print exactly `/beegfs/black/nascent-lulcc`. A stale `/beegfs/black` passes the contract
  check but points every path at the wrong tree, which is worse than leaving it unset. If it is wrong,
  fix `.env` and run `source .env` again.
- The contract check must print `Stage 7 path contract OK.`

Every new login02 shell needs `source .env` again before any `sbatch` or before the archive script.

---

## Step 3: prove the code is the code you pushed (before submitting anything)

**Where:** login02 shell, in `/home/black/nascent-lulcc`.

```bash
git log --oneline -1
grep -c "STATE archive_allocation_outputs" scripts/archive_allocation_outputs.sh
grep -c "has no cell equal to 1" src/implement_spatial_interventions.R
```

| Check | Must print |
|-------|------------|
| `git log --oneline -1` | the same hash you noted in step 0 |
| `grep -c "STATE archive_allocation_outputs"` | 1 or more (added by this task) |
| `grep -c "has no cell equal to 1"` | 2 (the mask guard from phase 05.1) |

If any of these does not match, stop. Do not archive or submit with code you have not identified.

---

## Step 4: archive the pre-intervention output trees

**Where:** login02 shell, in `/home/black/nascent-lulcc`, after `source .env`. The archive script is
not an sbatch job. It runs one short R process (the same `get_config()` probe the sweep launcher runs
on login02, which only reads YAML) and otherwise only uses `find`, `stat` and `mv`. It never opens a
raster, never copies and never deletes.

### 4a. Make sure no allocation job is running

```bash
squeue -u black
```

The list must contain no `alloc-region`, `lulc-allocation-smoke`, `assemble-mosaic`, `iv-verify`,
`wr04-probe`, `lulc-allocation`, `lulc-allocation-profile` or `lulc-dinamica-only` job. (`--apply`
also checks this and refuses if any are queued or running.)

### 4b. Dry run (moves nothing)

```bash
mkdir -p logs
bash scripts/archive_allocation_outputs.sh --tag pre_interventions_phase5 2>&1 | tee logs/archive-dryrun-pre_interventions_phase5.log
```

Read the output:

- `SIM_OUT:` must be `/beegfs/black/nascent-lulcc/outputs/simulations`.
- `Archive:` must be `/beegfs/black/nascent-lulcc/outputs/simulations/_archive_pre_interventions_phase5`.
- `[NAT] region posteriors:` should be at least 40. That is the complete Phase 3.6 run: 4 regions x
  10 posterior years. In costa_peruana and andes the 2032 posteriors were overwritten in place by the
  05.1 intervention smokes, so the count stays the same.
- `[CUL]` and `[SOC]` show partial counts for costa_peruana and andes, left by the 05.1 WR-04 probe
  sweep. `[BAU]` may also be present.
- **If NAT shows 0 or `absent`, the root is wrong. Stop and recheck step 2.**
- Any `left in place:` lines are entries the script will not touch (other folders, earlier archives).
- A `WARNING: these jobs are queued or running` block means `--apply` will refuse. Wait for those jobs
  to finish.

### 4c. Apply

```bash
bash scripts/archive_allocation_outputs.sh --tag pre_interventions_phase5 --apply 2>&1 | tee logs/archive-apply-pre_interventions_phase5.log
```

- `--apply` performs the renames. Without it, nothing moves.
- `--tag pre_interventions_phase5` names the archive folder. If the folder already holds a scenario of
  the same name, the script refuses and moves nothing. It never merges or overwrites.

The last two lines must read

```text
Region posteriors still under $SIM_OUT/{BAU NAT CUL SOC}: 0
STATE archive_allocation_outputs mode=apply sim_out=/beegfs/black/nascent-lulcc/outputs/simulations archive=/beegfs/black/nascent-lulcc/outputs/simulations/_archive_pre_interventions_phase5 moved=N
```

where N is the number of scenarios the dry run listed as present (not `absent`). Any `ERROR:` line
means the script stopped; it prints what has and has not moved.

Confirm:

```bash
ls /beegfs/black/nascent-lulcc/outputs/simulations/_archive_pre_interventions_phase5
cat /beegfs/black/nascent-lulcc/outputs/simulations/_archive_pre_interventions_phase5/ARCHIVE_MANIFEST.tsv
find /beegfs/black/nascent-lulcc/outputs/simulations -mindepth 4 -maxdepth 4 -path '*/region_*/posterior.tif' -not -path '*/_archive_*' | wc -l
```

The `find ... | wc -l` must print `0`.

The archived trees, including the NAT 2032 smoke outputs behind the 05.1 evidence, are renamed, not
deleted. To put one back (only if you mean to), for example NAT:

```bash
mv -T /beegfs/black/nascent-lulcc/outputs/simulations/_archive_pre_interventions_phase5/NAT /beegfs/black/nascent-lulcc/outputs/simulations/NAT
```

(`-T` treats the destination as the final name, so `mv` cannot drop NAT inside an existing NAT folder.)

---

## Step 5: CUL pre-checks, then submit the smoke

**Where:** login02 shell, in `/home/black/nascent-lulcc`, after `source .env`. The pre-checks only
list files and read one small CSV as text. They are fine on login02.

### 5a. The four CUL masks used at 2024 exist

```bash
ls -l /beegfs/black/nascent-lulcc/inputs/spat_prob_perturb/protected_areas_mask_CUL_phase0_current.tif \
      /beegfs/black/nascent-lulcc/inputs/spat_prob_perturb/mining_concessions_mask.tif \
      /beegfs/black/nascent-lulcc/inputs/spat_prob_perturb/indigenous_lands_mask.tif \
      /beegfs/black/nascent-lulcc/inputs/spat_prob_perturb/urban_settlement_mask.tif
```

All four must be listed with a non-zero size. A missing mask stops the run at the pre-flight check
(`intervention mask: missing ...`), so fix that before submitting.

### 5b. Which target classes have transitions in the CUL 2022 rate tables

The run keeps only rows where `From*` differs from `To*` and `Rate` is not 0. This check applies the
same filter and counts the remaining rows whose `To*` is 104 (high-intensity agriculture),
105 (built-up and barren) or 106 (mining), per region file. The folder name of each region is not
recorded in the repo, so the command matches every CUL region folder:

```bash
for f in /beegfs/black/nascent-lulcc/outputs/transition_tables/simulation-lulc-areas-scalar-9.0x/CUL/*/CUL-*-trans_rates-2022.csv; do
  awk -F, -v file="$f" '
    NR == 1 { for (i = 1; i <= NF; i++) { h = $i; gsub(/"/, "", h); col[h] = i } next }
    {
      fr = $col["From*"]; to = $col["To*"]; rt = $col["Rate"]
      gsub(/"/, "", fr); gsub(/"/, "", to); gsub(/"/, "", rt)
      if (fr != to && rt + 0 != 0) n[to]++
    }
    END {
      if (!("To*" in col) || !("From*" in col) || !("Rate" in col)) { print file ": header lacks From*/To*/Rate"; exit }
      printf "%s  To104=%d To105=%d To106=%d\n", file, n["104"], n["105"], n["106"]
    }' "$f"
done 2>&1 | tee logs/cul-rate-precheck-2022.log
```

Read the line whose path contains the costa_peruana folder (spelled however the folder is named,
for example `Costa Peruana` or `costa_peruana`):

| costa_peruana result | What it means for this smoke |
|----------------------|------------------------------|
| To104, To105, To106 all 0 | The verifier **will FAIL** the Conservation_expansion_and_preservation ledger ("this run is not evidence for them"). That is a data outcome, not an engine defect. Decide before submitting whether a run is still worth it. |
| To106 = 0 | Mining_outside_restraint cannot change anything (rows_target=0). Relative/Decrease/Outside stays without live evidence. |
| To105 = 0 | Urban_densification cannot change anything. Relative/Increase/Inside stays without live evidence. |
| To105 > 0 and To106 > 0 | Both new combinations can be exercised. Proceed. |

### 5c. Submit the smoke

```bash
JOB=$(sbatch --parsable --partition=highmem --export=ALL,ALLOCATION_PROFILE_SCENARIO=CUL,ALLOCATION_REGION_FILTER=costa_peruana,ALLOCATION_YEAR_POST_FILTER=2024 scripts/submit_allocation_smoke.sh)
echo "$JOB" > logs/cul-smoke-2024.jobid
echo "$JOB"
```

What the flags do:

- `--parsable` makes `sbatch` print only the job id, so `JOB` holds a clean number.
- `--partition=highmem` prevents the 93 GB `compute` OOM. The script's own `--exclusive --mem=0` then
  takes the whole highmem node (about 188 GB).
- `--export=ALL,...` keeps the contract variables from `source .env` and adds the three filters.
  Without `ALL`, the job would see only the three filters and fail the contract check in seconds.
- `ALLOCATION_YEAR_POST_FILTER=2024` must be set. The script's default (2026) is not a posterior year.
  2024 is the first posterior year, so its starting map is the initial 2022 LULC and nothing from the
  archived trees is read.
- The job id is saved to `logs/cul-smoke-2024.jobid`. In a new shell, recover it with
  `JOB=$(cat logs/cul-smoke-2024.jobid)`.

Monitor:

```bash
squeue -j "$JOB"
tail -n 40 "logs/lulc-allocation-smoke-$JOB.out"
```

Expect roughly two hours (NAT took 2h11m).

---

## Step 6: check the smoke, run the verifier, collect evidence

**Where:** login02 shell, in `/home/black/nascent-lulcc`, after the smoke job has left the queue.
The verifier itself runs on a compute node through sbatch.

### 6a. Job accounting and the mask marker

```bash
cd /home/black/nascent-lulcc
source .env
JOB=$(cat logs/cul-smoke-2024.jobid)
sacct -j "$JOB" --format=JobID,State,Elapsed,MaxRSS,Partition
grep -c "intervention mask: " "logs/lulc-allocation-smoke-$JOB.out"
```

- `sacct` should show `COMPLETED` on `highmem`, with MaxRSS in the same range as NAT's 88.5 GB. Use the
  sacct MaxRSS figure, not the `PROFILE_MEM` line R prints.
- The `grep -c` must print `0`.

### 6b. Submit the verifier

```bash
VJOB=$(sbatch --parsable scripts/submit_verify_intervention_smoke.sh --scenario CUL --region costa_peruana --year 2024 --extra-log "logs/lulc-allocation-smoke-$JOB.out")
echo "$VJOB" > logs/cul-verify-2024.jobid
echo "$VJOB"
```

No `--partition`: the verifier needs 48 GB and 2 CPUs, which fits the `compute` partition.
`--extra-log` makes the verifier also scan the smoke job's stdout for forbidden error markers. In a
new shell, recover the id with `VJOB=$(cat logs/cul-verify-2024.jobid)`.

After it finishes:

```bash
tail -n 60 "logs/iv-verify-$VJOB.out"
sacct -j "$VJOB" --format=JobID,State,ExitCode
grep -c "Info: .* not counted" "logs/iv-verify-$VJOB.out"
```

### 6c. Text-only evidence extraction

```bash
grep -rh "AUDIT stage=intervention" /beegfs/black/nascent-lulcc/outputs/simulations/CUL/2024/region_costa_peruana --include='*.log'
grep -rhE "to_val=10[56]: because|intervention skip: to_val=10[56]|below the threshold for 10[56]|Percentage difference in average probability" /beegfs/black/nascent-lulcc/outputs/simulations/CUL/2024/region_costa_peruana --include='*.log'
cut -d, -f4,7,9,10,11,12,13,21,22,23 /beegfs/black/nascent-lulcc/outputs/simulations/CUL/2024/region_costa_peruana/intervention_prob_deltas_CUL_costa_peruana_2024.csv | column -s, -t
ls -l /beegfs/black/nascent-lulcc/outputs/simulations/CUL/2024/region_costa_peruana/posterior.tif
```

The `cut` keeps: intervention_id, zone, target_class, n_target, n_changed, mean_before, mean_after,
min_delta, max_delta, sum_abs_delta.

---

## Expected values and what would look wrong (written before the run)

### Can the verifier FAIL for CUL in costa_peruana?

**Yes, in one way.** The Absolute-0 ledger applies only to Absolute-0 interventions. In CUL that is
only Conservation_expansion_and_preservation (zone Inside, mask
`protected_areas_mask_CUL_phase0_current.tif`, targets 105, 104, 106). The verifier FAILs if:

- the region's `trans_rates.csv` has no row whose `To*` is 104, 105 or 106 (the ledger: "this run is
  not evidence for them"). Step 5b answers this before you submit; or
- such rows exist but no probability map has a non-NA cell inside the CUL phase0 protected-area mask;
  or
- `maps_checked` is 0 overall.

The NAT costa_peruana 2032 run passed with 12 Conservation Inside maps plus 4 Mining Outside maps
(`maps_checked=16`). That is only a loose reference: NAT uses a different rate table and a different
protected-area mask. Conservation rows in CUL costa_peruana are expected but not guaranteed.

### Verifier result

- PASS banner containing `scenario=CUL region=costa_peruana year=2024 interventions=5` and
  `telemetry_rows=9` (3 + 1 + 3 + 1 + 1 target-class rows: Conservation 3, Mining 1, Indigenous forest
  3, Indigenous agriculture 1, Urban 1).
- `maps_checked` greater than 0. It counts only Conservation_expansion_and_preservation Inside maps,
  because that is the only Absolute-0 CUL intervention.
- `Info: ... not counted` lines are acceptable (a transition's from-class absent under the mask).

### What the verifier does not check for CUL

The raster assertions check Absolute-0 zones only, and the Outside non-degeneracy check (CR-01) runs
only for Absolute-0 Outside interventions. Mining_outside_restraint is Relative, so that check does
not run. (`mining_concessions_mask.tif` was already shown to be 1-coded over costa_peruana in 05.1,
from the NAT job 841577 tree.)

So the two new Relative combinations are judged by the verifier only on: the intervention count and
ids, and the telemetry reconciliation (target classes, sum of n_changed equals rows_changed, sum of
sum_abs_delta equals the AUDIT value). **Direction and magnitude are your judgement**, from the AUDIT
lines, the branch log lines and the CSV, using the rules below.

How the Relative adjustment works: for each target class, the engine compares the average probability
of the top 10 percent of cells (90th percentile and above) inside the intervention zone with the same
in the other zone, then logs one branch line saying which zone it adjusts. Only cells at or above that
zone's 90th percentile move, by the percentage difference (at least 5 percent, the threshold).
`rows_target` counts target-class rows in both zones together.

### Mining_outside_restraint (Relative, Decrease, zone Outside, target 106)

The intervention zone is outside the mining concessions.

- `rows_target` > 0 when step 5b showed To106 > 0.
- `rows_changed` > 0 and smaller than `rows_target`, roughly a tenth of one zone's rows.
- Exactly one of `n_inc` / `n_dec` is non-zero. The branch line for `to_val=106` decides which:
  - `...>0 then decreasing the probability of the intervention pixels` means outside cells were
    lowered: `n_dec` > 0, `n_inc` = 0, `delta_max` <= 0.
  - `...<0 then increasing the probability of the non-intervention pixels` means cells inside the
    concessions were raised: `n_inc` > 0, `n_dec` = 0.
- So `n_inc` > 0 on a Decrease intervention is not wrong by itself. Check it against the branch line.
- Each adjusted cell moves by at least about 5 percent of its own probability.

### Urban_densification (Relative, Increase, zone Inside, target 105)

The intervention zone is inside the urban settlement mask.

- Same structure as Mining. The branch line for `to_val=105` decides the direction:
  - `...>0 then increasing the probability of the intervention pixels`: `n_inc` > 0, `n_dec` = 0,
    `delta_min` >= 0.
  - `...<0 then decreasing the probability of the non-intervention pixels`: `n_dec` > 0, `n_inc` = 0.
- Negative deltas are wrong only if the branch line says the inside cells were being increased.

### Would look wrong (report it, do not explain it away)

- `rows_changed` = 0 while `rows_target` > 0, with no `intervention skip: to_val=...` line for that
  class.
- Both `n_inc` > 0 and `n_dec` > 0 on a single-target-class Relative intervention.
- A delta sign that contradicts its branch line.
- `rows_changed` equal to `rows_target` (the 90th-percentile selection did not apply).
- `telemetry_rows` not 9, or `interventions` not 5.
- Any `intervention mask: ` line in the smoke log.
- No `posterior.tif` in `CUL/2024/region_costa_peruana`.

### Legitimate non-evidence (record it, it is not an engine failure)

- `intervention skip: to_val=106 one zone has no rows` or
  `intervention skip: to_val=106 no positive probabilities in one zone` (same for 105). The combination
  ran but had nothing to adjust, so it is still not exercised live.

### Before touching code

Classify first (the 05.1 rule): a guard that fires on genuinely bad data is fixed offline in the data,
never by weakening the guard.

---

## After the smoke

The CUL costa_peruana 2024 posterior stays in `/beegfs/black/nascent-lulcc/outputs/simulations/CUL`.
When the sweep runs, the launcher will still submit costa_peruana for CUL (9 of 10 posteriors
missing), and the driver will resume at 2028, using this 2024 posterior as the starting map. That is
valid: same code, starting from the initial 2022 LULC, exactly what the sweep's own first step does.

If you want every CUL posterior to come from the sweep job itself, archive it again before the sweep
(after a dry run without `--apply`):

```bash
bash scripts/archive_allocation_outputs.sh --scenarios CUL --tag post_cul_smoke_phase5 --apply 2>&1 | tee logs/archive-apply-post_cul_smoke_phase5.log
```

---

## Step 7: paste back as evidence

**Where:** login02 shell (copy the outputs into the chat).

1. Step 3: the `git log --oneline -1` hash and both `grep -c` counts.
2. Step 4: the per-scenario count blocks from `logs/archive-dryrun-pre_interventions_phase5.log`, the
   `STATE ... mode=apply ...` line, the `ARCHIVE_MANIFEST.tsv` contents and the `find ... | wc -l`
   result.
3. Step 5b: the costa_peruana line from `logs/cul-rate-precheck-2022.log`.
4. Step 6a: the smoke `sacct` line and the `intervention mask: ` count.
5. Step 6b: the verifier PASS or FAIL banner with its Conservation `[ok]` lines, the `sacct` line, and
   the `Info: ... not counted` count.
6. Step 6c: the five `AUDIT stage=intervention` lines plus the `AUDIT stage=intervention_summary`
   line; the branch log lines for `to_val=105` and `to_val=106`; the CSV rows for
   Mining_outside_restraint and Urban_densification.
