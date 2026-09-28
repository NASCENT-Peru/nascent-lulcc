---
phase: 05-integrate-spatial-interventions-branch-and-stage-interventio
plan: 16
status: complete
completed: 2026-09-28
requirements: [D-17]
autonomous: false
type: operator-gate
---

# 05-16: Operator gate — merge PR #2 and verify on HPC

Closes both `05-VERIFICATION.md` gaps with operator-reported evidence. No repository code changes
(`files_modified: []`). Ran last so the merge carries the gap-closure fixes rather than preceding them.

## Task 1 — Local pre-hand-off gate

Branch `spatial-interventions-integration`, HEAD `3f06dc3` at hand-off.

Suite: **PASS 684 | FAIL 4 | WARN 6 | SKIP 13**. The 4 are the pre-existing `test-prep-paths.R`
errors at lines 27/35/43/56 (`Error in file(con, "r"): cannot open the connection`), identities
verified at every wave boundary, not merely counted. Growth across the six gap-closure plans was
exactly additive with zero cross-plan interference: 459 → 510 → 568 → 623 → 684.

Shipped contracts held: frozen AUDIT `sprintf()` present exactly once; `resample(`/`project(` = 0 in
both `src/implement_spatial_interventions.R` and `src/allocation.r` (D-13); probability-table copies
= 0 (D-20).

## Task 2 — Merge PR #2 (D-17 / SC1 truth #16) — CLOSED

Operator merged via GitHub. **Independently verified by the orchestrator, not accepted on attestation:**

```
$ git merge-base --is-ancestor a917ba1 origin/main; echo "exit=$?"
exit=0

$ gh pr view 2 --json state,mergedAt,mergeCommit
state=MERGED mergedAt=2026-09-28T12:06:30Z mergeCommit=f54ccc2b35c512996fa619058ced59415ec94ce0

$ git log -1 --format='%h %s' origin/main
f54ccc2 Merge pull request #2 from NASCENT-Peru/spatial-interventions-integration
```

Additional check confirming the plan's ordering intent was actually achieved — that the merge carried
the gap-closure work and did not merely precede it:

```
$ git merge-base --is-ancestor 3f06dc3 origin/main; echo "exit=$?"
exit=0
```

`3f06dc3` is the wave-6 tracking commit, so all six gap-closure plans (05-10..05-15) are on `main`.
Merge commit preserves the colleague's four commits per D-01 (`--merge`, not `--squash`).

## Task 3 — HPC checksum and post-fix smoke (D-17 / truth #14) — CLOSED

### Checksum — the gap that was skipped in 05-06

Operator-reported: `sha256sum -c docs/spatial_interventions/masks.sha256` in
`/beegfs/black/nascent-lulcc/inputs/spat_prob_perturb` reported **OK for all 14 lines, 0 FAILED**.

This is operator-attested rather than orchestrator-verified — the masks live on beegfs and are not
reachable from the workstation. Recorded as such deliberately. It is the specific evidence truth #14
demands and that 05-06 substituted the validator's geometry checks for; geometry proves alignment,
the checksum proves the bytes survived transfer.

### Smoke run

Job **841577**, submitted from `login02` with `--partition=highmem`.

```
$ sacct -j 841577 --format=JobID,State,ExitCode,MaxRSS,Elapsed
JobID             State ExitCode     MaxRSS    Elapsed
------------ ---------- -------- ---------- ----------
841577        COMPLETED      0:0              02:11:20
841577.batch  COMPLETED      0:0  88558024K   02:11:20
841577.exte+  COMPLETED      0:0       256K   02:11:20
```

MaxRSS **88.5 GB**, comfortably inside highmem's 188 GB and below job 838021's 95.7 GB. No memory
regression from the telemetry work.

### Extended AUDIT lines (5 interventions + 1 summary, all 8 delta fields present)

```
AUDIT stage=intervention region=costa_peruana scenario=NAT year=2032 id=Conservation_expansion_and_preservation rank=1 type=Absolute zone=Inside to_vals=105,104,106 mask=protected_areas_mask_NAT_phase1_target.tif rows_target=7517803 rows_changed=1058116 delta_mean=-0.00830575 delta_med=0 delta_sd=0.0534815 delta_min=-0.990616 delta_max=0 n_inc=0 n_dec=1058116 sum_abs_delta=62441
AUDIT stage=intervention region=costa_peruana scenario=NAT year=2032 id=Mining_freeze_post_2030 rank=2 type=Absolute zone=Outside to_vals=106 mask=mining_concessions_mask.tif rows_target=15195834 rows_changed=612202 delta_mean=-0.000858948 delta_med=0 delta_sd=0.0131818 delta_min=-0.986166 delta_max=0 n_inc=0 n_dec=612202 sum_abs_delta=13052.4
AUDIT stage=intervention region=costa_peruana scenario=NAT year=2032 id=Indigenous_land_OECM_forest rank=3 type=Relative zone=Inside to_vals=105,104,106 mask=indigenous_lands_mask.tif rows_target=12393402 rows_changed=95750 delta_mean=-0.00115824 delta_med=0 delta_sd=0.0136095 delta_min=-0.189015 delta_max=0 n_inc=0 n_dec=95750 sum_abs_delta=14354.6
AUDIT stage=intervention region=costa_peruana scenario=NAT year=2032 id=Indigenous_land_OECM_ag rank=4 type=Relative zone=Inside to_vals=104 mask=indigenous_lands_mask.tif rows_target=1925778 rows_changed=45976 delta_mean=0.00554015 delta_med=0 delta_sd=0.0375444 delta_min=0 delta_max=0.373017 n_inc=45976 n_dec=0 sum_abs_delta=10669.1
AUDIT stage=intervention region=costa_peruana scenario=NAT year=2032 id=Urban_densification rank=5 type=Relative zone=Inside to_vals=105 mask=urban_settlement_mask.tif rows_target=9133978 rows_changed=523718 delta_mean=-0.00162168 delta_med=0 delta_sd=0.00724674 delta_min=-0.0499002 delta_max=0.0476049 n_inc=15604 n_dec=508114 sum_abs_delta=15782.6
AUDIT stage=intervention_summary region=costa_peruana scenario=NAT year=2032 n_interventions=5 cells_sum_gt1=31974
```

### Telemetry CSV head

`intervention_prob_deltas_NAT_costa_peruana_2032.csv` — header matches the 25-column contract exactly.

```
scenario,region,year,intervention_id,rank,type,zone,mask,target_class,n_target,n_changed,mean_before,mean_after,sd_before,sd_after,p05_delta,p25_delta,p50_delta,p75_delta,p95_delta,min_delta,max_delta,sum_abs_delta,prob_mass_before,prob_mass_after
NAT,costa_peruana,2032,Conservation_expansion_and_preservation,1,Absolute,Inside,protected_areas_mask_NAT_phase1_target.tif,105,1291373,677048,0.03571616177797,0,0.110865817699558,0,-0.2054,-0.0136,-8e-04,0,0,-0.99061564847189,0,46122.8869837025,46122.8869837025,0
NAT,costa_peruana,2032,Conservation_expansion_and_preservation,1,Absolute,Inside,protected_areas_mask_NAT_phase1_target.tif,104,3113215,288724,0.0042970014314408,0,0.0336558043201911,0,-0.006,0,0,0,0,-0.983327280530886,0,13377.4893113829,13377.4893113829,0
```

### Verifier banner

```
telemetry: 9 rows, 5 interventions, 3 classes [ok]

PASS verify_intervention_smoke scenario=NAT region=costa_peruana year=2032 interventions=5 maps_checked=16 telemetry_rows=9
```

Forbidden pre-flight markers: none present. The verifier's own forbidden-marker list (extended by
05-14 to the full 05-09/05-10/05-13 family) is scanned as part of the run, so the PASS banner
subsumes the manual grep.

## Independent cross-checks the orchestrator performed on the reported evidence

**Telemetry row count is arithmetically correct.** 9 rows = one per intervention x target class:
Conservation 3 (105,104,106) + Mining_freeze 1 (106) + Indigenous_forest 3 + Indigenous_ag 1 +
Urban_densification 1 = 9. "3 classes" = {104,105,106}. Matches.

**`maps_checked=16` is correct.** 12 Conservation zone assertions + 4 Mining_freeze = 16. Only the
two Absolute interventions get `max prob in zone = 0` assertions, since Relative ones do not zero
anything — so 16 is the right denominator, not an undercount.

**CSV and AUDIT reconcile (D-22).** For `Conservation_expansion_and_preservation` across
`to_vals=105,104,106`, the two visible CSV rows sum to `n_changed` 965,772 and `sum_abs_delta`
59,500.376 against AUDIT totals of 1,058,116 and 62,441 — leaving a consistent remainder of 92,344 /
2,940.6 for class 106. Two independently computed sinks agreeing.

**Absolute-to-0 semantics confirmed in raw data.** Class 105 row: `mean_after=0`, `sd_after=0`,
`max_delta=0`, `prob_mass_after=0`, and `sum_abs_delta == prob_mass_before` to 13 significant figures
(46122.8869837025). Driving every targeted row to zero must make total absolute change equal starting
mass; it does.

**WR-07 fix visibly working.** Every zone line reports a real `n_zone` (862,288 / 313,649 / 97,354 /
18,082 / 1,839,924 / 3,628,248 / 1,672,549 / 1,721,727 / 8,173,310) alongside the max assertion.
Pre-05-14 a zero-cell zone printed `[ok]` and counted toward `maps_checked`.

**A `Decrease` valency producing positive deltas is correct, not a defect.** Initially flagged by the
orchestrator and then resolved against the code: rank 3 and rank 4 are both declared
`Prob_adjust_valency: Decrease`, both `zone=Inside`, both on `indigenous_lands_mask.tif`, yet rank 3
is all-negative and rank 4 all-positive. `relative_prob_adjust()` has two routes to "make inside
relatively less attractive": `Perc_diff >= 0` lowers the inside pixels
(`prob + (prob/100) * -(Perc_diff)`), `Perc_diff < 0` raises the outside pixels
(`prob + (prob/100) * abs(Perc_diff)`). Rank 3 had inside above outside, rank 4 had inside already
below outside. Notably the `Perc_diff < 0` branch is the one 05-11's WR-01(b) fix touched, so seeing
it fire on real data over 45,976 rows is positive evidence the boundary is reachable.

## Expected differences from job 838021 — confirmed, not regressions

Per 05-11, `rows_changed` values and `cells_sum_gt1` legitimately moved: the percentile comparison is
now `>=`, the `Perc_diff == 0` dead zone is gone, and clamps are per-index-set. This run was judged
against the new semantics, **not** diffed against 838021.

## Out of scope observation, logged not actioned

The verifier's `Info (not assessed)` block shows `placed` exactly 1 below `demanded` for ~20 of 25
transitions (e.g. 47481/47480, 86338/86337, 100256/100255), with a handful equal (86/86, 43/43, 0/0,
7885/7885). A systematic one-cell shortfall across most transitions looks more like an off-by-one or
floor artifact in the allocator than intervention-driven scarcity, which would be irregular. It is
explicitly not assessed by the verifier, is unrelated to intervention telemetry, and is out of
Phase 5 scope. Worth a look by whichever phase owns allocation demand satisfaction.

## Requirements closed

D-17 — both `05-VERIFICATION.md` gaps closed with operator-reported evidence, neither claimed from
local state.

## Self-Check

- PR #2 `state=MERGED`, merge commit `f54ccc2` — VERIFIED by orchestrator
- `git merge-base --is-ancestor a917ba1 origin/main` exit 0 — VERIFIED by orchestrator
- Gap-closure commits on `main` (`3f06dc3` ancestor of `origin/main`) — VERIFIED by orchestrator
- `sha256sum -c` 14 OK / 0 FAILED — OPERATOR-ATTESTED (not reachable from workstation)
- `sacct` 841577 COMPLETED 0:0 — REPORTED verbatim
- 5 extended AUDIT lines + 1 summary line, all 8 delta fields — REPORTED verbatim
- Telemetry CSV present, 25-column header exact — REPORTED verbatim
- `PASS ... maps_checked=16 telemetry_rows=9`, both non-zero — REPORTED verbatim

Self-Check: PASSED
