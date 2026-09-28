# Phase 5: Integrate spatial_interventions branch and stage intervention masks - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md. This log keeps the alternatives that were considered.

**Date:** 2026-09-22
**Phase:** 05-integrate-spatial-interventions-branch-and-stage-interventio
**Areas discussed:** Port strategy, Mask path resolution, HPC placement & staging, Validation & smoke proof

---

## Port strategy

| Question | Options | Selected |
|---|---|---|
| How to get the branch onto main | git merge + resolve / fresh co-authored port / rebase onto main | git merge + resolve |
| SSPx_interventions.yml | Delete as branch does / move to config/old/ and commit | Delete |
| Legacy intervention code | Leave untouched / move to src/old/ / you decide | Move to src/old/ |
| Branch docs placement | Move under docs/ / keep as branch lays out | Other: "move under docs and convert to markdown" |

## Mask path resolution

| Question | Options | Selected |
|---|---|---|
| YAML mask paths | Bare filenames under spat_prob_perturb_dir / keep spatial_masks/ + mask_root key / project-root-relative as written | Bare filenames under spat_prob_perturb_dir |
| interventions_dir | config/ in repo / alongside masks | config/ in repo |
| Mask lookup strategy | You decide / pre-crop per region / read national mask each call | You decide |

## HPC placement & staging

| Question | Options | Selected |
|---|---|---|
| Transfer | Documented rsync in README_HPC / staging script / git LFS | Documented rsync |
| Stage scope | Only referenced masks / mirror whole folder | Only referenced masks |
| Operator | Operator-gated checkpoint / local smoke only | Operator-gated checkpoint |

## Validation & smoke proof

| Question | Options | Selected |
|---|---|---|
| Missing mask | Fail fast in Stage 7 pre-flight / warn and skip | Fail fast in pre-flight |
| Grid mismatch | Hard fail, fix offline / resample on the fly | Hard fail |
| Check form | Standalone R script + report / one-off check | Standalone R script + report |
| Smoke run | NAT × region × 2032 / NAT × region × 2024 / all 4 scenarios one step | NAT × region × 2032 |

## Claude's Discretion
- Mask-to-cell join and caching strategy
- Docs folder and validator script naming
- Whether the branch function's reserved parameters are kept

## Deferred Ideas
- Scientific validation of intervention magnitudes, after the Phase 4 sweep
- Mask staging script or LFS, if masks start changing often
