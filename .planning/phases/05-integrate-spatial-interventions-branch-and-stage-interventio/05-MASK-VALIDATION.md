# Intervention Mask Validation

- Run: 2026-09-22 13:05:02 CEST
- Git: 12f247f
- Environment: local
- mask_dir: `D:/C.3_Modelling/nascent-lulcc-agg/inputs/spat_prob_perturb`
- interventions_dir: `C:/Users/black/OneDrive - Leibniz-Zentrum für Agrarlandschaftsforschung (ZALF) e.V/Documents/git/nascent-lulcc/config`
- Reference grid: D:/C.3_Modelling/nascent-lulcc-agg/inputs/spatial_reference_grid/ref_grid_aggregated.tif, crs EPSG:4326, res 0.0008084837557 x 0.0008084837557, extent (-81.70617684, -68.30232465, -18.84494786, 0.7138911563), 24192 rows x 16579 cols
- Scenarios: BAU, NAT, CUL, SOC
- Posterior years: 2024, 2028, 2032, 2036, 2040, 2044, 2048, 2052, 2056, 2060
- Checks: existence (D-12), compareGeom + nlyr == 1 hard fail, no resampling (D-13), values within {0,1,NA}, orphan top-level .tif

## Summary

- Referenced (scenario, intervention, year) entries: 138
- Distinct referenced masks: 14
- Distinct referenced masks present: 14
- Grid match (compareGeom TRUE): 14 / 14
- Orphan top-level .tif: 0
- FAIL: 0, WARN: 0, INFO: 13

## References per scenario

| scenario | id | year | mask | exists |
|---|---|---|---|---|
| BAU | ineffective_conservation | 2024 | protected_areas_mask_BAU.tif | yes |
| BAU | ineffective_conservation | 2028 | protected_areas_mask_BAU.tif | yes |
| BAU | ineffective_conservation | 2032 | protected_areas_mask_BAU.tif | yes |
| BAU | ineffective_conservation | 2036 | protected_areas_mask_BAU.tif | yes |
| BAU | ineffective_conservation | 2040 | protected_areas_mask_BAU.tif | yes |
| BAU | ineffective_conservation | 2044 | protected_areas_mask_BAU.tif | yes |
| BAU | ineffective_conservation | 2048 | protected_areas_mask_BAU.tif | yes |
| BAU | ineffective_conservation | 2052 | protected_areas_mask_BAU.tif | yes |
| BAU | ineffective_conservation | 2056 | protected_areas_mask_BAU.tif | yes |
| BAU | ineffective_conservation | 2060 | protected_areas_mask_BAU.tif | yes |
| NAT | Conservation_expansion_and_preservation | 2024 | protected_areas_mask_NAT_phase0_current.tif | yes |
| NAT | Conservation_expansion_and_preservation | 2028 | protected_areas_mask_NAT_phase0_current.tif | yes |
| NAT | Conservation_expansion_and_preservation | 2032 | protected_areas_mask_NAT_phase1_target.tif | yes |
| NAT | Conservation_expansion_and_preservation | 2036 | protected_areas_mask_NAT_phase2_full.tif | yes |
| NAT | Conservation_expansion_and_preservation | 2040 | protected_areas_mask_NAT_phase2_full.tif | yes |
| NAT | Conservation_expansion_and_preservation | 2044 | protected_areas_mask_NAT_phase2_full.tif | yes |
| NAT | Conservation_expansion_and_preservation | 2048 | protected_areas_mask_NAT_phase2_full.tif | yes |
| NAT | Conservation_expansion_and_preservation | 2052 | protected_areas_mask_NAT_phase2_full.tif | yes |
| NAT | Conservation_expansion_and_preservation | 2056 | protected_areas_mask_NAT_phase2_full.tif | yes |
| NAT | Conservation_expansion_and_preservation | 2060 | protected_areas_mask_NAT_phase2_full.tif | yes |
| NAT | Mining_freeze_post_2030 | 2032 | mining_concessions_mask.tif | yes |
| NAT | Mining_freeze_post_2030 | 2036 | mining_concessions_mask.tif | yes |
| NAT | Mining_freeze_post_2030 | 2040 | mining_concessions_mask.tif | yes |
| NAT | Mining_freeze_post_2030 | 2044 | mining_concessions_mask.tif | yes |
| NAT | Mining_freeze_post_2030 | 2048 | mining_concessions_mask.tif | yes |
| NAT | Mining_freeze_post_2030 | 2052 | mining_concessions_mask.tif | yes |
| NAT | Mining_freeze_post_2030 | 2056 | mining_concessions_mask.tif | yes |
| NAT | Mining_freeze_post_2030 | 2060 | mining_concessions_mask.tif | yes |
| NAT | Indigenous_land_OECM_forest | 2024 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_forest | 2028 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_forest | 2032 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_forest | 2036 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_forest | 2040 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_forest | 2044 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_forest | 2048 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_forest | 2052 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_forest | 2056 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_forest | 2060 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_ag | 2024 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_ag | 2028 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_ag | 2032 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_ag | 2036 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_ag | 2040 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_ag | 2044 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_ag | 2048 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_ag | 2052 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_ag | 2056 | indigenous_lands_mask.tif | yes |
| NAT | Indigenous_land_OECM_ag | 2060 | indigenous_lands_mask.tif | yes |
| NAT | Urban_densification | 2024 | urban_settlement_mask.tif | yes |
| NAT | Urban_densification | 2028 | urban_settlement_mask.tif | yes |
| NAT | Urban_densification | 2032 | urban_settlement_mask.tif | yes |
| NAT | Urban_densification | 2036 | urban_settlement_mask.tif | yes |
| NAT | Urban_densification | 2040 | urban_settlement_mask.tif | yes |
| NAT | Urban_densification | 2044 | urban_settlement_mask.tif | yes |
| NAT | Urban_densification | 2048 | urban_settlement_mask.tif | yes |
| NAT | Urban_densification | 2052 | urban_settlement_mask.tif | yes |
| NAT | Urban_densification | 2056 | urban_settlement_mask.tif | yes |
| NAT | Urban_densification | 2060 | urban_settlement_mask.tif | yes |
| CUL | Conservation_expansion_and_preservation | 2024 | protected_areas_mask_CUL_phase0_current.tif | yes |
| CUL | Conservation_expansion_and_preservation | 2028 | protected_areas_mask_CUL_phase0_current.tif | yes |
| CUL | Conservation_expansion_and_preservation | 2032 | protected_areas_mask_CUL_phase1_target.tif | yes |
| CUL | Conservation_expansion_and_preservation | 2036 | protected_areas_mask_CUL_phase2_full.tif | yes |
| CUL | Conservation_expansion_and_preservation | 2040 | protected_areas_mask_CUL_phase2_full.tif | yes |
| CUL | Conservation_expansion_and_preservation | 2044 | protected_areas_mask_CUL_phase2_full.tif | yes |
| CUL | Conservation_expansion_and_preservation | 2048 | protected_areas_mask_CUL_phase2_full.tif | yes |
| CUL | Conservation_expansion_and_preservation | 2052 | protected_areas_mask_CUL_phase2_full.tif | yes |
| CUL | Conservation_expansion_and_preservation | 2056 | protected_areas_mask_CUL_phase2_full.tif | yes |
| CUL | Conservation_expansion_and_preservation | 2060 | protected_areas_mask_CUL_phase2_full.tif | yes |
| CUL | Mining_outside_restraint | 2024 | mining_concessions_mask.tif | yes |
| CUL | Mining_outside_restraint | 2028 | mining_concessions_mask.tif | yes |
| CUL | Mining_outside_restraint | 2032 | mining_concessions_mask.tif | yes |
| CUL | Mining_outside_restraint | 2036 | mining_concessions_mask.tif | yes |
| CUL | Mining_outside_restraint | 2040 | mining_concessions_mask.tif | yes |
| CUL | Mining_outside_restraint | 2044 | mining_concessions_mask.tif | yes |
| CUL | Mining_outside_restraint | 2048 | mining_concessions_mask.tif | yes |
| CUL | Mining_outside_restraint | 2052 | mining_concessions_mask.tif | yes |
| CUL | Mining_outside_restraint | 2056 | mining_concessions_mask.tif | yes |
| CUL | Mining_outside_restraint | 2060 | mining_concessions_mask.tif | yes |
| CUL | Indigenous_land_OECM_forest | 2024 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_forest | 2028 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_forest | 2032 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_forest | 2036 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_forest | 2040 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_forest | 2044 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_forest | 2048 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_forest | 2052 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_forest | 2056 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_forest | 2060 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_ag | 2024 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_ag | 2028 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_ag | 2032 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_ag | 2036 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_ag | 2040 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_ag | 2044 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_ag | 2048 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_ag | 2052 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_ag | 2056 | indigenous_lands_mask.tif | yes |
| CUL | Indigenous_land_OECM_ag | 2060 | indigenous_lands_mask.tif | yes |
| CUL | Urban_densification | 2024 | urban_settlement_mask.tif | yes |
| CUL | Urban_densification | 2028 | urban_settlement_mask.tif | yes |
| CUL | Urban_densification | 2032 | urban_settlement_mask.tif | yes |
| CUL | Urban_densification | 2036 | urban_settlement_mask.tif | yes |
| CUL | Urban_densification | 2040 | urban_settlement_mask.tif | yes |
| CUL | Urban_densification | 2044 | urban_settlement_mask.tif | yes |
| CUL | Urban_densification | 2048 | urban_settlement_mask.tif | yes |
| CUL | Urban_densification | 2052 | urban_settlement_mask.tif | yes |
| CUL | Urban_densification | 2056 | urban_settlement_mask.tif | yes |
| CUL | Urban_densification | 2060 | urban_settlement_mask.tif | yes |
| SOC | Conservation_expansion_and_preservation | 2024 | protected_areas_mask_SOC_phase0_current.tif | yes |
| SOC | Conservation_expansion_and_preservation | 2028 | protected_areas_mask_SOC_phase0_current.tif | yes |
| SOC | Conservation_expansion_and_preservation | 2032 | protected_areas_mask_SOC_phase1_target.tif | yes |
| SOC | Conservation_expansion_and_preservation | 2036 | protected_areas_mask_SOC_phase2_full.tif | yes |
| SOC | Conservation_expansion_and_preservation | 2040 | protected_areas_mask_SOC_phase2_full.tif | yes |
| SOC | Conservation_expansion_and_preservation | 2044 | protected_areas_mask_SOC_phase2_full.tif | yes |
| SOC | Conservation_expansion_and_preservation | 2048 | protected_areas_mask_SOC_phase2_full.tif | yes |
| SOC | Conservation_expansion_and_preservation | 2052 | protected_areas_mask_SOC_phase2_full.tif | yes |
| SOC | Conservation_expansion_and_preservation | 2056 | protected_areas_mask_SOC_phase2_full.tif | yes |
| SOC | Conservation_expansion_and_preservation | 2060 | protected_areas_mask_SOC_phase2_full.tif | yes |
| SOC | Mining_in_low_ES_areas | 2024 | low_es_value_mask.tif | yes |
| SOC | Mining_in_low_ES_areas | 2028 | low_es_value_mask.tif | yes |
| SOC | Mining_in_low_ES_areas | 2032 | low_es_value_mask.tif | yes |
| SOC | Mining_in_low_ES_areas | 2036 | low_es_value_mask.tif | yes |
| SOC | Mining_in_low_ES_areas | 2040 | low_es_value_mask.tif | yes |
| SOC | Mining_in_low_ES_areas | 2044 | low_es_value_mask.tif | yes |
| SOC | Mining_in_low_ES_areas | 2048 | low_es_value_mask.tif | yes |
| SOC | Mining_in_low_ES_areas | 2052 | low_es_value_mask.tif | yes |
| SOC | Mining_in_low_ES_areas | 2056 | low_es_value_mask.tif | yes |
| SOC | Mining_in_low_ES_areas | 2060 | low_es_value_mask.tif | yes |
| SOC | Urban_densification | 2024 | urban_settlement_mask.tif | yes |
| SOC | Urban_densification | 2028 | urban_settlement_mask.tif | yes |
| SOC | Urban_densification | 2032 | urban_settlement_mask.tif | yes |
| SOC | Urban_densification | 2036 | urban_settlement_mask.tif | yes |
| SOC | Urban_densification | 2040 | urban_settlement_mask.tif | yes |
| SOC | Urban_densification | 2044 | urban_settlement_mask.tif | yes |
| SOC | Urban_densification | 2048 | urban_settlement_mask.tif | yes |
| SOC | Urban_densification | 2052 | urban_settlement_mask.tif | yes |
| SOC | Urban_densification | 2056 | urban_settlement_mask.tif | yes |
| SOC | Urban_densification | 2060 | urban_settlement_mask.tif | yes |

## Grid and values per mask

| mask | compareGeom | crs | res | dims (rows x cols x lyr) | values | ones | ones_outside_ref |
|---|---|---|---|---|---|---|---|
| indigenous_lands_mask.tif | TRUE | EPSG:4326 | 0.0008084837557 x 0.0008084837557 | 24192 x 16579 x 1 | 1, NA | 42,117,450 | 8,784 |
| low_es_value_mask.tif | TRUE | EPSG:4326 | 0.0008084837557 x 0.0008084837557 | 24192 x 16579 x 1 | 1, NA | 40,777,549 | 0 |
| mining_concessions_mask.tif | TRUE | EPSG:4326 | 0.0008084837557 x 0.0008084837557 | 24192 x 16579 x 1 | 1, NA | 20,136,257 | 48,270 |
| protected_areas_mask_BAU.tif | TRUE | EPSG:4326 | 0.0008084837557 x 0.0008084837557 | 24192 x 16579 x 1 | 1, NA | 28,330,871 | 235,814 |
| protected_areas_mask_CUL_phase0_current.tif | TRUE | EPSG:4326 | 0.0008084837557 x 0.0008084837557 | 24192 x 16579 x 1 | 1, NA | 28,330,871 | 235,814 |
| protected_areas_mask_CUL_phase1_target.tif | TRUE | EPSG:4326 | 0.0008084837557 x 0.0008084837557 | 24192 x 16579 x 1 | 1, NA | 44,580,520 | 328,861 |
| protected_areas_mask_CUL_phase2_full.tif | TRUE | EPSG:4326 | 0.0008084837557 x 0.0008084837557 | 24192 x 16579 x 1 | 1, NA | 48,671,725 | 331,013 |
| protected_areas_mask_NAT_phase0_current.tif | TRUE | EPSG:4326 | 0.0008084837557 x 0.0008084837557 | 24192 x 16579 x 1 | 1, NA | 28,330,871 | 235,814 |
| protected_areas_mask_NAT_phase1_target.tif | TRUE | EPSG:4326 | 0.0008084837557 x 0.0008084837557 | 24192 x 16579 x 1 | 1, NA | 44,597,597 | 267,260 |
| protected_areas_mask_NAT_phase2_full.tif | TRUE | EPSG:4326 | 0.0008084837557 x 0.0008084837557 | 24192 x 16579 x 1 | 1, NA | 48,688,963 | 271,150 |
| protected_areas_mask_SOC_phase0_current.tif | TRUE | EPSG:4326 | 0.0008084837557 x 0.0008084837557 | 24192 x 16579 x 1 | 1, NA | 28,330,871 | 235,814 |
| protected_areas_mask_SOC_phase1_target.tif | TRUE | EPSG:4326 | 0.0008084837557 x 0.0008084837557 | 24192 x 16579 x 1 | 1, NA | 43,568,510 | 306,530 |
| protected_areas_mask_SOC_phase2_full.tif | TRUE | EPSG:4326 | 0.0008084837557 x 0.0008084837557 | 24192 x 16579 x 1 | 1, NA | 48,683,953 | 310,224 |
| urban_settlement_mask.tif | TRUE | EPSG:4326 | 0.0008084837557 x 0.0008084837557 | 24192 x 16579 x 1 | 1, NA | 1,067,800 | 24,396 |

## Orphans and local-only entries

- Orphan top-level .tif: none
- .DS_Store: local-only, not staged (INFO, D-10)
- nascent-pa-prioritization/: local-only, not staged (INFO, D-10)
- README.txt: local-only, not staged (INFO, D-10)
- spatial_masks/: local-only, not staged (INFO, D-10)
- urban_settlement_mask_methodology.txt: local-only, not staged (INFO, D-10)

## Findings

| level | check | subject | detail |
|---|---|---|---|
| INFO | outside-ref | indigenous_lands_mask.tif | 8,784 mask==1 cells where the ref grid is NA (outside model domain, harmless) |
| INFO | outside-ref | mining_concessions_mask.tif | 48,270 mask==1 cells where the ref grid is NA (outside model domain, harmless) |
| INFO | outside-ref | protected_areas_mask_BAU.tif | 235,814 mask==1 cells where the ref grid is NA (outside model domain, harmless) |
| INFO | outside-ref | protected_areas_mask_CUL_phase0_current.tif | 235,814 mask==1 cells where the ref grid is NA (outside model domain, harmless) |
| INFO | outside-ref | protected_areas_mask_CUL_phase1_target.tif | 328,861 mask==1 cells where the ref grid is NA (outside model domain, harmless) |
| INFO | outside-ref | protected_areas_mask_CUL_phase2_full.tif | 331,013 mask==1 cells where the ref grid is NA (outside model domain, harmless) |
| INFO | outside-ref | protected_areas_mask_NAT_phase0_current.tif | 235,814 mask==1 cells where the ref grid is NA (outside model domain, harmless) |
| INFO | outside-ref | protected_areas_mask_NAT_phase1_target.tif | 267,260 mask==1 cells where the ref grid is NA (outside model domain, harmless) |
| INFO | outside-ref | protected_areas_mask_NAT_phase2_full.tif | 271,150 mask==1 cells where the ref grid is NA (outside model domain, harmless) |
| INFO | outside-ref | protected_areas_mask_SOC_phase0_current.tif | 235,814 mask==1 cells where the ref grid is NA (outside model domain, harmless) |
| INFO | outside-ref | protected_areas_mask_SOC_phase1_target.tif | 306,530 mask==1 cells where the ref grid is NA (outside model domain, harmless) |
| INFO | outside-ref | protected_areas_mask_SOC_phase2_full.tif | 310,224 mask==1 cells where the ref grid is NA (outside model domain, harmless) |
| INFO | outside-ref | urban_settlement_mask.tif | 24,396 mask==1 cells where the ref grid is NA (outside model domain, harmless) |

VERDICT: PASS
