# Lazy predictor test fixture

Synthetic hive-partitioned Parquet fixture for
`tests/testthat/test-allocation-lazy-predictor.R` (Phase 3.5 Plan 02).

The fixture is **generated reproducibly at test time** by `.build_lazy_fixture()`
into a `withr::local_tempdir()` — no binary Parquet is committed to the repo.
This file documents the schema and partitioning that builder produces so the
contract is reviewable without running the test, and so the layout can be kept
in sync with the production datasets opened in
`generate_probability_maps()` (`src/allocation.r` L2237-2249).

## Why these shapes

The fixture mirrors the production Arrow datasets **exactly**, so that
`load_from_class_predictor_data()` (and the inline eager-reference computation
in the equivalence test) are exercised against the same partitioning the real
pipeline uses:

- **static** dataset: `arrow::hive_partition(region = arrow::int32())`
- **dynamic** dataset: `arrow::hive_partition(scenario = arrow::utf8(), region = arrow::int32())`

`set.seed(42L)` is set before the predictor columns are drawn so the values are
deterministic across runs.

## static/ — partition `region` (int32)

| column   | type    | notes                                    |
| -------- | ------- | ---------------------------------------- |
| cell_id  | int32   | join key                                 |
| stat_a   | double  | static predictor                         |
| stat_b   | double  | static predictor                         |
| region   | int32   | hive partition (NOT a data column)       |

Partitions / cell-id ranges:

- `region=1`: `cell_id` 1..200
- `region=2`: `cell_id` 1001..1100 (disjoint id range — a region-filter leak
  would be obvious because region-2 ids are >= 1001)

## dynamic/ — partition `scenario` (utf8) + `region` (int32)

| column   | type    | notes                                    |
| -------- | ------- | ---------------------------------------- |
| cell_id  | int32   | join key                                 |
| dyn_x    | double  | dynamic predictor                        |
| scenario | utf8    | hive partition (NOT a data column)       |
| region   | int32   | hive partition (NOT a data column)       |

Partitions / cell-id ranges (all under `scenario=baseline`):

- `scenario=baseline/region=1`: `cell_id` 1..150
- `scenario=baseline/region=2`: `cell_id` 1001..1100

The dynamic dataset **omits region-1 cell_ids 151..200 on purpose**: those cells
exist as static/from-class cells but have no dynamic predictor row. This
exercises the NA-fill keyed-join semantics (`region_pred_dt[J(lookup_keys),
nomatch = NA]`, `src/allocation.r` L2372) that the lazy path must preserve
(Pitfall 4) — a from-class cell missing from a predictor source is KEPT with NA,
never silently dropped.

## From-class key sets used by the assertions

The tests treat arbitrary `cell_id` subsets as a from-class's sparse
`ref_cell_id` keys (the production `from_idx$ref_cell_id`):

- Equivalence test keys: `c(5, 17, 42, 120, 175, 199)` — spans the dynamic-
  populated head (<=150) and the static-only tail (>150) so NA-fill is covered.
- Scoping test keys: `c(3, 8, 88)` with cols `c("stat_a", "dyn_x")` (stat_b
  omitted to prove column projection).
- Cache test: from_val `101` -> keys `c(1, 2, 3)`, from_val `102` -> keys
  `c(50, 51)`; the loop visits `101, 102, 101` and must collect only twice.
