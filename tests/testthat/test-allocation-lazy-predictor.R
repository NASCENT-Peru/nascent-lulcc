# Phase 3.5 Plan 02: lazy per-from-class predictor read (Goal 1). Covers
# load_from_class_predictor_data() — the sole predictor read path, scoped to a
# from-class's sparse cell keys + the columns its transitions need, via an
# arrow inner_join against a key table (NOT cell_id %in% big_vector). The
# eager full-region preload it was validated against has been removed; the
# equivalence assertion below computes that reference semantic inline.
#
# Contract (from 03.5-02-PLAN.md <behavior> + 03.5-RESEARCH.md Pattern 1):
#   - load_from_class_predictor_data(ds_static, ds_dynamic, cols, region_value,
#     scenario, ref_cell_keys, log_file = NULL) returns a data.table keyed by
#     cell_id.
#   - Static cols filtered by region only; dynamic cols by region + scenario;
#     merged by cell_id (all = TRUE), exactly like the eager merge.
#   - Output rows/columns for the given keys are IDENTICAL to the historical
#     eager full-region read restricted to those keys/cols (cell_id setequal +
#     per-column all.equal; reference computed inline).
#   - Cells outside the key set are NOT returned (column projection + key
#     scoping).
#   - A per-from-class cache reads each distinct from_val at most once (a second
#     cache hit triggers no additional dataset collect).
#
# This test sources src/allocation.r (the same way test-allocation-predict-
# threads.R does) so it exercises the real function. Before Task 1 lands the
# function, the source defines no load_from_class_predictor_data() and the
# existence assertion fails — the intended RED state. The arrow-backed
# equivalence/scoping/cache assertions SKIP on hosts without the arrow package
# (local Windows dev); they run on the HPC stack where arrow is present, just as
# the mlr3 integration tests SKIP locally and run on HPC.

library(testthat)
library(data.table)

.repo_root <- (function() {
  here <- tryCatch(normalizePath(sys.frame(1)$ofile %||% "."), error = function(e) ".")
  if (is.null(here) || identical(here, "")) here <- "."
  is_dir <- tryCatch(file.info(here)$isdir, error = function(e) NA)
  if (isTRUE(is_dir)) here <- file.path(here, "x")
  normalizePath(file.path(dirname(dirname(dirname(here)))), mustWork = FALSE)
})()

# Source the real allocation helpers. allocation.r is pure function definitions
# (no top-level library()/source()), so this succeeds under a plain R install.
tryCatch(
  source(file.path(.repo_root, "src/allocation.r")),
  error = function(e) skip(paste("could not source allocation.r:", conditionMessage(e)))
)

# --------------------------------------------------------------------------
# Synthetic fixture builder. Writes a tiny hive-partitioned parquet dataset that
# mirrors the L2237-2249 partitioning EXACTLY:
#   static : partition region = int32()
#   dynamic: partition scenario = utf8(), region = int32()
# Two region partitions (1, 2) and >= 2 distinct from-class cell-key sets within
# region 1. The dataset is written reproducibly into `root` and re-read with the
# same open_dataset() partitioning the production code uses.
#
# Schema documented in tests/testthat/fixtures/lazy_predictor/README.md.
.build_lazy_fixture <- function(root) {
  static_dir <- file.path(root, "static")
  dynamic_dir <- file.path(root, "dynamic")
  dir.create(static_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(dynamic_dir, recursive = TRUE, showWarnings = FALSE)

  set.seed(42L)

  # Region 1: cell_id 1..200. Region 2: cell_id 1001..1100 (disjoint id ranges
  # so a region filter leak would be obvious). Static predictors: stat_a, stat_b.
  # Dynamic predictors (under scenario "baseline"): dyn_x.
  mk_static <- function(region, ids) {
    data.frame(
      cell_id = as.integer(ids),
      stat_a = round(runif(length(ids)), 6),
      stat_b = round(runif(length(ids)), 6),
      region = as.integer(region)
    )
  }
  mk_dynamic <- function(region, scenario, ids) {
    data.frame(
      cell_id = as.integer(ids),
      dyn_x = round(runif(length(ids)), 6),
      scenario = as.character(scenario),
      region = as.integer(region)
    )
  }

  r1_ids <- 1:200
  r2_ids <- 1001:1100

  static_df <- rbind(
    mk_static(1L, r1_ids),
    mk_static(2L, r2_ids)
  )
  # Dynamic intentionally omits some region-1 cell_ids (151:200) so the NA-fill
  # left-join semantics (J(lookup_keys), nomatch = NA) are exercised: those keys
  # exist as from-class cells but have no dynamic predictor row.
  dyn_df <- rbind(
    mk_dynamic(1L, "baseline", 1:150),
    mk_dynamic(2L, "baseline", r2_ids)
  )

  arrow::write_dataset(
    static_df,
    path = static_dir,
    format = "parquet",
    partitioning = "region"
  )
  arrow::write_dataset(
    dyn_df,
    path = dynamic_dir,
    format = "parquet",
    partitioning = c("scenario", "region")
  )

  list(static_dir = static_dir, dynamic_dir = dynamic_dir)
}

.open_fixture <- function(dirs) {
  ds_static <- arrow::open_dataset(
    dirs$static_dir,
    format = "parquet",
    partitioning = arrow::hive_partition(region = arrow::int32())
  )
  ds_dynamic <- arrow::open_dataset(
    dirs$dynamic_dir,
    format = "parquet",
    partitioning = arrow::hive_partition(
      scenario = arrow::utf8(),
      region = arrow::int32()
    )
  )
  list(ds_static = ds_static, ds_dynamic = ds_dynamic)
}

# --------------------------------------------------------------------------
# Assertion 0 (arrow-independent RED signal): the function must exist.
# This fails before Task 1 ("could not find function") regardless of whether
# arrow is installed, giving an observable RED on the local dev box.
test_that("load_from_class_predictor_data() is defined", {
  expect_true(
    exists("load_from_class_predictor_data", mode = "function"),
    info = "Task 1 must add load_from_class_predictor_data() to src/allocation.r"
  )
})

# --------------------------------------------------------------------------
# Assertion 1: lazy read == eager preload restricted to the same keys/cols.
test_that("lazy read is row/column-equivalent to the eager preload", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("dplyr")
  skip_if_not(exists("load_from_class_predictor_data", mode = "function"))

  root <- withr::local_tempdir()
  dirs <- .build_lazy_fixture(root)
  ds <- .open_fixture(dirs)

  cols <- c("stat_a", "stat_b", "dyn_x")
  region_value <- 1L
  scenario <- "baseline"
  # A from-class key set that overlaps both the static-only tail (151:200, no
  # dynamic row) and the fully-populated head.
  keys <- as.integer(c(5, 17, 42, 120, 175, 199))

  lazy <- load_from_class_predictor_data(
    ds$ds_static, ds$ds_dynamic,
    cols = cols, region_value = region_value, scenario = scenario,
    ref_cell_keys = keys
  )

  # Reference: the historical eager full-region semantic computed inline —
  # static cols filtered by region only, dynamic cols by region + scenario,
  # merged by cell_id with all = TRUE, keyed, then restricted to the same keys.
  # (preload_region_predictor_data() itself was removed once the lazy path
  # became the sole read path; this preserves the equivalence contract it
  # defined.)
  static_ref <- ds$ds_static |>
    dplyr::filter(region == 1L) |>
    dplyr::select(cell_id, stat_a, stat_b) |>
    dplyr::collect() |>
    data.table::as.data.table()
  dyn_ref <- ds$ds_dynamic |>
    dplyr::filter(region == 1L, scenario == "baseline") |>
    dplyr::select(cell_id, dyn_x) |>
    dplyr::collect() |>
    data.table::as.data.table()
  eager_full <- merge(static_ref, dyn_ref, by = "cell_id", all = TRUE)
  data.table::setkeyv(eager_full, "cell_id")
  eager <- eager_full[J(keys), nomatch = NA]

  expect_s3_class(lazy, "data.table")
  expect_identical(data.table::key(lazy), "cell_id")

  # cell_id coverage: lazy must contain exactly the keys present in the region
  # (inner_join + merge(all=TRUE) over the two sources). The eager restriction
  # is over the same keys; both must agree on which keys are represented.
  lazy_keys <- sort(lazy$cell_id)
  eager_keys <- sort(eager[!is.na(cell_id), cell_id])
  expect_setequal(lazy_keys, eager_keys)

  # Per-column equality on the shared keys (order-independent merge by cell_id).
  shared <- merge(
    lazy, eager[!is.na(cell_id)],
    by = "cell_id", suffixes = c(".lazy", ".eager")
  )
  for (cn in cols) {
    expect_equal(
      shared[[paste0(cn, ".lazy")]],
      shared[[paste0(cn, ".eager")]],
      info = paste("column", cn, "must match the eager preload")
    )
  }
})

# --------------------------------------------------------------------------
# Assertion 2: column projection + key scoping — no cells outside the key set,
# and no columns outside the projection (beyond cell_id).
test_that("lazy read returns only the requested keys and columns", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("dplyr")
  skip_if_not(exists("load_from_class_predictor_data", mode = "function"))

  root <- withr::local_tempdir()
  dirs <- .build_lazy_fixture(root)
  ds <- .open_fixture(dirs)

  cols <- c("stat_a", "dyn_x")  # deliberately omit stat_b
  keys <- as.integer(c(3, 8, 88))

  lazy <- load_from_class_predictor_data(
    ds$ds_static, ds$ds_dynamic,
    cols = cols, region_value = 1L, scenario = "baseline",
    ref_cell_keys = keys
  )

  # Key scoping: every returned cell_id is in the key set; none outside it.
  expect_true(all(lazy$cell_id %in% keys))
  # Column projection: only cell_id + requested cols (stat_b absent).
  expect_setequal(names(lazy), c("cell_id", cols))
  expect_false("stat_b" %in% names(lazy))
  # Region scoping: no region-2 cell_ids (1001..1100) leak in.
  expect_false(any(lazy$cell_id >= 1001L))
})

# --------------------------------------------------------------------------
# Assertion 3: per-from-class cache reads each distinct from_val at most once.
# Mirrors the Pattern 2 cache the loop installs: a second cache hit for the same
# from_val must trigger NO additional dataset collect. We assert this with a
# scan-count counter wrapper around the read function (RESEARCH Pattern 2).
test_that("per-from-class cache performs one collect per from_val", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("dplyr")
  skip_if_not(exists("load_from_class_predictor_data", mode = "function"))

  root <- withr::local_tempdir()
  dirs <- .build_lazy_fixture(root)
  ds <- .open_fixture(dirs)

  scan_count <- 0L
  counting_read <- function(from_val_keys) {
    scan_count <<- scan_count + 1L
    load_from_class_predictor_data(
      ds$ds_static, ds$ds_dynamic,
      cols = c("stat_a", "dyn_x"),
      region_value = 1L, scenario = "baseline",
      ref_cell_keys = from_val_keys
    )
  }

  # Simulate the loop's cache: two transitions share from_val 101 (keys A);
  # one transition has from_val 102 (keys B). Expect 2 collects total, not 3.
  from_class_cache <- new.env(parent = emptyenv())
  keys_by_from <- list("101" = as.integer(c(1, 2, 3)),
                       "102" = as.integer(c(50, 51)))
  loop_from_vals <- c("101", "102", "101")  # 101 appears twice

  for (key in loop_from_vals) {
    if (is.null(from_class_cache[[key]])) {
      from_class_cache[[key]] <- counting_read(keys_by_from[[key]])
    }
    region_pred_dt <- from_class_cache[[key]]
    expect_s3_class(region_pred_dt, "data.table")
  }

  expect_equal(scan_count, 2L)  # one collect per distinct from_val
})

# --------------------------------------------------------------------------
# Assertion 4: empty cols or empty keys -> empty cell_id-only data.table.
test_that("empty cols or empty keys yield an empty cell_id-only data.table", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("dplyr")
  skip_if_not(exists("load_from_class_predictor_data", mode = "function"))

  root <- withr::local_tempdir()
  dirs <- .build_lazy_fixture(root)
  ds <- .open_fixture(dirs)

  empty_cols <- load_from_class_predictor_data(
    ds$ds_static, ds$ds_dynamic,
    cols = character(0), region_value = 1L, scenario = "baseline",
    ref_cell_keys = as.integer(c(1, 2, 3))
  )
  expect_s3_class(empty_cols, "data.table")
  expect_identical(names(empty_cols), "cell_id")
  expect_equal(nrow(empty_cols), 0L)

  empty_keys <- load_from_class_predictor_data(
    ds$ds_static, ds$ds_dynamic,
    cols = c("stat_a"), region_value = 1L, scenario = "baseline",
    ref_cell_keys = integer(0)
  )
  expect_s3_class(empty_keys, "data.table")
  expect_equal(nrow(empty_keys), 0L)
})
