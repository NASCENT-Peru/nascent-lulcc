# Phase 5 Plan 02: spatial interventions engine. Covers
#   - resolve_intervention_masks(): shared YAML -> mask path resolver (D-05 bare
#     filenames under mask_dir, D-14 fail-fast on implemented years without a
#     Dynamic entry). Consumed later by the Stage 7 pre-flight and the
#     standalone validator.
#   - .mask_inside_lut(): per-call cached logical LUT indexed by region cell_id,
#     built from a single cell-number terra::extract per mask file.
#   - implement_spatial_interventions() + absolute/relative helpers: fail-fast on
#     missing masks, NaN guard, AUDIT logging (D-15).
#
# Fixtures are built in withr::local_tempdir(): a 4-row x 5-col raster mask and
# <scenario>_interventions.yml files written with yaml::write_yaml().

library(testthat)
library(data.table)

# IN-06: `.repo_root` is defined once in
# tests/testthat/helper-spatial-interventions.R, which testthat loads before
# this file under both `test_file()` and `test_dir()`. The guard keeps a bare
# `source()` of this file working WITHOUT reintroducing the sourcing-frame
# `ofile` bootstrap this plan removed - see the helper's header for why that
# bootstrap was wrong twice over.
if (!exists(".repo_root", inherits = TRUE)) {
  source(testthat::test_path("helper-spatial-interventions.R"))
}

# Source into the global env (not a baseenv child) so data.table's cedta check
# treats the engine code as data.table-aware.
source(file.path(.repo_root, "src", "utils.r"))
source(file.path(.repo_root, "src", "implement_spatial_interventions.R"))

# --------------------------------------------------------------------------
# Fixture helpers

# National reference cells of region cell_ids 1..6 on the 4x5 grid.
.ref_cells <- c(2L, 3L, 7L, 8L, 13L, 20L)
# Mask = 1 on national cells 2, 7, 13 -> region cell_ids 1, 3, 5 are inside.
.mask_cells <- c(2L, 7L, 13L)

# The reference grid the national cell numbers in `ref_cell_id` are defined on.
# A cell number is only meaningful on this exact grid, which is why the engine
# must refuse any mask that is not on it (CR-02).
.ref_grid <- function() {
  terra::rast(nrows = 4, ncols = 5, xmin = 0, xmax = 5, ymin = 0, ymax = 4)
}

# Write a single-layer mask onto a caller-supplied template raster. Passing a
# template that is NOT .ref_grid() is how the CR-02 mis-gridded fixtures are
# built (shifted extent, smaller extent).
#
# `value` is the burn value written at `cells`; it defaults to 1 so every
# pre-existing call site is byte-identical. Passing value = 255 is how the CR-01
# out-of-domain fixture is built (the gdal_rasterize / QGIS 8-bit default).
# `value` may also be a vector aligned element for element with `cells` (the
# `v[keep] <- value` below already supports it), which is how the GAP-2
# multiple-distinct-bad-values fixture is built. Cells not listed stay NA, so
# `cells = integer(0)` yields an entirely NA mask.
.write_mask_on <- function(dir, template, name = "mask_a.tif", cells = .mask_cells,
                           value = 1) {
  r <- terra::rast(template)
  v <- rep(NA_real_, terra::ncell(r))
  keep <- cells[cells >= 1L & cells <= terra::ncell(r)]
  v[keep] <- value
  terra::values(r) <- v
  path <- file.path(dir, name)
  terra::writeRaster(r, path, overwrite = TRUE)
  path
}

.write_mask <- function(dir, name = "mask_a.tif", cells = .mask_cells, value = 1) {
  .write_mask_on(dir, .ref_grid(), name = name, cells = cells, value = value)
}

# A true categorical GeoTIFF ON the reference grid: integer 1 at `cells`, 0
# elsewhere, plus a category table, so terra::extract() round-trips a FACTOR.
# compareGeom() and nlyr() both pass, and terra::minmax() reads 0..1, so only an
# explicit is.numeric()/is.factor() check catches it (CR-01).
.write_categorical_mask <- function(dir, name = "mask_cat.tif", cells = .mask_cells) {
  r <- .ref_grid()
  v <- rep(0L, terra::ncell(r))
  keep <- cells[cells >= 1L & cells <= terra::ncell(r)]
  v[keep] <- 1L
  terra::values(r) <- v
  levels(r) <- data.frame(value = c(0L, 1L), label = c("outside", "inside"))
  path <- file.path(dir, name)
  terra::writeRaster(r, path, overwrite = TRUE)
  path
}

# A multi-band raster ON the reference grid: compareGeom() passes (it ignores
# layer count by default), so only an explicit terra::nlyr() check catches it
# before `[[1L]]` silently takes band 1 (CR-02c).
.write_multilayer_mask <- function(dir, name = "mask_multi.tif", n_layers = 2L) {
  r <- .ref_grid()
  v <- rep(NA_real_, terra::ncell(r))
  v[.mask_cells] <- 1
  terra::values(r) <- v
  stack <- do.call(c, rep(list(r), n_layers))
  path <- file.path(dir, name)
  terra::writeRaster(stack, path, overwrite = TRUE)
  path
}

# The reference grid on disk. The engine takes a path (not a SpatRaster) so it
# owns the read and the terra pointer lifetime stays in one scope.
.write_ref_grid <- function(dir, name = "ref_grid.tif") {
  r <- .ref_grid()
  terra::values(r) <- seq_len(terra::ncell(r))
  path <- file.path(dir, name)
  terra::writeRaster(r, path, overwrite = TRUE)
  path
}

.write_yaml <- function(dir, scenario, entries) {
  path <- file.path(dir, paste0(scenario, "_interventions.yml"))
  yaml::write_yaml(entries, path)
  path
}

.cell_index <- function() {
  data.table(cell_id = 1:6, ref_cell_id = .ref_cells)
}

.static_entry <- function(id = "iv_static", mask = "mask_a.tif",
                          years = list(2028L, 2032L), stage = "Allocation",
                          ...) {
  base <- list(
    Intervention_stage = stage,
    Intervention_ID = id,
    Mask_type = "Static",
    Intervention_mask = mask,
    Time_steps_implemented = years,
    # Minimal valid Prob_adjust_* field set so mask-resolution fixtures satisfy
    # the resolver's per-entry schema gate (CR-03). Override via `...`.
    Prob_adjust_type = "Absolute",
    Prob_adjust_value = 0,
    Prob_adjust_zone = "Inside",
    Transition_target_classes = list("built_up_and_barren_lands")
  )
  utils::modifyList(base, list(...))
}

.resolver_cols <- c(
  "scenario", "intervention_id", "rank", "year", "mask_type",
  "mask_name", "mask_path", "exists", "entry_index"
)

# Log files only exist once the engine has written its first line; a resolver
# stop happens before that.
.log_lines <- function(log_file) {
  if (file.exists(log_file)) readLines(log_file) else character(0)
}

# --------------------------------------------------------------------------
# resolve_intervention_masks()

test_that("resolver: Static mask yields one row per implemented year under mask_dir", {
  scratch <- withr::local_tempdir()
  mask_dir <- file.path(scratch, "masks")
  dir.create(mask_dir)
  .write_mask(mask_dir)
  .write_yaml(scratch, "BAU", list(.static_entry(Intervention_ranking = 1L)))

  res <- resolve_intervention_masks(scratch, mask_dir, "BAU", years = c(2028L, 2032L))
  expect_s3_class(res, "data.frame")
  expect_identical(names(res), .resolver_cols)
  expect_equal(nrow(res), 2L)
  expect_identical(res$year, c(2028L, 2032L))
  expect_identical(res$mask_path, rep(file.path(mask_dir, "mask_a.tif"), 2L))
  expect_true(all(res$exists))
  expect_equal(res$rank, c(1, 1))
})

test_that("resolver: Dynamic mask picks the per-year entry", {
  scratch <- withr::local_tempdir()
  entry <- list(
    Intervention_stage = "Allocation",
    Intervention_ID = "iv_dyn",
    Mask_type = "Dynamic",
    Intervention_mask = list("2028" = "m2028.tif", "2032" = "m2032.tif"),
    Time_steps_implemented = list(2028L, 2032L),
    Prob_adjust_type = "Absolute",
    Prob_adjust_value = 0,
    Prob_adjust_zone = "Inside",
    Transition_target_classes = list("built_up_and_barren_lands")
  )
  .write_yaml(scratch, "BAU", list(entry))
  res <- resolve_intervention_masks(scratch, scratch, "BAU", years = c(2028L, 2032L))
  expect_identical(res$mask_name, c("m2028.tif", "m2032.tif"))
  expect_true(all(is.na(res$rank)))
})

test_that("resolver: non-implemented years give no rows and keep the 9 columns", {
  scratch <- withr::local_tempdir()
  .write_yaml(scratch, "BAU", list(.static_entry()))
  res <- resolve_intervention_masks(scratch, scratch, "BAU", years = 2030L)
  expect_equal(nrow(res), 0L)
  expect_identical(names(res), .resolver_cols)
})

test_that("resolver: implemented year without a Dynamic entry is an error (D-14)", {
  scratch <- withr::local_tempdir()
  entry <- list(
    Intervention_stage = "Allocation",
    Intervention_ID = "iv_dyn",
    Mask_type = "Dynamic",
    Intervention_mask = list("2028" = "m2028.tif"),
    Time_steps_implemented = list(2028L, 2032L),
    Prob_adjust_type = "Absolute",
    Prob_adjust_value = 0,
    Prob_adjust_zone = "Inside",
    Transition_target_classes = list("built_up_and_barren_lands")
  )
  .write_yaml(scratch, "BAU", list(entry))
  expect_error(
    resolve_intervention_masks(scratch, scratch, "BAU", years = 2032L),
    "no Dynamic Intervention_mask entry"
  )
})

test_that("resolver: mask names with separators or '..' are rejected (D-05)", {
  scratch <- withr::local_tempdir()
  .write_yaml(scratch, "A", list(.static_entry(mask = "spatial_masks/x.tif")))
  expect_error(resolve_intervention_masks(scratch, scratch, "A", 2028L), "bare filename")
  .write_yaml(scratch, "B", list(.static_entry(mask = "../x.tif")))
  expect_error(resolve_intervention_masks(scratch, scratch, "B", 2028L), "bare filename")
  .write_yaml(scratch, "C", list(.static_entry(mask = "sub\\x.tif")))
  expect_error(resolve_intervention_masks(scratch, scratch, "C", 2028L), "bare filename")
})

test_that("resolver: missing mask file is reported via exists = FALSE, not an error", {
  scratch <- withr::local_tempdir()
  .write_yaml(scratch, "BAU", list(.static_entry(mask = "nope.tif")))
  res <- resolve_intervention_masks(scratch, scratch, "BAU", 2028L)
  expect_equal(nrow(res), 1L)
  expect_false(res$exists)
})

test_that("resolver: non-Allocation entries are ignored", {
  scratch <- withr::local_tempdir()
  .write_yaml(scratch, "BAU", list(
    .static_entry(id = "iv_demand", stage = "Demand"),
    .static_entry(id = "iv_alloc")
  ))
  res <- resolve_intervention_masks(scratch, scratch, "BAU", 2028L)
  expect_identical(res$intervention_id, "iv_alloc")
})

test_that("resolver: duplicate Intervention_ID and unknown Mask_type stop", {
  scratch <- withr::local_tempdir()
  .write_yaml(scratch, "DUP", list(.static_entry(id = "x"), .static_entry(id = "x")))
  expect_error(resolve_intervention_masks(scratch, scratch, "DUP", 2028L), "duplicate Intervention_ID")
  bad <- .static_entry()
  bad$Mask_type <- "Weird"
  .write_yaml(scratch, "BAD", list(bad))
  expect_error(resolve_intervention_masks(scratch, scratch, "BAD", 2028L), "Unknown Mask_type")
})

test_that("resolver: missing YAML is an error", {
  scratch <- withr::local_tempdir()
  expect_error(
    resolve_intervention_masks(scratch, scratch, "NOPE", 2028L),
    "interventions YAML missing"
  )
})

# --------------------------------------------------------------------------
# .mask_inside_lut()

test_that(".mask_inside_lut maps region cell_id to mask membership and caches", {
  scratch <- withr::local_tempdir()
  path <- .write_mask(scratch)
  ref <- .ref_grid()
  cache <- new.env(parent = emptyenv())
  lut <- .mask_inside_lut(path, .cell_index(), cache, ref_grid = ref)
  expect_type(lut, "logical")
  expect_length(lut, 6L)
  expect_identical(which(lut), c(1L, 3L, 5L))

  # The cache key is no longer the bare path: it is path|mtime|size|n (IN-03).
  keys <- ls(cache, all.names = TRUE)
  expect_length(keys, 1L)
  expect_true(startsWith(keys[[1L]], normalizePath(path, mustWork = TRUE)))

  # An untouched file is still served from the cache: one key, same value.
  lut2 <- .mask_inside_lut(path, .cell_index(), cache, ref_grid = ref)
  expect_identical(lut2, lut)
  expect_length(ls(cache, all.names = TRUE), 1L)
})

test_that("IN-03: the LUT cache key tracks file mtime", {
  # Before this plan the cache key was the normalised path ALONE, and this
  # block asserted the opposite of what it asserts now: that overwriting the
  # mask on disk returned the STALE LUT. That documented staleness was the
  # IN-03 hazard - benign only while the cache stayed call-scoped, and a
  # silent-corruption bug the moment the cache was hoisted to a session-level
  # one (as `.transition_model_cache` already is). The key now carries
  # path | mtime | size | length(cell_id), so an overwritten mask cannot be
  # served from cache.
  scratch <- withr::local_tempdir()
  path <- .write_mask(scratch)
  ref <- .ref_grid()
  cache <- new.env(parent = emptyenv())
  lut <- .mask_inside_lut(path, .cell_index(), cache, ref_grid = ref)
  expect_identical(which(lut), c(1L, 3L, 5L))

  .write_mask(scratch, cells = c(3L, 8L))
  Sys.setFileTime(path, Sys.time() + 5)
  lut2 <- .mask_inside_lut(path, .cell_index(), cache, ref_grid = ref)
  expect_identical(which(lut2), c(2L, 4L))
  expect_length(ls(cache, all.names = TRUE), 2L)
})

test_that("CR-02a: a mask on a shifted grid is rejected, not silently applied", {
  scratch <- withr::local_tempdir()
  shifted <- terra::rast(nrows = 4, ncols = 5, xmin = 100, xmax = 105, ymin = 0, ymax = 4)
  path <- .write_mask_on(scratch, shifted, name = "mask_shift.tif")
  # Same dimensions, different geography: pre-fix this produced NO warning at
  # all and applied an entirely fictional cell set.
  expect_error(
    .mask_inside_lut(
      path, .cell_index(), new.env(parent = emptyenv()), ref_grid = .ref_grid()
    ),
    "not on the reference grid"
  )
})

test_that("CR-02b: a mask with a smaller extent is rejected before out-of-range extraction", {
  scratch <- withr::local_tempdir()
  small <- terra::rast(nrows = 2, ncols = 2, xmin = 0, xmax = 2, ymin = 0, ymax = 2)
  path <- .write_mask_on(scratch, small, name = "mask_small.tif", cells = c(1L, 2L))
  # Pre-fix, terra::extract() emitted "[extract] out of range cell numbers
  # detected" to stderr - never to the worker log - and the run continued on a
  # truncated cell set. The geometry check must fire first, as an error.
  expect_no_warning(
    expect_error(
      .mask_inside_lut(
        path, .cell_index(), new.env(parent = emptyenv()), ref_grid = .ref_grid()
      ),
      "not on the reference grid"
    )
  )
})

test_that("CR-02c: a multi-layer mask is rejected", {
  scratch <- withr::local_tempdir()
  path <- .write_multilayer_mask(scratch)
  expect_error(
    .mask_inside_lut(
      path, .cell_index(), new.env(parent = emptyenv()), ref_grid = .ref_grid()
    ),
    "has 2 layers (expected 1)",
    fixed = TRUE
  )
})

test_that("WR-10: degenerate cell_index is rejected", {
  scratch <- withr::local_tempdir()
  path <- .write_mask(scratch)
  ref <- .ref_grid()
  msg <- "cell_index\\$cell_id must be non-empty, non-NA and >= 1"

  # max(integer(0)) is -Inf -> logical(-Inf) errored opaquely.
  expect_error(
    .mask_inside_lut(
      path, data.table(cell_id = integer(0), ref_cell_id = integer(0)),
      new.env(parent = emptyenv()), ref_grid = ref
    ),
    msg
  )
  # max() with an NA is NA -> "vector size cannot be NA".
  expect_error(
    .mask_inside_lut(
      path, data.table(cell_id = c(1L, NA_integer_), ref_cell_id = c(2L, 7L)),
      new.env(parent = emptyenv()), ref_grid = ref
    ),
    msg
  )
  # cell_id == 0 silently shortened the subscript vector and mis-selected rows.
  expect_error(
    .mask_inside_lut(
      path, data.table(cell_id = c(0L, 1L), ref_cell_id = c(2L, 7L)),
      new.env(parent = emptyenv()), ref_grid = ref
    ),
    msg
  )
})

test_that("WR-10b: out-of-range ref_cell_id is rejected", {
  scratch <- withr::local_tempdir()
  path <- .write_mask(scratch)
  expect_error(
    .mask_inside_lut(
      path, data.table(cell_id = c(1L, 2L), ref_cell_id = c(2L, 25L)),
      new.env(parent = emptyenv()), ref_grid = .ref_grid()
    ),
    "ref_cell_id .* outside the reference grid"
  )
})

test_that("IN-07: a mask removed after resolution reports the forbidden marker", {
  scratch <- withr::local_tempdir()
  mask_dir <- file.path(scratch, "masks")
  dir.create(mask_dir)
  path <- .write_mask(mask_dir)
  .write_yaml(scratch, "TOCTOU", list(.static_entry()))
  res <- resolve_intervention_masks(scratch, mask_dir, "TOCTOU", years = 2028L)
  expect_true(all(res$exists))

  # beegfs drops the mount between the resolver's file.exists() and the read.
  expect_true(file.remove(path))
  err <- tryCatch(
    .mask_inside_lut(
      res$mask_path[1], .cell_index(), new.env(parent = emptyenv()),
      ref_grid = .ref_grid()
    ),
    error = function(e) conditionMessage(e)
  )
  # verify_intervention_smoke.r treats this literal as fatal.
  expect_true(grepl("intervention mask missing", err, fixed = TRUE))
})

test_that("CR-01: a mask burned with 255 is rejected, not read as empty", {
  # 255 is the default burn value of gdal_rasterize and of QGIS "Rasterize" on
  # an 8-bit output. Pre-fix, `v == 1` was FALSE everywhere, so the LUT was all
  # FALSE and `Prob_adjust_zone: Outside` inverted onto the whole region with no
  # error and no log line.
  scratch <- withr::local_tempdir()
  path <- .write_mask(scratch, name = "mask_255.tif", value = 255)
  ref <- terra::rast(.write_ref_grid(scratch))
  expect_error(
    .mask_inside_lut(
      path, .cell_index(), new.env(parent = emptyenv()), ref_grid = ref
    ),
    "outside {0,1,NA}",
    fixed = TRUE
  )
  # The offending value must be reported, not just its existence.
  expect_error(
    .mask_inside_lut(
      path, .cell_index(), new.env(parent = emptyenv()), ref_grid = ref
    ),
    "255",
    fixed = TRUE
  )
})

test_that("CR-01: a categorical mask is rejected before the 0/1 comparison", {
  # terra::extract() returns a factor for a categorical GeoTIFF, so `v != 0` and
  # `v != 1` are meaningless there; the non-numeric guard has to fire first.
  scratch <- withr::local_tempdir()
  path <- .write_categorical_mask(scratch)
  ref <- terra::rast(.write_ref_grid(scratch))
  expect_error(
    .mask_inside_lut(
      path, .cell_index(), new.env(parent = emptyenv()), ref_grid = ref
    ),
    "is categorical/non-numeric",
    fixed = TRUE
  )
})

test_that("CR-01: a correctly 1-coded mask still yields the same LUT", {
  # No-regression control for the two guards above: .mask_cells are national
  # cells 2, 7, 13 -> region cell_ids 1, 3, 5.
  scratch <- withr::local_tempdir()
  path <- .write_mask(scratch)
  ref <- terra::rast(.write_ref_grid(scratch))
  lut <- .mask_inside_lut(
    path, .cell_index(), new.env(parent = emptyenv()), ref_grid = ref
  )
  expect_identical(lut, c(TRUE, FALSE, TRUE, FALSE, TRUE, FALSE))
})

# --------------------------------------------------------------------------
# R51 gap-closure: engine mask guard (GAP-2, R51-IN-02, R51-IN-03)
#
# Every fixture above is either a 0/1 mask with at least one 1 inside the
# region, or a 255/categorical mask, which is exactly why an all-zero mask
# shipped: it is in domain, so it passed the CR-01 guard and produced an
# all-FALSE LUT in silence.

test_that("GAP-2: an all-zero mask is rejected by the engine, with the colon-free wording", {
  scratch <- withr::local_tempdir()
  path <- .write_mask(scratch, name = "mask_zero.tif", cells = .mask_cells, value = 0)
  ref <- terra::rast(.write_ref_grid(scratch))
  expect_error(
    .mask_inside_lut(
      path, .cell_index(), new.env(parent = emptyenv()), ref_grid = ref
    ),
    "has no cell equal to 1",
    fixed = TRUE
  )
  msg <- tryCatch(
    .mask_inside_lut(
      path, .cell_index(), new.env(parent = emptyenv()), ref_grid = ref
    ),
    error = function(e) conditionMessage(e)
  )
  # The engine emits the COLON-FREE form; the Stage 7 pre-flight is the emitter
  # that carries the colon. The two wordings are deliberate, not a drift.
  expect_true(grepl("intervention mask ", msg, fixed = TRUE))
  expect_false(grepl("intervention mask: ", msg, fixed = TRUE))
  # An all-zero mask is IN domain, so the value-domain stop must not be what
  # fired here.
  expect_false(grepl("outside {0,1,NA}", msg, fixed = TRUE))
})

test_that("GAP-2: a mask whose only values lie outside the region is still rejected", {
  # National cells 1 and 4 are not in .ref_cells, so every value this region
  # samples is NA while the mask itself is not all-NA. A guard decided on the
  # region sample alone would pass this; the national value table catches it.
  scratch <- withr::local_tempdir()
  path <- .write_mask(scratch, name = "mask_zero_out.tif", cells = c(1L, 4L), value = 0)
  ref <- terra::rast(.write_ref_grid(scratch))
  msg <- tryCatch(
    .mask_inside_lut(
      path, .cell_index(), new.env(parent = emptyenv()), ref_grid = ref
    ),
    error = function(e) conditionMessage(e)
  )
  expect_true(grepl("has no cell equal to 1", msg, fixed = TRUE))
  expect_false(grepl("intervention mask: ", msg, fixed = TRUE))
  expect_false(grepl("outside {0,1,NA}", msg, fixed = TRUE))
})

test_that("GAP-2: an all-NA mask is tolerated by the engine guard", {
  # An empty national value table means "no data anywhere", which is tolerated
  # here exactly as the Stage 7 pre-flight tolerates it: judging a degenerate
  # mask is the smoke verifier's Outside-zone proof's job, not this function's.
  # This also pins the branch ORDER: any(logical(0)) is FALSE, so if the no-1
  # test ran before the empty-table test this block would go red.
  scratch <- withr::local_tempdir()
  path <- .write_mask(scratch, name = "mask_na.tif", cells = integer(0))
  ref <- terra::rast(.write_ref_grid(scratch))
  expect_no_error(
    lut <- .mask_inside_lut(
      path, .cell_index(), new.env(parent = emptyenv()), ref_grid = ref
    )
  )
  expect_identical(lut, rep(FALSE, 6L))
})

test_that("GAP-2: a validly 1-coded mask that misses the region is not an error", {
  # The false-reject guard. A national mask is applied region by region, so a
  # region its polygons do not reach is routine; stopping here would FAIL every
  # such region. The 1s sit on national cells 1 and 4, outside .ref_cells.
  scratch <- withr::local_tempdir()
  path <- .write_mask(scratch, name = "mask_elsewhere.tif", cells = c(1L, 4L), value = 1)
  ref <- terra::rast(.write_ref_grid(scratch))
  expect_no_error(
    lut <- .mask_inside_lut(
      path, .cell_index(), new.env(parent = emptyenv()), ref_grid = ref
    )
  )
  expect_identical(lut, rep(FALSE, 6L))
})

test_that("R51-IN-02: categorical is asserted with is.factor, not inferred from the extract() return type", {
  # terra::extract() returning a factor for a categorical raster is an
  # undocumented implementation detail. Mock it to return plain numeric 1s -- a
  # value set that is in domain AND contains a 1, so every other guard passes --
  # and the stop must still fire, which it can only do via terra::is.factor(m).
  scratch <- withr::local_tempdir()
  path <- .write_categorical_mask(scratch)
  ref <- terra::rast(.write_ref_grid(scratch))
  # MOCK-BITE GUARD: terra::extract is an S4 generic, so interception is not
  # guaranteed. Count the calls and assert the mock actually ran; without this
  # the block would pass on the real factor-returning extract() and prove
  # nothing about is.factor().
  hits <- new.env(parent = emptyenv())
  hits$n <- 0L
  testthat::local_mocked_bindings(
    extract = function(x, y, ...) {
      hits$n <- hits$n + 1L
      data.frame(lyr1 = rep(1, length(y)))
    },
    .package = "terra"
  )
  expect_error(
    .mask_inside_lut(
      path, .cell_index(), new.env(parent = emptyenv()), ref_grid = ref
    ),
    "is categorical/non-numeric",
    fixed = TRUE
  )
  expect_gte(hits$n, 1L)
})

test_that("R51-IN-03: the bad-value list reports its true size and says when it is truncated", {
  scratch <- withr::local_tempdir()
  ref <- terra::rast(.write_ref_grid(scratch))
  many <- .write_mask(
    scratch, name = "mask_six_bad.tif", cells = .ref_cells, value = c(2, 3, 4, 5, 6, 7)
  )
  msg_many <- tryCatch(
    .mask_inside_lut(
      many, .cell_index(), new.env(parent = emptyenv()), ref_grid = ref
    ),
    error = function(e) conditionMessage(e)
  )
  expect_true(grepl("6 distinct bad value(s)", msg_many, fixed = TRUE))
  expect_true(grepl("showing the first 5", msg_many, fixed = TRUE))

  one <- .write_mask(scratch, name = "mask_one_bad.tif", value = 255)
  msg_one <- tryCatch(
    .mask_inside_lut(
      one, .cell_index(), new.env(parent = emptyenv()), ref_grid = ref
    ),
    error = function(e) conditionMessage(e)
  )
  expect_true(grepl("1 distinct bad value(s)", msg_one, fixed = TRUE))
  expect_false(grepl("showing the first", msg_one, fixed = TRUE))
})

# --------------------------------------------------------------------------
# implement_spatial_interventions() engine

.class_map <- c(
  forested_areas = 101L,
  low_intensity_agricultural_areas = 103L,
  high_intensity_agricultural_areas = 104L,
  built_up_and_barren_lands = 105L,
  mining = 106L
)

# Region cells 1..6; cells 1, 3, 5 are inside the mask. Cell 5 has from_val
# 102, all others 101. Two target classes (105, 104) per cell.
.norm <- function() {
  cells <- 1:6
  from <- c(101L, 101L, 101L, 101L, 102L, 101L)
  dt <- rbind(
    data.table(cell_id = cells, from_val = from, to_val = 105L,
               prob = c(0.2, 0.3, 0.4, 0.5, 0.6, 0.1)),
    data.table(cell_id = cells, from_val = from, to_val = 104L,
               prob = rep(0.1, 6))
  )
  dt[, row_idx := seq_len(.N)]
  dt[, x := as.numeric(cell_id)]
  dt[, y := 1]
  setcolorder(dt, c("row_idx", "from_val", "to_val", "cell_id", "x", "y", "prob"))
  dt
}

.engine_fixture <- function(entries, scenario = "BAU", write_mask = TRUE) {
  scratch <- withr::local_tempdir(.local_envir = parent.frame())
  mask_dir <- file.path(scratch, "masks")
  dir.create(mask_dir)
  if (write_mask) .write_mask(mask_dir)
  .write_yaml(scratch, scenario, entries)
  list(
    interventions_dir = scratch,
    mask_dir = mask_dir,
    ref_grid_path = .write_ref_grid(scratch),
    scenario = scenario,
    log_file = file.path(scratch, "t.log")
  )
}

#' `telemetry_dir` is only forwarded when the caller supplies it, so the blocks
#' that predate plan 05-13 keep calling the engine with its original formals and
#' the D-18 CSV stays opt-in (NULL = fixture mode).
.run_engine <- function(fx, normalized, year = 2028L, telemetry_dir = NULL,
                        region_label = "R1") {
  args <- list(
    normalized = normalized,
    cell_index = .cell_index(),
    class_name_to_value = .class_map,
    interventions_dir = fx$interventions_dir,
    mask_dir = fx$mask_dir,
    ref_grid_path = fx$ref_grid_path,
    scenario = fx$scenario,
    simulation_time_step = year,
    log_file = fx$log_file,
    region_label = region_label
  )
  if (!is.null(telemetry_dir)) args$telemetry_dir <- telemetry_dir
  do.call(implement_spatial_interventions, args)
}

.abs_entry <- function(id = "iv_abs", zone = "Inside", value = 0,
                       targets = list("built_up_and_barren_lands"),
                       from = list("None"), rank = 1L, mask = "mask_a.tif") {
  list(
    Intervention_stage = "Allocation",
    Intervention_ID = id,
    Intervention_ranking = rank,
    Mask_type = "Static",
    Intervention_mask = mask,
    Time_steps_implemented = list(2028L),
    Prob_adjust_type = "Absolute",
    Prob_adjust_zone = zone,
    Prob_adjust_value = value,
    Transition_target_classes = targets,
    From_lulc_filter = from
  )
}

.rel_entry <- function(id = "iv_rel", valency = "Decrease", zone = "Inside",
                       targets = list("built_up_and_barren_lands"), rank = 1L) {
  list(
    Intervention_stage = "Allocation",
    Intervention_ID = id,
    Intervention_ranking = rank,
    Mask_type = "Static",
    Intervention_mask = "mask_a.tif",
    Time_steps_implemented = list(2028L),
    Prob_adjust_type = "Relative",
    Prob_adjust_valency = valency,
    Prob_adjust_zone = zone,
    Prob_adjust_intervention_percentile = 50,
    Prob_adjust_non_intervention_percentile = 50,
    Prob_adjust_threshold = 5,
    Transition_target_classes = targets,
    From_lulc_filter = list("None")
  )
}

.audit_lines <- function(log_file) {
  l <- readLines(log_file)
  list(
    iv = grep("AUDIT stage=intervention region=", l, value = TRUE, fixed = TRUE),
    summary = grep("AUDIT stage=intervention_summary", l, value = TRUE, fixed = TRUE),
    all = l
  )
}

# The intervention AUDIT line is written through log_msg(), so every log line
# carries a "<timestamp> | " prefix. Telemetry assertions are made over the
# AUDIT body only, so the frozen field order can be pinned with one regex and
# the field count is comparable across runs. These three live here rather than
# in the plan 05-12 section below because test bodies execute in file order and
# blocks above that section read AUDIT fields too.
.audit_body <- function(line) {
  at <- regexpr("AUDIT stage=intervention ", line, fixed = TRUE)
  substring(line, at)
}

.audit_nfields <- function(line) {
  length(strsplit(.audit_body(line), " ", fixed = TRUE)[[1L]])
}

.audit_field <- function(line, name) {
  m <- regmatches(line, regexpr(paste0("(^| )", name, "=[^ ]+"), line))
  if (length(m) == 0L) return(NA_character_)
  sub(paste0("^ ?", name, "="), "", m)
}

test_that("engine: Absolute=0 Inside with From filter edits only inside from-class rows", {
  fx <- .engine_fixture(list(.abs_entry(from = list("forested_areas"))))
  dt <- .norm()
  before <- copy(dt)
  out <- .run_engine(fx, dt)
  sel <- out$to_val == 105L & out$cell_id %in% c(1L, 3L)
  expect_true(all(out$prob[sel] == 0))
  expect_equal(out$prob[!sel], before$prob[!sel])

  a <- .audit_lines(fx$log_file)
  expect_length(a$iv, 1L)
  expect_match(a$iv, "region=R1 scenario=BAU year=2028 id=iv_abs rank=1 type=Absolute zone=Inside to_vals=105 mask=mask_a.tif", fixed = TRUE)
  expect_match(a$iv, "rows_changed=2", fixed = TRUE)
  expect_length(a$summary, 1L)
  expect_match(a$summary, "n_interventions=1", fixed = TRUE)
})

test_that("engine: Absolute=0 Outside only changes outside rows", {
  fx <- .engine_fixture(list(.abs_entry(zone = "Outside")))
  dt <- .norm()
  before <- copy(dt)
  out <- .run_engine(fx, dt)
  sel <- out$to_val == 105L & out$cell_id %in% c(2L, 4L, 6L)
  expect_true(all(out$prob[sel] == 0))
  expect_equal(out$prob[!sel], before$prob[!sel])
  expect_match(.audit_lines(fx$log_file)$iv, "rows_target=3 rows_changed=3", fixed = TRUE)
})

test_that("engine: year not implemented leaves normalized unchanged", {
  fx <- .engine_fixture(list(.abs_entry()))
  dt <- .norm()
  before <- copy(dt)
  out <- .run_engine(fx, dt, year = 2030L)
  expect_identical(out, before)
  a <- .audit_lines(fx$log_file)
  expect_length(a$iv, 0L)
  expect_length(a$summary, 1L)
  expect_match(a$summary, "n_interventions=0", fixed = TRUE)
})

test_that("engine: missing mask file stops before touching probabilities (D-14)", {
  fx <- .engine_fixture(list(.abs_entry()), write_mask = FALSE)
  dt <- .norm()
  before <- copy(dt)
  expect_error(.run_engine(fx, dt), "intervention mask missing")
  expect_identical(dt, before)
})

test_that("engine: NaN path (no positive inside probabilities) is skipped and logged", {
  fx <- .engine_fixture(list(.rel_entry(targets = list("high_intensity_agricultural_areas"))))
  dt <- .norm()
  dt[to_val == 104L & cell_id %in% c(1L, 3L, 5L), prob := 0]
  before <- copy(dt)
  expect_no_error(out <- .run_engine(fx, dt))
  expect_equal(out$prob, before$prob)
  expect_true(any(grepl("skip", .audit_lines(fx$log_file)$all, fixed = TRUE)))
})

test_that("engine: Relative Decrease lowers inside values and never raises any", {
  fx <- .engine_fixture(list(.rel_entry()))
  dt <- .norm()
  before <- copy(dt)
  out <- .run_engine(fx, dt)
  inside <- out$to_val == 105L & out$cell_id %in% c(1L, 3L, 5L)
  expect_true(any(out$prob[inside] < before$prob[inside]))
  expect_true(all(out$prob <= before$prob + 1e-12))
})

test_that("WR-09: zeros from Absolute=0 survive a Relative Increase that actually runs", {
  # This block used to pass for the wrong reason (05-REVIEW.md WR-09). Its
  # rank-1 entry was an unfiltered .abs_entry(), which zeroes EVERY inside row
  # of the target class, so the rank-2 Relative Increase found no positive
  # probabilities in its intervention zone, the NaN guard fired, and the
  # adjuster returned via `next` without touching anything. What the block
  # actually proved was "a skipped intervention changes nothing" - the zeros
  # were never at risk.
  #
  # The fix is to zero only PART of one zone, so both zones still hold
  # positives and the guard cannot fire. `from = "forested_areas"` restricts
  # rank 1 to from_val 101, and inside cell 5 carries from_val 102, so after
  # rank 1 the inside rows of class 105 are (cell 1) 0, (cell 3) 0,
  # (cell 5) 0.6 while the outside rows keep 0.3, 0.5, 0.1.
  #
  # Rank 2 then computes, by hand:
  #   Intervention_ptile_val  = quantile(0.6, .5)         = 0.6
  #     (the percentile is taken over the POSITIVE intervention values only,
  #      which is the first reason a zeroed row can never be selected)
  #   Intervention_ptile_mean = mean(0.6)                 = 0.60
  #   Non_int_ptile_val       = quantile(c(.3,.5,.1), .5) = 0.3
  #   Non_int_ptile_mean      = mean(.3, .5)              = 0.40
  #   Perc_diff = (0.60 - 0.40) / 0.50 * 100 = 40 -> Increase / Perc_diff >= 0
  # so cell 5 rises to 0.84 and the zeros at cells 1 and 3 stay zero.
  fx <- .engine_fixture(list(
    .abs_entry(id = "iv_zero", rank = 1L, from = list("forested_areas")),
    .rel_entry(id = "iv_inc", valency = "Increase", rank = 2L)
  ), scenario = "ZEROSURVIVE")
  dt <- .norm()
  before <- copy(dt)
  out <- .run_engine(fx, dt)

  # 1. Zero preservation: the rows rank 1 zeroed are still exactly 0 after the
  #    Relative pass wrote to the same target class and zone.
  zeroed <- out$to_val == 105L & out$cell_id %in% c(1L, 3L)
  expect_true(all(out$prob[zeroed] == 0))

  # 2. The table changed at all.
  expect_false(identical(out$prob, before$prob))

  # 3. The RANK-2 pass specifically ran and moved rows. This is the assertion
  #    the old block lacked: the engine writes an AUDIT line even for an
  #    adjuster that skipped every target class, so the line count alone is
  #    necessary but NOT sufficient. rows_changed / sum_abs_delta are read off
  #    the rank-2 line only.
  a <- .audit_lines(fx$log_file)
  expect_length(a$iv, 2L)
  expect_match(a$iv[1], "id=iv_zero", fixed = TRUE)
  expect_match(a$iv[2], "id=iv_inc", fixed = TRUE)
  expect_identical(.audit_field(a$iv[2], "rows_changed"), "1")
  expect_identical(.audit_field(a$iv[2], "n_inc"), "1")
  expect_gt(as.numeric(.audit_field(a$iv[2], "sum_abs_delta")), 0)
  expect_equal(
    out$prob[out$to_val == 105L & out$cell_id == 5L], 0.84, tolerance = 1e-9
  )

  # 4. No intervention was skipped, for either rank.
  expect_false(any(grepl("intervention skip:", a$all, fixed = TRUE)))
})

test_that("engine: interventions are applied in rank order with one AUDIT line each", {
  fx <- .engine_fixture(list(
    .abs_entry(id = "iv_second", rank = 2L, value = 0.05),
    .abs_entry(id = "iv_first", rank = 1L, value = 0.01)
  ))
  .run_engine(fx, .norm())
  a <- .audit_lines(fx$log_file)
  expect_length(a$iv, 2L)
  expect_match(a$iv[1], "id=iv_first", fixed = TRUE)
  expect_match(a$iv[2], "id=iv_second", fixed = TRUE)
  expect_length(a$summary, 1L)
  expect_match(a$summary, "n_interventions=2 cells_sum_gt1=0", fixed = TRUE)
})

test_that("engine: unknown class name in Transition_target_classes stops", {
  fx <- .engine_fixture(list(.abs_entry(targets = list("not_a_class"))))
  expect_error(.run_engine(fx, .norm()), "Unknown class_name")
})
# --------------------------------------------------------------------------
# Phase 5 Plan 07 gap closure: resolver-owned identity, mask type and
# Prob_adjust_* schema (05-REVIEW.md CR-01, CR-03, WR-06, IN-01).

# A complete Allocation entry that is missing only the Intervention_ID key —
# exactly what a typo'd key (`Intervention_Id`, `intervention_id`) produces.
.noid_entry <- function(rank = 1L) {
  list(
    Intervention_stage = "Allocation",
    Intervention_ranking = rank,
    Mask_type = "Static",
    Intervention_mask = "mask_a.tif",
    Time_steps_implemented = list(2028L),
    Prob_adjust_type = "Absolute",
    Prob_adjust_zone = "Inside",
    Prob_adjust_value = 0,
    Transition_target_classes = list("built_up_and_barren_lands"),
    From_lulc_filter = list("None")
  )
}

test_that("CR-01: an Allocation entry without Intervention_ID is an error, not a silent skip", {
  scratch <- withr::local_tempdir()
  .write_yaml(scratch, "NOID", list(.noid_entry()))
  expect_error(
    resolve_intervention_masks(scratch, scratch, "NOID", years = 2028L),
    "no usable Intervention_ID"
  )

  # An empty-string or non-scalar id is equally unusable.
  e_empty <- .noid_entry()
  e_empty$Intervention_ID <- ""
  .write_yaml(scratch, "EMPTYID", list(e_empty))
  expect_error(
    resolve_intervention_masks(scratch, scratch, "EMPTYID", years = 2028L),
    "no usable Intervention_ID"
  )
})

test_that("CR-03: a Relative entry missing Prob_adjust_threshold stops the resolver", {
  scratch <- withr::local_tempdir()
  bad <- .rel_entry(id = "iv_no_threshold")
  bad$Prob_adjust_threshold <- NULL
  .write_yaml(scratch, "NOTHR", list(bad))
  expect_error(
    resolve_intervention_masks(scratch, scratch, "NOTHR", years = 2028L),
    "missing/!scalar"
  )
  expect_error(
    resolve_intervention_masks(scratch, scratch, "NOTHR", years = 2028L),
    "Prob_adjust_threshold"
  )
})

test_that("CR-03: non-numeric, out-of-range and unknown Prob_adjust_* values stop the resolver", {
  scratch <- withr::local_tempdir()

  e_abc <- .rel_entry(id = "iv_abc")
  e_abc$Prob_adjust_intervention_percentile <- "abc"
  .write_yaml(scratch, "ABC", list(e_abc))
  expect_error(resolve_intervention_masks(scratch, scratch, "ABC", 2028L), "non-numeric")

  e_150 <- .rel_entry(id = "iv_150")
  e_150$Prob_adjust_non_intervention_percentile <- 150
  .write_yaml(scratch, "P150", list(e_150))
  expect_error(
    resolve_intervention_masks(scratch, scratch, "P150", 2028L),
    "percentile outside 0-100"
  )

  e_side <- .abs_entry(id = "iv_side")
  e_side$Prob_adjust_type <- "Sideways"
  .write_yaml(scratch, "SIDE", list(e_side))
  expect_error(
    resolve_intervention_masks(scratch, scratch, "SIDE", 2028L),
    "Unknown Prob_adjust_type"
  )

  e_notgt <- .abs_entry(id = "iv_notgt")
  e_notgt$Transition_target_classes <- NULL
  .write_yaml(scratch, "NOTGT", list(e_notgt))
  expect_error(
    resolve_intervention_masks(scratch, scratch, "NOTGT", 2028L),
    "Transition_target_classes"
  )
})

test_that("CR-03: the Prob_adjust_* schema is checked even when no requested year is implemented", {
  scratch <- withr::local_tempdir()
  bad <- .rel_entry(id = "iv_inactive")
  bad$Prob_adjust_threshold <- NULL
  .write_yaml(scratch, "INACT", list(bad))
  expect_error(
    resolve_intervention_masks(scratch, scratch, "INACT", years = 2030L),
    "missing/!scalar"
  )
})

test_that("WR-06: the resolver returns entry_index and the parsed Allocation entries", {
  scratch <- withr::local_tempdir()
  .write_yaml(scratch, "IDX", list(
    .static_entry(id = "iv_demand", stage = "Demand"),
    .static_entry(id = "iv_a", Intervention_ranking = 1L),
    .static_entry(id = "iv_b", Intervention_ranking = 2L)
  ))
  res <- resolve_intervention_masks(scratch, scratch, "IDX", years = 2028L)
  expect_true("entry_index" %in% names(res))
  expect_type(res$entry_index, "integer")

  entries <- attr(res, "entries")
  expect_false(is.null(entries))
  # Allocation-filtered: the Demand entry is not in the list.
  expect_length(entries, 2L)
  expect_identical(
    entries[[res$entry_index[1]]][["Intervention_ID"]],
    res$intervention_id[1]
  )
  expect_identical(
    entries[[res$entry_index[2]]][["Intervention_ID"]],
    res$intervention_id[2]
  )

  # The zero-row return carries the same contract.
  res0 <- resolve_intervention_masks(scratch, scratch, "IDX", years = 2030L)
  expect_identical(res0$entry_index, integer(0))
  expect_false(is.null(attr(res0, "entries")))
})

test_that("IN-01: an unknown Mask_type stops even when no requested year is implemented", {
  scratch <- withr::local_tempdir()
  bad <- .static_entry(id = "iv_weird")
  bad$Mask_type <- "Weird"
  .write_yaml(scratch, "IN01", list(bad))
  # .static_entry() implements 2028/2032 only: pre-fix the check lived inside
  # the per-year loop and never fired for 2030.
  expect_error(
    resolve_intervention_masks(scratch, scratch, "IN01", years = 2030L),
    "Unknown Mask_type"
  )
})

# --------------------------------------------------------------------------
# Gap closure, engine half: no silent skip, no partial mutation.

test_that("CR-01: the engine aborts on a missing Intervention_ID without mutating probabilities", {
  fx <- .engine_fixture(list(.noid_entry()), scenario = "NOID")
  dt <- .norm()
  before <- copy(dt)
  expect_error(.run_engine(fx, dt), "no usable Intervention_ID")
  expect_equal(as.data.frame(dt), as.data.frame(before))
  # The pre-fix engine logged "... - skipping intervention." and reported PASS.
  lines <- .log_lines(fx$log_file)
  expect_false(any(grepl("- skipping intervention.", lines, fixed = TRUE)))
  expect_length(grep("AUDIT stage=intervention_summary", lines, fixed = TRUE), 0L)
})

test_that("CR-03: a rank-2 entry with a missing Prob_adjust_threshold aborts before rank 1 mutates", {
  bad <- .rel_entry(id = "iv_rank2", rank = 2L)
  bad$Prob_adjust_threshold <- NULL
  fx <- .engine_fixture(
    list(.abs_entry(id = "iv_rank1", rank = 1L, value = 0), bad),
    scenario = "PARTIAL"
  )
  dt <- .norm()
  before <- copy(dt)
  expect_error(.run_engine(fx, dt), "missing/!scalar")
  # The blast radius the review reproduced: pre-fix, rank 1 had already
  # rewritten the probability surface by the time rank 2 crashed.
  expect_equal(as.data.frame(dt), as.data.frame(before))
  expect_length(
    grep("AUDIT stage=intervention region=", .log_lines(fx$log_file), fixed = TRUE),
    0L
  )
})

test_that("WR-06: the engine drives its loop off the resolver rows, one AUDIT line per row", {
  fx <- .engine_fixture(
    list(
      .abs_entry(id = "iv_second", rank = 2L, value = 0.05),
      .abs_entry(id = "iv_first", rank = 1L, value = 0.01)
    ),
    scenario = "WR06"
  )
  .run_engine(fx, .norm())
  a <- .audit_lines(fx$log_file)
  expect_length(a$iv, 2L)
  expect_true(all(nzchar(a$iv)))
  expect_match(a$iv[1], "id=iv_first rank=1 ", fixed = TRUE)
  expect_match(a$iv[2], "id=iv_second rank=2 ", fixed = TRUE)
})

# --------------------------------------------------------------------------
# Phase 5 Plan 09 gap closure, engine half: runtime mask geometry validation
# (05-REVIEW.md CR-02).

test_that("CR-02a: the engine aborts on a shifted mask without editing probabilities", {
  fx <- .engine_fixture(list(.abs_entry()), scenario = "SHIFT", write_mask = FALSE)
  shifted <- terra::rast(nrows = 4, ncols = 5, xmin = 100, xmax = 105, ymin = 0, ymax = 4)
  .write_mask_on(fx$mask_dir, shifted, name = "mask_a.tif")
  dt <- .norm()
  before <- copy(dt)
  # Pre-fix this reported rows_target=3 rows_changed=3 on a fictional geography.
  expect_error(.run_engine(fx, dt), "not on the reference grid")
  expect_equal(as.data.frame(dt), as.data.frame(before))
  expect_length(
    grep("AUDIT stage=intervention region=", .log_lines(fx$log_file), fixed = TRUE),
    0L
  )
})

test_that("CR-02: an unreadable reference grid stops the engine", {
  fx <- .engine_fixture(list(.abs_entry()), scenario = "NOREF")
  fx$ref_grid_path <- file.path(fx$interventions_dir, "no_such_ref_grid.tif")
  dt <- .norm()
  before <- copy(dt)
  expect_error(.run_engine(fx, dt), "intervention ref grid unreadable")
  expect_equal(as.data.frame(dt), as.data.frame(before))
})

# --------------------------------------------------------------------------
# R51 gap-closure: engine WARN and abort (GAP-2)

.zone_warn_lines <- function(log_file) {
  grep("WARN intervention zone:", .log_lines(log_file), fixed = TRUE, value = TRUE)
}

test_that("GAP-2: the engine aborts on an all-zero mask without editing probabilities", {
  fx <- .engine_fixture(list(.abs_entry()), scenario = "ZEROMASK", write_mask = FALSE)
  .write_mask(fx$mask_dir, cells = .mask_cells, value = 0)
  dt <- .norm()
  before <- copy(dt)
  # Pre-fix this ran to completion against an all-FALSE LUT.
  expect_error(.run_engine(fx, dt), "has no cell equal to 1")
  expect_equal(as.data.frame(dt), as.data.frame(before))
  expect_length(
    grep("AUDIT stage=intervention region=", .log_lines(fx$log_file), fixed = TRUE),
    0L
  )
})

test_that("GAP-2: a validly 1-coded mask that misses the region logs the WARN and proceeds (zone Inside)", {
  fx <- .engine_fixture(
    list(.abs_entry(zone = "Inside")), scenario = "MISSIN", write_mask = FALSE
  )
  .write_mask(fx$mask_dir, cells = c(1L, 4L), value = 1)
  expect_no_error(.run_engine(fx, .norm()))
  warns <- .zone_warn_lines(fx$log_file)
  expect_length(
    grep(
      "WARN intervention zone: iv_abs mask mask_a.tif does not intersect region R1; Prob_adjust_zone=Inside therefore applies to none of the region",
      warns, fixed = TRUE
    ),
    1L
  )
  # The WARN must not trip any marker the smoke verifier treats as fatal.
  expect_false(any(grepl("has no cell equal to 1", warns, fixed = TRUE)))
  expect_false(any(grepl("intervention mask: ", warns, fixed = TRUE)))
})

test_that("GAP-2: a validly 1-coded mask that misses the region logs the WARN and proceeds (zone Outside)", {
  # `all` is the honest statement here: the complement of an empty Inside zone
  # is the whole region, which is the impact this phase exists to surface.
  fx <- .engine_fixture(
    list(.abs_entry(zone = "Outside")), scenario = "MISSOUT", write_mask = FALSE
  )
  .write_mask(fx$mask_dir, cells = c(1L, 4L), value = 1)
  expect_no_error(.run_engine(fx, .norm()))
  warns <- .zone_warn_lines(fx$log_file)
  expect_length(
    grep(
      "WARN intervention zone: iv_abs mask mask_a.tif does not intersect region R1; Prob_adjust_zone=Outside therefore applies to all of the region",
      warns, fixed = TRUE
    ),
    1L
  )
  expect_false(any(grepl("has no cell equal to 1", warns, fixed = TRUE)))
  expect_false(any(grepl("intervention mask: ", warns, fixed = TRUE)))
})

test_that("GAP-2: an intersecting mask logs no non-intersection WARN", {
  # The over-firing guard: a normal run must stay quiet.
  fx <- .engine_fixture(list(.abs_entry()), scenario = "INTERSECT")
  expect_no_error(.run_engine(fx, .norm()))
  expect_length(.zone_warn_lines(fx$log_file), 0L)
})

# --------------------------------------------------------------------------
# Phase 5 Plan 11 gap closure: adjustment semantics inside
# relative_prob_adjust() and absolute_prob_adjust()
# (05-REVIEW.md WR-01, WR-02, WR-03, IN-04).

# Rewrite only the to_val 105 rows: region cells 1, 3, 5 are inside the mask,
# 2, 4, 6 outside. The to_val 104 rows keep .norm()'s 0.1 so they can double as
# out-of-scope witnesses.
.norm_split <- function(inside, outside) {
  dt <- .norm()
  dt[to_val == 105L & cell_id %in% c(1L, 3L, 5L), prob := inside]
  dt[to_val == 105L & cell_id %in% c(2L, 4L, 6L), prob := outside]
  dt
}

.because_lines <- function(log_file) {
  grep(
    "because the Prob_adjust_valency is", .log_lines(log_file),
    fixed = TRUE, value = TRUE
  )
}

test_that("WR-01a: a Relative intervention with probabilities tied at the percentile still adjusts rows", {
  # quantile(c(0.4, 0.4, 0.4), 0.5) IS 0.4, so Intervention_ptile_mean was taken
  # over all three inside rows while the strict `>` selection matched none: the
  # AUDIT line reported an applied intervention with rows_changed=0.
  fx <- .engine_fixture(list(.rel_entry(valency = "Decrease")), scenario = "TIE")
  dt <- .norm_split(inside = 0.4, outside = 0.1)
  before <- copy(dt)
  out <- .run_engine(fx, dt)
  inside <- out$to_val == 105L & out$cell_id %in% c(1L, 3L, 5L)
  expect_true(any(out$prob[inside] != before$prob[inside]))
  a <- .audit_lines(fx$log_file)
  expect_length(a$iv, 1L)
  expect_false(grepl("rows_changed=0", a$iv[1], fixed = TRUE))
})

test_that("WR-01b: an exactly zero percentage difference applies the threshold (Decrease)", {
  # Identical inside/outside distributions -> Perc_diff is exactly 0, which fell
  # between `if (Perc_diff > 0)` and `else if (Perc_diff < 0)`: the threshold,
  # whose whole purpose is to guarantee a nudge when the zones are
  # indistinguishable, was silently skipped.
  fx <- .engine_fixture(list(.rel_entry(valency = "Decrease")), scenario = "ZERODEC")
  dt <- .norm_split(inside = 0.3, outside = 0.3)
  before <- copy(dt)
  out <- .run_engine(fx, dt)
  inside <- out$to_val == 105L & out$cell_id %in% c(1L, 3L, 5L)
  expect_true(all(out$prob[inside] < before$prob[inside]))
  a <- .audit_lines(fx$log_file)
  expect_length(a$iv, 1L)
  expect_false(grepl("rows_changed=0", a$iv[1], fixed = TRUE))
  expect_true(any(grepl(
    "The Percentage difference is below the threshold", a$all, fixed = TRUE
  )))
})

test_that("WR-01c: an exactly zero percentage difference applies the threshold (Increase)", {
  fx <- .engine_fixture(list(.rel_entry(valency = "Increase")), scenario = "ZEROINC")
  dt <- .norm_split(inside = 0.3, outside = 0.3)
  before <- copy(dt)
  out <- .run_engine(fx, dt)
  inside <- out$to_val == 105L & out$cell_id %in% c(1L, 3L, 5L)
  expect_true(all(out$prob[inside] > before$prob[inside]))
  expect_true(any(grepl(
    "The Percentage difference is below the threshold",
    .log_lines(fx$log_file), fixed = TRUE
  )))
})

test_that("WR-02: an Absolute intervention does not rewrite rows outside its target class", {
  # The 104 row is outside the mask and outside the declared target class, and
  # carries a deliberately out-of-range 1.7. The table-wide clamp rewrote it to
  # 1 without counting it in rows_changed.
  fx <- .engine_fixture(list(.abs_entry()), scenario = "SCOPEABS")
  dt <- .norm()
  dt[to_val == 104L & cell_id == 2L, prob := 1.7]
  out <- .run_engine(fx, dt)
  expect_equal(out$prob[out$to_val == 104L & out$cell_id == 2L], 1.7)
  inside <- out$to_val == 105L & out$cell_id %in% c(1L, 3L, 5L)
  expect_true(all(out$prob[inside] == 0))
})

test_that("WR-02: a Relative intervention does not rewrite rows outside its target class", {
  fx <- .engine_fixture(list(.rel_entry(valency = "Decrease")), scenario = "SCOPEREL")
  dt <- .norm()
  dt[to_val == 104L & cell_id == 2L, prob := 1.7]
  before <- copy(dt)
  out <- .run_engine(fx, dt)
  expect_equal(out$prob[out$to_val == 104L & out$cell_id == 2L], 1.7)
  inside <- out$to_val == 105L & out$cell_id %in% c(1L, 3L, 5L)
  expect_true(any(out$prob[inside] < before$prob[inside]))
})

test_that("WR-03: the threshold log reports the signed value actually assigned (Increase)", {
  # Perc_diff = (0.30 - 0.31) / 0.305 * 100 = -3.279, below the threshold of 5.
  # The Increase / Perc_diff < 0 branch assigns -(threshold) but logged the
  # pre-threshold Perc_diff.
  fx <- .engine_fixture(list(.rel_entry(valency = "Increase")), scenario = "SIGNINC")
  .run_engine(fx, .norm_split(inside = 0.30, outside = 0.31))
  l <- .log_lines(fx$log_file)
  expect_true(any(grepl("setting to threshold value: -5", l, fixed = TRUE)))
  expect_false(any(grepl("setting to threshold value: -3", l, fixed = TRUE)))
})

test_that("WR-03: the threshold log reports the signed value actually assigned (Decrease)", {
  # Same Perc_diff < 0 magnitude; the Decrease branch assigns -(threshold) while
  # logging the positive threshold.
  fx <- .engine_fixture(list(.rel_entry(valency = "Decrease")), scenario = "SIGNDEC")
  .run_engine(fx, .norm_split(inside = 0.30, outside = 0.31))
  l <- .log_lines(fx$log_file)
  expect_true(any(grepl("setting to threshold value: -5", l, fixed = TRUE)))
  expect_false(any(grepl("setting to threshold value: 5", l, fixed = TRUE)))
})

test_that("IN-04: the valency explanation line is not an orphan fragment", {
  fx <- .engine_fixture(list(.rel_entry(valency = "Decrease")), scenario = "ORPHAN")
  .run_engine(fx, .norm())
  b <- .because_lines(fx$log_file)
  expect_gt(length(b), 0L)
  for (ln in b) {
    tv <- as.integer(regexpr("to_val=", ln, fixed = TRUE))
    bc <- as.integer(regexpr("because the Prob_adjust_valency is", ln, fixed = TRUE))
    expect_gt(tv, 0L)
    expect_lt(tv, bc)
  }
})

# --------------------------------------------------------------------------
# Phase 5 Plan 12 gap closure: probability-change telemetry (D-18, D-19, D-20)
# with regression tests that fail against the pre-plan engine (D-23).

# .audit_body(), .audit_nfields() and .audit_field() are defined next to
# .audit_lines() above, because test bodies run in file order and the WR-09
# block earlier in this file reads AUDIT fields too.

# Field count of the AUDIT body BEFORE plan 05-12 appended the delta fields:
#   AUDIT stage=intervention region= scenario= year= id= rank= type= zone=
#   to_vals= mask= rows_target= rows_changed=
.AUDIT_FIELDS_PRE_05_12 <- 13L

# The per-target-class statistics contract returned by both adjusters (D-18).
.stats_cols <- c(
  "target_class", "n_target", "n_changed", "mean_before", "mean_after",
  "sd_before", "sd_after", "p05_delta", "p25_delta", "p50_delta", "p75_delta",
  "p95_delta", "min_delta", "max_delta", "sum_abs_delta", "prob_mass_before",
  "prob_mass_after", "n_inc", "n_dec"
)

# inside_lut for the .norm() fixture: region cell_ids 1, 3, 5 are inside.
.inside_lut <- function() {
  lut <- logical(6L)
  lut[c(1L, 3L, 5L)] <- TRUE
  lut
}

test_that("D-18: the AUDIT intervention line keeps its shipped prefix and appends delta fields", {
  fx <- .engine_fixture(list(.abs_entry()))
  .run_engine(fx, .norm())
  a <- .audit_lines(fx$log_file)
  expect_length(a$iv, 1L)

  # The shipped prefix, byte for byte, exactly as the pre-05-12 tests pin it.
  expect_match(
    a$iv,
    "region=R1 scenario=BAU year=2028 id=iv_abs rank=1 type=Absolute zone=Inside to_vals=105 mask=mask_a.tif",
    fixed = TRUE
  )

  # One regex over the whole line so the appended order is genuinely pinned:
  # no field may be reordered, renamed or dropped.
  expect_match(
    a$iv,
    paste0(
      "AUDIT stage=intervention region=R1 scenario=BAU year=2028 id=iv_abs ",
      "rank=1 type=Absolute zone=Inside to_vals=105 mask=mask_a\\.tif ",
      "rows_target=3 rows_changed=3 ",
      "delta_mean=\\S+ delta_med=\\S+ delta_sd=\\S+ delta_min=\\S+ delta_max=\\S+ ",
      "n_inc=[0-9]+ n_dec=[0-9]+ sum_abs_delta=\\S+$"
    )
  )

  # Exactly eight appended whitespace-delimited fields, no more, no fewer.
  expect_identical(.audit_nfields(a$iv), .AUDIT_FIELDS_PRE_05_12 + 8L)

  # The shipped smoke verifier's own id extraction must still work.
  expect_identical(sub("^.* id=([^ ]+) .*$", "\\1", a$iv), "iv_abs")
})

test_that("D-19: delta statistics describe only this intervention's change", {
  # Inside rows of to_val 105 are cell_ids 1, 3, 5 with probs 0.2, 0.4, 0.6.
  # Absolute=0 drives all three to 0, so by hand:
  #   deltas   = -0.2, -0.4, -0.6
  #   mean     = -0.4      median = -0.4      sd = 0.2
  #   min      = -0.6      max    = -0.2
  #   n_inc    = 0         n_dec  = 3         sum|d| = 1.2
  fx <- .engine_fixture(list(.abs_entry()), scenario = "DELTA")
  .run_engine(fx, .norm())
  ln <- .audit_lines(fx$log_file)$iv
  expect_length(ln, 1L)

  expect_identical(.audit_field(ln, "delta_min"), "-0.6")
  expect_identical(.audit_field(ln, "delta_max"), "-0.2")
  expect_identical(.audit_field(ln, "n_dec"), "3")
  expect_identical(.audit_field(ln, "n_inc"), "0")
  expect_identical(.audit_field(ln, "sum_abs_delta"), "1.2")
  expect_equal(as.numeric(.audit_field(ln, "delta_mean")), -0.4, tolerance = 1e-6)
  expect_equal(as.numeric(.audit_field(ln, "delta_med")), -0.4, tolerance = 1e-6)
  expect_equal(as.numeric(.audit_field(ln, "delta_sd")), 0.2, tolerance = 1e-6)
})

test_that("D-19b: a second intervention's before values are the first intervention's output", {
  # Rank 1 zeroes the inside rows; rank 2 targets the same rows with the same
  # Absolute value. If "before" were the call-entry snapshot, rank 2 would
  # report the rank-1 movement again. It must report that nothing moved.
  fx <- .engine_fixture(
    list(
      .abs_entry(id = "iv_r1", rank = 1L, value = 0),
      .abs_entry(id = "iv_r2", rank = 2L, value = 0)
    ),
    scenario = "SEQ"
  )
  .run_engine(fx, .norm())
  a <- .audit_lines(fx$log_file)
  expect_length(a$iv, 2L)
  expect_match(a$iv[1], "id=iv_r1", fixed = TRUE)
  expect_match(a$iv[2], "id=iv_r2", fixed = TRUE)

  # Rank 1 sees the untouched probabilities.
  expect_identical(.audit_field(a$iv[1], "sum_abs_delta"), "1.2")
  expect_identical(.audit_field(a$iv[1], "n_dec"), "3")

  # Rank 2 sees rank 1's output: nothing left to move.
  expect_identical(.audit_field(a$iv[2], "sum_abs_delta"), "0")
  expect_identical(.audit_field(a$iv[2], "n_dec"), "0")
  expect_identical(.audit_field(a$iv[2], "n_inc"), "0")
  expect_equal(as.numeric(.audit_field(a$iv[2], "delta_max")), 0)
  expect_equal(as.numeric(.audit_field(a$iv[2], "delta_min")), 0)
})

test_that("D-18b: a skipped target class still produces a statistics row", {
  dt <- .norm()
  res <- absolute_prob_adjust(
    normalized = dt,
    Prob_adjust_zone = "Inside",
    Prob_adjust_value = 0,
    Target_classes = c(105L, 999L),
    From_filter_vals = NULL,
    inside_lut = .inside_lut(),
    log_file = NULL
  )
  expect_true(is.data.frame(res$stats))
  expect_identical(nrow(res$stats), 2L)
  expect_identical(res$stats$target_class, c(105L, 999L))

  # Class 105 was adjusted.
  expect_identical(res$stats$n_target[1], 3L)
  expect_identical(res$stats$n_changed[1], 3L)
  expect_equal(res$stats$mean_before[1], 0.4)
  expect_equal(res$stats$mean_after[1], 0)
  expect_equal(res$stats$sum_abs_delta[1], 1.2)
  expect_identical(res$stats$n_dec[1], 3L)
  expect_identical(res$stats$n_inc[1], 0L)

  # Class 999 has no rows: it still contributes a row, with the skipped shape.
  expect_identical(res$stats$n_target[2], 0L)
  expect_identical(res$stats$n_changed[2], 0L)
  expect_identical(res$stats$n_inc[2], 0L)
  expect_identical(res$stats$n_dec[2], 0L)
  expect_equal(res$stats$sum_abs_delta[2], 0)
  expect_true(is.na(res$stats$mean_before[2]))
  expect_true(is.na(res$stats$sd_after[2]))
  expect_true(is.na(res$stats$p50_delta[2]))
  expect_true(is.na(res$stats$prob_mass_before[2]))
})

test_that("D-18c: the helper statistics contract", {
  res_abs <- absolute_prob_adjust(
    normalized = .norm(),
    Prob_adjust_zone = "Inside",
    Prob_adjust_value = 0,
    Target_classes = c(105L, 999L),
    From_filter_vals = NULL,
    inside_lut = .inside_lut(),
    log_file = NULL
  )
  expect_identical(names(res_abs$stats), .stats_cols)
  expect_type(res_abs$stats$target_class, "integer")
  expect_type(res_abs$stats$n_target, "integer")
  expect_type(res_abs$stats$n_changed, "integer")
  expect_type(res_abs$stats$n_inc, "integer")
  expect_type(res_abs$stats$n_dec, "integer")
  expect_type(res_abs$stats$mean_before, "double")

  res_rel <- relative_prob_adjust(
    Prob_adjust_valency = "Decrease",
    Prob_adjust_intervention_percentile = 0.5,
    Prob_adjust_non_intervention_percentile = 0.5,
    Prob_adjust_threshold = 5,
    Prob_adjust_zone = "Inside",
    Target_classes = c(105L, 999L),
    From_filter_vals = NULL,
    inside_lut = .inside_lut(),
    normalized = .norm(),
    log_file = NULL
  )
  expect_identical(names(res_rel$stats), .stats_cols)
  expect_identical(nrow(res_rel$stats), 2L)
  expect_identical(res_rel$stats$target_class, c(105L, 999L))
  expect_identical(res_rel$stats$n_target[2], 0L)
})

test_that("D-20: no full-table copy per intervention", {
  src <- readLines(
    file.path(.repo_root, "src", "implement_spatial_interventions.R"),
    warn = FALSE
  )
  expect_false(any(grepl("copy(normalized", src, fixed = TRUE)))
  expect_false(any(grepl("data.table::copy(", src, fixed = TRUE)))
  # D-13 stays closed too: the engine never resamples or reprojects.
  expect_false(any(grepl("resample(", src, fixed = TRUE)))
  expect_false(any(grepl("project(", src, fixed = TRUE)))
})

# --------------------------------------------------------------------------
# Phase 5 Plan 13 gap closure: the per-intervention x per-target-class
# telemetry CSV (D-18 part 2), its non-fatal write rule (D-21) and filename
# safety (T-05-47), with regression tests that fail against the pre-plan
# engine (D-23).

# The D-18 CSV contract, in order. The trailing 17 columns are res$stats minus
# the AUDIT-only n_inc / n_dec; the leading 8 identify the intervention.
.csv_cols <- c(
  "scenario", "region", "year", "intervention_id", "rank", "type", "zone",
  "mask", "target_class", "n_target", "n_changed", "mean_before", "mean_after",
  "sd_before", "sd_after", "p05_delta", "p25_delta", "p50_delta", "p75_delta",
  "p95_delta", "min_delta", "max_delta", "sum_abs_delta", "prob_mass_before",
  "prob_mass_after"
)

# Paths are returned RELATIVE to `dir`, which is what makes the "nothing
# escaped the directory" assertion in D-18d meaningful.
.telemetry_csvs <- function(dir) {
  list.files(dir, pattern = "^intervention_prob_deltas_", recursive = TRUE)
}

.two_class_targets <- list(
  "built_up_and_barren_lands", "high_intensity_agricultural_areas"
)

.two_abs_entries <- function() {
  list(
    .abs_entry(id = "iv_a", rank = 1L, targets = .two_class_targets),
    .abs_entry(id = "iv_b", rank = 2L, targets = .two_class_targets)
  )
}

test_that("D-18: the telemetry CSV is written with the exact 25-column contract", {
  fx <- .engine_fixture(.two_abs_entries(), scenario = "BAU")
  tdir <- withr::local_tempdir()
  .run_engine(fx, .norm(), telemetry_dir = tdir)

  path <- file.path(tdir, "intervention_prob_deltas_BAU_r1_2028.csv")
  expect_true(file.exists(path))
  csv <- utils::read.csv(path, stringsAsFactors = FALSE)
  expect_identical(names(csv), .csv_cols)

  # The staging file never survives a successful write (atomic rename).
  expect_false(file.exists(paste0(path, ".tmp")))

  expect_identical(unique(csv$scenario), "BAU")
  expect_identical(unique(csv$region), "r1")
  expect_identical(unique(csv$year), 2028L)
  expect_identical(unique(csv$type), "Absolute")
  expect_identical(unique(csv$zone), "Inside")
  expect_identical(unique(csv$mask), "mask_a.tif")

  wrote <- grep(
    "intervention telemetry: wrote ", .log_lines(fx$log_file),
    value = TRUE, fixed = TRUE
  )
  expect_length(wrote, 1L)
  expect_match(wrote, "wrote 4 rows to ", fixed = TRUE)
})

test_that("D-18b: one row per intervention x target class", {
  fx <- .engine_fixture(.two_abs_entries(), scenario = "CARD")
  tdir <- withr::local_tempdir()
  .run_engine(fx, .norm(), telemetry_dir = tdir)

  csv <- utils::read.csv(
    file.path(tdir, "intervention_prob_deltas_CARD_r1_2028.csv"),
    stringsAsFactors = FALSE
  )
  expect_identical(nrow(csv), 4L)
  expect_identical(sort(unique(csv$intervention_id)), c("iv_a", "iv_b"))
  expect_identical(sort(unique(csv$target_class)), c(104L, 105L))
  expect_identical(
    anyDuplicated(paste(csv$intervention_id, csv$target_class)), 0L
  )

  # The CSV and the AUDIT line may never disagree about what was targeted or
  # how far it moved; plan 05-14 cross-checks the same two numbers.
  a <- .audit_lines(fx$log_file)$iv
  expect_length(a, 2L)
  for (ln in a) {
    id <- .audit_field(ln, "id")
    rows <- csv$intervention_id == id
    expect_identical(
      sum(csv$n_target[rows]), as.integer(.audit_field(ln, "rows_target"))
    )
    expect_identical(
      sum(csv$n_changed[rows]), as.integer(.audit_field(ln, "rows_changed"))
    )
    expect_equal(
      sum(csv$sum_abs_delta[rows]),
      as.numeric(.audit_field(ln, "sum_abs_delta")),
      tolerance = 1e-6
    )
  }
})

test_that("D-18c: Absolute-to-0 rows report mean_after = 0", {
  fx <- .engine_fixture(.two_abs_entries(), scenario = "ABSZERO")
  tdir <- withr::local_tempdir()
  .run_engine(fx, .norm(), telemetry_dir = tdir)

  csv <- utils::read.csv(
    file.path(tdir, "intervention_prob_deltas_ABSZERO_r1_2028.csv"),
    stringsAsFactors = FALSE
  )
  adjusted <- csv$n_target > 0L
  expect_true(any(adjusted))
  expect_true(all(csv$mean_after[adjusted] == 0))
  expect_true(all(csv$prob_mass_after[adjusted] == 0))
  # A skipped class carries the NA shape rather than a misleading 0.
  expect_true(all(is.na(csv$mean_after[!adjusted])))
})

test_that("D-21: a failing CSV write warns and does not abort the run", {
  fx <- .engine_fixture(list(.abs_entry()), scenario = "NOWRITE")
  scratch <- withr::local_tempdir()
  blocker <- file.path(scratch, "blocker")
  writeLines("not a directory", blocker)
  dead_dir <- file.path(blocker, "telemetry")

  out <- NULL
  expect_no_error(out <- .run_engine(fx, .norm(), telemetry_dir = dead_dir))

  # The allocation work itself still landed: inside rows of class 105 zeroed,
  # outside rows untouched. A telemetry failure costs telemetry, not the run.
  expect_true(all(out[to_val == 105L & cell_id %in% c(1L, 3L, 5L), prob] == 0))
  expect_true(all(out[to_val == 105L & cell_id %in% c(2L, 4L, 6L), prob] > 0))

  warn_lines <- grep(
    "WARN intervention telemetry:", .log_lines(fx$log_file),
    value = TRUE, fixed = TRUE
  )
  expect_length(warn_lines, 1L)
  expect_match(warn_lines, "failed to write ", fixed = TRUE)
  expect_length(.telemetry_csvs(scratch), 0L)

  # The whole-call summary AUDIT line is still written.
  expect_length(.audit_lines(fx$log_file)$summary, 1L)
})

test_that("D-21b: telemetry_dir = NULL writes nothing and does not warn", {
  fx <- .engine_fixture(list(.abs_entry()), scenario = "NOTEL")
  expect_no_error(.run_engine(fx, .norm(), telemetry_dir = NULL))
  l <- .log_lines(fx$log_file)
  expect_false(any(grepl("WARN intervention telemetry:", l, fixed = TRUE)))
  expect_false(any(grepl("intervention telemetry: wrote", l, fixed = TRUE)))
  expect_length(.telemetry_csvs(fx$interventions_dir), 0L)
})

test_that("D-18d: the filename is sanitised (T-05-47)", {
  # The region token is the slug the allocation driver builds
  # (gsub(" ", "_", tolower(region_label))) and the smoke verifier passes as
  # --region, so a labelled region lands where --output-root resolves it.
  fx <- .engine_fixture(list(.abs_entry()), scenario = "BAU")
  tdir <- withr::local_tempdir()
  .run_engine(fx, .norm(), telemetry_dir = tdir, region_label = "Costa Peruana")
  expect_true(file.exists(
    file.path(tdir, "intervention_prob_deltas_BAU_costa_peruana_2028.csv")
  ))

  # A hostile scenario label. `masks/../EVIL` resolves to the same YAML the
  # fixture wrote (masks/ exists inside the fixture scratch), so the engine
  # genuinely runs; the name it composes must not be able to climb out of
  # telemetry_dir.
  fx2 <- .engine_fixture(list(.abs_entry()), scenario = "masks/../EVIL")
  tdir2 <- withr::local_tempdir()
  .run_engine(fx2, .norm(), telemetry_dir = tdir2)

  rel <- .telemetry_csvs(tdir2)
  expect_length(rel, 1L)
  expect_identical(basename(rel), rel)
  expect_false(grepl("/", rel, fixed = TRUE))
  expect_false(grepl("\\", rel, fixed = TRUE))
  expect_false(grepl("..", rel, fixed = TRUE))
  expect_true(file.exists(file.path(tdir2, rel)))
})

# --------------------------------------------------------------------------
# Phase 5 Plan 15 gap closure: the valency / zone combinations the four
# shipped production configs actually use (05-REVIEW.md WR-08), and the
# rejection guard that pairs with them.
#
# Enumerated from config/{BAU,NAT,CUL,SOC}_interventions.yml and
# docs/spatial_interventions/parameter_provenance.md - NOT from review prose,
# whose WR-11 text misattributes the `Outside` + `Prob_adjust_value: 0`
# pairing. The 14 shipped Allocation entries use six distinct combinations:
#
#   type     | valency                          | zone    | covered by
#   ---------+----------------------------------+---------+---------------------
#   Absolute | n/a (value 0)                    | Inside  | "engine: Absolute=0 Inside with From filter ..."
#   Absolute | n/a (value 0)                    | Outside | "engine: Absolute=0 Outside only changes outside rows"
#   Relative | Decrease                         | Inside  | "engine: Relative Decrease lowers inside values ..."
#   Relative | Increase                         | Inside  | "WR-01c: ... applies the threshold (Increase)"
#   Relative | Decrease                         | Outside | WR-08b, below   <- was uncovered
#   Relative | Increase_inside_decrease_outside | Inside  | WR-08, below    <- was uncovered
#
# Shipped users of the two formerly uncovered rows:
#   NAT/SOC `Urban_densification`  -> Relative Increase_inside_decrease_outside, Inside
#   CUL `Mining_outside_restraint` -> Relative Decrease, Outside (no Prob_adjust_value key)
#
# Expectations are written against the post-05-11 semantics (`>=` percentile
# alignment, `Perc_diff >= 0` dispatch, per-index-set clamps).

# `.norm()` for to_val 105: inside cells 1, 3, 5 hold 0.2, 0.4, 0.6 and
# outside cells 2, 4, 6 hold 0.3, 0.5, 0.1. Under a 50th-percentile
# Increase_inside_decrease_outside that means, by hand:
#   Intervention_ptile_val  = quantile(c(.2,.4,.6), .5) = 0.4
#   Intervention_ptile_mean = mean(.4, .6)              = 0.50
#   Non_int_ptile_val       = quantile(c(.3,.5,.1), .5) = 0.3
#   Non_int_ptile_mean      = mean(.3, .5)              = 0.40
#   Perc_diff = (0.50 - 0.40) / 0.45 * 100 = 22.222 -> above the threshold of 5
# so the inside rows at/above 0.4 (cells 3, 5) rise and the outside rows
# at/above 0.3 (cells 2, 4) fall, in the SAME pass.
test_that("WR-08: Relative Increase_inside_decrease_outside moves both zones (NAT/SOC Urban_densification)", {
  fx <- .engine_fixture(
    list(.rel_entry(valency = "Increase_inside_decrease_outside")),
    scenario = "BOTHZONES"
  )
  dt <- .norm()
  before <- copy(dt)
  out <- .run_engine(fx, dt)

  tgt <- out$to_val == 105L
  rose <- tgt & out$cell_id %in% c(3L, 5L)
  fell <- tgt & out$cell_id %in% c(2L, 4L)
  expect_true(all(out$prob[rose] > before$prob[rose]))
  expect_true(all(out$prob[fell] < before$prob[fell]))

  # Below their respective percentiles, so untouched by either half.
  untouched <- tgt & out$cell_id %in% c(1L, 6L)
  expect_equal(out$prob[untouched], before$prob[untouched])
  # A class this intervention does not declare is never written (WR-02).
  expect_equal(out$prob[out$to_val == 104L], before$prob[before$to_val == 104L])

  a <- .audit_lines(fx$log_file)
  expect_length(a$iv, 1L)
  expect_match(
    a$iv, "id=iv_rel rank=1 type=Relative zone=Inside to_vals=105", fixed = TRUE
  )
  expect_identical(.audit_field(a$iv, "rows_target"), "6")
  expect_identical(.audit_field(a$iv, "rows_changed"), "4")
  expect_gt(as.integer(.audit_field(a$iv, "rows_changed")), 0L)
  # The signature of this valency: one pass, both directions.
  expect_identical(.audit_field(a$iv, "n_inc"), "2")
  expect_identical(.audit_field(a$iv, "n_dec"), "2")

  # The branch RAN. Neither the one-zone-empty skip nor the NaN guard fired,
  # so this is not a vacuous "nothing happened and nothing changed" pass.
  expect_false(any(grepl("intervention skip:", a$all, fixed = TRUE)))
  expect_true(any(grepl(
    "increasing the probability of the intervention pixels and decreasing the probability of the non-intervention pixels",
    a$all,
    fixed = TRUE
  )))
})

# CUL `Mining_outside_restraint`: Relative / Decrease with the intervention
# zone OUTSIDE the mask, which swaps Intervention_idx and Non_intervention_idx.
# Fixture: outside 0.5, inside 0.2, so
#   Intervention (outside) ptile mean = 0.50
#   Non-intervention (inside) ptile mean = 0.20
#   Perc_diff = (0.50 - 0.20) / 0.35 * 100 = 85.71 -> Decrease, Perc_diff >= 0
# decreases the INTERVENTION (outside) rows. The same fixture with
# zone = "Inside" would take the Perc_diff < 0 arm and RAISE those outside
# rows instead, so the direction asserted here genuinely pins the swap rather
# than merely observing that something moved.
test_that("WR-08b: Relative zone=Outside swaps the intervention zone (CUL Mining_outside_restraint)", {
  fx <- .engine_fixture(
    list(.rel_entry(valency = "Decrease", zone = "Outside")),
    scenario = "ZONESWAP"
  )
  dt <- .norm_split(inside = 0.2, outside = 0.5)
  before <- copy(dt)
  out <- .run_engine(fx, dt)

  tgt <- out$to_val == 105L
  outside <- tgt & out$cell_id %in% c(2L, 4L, 6L)
  inside <- tgt & out$cell_id %in% c(1L, 3L, 5L)
  expect_true(all(out$prob[outside] < before$prob[outside]))
  expect_equal(out$prob[inside], before$prob[inside])
  expect_equal(out$prob[out$to_val == 104L], before$prob[before$to_val == 104L])

  a <- .audit_lines(fx$log_file)
  expect_length(a$iv, 1L)
  expect_match(
    a$iv, "id=iv_rel rank=1 type=Relative zone=Outside to_vals=105", fixed = TRUE
  )
  expect_identical(.audit_field(a$iv, "rows_target"), "6")
  expect_identical(.audit_field(a$iv, "rows_changed"), "3")
  expect_identical(.audit_field(a$iv, "n_dec"), "3")
  expect_identical(.audit_field(a$iv, "n_inc"), "0")

  # The branch RAN.
  expect_false(any(grepl("intervention skip:", a$all, fixed = TRUE)))
  expect_true(any(grepl(
    "then decreasing the probability of the intervention pixels", a$all,
    fixed = TRUE
  )))
})

test_that("WR-08c: Increase_inside_decrease_outside with zone=Outside is rejected", {
  # No shipped config writes this pairing, and the guard is what keeps it that
  # way: the valency names the two zones itself, so an Outside zone would
  # silently invert the intervention's meaning.
  fx <- .engine_fixture(
    list(.rel_entry(
      valency = "Increase_inside_decrease_outside", zone = "Outside"
    )),
    scenario = "BADZONE"
  )
  dt <- .norm()
  before <- copy(dt)
  expect_error(.run_engine(fx, dt), "must be 'Inside'", fixed = TRUE)
  # The guard fires before any write, so nothing is half-adjusted.
  expect_equal(as.data.frame(dt), as.data.frame(before))
  expect_length(
    grep("AUDIT stage=intervention region=", .log_lines(fx$log_file), fixed = TRUE),
    0L
  )
})

