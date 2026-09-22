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

.repo_root <- (function() {
  here <- tryCatch(normalizePath(sys.frame(1)$ofile %||% "."), error = function(e) ".")
  if (is.null(here) || identical(here, "")) here <- "."
  is_dir <- tryCatch(file.info(here)$isdir, error = function(e) NA)
  if (isTRUE(is_dir)) here <- file.path(here, "x")
  normalizePath(file.path(dirname(dirname(dirname(here)))), mustWork = FALSE)
})()

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

.write_mask <- function(dir, name = "mask_a.tif", cells = .mask_cells) {
  r <- terra::rast(nrows = 4, ncols = 5, xmin = 0, xmax = 5, ymin = 0, ymax = 4)
  v <- rep(NA_real_, terra::ncell(r))
  v[cells] <- 1
  terra::values(r) <- v
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
  c(list(
    Intervention_stage = stage,
    Intervention_ID = id,
    Mask_type = "Static",
    Intervention_mask = mask,
    Time_steps_implemented = years
  ), list(...))
}

.resolver_cols <- c(
  "scenario", "intervention_id", "rank", "year", "mask_type",
  "mask_name", "mask_path", "exists"
)

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
    Time_steps_implemented = list(2028L, 2032L)
  )
  .write_yaml(scratch, "BAU", list(entry))
  res <- resolve_intervention_masks(scratch, scratch, "BAU", years = c(2028L, 2032L))
  expect_identical(res$mask_name, c("m2028.tif", "m2032.tif"))
  expect_true(all(is.na(res$rank)))
})

test_that("resolver: non-implemented years give no rows and keep the 8 columns", {
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
    Time_steps_implemented = list(2028L, 2032L)
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
  cache <- new.env(parent = emptyenv())
  lut <- .mask_inside_lut(path, .cell_index(), cache)
  expect_type(lut, "logical")
  expect_length(lut, 6L)
  expect_identical(which(lut), c(1L, 3L, 5L))

  key <- normalizePath(path, mustWork = TRUE)
  expect_true(exists(key, envir = cache, inherits = FALSE))
  # Overwrite the file with a different mask: a cached second call must return
  # the cached object without re-reading it.
  .write_mask(scratch, cells = c(3L, 8L))
  lut2 <- .mask_inside_lut(path, .cell_index(), cache)
  expect_identical(lut2, lut)
  expect_identical(which(.mask_inside_lut(path, .cell_index(), new.env(parent = emptyenv()))), c(2L, 4L))
})
