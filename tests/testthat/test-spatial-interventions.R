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
    Time_steps_implemented = list(2028L, 2032L)
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
    scenario = scenario,
    log_file = file.path(scratch, "t.log")
  )
}

.run_engine <- function(fx, normalized, year = 2028L) {
  implement_spatial_interventions(
    normalized = normalized,
    cell_index = .cell_index(),
    class_name_to_value = .class_map,
    interventions_dir = fx$interventions_dir,
    mask_dir = fx$mask_dir,
    scenario = fx$scenario,
    simulation_time_step = year,
    log_file = fx$log_file,
    region_label = "R1"
  )
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

test_that("engine: zeros from Absolute=0 stay zero after a Relative Increase", {
  fx <- .engine_fixture(list(
    .abs_entry(id = "iv_zero", rank = 1L),
    .rel_entry(id = "iv_inc", valency = "Increase", rank = 2L)
  ))
  out <- .run_engine(fx, .norm())
  inside <- out$to_val == 105L & out$cell_id %in% c(1L, 3L, 5L)
  expect_true(all(out$prob[inside] == 0))
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

