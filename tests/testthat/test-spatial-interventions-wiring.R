library(testthat)

# Phase 5 Plan 04 wiring checks for the spatial interventions engine.
#
# Static text assertions (same style as test-allocation-single-source-writer.R)
# lock the generate_probability_maps() hook shape and the fatal sourcing guard
# in scripts/run_allocation.r. They never run allocation itself.

# IN-06: `.repo_root` is defined once in
# tests/testthat/helper-spatial-interventions.R, which testthat loads before
# this file under both `test_file()` and `test_dir()`. The guard keeps a bare
# `source()` of this file working WITHOUT reintroducing the sourcing-frame
# `ofile` bootstrap this plan removed - see the helper's header for why that
# bootstrap was wrong twice over.
if (!exists(".repo_root", inherits = TRUE)) {
  source(testthat::test_path("helper-spatial-interventions.R"))
}

allocation_text <- paste(
  readLines(file.path(.repo_root, "src", "allocation.r"), warn = FALSE),
  collapse = "\n"
)
run_script_text <- paste(
  readLines(file.path(.repo_root, "scripts", "run_allocation.r"), warn = FALSE),
  collapse = "\n"
)

# --- D-07: posterior year threading ------------------------------------------

test_that("generate_probability_maps() takes year_post as a formal (D-07)", {
  expect_match(
    allocation_text,
    "generate_probability_maps <- function\\([^)]*year_post"
  )
  expect_match(allocation_text, "#' @param year_post", fixed = TRUE)
})

test_that("setup_allocation_inputs() passes year_post to generate_probability_maps() (D-07)", {
  call_pos <- regexpr("generate_probability_maps(\n", allocation_text, fixed = TRUE)[[1L]]
  expect_gt(call_pos, 0L)
  call_text <- substr(allocation_text, call_pos, call_pos + 600L)
  expect_match(call_text, "year_post = year_post", fixed = TRUE)
})

test_that("the interventions hook uses the posterior year, not year_ant + step_length (D-07)", {
  expect_match(allocation_text, "simulation_time_step = year_post", fixed = TRUE)
  expect_no_match(allocation_text, 'year_ant + config[["step_length"]]', fixed = TRUE)
})

# --- Hook shape and placement --------------------------------------------------

test_that("the hook runs after normalisation and before the per-transition TIF writer", {
  norm_pos <- regexpr("data.table::setkey(normalized, row_idx)", allocation_text, fixed = TRUE)[[1L]]
  hook_pos <- regexpr("normalized <- implement_spatial_interventions(", allocation_text, fixed = TRUE)[[1L]]
  write_pos <- regexpr("Saving probability maps", allocation_text, fixed = TRUE)[[1L]]
  expect_gt(norm_pos, 0L)
  expect_gt(hook_pos, 0L)
  expect_gt(write_pos, 0L)
  expect_gt(hook_pos, norm_pos)
  expect_lt(hook_pos, write_pos)
})

test_that("the hook passes in-scope cell_index, class map, mask_dir and interventions_dir (D-05/D-06)", {
  hook_pos <- regexpr("normalized <- implement_spatial_interventions(", allocation_text, fixed = TRUE)[[1L]]
  write_pos <- regexpr("Saving probability maps", allocation_text, fixed = TRUE)[[1L]]
  hook_text <- substr(allocation_text, hook_pos - 400L, write_pos)
  expect_match(hook_text, "class_name_to_value <- load_allocation_class_map(config)", fixed = TRUE)
  expect_match(hook_text, "anterior_dt[, .(cell_id, ref_cell_id)]", fixed = TRUE)
  expect_match(hook_text, 'mask_dir\\s*=\\s*config\\[\\["spat_prob_perturb_dir"\\]\\]')
  expect_match(hook_text, 'interventions_dir\\s*=\\s*config\\[\\["interventions_dir"\\]\\]')
  expect_match(hook_text, "region_label\\s*=\\s*region_label")
  # The old merged-branch arguments are gone.
  expect_no_match(hook_text, "trans_rates_dt\\s*=\\s*trans_rates_dt")
  expect_no_match(hook_text, "anterior\\s*=\\s*anterior[,\n ]")
  # normalized is re-keyed on row_idx before the writer reads it.
  after_hook <- substr(allocation_text, hook_pos, write_pos)
  expect_match(after_hook, "data.table::setkey(normalized, row_idx)", fixed = TRUE)
})

test_that("CR-02: the hook threads ref_grid_path into the interventions engine", {
  # The engine validates every mask against this grid at runtime (D-13). If the
  # argument is ever dropped the call errors on a missing formal, but this
  # static check fails first and names the cause.
  call_pos <- regexpr(
    "normalized <- implement_spatial_interventions(", allocation_text, fixed = TRUE
  )[[1L]]
  expect_gt(call_pos, 0L)
  call_text <- substr(allocation_text, call_pos, call_pos + 600L)
  expect_match(call_text, 'ref_grid_path\\s*=\\s*config\\[\\["ref_grid_path"\\]\\]')
  # It sits immediately after mask_dir, mirroring the formals order.
  expect_match(
    call_text,
    'mask_dir\\s*=\\s*config\\[\\["spat_prob_perturb_dir"\\]\\],\\s*\n\\s*ref_grid_path\\s*='
  )
})

# --- Fatal sourcing guard in run_allocation.r --------------------------------

test_that("run_allocation.r sources implement_spatial_interventions.R after allocation.r and not the legacy file", {
  alloc_pos <- regexpr('"src/allocation.r"', run_script_text, fixed = TRUE)[[1L]]
  isi_pos <- regexpr('"src/implement_spatial_interventions.R"', run_script_text, fixed = TRUE)[[1L]]
  expect_gt(alloc_pos, 0L)
  expect_gt(isi_pos, alloc_pos)
  expect_no_match(run_script_text, "lulcc.spatprobmanipulation.r", fixed = TRUE)
})

test_that("run_allocation.r aborts on source errors and missing engine functions (T-05-12)", {
  expect_match(run_script_text, 'quit(save = "no", status = 1)', fixed = TRUE)
  expect_match(
    run_script_text,
    'exists("implement_spatial_interventions", mode = "function")',
    fixed = TRUE
  )
  expect_match(
    run_script_text,
    'exists("resolve_intervention_masks", mode = "function")',
    fixed = TRUE
  )
})

# --- D-14: Stage 7 pre-flight intervention mask checks ----------------------
#
# Sourced into one baseenv-parented env, like test-allocation-preflight.R, so
# validate_allocation_runtime() finds resolve_intervention_masks() at call time.

.wenv <- new.env(parent = baseenv())
sys.source(file.path(.repo_root, "src", "utils.r"), envir = .wenv)
sys.source(file.path(.repo_root, "src", "allocation.r"), envir = .wenv)
sys.source(file.path(.repo_root, "src", "implement_spatial_interventions.R"), envir = .wenv)

# WR-04: the same two files WITHOUT the interventions engine. baseenv() has no
# path back to the global env, so exists("resolve_intervention_masks") is FALSE
# inside validate_allocation_runtime() here. This is the shape a caller that
# forgot to source src/implement_spatial_interventions.R gets, and the gate must
# report it rather than skipping itself.
.wenv_noengine <- new.env(parent = baseenv())
sys.source(file.path(.repo_root, "src", "utils.r"), envir = .wenv_noengine)
sys.source(file.path(.repo_root, "src", "allocation.r"), envir = .wenv_noengine)

.write_nat_yaml <- function(dir, dynamic_years = c(2028L, 2032L)) {
  mask_lines <- paste0("    '", dynamic_years, "': nat_mask_", dynamic_years, ".tif")
  lines <- c(
    "- Intervention_stage: Allocation",
    "  Intervention_ID: Test_dynamic",
    "  Intervention_ranking: 1",
    "  Time_steps_implemented:",
    "    - 2028",
    "    - 2032",
    "  Transition_target_classes: mining",
    "  Prob_adjust_type: Absolute",
    "  Prob_adjust_value: 0",
    "  Prob_adjust_zone: Inside",
    "  Mask_type: Dynamic",
    "  Intervention_mask:",
    mask_lines
  )
  writeLines(lines, file.path(dir, "NAT_interventions.yml"))
}

# --- CR-02 pre-flight geometry fixtures --------------------------------------
#
# The reference grid the staged masks must sit on. Same 4x5 grid as
# test-spatial-interventions.R's .ref_grid(), kept local so the two files stay
# independent (they are sourced separately by test_dir()).
.wiring_ref_grid <- function() {
  terra::rast(nrows = 4, ncols = 5, xmin = 0, xmax = 5, ymin = 0, ymax = 4)
}

.write_wiring_ref_grid <- function(dir, name = "wiring_ref_grid.tif") {
  r <- .wiring_ref_grid()
  terra::values(r) <- seq_len(terra::ncell(r))
  path <- file.path(dir, name)
  terra::writeRaster(r, path, overwrite = TRUE)
  path
}

# A real single-layer mask. The pre-flight now opens every mask it resolves, so
# the zero-byte file.create() stubs these blocks used before would be reported
# as unreadable.
.write_wiring_mask <- function(dir, name, template = .wiring_ref_grid()) {
  r <- terra::rast(template)
  terra::values(r) <- rep(1, terra::ncell(r))
  path <- file.path(dir, name)
  terra::writeRaster(r, path, overwrite = TRUE)
  path
}

.write_wiring_masks <- function(dir, names) {
  vapply(names, function(n) .write_wiring_mask(dir, n), character(1))
}

# ON the reference grid but two-banded: compareGeom() ignores layer count, so
# only an explicit nlyr() check catches it (mirrors .write_multilayer_mask()).
.write_wiring_multilayer_mask <- function(dir, name, n_layers = 2L) {
  r <- .wiring_ref_grid()
  terra::values(r) <- rep(1, terra::ncell(r))
  stack <- do.call(c, rep(list(r), n_layers))
  path <- file.path(dir, name)
  terra::writeRaster(stack, path, overwrite = TRUE)
  path
}

# CR-01 value-domain fixtures. All three sit ON the reference grid and are
# single-layer, so they reach the new value-domain block with the nlyr and
# compareGeom branches satisfied.

# Every cell burned with `value` (255 = the gdal_rasterize / QGIS 8-bit default,
# 0 = an empty burn, 0.5 = a value the old range test could not see).
.write_wiring_valued_mask <- function(dir, name, value,
                                      template = .wiring_ref_grid()) {
  r <- terra::rast(template)
  terra::values(r) <- rep(value, terra::ncell(r))
  path <- file.path(dir, name)
  terra::writeRaster(r, path, overwrite = TRUE)
  path
}

# A true categorical GeoTIFF: its cells carry labels rather than numbers, so
# only terra::is.factor() catches it at pre-flight.
.write_wiring_categorical_mask <- function(dir, name,
                                           template = .wiring_ref_grid()) {
  r <- terra::rast(template)
  v <- rep(0L, terra::ncell(r))
  v[c(2L, 7L, 13L)] <- 1L
  terra::values(r) <- v
  levels(r) <- data.frame(value = c(0L, 1L), label = c("outside", "inside"))
  path <- file.path(dir, name)
  terra::writeRaster(r, path, overwrite = TRUE)
  path
}

# All NA: terra::freq() returns a zero-row table here, which is the branch the
# pre-flight uses to tolerate an all-NA mask (see the CR-01 block below).
.write_wiring_all_na_mask <- function(dir, name) {
  r <- .wiring_ref_grid()
  terra::values(r) <- rep(NA_real_, terra::ncell(r))
  path <- file.path(dir, name)
  terra::writeRaster(r, path, overwrite = TRUE)
  path
}

.wiring_config <- function(dir,
                           ref_grid_path = .write_wiring_ref_grid(dir),
                           simulation_year_steps = c(2024L, 2028L, 2032L),
                           profile_timestep_index = NULL,
                           simulation_output_dir = NULL) {
  cfg <- list(
    interventions_dir = dir,
    spat_prob_perturb_dir = dir,
    scenario_names = "NAT",
    ref_grid_path = ref_grid_path,
    simulation_year_steps = simulation_year_steps
  )
  if (!is.null(profile_timestep_index)) {
    cfg[["profile_timestep_index"]] <- profile_timestep_index
  }
  # Absent by default so every pre-existing block keeps exercising the
  # "key not configured -> check skipped silently" path (D-21).
  if (!is.null(simulation_output_dir)) {
    cfg[["simulation_output_dir"]] <- simulation_output_dir
  }
  cfg
}

.intervention_lines <- function(x) x[grepl("^intervention ", x)]

test_that("pre-flight lists a missing mask for an active posterior year (D-14)", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_mask(dir, "nat_mask_2028.tif")
  res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir))
  lines <- .intervention_lines(res)
  expect_length(lines, 1L)
  expect_match(lines, "intervention mask: missing ", fixed = TRUE)
  expect_match(lines, "nat_mask_2032.tif (scenario=NAT id=Test_dynamic year=2032)", fixed = TRUE)
})

test_that("pre-flight emits no intervention line when every mask is present", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir))
  expect_length(.intervention_lines(res), 0L)
})

test_that("resolver config errors become 'intervention config:' lines without stopping", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir, dynamic_years = 2028L)
  .write_wiring_mask(dir, "nat_mask_2028.tif")
  res <- NULL
  expect_no_error(res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir)))
  lines <- .intervention_lines(res)
  expect_length(lines, 1L)
  expect_match(lines, "^intervention config: .*year 2032")
})

test_that("ALLOCATION_YEAR_POST_FILTER narrows the checked years", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = "2028")
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_mask(dir, "nat_mask_2028.tif")
  res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir))
  expect_false(any(grepl("intervention mask: missing", res, fixed = TRUE)))
})

test_that("config without interventions_dir skips the intervention checks", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  cfg <- .wiring_config(dir)
  cfg[["interventions_dir"]] <- NULL
  res <- .wenv$validate_allocation_runtime(config = cfg)
  expect_length(.intervention_lines(res), 0L)
})

test_that("fixture-mode pre-flight never adds intervention lines", {
  res <- .wenv$validate_allocation_runtime(fixture = list(
    env = "MISSING_ENV_WIRING",
    packages = character(0),
    files = "missing/wiring.tif",
    dinamica = NULL
  ))
  expect_false(any(grepl("intervention", res, fixed = TRUE)))
  expect_true(any(grepl("MISSING_ENV_WIRING", res, fixed = TRUE)))
})

# --- Plan 05-10 gap closure: WR-04 / WR-05 / CR-02 pre-flight ----------------

test_that("WR-04: pre-flight reports a missing engine instead of skipping itself", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  # Not a single mask on disk: with the engine loaded this config produces two
  # "missing mask" lines, so a silent PASS here can only come from the
  # fail-open guard.
  res <- .wenv_noengine$validate_allocation_runtime(config = .wiring_config(dir))
  lines <- .intervention_lines(res)
  expect_length(lines, 1L)
  expect_match(lines, "resolve_intervention_masks\\(\\) not loaded")
  expect_match(
    lines,
    "source src/implement_spatial_interventions.R",
    fixed = TRUE
  )
})

test_that("WR-04: fixture mode still bypasses the engine check entirely", {
  res <- .wenv_noengine$validate_allocation_runtime(fixture = list(
    env = "MISSING_ENV_WIRING",
    packages = character(0),
    files = "missing/wiring.tif",
    dinamica = NULL
  ))
  expect_false(any(grepl("intervention", res, fixed = TRUE)))
})

test_that("WR-05: profile_timestep_index narrows the checked years", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_mask(dir, "nat_mask_2028.tif")
  # Posterior years are 2028, 2032. Index 1 pins the run to 2028, so the
  # unstaged 2032 mask must not block it.
  res <- .wenv$validate_allocation_runtime(
    config = .wiring_config(dir, profile_timestep_index = 1L)
  )
  expect_false(any(grepl("intervention mask: missing", res, fixed = TRUE)))
})

test_that("WR-05: profile_timestep_index still demands the year it does pin", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_mask(dir, "nat_mask_2028.tif")
  res <- .wenv$validate_allocation_runtime(
    config = .wiring_config(dir, profile_timestep_index = 2L)
  )
  lines <- .intervention_lines(res)
  expect_length(lines, 1L)
  expect_match(lines, "intervention mask: missing ", fixed = TRUE)
  expect_match(lines, "nat_mask_2032.tif (scenario=NAT id=Test_dynamic year=2032)", fixed = TRUE)
})

test_that("WR-05b: an out-of-range profile_timestep_index is reported, not ignored", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  res <- .wenv$validate_allocation_runtime(
    config = .wiring_config(dir, profile_timestep_index = 9L)
  )
  lines <- .intervention_lines(res)
  expect_length(lines, 1L)
  expect_match(lines, "profile_timestep_index=9 is outside the valid range")
  expect_match(lines, "1\\.\\.2")
})

test_that("WR-05b: a non-integer profile_timestep_index is reported", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  res <- .wenv$validate_allocation_runtime(
    config = .wiring_config(dir, profile_timestep_index = "not-a-number")
  )
  lines <- .intervention_lines(res)
  expect_length(lines, 1L)
  expect_match(lines, "profile_timestep_index must be a positive integer", fixed = TRUE)
})

test_that("WR-05c: profile_timestep_index applies after ALLOCATION_YEAR_POST_FILTER", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = "2032")
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  # The env filter leaves exactly one year (2032); index 1 must then select
  # THAT year, not the schedule's first posterior year. Only the 2032 mask is
  # staged, exactly as the driver would read it.
  .write_wiring_mask(dir, "nat_mask_2032.tif")
  res <- .wenv$validate_allocation_runtime(
    config = .wiring_config(dir, profile_timestep_index = 1L)
  )
  expect_length(.intervention_lines(res), 0L)
  expect_false(any(grepl("nat_mask_2028.tif", res, fixed = TRUE)))
})

test_that("CR-02: pre-flight rejects a mask that is not on the reference grid", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_mask(dir, "nat_mask_2028.tif")
  shifted <- terra::rast(nrows = 4, ncols = 5, xmin = 100, xmax = 105, ymin = 0, ymax = 4)
  .write_wiring_mask(dir, "nat_mask_2032.tif", template = shifted)
  res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir))
  lines <- .intervention_lines(res)
  expect_length(lines, 1L)
  expect_match(lines, "is not on the reference grid")
  expect_match(lines, "nat_mask_2032.tif", fixed = TRUE)
  # The aligned mask is silent.
  expect_false(any(grepl("nat_mask_2028.tif", lines, fixed = TRUE)))
})

test_that("CR-02: pre-flight rejects a multi-layer mask", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_mask(dir, "nat_mask_2028.tif")
  .write_wiring_multilayer_mask(dir, "nat_mask_2032.tif")
  res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir))
  lines <- .intervention_lines(res)
  expect_length(lines, 1L)
  expect_match(lines, "layers \\(expected 1\\)")
  expect_match(lines, "nat_mask_2032.tif", fixed = TRUE)
})

test_that("CR-02: pre-flight reports an unreadable mask file", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_mask(dir, "nat_mask_2028.tif")
  # Present but not a raster: resolve_intervention_masks() reports exists=TRUE.
  file.create(file.path(dir, "nat_mask_2032.tif"))
  res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir))
  lines <- .intervention_lines(res)
  expect_length(lines, 1L)
  expect_match(lines, "intervention mask: unreadable ", fixed = TRUE)
  expect_match(lines, "nat_mask_2032.tif", fixed = TRUE)
})

test_that("CR-02b: pre-flight reports an unreadable reference grid rather than skipping geometry", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  cfg <- .wiring_config(dir, ref_grid_path = file.path(dir, "no_such_ref_grid.tif"))
  res <- .wenv$validate_allocation_runtime(config = cfg)
  lines <- .intervention_lines(res)
  expect_length(lines, 1L)
  expect_match(lines, "cannot verify mask geometry", fixed = TRUE)
  expect_match(lines, "no_such_ref_grid.tif", fixed = TRUE)
})

test_that("CR-02b: an empty ref_grid_path is reported, not treated as 'no check needed'", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  cfg <- .wiring_config(dir, ref_grid_path = "")
  res <- .wenv$validate_allocation_runtime(config = cfg)
  expect_true(any(grepl("cannot verify mask geometry", res, fixed = TRUE)))
})

test_that("CR-02: each unique mask path is opened at most once per pre-flight", {
  # Two scenarios sharing one mask directory and one mask file: the mis-gridded
  # file must be reported once, not once per scenario.
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  yaml_lines <- c(
    "- Intervention_stage: Allocation",
    "  Intervention_ID: Test_static",
    "  Intervention_ranking: 1",
    "  Time_steps_implemented:",
    "    - 2028",
    "  Transition_target_classes: mining",
    "  Prob_adjust_type: Absolute",
    "  Prob_adjust_value: 0",
    "  Prob_adjust_zone: Inside",
    "  Mask_type: Static",
    "  Intervention_mask: shared_mask.tif"
  )
  writeLines(yaml_lines, file.path(dir, "NAT_interventions.yml"))
  writeLines(yaml_lines, file.path(dir, "BAU_interventions.yml"))
  shifted <- terra::rast(nrows = 4, ncols = 5, xmin = 100, xmax = 105, ymin = 0, ymax = 4)
  .write_wiring_mask(dir, "shared_mask.tif", template = shifted)
  cfg <- .wiring_config(dir, simulation_year_steps = c(2024L, 2028L))
  cfg[["scenario_names"]] <- c("NAT", "BAU")
  res <- .wenv$validate_allocation_runtime(config = cfg)
  lines <- .intervention_lines(res)
  expect_length(lines, 1L)
  expect_match(lines, "is not on the reference grid")
})

# --- CR-01 pre-flight value-domain fixtures ---------------------------------

test_that("CR-01: pre-flight rejects a mask whose values are outside {0,1,NA}", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  # Overwrite one of the resolved masks so the resolver still finds both.
  .write_wiring_valued_mask(dir, "nat_mask_2032.tif", value = 255)
  res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir))
  lines <- .intervention_lines(res)
  expect_length(lines, 1L)
  expect_true(grepl("outside {0,1,NA}", lines, fixed = TRUE))
  expect_true(startsWith(lines, "intervention mask: "))
  expect_match(lines, "nat_mask_2032.tif", fixed = TRUE)
  # The conforming mask is silent.
  expect_false(any(grepl("nat_mask_2028.tif", lines, fixed = TRUE)))
})

test_that("CR-01: pre-flight rejects a categorical mask", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  .write_wiring_categorical_mask(dir, "nat_mask_2032.tif")
  res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir))
  lines <- .intervention_lines(res)
  expect_length(lines, 1L)
  expect_true(grepl("is categorical/non-numeric", lines, fixed = TRUE))
  expect_true(startsWith(lines, "intervention mask: "))
  expect_match(lines, "nat_mask_2032.tif", fixed = TRUE)
})

test_that("CR-01: an all-NA mask does not crash the pre-flight", {
  # History: terra::minmax(compute = TRUE) returned NaN NaN on an all-NA raster,
  # so the bare `mm[1] >= 0 && mm[2] <= 1` the pre-flight used to run threw
  # "missing value where TRUE/FALSE needed" - itself a forbidden marker in the
  # smoke verifier - and `all(is.finite(mm))` was the guard against it. There is
  # no min/max call left, so the guard is now the empty-`vals` branch of the freq
  # scan: an NA cell never produces a freq row, so an all-NA mask yields a
  # zero-row table and is passed over in silence. An all-NA mask is degenerate
  # but its value-domain claim is vacuously true, so it must pass this gate; the
  # degeneracy is the smoke verifier's D-04 assertion to make.
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  .write_wiring_all_na_mask(dir, "nat_mask_2032.tif")
  expect_no_error(
    res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir))
  )
  expect_false(any(grepl(
    "missing value where TRUE/FALSE needed", res, fixed = TRUE
  )))
  expect_false(any(grepl("outside {0,1,NA}", res, fixed = TRUE)))
  expect_length(.intervention_lines(res), 0L)
  expect_false(any(grepl("has no cell equal to 1", res, fixed = TRUE)))
  expect_false(any(grepl("values could not be read", res, fixed = TRUE)))
})

test_that("CR-01: a valid 1-coded mask adds no intervention line", {
  # No-regression control for the three blocks above.
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir))
  expect_length(.intervention_lines(res), 0L)
})

# --- R51 gap-closure pre-flight fixtures (GAP-2, GAP-3, R51-WR-03, R51-IN-04) ---
#
# The blocks above pin in-range, single-defect masks, which is exactly why the
# gaps below shipped: a guard that is only ever shown conforming inputs and one
# obviously-broken input proves its envelope, not its predicate.

test_that("GAP-2: pre-flight rejects an all-zero mask (no cell equal to 1)", {
  # minmax() returned (0, 0) for this mask, so the old range test passed it, and
  # the engine's own bad-value set `v[!is.na(v) & v != 0 & v != 1]` is empty for
  # it too. With Prob_adjust_zone: Outside the all-FALSE lookup then made the
  # zone the whole region - the exact impact this phase exists to prevent.
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  .write_wiring_valued_mask(dir, "nat_mask_2032.tif", value = 0)
  res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir))
  lines <- .intervention_lines(res)
  expect_length(lines, 1L)
  expect_true(startsWith(lines, "intervention mask: "))
  expect_true(grepl("has no cell equal to 1", lines, fixed = TRUE))
  expect_match(lines, "nat_mask_2032.tif", fixed = TRUE)
  # An all-zero mask is in-domain, so it must NOT be reported as out-of-domain.
  expect_false(any(grepl("outside {0,1,NA}", lines, fixed = TRUE)))
  # The conforming sibling is silent.
  expect_false(any(grepl("nat_mask_2028.tif", lines, fixed = TRUE)))
})

test_that("GAP-3: pre-flight rejects an in-range fractional mask that a min/max test cannot see", {
  # 0.5 sits inside [0, 1], so `!(mm[1] >= 0 && mm[2] <= 1)` was FALSE and this
  # mask reached .mask_inside_lut() hours later, in the first region. It is also
  # the fixture that pins `digits = 12`: under terra::freq()'s default
  # digits = 0 this raster reports value 1 and reads as a valid all-1 mask.
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  .write_wiring_valued_mask(dir, "nat_mask_2032.tif", value = 0.5)
  res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir))
  lines <- .intervention_lines(res)
  expect_length(lines, 1L)
  expect_true(startsWith(lines, "intervention mask: "))
  expect_true(grepl("outside {0,1,NA}", lines, fixed = TRUE))
  expect_match(lines, "0.5", fixed = TRUE)
  expect_match(lines, "nat_mask_2032.tif", fixed = TRUE)
})

test_that("R51-WR-03: an unreadable value scan is a hard stop while an all-NA mask is still tolerated", {
  # Why a mock and not a corrupted file: corrupting the payload bytes of a
  # GeoTIFF does not reliably make freq() raise - the garbage reads back as
  # values or as zeros - so a genuinely unreadable value block cannot be
  # fabricated deterministically. local_mocked_bindings() replaces the binding
  # that `terra::freq(...)` resolves inside the baseenv()-parented .wenv and is
  # restored on block exit.
  #
  # Both directions of WR-03 in ONE run: 2028 is genuinely all NA (tolerated,
  # silent) and 2032 raises (hard stop). A fix that makes unreadable fatal by
  # also rejecting all-NA has not closed WR-03, it has re-broken D-02.
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  .write_wiring_all_na_mask(dir, "nat_mask_2028.tif")
  # Captured BEFORE the mock is installed: calling terra::freq() from inside the
  # mock would resolve to the mock and recurse.
  orig_freq <- terra::freq
  testthat::local_mocked_bindings(
    freq = function(x, ...) {
      if (any(grepl("nat_mask_2032", terra::sources(x), fixed = TRUE))) {
        stop("mocked truncated value block")
      }
      orig_freq(x, ...)
    },
    .package = "terra"
  )
  res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir))
  lines <- .intervention_lines(res)
  expect_length(lines, 1L)
  expect_true(startsWith(lines, "intervention mask: "))
  expect_match(lines, "nat_mask_2032.tif", fixed = TRUE)
  expect_true(grepl("values could not be read", lines, fixed = TRUE))
  expect_match(lines, "mocked truncated value block", fixed = TRUE)
  # The all-NA mask is still tolerated: no line, no crash, and in particular not
  # rejected for having no cell equal to 1.
  expect_false(any(grepl("nat_mask_2028.tif", lines, fixed = TRUE)))
  expect_false(any(grepl("has no cell equal to 1", res, fixed = TRUE)))
  expect_false(any(grepl(
    "missing value where TRUE/FALSE needed", res, fixed = TRUE
  )))
})

test_that("R51-WR-03: an unexpected freq shape is reported as unreadable, not accepted", {
  # The old code's `length(mm) == 2L` term degraded any unexpected shape to
  # "accept". Both staged masks are valid, so the only reason a line appears is
  # that the scan could not be interpreted.
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  testthat::local_mocked_bindings(
    freq = function(x, ...) list(),
    .package = "terra"
  )
  res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir))
  lines <- .intervention_lines(res)
  expect_length(lines, 2L)
  expect_true(all(startsWith(lines, "intervention mask: ")))
  expect_true(all(grepl("values could not be read", lines, fixed = TRUE)))
  expect_true(all(grepl("unexpected freq shape", lines, fixed = TRUE)))
})

test_that("REVIEW WR-03 (is.factor half): a raised categorical probe is reported as unreadable, not as 'not categorical'", {
  # The companion of the block above, for the other swallowed tryCatch: the old
  # handler returned FALSE, i.e. "not categorical", for a probe that never
  # answered. When the probe itself raises, the categorical verdict is UNKNOWN,
  # not false, and the mask must be reported as unreadable.
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  # Scoped to the staged masks: terra calls is.factor() internally on plain
  # vectors, so an unconditional mock raises inside terra itself rather than at
  # the pre-flight's probe. Everything that is not one of our mask rasters is
  # delegated to the binding captured before the mock was installed.
  #
  # freq() is stubbed to a well-formed all-1 table in the same breath, and that
  # is what makes this block isolate the is.factor branch: terra::freq() calls
  # is.factor() on the SpatRaster internally, so with a live freq() this mask
  # would be reported as unreadable through the FREQ handler even if the
  # is.factor handler still swallowed its failure. With freq() stubbed, the only
  # path to a line is the is.factor handler.
  orig_is_factor <- terra::is.factor
  testthat::local_mocked_bindings(
    is.factor = function(x, ...) {
      if (inherits(x, "SpatRaster") &&
          any(grepl("nat_mask_", terra::sources(x), fixed = TRUE))) {
        stop("mocked is.factor failure")
      }
      orig_is_factor(x, ...)
    },
    freq = function(x, ...) data.frame(layer = 1L, value = 1, count = 20L),
    .package = "terra"
  )
  res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir))
  lines <- .intervention_lines(res)
  expect_length(lines, 2L)
  expect_true(all(startsWith(lines, "intervention mask: ")))
  expect_true(all(grepl("values could not be read", lines, fixed = TRUE)))
  expect_true(all(grepl("mocked is.factor failure", lines, fixed = TRUE)))
  expect_false(any(grepl("is categorical/non-numeric", lines, fixed = TRUE)))
})

test_that("R51-IN-04: a mask that is both mis-coded and mis-gridded reports both defects", {
  # The pre-flight exists to surface everything before a multi-hour run. It used
  # to `next` on the first defect, so the operator fixed the burn value, re-ran
  # Stage 7, and only then learned the grid was wrong too.
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  shifted <- terra::rast(nrows = 4, ncols = 5, xmin = 100, xmax = 105, ymin = 0, ymax = 4)

  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  .write_wiring_valued_mask(dir, "nat_mask_2032.tif", value = 255, template = shifted)
  res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir))
  lines <- .intervention_lines(res)
  expect_length(lines, 2L)
  expect_true(all(grepl("nat_mask_2032.tif", lines, fixed = TRUE)))
  expect_true(any(grepl("outside {0,1,NA}", lines, fixed = TRUE)))
  expect_true(any(grepl("is not on the reference grid", lines, fixed = TRUE)))

  # Same for the categorical/geometry pair: the categorical verdict skips the
  # value scan but no longer hides the geometry check.
  dir2 <- withr::local_tempdir()
  .write_nat_yaml(dir2)
  .write_wiring_masks(dir2, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  .write_wiring_categorical_mask(dir2, "nat_mask_2032.tif", template = shifted)
  res2 <- .wenv$validate_allocation_runtime(config = .wiring_config(dir2))
  lines2 <- .intervention_lines(res2)
  expect_length(lines2, 2L)
  expect_true(all(grepl("nat_mask_2032.tif", lines2, fixed = TRUE)))
  expect_true(any(grepl("is categorical/non-numeric", lines2, fixed = TRUE)))
  expect_true(any(grepl("is not on the reference grid", lines2, fixed = TRUE)))
})

test_that("GAP-3: the pre-flight value scan is a freq scan with explicit digits and no minmax", {
  loop_pos <- regexpr(
    "for (mask_path in existing_masks)", allocation_text, fixed = TRUE
  )[[1L]]
  expect_gt(loop_pos, 0L)
  block <- substr(allocation_text, loop_pos, loop_pos + 8000L)
  expect_match(block, "terra::freq(m, digits = 12)", fixed = TRUE)
  code <- readLines(file.path(.repo_root, "src", "allocation.r"), warn = FALSE)
  code <- code[!grepl("^[[:space:]]*#", code)]
  expect_false(any(grepl("minmax", code, fixed = TRUE)))
})

# --- D-07: repo YAMLs agree with the posterior-year schedule ----------------

.find_key <- function(x, key) {
  if (!is.list(x)) return(NULL)
  if (!is.null(names(x)) && key %in% names(x)) return(x[[key]])
  for (el in x) {
    hit <- .find_key(el, key)
    if (!is.null(hit)) return(hit)
  }
  NULL
}

for (.cfg_name in c("local_config.yaml", "hpc_config.yaml")) {
  for (.scn in c("BAU", "NAT", "CUL", "SOC")) {
    local({
      cfg_name <- .cfg_name
      scn <- .scn
      test_that(sprintf("%s_interventions.yml years are posterior years of %s (D-07)", scn, cfg_name), {
        cfg <- yaml::read_yaml(file.path(.repo_root, "config", cfg_name))
        steps <- as.integer(unlist(.find_key(cfg, "simulation_year_steps")))
        expect_gt(length(steps), 1L)
        posterior <- utils::tail(steps, -1L)

        entries <- yaml::read_yaml(file.path(.repo_root, "config", paste0(scn, "_interventions.yml")))
        years <- sort(unique(as.integer(unlist(lapply(entries, function(e) e[["Time_steps_implemented"]])))))
        expect_gt(length(years), 0L)
        expect_true(
          all(years %in% posterior),
          info = sprintf("non-posterior years: %s", paste(setdiff(years, posterior), collapse = ", "))
        )

        resolved <- NULL
        expect_no_error(resolved <- .wenv$resolve_intervention_masks(
          file.path(.repo_root, "config"), tempdir(), scn, years
        ))
        expect_gt(nrow(resolved), 0L)
        expect_false(any(grepl("/", resolved$mask_name, fixed = TRUE)))
      })
    })
  }
}

# --- Plan 13: probability-delta telemetry wiring (D-18, D-21) ---------------

test_that("D-18: the hook passes telemetry_dir = work_dir", {
  hook_pos <- regexpr(
    "normalized <- implement_spatial_interventions(", allocation_text, fixed = TRUE
  )[[1L]]
  write_pos <- regexpr("Saving probability maps", allocation_text, fixed = TRUE)[[1L]]
  expect_gt(hook_pos, 0L)
  expect_gt(write_pos, hook_pos)
  hook_text <- substr(allocation_text, hook_pos, write_pos)
  expect_match(hook_text, "telemetry_dir = work_dir", fixed = TRUE)

  # work_dir IS the region directory: prob_map_dir is built under it, and the
  # smoke verifier resolves the same directory from --output-root / --scenario
  # / --year / --region. The CSV therefore lands next to probability_map_dir.
  expect_match(
    allocation_text,
    'prob_map_dir <- file.path(work_dir, "probability_map_dir")',
    fixed = TRUE
  )
  expect_match(
    allocation_text,
    'region_suffix <- gsub(" ", "_", tolower(region_label))',
    fixed = TRUE
  )
})

test_that("D-21: pre-flight reports an unwritable simulation output root", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  # A regular file standing where a directory must be: no ancestor of
  # <blocker>/runs is a writable directory, which is the shape an operator
  # gets from a typo'd or stale simulation_output_dir on shared scratch.
  blocker <- file.path(dir, "not_a_directory")
  writeLines("regular file", blocker)

  res <- .wenv$validate_allocation_runtime(
    config = .wiring_config(dir, simulation_output_dir = file.path(blocker, "runs"))
  )
  lines <- .intervention_lines(res)
  expect_length(lines, 1L)
  expect_match(
    lines, "intervention telemetry: output directory not writable", fixed = TRUE
  )
  expect_match(lines, "runs", fixed = TRUE)
})

test_that("D-21: a writable simulation output root adds no telemetry line", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))
  out_root <- file.path(dir, "sim_out")
  dir.create(out_root)

  res <- .wenv$validate_allocation_runtime(
    config = .wiring_config(dir, simulation_output_dir = out_root)
  )
  expect_length(.intervention_lines(res), 0L)
})

test_that("D-21: an output root that does not exist yet is judged by its nearest ancestor", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  .write_wiring_masks(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif"))

  # The run creates <out_root>/<scenario>/<year>/region_<region> itself, so a
  # not-yet-created root under a writable parent is NOT a gap — and the
  # pre-flight must not create it either.
  out_root <- file.path(dir, "sim_out_not_created_yet")
  res <- .wenv$validate_allocation_runtime(
    config = .wiring_config(dir, simulation_output_dir = out_root)
  )
  expect_length(.intervention_lines(res), 0L)
  expect_false(dir.exists(out_root))
})
