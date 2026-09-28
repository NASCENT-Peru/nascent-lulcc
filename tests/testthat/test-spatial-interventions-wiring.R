library(testthat)

# Phase 5 Plan 04 wiring checks for the spatial interventions engine.
#
# Static text assertions (same style as test-allocation-single-source-writer.R)
# lock the generate_probability_maps() hook shape and the fatal sourcing guard
# in scripts/run_allocation.r. They never run allocation itself.

.repo_root <- (function() {
  here <- tryCatch(normalizePath(sys.frame(1)$ofile %||% "."), error = function(e) ".")
  if (is.null(here) || identical(here, "")) here <- "."
  is_dir <- tryCatch(file.info(here)$isdir, error = function(e) NA)
  if (isTRUE(is_dir)) here <- file.path(here, "x")
  normalizePath(file.path(dirname(dirname(dirname(here)))), mustWork = FALSE)
})()

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

.wiring_config <- function(dir,
                           ref_grid_path = .write_wiring_ref_grid(dir),
                           simulation_year_steps = c(2024L, 2028L, 2032L),
                           profile_timestep_index = NULL) {
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
