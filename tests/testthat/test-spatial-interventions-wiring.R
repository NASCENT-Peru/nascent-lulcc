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

.wiring_config <- function(dir) {
  list(
    interventions_dir = dir,
    spat_prob_perturb_dir = dir,
    scenario_names = "NAT",
    simulation_year_steps = c(2024L, 2028L, 2032L)
  )
}

.intervention_lines <- function(x) x[grepl("^intervention ", x)]

test_that("pre-flight lists a missing mask for an active posterior year (D-14)", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir)
  file.create(file.path(dir, "nat_mask_2028.tif"))
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
  file.create(file.path(dir, c("nat_mask_2028.tif", "nat_mask_2032.tif")))
  res <- .wenv$validate_allocation_runtime(config = .wiring_config(dir))
  expect_length(.intervention_lines(res), 0L)
})

test_that("resolver config errors become 'intervention config:' lines without stopping", {
  withr::local_envvar(ALLOCATION_YEAR_POST_FILTER = NA)
  dir <- withr::local_tempdir()
  .write_nat_yaml(dir, dynamic_years = 2028L)
  file.create(file.path(dir, "nat_mask_2028.tif"))
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
  file.create(file.path(dir, "nat_mask_2028.tif"))
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
