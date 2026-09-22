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
